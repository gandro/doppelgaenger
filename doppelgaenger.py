import os, stat
import io, hashlib
import sqlite3
import concurrent.futures
import multiprocessing
import heapq
import sys
import time # todo
import fnmatch

"""
    TODO:

        - Blacklist
"""

root = "/tmp/./ramdisk"

class File:

    def __init__(self, name, path, size = 0, time = 0, sha1 = None):
        self.name = name
        self.path = path
        self.size = size
        self.time = time
        self.sha1 = sha1

class Tree:

    def __init__(self, rootpath, dbfile = None):
        self.rootpath = rootpath.encode()
        if not dbfile:
            dbfile = os.path.join(rootpath, '.doppelgaenger.db')
        self.dbfile = dbfile
        try:
            dgignore = open(os.path.join(rootpath, '.dgignore'), "rb")
        except IOError:
            self.blacklist = []
        else:
            with dgignore:
                self.blacklist = [line.strip() for line in dgignore.readlines()]
        self.blacklist.append(os.path.relpath(dbfile.encode(), self.rootpath))

    def walk(self):
        files = list()
        action = Action("Searching files")

        for dirpath, dirnames, filenames in os.walk(self.rootpath, onerror = action.report_warning):
            for file in filenames:
                abspath = os.path.join(dirpath, file)
                relpath = os.path.relpath(abspath, self.rootpath)
                for pattern in self.blacklist:
                    if fnmatch.fnmatch(relpath, pattern):
                        break
                else:
                    try:
                        name = file
                        path = os.path.relpath(dirpath, self.rootpath)
                        size = os.path.getsize(abspath)
                        time = int(os.path.getmtime(abspath))
                    except OSError as error:
                        action.report_warning(error)
                        pass
                    else:
                        action.update_progress()
                        files.append(File(name = name, path = path, size = size, time = time))

        action.finish_progress()
        return files

    def calculate_checksums(self, files):
        action = Action("Calculating checksums")
        action.start_progress(len(files))
        with concurrent.futures.ThreadPoolExecutor(multiprocessing.cpu_count()) as executor:
            futures = [executor.submit(self._file_checksum, file, action) for file in files]
            for future in concurrent.futures.as_completed(futures):
                action.update_progress()
                yield future.result()
            action.finish_progress()

    def _file_checksum(self, file, action = None):
        sha1 = hashlib.sha1()
        path = os.path.join(self.rootpath, file.path, file.name)
        try:
            blob = open(path, 'rb')
        except IOError as error:
            if action:
                action.report_warning(error)
        else:
            with blob:
                data = blob.read(io.DEFAULT_BUFFER_SIZE)
                while data:
                    sha1.update(data)
                    data = blob.read(io.DEFAULT_BUFFER_SIZE)
        file.sha1 = sha1.hexdigest()
        return file

    def _tree_checksum_aggregate(self, action):
        class TreeChecksumAggregate:
            def __init__(self):
                self.heap = []

            def step(self, name, size, time, sha1):
                data = b"\0".join((
                    sha1.encode(), str(size).encode(), str(time).encode(), name
                ))
                heapq.heappush(self.heap, data)

            def finalize(self):
                sha1 = hashlib.sha1()
                while self.heap:
                    sha1.update(heapq.heappop(self.heap))
                action.update_progress()
                return sha1.hexdigest()

        return TreeChecksumAggregate

class Index:

    DB = "main"
    FILETABLE = "FileTable"
    TREETABLE = "TreeTable"

    def __init__(self, tree):
        self.tree = tree

        self.connection = sqlite3.connect(self.tree.dbfile)
        self.connection.row_factory = sqlite3.Row
        self.connection.create_function("dirname", 1, os.path.dirname)

        self.db = self.connection.cursor()

    def execute(self, query, parameters = ()):
        query = query.format(
            db = self.DB,
            filetable = self.FILETABLE, old_filetable = "Old" + self.FILETABLE,
            treetable = self.TREETABLE, old_treetable = "Old" + self.TREETABLE
        )
        self.db.execute(query, parameters)

    def create(self):
        self.create_filetable()
        self.create_treetable()

    def create_filetable(self, checksum = True):
        self.execute("DROP TABLE IF EXISTS {filetable}")
        self.execute("""CREATE TABLE {filetable} (
            name    TEXT        NOT NULL,
            path    TEXT        NOT NULL,
            size    INTEGER     NOT NULL,
            time    INTEGER     NOT NULL,
            sha1    TEXT,
            PRIMARY KEY (name, path)
        )""")

        files = self.tree.walk()

        if checksum:
            filelist = self.tree.calculate_checksums(files)
        else:
            filelist = iter(files)

        for file in filelist:
            self.execute("""INSERT INTO {filetable} (name, path, size, time, sha1)
                                VALUES (?, ?, ?, ?, ?)""",
                                (file.name, file.path, file.size, file.time, file.sha1))
        self.connection.commit()


    def create_treetable(self):
        action = Action("Processing folders")
        self.connection.create_aggregate("sha1", 4, self.tree._tree_checksum_aggregate(action))

        self.execute("DROP TABLE IF EXISTS {treetable}")
        self.execute("""CREATE TABLE {treetable} (
            folder    TEXT        PRIMARY KEY,
            sha1      TEXT
        )""")

        self.execute("""INSERT INTO {treetable} (folder, sha1)
                            SELECT path as folder, sha1(name, size, time, sha1) as sha1
                              FROM {filetable} GROUP BY path
                        """)
        action.finish_progress()
        self.connection.commit()

    def update(self):
        self.execute("DROP TABLE IF EXISTS {old_filetable}")
        self.execute("ALTER TABLE {filetable} RENAME TO {old_filetable}")
        self.create_filetable(checksum = False)

        self.execute("""INSERT OR REPLACE INTO {filetable} (name, path, time, size, sha1)
                              SELECT old.name, old.path, old.time, old.size, old.sha1
                                FROM {old_filetable} as old, {filetable} as new
                                WHERE (new.name = old.name)
                                AND (new.path = old.path)
                                AND (new.time = old.time)
                                AND (new.size = old.size)
                        """)

        self.execute("SELECT name, path FROM {filetable} WHERE sha1 IS NULL")
        files = [File(**row) for row in self.db.fetchall()]
        for file in self.tree.calculate_checksums(files):
            self.execute("UPDATE {filetable} SET sha1 = ? WHERE name = ? AND path = ?",
                                (file.sha1, file.name, file.path))

        self.execute("DROP TABLE {old_filetable}")
        self.create_treetable()
        self.connection.commit()

    def __del__(self):
        self.connection.close()

class IndexQuery:

    CONSTANTS = {
        "FileTable": Index.FILETABLE,
        "TreeTable": Index.TREETABLE,
        "FileResultA": """
            join_path(a.path, a.name) as a_path, a.size as a_size, 
            a.time as a_time, a.sha1 as a_sha1
        """,
        "TreeResultA": """
            a.folder as a_folder, a.sha1 as a_sha1
        """,
        "FileResultB": """
            join_path(b.path, b.name) as b_path, b.size as b_size,
            b.time as b_time, b.sha1 as b_sha1
        """,
        "TreeResultB": """
            b.folder as b_folder, b.sha1 as b_sha1
        """
    }

    MISSING_IN_A = [
    """
        CREATE TEMPORARY VIEW MissingFoldersInA AS
        SELECT b.folder, b.sha1 FROM b.{TreeTable} as b
        WHERE
            NOT EXISTS (
                SELECT a.folder, a.sha1 FROM a.{TreeTable} as a
                WHERE a.folder = b.folder OR a.sha1 = b.sha1
            )
    """,
    """
        SELECT {FileResultB} FROM b.{FileTable} as b
        WHERE
            NOT EXISTS (
                SELECT a.name, a.path, a.sha1 FROM a.{FileTable} as a
                WHERE (a.name = b.name AND a.path = b.path) OR a.sha1 = b.sha1
            )
            AND b.path NOT IN (SELECT folder FROM MissingFoldersInA)
    """,
    """SELECT {TreeResultB} FROM MissingFoldersInA as b"""
    ]

    MISSING_IN_B = [
    """
        CREATE TEMPORARY VIEW MissingFoldersInB AS
        SELECT a.folder, a.sha1 FROM a.{TreeTable} as a
        WHERE
            NOT EXISTS (
                SELECT b.folder, b.sha1 FROM b.{TreeTable} as b
                WHERE a.folder = b.folder OR a.sha1 = b.sha1
            )
    """,
    """
        SELECT {FileResultA} FROM a.{FileTable} as a
        WHERE
            NOT EXISTS (
                SELECT b.name, b.path, b.sha1 FROM b.{FileTable} as b
                WHERE (a.name = b.name AND a.path = b.path) OR a.sha1 = b.sha1
            )
            AND a.path NOT IN (SELECT folder FROM MissingFoldersInB)
    """,
    """SELECT {TreeResultA} FROM MissingFoldersInB as a"""
    ]

    SHA1_CONFLICT = """
        SELECT {FileResultA}, {FileResultB}
        FROM a.{FileTable} as a, b.{FileTable} as b
        WHERE 
            join_path(a.path, a.name) = join_path(b.path, b.name) 
            AND a.sha1 != b.sha1
    """

    PATH_CONFLICT = [
    """
        CREATE TEMPORARY VIEW ConflictingFolders AS
        SELECT a.folder as a_folder, b.folder as b_folder, a.sha1
        FROM a.{TreeTable} as a INNER JOIN b.{TreeTable} as b USING (sha1)
        WHERE
            a.folder NOT IN (
                SELECT folder FROM b.{TreeTable} as b WHERE b.sha1 = a.sha1
            )
            OR b.folder NOT IN (
                SELECT folder FROM a.{TreeTable} as a WHERE a.sha1 = b.sha1
            )
    """,
    """
        SELECT {FileResultA}, {FileResultB}
        FROM a.{FileTable} as a INNER JOIN b.{FileTable} as b USING (sha1)
        WHERE
            (join_path(a.path, a.name) NOT IN (
                SELECT join_path(path, name) FROM b.{FileTable} as b
                WHERE b.sha1 = a.sha1
            )
            OR join_path(b.path, b.name) NOT IN (
                SELECT join_path(path, name) FROM a.{FileTable} as a
                WHERE a.sha1 = b.sha1
            ))
            AND NOT EXISTS (
                SELECT * FROM ConflictingFolders 
                WHERE a_folder = a.path AND b_folder = b.path
            )

     """,
     """SELECT a_folder, b_folder, sha1 as a_sha1, sha1 as b_sha1 FROM ConflictingFolders""",
     ]



class IndexComparator:
    def __init__(self, tree_a, tree_b):
        self.connection = sqlite3.connect(":memory:")
        self.connection.row_factory = sqlite3.Row
        self.connection.create_function("join_path", 2, os.path.join)
        self.db = self.connection.cursor()

        self.db.execute("ATTACH DATABASE ? AS ?", (tree_a.dbfile, "a"))
        self.db.execute("ATTACH DATABASE ? AS ?", (tree_b.dbfile, "b"))

    def query(self, query):
        self.db.execute(query.format(**IndexQuery.CONSTANTS))
        return self.db.fetchall()

    def queries(self, queries):
        for query in queries:
            yield self.query(query)

class Action:

    def __init__(self, action):
        self.action = action
        self.total = 0
        self.processed = 0
        self.last_update = 0
        self.output_length = 0

    def start_progress(self, total):
        self.total = total
        self.redraw_progress()

    def update_progress(self, step = 1):
        self.processed += step

        if time.time() - self.last_update > 0.2:
            self.last_update = time.time()
            self.redraw_progress()

    def finish_progress(self):
        self.redraw_progress(done = True)

    def redraw_progress(self, done = False):
        eol = ", done.\n" if done else "\r"

        if self.total > 0:
            output = "{action}: {percent:.2%} ({processed}/{total}){eol}"
            output = output.format(
                action = self.action,
                percent = self.processed/self.total,
                processed = self.processed, total = self.total,
                eol = eol
            )
        else:
            output = "{action}: {processed}{eol}"
            output = output.format(
                action = self.action, processed = self.processed, eol = eol
            )

        self.output_length = len(output)
        sys.stderr.write(output)

    def parse_error(self, error):
        if isinstance(error, OSError) or isinstance(error, IOError):
            error = error.strerror + ": " + error.filename

        return error

    def report_warning(self, error):
        error = self.parse_error(error)
        self.report_message("Warning: {error}".format(error = error))

    def report_message(self, message):
        if self.output_length:
            sys.stderr.write(" " * self.output_length + "\r")

        sys.stderr.write("{action}: {message}\n".format(action = self.action, message = message))
        if self.processed:
            self.redraw_progress()


a = Tree("/tmp/a")
b = Tree("/tmp/b")


#for file in a.walk():
    #print(file.__dict__)

i = Index(a)
#i.create()
i.update()

i = Index(b)
#i.create()
i.update()

#i.update()


#i = Index(b)
#i.create()
#i.update()

c = IndexComparator(a, b)
#for f in c.queries(IndexQuery.PATH_CONFLICT):
#    for s in f:
#        print(dict(s))


for f in c.query(IndexQuery.SHA1_CONFLICT):
    print(dict(f))

##print(IndexQuery._IndexQuery__COLUMNS)
#for f in c.query(IndexQuery.COMPLETE_DIFF):
#    out = { "conflict" : "", "a" : "<missing>", "b" : "<missing>" }
#    if f["a_path"]:
#        out["a"] = f["a_path"]
#    if f["b_path"]:
#        out["b"] = f["b_path"]
#    if f["a_path"] and not f["b_path"]:
#        out["conflict"] = "---"
#    if f["b_path"] and not f["a_path"]:
#        out["conflict"] = "+++"
#    if f["b_path"] == f["a_path"]:
#        out["conflict"] = "!!!"
#    if f["b_sha1"] == f["a_sha1"]:
#        out["conflict"] = "==="
#    print(out["a"], out["conflict"], out["b"], sep = "\t\t")
#    #print(dict(f))

pass
