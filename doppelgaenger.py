import os, stat
import io, hashlib
import sqlite3
import concurrent.futures
import multiprocessing

"""
    TODO:

        - Verzeichnis-SHA1 oder so?
        - Blacklist
"""

root = "/tmp/./ramdisk"

class File:

    def __init__(self, path, size = 0, time = 0, sha1 = None):
        self.path = path
        self.size = size
        self.time = time
        self.sha1 = sha1

class Tree:

    def __init__(self, rootpath, dbfile = None):
        self.rootpath = rootpath
        if not dbfile:
            dbfile = os.path.join(self.rootpath, '.doppelganger.db')
        self.dbfile = dbfile

    def walk(self):
        for dirpath, dirnames, filenames in os.walk(self.rootpath):
            for file in filenames:
                abspath = os.path.join(dirpath, file)

                path = os.path.relpath(abspath, self.rootpath)
                size = os.path.getsize(abspath)
                time = int(os.path.getmtime(abspath))

                yield File(path = path, size = size, time = time)

    def __sha1sum(self, file):
        sha1 = hashlib.sha1()
        path = os.path.join(self.rootpath, file.path)
        with open(path, 'rb') as blob:
            data = blob.read(io.DEFAULT_BUFFER_SIZE)
            while data:
                sha1.update(data)
                data = blob.read(io.DEFAULT_BUFFER_SIZE)
        file.sha1 = sha1.hexdigest()
        return file

    def calc_checksums(self, files):
        with concurrent.futures.ThreadPoolExecutor(multiprocessing.cpu_count()) as executor:
            futures = [executor.submit(self.__sha1sum, file) for file in files]
            for future in concurrent.futures.as_completed(futures):
                yield future.result()

class Index:

    db = "main"
    table = "FileTable"
    old_table = "OldFileTable"

    def __init__(self, tree):
        self.tree = tree

        self.connection = sqlite3.connect(self.tree.dbfile)
        self.connection.row_factory = sqlite3.Row
        self.sql = self.connection.cursor()

    def execute(self, query, parameters = ()):
        query = query.format(
            db = self.db,
            table = self.table,
            old_table = self.old_table
        )
        self.sql.execute(query, parameters)

    def create(self, checksum = True):
        self.execute("DROP TABLE IF EXISTS {table}")
        self.execute("""CREATE TABLE main.FileTable (
            path    TEXT        PRIMARY KEY,
            size    INTEGER     NOT NULL,
            time    INTEGER     NOT NULL,
            sha1    TEXT
        )""")

        files = self.tree.walk()
        if checksum:
            files = self.tree.calc_checksums(files)

        for file in files:
            self.execute("""INSERT INTO {table} (path, size, time, sha1)
                                VALUES (?, ?, ?, ?)""",
                                (file.path, file.size, file.time, file.sha1))
        self.connection.commit()

    def update(self):
        self.execute("ALTER TABLE {table} RENAME TO {old_table}")
        self.create(checksum = False)

        self.execute("""INSERT OR REPLACE INTO {table} (path, time, size, sha1)
                              SELECT new.path, new.time, new.size, old.sha1
                                FROM {old_table} as old, {table} as new
                                WHERE (new.path = old.path)
                                AND (new.time = old.time) AND (new.size = old.size)
                        """)

        self.execute("SELECT path FROM {table} WHERE sha1 IS NULL")
        files = [File(**row) for row in self.sql.fetchall()]
        for file in self.tree.calc_checksums(files):
            self.execute("UPDATE {table} SET sha1 = ? WHERE path = ?",
                                (file.sha1, file.path))

        self.execute("DROP TABLE {old_table}")
        self.connection.commit()

    def __del__(self):
        self.connection.close()

class IndexQuery:

    __TABLE = Index.table

    __COLUMNS_A = """
        a.path as a_path, a.size as a.size, a.time as a_time, a.sha1 as a_sha1
    """

    __COLUMNS_B = """
        b.path as b_path, b.size as b_size, b.time as b_time, b.sha1 as b_sha1
    """

    __COLUMNS = """
        a.path as a_path, a.size as a_size, a.time as a_time, a.sha1 as a_sha1,
        b.path as b_path, b.size as b_size, b.time as b_time, b.sha1 as b_sha1
    """

    SHA1_CONFLICT = """
        SELECT {columns} FROM a.{table} as a, b.{table} as b
        WHERE a.path = b.path AND a.sha1 != b.sha1
    """.format(columns = __COLUMNS, table = __TABLE)

    PATH_CONFLICT = """
        SELECT {columns} FROM a.{table} as a, b.{table} as b
        WHERE a.path != b.path AND a.sha1 = b.sha1
    """.format(columns = __COLUMNS, table = __TABLE)

    MISSING_IN_A = """
        SELECT {columns_b} FROM b.{table} as b
        WHERE NOT EXISTS (
            SELECT a.path, a.sha1 FROM a.{table} as a
            WHERE a.path = b.path OR a.sha1 = b.sha1
        )
    """.format(columns_b = __COLUMNS_B, table = __TABLE)

    MISSING_IN_B = """
        SELECT {columns_a} FROM a.{table} as a
        WHERE NOT EXISTS (
            SELECT b.path, b.sha1 FROM b.{table} as b
            WHERE a.path = b.path OR a.sha1 = b.sha1
        )

    """.format(columns_a = __COLUMNS_A, table = __TABLE)

    COMPLETE_DIFF = """
        SELECT {columns} FROM a.{table} as a, b.{table} as b
        WHERE
            (a.path != b.path AND a.sha1 = b.sha1) OR
            (a.path = b.path AND a.sha1 != b.sha1)

        UNION

        SELECT
            a.path as a_path, a.size as a_size, a.time as a_time, a.sha1 as a_sha1,
            NULL   as b_path, NULL   as b_size, NULL   as b_time, NULL   as b_sha1
        FROM a.{table} as a
        WHERE NOT EXISTS (
            SELECT b.path, b.sha1 FROM b.{table} as b
            WHERE a.path = b.path OR a.sha1 = b.sha1
        )

        UNION

        SELECT
            NULL   as a_path, NULL   as b_size, NULL   as b_time, NULL   as b_sha1,
            b.path as b_path, b.size as b_size, b.time as b_time, b.sha1 as b_sha1
        FROM b.{table} as b
        WHERE NOT EXISTS (
            SELECT a.path, a.sha1 FROM a.{table} as a
            WHERE a.path = b.path OR a.sha1 = b.sha1
        )
    """.format(columns = __COLUMNS, table = __TABLE)


class IndexComparator:

    def __init__(self, tree_a, tree_b):
        self.connection = sqlite3.connect(":memory:")
        self.connection.row_factory = sqlite3.Row
        self.sql = self.connection.cursor()

        self.sql.execute("ATTACH DATABASE ? AS ?", (tree_a.dbfile, "a"))
        self.sql.execute("ATTACH DATABASE ? AS ?", (tree_b.dbfile, "b"))

    def query(self, query):
        self.sql.execute(query)
        return self.sql.fetchall()

a = Tree("/tmp/a")
b = Tree("/tmp/b")

i = Index(a)
i.create()
i.update()

i = Index(b)
i.create()
i.update()

c = IndexComparator(a, b)

#print(IndexQuery._IndexQuery__COLUMNS)
for f in c.query(IndexQuery.COMPLETE_DIFF):
    out = { "conflict" : "", "a" : "<missing>", "b" : "<missing>" }
    if f["a_path"]:
        out["a"] = f["a_path"]
    if f["b_path"]:
        out["b"] = f["b_path"]
    if f["a_path"] and not f["b_path"]:
        out["conflict"] = "---"
    if f["b_path"] and not f["a_path"]:
        out["conflict"] = "+++"
    if f["b_path"] == f["a_path"]:
        out["conflict"] = "!!!"
    if f["b_sha1"] == f["a_sha1"]:
        out["conflict"] = "==="
    print(out["a"], out["conflict"], out["b"], sep = "\t\t")
    #print(dict(f))

pass
