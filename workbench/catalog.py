import contextlib
import sqlite3
from pathlib import Path
from .io import canonical,digest,read_bundle

class Catalog:
    def __init__(self,path):
        self.path=Path(path);self.path.parent.mkdir(parents=True,exist_ok=True)
        with self.connection() as conn:
            conn.execute('CREATE TABLE IF NOT EXISTS imports(id INTEGER PRIMARY KEY, name TEXT NOT NULL, sha TEXT NOT NULL UNIQUE)')
            conn.execute('CREATE TABLE IF NOT EXISTS source_rows(import_id INTEGER NOT NULL REFERENCES imports(id), source TEXT NOT NULL, ordinal INTEGER NOT NULL, body BLOB NOT NULL, PRIMARY KEY(import_id,source,ordinal))')
    @contextlib.contextmanager
    def connection(self):
        conn=sqlite3.connect(self.path,timeout=5);conn.execute('PRAGMA foreign_keys=ON')
        try:
            with conn:yield conn
        finally:conn.close()
    def ingest(self,root):
        manifest,tables=read_bundle(root);sha=digest({'manifest':manifest,'tables':tables})
        with self.connection() as conn:
            conn.execute('BEGIN IMMEDIATE')
            row=conn.execute('SELECT id FROM imports WHERE sha=?',(sha,)).fetchone()
            if row:return row[0]
            number=conn.execute('INSERT INTO imports(name,sha) VALUES(?,?)',(manifest['name'],sha)).lastrowid
            conn.executemany('INSERT INTO source_rows VALUES(?,?,?,?)',((number,source,i,canonical(row)) for source,rows in tables.items() for i,row in enumerate(rows)))
        return number
    def summary(self):
        with self.connection() as conn:
            return [{'id':r[0],'name':r[1],'sha256':r[2],'rows':r[3]} for r in conn.execute('SELECT i.id,i.name,i.sha,count(r.ordinal) FROM imports i LEFT JOIN source_rows r ON i.id=r.import_id GROUP BY i.id ORDER BY i.id')]
    def rows(self,number,source):
        import json
        with self.connection() as conn:
            return [json.loads(r[0]) for r in conn.execute('SELECT body FROM source_rows WHERE import_id=? AND source=? ORDER BY ordinal',(number,source))]
