import tempfile
import unittest
from pathlib import Path
from workbench.catalog import Catalog
from workbench.io import canonical,digest,read_json,write_json,inspect_bundle
ROOT=Path(__file__).resolve().parents[1]

class CatalogTests(unittest.TestCase):
    def test_import_is_immutable_and_reopenable(self):
        info=inspect_bundle(ROOT/'samples')
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/'raw.sqlite3';c=Catalog(path);number=c.ingest(ROOT/'samples')
            self.assertEqual(Catalog(path).ingest(ROOT/'samples'),number)
            self.assertEqual(c.summary()[0]['rows'],sum(x['rows'] for x in info['files']))
            source=info['files'][0]['path']
            self.assertEqual(c.rows(number,source),read_json(ROOT/'samples'/source))
    def test_json_and_digest(self):
        self.assertEqual(digest({'a':1,'b':2}),digest({'b':2,'a':1}))
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'x.json';write_json(p,{'值':'甲'});self.assertEqual(read_json(p),{'值':'甲'})
            p.write_text('{"a":1,"a":2}',encoding='utf-8')
            with self.assertRaises(ValueError):read_json(p)
        with self.assertRaises(ValueError):canonical({'x':float('nan')})
    def test_path_confinement(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d);write_json(p/'bundle.json',{'name':'x','files':[{'path':'../outside.json'}]})
            with self.assertRaises(ValueError):inspect_bundle(p)
