import unittest
from pathlib import Path
from workbench.io import read_json
from tests.domain_checks import check
class DomainTests(unittest.TestCase):
    def test_existing_domain_operations(self):
        check(self,Path(__file__).resolve().parents[1],read_json)
