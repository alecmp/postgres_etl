import unittest
from src.etl.extract import extract_data

class TestExtract(unittest.TestCase):
    def test_extract(self):
        data = extract_data('test_api')
        self.assertGreater(len(data), 0)
