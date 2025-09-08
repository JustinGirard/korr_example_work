# csv_context_tests.py
import unittest
from pathlib import Path
from typing import List, Dict, Any
import json
from csv_mcp import CsvContext  # adjust import if your class lives elsewhere
CSV_PATH = Path("./data/extract_2/sensors.csv")

class UnitTests(unittest.TestCase):
    def setUp(self):
        if not CSV_PATH.exists():
            self.skipTest(f"CSV not found: {CSV_PATH}")
        self.ctx = CsvContext(str(CSV_PATH))
        self.expected_cols = ["name", "time", "Latitude", "Longitude", "Height", "source"]

    def test_schema(self):
        sch = json.loads(self.ctx.schema())
        self.assertIn("columns", sch)
        for c in self.expected_cols:
            #print(c)
            self.assertIn(c, sch["columns"])
        self.assertGreater(sch["rows"], 0)

    def test_head(self):
        rows: List[Dict[str, Any]] = json.loads(self.ctx.head(3))
        print(rows)
        self.assertGreaterEqual(len(rows), 1)
        for c in self.expected_cols:
            self.assertIn(c, rows[0])

    def test_sql_count(self):
        res = json.loads(self.ctx.sql("SELECT COUNT(*) AS n FROM df", limit=1))
        self.assertEqual(len(res), 1)
        self.assertIsInstance(res[0]["n"], (int, float))
        self.assertGreater(res[0]["n"], 0)

    def test_sql_filter_and_limit(self):
        res = json.loads(self.ctx.sql("SELECT name, Height FROM df WHERE Height > 372.997 ORDER BY Height DESC", limit=5))
        self.assertLessEqual(len(res), 5)
        if res:
            self.assertIn("name", res[0])
            self.assertIn("Height", res[0])

    def test_summary(self):
        summ = json.loads(self.ctx.summary())
        self.assertIn("summary", summ)
        self.assertIn("Height", summ["summary"])  # numeric column present
        self.assertTrue(len(summ["summary"]["Height"]) > 0)

if __name__ == "__main__":
    unittest.main()
    #unittest.main(defaultTest="UnitTests.test_head")