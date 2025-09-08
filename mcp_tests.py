
import asyncio
import sys
import unittest
from pathlib import Path
from typing import Any, Dict, List
from fastmcp import Client 
import socket, time, subprocess
from nodejobs.jobs import Jobs, JobRecord
import json

def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


CSV_PATH = Path("./data/extract_2/sensors.csv")
SERVICE_SCRIPT = Path("csv_mcp.py")  # file that defines CsvProtocolService.run_stdio

def _data(x):
    if isinstance(x, dict):
        return x.get("data", x)
    if hasattr(x, "data"):   
        return x.data
    return x

class CsvMCPStdIOTests(unittest.TestCase):
    loop = None
    client: Client | None = None

    @classmethod
    def setUpClass(cls):
        if not CSV_PATH.exists():
            raise FileNotFoundError(f"CSV not found: {CSV_PATH}")

        cls.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(cls.loop)

        host, port, path = "127.0.0.1", _free_port(), "/mcp"
        cls.base_url = f"http://{host}:{port}{path}"

        cls.jobs = Jobs(db_path="./test_nodejobs_db")
        cls.job_id = "csv_mcp_http_test"

        cmd = [
            sys.executable, str(SERVICE_SCRIPT),
            "run_http",
            f"csv_path={CSV_PATH}",
            "transport=http",
            f"host={host}",
            f"port={port}",
            f"path={path}",
        ]
        cls.jobs.run(command=cmd, job_id=cls.job_id)


        # Verify port is open and /mcp route works
        def _port_open(host, port, timeout=5):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(timeout)
                try:
                    s.connect((host, port))
                    return True
                except Exception:
                    return False
        # Manual TCP check
        timeout = time.time() + 10
        ok = False
        while time.time() < timeout:
            if _port_open(host, port):
                ok = True
                break
            print(f"Port {port} not open yet, retrying...")
            time.sleep(2)

        assert ok, f"Port {port} not accepting connections"
        print("---SERVER ONLINE --- ")

        # Wait until it transitions out of starting
        timeout = time.time() + 10
        while cls.jobs.get_status(cls.job_id).status == JobRecord.Status.c_starting:
            assert time.time() < timeout, "Server failed to start in time"
            time.sleep(0.2)
        cls.client =  Client(cls.base_url)

    @classmethod
    def tearDownClass(cls):
        try:
            if cls.client is not None:
                cls.loop.run_until_complete(cls.client.close())
        finally:
            cls.client = None
            if hasattr(cls, "jobs") and cls.jobs:
                cls.jobs.stop(cls.job_id)
            if cls.loop is not None:
                cls.loop.close()
                cls.loop = None

    def test_schema(self):
        async def asy_test():         
            #async with Client(self.base_url) as client: # for some reason I dont have time to look at, I need to make a new one
            async with  self.client as client:
                res = await client.call_tool("csv_schema", {})
                sch = json.loads(_data(res))
                print(sch)
                for c in ["name", "time", "Latitude", "Longitude", "Height", "source"]:
                    self.assertIn(c, sch["columns"])
                self.assertGreater(sch["rows"], 0)

        asyncio.run(asy_test())


    def test_head(self):
        async def asy_test():
            async with  self.client as client:
                res = await client.call_tool("csv_head", {"n": 3})
                jsondata:str = _data(res)
                rows: List[Dict[str, Any]] = json.loads(jsondata)
                self.assertGreaterEqual(len(rows), 1)
                for c in ["name", "time", "Latitude", "Longitude", "Height", "source"]:
                    self.assertIn(c, rows[0])
        asyncio.run(asy_test())

    def test_sql_count(self):
        async def asy_test():
            async with  self.client as client:
                res = await client.call_tool("sql", {"query": "SELECT COUNT(*) AS n FROM df", "limit": 1})
                out = json.loads(_data(res))
                self.assertEqual(len(out), 1)
                self.assertGreater(out[0]["n"], 0)
        asyncio.run(asy_test())

    def test_sql_filter_and_limit(self):
        async def asy_test():
            q = "SELECT name, Height FROM df WHERE Height > 372.997 ORDER BY Height DESC"
            async with  self.client as client:
                res = await client.call_tool("sql", {"query": q, "limit": 5})
                out = json.loads(_data(res))
                self.assertLessEqual(len(out), 5)
                if out:
                    self.assertIn("name", out[0])
                    self.assertIn("Height", out[0])
        asyncio.run(asy_test())

    def test_summary(self):
        async def asy_test():
            async with  self.client as client:
                res = await client.call_tool("summary", {})
                summ = json.loads(_data(res))
                self.assertIn("summary", summ)
                self.assertIn("Height", summ["summary"])
                self.assertTrue(len(summ["summary"]["Height"]) > 0)
        asyncio.run(asy_test())


if __name__ == "__main__":
    try:
        unittest.main()
        #unittest.main(defaultTest="CsvMCPStdIOTests.test_schema")
        #unittest.main(defaultTest="CsvMCPStdIOTests.test_head")
        
    finally:
        # Safety net: ensure any spawned client is closed if unittest aborts early
        cls = CsvMCPStdIOTests
        if getattr(cls, "client", None) is not None and getattr(cls, "loop", None) is not None:
            try:
                cls.loop.run_until_complete(cls.client.aclose())
            except Exception:
                pass
            try:
                cls.loop.close()
            except Exception:
                pass
