"""Small ClickHouse HTTP client using only Python's standard library."""
import base64
import json
import os
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


class ClickHouse:
    def __init__(self):
        self.url = os.getenv("CLICKHOUSE_URL", "http://localhost:8123").rstrip("/")
        user = os.getenv("CLICKHOUSE_USER", "homework")
        password = os.getenv("CLICKHOUSE_PASSWORD", "homework_local_only")
        self.auth = base64.b64encode(f"{user}:{password}".encode()).decode()

    def query(self, sql, **settings):
        request = Request(self.url + "/?" + urlencode(settings), data=sql.encode(), headers={
            "Authorization": "Basic " + self.auth,
            "Content-Type": "text/plain; charset=utf-8",
        })
        try:
            with urlopen(request, timeout=120) as response:
                return response.read().decode(), json.loads(response.headers.get("X-ClickHouse-Summary", "{}"))
        except HTTPError as exc:
            raise RuntimeError(f"ClickHouse HTTP {exc.code}: {exc.read().decode()}") from exc

    def execute_file(self, path):
        # Project SQL has no semicolons inside quoted literals or comments.
        sql = "\n".join(line.split("--", 1)[0] for line in Path(path).read_text().splitlines())
        for statement in sql.split(";"):
            if statement.strip():
                self.query(statement)


def read_query(path):
    return Path(path).read_text().strip().rstrip(";")
