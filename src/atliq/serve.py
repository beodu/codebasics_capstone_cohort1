"""HTTP server that re-queries gold tables on every page load."""
from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .dashboard import render_html
from .loggingutil import log


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        log(fmt % args)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path in ("/", "/index.html", "/dashboard"):
            qs = parse_qs(parsed.query)
            body = render_html(
                from_date=(qs.get("from") or [None])[0] or None,
                to_date=(qs.get("to") or [None])[0] or None,
                category=(qs.get("category") or [None])[0] or None,
            ).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_error(404)


def serve(host: str = "127.0.0.1", port: int = 8080) -> None:
    httpd = ThreadingHTTPServer((host, port), Handler)
    log(f"dashboard  http://{host}:{port}")
    log("each request reads atliq_dw.gold on SQL Server")
    httpd.serve_forever()
