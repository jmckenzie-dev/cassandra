#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Serve the Short Read Protection presentation on every IPv4 interface."""

from __future__ import annotations

import argparse
import errno
import ipaddress
import random
import socket
import struct
import sys
import threading
import traceback
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Iterable, TextIO
from urllib.parse import urlsplit


BIND_ADDRESS = "0.0.0.0"
FIRST_PORT = 50_001
LAST_PORT = 65_535
PAGE_NAME = "short-read-protection.html"


class Tee:
    """Write one text stream to the console and a timestamped log."""

    def __init__(self, streams: Iterable[TextIO], lock: threading.Lock) -> None:
        self._streams = tuple(streams)
        self._lock = lock

    @property
    def encoding(self) -> str:
        return self._streams[0].encoding

    def write(self, text: str) -> int:
        with self._lock:
            for stream in self._streams:
                stream.write(text)
                stream.flush()
        return len(text)

    def flush(self) -> None:
        with self._lock:
            for stream in self._streams:
                stream.flush()

    def isatty(self) -> bool:
        return self._streams[0].isatty()

    def fileno(self) -> int:
        return self._streams[0].fileno()


class PresentationServer(ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Serve docs/short-read-protection.html on 0.0.0.0 using the first "
            "available port found above 50000."
        )
    )
    parser.add_argument(
        "--probe",
        action="store_true",
        help="bind, fetch the page once through loopback, and exit",
    )
    parser.add_argument(
        "--log-directory",
        type=Path,
        help="override the default project-local ./logs directory",
    )
    return parser.parse_args()


def configure_output(log_directory: Path) -> tuple[TextIO, TextIO, TextIO, Path]:
    log_directory.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    log_path = log_directory / f"serve-short-read-protection-python-{timestamp}.log"
    log_file = log_path.open("a", encoding="utf-8", buffering=1)
    original_stdout = sys.stdout
    original_stderr = sys.stderr
    lock = threading.Lock()
    sys.stdout = Tee((original_stdout, log_file), lock)  # type: ignore[assignment]
    sys.stderr = Tee((original_stderr, log_file), lock)  # type: ignore[assignment]
    return original_stdout, original_stderr, log_file, log_path


def restore_output(original_stdout: TextIO, original_stderr: TextIO, log_file: TextIO) -> None:
    sys.stdout.flush()
    sys.stderr.flush()
    sys.stdout = original_stdout
    sys.stderr = original_stderr
    log_file.close()


def make_handler(page: bytes) -> type[BaseHTTPRequestHandler]:
    class PresentationHandler(BaseHTTPRequestHandler):
        server_version = "CassandraSRPPresentation/1.0"

        def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
            self._serve(include_body=True)

        def do_HEAD(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
            self._serve(include_body=False)

        def _serve(self, include_body: bool) -> None:
            path = urlsplit(self.path).path
            if path == "/favicon.ico":
                self.send_response(204)
                self.end_headers()
                return

            if path not in ("/", "/index.html", f"/{PAGE_NAME}"):
                self.send_error(404, "This server exposes only the SRP presentation")
                return

            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(page)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Referrer-Policy", "no-referrer")
            self.send_header(
                "Content-Security-Policy",
                "default-src 'none'; style-src 'unsafe-inline'; "
                "script-src 'unsafe-inline'; img-src data:; base-uri 'none'; "
                "form-action 'none'; frame-ancestors 'none'",
            )
            self.end_headers()
            if include_body:
                self.wfile.write(page)

        def log_message(self, message_format: str, *args: object) -> None:
            timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
            print(f"[{timestamp}] {self.client_address[0]} {message_format % args}")

    return PresentationHandler


def bind_server(page: bytes) -> PresentationServer:
    """Bind directly while scanning, avoiding a check-then-bind race."""

    span = LAST_PORT - FIRST_PORT + 1
    first_candidate = random.SystemRandom().randrange(FIRST_PORT, LAST_PORT + 1)
    handler = make_handler(page)

    for offset in range(span):
        port = FIRST_PORT + ((first_candidate - FIRST_PORT + offset) % span)
        try:
            return PresentationServer((BIND_ADDRESS, port), handler)
        except OSError as error:
            if error.errno not in (errno.EADDRINUSE, errno.EACCES):
                raise

    raise RuntimeError(f"No available IPv4 port in {FIRST_PORT}-{LAST_PORT}")


def ioctl_interface_addresses() -> set[str]:
    """Return Linux/POSIX IPv4 interface addresses when ioctl is available."""

    addresses: set[str] = set()
    try:
        import fcntl  # POSIX only
    except ImportError:
        return addresses

    for _, interface_name in socket.if_nameindex():
        request = struct.pack("256s", interface_name.encode("utf-8")[:15])
        interface_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            result = fcntl.ioctl(interface_socket.fileno(), 0x8915, request)
        except OSError:
            continue
        finally:
            interface_socket.close()
        addresses.add(socket.inet_ntoa(result[20:24]))

    return addresses


def hostname_addresses() -> set[str]:
    addresses: set[str] = set()
    for hostname in {socket.gethostname(), socket.getfqdn()}:
        try:
            records = socket.getaddrinfo(hostname, None, socket.AF_INET)
        except socket.gaierror:
            continue
        addresses.update(record[4][0] for record in records)
    return addresses


def routed_addresses() -> set[str]:
    """Ask the kernel which local IPv4 addresses selected routes would use."""

    addresses: set[str] = set()
    for destination in ("192.0.2.1", "198.51.100.1", "8.8.8.8"):
        route_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            route_socket.connect((destination, 9))
            addresses.add(route_socket.getsockname()[0])
        except OSError:
            continue
        finally:
            route_socket.close()
    return addresses


def interface_addresses() -> list[str]:
    addresses = {"127.0.0.1"}
    addresses.update(ioctl_interface_addresses())
    addresses.update(hostname_addresses())
    addresses.update(routed_addresses())
    addresses.discard(BIND_ADDRESS)

    def address_order(value: str) -> tuple[int, int]:
        address = ipaddress.ip_address(value)
        category = 2 if address.is_loopback else 1 if address.is_link_local else 0
        return category, int(address)

    return sorted(addresses, key=address_order)


def address_label(value: str) -> str:
    address = ipaddress.ip_address(value)
    if address.is_loopback:
        return "loopback"
    if address.is_link_local:
        return "link-local"
    if address.is_private:
        return "private/interface"
    return "public/interface"


def print_endpoints(server: PresentationServer, page_path: Path, log_path: Path) -> None:
    port = server.server_address[1]
    print()
    print("Short Read Protection presentation")
    print(f"  page: {page_path}")
    print(f"  log:  {log_path}")
    print(f"  bind: http://{BIND_ADDRESS}:{port}/ (all IPv4 interfaces)")
    print()
    print("Clickable interface URLs:")
    for address in interface_addresses():
        print(f"  http://{address}:{port}/  [{address_label(address)}]")
    print()
    print("Press Ctrl-C to stop.")
    print()


def probe_server(server: PresentationServer, expected_page: bytes) -> None:
    thread = threading.Thread(target=server.serve_forever, name="srp-probe", daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{server.server_address[1]}/"
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            received = response.read()
            if response.status != 200 or received != expected_page:
                raise RuntimeError("Presentation probe received an unexpected response")
        print(f"Probe succeeded: {url} returned {len(received)} bytes with HTTP 200")
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def run(arguments: argparse.Namespace, log_path: Path) -> int:
    script_directory = Path(__file__).resolve().parent
    page_path = script_directory / PAGE_NAME
    if not page_path.is_file():
        raise FileNotFoundError(f"Presentation not found: {page_path}")

    page = page_path.read_bytes()
    server = bind_server(page)
    print_endpoints(server, page_path, log_path)

    if arguments.probe:
        probe_server(server, page)
        return 0

    try:
        with server:
            server.serve_forever(poll_interval=0.25)
    except KeyboardInterrupt:
        print("Stopping Short Read Protection presentation server.")
    return 0


def main() -> int:
    arguments = parse_arguments()
    project_root = Path(__file__).resolve().parent.parent
    log_directory = arguments.log_directory or project_root / "logs"
    original_stdout, original_stderr, log_file, log_path = configure_output(log_directory)

    exit_code = 0
    try:
        print(f"Python server output is also being written to {log_path}")
        exit_code = run(arguments, log_path)
    except Exception:
        traceback.print_exc()
        exit_code = 1
    finally:
        restore_output(original_stdout, original_stderr, log_file)

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
