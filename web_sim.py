"""
Live TCP visualiser for Q1 and Q2 (web UI).

Runs the real servers (q1_server.py, q2_web_server.py) and a real client
socket, and streams every step to the browser as Server-Sent Events:
the TCP three-way handshake, each data segment and ACK, the FIN teardown,
TCP state changes and the server's own console output.

Note: Python's socket API cannot see the SYN/ACK/FIN control segments
(the OS sends them inside connect(), close(), ...). They are reported at
the moment the matching socket call happens; sequence/ack numbers follow
the TCP rules using random initial sequence numbers and the real payload
sizes. Payloads and server output are real.

Run:   python web_sim.py
Open:  http://127.0.0.1:8000   (opened automatically)
"""
import json
import os
import queue
import random
import subprocess
import sys
import threading
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from socket import socket, AF_INET, SOCK_STREAM, SHUT_WR
from urllib.parse import parse_qs, urlparse

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INDEX_HTML = os.path.join(BASE_DIR, "web", "index.html")
HOST = "127.0.0.1"
UI_PORT = 8000
Q1_PORT = 12000
Q2_PORT = 6789

# The lab servers use fixed ports, so only one simulation may run at a time
run_lock = threading.Lock()


class Cancelled(Exception):
    """Raised inside a run when the browser disconnects."""


class Run:
    """One simulation run; its events are queued for the SSE stream."""

    def __init__(self, delay):
        self.delay = delay
        self.events = queue.Queue()
        self.cancelled = threading.Event()
        self.t0 = time.perf_counter()
        self.packet_no = 0

    def emit(self, kind, **data):
        data["type"] = kind
        data["t"] = round((time.perf_counter() - self.t0) * 1000, 2)
        self.events.put(data)

    def pause(self, factor=1.0):
        if self.cancelled.wait(self.delay * factor):
            raise Cancelled()

    def state(self, side, name):
        self.emit("state", side=side, state=name)


class TcpConnection:
    """Tracks sequence/ack numbers for one client <-> server connection."""

    def __init__(self, run, client_port, server_port):
        self.run = run
        self.ports = {"client": client_port, "server": server_port}
        self.isn = {"client": random.getrandbits(32), "server": random.getrandbits(32)}
        self.next_seq = dict(self.isn)

    def packet(self, sender, flags, payload=b"", info=""):
        receiver = "server" if sender == "client" else "client"
        seq = self.next_seq[sender]
        ack = self.next_seq[receiver] if "ACK" in flags else None
        self.run.packet_no += 1
        self.run.emit(
            "packet",
            no=self.run.packet_no,
            src=sender,
            srcPort=self.ports[sender],
            dstPort=self.ports[receiver],
            flags=flags,
            seq=seq,
            ack=ack,
            relSeq=(seq - self.isn[sender]) % 2**32,
            relAck=None if ack is None else (ack - self.isn[receiver]) % 2**32,
            len=len(payload),
            payload=payload.decode("utf-8", errors="replace"),
            hex=payload.hex(),
            info=info,
        )
        # SYN and FIN each use up one sequence number
        self.next_seq[sender] = (seq + len(payload) + ("SYN" in flags) + ("FIN" in flags)) % 2**32


# ---------------------------------------------------------------- servers
def start_server(run, script, port):
    """Launch a lab server, stream its stdout as log events, wait until it listens."""
    probe = socket(AF_INET, SOCK_STREAM)
    probe.settimeout(0.5)
    in_use = probe.connect_ex((HOST, port)) == 0
    probe.close()
    if in_use:
        raise RuntimeError(
            f"Port {port} is already in use. Stop any other copy of the lab servers and try again."
        )

    proc = subprocess.Popen(
        [sys.executable, "-u", script],
        cwd=BASE_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    ready = threading.Event()

    def pump_output():
        for line in proc.stdout:
            line = line.rstrip("\n")
            run.emit("log", text=line)
            if "ready" in line:
                ready.set()
        ready.set()  # process ended

    reader = threading.Thread(target=pump_output, daemon=True)
    reader.start()
    if not ready.wait(5) or proc.poll() is not None:
        proc.kill()
        raise RuntimeError(f"{script} failed to start")

    run.emit("call", side="server", text=f"bind(('{HOST}', {port}))  listen()  accept()")
    run.state("server", "LISTEN")
    return proc, reader


def stop_server(proc, reader):
    if proc.poll() is None:
        proc.terminate()
    proc.wait()
    reader.join(1)


# ---------------------------------------------------------------- TCP phases
def open_connection(run, server_port):
    """Connect a real client socket and report the three-way handshake."""
    sock = socket(AF_INET, SOCK_STREAM)
    sock.settimeout(5)
    sock.bind((HOST, 0))  # choose the ephemeral port now so the SYN can show it
    conn = TcpConnection(run, sock.getsockname()[1], server_port)
    run.emit("conn", host=HOST, clientPort=conn.ports["client"], serverPort=server_port)
    run.emit("phase", title="TCP three-way handshake")
    run.emit("call", side="client", text=f"connect(('{HOST}', {server_port}))")

    started = time.perf_counter()
    sock.connect((HOST, server_port))
    connect_ms = (time.perf_counter() - started) * 1000
    run.pause(0.5)

    conn.packet("client", ["SYN"], info="Client asks to open a connection")
    run.state("client", "SYN_SENT")
    run.pause()
    conn.packet("server", ["SYN", "ACK"], info="Server agrees and sends its own SYN")
    run.state("server", "SYN_RCVD")
    run.pause()
    conn.packet("client", ["ACK"], info=f"Handshake complete; connect() returned in {connect_ms:.2f} ms")
    run.state("client", "ESTABLISHED")
    run.state("server", "ESTABLISHED")
    run.pause()
    return sock, conn


def close_connection(run, sock, conn, server_closes_first, fin_received=False):
    """Close the connection and report the four-way FIN teardown."""
    run.emit("phase", title="Connection teardown")
    if server_closes_first:
        if not fin_received and sock.recv(1) != b"":
            raise RuntimeError("Expected the server to close the connection")
        conn.packet("server", ["FIN", "ACK"], info="Server closed its socket")
        run.state("server", "FIN_WAIT_1")
        run.state("client", "CLOSE_WAIT")
        run.emit("call", side="client", text="recv() returned b''  (FIN received)")
        run.pause()
        conn.packet("client", ["ACK"], info="Client acknowledges the server's FIN")
        run.state("server", "FIN_WAIT_2")
        run.pause()
        run.emit("call", side="client", text="close()")
        sock.close()
        conn.packet("client", ["FIN", "ACK"], info="Client closes its side")
        run.state("client", "LAST_ACK")
        run.pause()
        conn.packet("server", ["ACK"], info="Server acknowledges; connection closed")
        run.state("server", "TIME_WAIT")
        run.state("client", "CLOSED")
        run.pause()
        run.state("server", "CLOSED")
    else:
        run.emit("call", side="client", text="shutdown(SHUT_WR)")
        sock.shutdown(SHUT_WR)
        conn.packet("client", ["FIN", "ACK"], info="Client has nothing more to send")
        run.state("client", "FIN_WAIT_1")
        run.state("server", "CLOSE_WAIT")
        run.pause()
        conn.packet("server", ["ACK"], info="Server acknowledges the client's FIN")
        run.state("client", "FIN_WAIT_2")
        run.pause()
        if sock.recv(1) != b"":
            raise RuntimeError("Expected the server to close the connection")
        conn.packet("server", ["FIN", "ACK"], info="Server closed its socket")
        run.state("server", "LAST_ACK")
        run.pause()
        conn.packet("client", ["ACK"], info="Client acknowledges; connection closed")
        run.state("client", "TIME_WAIT")
        run.state("server", "CLOSED")
        sock.close()
        run.pause()
        run.state("client", "CLOSED")
    run.pause()


# ---------------------------------------------------------------- scenarios
def scenario_q1(run, messages):
    run.emit("scenario", id="q1", title="Q1 · TCP echo server", script="q1_server.py", serverPort=Q1_PORT)
    proc, reader = start_server(run, "q1_server.py", Q1_PORT)
    try:
        run.pause()
        sock, conn = open_connection(run, Q1_PORT)
        try:
            run.emit("phase", title="Data transfer")
            said_bye = False
            for message in messages:
                data = message.encode()
                run.emit("call", side="client", text=f"send({data!r})")
                sock.send(data)
                conn.packet("client", ["PSH", "ACK"], data, info=f"Client sends {len(data)} bytes")
                run.pause()
                reply = sock.recv(1024)
                conn.packet("server", ["PSH", "ACK"], reply, info=f"Server replies with {len(reply)} bytes")
                run.pause()
                conn.packet("client", ["ACK"], info="Client acknowledges the reply")
                run.pause()
                if message.strip().lower() == "bye":
                    said_bye = True  # the server closes after "bye"
                    break
            close_connection(run, sock, conn, server_closes_first=said_bye)
        finally:
            sock.close()
    finally:
        stop_server(proc, reader)


def scenario_q2(run, path):
    run.emit("scenario", id="q2", title=f"Q2 · Web server · GET {path}", script="q2_web_server.py", serverPort=Q2_PORT)
    proc, reader = start_server(run, "q2_web_server.py", Q2_PORT)
    try:
        run.pause()
        sock, conn = open_connection(run, Q2_PORT)
        try:
            run.emit("phase", title="HTTP request / response")
            request = (
                f"GET {path} HTTP/1.1\r\n"
                f"Host: {HOST}:{Q2_PORT}\r\n"
                "User-Agent: TcpLiveLab/1.0\r\n"
                "Accept: text/html\r\n"
                "\r\n"
            ).encode()
            run.emit("call", side="client", text=f"sendall(b'GET {path} HTTP/1.1 ...')")
            sock.sendall(request)
            conn.packet("client", ["PSH", "ACK"], request, info=f"GET {path} HTTP/1.1")
            run.pause()

            # The server sends its response and then closes, so read until b"" (its FIN)
            response = b""
            while chunk := sock.recv(4096):
                if response:
                    info = f"Continuation, {len(chunk)} bytes"
                else:
                    info = chunk.split(b"\r\n", 1)[0].decode(errors="replace")
                response += chunk
                conn.packet("server", ["PSH", "ACK"], chunk, info=info)
                run.pause()
                conn.packet("client", ["ACK"], info="Browser acknowledges the data")
                run.pause()

            head, _, body = response.partition(b"\r\n\r\n")
            lines = head.decode(errors="replace").split("\r\n")
            run.emit("http", path=path, status=lines[0], headers=lines[1:],
                     body=body.decode("utf-8", errors="replace"))
            close_connection(run, sock, conn, server_closes_first=True, fin_received=True)
        finally:
            sock.close()
        proc.wait(5)
    finally:
        stop_server(proc, reader)


def clean_path(path):
    path = "".join(ch for ch in path if ch.isprintable() and not ch.isspace())[:200]
    return path if path.startswith("/") else "/" + path


def execute(run, query):
    scenario = query.get("scenario", ["q1"])[0]
    messages = [m.strip()[:200] for m in query.get("msg", []) if m.strip()][:10]
    messages = messages or ["Hello server!", "bye"]
    path = clean_path(query.get("path", ["/HelloWorld.html"])[0])
    try:
        if scenario in ("q1", "all"):
            scenario_q1(run, messages)
        if scenario == "q2":
            scenario_q2(run, path)
        if scenario == "all":
            scenario_q2(run, "/HelloWorld.html")
            scenario_q2(run, "/missing.html")
    except Cancelled:
        pass
    except Exception as exc:
        run.emit("error", text=str(exc))
    finally:
        run.emit("done")


# ---------------------------------------------------------------- HTTP UI server
class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # keep the terminal quiet

    def do_GET(self):
        url = urlparse(self.path)
        if url.path in ("/", "/index.html"):
            with open(INDEX_HTML, "rb") as f:
                body = f.read()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif url.path == "/api/run":
            self.stream_run(parse_qs(url.query))
        else:
            self.send_error(404)

    def stream_run(self, query):
        if not run_lock.acquire(blocking=False):
            self.send_error(409, "A simulation is already running")
            return
        try:
            try:
                delay = min(max(float(query.get("delay", ["0.9"])[0]), 0.1), 3.0)
            except ValueError:
                delay = 0.9
            run = Run(delay)
            worker = threading.Thread(target=execute, args=(run, query), daemon=True)

            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            worker.start()
            try:
                while True:
                    event = run.events.get()
                    self.wfile.write(f"data: {json.dumps(event)}\n\n".encode())
                    self.wfile.flush()
                    if event["type"] == "done":
                        break
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                run.cancelled.set()  # browser went away: stop the run early
            worker.join()
        finally:
            run_lock.release()


if __name__ == "__main__":
    server = ThreadingHTTPServer((HOST, UI_PORT), Handler)
    server.daemon_threads = True
    url = f"http://{HOST}:{UI_PORT}"
    print(f"TCP Live Lab running at {url}  (Ctrl+C to stop)")
    if "--no-browser" not in sys.argv:
        webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped")
    finally:
        server.server_close()
