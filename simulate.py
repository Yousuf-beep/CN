"""
Automated simulation of Q1 and Q2.

Starts the real servers (q1_server.py, q2_web_server.py) as background
processes and plays the client side automatically, so no typing or
browser is needed. Server output is shown with a [SERVER] tag and the
simulated client's actions with a [CLIENT] tag.

Run:  python simulate.py
"""
import os
import subprocess
import sys
import threading
import time
from socket import socket, AF_INET, SOCK_STREAM

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
HOST = "127.0.0.1"
Q1_PORT = 12000
Q2_PORT = 6789
STEP_DELAY = 1.0  # pause between simulated actions (seconds)

os.system("")  # enable ANSI colours in Windows consoles
CYAN, GREEN, YELLOW, MAGENTA, RESET = "\033[96m", "\033[92m", "\033[93m", "\033[95m", "\033[0m"
print_lock = threading.Lock()


def log(tag, colour, text):
    with print_lock:
        for line in text.splitlines() or [""]:
            print(f"{colour}[{tag}]{RESET} {line}", flush=True)


def banner(title):
    with print_lock:
        print(f"\n{MAGENTA}{'=' * 60}\n  {title}\n{'=' * 60}{RESET}", flush=True)


def start_server(script):
    """Launch a server script, stream its output, and wait until it is listening."""
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
            log("SERVER", GREEN, line.rstrip("\n"))
            if "ready" in line:
                ready.set()
        ready.set()  # process ended

    reader = threading.Thread(target=pump_output, daemon=True)
    reader.start()
    if not ready.wait(5) or proc.poll() is not None:
        proc.kill()
        sys.exit(f"{script} failed to start")
    return proc, reader


# ---------------------------------------------------------------- Q1
def simulate_q1():
    banner("Q1 - Socket server/client simulation")
    proc, reader = start_server("q1_server.py")

    conversations = {
        "Client A": ["Hello server!", "How are you?", "Socket programming is fun", "bye"],
        "Client B": ["Hi, I am the second client", "bye"],
    }

    try:
        for name, messages in conversations.items():
            time.sleep(STEP_DELAY)
            client = socket(AF_INET, SOCK_STREAM)
            client.connect((HOST, Q1_PORT))
            log(name, CYAN, f"Connected to server {HOST}:{Q1_PORT}")

            for message in messages:
                time.sleep(STEP_DELAY)
                log(name, CYAN, f"--> {message}")
                client.send(message.encode())
                reply = client.recv(1024).decode()
                time.sleep(0.2)  # let the server's log line print first
                log(name, CYAN, f"<-- {reply}")

            client.close()
            log(name, CYAN, "Disconnected")
        time.sleep(STEP_DELAY)
    finally:
        log("SIM", YELLOW, "Stopping Q1 server")
        proc.terminate()
        proc.wait()
        reader.join(1)


# ---------------------------------------------------------------- Q2
def simulate_q2():
    banner("Q2 - One-request web server simulation")

    for path in ["/HelloWorld.html", "/missing.html"]:
        log("SIM", YELLOW, f"Starting web server for a request to {path}")
        proc, reader = start_server("q2_web_server.py")
        time.sleep(STEP_DELAY)

        request = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {HOST}:{Q2_PORT}\r\n"
            "User-Agent: SimulatedBrowser/1.0\r\n"
            "Accept: text/html\r\n"
            "\r\n"
        )
        try:
            client = socket(AF_INET, SOCK_STREAM)
            client.connect((HOST, Q2_PORT))
            log("BROWSER", CYAN, f"--> GET {path}")
            client.sendall(request.encode())

            # Read until the server closes the connection
            response = b""
            while chunk := client.recv(4096):
                response += chunk
            client.close()
            proc.wait(5)
        finally:
            if proc.poll() is None:
                proc.kill()
            reader.join(1)
        time.sleep(STEP_DELAY / 2)
        status_line = response.split(b"\r\n", 1)[0].decode()
        log("BROWSER", CYAN, f"<-- {status_line}")
        log("BROWSER", CYAN, "Full response received:")
        log("BROWSER", CYAN, response.decode(errors="replace").replace("\r\n", "\n"))
        time.sleep(STEP_DELAY)


if __name__ == "__main__":
    simulate_q1()
    simulate_q2()
    banner("Simulation complete")
