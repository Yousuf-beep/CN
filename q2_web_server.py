"""
Q2 - Simple Web server that processes only ONE request.

a) creates a connection socket when contacted by a client (browser)
b) receives the HTTP request from this connection
c) parses the request to determine the specific file being requested
d) gets the requested file from the server's file system
e) creates an HTTP response message: header lines followed by the file
f) sends the response over the TCP connection to the browser
If the file is not present, a "404 Not Found" response is returned.

Run:   python q2_web_server.py
Open:  http://127.0.0.1:6789/HelloWorld.html   (found)
       http://127.0.0.1:6789/missing.html      (404)
"""
import os
from socket import socket, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR

HOST = "127.0.0.1"
PORT = 6789
# Serve files from the folder this script lives in
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

CONTENT_TYPES = {
    ".html": "text/html",
    ".htm": "text/html",
    ".txt": "text/plain",
    ".css": "text/css",
    ".js": "application/javascript",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".gif": "image/gif",
}

# Prepare the server socket
server_socket = socket(AF_INET, SOCK_STREAM)
server_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
server_socket.bind((HOST, PORT))
server_socket.listen(1)
print(f"Web server ready at http://{HOST}:{PORT}/ (handles one request)")

# a) Create a connection socket when contacted by a client
connection_socket, addr = server_socket.accept()
print(f"Connection from {addr}")

try:
    # b) Receive the HTTP request
    request = connection_socket.recv(4096).decode(errors="replace")
    print("----- Request -----")
    print(request.replace("\r\n", "\n").strip())

    # c) Parse the request line, e.g. "GET /HelloWorld.html HTTP/1.1"
    request_line = request.split("\r\n")[0]
    parts = request_line.split()
    path = parts[1] if len(parts) >= 2 else "/"
    filename = path.split("?")[0].lstrip("/") or "HelloWorld.html"

    # Only allow files inside BASE_DIR
    filepath = os.path.abspath(os.path.join(BASE_DIR, filename))
    if not filepath.startswith(BASE_DIR + os.sep):
        raise FileNotFoundError(filename)

    # d) Get the requested file from the file system
    with open(filepath, "rb") as f:
        body = f.read()

    # e) Build the response: status line + header lines + blank line + file
    content_type = CONTENT_TYPES.get(os.path.splitext(filename)[1].lower(),
                                     "application/octet-stream")
    header = (
        "HTTP/1.1 200 OK\r\n"
        f"Content-Type: {content_type}\r\n"
        f"Content-Length: {len(body)}\r\n"
        "Connection: close\r\n"
        "\r\n"
    )
    # f) Send the response to the browser
    connection_socket.sendall(header.encode() + body)
    print(f"Response: 200 OK ({filename})")

except (FileNotFoundError, IsADirectoryError, PermissionError):
    # File not present -> 404 Not Found
    body = b"<html><body><h1>404 Not Found</h1></body></html>"
    header = (
        "HTTP/1.1 404 Not Found\r\n"
        "Content-Type: text/html\r\n"
        f"Content-Length: {len(body)}\r\n"
        "Connection: close\r\n"
        "\r\n"
    )
    connection_socket.sendall(header.encode() + body)
    print("Response: 404 Not Found")

finally:
    connection_socket.close()
    server_socket.close()
    print("Server closed after one request")
