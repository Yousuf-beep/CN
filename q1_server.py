"""
Q1 - Simple server/client system using socket programming (server side).

The server listens on a TCP port, accepts clients one after another,
and replies to every message with an upper-cased echo.
Type 'bye' from the client to close that connection.
"""
from socket import socket, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR

HOST = "127.0.0.1"
PORT = 12000

server_socket = socket(AF_INET, SOCK_STREAM)
server_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
server_socket.bind((HOST, PORT))
server_socket.listen(1)
print(f"Server is ready and listening on {HOST}:{PORT}")

try:
    while True:
        connection_socket, addr = server_socket.accept()
        print(f"Connected to client {addr}")

        while True:
            message = connection_socket.recv(1024).decode()
            if not message:
                break
            print(f"Client says: {message}")
            if message.strip().lower() == "bye":
                connection_socket.send("Goodbye!".encode())
                break
            connection_socket.send(f"Server reply: {message.upper()}".encode())

        connection_socket.close()
        print(f"Connection with {addr} closed")
except KeyboardInterrupt:
    print("\nServer shutting down")
finally:
    server_socket.close()
