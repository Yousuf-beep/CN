"""
Q1 - Simple server/client system using socket programming (client side).

Connects to q1_server.py, sends lines typed by the user and prints replies.
Type 'bye' to end the session.
"""
from socket import socket, AF_INET, SOCK_STREAM

SERVER_HOST = "127.0.0.1"
SERVER_PORT = 12000

client_socket = socket(AF_INET, SOCK_STREAM)
client_socket.connect((SERVER_HOST, SERVER_PORT))
print(f"Connected to server {SERVER_HOST}:{SERVER_PORT}")

while True:
    message = input("Enter message (type 'bye' to quit): ")
    client_socket.send(message.encode())
    reply = client_socket.recv(1024).decode()
    print(reply)
    if message.strip().lower() == "bye":
        break

client_socket.close()
