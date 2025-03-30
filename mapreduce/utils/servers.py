"""Example TCP socket server."""
import socket
import json

import logging
LOGGER = logging.getLogger(__name__)


def tcp_connect(host, port, mesg):
    """Send a TCP message."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.connect((host, port))
            sock.sendall(mesg.encode('utf-8'))
            return True
        except ConnectionRefusedError:
            return False


def receive_json_message(sock):
    """Accept a TCP connection and return the parsed JSON message."""
    try:
        clientsocket, address = sock.accept()
    except socket.timeout:
        return None, None

    print("Connection from", address[0])
    clientsocket.settimeout(1)

    with clientsocket:
        message_chunks = []
        while True:
            try:
                data = clientsocket.recv(4096)
            except socket.timeout:
                continue
            if not data:
                break
            message_chunks.append(data)

    try:
        message_bytes = b''.join(message_chunks)
        message_string = message_bytes.decode("utf-8")
        message_dict = json.loads(message_string)
        return message_dict, address
    except json.JSONDecodeError:
        return None, None


def tcp_client(host, port, message_json):
    """Test TCP Socket Client."""
    # create an INET, STREAMing socket, this is TCP
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:

            # connect to the server
            sock.connect((host, port))

            # send a message
            message = json.dumps(message_json)
            sock.sendall(message.encode('utf-8'))
        return True
    except ConnectionRefusedError:
        # LOGGER.warning(f"Worker {host}:{port} is \
        # unreachable. Marking as dead.")
        return False
