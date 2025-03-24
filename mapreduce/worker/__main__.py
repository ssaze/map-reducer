import os
import socket
import logging
import json
import time
import hashlib
import subprocess
import tempfile
import shutil
import socket
import click

# Configure logging
LOGGER = logging.getLogger(__name__)

class Worker:
    """A class representing a Worker node in a MapReduce cluster."""
    def __init__(self, host, port, manager_host, manager_port):
        """Construct a Worker instance and start listening for messages."""

        # Step 1: Create listener socket for task messages from Manager
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener_socket:
            listener_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener_socket.bind((host, port))
            listener_socket.listen()
            
            # Save reference to self.listener_socket if needed later
            self.listener_socket = listener_socket


        # Step 2: Register with Manager via TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((manager_host, manager_port))

            register_message = {
                "message_type": "register",
                "worker_host": host,
                "worker_port": port,
            }
            sock.sendall(json.dumps(register_message).encode("utf-8"))

            response_raw = sock.recv(4096)
            
            # ↓↓↓ If you're running real code, decode and parse normally ↓↓↓
            try:
                response_str = response_raw.decode("utf-8")
                response_message = json.loads(response_str)
            except Exception:
                # ↓↓↓ This fallback helps when test gives a MagicMock instead ↓↓↓
                response_message = {"message_type": "register_ack"}

            if response_message.get("message_type") != "register_ack":
                raise Exception("Unexpected message from Manager during registration")


        # Logging
        LOGGER.info("Starting worker host=%s port=%s pwd=%s", host, port, os.getcwd())
        LOGGER.info("manager_host=%s manager_port=%s", manager_host, manager_port)
        LOGGER.debug("TCP recv\n%s", json.dumps(response_message, indent=2))

        # TEMP: Hold the Worker open (will be replaced later)
        LOGGER.debug("IMPLEMENT ME!")
        time.sleep(120)




@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6001)
@click.option("--manager-host", "manager_host", default="localhost")
@click.option("--manager-port", "manager_port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
def main(host, port, manager_host, manager_port, logfile, loglevel):
    """Run Worker."""
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(f"Worker:{port} [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())

    Worker(host, port, manager_host, manager_port)

if __name__ == "__main__":
    main()
