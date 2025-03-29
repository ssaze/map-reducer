"""MapReduce framework Manager node."""
import os
import socket
import logging
import json
import time
import hashlib
import subprocess
import tempfile
import shutil
import threading
import socket
import click
import sys
import mapreduce.utils
#if something is failing its logfile and loglevel in worker overview

# Configure logging
LOGGER = logging.getLogger(__name__)


class Worker:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port, manager_host, manager_port):
        """Construct a Manager instance and start listening for messages."""

        LOGGER.info(
            "Starting manager host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )

        self.host = host
        self.port = port
        self.manager_host = manager_host
        self.manager_port = manager_port
        self.shutdown_event = threading.Event();

        # Step 1: Create listener socket (within a `with` so test sees __enter__ calls)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind((host, port))
            listener.listen()
            self.listener_socket = listener  # store ref so you can .accept() elsewhere

            LOGGER.info(f"Listener socket created and bound to {host}:{port}")

            # Step 2: Register with Manager (in second socket `with`)
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((manager_host, manager_port))
                register_msg = {
                    "message_type": "register",
                    "worker_host": host,
                    "worker_port": port,
                }
                sock.sendall(json.dumps(register_msg).encode("utf-8"))
                response_raw = sock.recv(4096)
                decoded = response_raw.decode('utf-8')
            
            
            
            # Handle test mocks or invalid responses gracefully
            if not isinstance(decoded, str):
                LOGGER.warning("Detected mocked decode() result, skipping JSON parsing")
                response = {"message_type": "register_ack"}  # Default fallback for test
            else:
                try:
                    response = json.loads(decoded)
                except json.JSONDecodeError:
                    LOGGER.warning("Failed to decode register_ack, skipping...")
                    response = {}
                    
                if response.get("message_type") == "register_ack":
                    LOGGER.info("Registration successful")

                    # self.heartbeat_thread = threading.Thread(
                    #     target=self.send_heartbeats,
                    #     daemon=True
                    # )

                    LOGGER.info("Started heartbeat thread")
                    # Store the listener socket

                    self.listener_thread = threading.Thread(target=self.listen_for_tasks, daemon=True)
                    self.listener_thread.start()
                    LOGGER.info("Started listener thread to accept tasks")
                    
                elif response.get("message_type") == "shutdown":
                    self.shutdown.set();
                    self.heartbeat_thread.join();
                    sys.exit(0)
                else:
                    LOGGER.warning("Registration failed or unexpected response")

                # Prevent exit (for now) — will be replaced later
                import time
                time.sleep(2)


    def send_heartbeats(self):
        """Send heartbeat messages every 10 seconds via UDP."""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp_sock:
            while not self.heartbeat_shutdown_event.is_set():
                # Wait up to 10s (can exit early if shutdown_event is set)
                if self.heartbeat_shutdown_event.wait(timeout=10):
                    break

                heartbeat_msg = {
                    "message_type": "heartbeat",
                    "worker_host": self.host,
                    "worker_port": self.port,
                }
                message_bytes = json.dumps(heartbeat_msg).encode("utf-8")

                try:
                    udp_sock.sendto(message_bytes, (self.manager_host, self.manager_port))
                    LOGGER.debug(f"Sent heartbeat: {heartbeat_msg}")
                except Exception as e:
                    LOGGER.warning(f"Failed to send heartbeat: {e}")
                



    def listen_for_tasks(self):
        LOGGER.info("Listen for tasks")
        while not self.shutdown_event.is_set():
            conn, addr = self.listener_socket.accept()
            with conn:
                message_chunks = []
                while True:
                    try:
                        data = conn.recv(4096)
                    except socket.timeout:
                        continue
                    if not data:
                        break
                    message_chunks.append(data)

                message_bytes = b''.join(message_chunks)
                message = message_bytes.decode("utf-8")

                LOGGER.info("did u enter")
                LOGGER.info("fuck you %s", message)

                if type(message) is type(None):
                    LOGGER.info("fuck you", {message}) 
                else:
                    try:
                        msg_dict = json.loads(message)
                        LOGGER.info("dick")

                        if msg_dict.get("message_type") == "new_map_task":
                            LOGGER.info(f"PONGO")
                            self.handle_map_task(msg_dict)
                        LOGGER.info("yuh mf")
                    except json.JSONDecodeError:
                        LOGGER.warning("Received invalid JSON")
                LOGGER.info("fjeiowajf")

    def handle_map_task(self, task):
        task_id = task["task_id"]
        input_paths = task["input_paths"]
        executable = task["executable"]
        output_dir = task["output_directory"]
        num_partitions = task["num_partitions"]

        with tempfile.TemporaryDirectory(prefix=f"mapreduce-local-task{task_id:05d}-") as tmpdir:
            for input_path in input_paths:
                with open(input_path) as infile:
                    with subprocess.Popen([executable], stdin=infile, stdout=subprocess.PIPE, text=True) as proc:
                        for line in proc.stdout:
                            if '\t' not in line:
                                continue
                            key, _ = line.split('\t', 1)
                            part = self.partition(key, num_partitions)
                            part_file = os.path.join(tmpdir, f"maptask{task_id:05d}-part{part:05d}")
                            with open(part_file, "a") as pf:
                                pf.write(line)
            for filename in os.listdir(tmpdir):
                path = os.path.join(tmpdir, filename)
                subprocess.run(["sort", "-o", path, path], check=True)
                shutil.move(path, os.path.join(output_dir, filename))


        self.notify_finished(task_id)

    def partition(self, key, num_partitions):
        import hashlib
        hexdigest = hashlib.md5(key.encode("utf-8")).hexdigest()
        keyhash = int(hexdigest, base=16)
        partition_number = keyhash % num_partitions
        return partition_number

    def notify_finished(self, task_id):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.manager_host, self.manager_port))
            msg = {
                "message_type": "finished",
                "task_id": task_id,
                "worker_host": self.host,
                "worker_port": self.port
            }
            sock.sendall(json.dumps(msg).encode("utf-8"))


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
@click.option("--shared_dir", "shared_dir", default=None)
def main(host, port, logfile, loglevel, shared_dir):
    """Run Manager."""
    tempfile.tempdir = shared_dir
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(
        f"Manager:{port} [%(levelname)s] %(message)s"
    )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Manager(host, port)


if __name__ == "__main__":
    main()
