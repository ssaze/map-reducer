"""MapReduce framework Worker node."""
import os
import socket
import logging
import json
import time
import hashlib
import subprocess
import tempfile
import pathlib
import shutil
import threading
import heapq
import click
import sys
from mapreduce.utils.servers import *


# Configure logging
LOGGER = logging.getLogger(__name__)


class Worker:
    """Docstring."""

    def __init__(self, host, port, manager_host, manager_port):
        """Init."""
        self.host = host
        self.port = port
        self.manager_host = manager_host
        self.manager_port = manager_port

        self.registered = False
        self.alive = True
        self.shutdown = threading.Event()
        self.alive_threads = []

        # LOGGER.info("Starting worker host=%s
        # port=%s pwd=%s", host, port, os.getcwd())
        # LOGGER.info("manager_host=%s manager_port=%s",
        # manager_host, manager_port)

        # Launch TCP listener in a separate thread
        self._start_tcp_thread()

        # Send registration message to the manager
        self._send_registration()

        # Wait for all threads to finish
        self._wait_for_threads()

    def _start_tcp_thread(self):
        thread = threading.Thread(target=self.startTCP)
        self.alive_threads.append(thread)
        thread.start()

    def _send_registration(self):
        registration_msg = json.dumps({
            "message_type": "register",
            "worker_host": self.host,
            "worker_port": self.port
        })
        tcp_connect(self.manager_host, self.manager_port, registration_msg)

    def _wait_for_threads(self):
        for thread in self.alive_threads:
            thread.join()

    def partition(self, key, num_partitions):
        """Docstring."""
        hexdigest = hashlib.md5(key.encode("utf-8")).hexdigest()
        keyhash = int(hexdigest, base=16)
        partition_number = keyhash % num_partitions
        return partition_number

    def handle_map_task(self, task_info):
        """Docstring."""
        task_id = task_info["task_id"]
        input_files = task_info["input_paths"]
        executable = task_info["executable"]
        output_dir = task_info["output_directory"]
        num_partitions = task_info["num_partitions"]

        temp_prefix = f"mapreduce-local-task{task_id:05d}-"

        with tempfile.TemporaryDirectory(prefix=temp_prefix) as temp_dir:
            file_handles = [None] * num_partitions

            for input_path in input_files:
                with open(input_path) as infile:
                    with subprocess.Popen(
                        [executable],
                        stdin=infile,
                        stdout=subprocess.PIPE,
                        text=True,
                    ) as map_process:
                        for line in map_process.stdout:
                            if "\t" not in line:
                                continue
                            key, _ = line.split("\t", 1)
                            partition_id = self.partition(key, num_partitions)

                            if file_handles[partition_id] is None:
                                filename = f"maptask{task_id:05d}-part{
                                    partition_id:05d}"
                                filepath = os.path.join(temp_dir, filename)
                                file_handles[partition_id] = open(filepath,
                                                                  "w+")

                            file_handles[partition_id].write(line)

            # Sort each partition using subprocess sort
            for handle in file_handles:
                if handle:
                    handle_path = handle.name
                    handle.close()
                    subprocess.run(["sort", "-o",
                                    handle_path, handle_path], check=True)

            shutil.copytree(temp_dir, output_dir, dirs_exist_ok=True)

    def handle_reduce_task(self, dictionary):  # unsafe
        """Docstring."""
        task_id = dictionary['task_id']
        prefix = f"mapreduce-local-task{task_id:05d}-"

        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            # Pipe executable input
            # Pipe input from memory to the reduce executable.
            # Merge the sorted intermediate files using heapq.merge().
            # Pipe the output of heapq.merge()
            # into the subprocess’s stdin.
            # Do not write the merge output to file.

            # Use the Python standard library
            # subprocess.Popen class to run
            # the reduce executable in a new process.
            # The input should be the
            # output of heapq.merge(). The output is a file.
            # We’ve provided sample code below,
            # which you’ll need to augment with a call to heapq.merge().

            executable = dictionary['executable']  # reduce executable
            instreams = [open(path)
                         for path in dictionary['input_paths']]
            # merged input files
            merged_stream = heapq.merge(*instreams)
            outfile_path = os.path.join(tmpdir, f"part-{task_id:05d}")
            outfile = open(outfile_path, "w")  # open output file

            with subprocess.Popen(
                [executable],
                text=True,
                stdin=subprocess.PIPE,
                stdout=outfile,
            ) as reduce_process:
                # Pipe input to reduce_process
                for line in merged_stream:
                    reduce_process.stdin.write(line)

            # Close input streams
            for f in instreams:
                f.close()

            # Move output file to final output
            # directory specified by the Manager
            shutil.copytree(tmpdir, dictionary['output_directory'],
                            dirs_exist_ok=True)

    def notify_finished(self, task_id):
        """Docstring."""
        message = json.dumps({
            "message_type": "finished",
            "task_id": task_id,
            "worker_host": self.host,
            "worker_port": self.port
        })
        tcp_connect(self.manager_host, self.manager_port, message)

    def startTCP(self):
        """Docstring."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
            server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_sock.bind((self.host, self.port))
            server_sock.listen()
            server_sock.settimeout(1)

            while self.alive:
                try:
                    clientsocket, address = server_sock.accept()
                except socket.timeout:
                    continue
                print("Connection from", address[0])

                # Socket recv() will block for a
                # maximum of 1 second.
                # If you omit
                # this, it blocks indefinitely, waiting for packets.
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
                    dictionary = json.loads(message_string)
                except json.JSONDecodeError:
                    continue

                # ⬇️ Dispatch message to appropriate handler
                self._handle_message(dictionary)

    def start_heartbeats(self):  # unsafe
        """Docstring."""
        destination = (self.manager_host, self.manager_port)
        heartbeat = json.dumps({
            "message_type": "heartbeat",
            "worker_host": self.host,
            "worker_port": self.port
        })
        while self.alive and self.registered:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                try:
                    sock.connect((destination))
                    sock.sendall(heartbeat.encode('utf-8'))
                except Exception as err:
                    LOGGER.warning(f"Failed to send heartbeat: {err}")
            time.sleep(2)

    def _handle_message(self, msg):
        msg_type = msg.get("message_type")
        handler_map = {
            "register_ack": self._handle_register_ack,
            "shutdown": self._handle_shutdown,
            "new_map_task": lambda:
            self._handle_task(self.handle_map_task, msg),
            "new_reduce_task": lambda:
            self._handle_task(self.handle_reduce_task, msg),
        }

        handler = handler_map.get(msg_type)
        if handler:
            handler()
        else:
            LOGGER.warning(f"Unknown message type: {msg_type}")

        time.sleep(0.1)

    def _handle_register_ack(self):
        LOGGER.info("Registration successful")
        self.registered = True
        heartbeat_thread = threading.Thread(target=self.start_heartbeats)
        heartbeat_thread.start()
        self.alive_threads.append(heartbeat_thread)

    def _handle_shutdown(self):
        self.alive = False
        self.shutdown.set()
        # sys.exit(0)
        # sends warnings for some reason

    def _handle_task(self, task_func, msg):
        LOGGER.info(f"Handling {msg['message_type']}")
        task_func(msg)
        self.notify_finished(msg["task_id"])
        LOGGER.info(f"Finished {msg['message_type']}")


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

