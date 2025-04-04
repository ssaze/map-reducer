"""MapReduce framework Worker node."""
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
from contextlib import ExitStack
import heapq
import click
from mapreduce.utils.servers import tcp_connect, receive_json_message

# Configure logging
LOGGER = logging.getLogger(__name__)


class Worker:
    """MapReduce worker node implementation."""

    def __init__(self, host, port, manager_host, manager_port):
        """Initialize."""
        self.host = host
        self.port = port
        self.manager_host = manager_host
        self.manager_port = manager_port
        # Consolidate boolean flags to reduce instance attributes

        self.shutdown = threading.Event()
        self.alive_threads = []

        # Launch TCP listener in a separate thread
        self._start_tcp_thread()

        # Send registration message to the manager
        self._send_registration()

        # Wait for all threads to finish
        self._wait_for_threads()

    def _start_tcp_thread(self):
        thread = threading.Thread(target=self._start_tcp)
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
        """Determine the partition for a key using MD5 hashing."""
        hexdigest = hashlib.md5(key.encode("utf-8")).hexdigest()
        keyhash = int(hexdigest, base=16)
        partition_number = keyhash % num_partitions
        return partition_number

    def handle_map_task(self, task_info):
        """Process a map task from the manager."""
        task_id = task_info["task_id"]
        input_files = task_info["input_paths"]
        executable = task_info["executable"]
        output_dir = task_info["output_directory"]
        num_partitions = task_info["num_partitions"]

        temp_prefix = f"mapreduce-local-task{task_id:05d}-"

        with tempfile.TemporaryDirectory(prefix=temp_prefix) as temp_dir:
            file_handles = self._create_partition_files(
                task_id, input_files, executable, num_partitions, temp_dir
            )

            self._sort_partitions(file_handles)
            shutil.copytree(temp_dir, output_dir, dirs_exist_ok=True)

    def _create_partition_files(
        self, task_id, input_files, executable, num_partitions, temp_dir
    ):
        """Create partitioned intermediate files for map output."""
        file_handles = [None] * num_partitions

        with ExitStack() as stack:
            for input_path in input_files:
                with open(input_path, encoding="utf-8") as infile:
                    with subprocess.Popen(
                        [executable],
                        stdin=infile,
                        stdout=subprocess.PIPE,
                        text=True,
                    ) as map_process:
                        for line in map_process.stdout:
                            if "\t" not in line:
                                continue
                            key = line.split("\t", 1)[0]
                            pid = self.partition(key, num_partitions)

                            if file_handles[pid] is None:
                                filepath = os.path.join(
                                    temp_dir,
                                    f"maptask{task_id:05d}-part{pid:05d}"
                                )
                                file_handles[pid] = stack.enter_context(
                                    open(filepath, "w+", encoding="utf-8")
                                )

                            file_handles[pid].write(line)

        return file_handles

    def _sort_partitions(self, file_handles):
        """Sort each intermediate partition file using Unix sort."""
        for handle in file_handles:
            if handle:
                handle_path = handle.name
                handle.close()
                with subprocess.Popen(
                    ["sort", "-o", handle_path, handle_path],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                ) as proc:
                    proc.communicate()
                    if proc.returncode != 0:
                        raise subprocess.CalledProcessError(
                            proc.returncode, proc.args
                        )

    def handle_reduce_task(self, dictionary):
        """Process a reduce task from the manager."""
        task_id = dictionary['task_id']
        prefix = f"mapreduce-local-task{task_id:05d}-"

        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            executable = dictionary['executable']
            outfile_path = os.path.join(tmpdir, f"part-{task_id:05d}")

            # Open all input files safely using ExitStack
            with ExitStack() as stack:
                instreams = [
                    stack.enter_context(open(path, encoding="utf-8"))
                    for path in dictionary['input_paths']
                ]

                merged_stream = heapq.merge(*instreams)

                with open(outfile_path, "w", encoding="utf-8") as outfile:
                    with subprocess.Popen(
                        [executable],
                        text=True,
                        stdin=subprocess.PIPE,
                        stdout=outfile,
                    ) as reduce_process:
                        for line in merged_stream:
                            reduce_process.stdin.write(line)

            # Copy to output directory after closing files
            shutil.copytree(
                tmpdir, dictionary['output_directory'], dirs_exist_ok=True
                )

    def notify_finished(self, task_id):
        """Send a completion message to the manager for a finished task."""
        message = json.dumps({
            "message_type": "finished",
            "task_id": task_id,
            "worker_host": self.host,
            "worker_port": self.port
        })
        tcp_connect(self.manager_host, self.manager_port, message)

    def _start_tcp(self):
        """Start TCP server to receive messages from the manager."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
            server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_sock.bind((self.host, self.port))
            server_sock.listen()
            server_sock.settimeout(1)

            while not self.shutdown.is_set():
                message_dict, _ = receive_json_message(server_sock)
                if message_dict:
                    self._handle_message(message_dict)

    def start_heartbeats(self):
        """Send periodic heartbeats to the manager."""
        destination = (self.manager_host, self.manager_port)
        heartbeat = json.dumps({
            "message_type": "heartbeat",
            "worker_host": self.host,
            "worker_port": self.port
        })
        while not self.shutdown.is_set():
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                sock.connect((destination))
                sock.sendall(heartbeat.encode('utf-8'))
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
            LOGGER.warning("Unknown message type: %s", msg_type)

        time.sleep(0.1)

    def _handle_register_ack(self):
        LOGGER.info("Registration successful")
        heartbeat_thread = threading.Thread(target=self.start_heartbeats)
        heartbeat_thread.start()
        self.alive_threads.append(heartbeat_thread)

    def _handle_shutdown(self):
        self.shutdown.set()

    def _handle_task(self, task_func, msg):
        LOGGER.info("Handling %s", msg['message_type'])
        task_func(msg)
        self.notify_finished(msg["task_id"])
        LOGGER.info("Finished %s", msg['message_type'])


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
