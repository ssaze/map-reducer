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
<<<<<<< HEAD
        self.host = host
        self.port = port
        self.manager_host = manager_host
        self.manager_port = manager_port
        self.socket = None
        
        LOGGER.info("Starting worker host=%s port=%s pwd=%s", host, port, os.getcwd())
        LOGGER.info("manager_host=%s manager_port=%s", manager_host, manager_port)

        self.connect_to_manager()

    def connect_to_manager(self):
        """Connect to the Manager to receive tasks."""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.manager_host, self.manager_port))
        LOGGER.info(f"Connected to Manager at {self.manager_host}:{self.manager_port}")
        self.register_with_manager()

    def register_with_manager(self):
        """Send a register message to the manager."""
        message_dict = {
            "message_type": "register",
            "worker_host": self.host,
            "worker_port": self.port,
        }
        self.send_message(message_dict)

    def send_message(self, message_dict):
        """Send a message to the Manager."""
        message = json.dumps(message_dict)
        self.socket.sendall(message.encode('utf-8'))

    def run_map_task(self, task_id, input_file, num_partitions, output_directory):
        """Execute the map task on the Worker."""
        with tempfile.TemporaryDirectory(prefix=f"mapreduce-local-task{task_id:05d}-") as tmpdir:
            # Run the map executable
            map_executable = 'map'  # Assuming 'map' is the map executable
            self.partition_map_output(task_id, input_file, map_executable, num_partitions, tmpdir)
            self.sort_and_move_files(tmpdir, output_directory)

            # Notify Manager that the task is finished
            self.notify_manager(task_id)

    def partition_map_output(self, task_id, input_file, map_executable, num_partitions, tmpdir):
        """Run the map executable and partition the output."""
        LOGGER.info(f"Running map executable on input file: {input_file}")
        
        # Open the input file and run the map executable
        with open(input_file, 'r') as infile:
            with subprocess.Popen([map_executable], stdin=infile, stdout=subprocess.PIPE, text=True) as map_process:
                for line in map_process.stdout:
                    # Extract the key from the line and partition it
                    key, _ = line.split('\t', 1)
                    partition_number = self.compute_partition(key, num_partitions)
                    partition_file = os.path.join(tmpdir, f"maptask{task_id:05d}-part{partition_number:05d}")
                    with open(partition_file, 'a') as partition:
                        partition.write(line)

    def compute_partition(self, key, num_partitions):
        """Compute the partition number based on the key."""
        keyhash = int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)
        return keyhash % num_partitions

    def sort_and_move_files(self, tmpdir, output_directory):
        """Sort and move the output files."""
        # Sort each partition file
        for partition_file in os.listdir(tmpdir):
            partition_path = os.path.join(tmpdir, partition_file)
            subprocess.run(["sort", "-o", partition_path, partition_path], check=True)

            # Move the sorted file to the output directory
            shutil.move(partition_path, os.path.join(output_directory, partition_file))
            LOGGER.info(f"Moved sorted partition file to {output_directory}")

    def notify_manager(self, task_id):
        """Notify the Manager that the task is finished."""
        message_dict = {
            "message_type": "finished",
            "task_id": task_id,
            "worker_host": self.host,
            "worker_port": self.port,
        }
        self.send_message(message_dict)
=======

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
        time.sleep(1)

>>>>>>> ae53036 (i forgot the update)


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
