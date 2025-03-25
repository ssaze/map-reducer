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
        LOGGER.info("Initializing Worker...")
        # Step 1: Create listener socket for task messages from Manager
        self.listener_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener_socket.bind((host, port))
        self.listener_socket.listen()
        LOGGER.info(f"Listener socket created and bound to {host}:{port}")


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
            response_message = json.loads(response_raw.decode('utf-8'))

            if response_message.get("message_type") == "register_ack":
                print("Registration successful!")
                self.start_heartbeats() # TODO
            else:
                print("Failed to register with Manager!")

        LOGGER.info("Starting worker host=%s port=%s pwd=%s", host, port, os.getcwd())
        LOGGER.info("manager_host=%s manager_port=%s", manager_host, manager_port)
        LOGGER.debug("TCP recv\n%s", json.dumps(response_message, indent=2))

        # Now start listening for tasks immediately
        self.listen_for_tasks()

    def send_message(self, conn, message_dict):
        """Send a message to the Manager through the provided connection."""
        message = json.dumps(message_dict)
        LOGGER.info(f"Sending message: {message}")
        conn.sendall(message.encode('utf-8'))

    def handle_task(self, conn, task_message):
        """Handle incoming task and execute corresponding function."""
        message_type = task_message.get("message_type")

        if message_type == "new_map_task":
            task_id = task_message["task_id"]
            for input_file in task_message["input_paths"]:
                self.run_map_task(conn, task_id, input_file, num_partitions, output_directory, map_executable)
            num_partitions = task_message["num_partitions"]
            output_directory = task_message["output_directory"]
            self.run_map_task(task_id, conn, input_file, num_partitions, output_directory)
        elif message_type == "new_reduce_task":
            # Implement the logic for handling reduce tasks here.
            pass
        else:
            LOGGER.warning(f"Unknown task type: {message_type}")

    def listen_for_tasks(self):
        """Listen for incoming task messages from the Manager."""
        while True:
            conn, addr = self.listener_socket.accept()
            with conn:
                LOGGER.info(f"Connected to Manager at {addr}")

                # Receive the task message
                message_raw = conn.recv(4096)
                if not message_raw:
                    LOGGER.warning("No message received, continuing to listen.")
                    continue  # Keep the loop alive even if no message was received

                try:
                    task_message = json.loads(message_raw.decode('utf-8'))
                    LOGGER.info(f"Received task message: {task_message}")

                    task_message = json.loads(message_raw.decode("utf-8"))
                    LOGGER.debug("DELETEOOOO received\n%s", json.dumps(task_message, indent=2))

                    self.handle_task(conn, task_message)
                except json.JSONDecodeError:
                    LOGGER.error("Failed to decode message.")
                    continue

    def run_map_task(self, conn, task_id, input_file, num_partitions, output_directory):
        """Execute the map task on the Worker."""
        with tempfile.TemporaryDirectory(prefix=f"mapreduce-local-task{task_id:05d}-") as tmpdir:
            # Run the map executable
            map_executable = 'map'  # Assuming 'map' is the map executable
            self.partition_map_output(task_id, input_file, map_executable, num_partitions, tmpdir)
            self.sort_and_move_files(tmpdir, output_directory)

            # Notify Manager that the task is finished
            LOGGER.info(f"Sending finish notification for task {task_id} to Manager")
            self.notify_manager(conn, task_id)

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

    def notify_manager(self, conn, task_id):
        """Notify the Manager that the task is finished."""
        message_dict = {
            "message_type": "finished",
            "task_id": task_id,
            "worker": [self.host, self.port],
        }
        self.send_message(conn, message_dict)




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
    print("BEFORE MAIN")
    main()
