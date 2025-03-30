"""Manager."""
import sys
import socket
import tempfile
import logging
import threading
import time
import shutil
import os
from pathlib import Path
from collections import deque

import click

from mapreduce.utils.ordered_dict import ThreadSafeOrderedDict
from mapreduce.manager.myheart import manager_udp_server, check_heartbeats
from mapreduce.utils.servers import tcp_client, receive_json_message
from mapreduce.manager.job import Job
from mapreduce.manager.myheart import handle_dead_worker

# Configure logging
LOGGER = logging.getLogger(__name__)


class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port):
        """Initialize Manager."""
        self.config = {
            'host': host,
            'port': port,
            'job': None,
            'threads': []
        }

        self.worker_data = {
            "workers": ThreadSafeOrderedDict(),
            "busy_workers": ThreadSafeOrderedDict(),
            "worker_heartbeats": ThreadSafeOrderedDict(),
            "dead_workers": set(),
        }

        self.threading_data = {
            'shutdown_event': threading.Event(),
            'new_job_alert_condition': threading.Condition()
        }

        self.job_execs = {
            'mapper_executable': None,
            'reducer_executable': None
        }

        self.job_queue = deque()
        self.next_job_id = 0

        with tempfile.TemporaryDirectory(prefix="mapreduce-shared-") as tmpdir:
            self.tmpdir = tmpdir
            # LOGGER.info("Created tmpdir %s", self.tmpdir)

            heartbeat_server_thread = threading.Thread(
                target=manager_udp_server,
                args=(self, host, port),
                daemon=True,
            )

            manager_tcp_server_thread = threading.Thread(
                target=manager_tcp_server,
                args=(self, host, port),
                daemon=True,
            )

            dead_worker_checking_thread = threading.Thread(
                target=check_heartbeats,
                args=(self,),
                daemon=True,
            )

            job_queue_thread = threading.Thread(
                target=job_queue,
                args=(self,),
                daemon=True,
            )

            self.config["threads"].extend([
                heartbeat_server_thread,
                manager_tcp_server_thread,
                dead_worker_checking_thread,
                job_queue_thread
            ])

            for thread in self.config["threads"]:
                thread.start()

            try:
                self.threading_data["shutdown_event"].wait()
            except KeyboardInterrupt:
                LOGGER.info("Shutdown signal received.")
            finally:
                LOGGER.info("Manager shutting down")
                self.cleanup()

    def cleanup(self):
        """Cleanup resources on shutdown."""
        for thread in self.config["threads"]:
            thread.join(timeout=2)
            if thread.is_alive():
                LOGGER.warning("Thread %s did not exit cleanly.", thread.name)

        LOGGER.info("All threads have completed.")
        sys.exit(0)


def get_available_worker(self):
    """Get the next available worker."""
    # LOGGER.info("Checking for available worker...")
    for worker in self.worker_data["workers"].keys():
        if (
            worker not in self.worker_data["busy_workers"]
            and worker not in self.worker_data["dead_workers"]
        ):
            # LOGGER.info(f"Worker {worker} is available.")
            return worker
    LOGGER.info("No available workers.")
    return None


def forward_shutdown_to_workers(self):
    """Send shutdown message to all registered workers."""
    LOGGER.debug("Forwarding shutdown to workers.")
    message = {"message_type": "shutdown"}
    for worker_host, worker_port in self.worker_data["workers"].keys():
        worker = (worker_host, worker_port)
        if worker in self.worker_data["dead_workers"]:
            continue
        tcp_client(worker_host, worker_port, message)


def manager_tcp_server(self, host, port):
    """Test TCP Socket Server."""
    # Create an INET, STREAMing socket, this is TCP
    # Note: context manager syntax allows for sockets to automatically be
    # closed when an exception is raised or control flow returns.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        # Bind the socket to the server
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        sock.listen()
        sock.settimeout(1)  # tutorial

        while not self.threading_data["shutdown_event"].is_set():
            message_dict, _ = receive_json_message(sock)
            if message_dict:
                message_handler(self, message_dict)
                print(message_dict)


def message_handler(self, message_dict):
    """Handle diff msg types for manager."""
    message_type = message_dict["message_type"]

    if message_type == "shutdown":
        LOGGER.info("Received shutdown request")
        forward_shutdown_to_workers(self)
        self.threading_data["shutdown_event"].set()

    elif message_type == "register":
        worker_host = message_dict["worker_host"]
        worker_port = message_dict["worker_port"]
        worker_key = (worker_host, worker_port)
        self.worker_data["workers"][worker_key] = None

        LOGGER.info("Registering Worker %s", worker_key)

        ack_message = {"message_type": "register_ack"}
        if tcp_client(worker_host, worker_port, ack_message):
            # i think it should give task immediately as it is
            # anyway register for heartbeats in case it dies immediately
            LOGGER.info("Sent ack to worker")
            self.worker_data["worker_heartbeats"][worker_key] = time.time()
        else:
            handle_dead_worker(self, worker_key)
            LOGGER.error("Registered worker unreachable as \
                          ack was being sent")

    elif message_type == "new_manager_job":
        job_id = self.next_job_id
        self.next_job_id += 1

        job = Job(
            job_id,
            message_dict["input_directory"],
            message_dict["output_directory"],
            message_dict["num_mappers"],
            message_dict["num_reducers"]
        )
        self.job_execs["mapper_executable"] = message_dict["mapper_executable"]
        self.job_execs["reducer_executable"] \
            = message_dict["reducer_executable"]
        self.job_queue.append(job)
        with self.threading_data["new_job_alert_condition"]:
            self.threading_data["new_job_alert_condition"].notify_all()
        # LOGGER.info(f"Received job {job_id} with {job.num_mappers} mappers, \
        #             {job.num_reducers} reducers, and queued it.")

    elif message_type == "finished":
        LOGGER.info("TASK MARKED COMPLETED")
        # "message_type": "finished",
        # "task_id": int,
        # "worker_host": string,
        # "worker_port": int
        worker = (message_dict["worker_host"], message_dict["worker_port"])
        task_id = message_dict["task_id"]

        if worker in self.worker_data["busy_workers"].keys():
            # LOGGER.info(f"REMOVING {worker} FROM BUSY WORKERS")
            del self.worker_data["busy_workers"][worker]
        else:
            LOGGER.info("SUPPOSED TO remove worker but not found")

        self.config["job"].task_finished(task_id)
        # LOGGER.info(f"Worker {worker} completed task {task_id}.")

    else:
        LOGGER.error("something terrible has happened we got a\
                      bad message type in message_handler")


def input_partitioning(input_dir, num_mappers):
    """Return a list of partitions."""
    LOGGER.info("Partitioning input files.")
    input_files = sorted(Path(input_dir).glob("*"))
    input_files.sort()  # Sort by name
    partitions = [[] for _ in range(num_mappers)]

    for idx, file in enumerate(input_files):
        partitions[idx % num_mappers].append(file)  # Round robin assignment

    string_partitions = [
        [str(path) for path in partition] for partition in partitions
    ]
    # LOGGER.info(f"Partitioned input files: {string_partitions}")
    return string_partitions


def start_mapping(self, string_partitions):
    """Map."""
    for task_id, input_paths in enumerate(string_partitions):
        task_message = {
            "message_type": "new_map_task",
            "task_id": task_id,
            "input_paths": input_paths,
            "executable": self.job_execs["mapper_executable"],
            "output_directory": self.config["job"].jobspecifictmpdir,
            "num_partitions": self.config["job"].num_reducers,
        }
        self.config["job"].add_task(task_id, task_message)
    # LOGGER.info("Added all partitions to job tasks list!")

    # LOGGER.info(f"NOTIFS: {self.threading_data["shutdown_event"].is_set()}, "
    #             f"{self.config["job"].mapping_finished_event.is_set()}")
    while (
        not self.threading_data["shutdown_event"].is_set() and
        not self.config["job"].mapping_finished_event.is_set()
    ):
        worker = get_available_worker(self)
        if worker:
            LOGGER.info("Worker available for task.")
            full = self.config["job"].get_task()
            if full:
                task_id, task = full
                # LOGGER.info(f"MANAGER MAPPING: SENDING TASK {task} "
                #             f"of TYPE {type(task)} TO WORKER {worker}")
                whost, wport = worker

                if tcp_client(whost, wport, task):
                    # LOGGER.info(f"TASK {task} SENT TO WORKER {worker}")
                    self.config["job"].assign_task(task_id, worker)
                    self.worker_data["busy_workers"][worker] = None
                else:
                    handle_dead_worker(self, worker)

        # LOGGER.info("Waiting for worker or task in manager mapping")
        time.sleep(1)
    # LOGGER.info("Exited start_mapping while loop")


def start_reducing(self):
    """Reduce."""
    # LOGGER.info("MANAGER: REDUCER RUNNING")
    tmpdir = self.config["job"].jobspecifictmpdir
    map_outputs = sorted(os.listdir(tmpdir))
    # LOGGER.info(f"MAP OUTPUTS: {map_outputs}")

    partitions = {}
    for file_name in map_outputs:
        if "part" in file_name:
            partition_id = file_name.split("-part")[-1]
            partitions.setdefault(partition_id, []).append(
                os.path.join(tmpdir, file_name)
            )
    # LOGGER.info(f"REDUCING PARTITIONS MADE: {partitions}")

    self.config["job"].post_map_reset()

    for task_id, partition in partitions.items():
        task_id = int(task_id)
        task_message = {
            "message_type": "new_reduce_task",
            "task_id": task_id,
            "executable": self.job_execs["reducer_executable"],
            "input_paths": partition,
            "output_directory": self.config["job"].output_dir,
        }
        self.config["job"].add_task(task_id, task_message)
    # LOGGER.info("Reducing tasks added to job class")

    while (
        not self.threading_data["shutdown_event"].is_set() and
        not self.config["job"].reducing_finished_event.is_set()
    ):
        worker = get_available_worker(self)
        if worker:
            # LOGGER.info("Worker available for reducing task.")
            full = self.config["job"].get_task()
            if full:
                task_id, task = full
                # LOGGER.info(f"MANAGER REDUCING: TASK AND WORKER AVAILABLE. "
                #             f"SENDING TASK {task} TO WORKER {worker}")
                whost, wport = worker

                if tcp_client(whost, wport, task):
                    # LOGGER.info(f"TASK {task} SENT TO WORKER {worker}")
                    self.config["job"].assign_task(task_id, worker)
                    # LOGGER.info("Through")
                    self.worker_data["busy_workers"][worker] = None
                else:
                    handle_dead_worker(self, worker)

        # LOGGER.info("Waiting for worker or task in manager reducing")
        time.sleep(1)
    # LOGGER.info("Exited start_reducing while loop")


def job_queue(self):
    """Jq."""
    while not self.threading_data["shutdown_event"].is_set():
        with self.threading_data["new_job_alert_condition"]:
            while not self.job_queue:
                self.threading_data["new_job_alert_condition"].wait()
                # LOGGER.info("New job added, new_job_alert_condition alerted")

            self.config["job"] = self.job_queue.popleft()

            output_dir = self.config["job"].output_dir
            if os.path.exists(output_dir):
                shutil.rmtree(output_dir)
            os.makedirs(output_dir)
            # LOGGER.info(f"Created output directory: {output_dir}")

            job_tmpdir = f"job-{self.config["job"].job_id:05d}"
            job_tmpdir_path = Path(self.tmpdir) / job_tmpdir
            self.config["job"].jobspecifictmpdir = str(job_tmpdir_path)
            os.makedirs(job_tmpdir_path, exist_ok=True)
            # LOGGER.info(f"Created temporary directory for job "
            #             f"{self.config["job"].job_id}: {job_tmpdir_path}")

            string_partitions = input_partitioning(
                self.config["job"].input_dir, self.config["job"].num_mappers
            )
            start_mapping(self, string_partitions)
            self.config["job"].post_map_reset()
            # LOGGER.info(f"BUSY WORKERS: {self.busy_workers.keys()}")
            # LOGGER.info("Busy workers should be removed after map reset!")
            self.worker_data["busy_workers"].clear()
            start_reducing(self)

            # LOGGER.info("DONE REDUCING!")
            if os.path.exists(job_tmpdir_path):
                shutil.rmtree(job_tmpdir_path)
        time.sleep(1)


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
