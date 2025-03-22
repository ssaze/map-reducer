"""MapReduce framework Manager node."""

import os
import tempfile
import logging
import json
import time
import threading
import click
import socket
import mapreduce.utils
from job import *


# Configure logging
LOGGER = logging.getLogger(__name__)


class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port, job_params):
        """Construct a Manager instance, start listening for messages, and create a Job."""
        LOGGER.info(
            "Starting manager host=%s port=%s pwd=%s",
            host,
            port,
            os.getcwd(),
        )

        self.host = host
        self.port = port
        self.workers = deque()  # Worker info
        self.busy_workers = set()
        self.worker_heartbeats = {}  # Worker heartbeat tracking
        self.dead_workers = set()  # Workers that failed
        self.job = None  # Current job being executed
        self.job_id, self.mapper_executable, self.reducer_executable, self.output_directory, self.num_reducers = job_params
        self.threads = []
        self.shutdown_event = threading.Event()  # Shutdown flag

        # Create a job based on the provided parameters
        self.create_job(*job_params)

        # Create temporary directory for shared data
        prefix = f"mapreduce-shared-"
        with tempfile.TemporaryDirectory(prefix=prefix) as self.tmpdir:
            LOGGER.info("Created tmpdir %s", self.tmpdir)
            self.start_heartbeat_listener()  # Start heartbeat listener
            self.start_dead_worker_handler()  # Start dead worker handler

            try:
                self.shutdown_event.wait()  # Wait for shutdown signal
            except KeyboardInterrupt:
                LOGGER.info("Shutdown signal received.")
            finally:
                LOGGER.info("Manager shutting down")
                self.cleanup()

        LOGGER.info("Cleaned up tmpdir %s", self.tmpdir)

    # ------------------------------- Job Creation and Initialization -------------------------------
    def create_job(self, job_id, mapper_executable, reducer_executable, output_directory, num_reducers):
        """Create and start a new MapReduce job."""
        self.job = Job(
            job_id,
            mapper_executable,
            reducer_executable,
            output_directory,
            num_reducers,
        )
        LOGGER.info(f"Job {self.job.job_id} created, starting mapping phase.")
        
    # ------------------------------- Mapping Phase Functions -------------------------------
    def partition_input_files(self, input_files, num_mappers):
        """
        Partitions sorted input files using round-robin assignment into num_mappers partitions.
        
        param input_files: list of input file names
        num_mappers: number of mapper workers available
        return a list of partitions
        """
        input_files.sort()  # sort by name
        partitions = [[] for _ in range(num_mappers)]  # Create empty lists for each mapper

        for idx, file in enumerate(input_files):
            partitions[idx % num_mappers].append(file)  # round robin assignment

        return partitions

    def assign_task_to_worker(self, worker, task):
        """ Assign a single task to an available worker. """
        # Ensure task is assigned only if the worker is not busy
        if worker not in self.busy_workers:
            self.busy_workers.add(worker)
            self.send_task_to_worker(worker, task)
            return True
        return False

    def start_mapping(self, input_files):
        """ Start the mapping phase of the job. """
        partitions = self.partition_input_files(input_files, self.job.num_reducers)
        task_messages = self.assign_tasks_to_workers(self.workers, partitions, self.job.mapper_executable, self.job.output_directory, self.job.num_reducers)
        
        for task_id, input_paths in enumerate(partitions):
            # Wait for an available worker if all are busy
            while len(self.busy_workers) == len(self.workers):
                time.sleep(1)  # Wait until a worker becomes free

            # Assign task to the next available worker
            available_worker = self.get_available_worker()
            if available_worker:
                task_message = {
                    "message_type": "new_map_task",
                    "task_id": task_id,
                    "input_paths": input_paths,
                    "executable": self.job.mapper_executable,
                    "output_directory": self.job.output_directory,
                    "num_partitions": self.job.num_reducers,
                }
                task = {"worker": available_worker, "task_message": json.dumps(task_message)}
                
                if self.assign_task_to_worker(available_worker, task):  # Assign task to worker
                    LOGGER.info(f"Assigned task {task_id} to worker {available_worker}")
                else:
                    LOGGER.warning(f"Worker {available_worker} is busy. Task {task_id} not assigned.")
            
    def get_available_worker(self):
        """ Get the next available worker. """
        for worker in self.workers:
            if worker not in self.busy_workers and worker not in self.dead_workers:
                return worker
        return None  # All workers are dead
    
    def send_task_to_worker(self, worker, task):
        """ Send the task to the worker via TCP. """
        worker_host, worker_port = worker
        try:
            tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tcp_socket.connect((worker_host, worker_port))
            task_data = json.dumps(task)
            tcp_socket.send(task_data.encode())
            tcp_socket.close()
        except Exception as e:
            LOGGER.warning(f"Failed to send task to worker {worker}: {e}")
            self.dead_workers.add(worker)  # Mark as dead and reassign task
            self.reassign_tasks(worker)

    def reassign_tasks(self, dead_worker_key):
        """ Reassign tasks that were allocated to the dead worker. """
        for task_id, worker in list(self.job.in_progress_tasks.items()):
            if worker == dead_worker_key:
                self.job.task_reset(task_id)  # Reset the task in the Job
                self.assign_task_to_next_worker(task_id)

    def assign_task_to_next_worker(self, task):
        """ Assign task to the next available worker. """
        available_worker = self.get_available_worker()
        if available_worker:
            task.assigned_worker = available_worker
            self.send_task_to_worker(available_worker, task)

    # ------------------------------- Reducing Phase Functions -------------------------------
    def start_reducing(self):
        """ Start the reducing phase of the job. """
        # Ensure mapping is complete before transitioning to reducing
        if self.job.phase == JobPhase.MAPPING:
            while self.job.pending_tasks or self.job.in_progress_tasks:
                time.sleep(1)  # Wait until all mapping tasks are complete
            self.job.phase = JobPhase.REDUCING  # Transition to reducing phase

        # Now process the reducing tasks
        while self.job.phase == JobPhase.REDUCING:
            task = self.job.next_task()
            if task:
                self.assign_task_to_next_worker(task)

    # ------------------------------- Worker Heartbeat and Failure Handling -------------------------------
    def listen_for_heartbeats(self):
        """ Listen for UDP heartbeats from workers. """
        udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_socket.bind((self.host, self.port))
        LOGGER.info("Manager is listening for heartbeats on UDP %s:%s", self.host, self.port)

        while not self.shutdown_event.is_set():
            try:
                message, addr = udp_socket.recvfrom(1024)
                heartbeat_data = json.loads(message.decode())
                self.process_heartbeat(heartbeat_data, addr)
            except Exception as e:
                LOGGER.error(f"Error while processing heartbeat: {e}")

    def process_heartbeat(self, heartbeat_data, addr):
        """ Process heartbeats from workers, marking them as alive or dead. """
        worker_host = heartbeat_data['worker_host']
        worker_port = heartbeat_data['worker_port']
        worker_key = (worker_host, worker_port)

        LOGGER.info(f"Received heartbeat from {worker_key}")

        # Reset missed heartbeats counter for the worker
        self.worker_heartbeats[worker_key] = 0

        # If the worker is in dead_workers, remove it
        if worker_key in self.dead_workers:
            self.dead_workers.remove(worker_key)
            LOGGER.info(f"Worker {worker_key} has been revived.")

    def handle_dead_workers(self):
        """ Monitor for dead workers and reassign tasks if needed. """
        while not self.shutdown_event.is_set():
            time.sleep(5)  # Periodic check for dead workers

            # Check for workers who have missed heartbeats
            for worker_key, missed_heartbeats in list(self.worker_heartbeats.items()):
                if missed_heartbeats >= 3:  # Worker missed 3 heartbeats
                    if worker_key not in self.dead_workers:
                        self.dead_workers.add(worker_key)
                        LOGGER.warning(f"Worker {worker_key} is dead. Reassigning tasks.")
                        self.reassign_tasks(worker_key)

                # Increment missed heartbeat counter for alive workers
                if worker_key not in self.dead_workers:
                    self.worker_heartbeats[worker_key] += 1

    # ------------------------------- Cleanup Function -------------------------------
    def cleanup(self):
        """ Cleanup resources on shutdown. """
        self.shutdown_event.set()  # Signal shutdown
        for thread in self.threads:
            thread.join()  # Ensure all threads finish

        LOGGER.info("All threads have completed.")
