import os
import sys
import tempfile
import logging
import json
import time
import threading
import socket
import pathlib
import shutil
from pathlib import Path
from collections import deque
from mapreduce.manager.job import Job, JobPhase

# Configure logging
LOGGER = logging.getLogger(__name__)

class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port, job_params=None):
        """Construct a Manager instance, start listening for messages, and optionally create a Job."""
        LOGGER.info(
            "Starting manager host=%s port=%s pwd=%s",
            host,
            port,
            os.getcwd(),
        )

        self.host = host
        self.port = port
        self.workers = deque()
        self.busy_workers = set()
        self.worker_heartbeats = {}
        self.dead_workers = set()
        self.job = None
        self.threads = []
        self.shutdown_event = threading.Event()
        self.worker_condition = threading.Condition()

        self.job_queue = deque()
        self.next_job_id = 0

        # Create temporary directory for shared map/reduce files
        self.tmpdir = tempfile.mkdtemp(prefix="mapreduce-shared-")
        LOGGER.info("Created tmpdir %s", self.tmpdir)

        job_thread = threading.Thread(target=self.handle_job_queue, daemon=True)
        job_thread.start()
        self.threads.append(job_thread)

        # Start TCP command listener (e.g., for shutdown)
        command_thread = threading.Thread(target=self.listen_for_commands, daemon=True)
        command_thread.start()
        self.threads.append(command_thread)

        # If job_params is provided, create the job and proceed
        if job_params is not None:
            self.job_id, self.mapper_executable, self.reducer_executable, self.output_directory, self.num_mappers, self.num_reducers = job_params
            self.create_job(*job_params)

            # Create temporary directory for shared map/reduce files
            prefix = f"mapreduce-shared-"
            self.start_heartbeat_listener()
            self.start_dead_worker_handler()

            try:
                self.shutdown_event.wait()  # Wait until shutdown signal
            except KeyboardInterrupt:
                LOGGER.info("Shutdown signal received.")
            finally:
                LOGGER.info("Manager shutting down")
                self.cleanup()

            LOGGER.info("Cleaned up tmpdir %s", self.tmpdir)
        else:
            # No job running; just wait for shutdown (used in test_shutdown)
            self.shutdown_event.wait()
            LOGGER.info("Manager received shutdown event (no job). Cleaning up.")
            self.cleanup()

    # ------------------------------- Job Creation and Initialization -------------------------------
    def create_job(self, job_id, mapper_executable, reducer_executable, output_directory, num_mappers, num_reducers):
        """Create and start a new MapReduce job."""
        self.job = Job(
            job_id,
            mapper_executable,
            reducer_executable,
            output_directory,
            num_mappers,
            num_reducers,
        )
        LOGGER.info(f"Job {self.job.job_id} created, starting mapping phase.")

    def get_available_worker(self):
        """ Get the next available worker. """
        for worker in self.workers:
            if worker not in self.busy_workers and worker not in self.dead_workers:
                return worker
        return None  # All workers are dead
    # ------------------------------- General Task Management -------------------------------
    def assign_task_to_worker(self, worker, task):
        """ Assign a single task to an available worker. """
        # Ensure task is assigned only if the worker is not busy
        with self.job.lock:  # Critical section for modifying busy workers
            if worker not in self.busy_workers:
                self.busy_workers.add(worker)
                task_message = json.loads(task["task_message"])
                task_id = task_message["task_id"]
                if self.job.assign_task(task_id, worker):  # Update the Job to assign this task
                    self.send_task_to_worker(worker, task)
                    return True
        return False

    def send_task_to_worker(self, worker, task):
        """ Send the task to the worker via TCP. """
        worker_host, worker_port = worker
        try:
            tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tcp_socket.connect((worker_host, worker_port))
            task_data = json.dumps(task)
            LOGGER.info(f"Calling sendall() with: {task_data}")
            tcp_socket.sendall(task_data.encode()) #changed to sendall EDIT
            tcp_socket.close()
            LOGGER.info(f"Sending task to worker {worker}: {task}") # Log the task sent (should be file01, etc)
        except Exception as e:
            LOGGER.warning(f"Failed to send task to worker {worker}: {e}")
            self.dead_workers.add(worker)  # Mark as dead and reassign task
            self.reassign_tasks(worker)

    def reassign_tasks(self, dead_worker_key):
        """ Reassign tasks that were allocated to the dead worker. """
        with self.job.lock:
            failed_tasks = [
                task_id for task_id, worker in self.job.in_progress_tasks.items()
                if worker == dead_worker_key
            ]

            # Reset failed tasks and notify waiting threads
            for task_id in failed_tasks:
                self.job.task_reset(task_id)
                LOGGER.warning(f"Task {task_id} reassigned from dead worker {dead_worker_key}")

        with self.job.condition:
            self.job.condition.notify_all()

    def assign_task_to_next_worker(self, task):
        """ Assign task to the next available worker. """
        available_worker = self.get_available_worker()
        if available_worker:
            task.assigned_worker = available_worker
            self.send_task_to_worker(available_worker, task)
    
    def handle_worker_message(self, message):
        """Processes worker messages (task completion, failure, etc.)."""
        message_data = json.loads(message)

        if message_data["message_type"] == "task_complete":
            worker = tuple(message_data["worker"])
            task_id = message_data["task_id"]

            with self.job.lock:
                # Remove worker from busy set and mark task as complete
                if worker in self.busy_workers:
                    self.busy_workers.remove(worker)
                
                self.job.task_finished(task_id)  # Update job state

                LOGGER.info(f"Worker {worker} completed task {task_id}.")

                # Notify any waiting threads that job state has changed
                with self.job.condition:
                    self.job.condition.notify_all()  # Wake up waiting threads


    # ------------------------------- Worker Register and Shutdown -------------------------------
    def forward_shutdown_to_workers(self):
        """Send shutdown message to all registered workers."""
        for worker_host, worker_port in self.workers:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((worker_host, worker_port))
                    shutdown_message = json.dumps({"message_type": "shutdown"})
                    s.sendall(shutdown_message.encode('utf-8'))
                    LOGGER.info(f"Sent shutdown message to worker {worker_host}:{worker_port}")
            except Exception as e:
                LOGGER.warning(f"Failed to send shutdown to worker {worker_host}:{worker_port}: {e}")


    def listen_for_commands(self):
        """Main TCP server to receive shutdown and register commands."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tcp_socket:
            tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            tcp_socket.bind((self.host, self.port))
            tcp_socket.listen()

            LOGGER.info("Manager is listening for job submissions or shutdown on TCP %s:%s", self.host, self.port)

            while not self.shutdown_event.is_set():
                try:
                    conn, addr = tcp_socket.accept()
                    with conn:
                        try:
                            message_raw = conn.recv(4096)
                        except StopIteration:
                            # End of mock data during testing
                            LOGGER.warning("Mocked conn.recv() exhausted.")
                            break

                        if not message_raw:
                            continue

                        message = message_raw.decode()
                        message_data = json.loads(message)


                        message_data = json.loads(message)
                        LOGGER.debug("TCP recv\n%s", json.dumps(message_data, indent=2))

                        if message_data.get("message_type") == "shutdown":
                            LOGGER.info("Received shutdown request")
                            self.forward_shutdown_to_workers()
                            self.shutdown_event.set()
                            return  # Don't call sys.exit or cleanup here

                        elif message_data.get("message_type") == "register":
                            worker_host = message_data["worker_host"]
                            worker_port = message_data["worker_port"]
                            worker_key = (worker_host, worker_port)
                            self.workers.append(worker_key)

                            LOGGER.info("Registering Worker %s", worker_key)

                            # Send register_ack
                            ack_msg = {"message_type": "register_ack"}
                            conn.sendall(json.dumps(ack_msg).encode('utf-8'))

                        elif message_data.get("message_type") == "new_manager_job":
                            job_id = self.next_job_id
                            self.next_job_id += 1

                            input_directory = message_data["input_directory"]
                            output_directory = message_data["output_directory"]
                            mapper_executable = message_data["mapper_executable"]
                            reducer_executable = message_data["reducer_executable"]
                            num_mappers = message_data["num_mappers"]
                            num_reducers = message_data["num_reducers"]

                            job = Job(
                                job_id,
                                mapper_executable,
                                reducer_executable,
                                output_directory,
                                num_mappers,
                                num_reducers,
                            )

                            LOGGER.info(f"Received new job request. Assigned job_id={job_id}")
                            self.job_queue.append((job, input_directory, num_mappers))

                            # Optional: Trigger job processing now or from a separate thread


                except Exception as e:
                    import traceback
                    LOGGER.error("Error in TCP command listener:\n%s", traceback.format_exc())


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

    def handle_job_queue(self):
        while not self.shutdown_event.is_set():
            if not self.job_queue:
                time.sleep(1)
                continue

            if self.shutdown_event.is_set():
                break  # ✅ double check before processing new job


            # Get next job
            job, input_dir, num_mappers = self.job_queue.popleft()
            job_id = job.job_id
            output_dir = job.output_directory
            mapper = job.mapper_executable
            reducer = job.reducer_executable
            num_mappers = job.num_mappers
            num_reducers = job.num_reducers

            # Step 1: Clean and recreate output directory
            if os.path.exists(output_dir):
                shutil.rmtree(output_dir)
            os.makedirs(output_dir)

            # Step 2: Create shared temp dir for this job
            self.shared_job_dir = os.path.join(self.tmpdir, f"job-{job_id:05d}")
            os.makedirs(self.shared_job_dir)

            LOGGER.info(f"Starting job {job_id} with mapper={mapper}, reducer={reducer}")

            # Step 3: Set and run Job
            self.job = job
            input_files = sorted(Path(input_dir).glob("*"))
            input_paths = [str(p) for p in input_files]
            self.start_mapping(input_paths)

            # Wait for job to complete
            with self.job.condition:
                while not self.job.is_complete():
                    self.job.condition.wait()

            LOGGER.info(f"Job {job_id} complete")



    def start_mapping(self, input_files):
        """Start the mapping phase with fault tolerance."""
        partitions = self.partition_input_files(input_files, self.job.num_mappers)

        # Add all mapping tasks to the Job
        for task_id, input_paths in enumerate(partitions):
            task_message = {
                "message_type": "new_map_task",
                "task_id": task_id,
                "input_paths": input_paths,
                "executable": self.job.mapper_executable,
                "output_directory": self.job.output_directory,
                "num_partitions": self.job.num_reducers,
            }
            self.job.add_task(json.dumps(task_message))
        # notify jobs added
        with self.job.condition:
            self.job.condition.notify_all()

        # Assign tasks to workers as they become available
        def worker_task_handler():
            while self.job.phase == JobPhase.MAPPING and not self.shutdown_event.is_set():
                with self.job.condition:
                    while not self.job.pending_tasks and self.job.phase == JobPhase.MAPPING and not self.shutdown_event.is_set():
                        self.job.condition.wait(timeout=1)


                    worker = self.get_available_worker()
                    if worker:
                        task_data = self.job.next_task()
                        if task_data:
                            task_id = json.loads(task_data)["task_id"]
                            if self.job.assign_task(task_id, worker):
                                self.send_task_to_worker(worker, {"worker": worker, "task_message": task_data})
                                LOGGER.info(f"Assigned task {task_id} to worker {worker}")

        # Start worker task assignment in a thread
        task_thread = threading.Thread(target=worker_task_handler, daemon=True)
        self.threads.append(task_thread)
        task_thread.start()

        # Wait until all mapping tasks are done before moving to reducing
        with self.job.condition:
            while self.job.phase == JobPhase.MAPPING and not self.job.all_tasks_completed():
                self.job.condition.wait()  # Wait until mapping is complete

        self.start_reducing()  # Move to reducing phase

    # ------------------------------- Reducing Phase Functions -------------------------------
    def start_reducing(self):
        """Start the reducing phase of the job."""
        # Ensure mapping is complete before transitioning to reducing
        if self.job.phase == JobPhase.MAPPING:
            while self.job.pending_tasks or self.job.in_progress_tasks:
                time.sleep(1)  # Wait until all mapping tasks are complete
            self.job.phase = JobPhase.REDUCING  # Transition to reducing phase

        shared_temp_dir = self.shared_job_dir  # The shared directory for intermediate map outputs
        num_reducers = self.job.num_reducers
        worker_list = list(self.workers)  # Get the list of available workers

        # Ensure there are workers available
        if not worker_list:
            LOGGER.error("No workers available for reducing phase.")
            return

        # Dictionary to store reduce tasks
        reduce_tasks = {i: [] for i in range(num_reducers)}

        # Identify all map output files
        map_output_files = list(pathlib.Path(shared_temp_dir).glob("maptask*-part*"))

        # Assign files to partitions
        for file in map_output_files:
            partition_number = int(file.name.split('-')[1].split('part')[1])  # Extract partition number
            reduce_tasks[partition_number].append(str(file))  # Store file path

        # Assign reduce tasks to workers
        task_id = 0
        for partition, files in reduce_tasks.items():
            worker = worker_list[task_id % len(worker_list)]  # Round-robin assignment
            message = {
                "message_type": "new_reduce_task",
                "task_id": task_id,
                "input_paths": files,
                "executable": self.job.reducer_executable,
                "output_directory": self.job.output_directory
            }

            # Add the reduce task to the job queue
            self.job.add_task(json.dumps(message))
            task_id += 1

        # Notify that tasks have been added
        with self.job.condition:
            self.job.condition.notify_all()

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

        with self.job.lock:
            # Reset missed heartbeats counter for the worker
            self.worker_heartbeats[worker_key] = 0
            # If the worker is in dead_workers, remove it
            if worker_key in self.dead_workers:
                self.dead_workers.remove(worker_key)
                with self.job.condition:
                    self.job.condition.notify_all()
                LOGGER.info(f"Worker {worker_key} has been revived.")
                # Notify other threads that the state of the worker has changed


    def handle_dead_workers(self):
        """Monitor for dead workers and reassign their tasks."""
        while not self.shutdown_event.is_set():
            time.sleep(5)  # Periodic check for dead workers

            with self.job.lock:
                for worker_key, missed_heartbeats in list(self.worker_heartbeats.items()):
                    if missed_heartbeats >= 3:  # Worker is dead
                        if worker_key not in self.dead_workers:
                            self.dead_workers.add(worker_key)
                            LOGGER.warning(f"Worker {worker_key} is dead. Reassigning tasks.")
                            self.reassign_tasks(worker_key)

                    # Increment missed heartbeat count for alive workers
                    if worker_key not in self.dead_workers:
                        self.worker_heartbeats[worker_key] += 1
            with self.job.condition:
                self.job.condition.notify_all()

    # ------------------------------- Cleanup Function -------------------------------
    def cleanup(self):
        """ Cleanup resources on shutdown. """
        self.shutdown_event.set()  # Signal shutdown
        for thread in self.threads:
            thread.join(timeout=2)
            if thread.is_alive():
                LOGGER.warning("Thread %s did not exit cleanly.", thread.name)

        LOGGER.info("All threads have completed.")
        sys.exit(0)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="MapReduce Manager")
    parser.add_argument("--host", type=str, default="localhost", help="Host to bind the manager")
    parser.add_argument("--port", type=int, default=6000, help="Port to bind the manager")
    parser.add_argument("--loglevel", type=str, default="INFO", help="Logging level")
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.loglevel.upper()))
    Manager(args.host, args.port)

if __name__ == "__main__":
    main()