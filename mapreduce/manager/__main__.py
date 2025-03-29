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
import click

from pathlib import Path
from collections import deque
from mapreduce.manager.job import Job, JobPhase

# Configure logging
LOGGER = logging.getLogger(__name__)

class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port, shared_dir):
        """Construct a Manager instance, start listening for messages, and optionally create a Job."""
        LOGGER.info(
            "Starting manager host=%s port=%s pwd=%s",
            host,
            port,
            os.getcwd(),
        )

        self.host = host
        self.port = port

        self.job = None
        self.workers = deque()
        self.busy_workers = set()
        self.worker_heartbeats = {}
        self.dead_workers = set()
        self.threads = []

        self.shutdown_event = threading.Event()
        self.new_job_alert_condition = threading.Condition()

        self.job_queue = deque()
        self.next_job_id = 0

        # BEGIN THREAD INIT


        # Create temporary directory for shared map/reduce files
        prefix = f"mapreduce-shared-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            self.tmpdir = tmpdir
            LOGGER.info("Created tmpdir %s", tmpdir)

            # TODO: Add all code needed so that this `with` block doesn't end until the Manager shuts down.



        LOGGER.info("Cleaned up tmpdir %s", tmpdir)

        # JOB QUEUE THREAD HANDLE_JOB_QUEUE
        job_thread = threading.Thread(target=self.handle_job_queue, daemon=True)
        job_thread.start()
        self.threads.append(job_thread)

        # Start TCP command listener (e.g., for shutdown)
        command_thread = threading.Thread(target=self.listen_for_commands, daemon=True)
        command_thread.start()
        self.threads.append(command_thread)

        LOGGER.info("INITIALIZED PROPERLY")

        LOGGER.info(f"THREAD COUNT: {threading.active_count()}")
        self.start_heartbeat_listener()
        LOGGER.info(f"THREAD COUNT: {threading.active_count()}")
        self.start_dead_worker_handler()

        try:
            self.shutdown_event.wait()  # Wait until shutdown signal
        except KeyboardInterrupt:
            LOGGER.info("Shutdown signal received.")
        finally:
            LOGGER.info("Manager shutting down")
            self.cleanup()

        LOGGER.info("Cleaned up tmpdir %s", self.tmpdir)

    # ------------------------------- Job Creation and Initialization -------------------------------
    def get_available_worker(self):
        """ Get the next available worker. """
        LOGGER.info("Checking for available worker...")
        for worker in self.workers:
            if worker not in self.busy_workers and worker not in self.dead_workers:
                LOGGER.info(f"Worker {worker} is available.")
                return worker
        LOGGER.info("No available workers.")
        return None  # All workers are dead    
    # ------------------------------- Worker Register and Shutdown and Listening Commands -------------------------------
    def forward_shutdown_to_workers(self):
        """Send shutdown message to all registered workers."""
        LOGGER.debug(f"Forwarding shutdown to workers.")
        for worker_host, worker_port in self.workers:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((worker_host, worker_port))
                    shutdown_message = json.dumps({"message_type": "shutdown"})
                    s.sendall(shutdown_message.encode('utf-8'))
                    LOGGER.info(f"Sent shutdown message to worker {worker_host}:{worker_port}")
            except Exception as e:
                LOGGER.warning(f"Failed to send shutdown to worker {worker_host}:{worker_port}: {e}")
    # ------------------------------- Worker Heartbeat and Failure Handling -------------------------------
    def process_heartbeat(self, heartbeat_data):
        """ Process heartbeats from workers, marking them as alive or dead. """
        worker_host = heartbeat_data['worker_host']
        worker_port = heartbeat_data['worker_port']
        worker_key = (worker_host, worker_port)

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

    def start_dead_worker_handler(self):
        """Start handling dead workers and reassign their tasks."""
        dead_worker_thread = threading.Thread(target=self.handle_dead_workers, daemon=True)
        self.threads.append(dead_worker_thread)
        dead_worker_thread.start()


    def handle_dead_workers(self):
        """Monitor for dead workers and reassign their tasks."""
        while not self.shutdown_event.is_set():
            time.sleep(5)  # Periodic check for dead workers

            with self.job.lock:
                for worker_key, missed_heartbeats in list(self.worker_heartbeats.items()):
                    if missed_heartbeats >= 5:  # Worker is dead
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