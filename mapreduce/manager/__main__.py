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

from mapreduce.utils.ordered_dict import *
from mapreduce.manager.tcp_listener import *
from mapreduce.manager.myheart import *
from mapreduce.manager.fuckingmaininputmapreduce import job_queue


#TODO ignore misbehaving workers


# Configure logging
LOGGER = logging.getLogger(__name__)

class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port):
        "ohhhhh i wish the lord would take me now"
        # variable inits
        self.host = host
        self.port = port
        self.job = None
        self.workers = OrderedDict()
        self.busy_workers = OrderedDict()
        self.worker_heartbeats = ThreadSafeOrderedDict()
        self.dead_workers = set()
        self.threads = []

        self.shutdown_event = threading.Event()
        self.new_job_alert_condition = threading.Condition()

        self.job_queue = deque()
        self.next_job_id = 0

        # Create temporary directory for shared map/reduce files
        prefix = f"mapreduce-shared-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            # everything in here so this `with` block doesn't end 
            # until the Manager shuts down.
            self.tmpdir = tmpdir
            LOGGER.info("Created tmpdir %s", self.tmpdir)

            # threading
            heartbeat_server_thread = threading.Thread(target=manager_udp_server, args=(self, host, port), daemon=True)
            manager_tcp_server_thread = threading.Thread(target=manager_tcp_server, args=(self, host, port), daemon=True)
            dead_worker_checking_thread = threading.Thread(target=check_heartbeats, args=(self,), daemon=True)
            job_queue_thread = threading.Thread(target=job_queue, args=(self,), daemon=True)

            self.threads.extend([heartbeat_server_thread, 
                                 manager_tcp_server_thread, 
                                 dead_worker_checking_thread, 
                                 job_queue_thread])

            for thread in self.threads:
                thread.start()

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
        for worker in self.workers.keys():
            if worker not in self.busy_workers.keys() and worker not in self.dead_workers:
                LOGGER.info(f"Worker {worker} is available.")
                return worker
        LOGGER.info("No available workers.")
        return None  # All workers are dead    
    # ------------------------------- Worker Register and Shutdown and Listening Commands -------------------------------
    def forward_shutdown_to_workers(self):
        """Send shutdown message to all registered workers."""
        LOGGER.debug(f"Forwarding shutdown to workers.")
        message = {"message_type": "shutdown"}
        for worker_host, worker_port in self.workers.keys():
            worker = (worker_host, worker_port)
            if worker in self.dead_workers:
                continue
            try:
                if tcp_client(worker_host, worker_port, message):
                    LOGGER.info(f"Sent shutdown message to worker {worker_host}:{worker_port}")
                else:
                    LOGGER.info(f"tcp client failed to sent shutdown message to worker {worker_host}:{worker_port}")
            except Exception as e:
                LOGGER.warning(f"Failed to send shutdown to worker {worker_host}:{worker_port}: {e}")
    # ------------------------------- Cleanup Function -------------------------------
    def cleanup(self):
        """ Cleanup resources on shutdown. """
        # self.shutdown_event.set() - already calls on msg receive
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
