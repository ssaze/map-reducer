"""Manager."""
import sys
import tempfile
import logging
import threading
from collections import deque

import click

from mapreduce.utils.ordered_dict import OrderedDict, ThreadSafeOrderedDict
from mapreduce.manager.tcp_listener import manager_tcp_server
from mapreduce.manager.myheart import manager_udp_server, check_heartbeats
from mapreduce.manager.fuckingmaininputmapreduce import job_queue
from mapreduce.utils.servers import tcp_client

# Configure logging
LOGGER = logging.getLogger(__name__)


class WorkerRegistry:
    def __init__(self):
        self.workers = OrderedDict()
        self.busy_workers = OrderedDict()
        self.worker_heartbeats = ThreadSafeOrderedDict()
        self.dead_workers = set()

class JobQueueManager:
    def __init__(self):
        self.job = None
        self.job_queue = deque()
        self.next_job_id = 0
        self.new_job_alert_condition = threading.Condition()


class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port):
        """Initialize Manager."""
        
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

            self.threads.extend([
                heartbeat_server_thread,
                manager_tcp_server_thread,
                dead_worker_checking_thread,
                job_queue_thread
            ])

            for thread in self.threads:
                thread.start()

            try:
                self.shutdown_event.wait()
            except KeyboardInterrupt:
                LOGGER.info("Shutdown signal received.")
            finally:
                LOGGER.info("Manager shutting down")
                self.cleanup()

            # LOGGER.info("Cleaned up tmpdir %s", self.tmpdir)

    def get_available_worker(self):
        """Get the next available worker."""
        # LOGGER.info("Checking for available worker...")
        for worker in self.workers.keys():
            if (
                worker not in self.busy_workers
                and worker not in self.dead_workers
            ):
                # LOGGER.info(f"Worker {worker} is available.")
                return worker
        LOGGER.info("No available workers.")
        return None

    def forward_shutdown_to_workers(self):
        """Send shutdown message to all registered workers."""
        LOGGER.debug("Forwarding shutdown to workers.")
        message = {"message_type": "shutdown"}
        for worker_host, worker_port in self.workers.keys():
            worker = (worker_host, worker_port)
            if worker in self.dead_workers:
                continue
            try:
                if tcp_client(worker_host, worker_port, message):
                    LOGGER.info(
                        f"Sent shutdown to worker {worker_host}:{worker_port}"
                    )
                else:
                    LOGGER.info(
                        f"tcp fail sd msg to wrkr {worker_host}:{worker_port}"
                    )
            except Exception as e:
                LOGGER.warning(
                    f"failed send sd to wrkr {worker_host}:{worker_port}: {e}"
                )

    def cleanup(self):
        """Cleanup resources on shutdown."""
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
