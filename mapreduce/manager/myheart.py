"""Example UDP socket server."""
import socket
import json
import logging
import time
from mapreduce.manager.job import *

def manager_udp_server(self, host, port):
    # Create an INET, DGRAM socket, this is UDP
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:

        # Bind the UDP socket to the server
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        sock.settimeout(1)

        # No sock.listen() since UDP doesn't establish connections like TCP

        # Receive incoming UDP messages
        while not self.shutdown_event.is_set():
            try:
                message_bytes = sock.recv(4096)
            except socket.timeout:
                continue

            try:
                message_str = message_bytes.decode("utf-8")
                message_dict = json.loads(message_str)
                if message_dict["message_type"] == "heartbeat":
                    worker_host = message_dict["worker_host"]
                    worker_port = message_dict["worker_port"]
                    worker = (worker_host, worker_port)
                    handle_heartbeat(self, worker)
                else:
                    LOGGER.error(f"!!!wrong message type on heartbeat line!!!")

            except UnicodeDecodeError as e:
                LOGGER.error(f"Failed to decode message from {host}:{port}. Error: {e}")
                continue
            except json.JSONDecodeError as e:
                LOGGER.error(f"Failed to parse JSON from message: {message_str}. Error: {e}")
                continue
            except:
                LOGGER.error(f"failed heartbeat processing for some other reason")

def handle_heartbeat(self, worker):
    current_time = time.time()
    if worker not in self.workers:
        LOGGER.info(f"heartbeat from unregistered {worker} skipped (spec)")
        return
    if worker in self.dead_workers:
        LOGGER.info(f"Worker {worker} revived.")
        self.dead_workers.remove(worker)

    self.worker_heartbeats[worker] = current_time
    LOGGER.info(f"Received heartbeat from worker {worker} at {current_time}")

# below run on sep thread
def check_heartbeats(self):
    while not self.shutdown_event.is_set():
        current_time = time.time()
        for worker, last_heartbeat in self.worker_heartbeats.items():
            if current_time - last_heartbeat > 10:
                LOGGER.warning(f"Worker {worker} has missed {5} heartbeats and is assumed dead.")
                handle_dead_worker(self, worker)
        time.sleep(2)

def handle_dead_worker(self, worker):
    # marking dead
    if worker in self.worker_heartbeats:
        del self.worker_heartbeats[worker]
    self.dead_workers.add(worker)

    if worker in self.busy_workers.keys():
        # had task assigned to it. need to reassign
        del self.busy_workers[worker]

        # find its task id
        matching_task_ids = [id for id, val in self.job.in_progress_tasks.items() if val == worker]
        LOGGER.info(f"Matching task ids for dead worker (should only be 1): {matching_task_ids}")
        matching_id = matching_task_ids[0]

        # need to reassign
        task_dict = self.job.task_reference_dict[matching_id] # corresponding task msg
        LOGGER.info(f"Reassigning task {task_dict}. Should have task_id in here")
        self.job.reset_task(task_dict)
