"""Example TCP socket server."""
import socket
import json
import logging
import time
from mapreduce.manager.job import *
from mapreduce.utils.servers import tcp_client
from mapreduce.manager.myheart import handle_dead_worker

LOGGER = logging.getLogger(__name__)
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
        sock.settimeout(1) # tutorial

        while not self.shutdown_event.is_set():
            # Wait for a connection for 1s.  The socket library avoids consuming
            # CPU while waiting for a connection.
            try:
                clientsocket, address = sock.accept()
            except socket.timeout:
                continue
            print("Connection from", address[0])

            # Socket recv() will block for a maximum of 1 second.  If you omit
            # this, it blocks indefinitely, waiting for packets.
            clientsocket.settimeout(1)

            # Receive data, one chunk at a time.  If recv() times out before we
            # can read a chunk, then go back to the top of the loop and try
            # again.  When the client closes the connection, recv() returns
            # empty data, which breaks out of the loop.  We make a simplifying
            # assumption that the client will always cleanly close the
            # connection.
            with clientsocket:
                message_chunks = []
                while True:
                    try:
                        data = clientsocket.recv(4096)
                    except socket.timeout:
                        continue
                    if not data:
                        break
                    message_chunks.append(data)

            # Decode list-of-byte-strings to UTF8 and parse JSON data
            message_bytes = b''.join(message_chunks)
            message_str = message_bytes.decode("utf-8")

            try:
                message_dict = json.loads(message_str)
                message_handler(self, message_dict)
            except json.JSONDecodeError:
                continue
            print(message_dict)

def message_handler(self, message_dict):
    message_type = message_dict["message_type"]

    if message_type == "shutdown":
        LOGGER.info("Received shutdown request")
        self.forward_shutdown_to_workers()
        self.shutdown_event.set()
        return

    elif message_type == "register":
        worker_host = message_dict["worker_host"]
        worker_port = message_dict["worker_port"]
        worker_key = (worker_host, worker_port)
        self.workers[worker_key] = None

        LOGGER.info("Registering Worker %s", worker_key)

        ack_message = {"message_type": "register_ack"}
        if tcp_client(worker_host, worker_port, ack_message):
            # i think it should give task immediately as it is
            # anyway register for heartbeats in case it dies immediately
            LOGGER.info("Sent ack to worker")
            self.worker_heartbeats[worker_key] = time.time()
        else:
            handle_dead_worker(self, worker_key)
            LOGGER.error("Registered worker unreachable as ack was being sent")
            


    elif message_type == "new_manager_job":
        job_id = self.next_job_id
        self.next_job_id += 1

        job = Job(
            job_id,
            message_dict["input_directory"],
            message_dict["output_directory"],
            message_dict["mapper_executable"],
            message_dict["reducer_executable"],
            message_dict["num_mappers"],
            message_dict["num_reducers"]
        )
        self.job_queue.append(job)
        with self.new_job_alert_condition:
            self.new_job_alert_condition.notify_all()
        LOGGER.info(f"Received job {job_id} with {job.num_mappers} mappers, {job.num_reducers} reducers, and queued it.")
    
    elif message_type == "finished":
        LOGGER.info(f"TASK MARKED COMPLETED")
        # "message_type": "finished",
        # "task_id": int,
        # "worker_host": string,
        # "worker_port": int
        worker = (message_dict["worker_host"], message_dict["worker_port"])
        task_id = message_dict["task_id"]

        if worker in self.busy_workers.keys():
            LOGGER.info(f"REMOVING {worker} FROM BUSY WORKERS")
            del self.busy_workers[worker]
        else:
            LOGGER.info(f"SUPPOSED TO REMOVE WORKER {worker} FROM BUSY, DID NOT FIND WORKER")

        self.job.task_finished(task_id)
        LOGGER.info(f"Worker {worker} completed task {task_id}.")

    else:
        LOGGER.error("something terrible has happened we got a bad message type in message_handler")
