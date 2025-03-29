from pathlib import Path
from __main__ import Manager
from job import *
from utils.tcp import *
import time
import json
import os

LOGGER = logging.getLogger(__name__)

def input_partitioning(input_dir, num_mappers):
    """
    return a list of partitions
    """
    LOGGER.info("Partitioning input files.")
    input_files = sorted(Path(input_dir).glob("*"))
    input_files.sort()  # sort by name
    partitions = [[] for _ in range(num_mappers)]  # Create empty lists for each mapper

    for idx, file in enumerate(input_files):
        partitions[idx % num_mappers].append(file)  # round robin assignment
    # PARTITIONS ARE LIST OF LIST OF PATHS
    string_partitions = [[str(path) for path in partition] for partition in partitions]
    LOGGER.info(f"Partitioned input files: {string_partitions}")
    return string_partitions

def start_mapping(self, string_partitions):
    for task_id, input_paths in enumerate(string_partitions):
        # add all tasks to job queue
        task_message = {
            "message_type": "new_map_task",
            "task_id": task_id,
            "input_paths": input_paths,
            "executable": self.job.mapper_executable,
            "output_directory": self.job.output_directory,
            "num_partitions": self.job.num_reducers,
        }
        self.job.add_task(json.dumps(task_message))
    LOGGER.info("Added all partitions to job tasks list!!")
        
    while not self.shutdown_event.is_set() and not self.job.mapping_finished_notif:
        worker = self.get_available_worker()
        if worker: # worker available
            task = self.job.get_task() # try getting task
            if task: # task and worker available. assigning
                LOGGER.info("MANAGER MAPPING: TASK AND WORKER AVAILABLE. SENDING TASK {task} TO WORKER {worker}")
                whost, wport = worker
                tcp_client(whost, wport, task)
                LOGGER.info("TASK {task} SENT TO WORKER {worker}")
                self.job.assign_task(task["task_id"], worker)
        LOGGER.info("waiting for worker or task in manager mapping")
        time.sleep(1)
    LOGGER.info("EXITED START_MAPPING WHILE LOOP")

def start_reducing(self):
    LOGGER.info("MANAGER: REDUCER RUNNING")
    # eg in /tmp/mapreduce-shared-XYZ123/job-00000
    # the maptask00000-part00000 stuff
    tmpdir = self.tmpdir
    map_outputs = sorted(os.listdir(tmpdir))

    partitions = {}
    for file_name in map_outputs:
        if "part" in file_name:
            partition_id = file_name.split("-part")[-1]  # partition number
            partitions.setdefault(partition_id, []).append(os.path.join(tmpdir, file_name)) # create partitions
    LOGGER.info("REDUCING PARTITIONS MADE: {partitions}")

    # update job
    self.job.post_map_reset()
    for task_id, partition in partitions.items():
        task_message = {
            "message_type": "new_reduce_task",
            "task_id": task_id,
            "executable": self.job.reducer_executable,
            "input_paths": partition,
            "output_directory": self.tmpdir,
        }
        self.job.add_task(json.dumps(task_message))
    LOGGER.info("REDUCING tasks added to job class")

    # assigning partitions
    while not self.shutdown_event.is_set() and not self.job.reducing_finished_notif:
        worker = self.get_available_worker()
        if worker:
            task = self.job.get_task()
            if task:
                LOGGER.info("MANAGER REDUCING: TASK AND WORKER AVAILABLE. SENDING TASK {task} TO WORKER {worker}")
                whost, wport = worker
                tcp_client(whost, wport, task)
                LOGGER.info("TASK {task} SENT TO WORKER {worker}")
                self.job.assign_task(task["task_id"], worker)
        LOGGER.info("waiting for worker or task in manager reducing")
        time.sleep(1)
    LOGGER.info("EXITED START_REDUCING WHILE LOOP")

def job_queue(self):
    while not self.shutdown_event.is_set():
        if self.job_queue:
            self.job = self.job_queue.popleft()

            # INPUT PARTITION
            string_partitions = input_partitioning(self.job.input_dir, self.job.num_mappers)
            # SEND PARTITION RESULT TO MAPPER
            start_mapping(self, string_partitions)
            
        time.sleep(1)