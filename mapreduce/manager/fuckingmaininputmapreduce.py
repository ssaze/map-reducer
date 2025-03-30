from pathlib import Path
from mapreduce.manager.job import *
from mapreduce.utils.servers import *
from mapreduce.manager.myheart import *
import time
import json
import os
import shutil

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
            "output_directory": self.job.jobspecifictmpdir,
            "num_partitions": self.job.num_reducers,
        }
        self.job.add_task(task_id, task_message)
    LOGGER.info("Added all partitions to job tasks list!!")

    # tcp listener listens for completion msg, job notifies all
    LOGGER.info(f"NOTIFS: {self.shutdown_event.is_set()}, {self.job.mapping_finished_event.is_set()}")
    while not self.shutdown_event.is_set() and not self.job.mapping_finished_event.is_set():
        worker = self.get_available_worker()
        if worker: # worker available
            LOGGER.info("Worker available for task.")
            full = self.job.get_task() # try getting task

            if full: # task and worker available. assigning
                task_id, task = full
                LOGGER.info(f"MANAGER MAPPING: SENDING TASK {task} of TYPE {type(task)} TO WORKER {worker}")
                whost, wport = worker

                if tcp_client(whost, wport, task): # turns task dict into json and sends
                    LOGGER.info(f"TASK {task} SENT TO WORKER {worker}")
                    # just keeping  track below
                    self.job.assign_task(task_id, worker)
                    self.busy_workers[worker] = None
                else:
                    handle_dead_worker(self, worker)
                    # worker dead

        LOGGER.info("waiting for worker or task in manager mapping")
        time.sleep(2)
    LOGGER.info("EXITED START_MAPPING WHILE LOOP")

def start_reducing(self):
    LOGGER.info("MANAGER: REDUCER RUNNING")
    # eg in /tmp/mapreduce-shared-XYZ123/job-00000
    # the maptask00000-part00000 stuff
    tmpdir = self.job.jobspecifictmpdir
    map_outputs = sorted(os.listdir(tmpdir))
    LOGGER.info(f"MAP OUTPUTS: {map_outputs}")

    partitions = {}
    for file_name in map_outputs:
        if "part" in file_name:
            partition_id = file_name.split("-part")[-1]  # partition number
            partitions.setdefault(partition_id, []).append(os.path.join(tmpdir, file_name)) # create partitions
    LOGGER.info(f"REDUCING PARTITIONS MADE: {partitions}")

    # update job
    self.job.post_map_reset() # need to clear it out

    for task_id, partition in partitions.items():
        task_id = int(task_id)
        task_message = {
            "message_type": "new_reduce_task",
            "task_id": task_id,
            "executable": self.job.reducer_executable,
            "input_paths": partition,
            "output_directory": self.job.output_dir,
        }
        self.job.add_task(task_id, task_message)
    LOGGER.info("REDUCING tasks added to job class")

    # assigning partitions
    while not self.shutdown_event.is_set() and not self.job.reducing_finished_event.is_set():
        worker = self.get_available_worker()
        if worker:
            LOGGER.info("Worker available for reducing task.")
            full = self.job.get_task() # try getting task
            if full:
                task_id, task = full
                LOGGER.info(f"MANAGER REDUCING: TASK AND WORKER AVAILABLE. SENDING TASK {task} TO WORKER {worker}")
                whost, wport = worker

                if tcp_client(whost, wport, task):
                    LOGGER.info(f"TASK {task} SENT TO WORKER {worker}")
                    self.job.assign_task(task_id, worker)
                    # MARK AS BUSY IN SELF
                    LOGGER.info("THROUGH")
                    self.busy_workers[worker] = None
                else:
                    handle_dead_worker(self, worker)
                    

        LOGGER.info("waiting for worker or task in manager reducing")
        time.sleep(2)
    LOGGER.info("EXITED START_REDUCING WHILE LOOP")

def job_queue(self):
    while not self.shutdown_event.is_set():
        with self.new_job_alert_condition:
            while not self.job_queue:  # If the job queue is empty, wait
                self.new_job_alert_condition.wait()
                LOGGER.info("New job added, new_job_alert_condition alerted")

            # job available
            self.job = self.job_queue.popleft()

            # FINAL OUTPUT DIRECTORY
            output_dir = self.job.output_dir
            if os.path.exists(output_dir):
                shutil.rmtree(output_dir)
            os.makedirs(output_dir)
            LOGGER.info(f"Created output directory: {output_dir}")

            # JOB RELATED TEMP OUTPUTS
            job_tmpdir = f"job-{self.job.job_id:05d}"
            job_tmpdir_path = Path(self.tmpdir) / job_tmpdir
            self.job.jobspecifictmpdir = str(job_tmpdir_path)
            os.makedirs(job_tmpdir_path, exist_ok=True)
            LOGGER.info(f"Created temporary directory for job \
                        {self.job.job_id}: {job_tmpdir_path}")

            # INPUT PARTITION
            string_partitions = input_partitioning(self.job.input_dir,
                                                   self.job.num_mappers)
            # SEND PARTITION RESULT TO MAPPER
            start_mapping(self, string_partitions)
            self.job.post_map_reset()
            LOGGER.info(f"BUSY WORKERS: {self.busy_workers.keys()} \n \n \n")
            LOGGER.info("Busy workers should be removed after map reset!")
            self.busy_workers.clear()
            start_reducing(self)

            LOGGER.info("DONE REDUCING! \n \n \n \n \n")
            # REDUCING DONE - CLEANUP
            if os.path.exists(job_tmpdir_path):
                shutil.rmtree(job_tmpdir_path)
                LOGGER.info(f"Deleted temporary directory for job \
                            {self.job.job_id}: {job_tmpdir_path}")
        time.sleep(1)
