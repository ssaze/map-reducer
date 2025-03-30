"""MapReduce program."""
import os
import time
# import json
import shutil
import logging
from pathlib import Path

# from mapreduce.manager.job import Job
from mapreduce.utils.servers import tcp_client
from mapreduce.manager.myheart import handle_dead_worker

LOGGER = logging.getLogger(__name__)


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
            "executable": self.job.mapper_executable,
            "output_directory": self.job.jobspecifictmpdir,
            "num_partitions": self.job.num_reducers,
        }
        self.job.add_task(task_id, task_message)
    # LOGGER.info("Added all partitions to job tasks list!")

    # LOGGER.info(f"NOTIFS: {self.shutdown_event.is_set()}, "
    #             f"{self.job.mapping_finished_event.is_set()}")
    while (
        not self.shutdown_event.is_set() and
        not self.job.mapping_finished_event.is_set()
    ):
        worker = self.get_available_worker()
        if worker:
            LOGGER.info("Worker available for task.")
            full = self.job.get_task()
            if full:
                task_id, task = full
                # LOGGER.info(f"MANAGER MAPPING: SENDING TASK {task} "
                #             f"of TYPE {type(task)} TO WORKER {worker}")
                whost, wport = worker

                if tcp_client(whost, wport, task):
                    # LOGGER.info(f"TASK {task} SENT TO WORKER {worker}")
                    self.job.assign_task(task_id, worker)
                    self.busy_workers[worker] = None
                else:
                    handle_dead_worker(self, worker)

        # LOGGER.info("Waiting for worker or task in manager mapping")
        time.sleep(1)
    # LOGGER.info("Exited start_mapping while loop")


def start_reducing(self):
    """Reduce."""
    # LOGGER.info("MANAGER: REDUCER RUNNING")
    tmpdir = self.job.jobspecifictmpdir
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

    self.job.post_map_reset()

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
    # LOGGER.info("Reducing tasks added to job class")

    while (
        not self.shutdown_event.is_set() and
        not self.job.reducing_finished_event.is_set()
    ):
        worker = self.get_available_worker()
        if worker:
            # LOGGER.info("Worker available for reducing task.")
            full = self.job.get_task()
            if full:
                task_id, task = full
                # LOGGER.info(f"MANAGER REDUCING: TASK AND WORKER AVAILABLE. "
                #             f"SENDING TASK {task} TO WORKER {worker}")
                whost, wport = worker

                if tcp_client(whost, wport, task):
                    # LOGGER.info(f"TASK {task} SENT TO WORKER {worker}")
                    self.job.assign_task(task_id, worker)
                    # LOGGER.info("Through")
                    self.busy_workers[worker] = None
                else:
                    handle_dead_worker(self, worker)

        # LOGGER.info("Waiting for worker or task in manager reducing")
        time.sleep(1)
    # LOGGER.info("Exited start_reducing while loop")


def job_queue(self):
    """Jq."""
    while not self.shutdown_event.is_set():
        with self.new_job_alert_condition:
            while not self.job_queue:
                self.new_job_alert_condition.wait()
                # LOGGER.info("New job added, new_job_alert_condition alerted")

            self.job = self.job_queue.popleft()

            output_dir = self.job.output_dir
            if os.path.exists(output_dir):
                shutil.rmtree(output_dir)
            os.makedirs(output_dir)
            # LOGGER.info(f"Created output directory: {output_dir}")

            job_tmpdir = f"job-{self.job.job_id:05d}"
            job_tmpdir_path = Path(self.tmpdir) / job_tmpdir
            self.job.jobspecifictmpdir = str(job_tmpdir_path)
            os.makedirs(job_tmpdir_path, exist_ok=True)
            # LOGGER.info(f"Created temporary directory for job "
            #             f"{self.job.job_id}: {job_tmpdir_path}")

            string_partitions = input_partitioning(
                self.job.input_dir, self.job.num_mappers
            )
            start_mapping(self, string_partitions)
            self.job.post_map_reset()
            # LOGGER.info(f"BUSY WORKERS: {self.busy_workers.keys()}")
            # LOGGER.info("Busy workers should be removed after map reset!")
            self.busy_workers.clear()
            start_reducing(self)

            # LOGGER.info("DONE REDUCING!")
            if os.path.exists(job_tmpdir_path):
                shutil.rmtree(job_tmpdir_path)
                # LOGGER.info(f"Deleted temporary directory for job "
                        # f"{self.job.job_id}: {job_tmpdir_path}")
        time.sleep(1)
