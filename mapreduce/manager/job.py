"""Docstring."""
import threading
from collections import deque
from enum import Enum
from mapreduce.utils.ordered_dict import *
import logging
import json


class JobPhase(Enum):
    """Enumeration for the job phase."""

    MAPPING = "mapping"
    REDUCING = "reducing"
    DONE = "done"


LOGGER = logging.getLogger(__name__)


class Job:
    """Represents a MapReduce Job, tracking its tasks and progress."""

    def __init__(self, job_id, input_dir, output_dir, mapper_executable,
                 reducer_executable, num_mappers, num_reducers):
        """Docstring."""
        self.job_id = job_id
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.mapper_executable = mapper_executable
        self.reducer_executable = reducer_executable
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers

        self.jobspecifictmpdir = None

        self.phase = JobPhase.MAPPING  # Start in the mapping phase

        # Task queues
        self.tasks = OrderedDict()  # Tasks to be assigned
        self.task_reference_dict = OrderedDict()
        self.in_progress_tasks = OrderedDict()  # task_id -> worker
        self.completed_tasks = OrderedDict()

        self.mapping_finished_event = threading.Event()
        self.reducing_finished_event = threading.Event()
        self.lock = threading.Lock()  # Protect shared state

    def add_task(self, task_id, task):
        """Add a new task to the pending queue."""
        # tasks are dict at this point
        LOGGER.info(f"TYPE TASK : {type(task)}")
        task_id = int(task["task_id"])
        LOGGER.info(f"task id in add task: {task_id}")
        self.tasks[task_id] = task

        if task_id not in self.task_reference_dict.keys():
            self.task_reference_dict[task_id] = task
        else:
            LOGGER.warning(f"Task {task_id} is alr in task_reference_dict.")

    def get_task(self):
        """Retrieve the next task for assignment, if available."""
        if self.tasks:
            return list(self.tasks.items())[0]
        return None

    def assign_task(self, task_id, worker):
        """Assign a task to a worker."""
        if task_id in self.in_progress_tasks.keys():
            LOGGER.info("ASSIGNED TASK ALREADY ASSIGNED")
            return False
        elif task_id in self.completed_tasks.keys():
            LOGGER.info("ASSIGNED TASK ALREADY COMPLETED")
            return False
        LOGGER.info(f"TASK {task_id} HAS BEEN ASSIGNED TO {worker}")
        self.in_progress_tasks[task_id] = worker
        self.tasks.pop(task_id)
        return True

    def task_finished(self, task_id):
        """Mark a task as completed."""
        # LOGGER.info(f"task_id: {task_id}.
        #  in progress keys: {self.in_progress_tasks.keys()}")
        if task_id in self.in_progress_tasks.keys():
            del self.in_progress_tasks[task_id]
            # Remove from in-progress
            self.completed_tasks[task_id] = None  # Mark as complete
            LOGGER.info(f"Task {task_id} marked COMPLETED")
            # LOGGER.info(f"Tasks remaining: {
            # self.tasks.keys()}, {self.in_progress_tasks.keys()}")

        # Check if we transition to reducing phase
        if (self.phase == JobPhase.MAPPING
                and not self.tasks
                and not self.in_progress_tasks):
            LOGGER.info(f"MOVING TO REDUCING")
            self.phase = JobPhase.REDUCING
            self.mapping_finished_event.set()

        # Check if we are fully done
        elif (self.phase == JobPhase.REDUCING
              and not self.tasks
              and not self.in_progress_tasks):
            LOGGER.info(f"JOB COMPLETE")
            self.phase = JobPhase.DONE
            self.reducing_finished_event.set()

    def reset_task(self, task):
        """
        Re-enqueue a task if its.

        worker failed and notify waiting threads.
        """
        task_id = task["task_id"]
        if task_id in self.in_progress_tasks.keys():
            del self.in_progress_tasks[task_id]
            actual_task = self.task_reference_dict[task_id]
            self.tasks[task_id] = actual_task   # Re-add
        else:
            LOGGER.info("SOMETHING WRONG AFTER WORKER DEATH REASSIGNMENT")

    def post_map_reset(self):
        """Docstring."""
        with self.lock:
            self.tasks.clear()
            self.task_reference_dict.clear()
            self.in_progress_tasks.clear()
            self.completed_tasks.clear()
            self.phase = JobPhase.REDUCING

