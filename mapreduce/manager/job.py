import threading
from collections import deque
from enum import Enum
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
        self.job_id = job_id
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.mapper_executable = mapper_executable
        self.reducer_executable = reducer_executable
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers

        self.phase = JobPhase.MAPPING  # Start in the mapping phase

        # Task queues
        self.tasks = deque()  # Tasks to be assigned
        self.task_reference_dict = {}
        self.in_progress_tasks = {}  # task_id -> worker
        self.completed_tasks = set()
        self.condition = threading.Condition()
        self.mapping_finished_notif = threading.Condition()
        self.reducing_finished_notif = threading.Condition()
        self.lock = threading.Lock()  # Protect shared state

    def add_task(self, task):
        """Add a new task to the pending queue."""
        # tasks are dict
        with self.lock:  
            self.tasks.append(task)
            task_id = task["task_id"]

            if task_id not in self.task_reference_dict:
                self.task_reference_dict[task_id] = task
            else:
                LOGGER.warning(f"Task {task_id} already exists in task_reference_dict. What the fuck happened")

        with self.condition:
            self.condition.notify_all() # NOTIFY NEW TASK

    def get_task(self):
        """Retrieve the next task for assignment, if available."""
        with self.lock:
            if self.tasks:
                return self.tasks[0]
            return None

    def assign_task(self, task_id, worker):
        """Assign a task to a worker."""
        with self.lock:
            if task_id in self.in_progress_tasks:
                LOGGER.info("ASSIGNED TASK ALREADY ASSIGNED")
                return False
            elif task_id in self.completed_tasks():
                LOGGER.info("ASSIGNED TASK ALREADY COMPLETED")
                return False
            LOGGER.info(f"TASK {task_id} HAS BEEN ASSIGNED TO {worker}")
            self.in_progress_tasks[task_id] = worker
            self.tasks.popleft()
            return True

    def task_finished(self, task_id):
        """Mark a task as completed."""
        with self.lock:
            if task_id in self.in_progress_tasks:
                del self.in_progress_tasks[task_id]  # Remove from in-progress
                self.completed_tasks.add(task_id)  # Mark as complete
                LOGGER.info(f"TASK {task_id} COMPLETED")

            # Check if we transition to reducing phase
            if self.phase == JobPhase.MAPPING and not self.tasks and not self.in_progress_tasks:
                with self.condition:
                    LOGGER.info(f"MOVING TO REDUCING")
                    self.phase = JobPhase.REDUCING
                    self.mapping_finished_notif.notify_all()

            # Check if we are fully done
            if self.phase == JobPhase.REDUCING and not self.tasks and not self.in_progress_tasks:
                with self.condition:
                    LOGGER.info(f"JOB COMPLETE")
                    self.phase = JobPhase.DONE
                    self.reducing_finished_notif.notify_all()

    def reset_task(self, task):
        """Re-enqueue a task if its worker failed and notify waiting threads."""
        with self.condition:
            task_id = task["task_id"]
            if task_id in self.in_progress_tasks:
                del self.in_progress_tasks[task_id]
                self.tasks.append(task_id)  # Re-add
            else:
                LOGGER.info("SOMETHING WRONG AFTER WORKER DEATH REASSIGNMENT")

    def post_map_reset(self):
        with self.lock:
            self.tasks.clear()
            self.task_reference_dict.clear()
            self.in_progress_tasks.clear()
            self.completed_tasks.clear()
            self.phase = JobPhase.REDUCING