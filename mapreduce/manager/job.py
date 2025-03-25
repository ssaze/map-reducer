import threading
from collections import deque
from enum import Enum
import logging

class JobPhase(Enum):
    """Enumeration for the job phase."""
    MAPPING = "mapping"
    REDUCING = "reducing"
    DONE = "done"

LOGGER = logging.getLogger(__name__)
class Job:
    """Represents a MapReduce Job, tracking its tasks and progress."""

    def __init__(self, job_id, mapper_executable, reducer_executable, output_directory, num_mappers, num_reducers):
        self.job_id = job_id
        self.mapper_executable = mapper_executable
        self.reducer_executable = reducer_executable
        self.output_directory = output_directory
        self.num_reducers = num_reducers
        self.num_mappers = num_mappers

        self.phase = JobPhase.MAPPING  # Start in the mapping phase

        # Task queues
        self.pending_tasks = deque()  # Tasks to be assigned
        self.in_progress_tasks = {}  # task_id -> worker
        self.completed_tasks = set()
        self.condition = threading.Condition()
        self.lock = threading.Lock()  # Protect shared state

    def add_task(self, task):
        """Add a new task to the pending queue."""
        with self.lock:
            self.pending_tasks.append(task)
        with self.condition:
            self.condition.notify_all()

    def next_task(self):
        """Retrieve the next task for assignment, if available."""
        with self.lock:
            if self.pending_tasks:
                return self.pending_tasks.popleft()
            return None

    def all_tasks_completed(self):
        """Return True if all tasks in the current phase are completed."""
        with self.lock:
            return not self.pending_tasks and not self.in_progress_tasks

    def assign_task(self, task_id, worker):
        """Assign a task to a worker."""
        with self.lock:
            if task_id in self.in_progress_tasks:
                LOGGER.info("TASK ALREADY ASSIGNED")
                return False  # Already assigned
            LOGGER.info("TASK HAS BEEN ASSIGNED")
            self.in_progress_tasks[task_id] = worker
            return True

    def task_finished(self, task_id):
        """Mark a task as completed."""
        with self.lock:
            if task_id in self.in_progress_tasks:
                del self.in_progress_tasks[task_id]  # Remove from in-progress
                self.completed_tasks.add(task_id)  # Mark as complete

            # Check if we transition to reducing phase
            if self.phase == JobPhase.MAPPING and not self.pending_tasks and not self.in_progress_tasks:
                self.phase = JobPhase.REDUCING

            # Check if we are fully done
            if self.phase == JobPhase.REDUCING and not self.pending_tasks and not self.in_progress_tasks:
                self.phase = JobPhase.DONE

    def task_reset(self, task_id):
        """Re-enqueue a task if its worker failed and notify waiting threads."""
        with self.condition:
            if task_id in self.in_progress_tasks:
                del self.in_progress_tasks[task_id]
                self.pending_tasks.appendleft(task_id)  # Re-add to front
                self.condition.notify_all()  # Re-add to the front

    def is_complete(self):
        """Return True if all tasks are finished."""
        with self.lock:
            return self.phase == JobPhase.DONE
