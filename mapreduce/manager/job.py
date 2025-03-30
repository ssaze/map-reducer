"""Job management for MapReduce."""
import logging
import threading
from enum import Enum
from mapreduce.utils.ordered_dict import OrderedDict


class JobPhase(Enum):
    """Enumeration for the job phase."""
    MAPPING = "mapping"
    REDUCING = "reducing"
    DONE = "done"


LOGGER = logging.getLogger(__name__)


class Job:
    """Represents a MapReduce Job, tracking its tasks and progress."""

    def __init__(self, job_id, input_dir, output_dir,
                 num_mappers, num_reducers):
        """Initialize a new MapReduce job with the given configuration."""
        self.config = {
            'job_id': job_id,
            'input_dir': input_dir,
            'output_dir': output_dir,
            'num_mappers': num_mappers,
            'num_reducers': num_reducers,
            'jobspecifictmpdir': None
        }

        self._internal = {
            'phase': JobPhase.MAPPING,
            'tasks': OrderedDict(),
            'task_reference_dict': OrderedDict(),
            'in_progress_tasks': OrderedDict(),
            'completed_tasks': OrderedDict(),
            'mapping_finished_event': threading.Event(),
            'reducing_finished_event': threading.Event(),
            'lock': threading.Lock()
        }

    # -------- Config Accessors --------
    @property
    def job_id(self):
        """Get the job ID."""
        return self.config['job_id']

    @property
    def input_dir(self):
        """Get the input directory."""
        return self.config['input_dir']

    @property
    def output_dir(self):
        """Get the output directory."""
        return self.config['output_dir']

    @property
    def num_mappers(self):
        """Get the number of mappers."""
        return self.config['num_mappers']

    @property
    def num_reducers(self):
        """Get the number of reducers."""
        return self.config['num_reducers']

    @property
    def jobspecifictmpdir(self):
        """Get the job-specific temp directory."""
        return self.config['jobspecifictmpdir']

    @jobspecifictmpdir.setter
    def jobspecifictmpdir(self, value):
        """Set the job-specific temp directory."""
        self.config['jobspecifictmpdir'] = value

    # -------- Runtime State Accessors --------
    @property
    def phase(self):
        """Get the current job phase."""
        return self._internal['phase']

    @phase.setter
    def phase(self, value):
        """Set the current job phase."""
        self._internal['phase'] = value

    @property
    def tasks(self):
        """Get the task queue."""
        return self._internal['tasks']

    @property
    def in_progress_tasks(self):
        """Get the in-progress task mapping."""
        return self._internal['in_progress_tasks']

    @property
    def completed_tasks(self):
        """Get the completed task mapping."""
        return self._internal['completed_tasks']

    @property
    def task_reference_dict(self):
        """Get the task reference dictionary."""
        return self._internal['task_reference_dict']

    @property
    def mapping_finished_event(self):
        """Get the event signaling mapping completion."""
        return self._internal['mapping_finished_event']

    @property
    def reducing_finished_event(self):
        """Get the event signaling reducing completion."""
        return self._internal['reducing_finished_event']

    @property
    def lock(self):
        """Get the threading lock."""
        return self._internal['lock']

    # -------- Job Logic Methods --------
    def add_task(self, task_id, task):
        """Add a new task to the pending queue."""
        # LOGGER.info(f"TYPE TASK : {type(task)}")
        task_id = int(task["task_id"])
        # LOGGER.info(f"task id in add task: {task_id}")
        self.tasks[task_id] = task

        if task_id not in self.task_reference_dict:
            self.task_reference_dict[task_id] = task
        # else:
            # LOGGER.warning(f"Task {task_id} is already in task_reference_dict.")

    def get_task(self):
        """Retrieve the next task for assignment, if available."""
        if self.tasks:
            return list(self.tasks.items())[0]
        return None

    def assign_task(self, task_id, worker):
        """Assign a task to a worker."""
        if task_id in self.in_progress_tasks:
            # LOGGER.info("ASSIGNED TASK ALREADY ASSIGNED")
            return False
        if task_id in self.completed_tasks:
            # LOGGER.info("ASSIGNED TASK ALREADY COMPLETED")
            return False
        # LOGGER.info(f"TASK {task_id} HAS BEEN ASSIGNED TO {worker}")
        self.in_progress_tasks[task_id] = worker
        self.tasks.pop(task_id)
        return True

    def task_finished(self, task_id):
        """Mark a task as completed and handle phase transitions."""
        if task_id in self.in_progress_tasks:
            del self.in_progress_tasks[task_id]
            self.completed_tasks[task_id] = None
            # LOGGER.info(f"Task {task_id} marked COMPLETED")

        if (self.phase == JobPhase.MAPPING
                and not self.tasks
                and not self.in_progress_tasks):
            # LOGGER.info("MOVING TO REDUCING")
            self.phase = JobPhase.REDUCING
            self.mapping_finished_event.set()

        elif (self.phase == JobPhase.REDUCING
              and not self.tasks
              and not self.in_progress_tasks):
            # LOGGER.info("JOB COMPLETE")
            self.phase = JobPhase.DONE
            self.reducing_finished_event.set()

    def reset_task(self, task):
        """Re-enqueue a task if its worker failed."""
        task_id = task["task_id"]
        if task_id in self.in_progress_tasks:
            del self.in_progress_tasks[task_id]
            actual_task = self.task_reference_dict[task_id]
            self.tasks[task_id] = actual_task
        # else:
            # LOGGER.info("SOMETHING WRONG AFTER WORKER DEATH REASSIGNMENT")

    def post_map_reset(self):
        """Clear map-phase data before entering reduce phase."""
        with self.lock:
            self.tasks.clear()
            self.task_reference_dict.clear()
            self.in_progress_tasks.clear()
            self.completed_tasks.clear()
            self.phase = JobPhase.REDUCING
