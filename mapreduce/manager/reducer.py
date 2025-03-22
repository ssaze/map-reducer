import pathlib
import json

def assign_reduce_tasks(shared_temp_dir, num_reducers, worker_list):
    # dictionary to hold reduce tasks by partition
    reduce_tasks = {i: [] for i in range(num_reducers)}

    # glob to find all map outputs
    map_output_files = list(pathlib.Path(shared_temp_dir).glob("maptask*-part*"))

    # group the map output files into partitions
    for file in map_output_files:
        # Extract the partition number (assumes the file name format is consistent)
        partition_number = int(file.name.split('-')[1].split('part')[1])
        reduce_tasks[partition_number].append(str(file))

    # Send reduce tasks to workers
    task_id = 0  # start from 0
    for partition, files in reduce_tasks.items():
        # Assign tasks to workers in the order of their registration
        worker = worker_list[task_id % len(worker_list)] # round-robin again
        message = {
            "message_type": "new_reduce_task",
            "task_id": task_id,
            "input_paths": files,
            "executable": "reducer.py",  # Assuming 'reducer.py' is the reducer script
            "output_directory": "/tmp/reduce_output"  # Specify output directory
        }
        print(f"Sending to Worker {worker}: {json.dumps(message, indent=4)}")
        task_id += 1
