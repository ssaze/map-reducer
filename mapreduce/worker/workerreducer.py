import heapq
import os
import subprocess
import tempfile
import shutil
import json
import socket

def run_reduce_task(task_id, input_paths, executable, output_directory, worker_host, worker_port):
    # Step 1: Merge input files using heapq.merge
    input_streams = [open(path, 'r') for path in input_paths]
    
    # Merging the input files (sorted)
    merged_input = heapq.merge(*input_streams)
    
    # Step 2: Create a temporary directory to store the reduce output
    with tempfile.TemporaryDirectory(prefix=f"mapreduce-local-task{task_id}-") as tmpdir:
        output_file_path = os.path.join(tmpdir, f"part-{task_id:05d}")
        
        with open(output_file_path, 'w') as outfile:
            # Step 3: Run the reduce executable via subprocess
            with subprocess.Popen(
                [executable],
                text=True,
                stdin=subprocess.PIPE,
                stdout=outfile,
            ) as reduce_process:
                # Pipe merged input to the reduce process
                for line in merged_input:
                    reduce_process.stdin.write(line)
                    
                # Close the stdin of the reduce process to signal the end of input
                reduce_process.stdin.close()
                
                # Wait for the process to finish
                reduce_process.wait()
        
        # Step 4: Move the output file to the final output directory
        shutil.move(output_file_path, os.path.join(output_directory, f"part-{task_id:05d}"))

    # Step 5: Send "finished" message to the manager
    finished_message = {
        "message_type": "finished",
        "task_id": task_id,
        "worker_host": worker_host,
        "worker_port": worker_port,
    }
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((worker_host, worker_port))
        s.sendall(json.dumps(finished_message).encode())

    print(f"Worker {worker_host}:{worker_port} finished task {task_id}")
