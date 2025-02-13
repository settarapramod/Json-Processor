import concurrent.futures
import logging
import time
import random

# Configure Logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[
        logging.FileHandler("task_processor.log"),
        logging.StreamHandler()
    ]
)

class Task:
    """Task class representing a unit of work."""
    def __init__(self, task_id, data):
        self.task_id = task_id
        self.data = data

    def __repr__(self):
        return f"Task(task_id={self.task_id}, data={self.data})"

def execute_task(task):
    """
    Function to execute a task.
    Simulates work by sleeping for a random duration.
    Includes error handling.
    """
    try:
        logging.info(f"Starting Task {task.task_id}")
        time.sleep(random.uniform(1, 3))  # Simulated processing time
        if random.random() < 0.1:  # Simulating occasional failures
            raise Exception(f"Simulated failure in Task {task.task_id}")
        result = f"Task {task.task_id} processed successfully"
        logging.info(result)
        return result
    except Exception as e:
        logging.error(f"Error processing Task {task.task_id}: {e}")
        return f"Task {task.task_id} failed"

def process_tasks_parallel(tasks, num_workers=None, max_retries=3):
    """
    Process a list of tasks in parallel.
    - Retries failed tasks up to `max_retries` times.
    - Uses `concurrent.futures.ProcessPoolExecutor` for better performance.
    
    :param tasks: List of Task objects.
    :param num_workers: Number of parallel workers (defaults to CPU count).
    :param max_retries: Number of retries for failed tasks.
    """
    num_workers = num_workers or min(4, concurrent.futures.cpu_count())  # Default to optimal workers
    logging.info(f"Starting parallel task execution with {num_workers} workers.")

    results = {}
    retry_queue = {task: 0 for task in tasks}  # Track retry attempts

    while retry_queue:
        with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as executor:
            future_to_task = {executor.submit(execute_task, task): task for task in retry_queue}

            for future in concurrent.futures.as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    result = future.result()
                    if "failed" in result and retry_queue[task] < max_retries:
                        retry_queue[task] += 1
                        logging.warning(f"Retrying Task {task.task_id} (Attempt {retry_queue[task]}/{max_retries})")
                    else:
                        results[task.task_id] = result
                        retry_queue.pop(task, None)  # Remove successful task from retry queue
                except Exception as e:
                    logging.error(f"Unhandled error in Task {task.task_id}: {e}")

    logging.info("All tasks completed.")
    return results

if __name__ == "__main__":
    # Sample list of tasks
    tasks = [Task(i, f"Data_{i}") for i in range(10)]

    # Execute tasks in parallel
    results = process_tasks_parallel(tasks)

    # Print Results
    for task_id, res in results.items():
        print(f"Task {task_id}: {res}")
