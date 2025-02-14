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
        if random.random() < 0.2:  # Simulating occasional failures (20% chance)
            raise Exception(f"Simulated failure in Task {task.task_id}")
        result = f"Task {task.task_id} processed successfully"
        logging.info(result)
        return result
    except Exception as e:
        logging.error(f"Error processing Task {task.task_id}: {e}")
        return None  # Indicate failure

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
    retry_count = {task.task_id: 0 for task in tasks}  # Track retries per task

    while tasks:
        failed_tasks = []  # Collect failed tasks for retry

        with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as executor:
            future_to_task = {executor.submit(execute_task, task): task for task in tasks}

            for future in concurrent.futures.as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    result = future.result()
                    if result:
                        results[task.task_id] = result  # Store successful task result
                    else:
                        if retry_count[task.task_id] < max_retries:
                            retry_count[task.task_id] += 1
                            logging.warning(f"Retrying Task {task.task_id} (Attempt {retry_count[task.task_id]}/{max_retries})")
                            failed_tasks.append(task)
                        else:
                            results[task.task_id] = f"Task {task.task_id} failed after {max_retries} attempts"
                except Exception as e:
                    logging.error(f"Unhandled error in Task {task.task_id}: {e}")

        tasks = failed_tasks  # Retry only failed tasks in the next loop

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
