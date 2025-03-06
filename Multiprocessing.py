import multiprocessing
import time
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class Executor:
    def __init__(self, num_workers=None, max_retries=3):
        """
        Initialize the Executor class.
        
        :param num_workers: Number of worker processes (defaults to CPU count).
        :param max_retries: Maximum retries for failed tasks.
        """
        self.num_workers = num_workers or min(4, multiprocessing.cpu_count())  # Default workers
        self.max_retries = max_retries
        self.task_event = multiprocessing.Event()  # Signal for task execution

    def _execute_task(self, task):
        """
        Function to execute a task. Handles retries internally.

        :param task: Task object to execute.
        :return: Task result or error message.
        """
        attempt = 0
        while attempt <= self.max_retries:
            try:
                logging.info(f"Executing Task {task.task_id} (Attempt {attempt+1}/{self.max_retries}) on PID {os.getpid()}")
                result = task.run()  # Assuming each task has a `run()` method
                
                if result:
                    logging.info(f"Task {task.task_id} completed successfully.")
                    return {"task_id": task.task_id, "status": "success", "result": result}

                raise Exception(f"Task {task.task_id} returned None")  # Treat None as failure
            except Exception as e:
                logging.error(f"Task {task.task_id} failed on attempt {attempt+1}: {e}")
                attempt += 1
                time.sleep(2 ** attempt)  # Exponential backoff for retries

        logging.critical(f"Task {task.task_id} failed after {self.max_retries} retries.")
        return {"task_id": task.task_id, "status": "failed", "error": f"Task failed after {self.max_retries} retries"}

    def process_tasks_parallel(self, tasks):
        """
        Continuously processes streaming tasks in parallel.
        
        :param tasks: List of Task objects.
        """
        logging.info(f"Starting continuous parallel execution with {self.num_workers} workers.")

        with multiprocessing.Pool(processes=self.num_workers) as pool:
            self.task_event.set()  # Allow tasks to start
            
            while True:  # Keep running indefinitely for streaming
                task_results = pool.map(self._execute_task, tasks)
                
                # Log failures but continue running other tasks
                for res in task_results:
                    if res["status"] == "failed":
                        logging.error(f"Task {res['task_id']} permanently failed: {res['error']}")

                time.sleep(5)  # Optional delay before next batch (adjust based on stream frequency)

    def start_execution(self, tasks):
        """
        Start task execution as a background process.

        :param tasks: List of Task objects.
        """
        process = multiprocessing.Process(target=self.process_tasks_parallel, args=(tasks,))
        process.daemon = True  # Ensures process runs in background without blocking
        process.start()
        logging.info("Executor started.")
