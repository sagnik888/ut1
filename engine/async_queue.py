import threading
import queue
from typing import Callable, Any
from loguru import logger

class AsyncExecutionQueue:
    """
    Bounded background worker pool for broker API calls and heavy tasks.
    Simultaneous valid signals progress independently without unbounded threads.
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(AsyncExecutionQueue, cls).__new__(cls)
                cls._instance._init()
            return cls._instance

    def _init(self):
        self.task_queue = queue.Queue(maxsize=500)
        self.worker_count = 3
        self.worker_threads = []
        for index in range(self.worker_count):
            worker = threading.Thread(
                target=self._process_queue,
                daemon=True,
                name=f"AsyncExecutionQueueWorker-{index + 1}",
            )
            worker.start()
            self.worker_threads.append(worker)
        self.worker_thread = self.worker_threads[0]
        logger.info(f"AsyncExecutionQueue initialized with {self.worker_count} workers.")

    def _process_queue(self):
        while True:
            try:
                task_name, func, args, kwargs = self.task_queue.get()
                logger.debug(f"Executing queued task: {task_name}")
                try:
                    func(*args, **kwargs)
                except Exception as e:
                    logger.error(f"❌ Error in AsyncExecutionQueue task '{task_name}': {e}")
                finally:
                    self.task_queue.task_done()
            except Exception as e:
                logger.error(f"AsyncExecutionQueue worker error: {e}")

    def submit(self, task_name: str, func: Callable, *args, **kwargs):
        """Submit a task to be executed asynchronously."""
        task = (task_name, func, args, kwargs)
        try:
            self.task_queue.put(task, block=False)
        except queue.Full:
            is_broker_execution = task_name.startswith(("PlaceOrder_", "CloseOrder_"))
            if is_broker_execution:
                logger.critical(f"⚠️ AsyncExecutionQueue full; waiting to enqueue broker task: {task_name}")
                self.task_queue.put(task, block=True)
            else:
                logger.warning(f"⚠️ AsyncExecutionQueue is full! Dropping non-critical task: {task_name}")

# Global singleton
execution_queue = AsyncExecutionQueue()
