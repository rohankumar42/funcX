import time
import logging
from collections import defaultdict
from threading import Thread
from funcx.sdk.client import FuncXClient

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
logger.addHandler(ch)


class FuncXScheduler:
    def __init__(self, fxc, endpoints=None):
        self._fxc = fxc

        # List of all FuncX endpoints we can execute on
        self._endpoints = set(endpoints) or set()

        # Average times for each function on each endpoint
        # TODO: this info is too unrealistic to have
        self._runtimes = defaultdict(lambda: defaultdict(float))
        self._num_executions = defaultdict(lambda: defaultdict(int))

        # Track all pending tasks
        self._pending_tasks = {}
        self._results = {}

        # Start a thread to wait for results and record runtimes
        self._watchdog_sleep_time = 0.5  # in seconds
        self._watchdog_thread = Thread(target=self._wait_for_results)
        self._watchdog_thread.start()

    def add_endpoint(self, endpoint_id):
        self._endpoints.add(endpoint_id)

    def remove_endpoiint(self, endpoint_id):
        self._endpoints.discard(endpoint_id)

    def run(self, *args, function_id=None, asynchronous=False, **kwargs):
        endpoint_id = self._choose_best_endpoint(*args,
                                                 function_id=function_id,
                                                 **kwargs)
        info = {
            'time_sent': time.time(),
            'function_id': function_id,
            'endpoint_id': endpoint_id
        }

        task_id = self._fxc.run(*args, function_id=function_id,
                                endpoint_id=endpoint_id,
                                asynchronous=asynchronous, **kwargs)

        logger.debug('Sent function {} to endpoint {} with task_id {}'
                     .format(function_id, endpoint_id, task_id))

        self._pending_tasks[task_id] = info
        return task_id

    def get_result(self, task_id, block=False):
        if task_id not in self._pending_tasks and task_id not in self._results:
            raise ValueError('Unknown task id {}'.format(task_id))

        if block:
            while task_id not in self._results:
                continue

        if task_id in self._results:
            res = self._results[task_id]
            del self._results[task_id]
            return res
        else:
            raise Exception("Task pending")

    def _choose_best_endpoint(self, *args, function_id, **kwargs):
        # TODO: actually choose something smartly
        import random
        return random.choice(list(self._endpoints))

    def _wait_for_results(self):

        while True:
            to_delete = set()
            # Convert to list first because otherwise, the dict may throw an
            # exception that its size has changed during iteration. This can
            # happen when new pending tasks are added to the dict.
            for task_id, info in list(self._pending_tasks.items()):
                try:
                    res = self._fxc.get_result(task_id)
                except Exception as e:
                    if not str(e).startswith("Task pending"):
                        logger.warn('Got unexpected exception:\t{}'.format(e))
                        raise
                    continue

                logger.debug('[WATCHDOG] Got result for task {}'
                             .format(task_id))
                self._update_average(task_id)
                self._results[task_id] = res
                to_delete.add(task_id)

                # Sleep, to prevent being throttled
                time.sleep(self._watchdog_sleep_time)

            for task_id in to_delete:
                del self._pending_tasks[task_id]

    def _update_average(self, task_id):
        info = self._pending_tasks[task_id]
        new_time = time.time() - info['time_sent']
        function_id = info['function_id']
        endpoint_id = info['endpoint_id']

        num_times = self._num_executions[function_id][endpoint_id]
        old_avg = self._runtimes[function_id][endpoint_id]
        new_avg = (old_avg * num_times + new_time) / (num_times + 1)
        self._runtimes[function_id][endpoint_id] = new_avg
        self._num_executions[function_id][endpoint_id] += 1


def funcx_sum(items):
    return sum(items)


if __name__ == "__main__":
    fxc = FuncXClient()
    func_uuid = fxc.register_function(funcx_sum,
                                      description="A sum function")
    payload = [1, 2, 3, 4, 66]

    endpoints = [
        # add your own funcx endpoints
    ]

    sched = FuncXScheduler(fxc, endpoints=endpoints)

    task_ids = []
    for i in range(10):
        task_ids.append(sched.run(payload, function_id=func_uuid))

    for task_id in task_ids:
        res = sched.get_result(task_id, block=True)
        print('Got res:', res)
