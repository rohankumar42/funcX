import time
import logging
from collections import defaultdict
from threading import Thread
from funcx.sdk.client import FuncXClient

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
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
        self._watchdog_sleep_time = 0.1  # in seconds
        self._watchdog_thread = Thread(target=self._wait_for_results)
        self._watchdog_thread.start()

    def add_endpoint(self, endpoint_id):
        self._endpoints.add(endpoint_id)

    def remove_endpoiint(self, endpoint_id):
        self._endpoints.discard(endpoint_id)

    def register_function(self, function, *args, **kwargs):
        return NotImplemented
        # wrapped_function = time_function(function)
        # return self._fxc.register_function(wrapped_function, *args, **kwargs)

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

        logger.info('[WATCHDOG] Thread started')

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
                        logger.warn('[WATCHDOG] Got unexpected exception:\t{}'
                                    .format(e))
                        raise
                    continue

                logger.debug('[WATCHDOG] Got result for task {} with runtime {}'
                             .format(task_id, res['runtime']))
                self._update_average(task_id, new_time=res['runtime'])
                self._results[task_id] = res['result']
                to_delete.add(task_id)

                # Sleep, to prevent being throttled
                time.sleep(self._watchdog_sleep_time)

            for task_id in to_delete:
                del self._pending_tasks[task_id]

    def _update_average(self, task_id, new_time):
        info = self._pending_tasks[task_id]
        function_id = info['function_id']
        endpoint_id = info['endpoint_id']

        num_times = self._num_executions[function_id][endpoint_id]
        old_avg = self._runtimes[function_id][endpoint_id]
        new_avg = (old_avg * num_times + new_time) / (num_times + 1)
        self._runtimes[function_id][endpoint_id] = new_avg
        self._num_executions[function_id][endpoint_id] += 1


def funcx_sum(items):
    import time
    start = time.time()
    result = sum(items)
    return {'result': result, 'runtime': time.time() - start}


if __name__ == "__main__":

    endpoints = [
        # add your own funcx endpoints
        '4b116d3c-1703-4f8f-9f6f-39921e5864df',  # public tutorial endpoint
    ]

    fxc = FuncXClient()
    sched = FuncXScheduler(fxc, endpoints=endpoints)
    func_uuid = fxc.register_function(funcx_sum, description="Summer")
    payload = [2, 34]

    task_ids = []
    for i in range(1):
        task_id = sched.run(payload, function_id=func_uuid)
        task_ids.append(task_id)
        print('task_id:', task_id)

    for task_id in task_ids:
        res = sched.get_result(task_id, block=True)
        print('Got res:', res)
