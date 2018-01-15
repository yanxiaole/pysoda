from TaskActor import TaskActor
import logging


class Scheduler():
    def __init__(self):
        self._logger = logging.getLogger("Scheduler")

    def run(self, rdd):
        dag = [rdd]
        while rdd._prev:
            dag.append(rdd._prev)
            rdd = rdd._prev

        header = dag[0]
        n_tasks = header._n_split
        self._logger.info("init {num} task actors".format(num=n_tasks))
        task_actors = [TaskActor.start() for _ in range(n_tasks)]
        for idx, path in enumerate(rdd._split):
            task_actors[idx].tell({'type': 'init', 'func': rdd._func, 'para': path})

