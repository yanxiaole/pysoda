from Executor import Executor
from TaskActor import TaskActor
from Collector import Collector
from Scheduler import Scheduler
from RDD import RDD, TextRDD
import time
import logging


class SparkContext:

    def __init__(self):
        self._executors = []
        self._num_executors = 0
        self._collector_ready = False
        self._res_list= []
        self._scheduler = Scheduler()
        self._logger = logging.getLogger("SparkContext")

    def read(self, path_list):
        return TextRDD(self, path_list)
    def text(self, path_list):
        if path_list is None or len(path_list) == 0:
            return None

        self._init_executors(len(path_list))
        for idx, path in enumerate(path_list):
            # self._executors[idx].tell({'type': 'map', 'func': open, 'para': path})
            # self._executors[idx].tell({'type': 'init', 'func': str, 'iter': iter([path, ])})
            self._executors[idx].tell({'type': 'init', 'func': open, 'para': path})
        return RDD(self)

    def map(self, func):
        for idx in range(self._num_executors):
            self._executors[idx].tell({'type': 'map', 'func': func})

    def flatmap(self, func):
        for idx in range(self._num_executors):
            self._executors[idx].tell({'type': 'flatmap', 'func': func})

    def run(self):
        self._collector_ready = False
        self._res_list = []
        collector_ref = Collector.start(self._num_executors, self)
        for idx in range(self._num_executors):
            self._executors[idx].tell({'type': 'run', 'collector': collector_ref})

        while not self._collector_ready:
            time.sleep(0.1)
        collector_ref.stop()
        return self._res_list


    def _init_executors(self, num):
        self._logger.info("init {num} executors".format_map(vars()))
        self._executors = [Executor.start() for _ in range(num)]
        self._num_executors = len(self._executors)

    def stop(self):
        self._logger.info("stopping all executors")
        [e.stop() for e in self._executors]

    def run_tasks(self, rdd):
        dag = self._scheduler.run(rdd)
        header = dag[0]
        n_tasks = header._n_split
        self._logger.info("init {num} task actors".format(num=n_tasks))
        task_actors = [TaskActor.start() for _ in range(n_tasks)]
        for idx, path in enumerate(rdd._split):
            task_actors[idx].tell({'type': 'init', 'func': rdd._func, 'para': path})
        return RDD(self)



if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',)
    sc = SparkContext()
    rdd = sc.text(['../src/RDD.py', '../src/SparkContext.py'])
    word_cnt = rdd \
        .flatmap(lambda line: line.split(' ')) \
        .count()
    print("word: {cnt}".format(cnt=word_cnt))

    '''
    cnt = rdd.map(lambda x: 1).count()
    print("cnt: {cnt}".format(cnt=cnt))
    '''

    sc.stop()
