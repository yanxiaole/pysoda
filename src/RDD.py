from abc import ABCMeta, abstractmethod
from itertools import chain


class RDD(metaclass=ABCMeta):
    def __init__(self, sc):
        self._sc = sc
        self._split = None

    def map(self, func):
        return MapRDD(self._sc, func)

    def flatmap(self, func):
        return FlatMapRDD(self._sc, func)

    def count(self):
        results = self.run()
        return len(results)

    def sum(self):
        results = self.run()
        return sum(results)

    @abstractmethod
    def run(self):
        return []


class TextRDD(RDD):
    def __init__(self, sc, path_list):
        super(TextRDD, self).__init__(sc)
        self._func = open
        self._split = path_list
        self._n_split = len(path_list)

    def run(self):
        # sc generate executors
        # sc send tasks
        return self._sc.run_tasks(self, self._n_split)


class MapRDD(RDD):
    def __init__(self, sc, func):
        super(MapRDD, self).__init__(sc)
        self._func = func

    def run(self):
        return map(self._func, self._split)


class FlatMapRDD(RDD):
    def __init__(self, sc, func):
        super(FlatMapRDD, self).__init__(sc)
        self._func = func

    def run(self):
        return self._sc.run_tasks(self)
