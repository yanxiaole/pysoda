import time

class RDD:
    def __init__(self, sc):
        self._sc = sc

    def map(self, func):
        self._sc.map(func)
        return RDD(self._sc)

    def flatmap(self, func):
        self._sc.flatmap(func)
        return RDD(self._sc)

    def count(self):
        results = self._sc.run()
        return len(results)

    def sum(self):
        results = self._sc.run()
        return sum(results)
