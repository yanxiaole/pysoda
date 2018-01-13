import pykka
import logging
from itertools import chain


def flatmap(f, items):
    return chain.from_iterable(map(f, items))


class Executor(pykka.ThreadingActor):
    def __init__(self):
        super(Executor, self).__init__()
        self._start_para = None
        self._start_func = None
        self._func_list = []
        self._logger = logging.getLogger("Executor")

    def on_receive(self, message):
        if message.get('type') == 'init':
            self._start_func = message.get('func')
            self._start_para = message.get('para')
            self._logger.debug(self._func_list)
        elif message.get('type') == 'map' or message.get('type') == 'flatmap':
            self._func_list.append(message)
            self._logger.debug(self._func_list)
        elif message.get('type') == 'run':
            self._logger.debug(message)
            collector_ref = message.get('collector')
            res_iter = iter(self._start_func(self._start_para))

            for func_cnf in self._func_list:
                func = func_cnf.get('func')
                if func_cnf.get('type') == 'map':
                    res_iter = map(func, res_iter)
                elif func_cnf.get('type') == 'flatmap':
                    res_iter = flatmap(func, res_iter)

            for res in res_iter:
                collector_ref.tell({'task': 'collect', 'res': res})
            collector_ref.tell({'task': 'done'})

