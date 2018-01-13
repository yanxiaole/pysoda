import pykka
import logging


class Collector(pykka.ThreadingActor):
    def __init__(self, num_task, sc):
        super(Collector, self).__init__()
        self._num_task = num_task
        self._num_task_done = 0
        self._logger = logging.getLogger("Collector")
        self._sc = sc

    def on_receive(self, message):
        if message.get('task') == 'collect' and not self._sc._collector_ready:
            self._sc._res_list.append(message.get('res'))
        elif message.get('task') == 'done' and not self._sc._collector_ready:
            self._num_task_done += 1
            if self._num_task_done == self._num_task:
                self._sc._collector_ready = True
                print(self._sc._res_list)

