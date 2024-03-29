from queue import Queue
from threading import Thread
from typing import Callable
from ..flow import Timeslice, Save
from typing import Type

def load(database: str, tables: list, function: Callable, timeslice: Timeslice, save: Type[Save] = None, maxparallel:int = 4):
    """Prototype multi-threaded loader"""

    def _load(q):
        while True:
            table, function, timeslice, save = q.get()
            print(f"Loading {database}.{table}")
            results = function(timeslice=timeslice, table=table, save=save)
            if results["error"].get("count", 0) > 0:
                print(results)
            else:
                print(f"Loaded {database}.{table}")
            q.task_done()

    q = Queue(maxsize=0)

    for _ in range(maxparallel):
        worker = Thread(target=_load, args=(q,))
        worker.setDaemon(True)
        worker.start()

    for table in tables:
        q.put((table, function, timeslice, save))

    q.join()
