from .common import *
from .context import c

class Worker:
    def __init__(self):
        self.c = c

    def work(self, monitor_dir=None):
        from .job import Job

        if monitor_dir is not None:
            # checking for changes
            from watchdog.observers import Observer
            from watchdog.events import FileSystemEventHandler

            class Handler(FileSystemEventHandler):
                def __init__(self):
                    super().__init__()

                    self.wishing_death = False

                def on_modified(self, event):
                    self.wishing_death = True
                
                def on_created(self, event):
                    self.wishing_death = True

                def on_deleted(self, event):
                    self.wishing_death = True

            observer = Observer()
            h = Handler()

            observer.schedule(h, monitor_dir, recursive=True)
            observer.start()


        from importlib import import_module
        from time import sleep

        last_slept = False

        while True:
            # check if we should exit
            if monitor_dir is not None and h.wishing_death:
                logger.info('Detected change, exiting.')
                break

            claim_id = dt.now()

            # find a task to work on
            # the requirements are either:
            # 1. ready is True
            # 2. ready is a dictionary, and all values are True

            _def = self.c.db.find_one_and_update(
                {
                    'completed': False,
                    'claimed': None,
                    '$or': [
                        {'ready': True},
                        {'trigger_ids': {'$exists': True, '$size': 0}},
                    ]
                },
                {'$set': {'claimed': claim_id}}
            )

            if _def is None:
                sleep(1)
                if not last_slept:
                    logger.info('Sleeping, no tasks available.')

                last_slept = True
                continue

            last_slept = False
            logger.info(f"Claimed task {_def['type']}: {_def['_id']}")

            J = self.c.load(_def['_id'])
            assert(isinstance(J, Job))

            result = J.run_wrapper()