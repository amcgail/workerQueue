from .common import *
from .context import c

class Worker:
    def __init__(self):
        self.c = c

    def work(self):
        from .job import Job

        from importlib import import_module
        from time import sleep

        last_slept = False

        while True:
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