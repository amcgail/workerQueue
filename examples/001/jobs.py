from common import *
from pymongo import MongoClient

logger = getLogger('worker')
logger.setLevel('INFO')

# and add a stremhandler
from logging import StreamHandler
sh = StreamHandler()
logger.addHandler(sh)

from time import sleep

class j:
    class A(Job):
        def run(self):
            print('A')
            sleep(1)
            return 'a_result'

    class B(Job):
        def run(self, a):
            print('B', a)
            sleep(1)
            print('B done')
            return 'b_result'

    class C(Job):
        def run(self, a, b):
            print('C', a, b)
            sleep(1)
            print('C done')

    class prequel(Job):
        def run(self):
            print('prequel')
            sleep(1)
            return 'prequel_result'

from workerQueue import context
context.db = MongoClient()['test']['jobs']
context.jobs = j

from multiprocessing import freeze_support

if __name__ == '__main__':
    freeze_support()

    context.db.drop()

    if True:
        a = j.A()
        c = j.C(a=a, b=j.B(a=a))

        preqs = [j.prequel() for _ in range(150)]
        [p.then(a) for p in preqs]

    from workerQueue import start_workers
    start_workers(15)