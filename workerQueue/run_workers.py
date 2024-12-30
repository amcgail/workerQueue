from .worker import Worker

from multiprocessing import Process
from time import sleep

def work():
    w = Worker()
    w.work()

# Create and manage workers in separate processes
def start_workers(num_workers):
    processes = []

    # Create processes for each worker
    for i in range(num_workers):
        process = Process(target=work)
        processes.append(process)
        process.start()

        sleep(0.25)

    # Wait for all workers to finish
    for process in processes:
        process.join()