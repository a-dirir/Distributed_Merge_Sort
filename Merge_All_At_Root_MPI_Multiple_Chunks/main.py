from mpi4py import MPI
from utils import *
import time
import sys

comm = MPI.COMM_WORLD
rank = comm.rank
nprocs = comm.size
status = MPI.Status()
num_elements = int(sys.argv[1])
chunks_to_workers_factor = int(sys.argv[2])

# class to store job metadata
class Job:
    def __init__(self, job_id, worker_id):
        self.job_id = job_id
        self.start_time = time.time()
        self.worker = worker_id
        self.status = False
        self.time_taken = 0

    def terminate_job(self):
        self.end_time = time.time()
        self.time_taken = self.end_time - self.start_time
        self.status = True

# class to store each worker jobs and data.
class Node:
    def __init__(self, worker_id):
        self.jobs = {}
        self.id = worker_id
        self.current_job = -1
        self.running_jobs = False

    def assign_job(self, job_id):
        if not self.running_jobs:
            self.jobs[str(job_id)] = Job(job_id,self.id)
            self.running_jobs = True
            self.current_job = job_id

    def finish_current_job(self):
        self.jobs[str(self.current_job)].terminate_job()
        self.running_jobs = False
        self.current_job = -1

if comm.rank == 0:
    start = time.time()
    max_num = 10000
    logs = []

    # This dictionary will hold data about each node.
    workers = {}
    for i in range(1, nprocs):
        workers[str(i)] = Node(i)

    # Instantiate a manager object to manage the data chunks, num of chunks = (nprocs - 1)
    data_manager = DataManager(num_elements, chunks_to_workers_factor*(nprocs - 1), max_num)
    # Merger will be used to merge received files.
    merger = Merger([])

    # Loop until all chunks sorted
    stop = False
    while not stop:
        recv_data = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
        tag = status.Get_tag()
        source = status.Get_source()

        # tag=0 means that a node is requesting for data..
        if tag == 0:
            arr_to_send, chunk_num = data_manager.get_next_chunk()
            if chunk_num != -1:
                comm.send(arr_to_send, dest=source, tag=1)
                workers[str(source)].assign_job(chunk_num)

        # tag=1 means that a node is sending sorted data..
        elif tag == 1:
            logs += [source]
            merger.merge_two_lists(recv_data)
            workers[str(source)].finish_current_job()

        if chunk_num != -1:
            continue

        # Break when all nodes has no more jobs.
        stop = True
        for i in range(1, nprocs):
            if workers[str(i)].running_jobs:
                stop = False
                break

    # This is to avoid any potential deadlock.
    for i in range(1, nprocs):
        comm.send('Done', dest=i, tag=2)

    final_data = merger.sorted_arr

    if len(final_data) == num_elements:
        print(logs)
        end = time.time()
        time_taken = end - start
        print(f'Time Taken is {time_taken}')

        with open('Statistics_Small_Chunks.csv', 'a') as f:
            f.write(f'{nprocs-1}, {num_elements}, {chunks_to_workers_factor}, {time_taken}\n')

else:
    # request chunks until there is no more chunks..
    while True:
        # Send a request for data message with a tag=0
        comm.send('Request For Data', dest=0, tag=0)
        recv_data = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
        tag = status.Get_tag()

        # tag=1 mean sorting order.
        if tag == 1:
            # Sort the array using merge sort, Send back the sorted array to the master node {node 0}
            sort = Sorting()
            data_sorted = sort.sort_arr(recv_data)
            comm.send(data_sorted, dest=0, tag=1)

        # tag=2 means no more chunk, terminate.
        elif tag == 2:
            break
