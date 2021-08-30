from mpi4py import MPI
from utils import *
import time
import sys
import copy

comm = MPI.COMM_WORLD
rank = comm.rank
nprocs = comm.size
status = MPI.Status()

if comm.rank == 0:
    start = time.time()
    num_elements = int(sys.argv[1])
    max_num = 10000

    # Instantiate a manager object to manage the data chunks, num of chunks = (nprocs - 1)
    data_manager = DataManager(num_elements, (nprocs - 1), max_num)
    merger = Merger([])

    # send data chunks to nodes
    for i in range(1, nprocs):
        arr_to_send, chunk_num = data_manager.get_next_chunk()
        comm.send(arr_to_send, dest=i, tag=0)


    num_chunks = data_manager.num_chunks
    recv_chunks = 0

    # this log will store all event with their timestamp.
    logs = {}

    # Loop until you receive all chunks.
    while recv_chunks < num_chunks:
        recv_data = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
        tag = status.Get_tag()
        source = status.Get_source()

        # tag=0 means  a node has sent its sorted data.
        if tag == 0:
            merger.merge_two_lists(recv_data)
            recv_chunks += 1
            if source < 10:
                logs[f'0{source}'] = {'0':time.time()}
            else:
                logs[f'{source}'] = {'0':time.time()}


    # send terminate message to avoid any deadlock.
    for i in range(1, nprocs):
        comm.send([], dest=i, tag=1)

    # Add the root timestamp.
    logs[f'00'] = {'1': time.time()}

    # Get the final sorted array.
    final_data = merger.sorted_arr

    # make sure sorting of all emelents
    if len(final_data) == num_elements:
        end = time.time()
        time_taken = end - start
        print(f'Time Taken is {time_taken}')

        # store results in excel sheet.
        with open('Statistics_MPI_Root.csv', 'a') as f:
            f.write(f'{nprocs-1}, {num_elements}, {time_taken}\n')

    dep_tree = []
    mapping = {}
    for i in range(1,num_chunks+1):
        if i < 10:
            mapping[f'0{i}'] = '00'
        else:
            mapping[f'{i}'] = '00'

    dep_tree.append(mapping)
    dep_tree.append({'00':'00'})

    color_mapping = copy.deepcopy(dep_tree)
    # initialize all nodes to red..
    for level in color_mapping:
        for key in level.keys():
            level[key] = (0, 0, 255)

    # simulate the full process
    drawer = DrawGraph(600, 1400, start)
    drawer.simulate_process(dep_tree, color_mapping, logs, nprocs, num_elements)


else:
    # receive data from root, sort it, and send it back.
    recv_data = comm.recv(source=0, tag=0, status=status)
    # Sort the array using merge sort
    sort = Sorting()
    data_sorted = sort.sort_arr(recv_data)
    # Send back the sorted array to the master node {node 0}
    comm.send(data_sorted, dest=0, tag=0)
