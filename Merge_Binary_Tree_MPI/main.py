from mpi4py import MPI
from utils import *
import time
import sys
import copy

comm = MPI.COMM_WORLD
rank = comm.rank
nprocs = comm.size
status = MPI.Status()

# append zero in front of the id is it is less than 10, used for drawing the graph.
if rank < 10:
    node_id = f'0{rank}'
else:
    node_id = f'{rank}'

# build dependency tree, and color mapping of the tree, red= "Not Finished", gree= 'Finished
dependency_tree = BuildDependencyGraph().build_dep_tree(nprocs - 1)
color_mapping = copy.deepcopy(dependency_tree)
# initialize all nodes to red..
for level in color_mapping:
    for key in level.keys():
        level[key] = (0,0,255)

# Get the number of iteration, ex: for 16 nodes, we will have 4 iterations before reaching the root.
max_iterations = len(dependency_tree)
current_itr = 0


if comm.rank == 0:
    start = time.time()
    num_elements = int(sys.argv[1])
    max_num = 10000

    # Instantiate a manager object to manage the data chunks, num of chunks = (nprocs - 1)
    data_manager_master = DataManager(num_elements, (nprocs - 1), max_num)

    # Send the data chunks into different nodes.
    for i in range(1, nprocs):
        arr_to_send, chunk_num = data_manager_master.get_next_chunk()
        comm.send(arr_to_send, dest=i, tag=2)

    # this log will store all event with their timestamp.
    logs = {}

    # Loop until you receive all chunks.
    stop = False
    while not stop:
        recv_data = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
        tag = status.Get_tag()
        source = status.Get_source()

        # tag=0 means node 1 have sent the fully sorted data.
        if tag == 0:
            end = time.time()
            time_taken = end - start
            stop = True
            final_data = recv_data
        # get the time taken by each node
        if tag == 1:
            if source < 10:
                logs[f'0{source}'] = recv_data
            else:
                logs[f'{source}'] = recv_data

        if len(logs) != (nprocs-1):
            stop = False

    # make sure sorting of all elements
    if len(final_data) == num_elements:
        # Fetch how much time node 1 taken, the last value in the log, since node 1 will have the full sorted list.
        index = len(logs['01']) - 1
        time_taken_node_1 = logs['01'][f'{index}'] - start

        end = time.time()
        time_taken = end - start

        print(f'Time Taken is Node_1:{time_taken_node_1}, Node_0:{time_taken}')

        with open('Statistics_MPI_Tree.csv', 'a') as f:
            f.write(f'{nprocs-1}, {num_elements} , {time_taken_node_1}, {time_taken}\n')

    drawer = DrawGraph(600, 1400, start)
    drawer.simulate_process(dependency_tree, color_mapping, logs, nprocs, num_elements)

else:
    # this log will record the time used by each node, sned it to the root.
    logs = {}

    # Receive data to be sorted from the master node
    data = comm.recv(source=0, tag=2)
    sort = Sorting()
    data_sorted = sort.sort_arr(data)
    # instantiate a merger to merger sorted list received from other nodes based on dependency graph.
    merger = Merger(data_sorted)

    # This will loop until the node has no more responsibility of sending sorted data
    # ex: after the first iteration, half of the nodes will proceed to the next iteration, other will terminate.
    while current_itr < max_iterations:
        # Get who I have to send to in this current iteration.
        node_to_send = dependency_tree[current_itr][node_id]
        # Send the sorted data if the receiving end is not myself. Then terminate..
        if node_to_send != node_id:
            dest = int(node_to_send)
            logs[f'{current_itr}'] = time.time()
            comm.send(logs, dest=0, tag=1)
            # Send my sorted array to the predefined destination.
            comm.send(merger.sorted_arr, dest=dest, tag=0)
            break
        elif node_to_send == node_id:
            # Wait for other node to send its sorted data since it is my duty to merge in this iteration..
            nodes_to_wait_for = []
            # Get nodes that have to send to me other than myself..
            for node in dependency_tree[current_itr].keys():
                if dependency_tree[current_itr][node] == node_id and node != node_id:
                    nodes_to_wait_for += [int(node)]

            # Receive data from nodes. Merge this data into the current sorted list.
            recv_nodes = []
            while len(recv_nodes) != len(nodes_to_wait_for):
                data = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
                tag = status.Get_tag()
                source = status.Get_source()
                merger.merge_two_lists(data)
                recv_nodes += [source]

            logs[f'{current_itr}'] = time.time()
            current_itr += 1
