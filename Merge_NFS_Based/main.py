from utils import *
import time
import os
import numpy as np
import copy
import sys
import glob


node_id = int(sys.argv[1]) - 1
nprocs = int(sys.argv[2]) - 1
num_elements = int(sys.argv[3])

if node_id == 0:
    start = time.time()
    max_num = 10000

    # Instantiate a manager object to manage the data chunks, num of chunks = (nprocs - 1)
    data_manager = DataManager(num_elements,nprocs,max_num)
    # Merger will be used to merge all files
    merger = Merger([])

    # Create directory to save data into, if the directory exist, delete all files.
    if not os.path.exists('Unsorted_Files'):
        os.mkdir('Unsorted_Files')
    else:
        files = glob.glob('Unsorted_Files/*')
        for f in files:
            os.remove(f)

    if not os.path.exists('Sorted_Files'):
        os.mkdir('Sorted_Files')
    else:
        files = glob.glob('Sorted_Files/*')
        for f in files:
            os.remove(f)

    # Save this chunk with node id. do it for all nodes.
    is_node_done_sorting = []
    for i in range(1,(nprocs+1)):
        arr_to_save, chunk_num = data_manager.get_next_chunk()
        is_node_done_sorting += [False]
        np.save(f'Unsorted_Files/n_{chunk_num}.npy', np.array(arr_to_save))

    end_distribution = time.time()

    # Wait until all slave nodes finish sorting
    is_not_done = True
    while is_not_done:
        is_not_done = False
        for i in range(1,(nprocs+1)):
            if os.path.exists(f'Sorted_Files/n_{i}.npy'):
                if is_node_done_sorting[i-1] != True:
                    is_node_done_sorting[i-1] = True
                    data = np.load(f'Sorted_Files/n_{i}.npy')
                    merger.merge_two_lists(data)
            else:
                is_not_done = True

    
    end_sorting = time.time()

    time_taken_distribution = end_distribution - start
    time_taken_sorting = end_sorting - end_distribution

    print(f'Time Taken is Ditribution:{time_taken_distribution}, Sorting:{time_taken_sorting}')

    with open('Statistics_NFS.csv', 'a') as f:
        f.write(f'{nprocs}, {num_elements} ,{time_taken_distribution}, {time_taken_sorting}\n')
    
else:
    # Wait until the master node finishes from creating data chunk..
    while True:
        if len(os.listdir('Unsorted_Files')) == nprocs:
            break

    # Open the unsorted file corresponding to this node, sometimes there is delay even
    # after the master node finishes from creating data chunk, thus, I have used try:except to catch file open errors.
    while True:
        try:
            unsorted_data = np.load(f'Unsorted_Files/n_{node_id}.npy')
            break
        except:
            continue

    # Sort data, and save the sorted array in the corresponding directory..
    sort = Sorting()
    sorted_data = sort.sort_arr(copy.deepcopy(unsorted_data))
    np.save(f'Sorted_Files/n_{node_id}.npy', sorted_data)
