import copy
import numpy as np
import cv2
import time

# This class is a utility class for creating, distributing.
class DataManager:
    def __init__(self, num_elements, num_chunks, max_num):
        self.arr = np.random.randint(0, max_num, num_elements)
        self.num_chunks = num_chunks
        self.chunk_size = int(np.round(len(self.arr) / num_chunks))
        self.index = 0
        self.chunk_num = 1
        self.sorted_arr = []

    # This method will give you the next chunk of data.
    def get_next_chunk(self):
        if self.chunk_num < self.num_chunks:
            arr_to_send = self.arr[self.index:self.index + self.chunk_size]
            self.index += self.chunk_size
        elif self.chunk_num == self.num_chunks:
            arr_to_send = self.arr[self.index:len(self.arr)]
            self.index = len(self.arr)
        else:
            arr_to_send = []
            return arr_to_send, -1

        self.chunk_num += 1
        return arr_to_send, (self.chunk_num - 1)


# This class is for sorting a given array of integers.. The sorting algorithm is Merge Sort.
class Sorting:
    def sort_arr(self,unsorted_arr):
        self.arr = unsorted_arr
        self.size = len(unsorted_arr)
        self.merge_sort(0, self.size - 1)
        return self.arr

    # This method is used for merging
    def merge(self, l, m, r):
        left_most = m - l + 1
        right_most = r - m
        l_arr = copy.deepcopy(self.arr[l:l + left_most])
        r_arr = copy.deepcopy(self.arr[m+1:m+1+right_most])

        i = 0; j = 0; k = l
        while i < left_most and j < right_most :
            if l_arr[i] <= r_arr[j]:
                self.arr[k] = l_arr[i]
                i += 1
            else:
                self.arr[k] = r_arr[j]
                j += 1
            k += 1

        while i < left_most:
            self.arr[k] = l_arr[i]
            i += 1; k += 1

        while j < right_most:
            self.arr[k] = r_arr[j]
            j += 1; k += 1

    # This method is used for splitting the array into left and right sub-lists.
    def merge_sort(self, l, r):
        if l < r:
            m = (l+(r-1))//2
            self.merge_sort(l, m)
            self.merge_sort(m+1, r)
            self.merge(l, m, r)
        return


# This class is used to merge two lists..
class Merger:
    def __init__(self, arr):
        self.sorted_arr = arr

    # This method will merge new received array into the current array.
    def merge_two_lists(self, recv_arr):
        new_arr = []
        i = 0;
        j = 0;

        if len(self.sorted_arr) == 0:
            self.sorted_arr = recv_arr
            return

        while i < len(self.sorted_arr) and j < len(recv_arr):
            if self.sorted_arr[i] <= recv_arr[j]:
                new_arr += [self.sorted_arr[i]]
                i += 1
            else:
                new_arr += [recv_arr[j]]
                j += 1

        while i < len(self.sorted_arr):
            new_arr += [self.sorted_arr[i]]
            i += 1;

        while j < len(recv_arr):
            new_arr += [recv_arr[j]]
            j += 1;

        self.sorted_arr = new_arr


# This is a simple class to resemble a node in the graph, will be used for drawing..
class Node:
    def __init__(self, x_pos, y_pos, text, parent,distance_from_root):
        self.x_pos = x_pos
        self.y_pos = y_pos
        self.text = text
        self.parent = parent
        self.distance_from_root = distance_from_root


# This class is used for drawing the dependency graph, show the interaction between nodes.
class DrawGraph:
    def __init__(self, height, width, start):
        self.height = height
        self.width = width
        self.tree_nodes = {}
        self.start_time = start

    # This function will draw a node in the graph.
    def draw_node(self, image,x_pos, y_pos, text, color=(0, 0, 255)):
        radius = 20
        thickness = 4
        image = cv2.circle(image, (x_pos,y_pos), radius, color, thickness)
        cv2.putText(image, text, (x_pos-15,y_pos+7), cv2.FONT_HERSHEY_DUPLEX, 0.75, (0, 0, 0), 1, cv2.LINE_AA)

    # This function will draw the set of leaf nodes in the graph.
    def draw_leaf(self, image, node_mapping, color_mapping, y_shift, distance_from_root):
        tmp = []
        idx = 0
        for key in node_mapping.keys():
            x_pos = idx * int(self.width / (len(node_mapping) + 1)) + 60
            y_pos = y_shift
            node = Node(x_pos, y_pos, key, node_mapping[key],distance_from_root)
            tmp += [node]
            self.draw_node(image, x_pos, y_pos, key, color_mapping[key])
            idx += 1

        self.tree_nodes[distance_from_root] = tmp

    # this function will draw nodes in particular layer of the graph layer by layer until reaching the root
    def draw_level(self, image, node_mapping, color_mapping, y_shift, distance_from_root):
        tmp = []
        for key in node_mapping.keys():
            left_most = 10000; right_most = 0;

            for node in self.tree_nodes[distance_from_root+1]:
                if node.parent == key:
                    if node.x_pos < left_most:
                        left_most = node.x_pos
                    if node.x_pos > right_most:
                        right_most = node.x_pos

            x_pos = left_most + int((right_most-left_most) / 2)
            y_pos = y_shift
            node = Node(x_pos, y_pos, key, node_mapping[key],distance_from_root)
            tmp += [node]
            self.draw_node(image, x_pos, y_pos, key, color_mapping[key])

        self.tree_nodes[distance_from_root] = tmp

    # This function will draw the connection between nodes and their parents
    def draw_connections(self, image):
        for level in self.tree_nodes:
            if level == 0:
                continue

            for node in self.tree_nodes[level]:
                for parent in self.tree_nodes[level-1]:
                    if parent.text == node.parent:
                        image = cv2.line(image, (node.x_pos, node.y_pos-20), (parent.x_pos, parent.y_pos+20), (255, 0, 0), 1)
                        break

    # This function will draw the full graph layer by layer until reaching the root
    def draw_tree(self, dep_tree, color_mapping, current_time):
        y_shifts = [550,450,350,250,150,50]
        window_name = 'Dependency Graph'
        passed_time = round((current_time - self.start_time),2)

        image = np.ones((self.height, self.width, 3), np.uint8) * 255
        self.draw_leaf(image,dep_tree[0], color_mapping[0], y_shifts[0], (len(dep_tree)-1))
        for i in range(1,len(dep_tree)):
            self.draw_level(image, dep_tree[i], color_mapping[i], y_shifts[i],(len(dep_tree) - i - 1))

        self.draw_connections(image)

        text = f'Time Taken: {passed_time} Seconds'
        cv2.putText(image, text, (20, 20), cv2.FONT_HERSHEY_DUPLEX, 0.75, (0, 0, 0), 1, cv2.LINE_AA)

        # uncomment the next statement to see the graph realtime.
        # cv2.imshow(window_name, image)
        cv2.waitKey(300)
        return image

    # This function will simulate the full process via drawing the graph in different timestamps
    def simulate_process(self, dep_tree, color_mapping, logs, nprocs, num_elements):
        # This will create an object to store frames to build the video at the end.
        out = cv2.VideoWriter(f'binary_merge_sort_mpi_{nprocs}_{num_elements}.avi', cv2.VideoWriter_fourcc(*'DIVX'), 15, (self.width, self.height))

        next_event = 1000000000000
        # This will loop until all events have been simulated, green means process done.
        while True:
            for node in logs:
                for event in logs[node]:
                    time = logs[node][event]
                    if time < next_event:
                        next_event = time
                        level = event
                        node_id = node

            if next_event == 1000000000000:
                break

            # Convert this node to green to show that this process is done.
            color_mapping[int(level)][node_id] = (0, 255, 0)
            img = self.draw_tree(dep_tree, color_mapping, next_event)

            # store this image frame in the video writter, write it 20 times to slow down the playback speed.
            # but writting the frame onetime is enough, but the video will be fast.
            for i in range(20):
                out.write(img)
            next_event = 1000000000000
            logs[node_id][level] = 1000000000000

        # release the video writter object.
        out.release()
