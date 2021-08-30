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
