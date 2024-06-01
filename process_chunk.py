import multiprocessing
import os
import mmap
from typing import List, Dict, Tuple
import struct

# Path to the file containing measurements
file_path = "measurements.txt"
def is_new_line(position: int, mm: mmap.mmap) -> bool:
    """
    Check if the given position in the memory-mapped file is the start of a new line.
    Args:
        position (int): The position to check.
        mm (mmap.mmap): The memory-mapped file object.
    Returns:
        bool: True if the position is the start of a new line, False otherwise.
    """
    if position == 0:
        return True
    else:
        mm.seek(position - 1)
        return mm.read(1) == b"\n"
    
def next_line(position: int, mm: mmap.mmap) -> int:
    """
    Move to the next line in the memory-mapped file from the given position.
    Args:
        position (int): The current position in the file.
        mm (mmap.mmap): The memory-mapped file object.
    Returns:
        int: The position of the start of the next line.
    """
    mm.seek(position)
    mm.readline()
    return mm.tell()

def process_chunk(chunk_start: int, chunk_end: int) -> Dict[bytes, List[float]]:
    """
    Process a chunk of the file and compute min, max, sum, and count of measurements for each location.
    Args:
        chunk_start (int): The start position of the chunk.
        chunk_end (int): The end position of the chunk.
    Returns:
        Dict[bytes, List[float]]: A dictionary with location as key and a list of min, max, sum, and count as value.
    """
    chunk_size = chunk_end - chunk_start
    print("Start:", chunk_start, " End:", chunk_end)
    with open(file_path, "r+b") as file:
        mm = mmap.mmap(file.fileno(), length=chunk_size, access=mmap.ACCESS_READ, offset=chunk_start)
        if chunk_start != 0:
            next_line(0, mm)
        result = dict()
        for line in iter(mm.readline, b""):
            location, temp_str = line.split(b";")
            measurement = float(temp_str)
            if location not in result:
                result[location] = [measurement, measurement, measurement, 1]  # min, max, sum, count
            else:
                _result = result[location]
                if measurement < _result[0]:
                    _result[0] = measurement
                if measurement > _result[1]:
                    _result[1] = measurement
                _result[2] += measurement
                _result[3] += 1
        mm.close()
        return result