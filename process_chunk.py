import multiprocessing
import os
import mmap
from typing import List, Dict, Tuple
from dataclasses import dataclass

@dataclass
class City:
    min : float
    max : float
    sum : float
    count : int

# Path to the file containing measurements
file_path = "../1brc/measurements.txt"

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

def process_chunk(chunk_start: int, chunk_end: int) -> Dict[bytes, City]:
    """
    Process a chunk of the file and compute min, max, sum, and count of measurements for each location.
    Args:
        chunk_start (int): The start position of the chunk.
        chunk_end (int): The end position of the chunk.
    Returns:
        Dict[bytes, City]: A dictionary with location as key and a City class with min, max, sum, and count as value.
    """
    chunk_size = chunk_end - chunk_start
    print("Start:", chunk_start, " End:", chunk_end)
    with open(file_path, "r+b") as file:
        mm = mmap.mmap(file.fileno(), length=chunk_size, access=mmap.ACCESS_READ, offset=chunk_start)
        if chunk_start != 0:
            next_line(0, mm)
        result : Dict[bytes, City] = dict()
        for line in iter(mm.readline, b""):
            location, temp_str = line.split(b";")
            measurement = float(temp_str)
            if location not in result:
                result[location] = City(measurement, measurement, measurement, 1)  # min, max, sum, count
            else:
                _result = result[location]
                if measurement < _result.min:
                    _result.min = measurement
                if measurement > _result.max:
                    _result.max = measurement
                _result.sum += measurement
                _result.count += 1
        mm.close()
        return result
    
def identify_chunks(num_processes: int) -> List[Tuple[int, int]]:
    """
    Identify chunks of the file to be processed by different processes.
    Args:
        num_processes (int): The number of processes to use.
    Returns:
        List[Tuple[int, int]]: A list of tuples where each tuple contains the start and end positions of a chunk.
    """
    chunk_results = []
    with open(file_path, "r+b") as file:
        mm = mmap.mmap(file.fileno(), 0, flags=mmap.MAP_PRIVATE, prot=mmap.PROT_READ)
        file_size = os.path.getsize(file_path)
        print("File Size:", file_size)
        print("Alloc Boundary:", mmap.ALLOCATIONGRANULARITY)
        chunk_size = file_size // num_processes
        chunk_size += mmap.ALLOCATIONGRANULARITY - (chunk_size % mmap.ALLOCATIONGRANULARITY)
        start = 0
        while start < file_size:
            end = start + chunk_size
            if end < file_size:
                end = next_line(end, mm)
            if start == end:
                end = next_line(end, mm)
            if end > file_size:
                end = file_size
                chunk_results.append((start, end))
                break
            chunk_results.append((start, end))
            start += chunk_size
        mm.close()
        return chunk_results

def perform_op() -> None:
    num_processes = os.cpu_count() or 1
    chunk_results = identify_chunks(num_processes)
    with multiprocessing.Pool(num_processes) as pool:
        ret_dicts = pool.starmap(process_chunk, chunk_results)
    shared_results : Dict[bytes, City] = dict()
    for return_dict in ret_dicts:
        for station, data in return_dict.items():
            if station in shared_results:
                _result = shared_results[station]
                _result = shared_results[station]
                if data.min < _result.min:
                    _result.min = data.min
                if data.max > _result.max:
                    _result.max = data.max
                _result.sum += data.sum
                _result.count += data.count
            else:
                shared_results[station] = data
    print("{", end="")
    for location, measurements in sorted(shared_results.items()):
        print(
            f"{location.decode('utf8')}={measurements.min:.1f}/{(measurements.sum / measurements.count) if measurements.count != 0 else 0:.1f}/{measurements.max:.1f}",
            end=", ",
        )
    print("\b\b} ")
