import multiprocessing
import os
import mmap
from typing import List, Dict, Tuple
import struct
from process_chunk import process_chunk, next_line, is_new_line

# Path to the file containing measurements
file_path = "measurements.txt"
    
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

def main() -> None:
    """Main function to process the file using multiple processes and print the results.
    """
    num_processes = os.cpu_count() or 1
    chunk_results = identify_chunks(num_processes)
    with multiprocessing.Pool(num_processes) as pool:
        ret_dicts = pool.starmap(process_chunk, chunk_results)
    shared_results : Dict[bytes, List[float]] = dict()
    for return_dict in ret_dicts:
        for station, data in return_dict.items():
            if station in shared_results:
                _result = shared_results[station]
                if data[0] < _result[0]:
                    _result[0] = data[0]
                if data[1] > _result[1]:
                    _result[1] = data[1]
                _result[2] += data[2]
                _result[3] += data[3]
            else:
                shared_results[station] = data
    print("{", end="")
    for location, measurements in sorted(shared_results.items()):
        print(
            f"{location.decode('utf8')}={measurements[0]:.1f}/{(measurements[2] / measurements[3]) if measurements[3] != 0 else 0:.1f}/{measurements[1]:.1f}",
            end=", ",
        )
    print("\b\b} ")

if __name__ == "__main__":
    main()

