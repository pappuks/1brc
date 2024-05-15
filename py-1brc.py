import multiprocessing.managers
import os
import mmap
import multiprocessing
from collections import defaultdict
from typing import DefaultDict, Tuple, Dict, List, Set

def process_chunk(chunk_start: int, chunk_size: int, shared_results: multiprocessing.managers.DictProxy) -> None:
    with open("measurements.txt", "r+b") as file:
        mm = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ)
        chunk_end = chunk_start + chunk_size

        station_data: DefaultDict[str, List[float]] = defaultdict(lambda: [float('inf'), -float('inf'), 0.0, 0])

        for i, line in enumerate(iter(mm.readline, b""), chunk_start):
            if i >= chunk_end:
                break

            station_name, temp_str = line.decode('utf-8').strip().split(';')
            temp = float(temp_str)

            station_data[station_name][0] = min(station_data[station_name][0], temp)  # Min
            station_data[station_name][1] = max(station_data[station_name][1], temp)  # Max
            station_data[station_name][2] += temp  # Sum
            station_data[station_name][3] += 1  # Count

        for station, data in station_data.items():
            #with shared_results. .get():
            shared_results[station] = (data[0], data[2] / data[3], data[1])

def main() -> None:
    num_processes = os.cpu_count() or 1
    chunk_size = int(1_000_000_000 / num_processes)  # 1 million rows per chunk
    #shared_results: Dict[str, Tuple[float, float, float]] = multiprocessing.Manager().dict()
    shared_results: multiprocessing.managers.DictProxy = multiprocessing.Manager().dict()

    processes: List[multiprocessing.Process] = []
    for i in range(0, 1_000_000_000, chunk_size):
        p = multiprocessing.Process(target=process_chunk, args=(i, chunk_size, shared_results))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    sorted_stations: List[str] = sorted(shared_results.keys())
    for station in sorted_stations:
        min_temp, mean_temp, max_temp = shared_results[station]
        print(f"{station};{min_temp:.1f}/{mean_temp:.1f}/{max_temp:.1f}")

if __name__ == "__main__":
    main()