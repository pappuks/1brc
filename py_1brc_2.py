import multiprocessing.managers
import os
import mmap
import multiprocessing
from collections import defaultdict
from typing import DefaultDict, Tuple, Dict, List, Set

def process_chunk(chunk_start: int, chunk_size: int, return_dict: multiprocessing.managers.DictProxy) -> None:
    with open("../1brc/measurements.txt", "r+b") as file:
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
            return_dict[station] = (data[0], data[2] / data[3], data[1])
            # if station in return_dict:
            #     existing_data = return_dict[station]
            #     return_dict[station] = (
            #         min(existing_data[0], data[0]),
            #         (existing_data[2] + data[2]) / (existing_data[3] + data[3]),
            #         max(existing_data[1], data[1])
            #     )
            # else:
            #     return_dict[station] = (data[0], data[2] / data[3], data[1])

def main() -> None:
    num_processes = os.cpu_count() or 1
    chunk_size = 100_000_000  # 1 million rows per chunk
    manager = multiprocessing.Manager()
    processes: List[multiprocessing.Process] = []
    ret_dicts : List[multiprocessing.managers.DictProxy] = []

    for i in range(0, 1_000_000_000, chunk_size):
        return_dict = manager.dict()
        ret_dicts.append(return_dict)
        p = multiprocessing.Process(target=process_chunk, args=(i, chunk_size, return_dict))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    shared_results: Dict[str, Tuple[float, float, float]] = manager.dict()
    for return_dict in ret_dicts:
        for station, data in return_dict.items():
            if station in shared_results:
                existing_data = shared_results[station]
                shared_results[station] = (
                    min(existing_data[0], data[0]),
                    (existing_data[1] + data[1]) / 2,
                    max(existing_data[2], data[2])
                )
            else:
                shared_results[station] = data

    sorted_stations: Set[str] = sorted(shared_results.keys())
    for station in sorted_stations:
        min_temp, mean_temp, max_temp = shared_results[station]
        print(f"{station};{min_temp:.1f}/{mean_temp:.1f}/{max_temp:.1f}")

if __name__ == "__main__":
    main()