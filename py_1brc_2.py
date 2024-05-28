import multiprocessing.managers
import os
import mmap
import multiprocessing
from collections import defaultdict
from typing import DefaultDict, Tuple, Dict, List, Set
import line_profiler

@line_profiler.profile
def process_chunk(chunk_start: int, chunk_size: int, return_dict: multiprocessing.managers.DictProxy) -> None:
    with open("measurements_100mil.txt", "r+b") as file:
        mm = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ)
        chunk_end = chunk_start + chunk_size

        #station_data: DefaultDict[str, List[float]] = defaultdict(lambda: [200.0, -200.0, 0.0, 0])

        result = dict()

        for i, line in enumerate(iter(mm.readline, b""), chunk_start):
            if i >= chunk_end:
                break

            location, temp_str = line.split(b";")
            measurement = float(temp_str)

            if location not in result:
                result[location] = [
                    measurement,
                    measurement,
                    measurement,
                    1,
                ]  # min, max, sum, count
            else:
                _result = result[location]
                if measurement < _result[0]:
                    _result[0] = measurement
                if measurement > _result[1]:
                    _result[1] = measurement
                _result[2] += measurement
                _result[3] += 1


        for station, data in result.items():
            return_dict[station] = data
            # if station in return_dict:
            #     existing_data = return_dict[station]
            #     return_dict[station] = (
            #         min(existing_data[0], data[0]),
            #         (existing_data[2] + data[2]) / (existing_data[3] + data[3]),
            #         max(existing_data[1], data[1])
            #     )
            # else:
            #     return_dict[station] = (data[0], data[2] / data[3], data[1])

@line_profiler.profile
def main() -> None:
    num_processes = os.cpu_count() or 1
    # file_path = "measurements.txt"
    # total_lines = count_lines(file_path)
    chunk_size = 100000000 // num_processes
    #chunk_size = 100_000_000  # 1 million rows per chunk
    manager = multiprocessing.Manager()
    processes: List[multiprocessing.Process] = []
    ret_dicts : List[multiprocessing.managers.DictProxy] = []

    for i in range(0, 100000000, chunk_size):
        return_dict = manager.dict()
        ret_dicts.append(return_dict)
        p = multiprocessing.Process(target=process_chunk, args=(i, chunk_size, return_dict))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    shared_results = dict()
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

    # sorted_stations: Set[str] = sorted(shared_results.keys())
    # for station in sorted_stations:
    #     min_temp, max_temp, total_temp,count = shared_results[station]
    #     mean_temp = total_temp / count
    #     print(f"{station};{min_temp:.1f}/{mean_temp:.1f}/{max_temp:.1f}")

    print("{", end="")
    for location, measurements in sorted(shared_results.items()):
        print(
            f"{location.decode('utf8')}={measurements[0]:.1f}/{(measurements[2] / measurements[3]) if measurements[3] !=0 else 0:.1f}/{measurements[1]:.1f}",
            end=", ",
        )
    print("\b\b} ")

if __name__ == "__main__":
    main()