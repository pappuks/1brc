# 1BRC in Python

I am a sucker for optimization. So when I heard about [1BRC](https://www.morling.dev/blog/one-billion-row-challenge/) I got intrigued and started experimenting with python implementations. **I achieved my goal and implemented the [fastest implementation](./py_1brc_final.py) running on CPython, without any external libraries .**

For my implementation I followed these principles:
- Python
    - Python is the primary language I use at my work. So I wanted to see how much I can push performance of native python app.
- CPython Interpreter
    - CPython remains the most popular interpreter with widest support, so I wanted to push for performance using this as this will also be translatable in day to day work.
- Don't use any 3rd party packages like DuckDb or Polars
    - Even though DuckDB and Polars based implementation is highly suited and also the fastest, my aim was to stick to native python libraries so as to push to find the best implementation.
    - This also applies to C extensions and usage of SIMD or Vector instructions
        - I want to keep the code portable and maintainable. SIMD instructions can be super performant and allow for taking full advantage of the processor without relying on compiler optimization, but they are not easy to maintain and the code will need to be re-written when changing processor architecture.
- Use cpu profiler (like `line_profiler`)
    - My aim was to use python profiler to optimize the code as much as possible
- Use GPT for code generation
    - I wanted to generate most of the code using GPT to help me jump start my implementation
- Take inspiration from existing python attempts
    - I wanted to look at existing attempts of 1BRC in python and take inspiration from them and then build my optimizations on top.
    - Credits:
        - https://github.com/ifnesi/1brc
        - https://github.com/dannyvankooten/1brc
        - https://github.com/dougmercer-yt/1brc


## Performance Benchmark (Macbook Pro M2 Pro 32GB RAM) (1 Billion Rows)

### File cached in memory

| Interpreter | File | Time (sec)|
|-------------|------|-----------|
| Python3 | py_1brc_final.py | 24.882 |
| Python3 | py_1brc_mypyc.py (process_chunk.py precompiled using mypyc) | 24.441 |
| Python3 | calculateAverage.py (from https://github.com/ifnesi/1brc) | 36.303 |
| Python3 | calculateAveragePyPy.py (from https://github.com/ifnesi/1brc) | 60.60 |
| Python3 | calculateAverageDuckDB.py (from https://github.com/ifnesi/1brc) | 9.556 |
| Python3 | calculateAveragePolars.py (from https://github.com/ifnesi/1brc) | 12.058 |
| Python3 | doug_booty4.py (from https://github.com/dougmercer-yt/1brc) | 62.91 |


| Interpreter | File | Time (sec)|
|-------------|------|-----------|
| PyPy3 | py_1brc_final.py | 16.741 |
| PyPy3 | py_1brc_mypyc.py | 16.726 |
| PyPy3 | calculateAverage.py (from https://github.com/ifnesi/1brc) | 28.647 |
| PyPy3 | calculateAveragePyPy.py (from https://github.com/ifnesi/1brc) | 15.249 |
| PyPy3 | doug_booty4.py (from https://github.com/dougmercer-yt/1brc) | 8.507 |

### File not cached in memory (run `sudo purge` before each run)

| Interpreter | File | Time (sec)|
|-------------|------|-----------|
| Python3 | py_1brc_final.py | 29.608 |
| Python3 | py_1brc_mypyc.py (process_chunk.py precompiled using mypyc) | 29.654 |
| Python3 | calculateAverage.py (from https://github.com/ifnesi/1brc) | 53.255 |
| Python3 | calculateAveragePyPy.py (from https://github.com/ifnesi/1brc) | 61.83 |
| Python3 | calculateAverageDuckDB.py (from https://github.com/ifnesi/1brc) | 9.726 |
| Python3 | calculateAveragePolars.py (from https://github.com/ifnesi/1brc) | 16.799 |
| Python3 | doug_booty4.py (from https://github.com/dougmercer-yt/1brc) | 69.68 |

| Interpreter | File | Time (sec)|
|-------------|------|-----------|
| PyPy3 | py_1brc_final.py | 20.473 |
| PyPy3 | py_1brc_mypyc.py | 20.333 |
| PyPy3 | calculateAverage.py (from https://github.com/ifnesi/1brc) | 43.903 |
| PyPy3 | calculateAveragePyPy.py (from https://github.com/ifnesi/1brc) | 16.374 |
| PyPy3 | doug_booty4.py (from https://github.com/dougmercer-yt/1brc) | 11.162 |

## Execution

- I started by asking GPT to generate python code using below prompt:
    > For a file with 1 billion rows which has each row having a city name and the corresponding temperature, where the city temperature can be repeated multiple times. Provide a python code which prints the min, mean and max temperature for each city in alphabetical fashion. Make the code most optimal without any dependency on external libraries.

    - As you can realize the above prompt missed providing details of the file format, optimizing for speed and not memory and giving any direction for multi-processing. After providing all those inputs in subsequent prompts I got the implementation I needed.
    - GPT kept on going towards providing solutions using `numpy` and `pandas` and I had to reinforce in subsequent prompts to not rely on third party libraries
    - At the end I got a code which spawned multiple processes and read the file using memory mapping. The code even though functional, but was far from being optimized. Main issues were:
        - The memory mapped file was being mapped for full file size by all child processes, which led to very in-efficient read performance
        - Data was shared between processes using `multiprocessing.managers.DictProxy` which was very in-efficient, as it would synchronize access to the dictionary, which was not really needed.
        - The access pattern to read and store data in dictionary was very sub-optimal
            ``` 
                combined_data[city]['sum'] += data['sum']
                combined_data[city]['count'] += data['count']
                combined_data[city]['min'] = min(combined_data[city]['min'], data['min'])
                combined_data[city]['max'] = max(combined_data[city]['max'], data['max'])
            ```
- I used memory mapped file to read the data in chunks equally divided by the number of processes.
- Using profiler (line_profiler) I was able to identify and fix performance at multiple places.
    - For sharing data between main and child processes it is more optimal to use `Pool.starmap` or `Pool.map`.
    - Below code is more optimal when updating dictionary elements (inspired by https://github.com/ifnesi/1brc):
        ```
            city_info = combined_data[city]
            city_info['sum'] += data['sum']
            city_info['count'] += data['count']
            if city_info['min'] > data['min']
                city_info['min'] = data['min']
            if city_info['max'] < data['max']
                city_info['max'] = data['max']
        ``` 
    - Type annotations improve readability and performance.
    - Removed few `if` checks which were redundant.

## Learnings

- Python Multiprocessing is very powerful in enabling multi core processing. 
    - Using `Pool.starmap` to execute tasks in independent processes is a very effective way to parallelizing work. 
    - Using `multiprocessing.managers.DictProxy` to transfer data between processes can be very slow as the runtime synchronizes access to the dictionary.
- Code generated by GPT is most likely going to be sub-optimal and you need to spend time in optimizing the code by understanding the core logic.
- PyPy gives good boost over CPython but compatibility of PyPy with external libraries is a limiting factor.
- MyPyC optimizations might not always give the performance boost you expect. So always measure after making the change.
- Strategies of reading and parsing each of the lines, identifying the semi colon, casting to float are the operations which you will perform 1 billion times, so effectively doing them will have good impact on the program execution.
- Same applies to the choice of data structure for storing the values, as you will be doing lookup in the map/dictionary/hashtable a billion times.
- The order of reading data from the file will have impact on performance, so optimize for that. Random access will hurt performance, so when reading in multiple threads ensure that each thread is reading contiguous portions of the file sequentially. 
    - The [identify_chunks](./py_1brc_final.py#L69) method identifies portions of the file which we can read in individual threads/processes.
- Some optimizations which did not work:
    - In CPython [custom integer parsing](https://github.com/dougmercer-yt/1brc/blob/main/src/community/doug_booty4_no_gc.py#L26) (as shown below ) was slower than just casing the byte values to float
        ```
        def to_int(x: bytes) -> int:
            # Parse sign
            if x[0] == 45:  # ASCII for "-"
                sign = -1
                idx = 1
            else:
                sign = 1
                idx = 0
            # Check the position of the decimal point
            if x[idx + 1] == 46:  # ASCII for "."
                # -#.# or #.#
                # 528 == ord("0") * 11
                result = sign * ((x[idx] * 10 + x[idx + 2]) - 528)
            else:
                # -##.# or ##.#
                # 5328 == ord("0") * 111
                result = sign * ((x[idx] * 100 + x[idx + 1] * 10 + x[idx + 3]) - 5328)

            return result
        ```
    - Mypyc compilation was not any faster than default CPython implementation.
    - Using a custom data class [City](./py_1brc_final.py#L8) did not improve performance over a `List[float]`.
- Optimizing for PyPy does not make the implementation any faster in CPython, but optimizing for CPython does make the implementation faster in PyPy.


