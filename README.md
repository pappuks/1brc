# 1BRC in Python

I am a sucker for optimization. So when I heard about 1BRC I naturally got intrigured. I set out to fulfill the challange using few principles:

## Principles
- Use Python
    - Python is the primary language I use at my work right now. Historically I have used C, C++, C#, Java, Scala, Go at work and for hobby projects, and I love learning new languages. And I believe each language has its strengths.
- Don't use any 3rd party packages like DuckDb or Polars
    - Even though DuckDB and Polars based implementation is highly suited and also the fastest, my aim was to sitck to native python libraries so as to push to find the best implementation
- Don't use SIMD or Vector instructions
    - Keep the code portable and maintainable. SIMD instructions can be super performant and allow for taking full advantage of the processor without relying on compiler optimization, but they are not easy to maintain and the code will need to be re-written when changing processor architecture.
- Use profiler
    - My aim was to use python profiler to optimize the code as much as possible and to also learn how to profile python code 
- Use GPT for code generation
    - I wanted to generate most of the code using GPT, so that it helps me jump start my implementation
- Take inspiration from existing python attempts
    - I wanted to look existing attempts of 1BRC in python and take inspiration from them and then build my optimizations on top of them

## Performance (Macbook Pro M2 Pro 32GB RAM) (1 Billion Rows)

### File cached in memory

| Interpreter | File | Time (sec)|
|-------------|------|-----------|
| PyPy3 | py_1brc_final.py | 16.741 |
| Python3 | py_1brc_final.py | 26.518 |
| PyPy3 | py_1brc_mypyc.py | 16.726 |
| Python3 | py_1brc_mypyc.py (process_chunk.py precompiled using mypyc) | 24.441 |
| Python3 | calculateAverageDuckDB.py (from https://github.com/ifnesi/1brc) | 9.556 |
| Python3 | calculateAverage.py (from https://github.com/ifnesi/1brc) | 36.303 |
| PyPy3 | calculateAveragePyPy.py (from https://github.com/ifnesi/1brc) | 15.249 |
| Python3 | calculateAveragePolars.py (from https://github.com/ifnesi/1brc) | 12.058 |

### File not cached in memory (run `sudo purge` before each run)

| Interpreter | File | Time (sec)|
|-------------|------|-----------|
| PyPy3 | py_1brc_final.py | 20.473 |
| Python3 | py_1brc_final.py | 29.608 |
| PyPy3 | py_1brc_mypyc.py | 20.333 |
| Python3 | py_1brc_mypyc.py (process_chunk.py precompiled using mypyc) | 29.654 |
| Python3 | calculateAverageDuckDB.py (from https://github.com/ifnesi/1brc) | 9.726 |
| Python3 | calculateAverage.py (from https://github.com/ifnesi/1brc) | 53.255 |
| PyPy3 | calculateAveragePyPy.py (from https://github.com/ifnesi/1brc) | 16.374 |
| Python3 | calculateAveragePolars.py (from https://github.com/ifnesi/1brc) | 16.799 |

## Execution

- I started by asking GPT to generate python code using below prompt:
    > For a file with 1 billion rows which has each row having a city name and the corresponding temperature, where the city temperature can be repeated multiple times. Provide a python code which prints the min, mean and max temperature for each city in alphabetical fashion. Make the code most optimal without any dependency on external libraries.

    - As you can realize the above prompt missed providing details of the file format, optimizing for speed and not memory and giving any direction for multi-processing. After providing all those imputs in subsequent prompts. 
    - GPT kept on going towards providing solutions using `numpy` and `pandas` and I had to reinforce in subsequent prompts to not rely on third party libraries
    - At the end I got a code which spawned multiple processes and read the file using memory mapping. The code even though functional, but was far from being optimized. Main issues were:
        - The memory mapped file was being mapped for full file size by all child processes, which led to very in-efficient read performance
        - Data was shared between processes using `multiprocessing.managers.DictProxy` which was very in-efficient, as it would syncronize access to the dictionary, which was not really needed.
        - The access pattern to read and store data in dictionary was very sub-optimal
        ``` 
            combined_data[city]['sum'] += data['sum']
            combined_data[city]['count'] += data['count']
            combined_data[city]['min'] = min(combined_data[city]['min'], data['min'])
            combined_data[city]['max'] = max(combined_data[city]['max'], data['max'])
        ```

        - Instead the below code is more optimal (inspired by https://github.com/ifnesi/1brc):

        ```
            city_info = combined_data[city]
            city_info['sum'] += data['sum']
            city_info['count'] += data['count']
            if city_info['min'] > data['min']
                city_info['min'] = data['min']
            if city_info['max'] < data['max']
                city_info['max'] = data['max']
        ``` 


## Learnings

- Python Multiprocessing is very powerful in enabling multi core processing. 
    - Using `Pool.startmap` to execute tasks in independent proceses is a very effective way to parallelizing work. 
    - Using `multiprocessing.managers.DictProxy` to transfer data between processes can be very slow as the runtime syncronizes access to the dictionary.
- Code generated by GPT is most likely going to be sub-optimal and you need to spend time in optimizing the code by understanding the core logic.
- In low memory situations using memory mapped files is going to be most performant as compared to direct file access.
    - When using memory mapped file do not access the same chunks in random patten in different processes, instead mount the memory mapped file with required length and offset in each process/thread.
- PyPy gives good boost over CPython but compatibility of PyPy with exeternal libraries is limiting factor
- MyPyC optimizations do yield results where you can shave off 4-5 seconds of execution time 
- The method of reading the 13GB measurement file (with 1 billion records) has a large impact on the performance of the program
    - When there is memory pressure then memory mapping the file gives best reasults
    - When the file is already precached in memory then using the default file operations yields best performance
- Strategies of reading and parsing each of the lines, identifying the semi colon, casting to float are the operations which you will perform 1 billion times, so effectively doing them will have good impact on the program execution.
- Same applies to the choice of data structure for storing the values, as you will be doing lookup in the map/dictionary/hashtable a billion times.
- The order of reading data from the file will have impact on performance, so optimize for that. Random access will hurt performance, so when reading in multiple threads ensure that each thread is reading contiguous portions of the file sequentially. 
    - The [identify_chunks](./py_1brc_final.py#L69) method identifies portions of the file which we can read by individual threads 
