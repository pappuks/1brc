# 1BRC in Python

I am a sucker for optimization. So when I heard about 1BRC I naturally got intrigured. I set out to fulfill the challange using few principles:

## Guardrails
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


## Learnings

- Python Multiprocessing is very powerful in enabling multi core processing. 
    - Using `Pool.startmap` to execute tasks in independent proceses is a very effective way to parallelizing work. 
    - Using `multiprocessing.managers.DictProxy` to transfer data between processes can be very slow as the runtime syncronizes access to the dictionary.
- Code generated by GPT is most likely going to be sub-optimal and you need to spend time in optimizing the code by understanding the core logic.
- In low memory situations using memory mapped files is going to be most performant as compared to direct file access.
    - When using memory mapped file do not access the same chunks in random patten in different processes, instead mount the memory mapped file with required length and offset.
