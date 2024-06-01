# 1BRC in Python

I am a sucker for optimization. So when I heard about 1BRC I naturally got intrigured. I set out to fulfill the challange using few principles:

- Use Python
    - Python is the primary language I use at my work right now. Historically I have used C, C++, C#, Java, Scala, Go at work and for hobby projects, and I love learning new languages. And I believe each language has its strengths.
- Don't use any 3rd party packages like DuckDb or Polars
    - Even though DuckDB and Polars based implementation is highly suited and also the fastest, my aim was to sitck to native python libraries so as to push to find the best implementation
- Don't use SIMD or Vector instructions
    - Keep the code portable and maintainable. SIMD instructions can be super performant and allow for taking full advantage of the processor without relying on compiler optimization