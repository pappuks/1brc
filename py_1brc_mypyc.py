import multiprocessing
import os
import mmap
from typing import List, Dict, Tuple
import struct
from process_chunk import perform_op

# Path to the file containing measurements
file_path = "measurements.txt"

def main() -> None:
    """Main function to process the file using multiple processes and print the results.
    """
    perform_op()
    

if __name__ == "__main__":
    main()

