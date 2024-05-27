import ctypes

# Load the shared library
lib = ctypes.CDLL('./libcitytemp.so')

# Define the argument and return types of the C function
lib.process_file.argtypes = [ctypes.c_char_p]
lib.process_file.restype = None

# Call the C function
filename = "../1brc/measurements.txt"
lib.process_file(filename.encode('utf-8'))