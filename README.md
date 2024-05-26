# 1brc

brew install llvm  

/opt/homebrew/opt/llvm/bin/clang -shared -o libtemperatures.dylib -fPIC -fopenmp temperatures.c

/opt/homebrew/opt/llvm/bin/clang++ -fPIC -shared -o libcitytemp.so temp.cpp -fopenmp

