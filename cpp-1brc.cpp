#include <iostream>
#include <fstream>
#include <vector>
#include <map>
#include <string>
#include <thread>
#include <mutex>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
const std::string file_path = "measurements_100mil.txt";
std::mutex mtx;
bool is_new_line(size_t position, char* mm) {
    if (position == 0) {
        return true;
    } else {
        return mm[position - 1] == '\n';
    }
}
size_t next_line(size_t position, char* mm, size_t file_size) {
    while (position < file_size && mm[position] != '\n') {
        position++;
    }
    return position + 1;
}
std::map<std::string, std::vector<float>> process_chunk(size_t chunk_start, size_t chunk_end) {
    size_t chunk_size = chunk_end - chunk_start;
    std::map<std::string, std::vector<float>> result;
    int fd = open(file_path.c_str(), O_RDONLY);
    if (fd == -1) {
        std::cerr << "Error opening file" << std::endl;
        return result;
    }
    char* mm = static_cast<char*>(mmap(nullptr, chunk_size, PROT_READ, MAP_PRIVATE, fd, chunk_start));
    if (mm == MAP_FAILED) {
        std::cerr << "Error mapping file" << std::endl;
        close(fd);
        return result;
    }
    if (chunk_start != 0) {
        chunk_start = next_line(0, mm, chunk_size);
    }
    for (size_t i = chunk_start; i < chunk_size; i = next_line(i, mm, chunk_size)) {
        std::string line(&mm[i], &mm[next_line(i, mm, chunk_size) - 1]);
        size_t delimiter_pos = line.find(';');
        std::string location = line.substr(0, delimiter_pos);
        float measurement = std::stof(line.substr(delimiter_pos + 1));
        if (result.find(location) == result.end()) {
            result[location] = {measurement, measurement, measurement, 1};
        } else {
            auto& _result = result[location];
            if (measurement < _result[0]) _result[0] = measurement;
            if (measurement > _result[1]) _result[1] = measurement;
            _result[2] += measurement;
            _result[3] += 1;
        }
    }
    munmap(mm, chunk_size);
    close(fd);
    return result;
}
std::vector<std::pair<size_t, size_t>> identify_chunks(int num_processes, size_t file_size) {
    std::vector<std::pair<size_t, size_t>> chunk_results;
    size_t chunk_size = file_size / num_processes;
    chunk_size -= chunk_size % sysconf(_SC_PAGESIZE);
    int fd = open(file_path.c_str(), O_RDONLY);
    if (fd == -1) {
        std::cerr << "Error opening file" << std::endl;
        return chunk_results;
    }
    char* mm = static_cast<char*>(mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0));
    if (mm == MAP_FAILED) {
        std::cerr << "Error mapping file" << std::endl;
        close(fd);
        return chunk_results;
    }
    size_t start = 0;
    while (start < file_size) {
        size_t end = start + chunk_size;
        if (end < file_size) {
            end = next_line(end, mm, file_size);
        }
        if (start == end) {
            end = next_line(end, mm, file_size);
        }
        if (end > file_size) {
            end = file_size;
            chunk_results.push_back({start, end});
            break;
        }
        chunk_results.push_back({start, end});
        start = end - (end % sysconf(_SC_PAGESIZE));
    }
    munmap(mm, file_size);
    close(fd);
    return chunk_results;
}
void merge_results(std::map<std::string, std::vector<float>>& shared_results, const std::map<std::string, std::vector<float>>& return_dict) {
    std::lock_guard<std::mutex> lock(mtx);
    for (const auto& entry : return_dict) {
        const std::string& station = entry.first;
        const std::vector<float>& data = entry.second;
        if (shared_results.find(station) != shared_results.end()) {
            auto& _result = shared_results[station];
            if (data[0] < _result[0]) _result[0] = data[0];
            if (data[1] > _result[1]) _result[1] = data[1];
            _result[2] += data[2];
            _result[3] += data[3];
        } else {
            shared_results[station] = data;
        }
    }
}
int main() {
    int num_processes = std::thread::hardware_concurrency();
    struct stat sb;
    if (stat(file_path.c_str(), &sb) == -1) {
        std::cerr << "Error getting file size" << std::endl;
        return 1;
    }
    size_t file_size = sb.st_size;
    std::vector<std::pair<size_t, size_t>> chunk_results = identify_chunks(num_processes, file_size);
    std::vector<std::thread> threads;
    std::vector<std::map<std::string, std::vector<float>>> results(num_processes);
    for (int i = 0; i < num_processes; ++i) {
        threads.emplace_back([&, i]() {
            results[i] = process_chunk(chunk_results[i].first, chunk_results[i].second);
        });
    }
    for (auto& t : threads) {
        t.join();
    }
    std::map<std::string, std::vector<float>> shared_results;
    for (const auto& result : results) {
        merge_results(shared_results, result);
    }
    std::cout << "{";
    for (const auto& entry : shared_results) {
        const std::string& location = entry.first;
        const std::vector<float>& measurements = entry.second;
        std::cout << location << "=" << measurements[0] << "/"
                  << (measurements[2] / measurements[3]) << "/"
                  << measurements[1] << ", ";
    }
    std::cout << "\b\b} " << std::endl;
    return 0;
}