#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <unordered_map>
#include <cassert>
#include <omp.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <cstring>
struct CityTemperature {
    std::string city;
    float min_temp;
    float max_temp;
    float total_temp;
    int count;
};
bool compare_city_temperatures(const CityTemperature& ct1, const CityTemperature& ct2) {
    return ct1.city < ct2.city;
}
extern "C" {
    void process_file(const char* filename) {
        int fd = open(filename, O_RDONLY);
        if (fd == -1) {
            perror("Failed to open file");
            return;
        }
        struct stat sb;
        if (fstat(fd, &sb) == -1) {
            perror("Failed to get file size");
            close(fd);
            return;
        }
        size_t file_size = sb.st_size;
        char* file_content = static_cast<char*>(mmap(NULL, file_size, PROT_READ, MAP_PRIVATE, fd, 0));
        if (file_content == MAP_FAILED) {
            perror("Failed to map file");
            close(fd);
            return;
        }
        close(fd);
        std::unordered_map<std::string, CityTemperature> cities;
        #pragma omp parallel
        {
            std::unordered_map<std::string, CityTemperature> local_cities;
            size_t chunk_size = file_size / omp_get_num_threads();
            int thread_id = omp_get_thread_num();
            size_t start = thread_id * chunk_size;
            size_t end = (thread_id == omp_get_num_threads() - 1) ? file_size : (thread_id + 1) * chunk_size;
            if (start != 0) {
                while (start < file_size && file_content[start - 1] != '\n') {
                    start++;
                }
            }
            if (end < file_size) {
                while (end < file_size && file_content[end] != '\n') {
                    end++;
                }
                end++;
            }
            for (size_t i = start; i < end; i++) {
                if (file_content[i] == '\n' || i == end - 1) {
                    size_t line_start = i;
                    while (line_start > start && file_content[line_start - 1] != '\n') {
                        line_start--;
                    }
                    std::string line(&file_content[line_start], i - line_start + 1);
                    char city[256];
                    float temp;
                    sscanf(line.c_str(), "%[^;];%f", city, &temp);
                    std::string city_str(city);
                    auto it = local_cities.find(city_str);
                    if (it != local_cities.end()) {
                        auto& entry = it->second;
                        entry.min_temp = std::min(entry.min_temp, temp);
                        entry.max_temp = std::max(entry.max_temp, temp);
                        entry.total_temp += temp;
                        entry.count++;
                    } else {
                        CityTemperature entry{city_str, temp, temp, temp, 1};
                        local_cities[city_str] = entry;
                    }
                }
            }
            #pragma omp critical
            {
                for (auto& pair : local_cities) {
                    auto& local_entry = pair.second;
                    auto it = cities.find(local_entry.city);
                    if (it != cities.end()) {
                        auto& main_entry = it->second;
                        main_entry.min_temp = std::min(main_entry.min_temp, local_entry.min_temp);
                        main_entry.max_temp = std::max(main_entry.max_temp, local_entry.max_temp);
                        main_entry.total_temp += local_entry.total_temp;
                        main_entry.count += local_entry.count;
                    } else {
                        cities[local_entry.city] = local_entry;
                    }
                }
            }
        }
        munmap(file_content, file_size);
        std::vector<CityTemperature> city_array;
        for (const auto& pair : cities) {
            city_array.push_back(pair.second);
        }
        std::sort(city_array.begin(), city_array.end(), compare_city_temperatures);
        for (const auto& cityTemp : city_array) {
            std::cout << cityTemp.city << ":" << cityTemp.min_temp << ";" << cityTemp.total_temp / cityTemp.count << ";" << cityTemp.max_temp << std::endl;
        }
    }
}