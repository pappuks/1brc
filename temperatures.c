#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <omp.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>

typedef struct {
    char city[100];
    float min_temp;
    float max_temp;
    float total_temp;
    int count;
} CityTemperature;

void process_file(const char *filename) {
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
    char *file_content = mmap(NULL, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (file_content == MAP_FAILED) {
        perror("Failed to map file");
        close(fd);
        return;
    }

    close(fd);

    int city_capacity = 10000; // Initial capacity
    CityTemperature *cities = malloc(city_capacity * sizeof(CityTemperature));
    assert(cities != NULL);
    int city_count = 0;

    #pragma omp parallel
    {
        CityTemperature *local_cities = malloc(city_capacity * sizeof(CityTemperature));
        assert(local_cities != NULL);
        int local_city_count = 0;

        #pragma omp for
        for (size_t i = 0; i < file_size; i++) {
            if (file_content[i] == '\n' || i == file_size - 1) {
                size_t line_start = i;
                while (line_start > 0 && file_content[line_start - 1] != '\n') {
                    line_start--;
                }

                char line[256];
                size_t line_length = i - line_start + 1;
                strncpy(line, &file_content[line_start], line_length);
                line[line_length] = '\0';

                char city[100];
                float temp;
                sscanf(line, "%[^;];%f", city, &temp);

                int found = 0;
                for (int j = 0; j < local_city_count; j++) {
                    if (strcmp(local_cities[j].city, city) == 0) {
                        if (temp < local_cities[j].min_temp) local_cities[j].min_temp = temp;
                        if (temp > local_cities[j].max_temp) local_cities[j].max_temp = temp;
                        local_cities[j].total_temp += temp;
                        local_cities[j].count++;
                        found = 1;
                        break;
                    }
                }

                if (!found) {
                    if (local_city_count == city_capacity) {
                        city_capacity *= 2;
                        local_cities = realloc(local_cities, city_capacity * sizeof(CityTemperature));
                        assert(local_cities != NULL);
                    }
                    strcpy(local_cities[local_city_count].city, city);
                    local_cities[local_city_count].min_temp = temp;
                    local_cities[local_city_count].max_temp = temp;
                    local_cities[local_city_count].total_temp = temp;
                    local_cities[local_city_count].count = 1;
                    local_city_count++;
                }
            }
        }

        #pragma omp critical
        {
            for (int i = 0; i < local_city_count; i++) {
                int found = 0;
                for (int j = 0; j < city_count; j++) {
                    if (strcmp(cities[j].city, local_cities[i].city) == 0) {
                        if (local_cities[i].min_temp < cities[j].min_temp) cities[j].min_temp = local_cities[i].min_temp;
                        if (local_cities[i].max_temp > cities[j].max_temp) cities[j].max_temp = local_cities[i].max_temp;
                        cities[j].total_temp += local_cities[i].total_temp;
                        cities[j].count += local_cities[i].count;
                        found = 1;
                        break;
                    }
                }

                if (!found) {
                    if (city_count == city_capacity) {
                        city_capacity *= 2;
                        cities = realloc(cities, city_capacity * sizeof(CityTemperature));
                        assert(cities != NULL);
                    }
                    cities[city_count] = local_cities[i];
                    city_count++;
                }
            }
        }

        free(local_cities);
    }

    munmap(file_content, file_size);

    // Sort cities alphabetically using qsort
    qsort(cities, city_count, sizeof(CityTemperature), (int(*)(const void *, const void *)) strcmp);

    // Print results
    for (int i = 0; i < city_count; i++) {
        printf("%s:%.2f;%.2f;%.2f\n", cities[i].city, cities[i].min_temp, cities[i].total_temp / cities[i].count, cities[i].max_temp);
    }

    free(cities);
}