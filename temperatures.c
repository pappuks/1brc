#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <omp.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>
#include "uthash.h"

typedef struct {
    const char *city; /* key */
    float min_temp;
    float max_temp;
    float total_temp;
    int count;
    UT_hash_handle hh; /* makes this structure hashable */
} CityTemperature;

int compare_city_temperatures(const void *a, const void *b) {
    const CityTemperature *ct1 = *(const CityTemperature **)a;
    const CityTemperature *ct2 = *(const CityTemperature **)b;
    return strcmp(ct1->city, ct2->city);
}

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

    CityTemperature *cities = NULL;

    #pragma omp parallel
    {
        CityTemperature *local_cities = NULL;
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
                char line[256];
                size_t line_length = i - line_start + 1;
                strncpy(line, &file_content[line_start], line_length);
                line[line_length] = '\0';
                char city[100];
                float temp;
                sscanf(line, "%[^;];%f", city, &temp);

                CityTemperature *entry = NULL;
                HASH_FIND_STR(local_cities, city, entry);
                if (entry) {
                    if (temp < entry->min_temp) entry->min_temp = temp;
                    if (temp > entry->max_temp) entry->max_temp = temp;
                    entry->total_temp += temp;
                    entry->count++;
                } else {
                    entry = malloc(sizeof(CityTemperature));
                    assert(entry != NULL);
                    entry->city = strdup(city); /* strdup to avoid invalid memory access */
                    entry->min_temp = temp;
                    entry->max_temp = temp;
                    entry->total_temp = temp;
                    entry->count = 1;
                    HASH_ADD_KEYPTR(hh, local_cities, entry->city, strlen(entry->city), entry);
                }
            }
        }

        #pragma omp critical
        {
            CityTemperature *entry, *tmp;
            HASH_ITER(hh, local_cities, entry, tmp) {
                CityTemperature *main_entry = NULL;
                HASH_FIND_STR(cities, entry->city, main_entry);
                if (main_entry) {
                    if (entry->min_temp < main_entry->min_temp) main_entry->min_temp = entry->min_temp;
                    if (entry->max_temp > main_entry->max_temp) main_entry->max_temp = entry->max_temp;
                    main_entry->total_temp += entry->total_temp;
                    main_entry->count += entry->count;
                } else {
                    main_entry = malloc(sizeof(CityTemperature));
                    assert(main_entry != NULL);
                    main_entry->city = strdup(entry->city); /* strdup to avoid invalid memory access */
                    main_entry->min_temp = entry->min_temp;
                    main_entry->max_temp = entry->max_temp;
                    main_entry->total_temp = entry->total_temp;
                    main_entry->count = entry->count;
                    HASH_ADD_KEYPTR(hh, cities, main_entry->city, strlen(main_entry->city), main_entry);
                }
                HASH_DEL(local_cities, entry);
                free((void *)entry->city);
                free(entry);
            }
        }
    }
    munmap(file_content, file_size);

    size_t count = HASH_COUNT(cities);
    CityTemperature **city_array = malloc(count * sizeof(CityTemperature *));
    assert(city_array != NULL);
    CityTemperature *entry, *tmp;
    size_t i = 0;
    HASH_ITER(hh, cities, entry, tmp) {
        city_array[i++] = entry;
    }
    qsort(city_array, count, sizeof(CityTemperature *), compare_city_temperatures);

    for (i = 0; i < count; i++) {
        printf("%s:%.2f;%.2f;%.2f\n", city_array[i]->city, city_array[i]->min_temp, city_array[i]->total_temp / city_array[i]->count, city_array[i]->max_temp);
        free((void *)city_array[i]->city);
        free(city_array[i]);
    }
    free(city_array);
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <filename>\n", argv[0]);
        return EXIT_FAILURE;
    }
    process_file(argv[1]);
    return EXIT_SUCCESS;
}