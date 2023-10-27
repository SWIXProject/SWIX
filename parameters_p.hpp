#pragma once
#define DATA_DIR "/data/Documents/data/"
#define FILE_NAME "f_books"
#define TIME_WINDOW 10000000
#define MATCH_RATE 100
#define TEST_LEN 10010000
#define MAX_TIMESTAMP TEST_LEN*2 +1
#define SEED 1
#define CPU_CLOCK 3400000000
#define RUNFLAG -1
#define NO_STD 1
// #define PRINT

#define NUM_THREADS 4
#define CACHELINE_SIZE (1 << 6)
#define NUM_SEARCH_PER_ROUND 1
#define NUM_UPDATE_PER_ROUND 5
// #define LOG_ON