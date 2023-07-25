#include <iostream>
#include "../utils/load_concurrent.hpp"
#include "../utils/print_util.hpp"

#include "../timer/timer.h"
#include "../timer/rdtsc.h"

//CONCURRENT QUEUE: FOR CALCULATING SIZE ONLY

/*Key and Timestamp Types*/
typedef uint64_t key_type;
typedef uint64_t time_type;
typedef uint64_t val_type;

typedef std::pair<key_type, time_type> content_type;
typedef std::vector<content_type> queue_type;

struct Perf
{
    uint64_t totalCycle;
    uint64_t totalCycleWithSync;
    size_t memoryUsage;
};

typedef Perf perf_type;

void start_benchmark(queue_type *queue, perf_type & perf);
uint64_t memory_usage(size_t queue_size);

int main(int argc, char **argv) 
{
    LOG_INFO("[Bulkloading AIDEL]");
    double time_s = 0.0;
    TIMER_DECLARE(0);
    TIMER_BEGIN(0);
    
    sosd_range_query<key_type,time_type>(DATA_DIR FILE_NAME);

    TIMER_END_S(0,time_s);
    LOG_INFO("%8.1lf s : %.40s", time_s, "sosd read");

    perf_type perf;
    perf.totalCycle = 0;
    perf.totalCycleWithSync = 0;
    perf.memoryUsage = 0;

    start_benchmark(nullptr, perf);

    cout << "Algorithm=QUEUE" << ";Threads=" << NUM_THREADS  << ";Data=" << FILE_NAME << ";MatchRate=" << MATCH_RATE << ";SearchPerRound=" << NUM_SEARCH_PER_ROUND;
    cout << ";UpdatePerRound=" << NUM_UPDATE_PER_ROUND <<";TimeWindow=" << TIME_WINDOW << ";TestLength=" << TEST_LEN-TIME_WINDOW;
    cout << ";TotalTime=" << (double)perf.totalCycle/CPU_CLOCK << ";TotalTimeSync=" << (double)perf.totalCycleWithSync/CPU_CLOCK;
    cout << ";MemoryUsage=" << perf.memoryUsage << ";";
    cout << endl;

    return 0;
}

void start_benchmark(queue_type *queue, perf_type & perf)
{
    auto startIt = benchmark_data.begin();
    auto endIt = benchmark_data.begin() + TIME_WINDOW;

    std::tuple<uint64_t,uint64_t,uint64_t> searchTuple;

    srand(1); //Change seed if necessary
    volatile int round = 1;
    int randint = 0;

    size_t total_mem = 0;
    int mem_count = 0;

    while (endIt != benchmark_data.begin() + TEST_LEN)
    {
        for (int i = 0; i < NUM_UPDATE_PER_ROUND; ++i)
        {   
            ++startIt;
            ++endIt;

            if (endIt == benchmark_data.begin() + TEST_LEN) { break;}
        }

        if (round % 1000 == 0)
        {
            total_mem += memory_usage(endIt-startIt);
            ++mem_count;
        }
        ++round;
    }

    perf.memoryUsage = total_mem / mem_count;
}

/*
Memory Usage
*/
uint64_t memory_usage(size_t queue_size)
{
    return sizeof(queue_type) + sizeof(content_type) * queue_size;
}