#pragma once
#include <iostream>
#include <mutex>
#include <atomic>
#include <unistd.h>

#include "../src/PSwix.hpp"

#include "../utils/load_concurrent.hpp"
#include "../timer/timer.h"
#include "../timer/rdtsc.h"

#include "../lib/multithread_queues/concurrent_queue.h"
#include "../lib/multithread_queues/reader_writer_queue.h"

using namespace std;

#ifndef LOAD_DATA_METHOD
#define LOAD_DATA_METHOD 0         
#endif

/*Key and Timestamp Types*/
typedef uint64_t key_type;
typedef uint64_t time_type;
typedef uint64_t val_type;

struct alignas(CACHELINE_SIZE) ThreadParam;
typedef ThreadParam thread_param_t;
typedef pswix::SWmeta<key_type, time_type> pswix_type;
typedef tuple<task_status,uint64_t,uint64_t,uint64_t> task_type; // tuple<task_type,lowerbound,timestamp,upperbound>

// volatile bool running = false;
atomic<bool> start_flag(false);
atomic<size_t> ready_threads(0);

struct alignas(CACHELINE_SIZE) ThreadParam {
    pswix_type *pswix;
    uint64_t time;
    uint32_t thread_id;
};

struct Perf
{
    uint64_t totalCycle;
    uint64_t totalCycleWithSync;
    size_t memoryUsage;
};
typedef Perf perf_type;

//Temporary task queue for each worker thread
moodycamel::ReaderWriterQueue<task_type> task_queue_worker[NUM_THREADS];

void prepare_index(pswix_type *&pswix);
void start_benchmark(pswix_type *pswix, perf_type & perf);
void query_dispatcher(pswix_type *pswix, perf_type & perf);
void *meta_thread(void *param);
void *worker_threads(void *param);

int main(int argc, char **argv)
{
    switch (LOAD_DATA_METHOD)
    {
        case 1:
            sosd_range_query_sequential<key_type,time_type>(DATA_DIR FILE_NAME);
            break;
        default:
            sosd_range_query<key_type,time_type>(DATA_DIR FILE_NAME);
            break;
    }
    pswix_type *pswix;

    perf_type perf;
    perf.totalCycle = 0;
    perf.totalCycleWithSync = 0;
    perf.memoryUsage = 0;

    prepare_index(pswix);

    startTimer(&perf.totalCycleWithSync);
    
    start_benchmark(pswix, perf);

    stopTimer(&perf.totalCycleWithSync);

    if (pswix != nullptr) delete pswix;

    #if PARTITION_METHOD == 1
    cout << "Algorithm=PSWIXNoEmpty";
    #else
    cout << "Algorithm=PSWIX";
    #endif
    cout << ";Threads=" << NUM_THREADS  << ";Data=" << FILE_NAME << ";MatchRate=" << MATCH_RATE << ";SearchPerRound=" << NUM_SEARCH_PER_ROUND;
    cout << ";UpdatePerRound=" << NUM_UPDATE_PER_ROUND <<";TimeWindow=" << TIME_WINDOW << ";TestLength=" << TEST_LEN-TIME_WINDOW;
    cout << ";TotalTime=" << (double)perf.totalCycle/CPU_CLOCK << ";TotalTimeSync=" << (double)perf.totalCycleWithSync/CPU_CLOCK;
    cout << ";MemoryUsage=" << perf.memoryUsage << ";";
    cout << endl;

    return 0;
}

/*
Prepare Index (Bulkload)
*/
void prepare_index(pswix_type *&pswix)
{    
    vector<pair<key_type,time_type>> data_initial;
    data_initial.reserve(TIME_WINDOW);
    for (auto it = benchmark_data.begin(); it != benchmark_data.begin()+TIME_WINDOW; ++it)
    {
        data_initial.push_back(make_pair(get<0>(*it),get<1>(*it)));
    }
    ASSERT_MESSAGE(data_initial.size() == TIME_WINDOW, "bulkload size is not equal to TIME_WINDOW");
    
    LOG_INFO("[Bulkloading DALi]");
    double time_s = 0.0;
    TIMER_DECLARE(0);
    TIMER_BEGIN(0);
    pswix = new pswix_type(NUM_THREADS, data_initial);
    TIMER_END_S(0,time_s);
    LOG_INFO("%8.1lf s : %.40s", time_s, "bulkloading");
}

/*
Create threads
*/
void start_benchmark(pswix_type *pswix, perf_type & perf)
{
    LOG_INFO("[Initializing & Checking Threads]");
    pthread_t threads[NUM_THREADS];
    thread_param_t thread_params[NUM_THREADS];
    for (size_t i = 0; i < NUM_THREADS; i++) {
        if ((uint64_t)(&(thread_params[i])) % CACHELINE_SIZE != 0) {
            LOG_ERROR("Wrong parameter address: %p", &(thread_params[i]));
            abort();
        }
    }

    LOG_INFO("[Loading DALi into Threads]");
    start_flag = false;
    for(size_t worker_i = 0; worker_i < NUM_THREADS; ++worker_i)
    {
        thread_params[worker_i].pswix = pswix;
        thread_params[worker_i].thread_id = worker_i;
        thread_params[worker_i].time = 0;
    }

    LOG_INFO("[Creating Threads]");
    for(size_t worker_i = 0; worker_i < NUM_THREADS; ++worker_i)
    {
        int return_code = pthread_create(&threads[worker_i], nullptr, worker_threads,
                                (void *)&thread_params[worker_i]);
        if (return_code) 
        {
            LOG_ERROR("Error gerenerating worker thread %i: return code = %i \n", worker_i, return_code);
            abort();
        }
    }

    LOG_INFO("[Waiting for all Threads]");
    while (ready_threads < NUM_THREADS);
    
    LOG_INFO("[Start Workload]");
    ready_threads = 0;
    start_flag = true;
    query_dispatcher(pswix, perf);

    LOG_INFO("[Finish Workload, joining threads]");
    void *status;
    for (size_t i = 0; i < NUM_THREADS; ++i) 
    {
        int return_code = pthread_join(threads[i], &status);
        if (return_code) 
        {
            LOG_ERROR("Unable to join: return code = %i \n",return_code);
            abort();
        }
    }

    for(int i = 0; i < NUM_THREADS; ++i)
    {
        perf.totalCycle += thread_params[i].time;
    }
}

/*
Query dispatcher
*/
void query_dispatcher(pswix_type *pswix, perf_type & perf)
{
    LOG_INFO("[Preparing Dispatcher]");
    auto startIt = benchmark_data.begin();
    auto endIt = benchmark_data.begin() + TIME_WINDOW;

    task_type search_task, insert_task, delete_task, round_end_task, finish_task;
    tuple<uint64_t,uint64_t,uint64_t> searchTuple;

    srand(1); //Change seed if necessary
    volatile int round = 1;

    size_t total_mem = 0;
    int mem_count = 0;

    while (endIt != benchmark_data.begin() + TEST_LEN)
    {
        LOG_INFO("[Dispatching round %i begins]", round);
        for (int i = 0; i < NUM_SEARCH_PER_ROUND; ++i)
        {
            searchTuple = benchmark_data.at((startIt - benchmark_data.begin()) + (rand() % ( (endIt - benchmark_data.begin()) - (startIt - benchmark_data.begin()) + 1 )));
            search_task = make_tuple(task_status::SEARCH, get<0>(searchTuple), get<1>(searchTuple), get<2>(searchTuple));

            // to all worker threads
            for (int worker = 0; worker < NUM_THREADS; ++worker)
            {
                task_queue_worker[worker].enqueue(search_task);
            }

        }

        for (int i = 0; i < NUM_UPDATE_PER_ROUND; ++i)
        {   
            insert_task = make_tuple(task_status::INSERT, get<0>(*endIt), get<1>(*endIt), numeric_limits<key_type>::max());
            delete_task = make_tuple(task_status::DELETE, get<0>(*startIt), get<1>(*startIt), numeric_limits<key_type>::max());

            // to all worker threads
            for (int worker = 0; worker < NUM_THREADS; ++worker)
            {
                task_queue_worker[worker].enqueue(insert_task);
                task_queue_worker[worker].enqueue(delete_task);
            }

            ++startIt;
            ++endIt;

            if (endIt == benchmark_data.begin() + TEST_LEN) { break;}
        }


        round_end_task = make_tuple(task_status::ROUND_END, 0, 0, 0);
        for (int worker = 0; worker < NUM_THREADS; ++worker)
        {
            task_queue_worker[worker].enqueue(round_end_task);
        }

        if (round % 1000 == 0)
        {
            total_mem += pswix->memory_usage();
            ++mem_count;
        }

        LOG_INFO("[Dispatching round %i finished]",round);
        ++round;

        LOG_INFO("[Waiting for next round]");
        while (ready_threads < NUM_THREADS) sleep(0.5);
        ready_threads = 0;
    }

    finish_task = make_tuple(task_status::FINISH, 0, 0, 0);
    for (int worker = 0; worker < NUM_THREADS; ++worker)
    {
        task_queue_worker[worker].enqueue(finish_task);
    }

    perf.memoryUsage = total_mem / mem_count;
    LOG_INFO("[Dispatcher finished: total rounds = %i]", round-1);
}

/*
Thread processes
*/
void *worker_threads(void *param)
{
    thread_param_t &thread_param = *(thread_param_t *)param;
    uint32_t thread_id = thread_param.thread_id;
    pswix_type *pswix = thread_param.pswix;
    LOG_INFO("[Created thread %u]", thread_id);
    ready_threads++;

    volatile int round = 1;
    volatile int count = 0;

    while (!start_flag);
    task_type task;
    bool found;

    while (true)
    {
        found = task_queue_worker[thread_id].try_dequeue(task);
        if (found)
        {
            uint64_t cycles = 0;
            startTimer(&cycles);

            if (get<0>(task) == task_status::ROUND_END || get<0>(task) == task_status::FINISH)
            {
                LOG_INFO("[Thread %u finished round %i: count %i]",thread_id, round, count);
                count = 0;
                ++round;
                ++ready_threads;

                if (get<0>(task) == task_status::FINISH) 
                {
                    stopTimer(&cycles);
                    thread_param.time += cycles;
                    return NULL; 
                }
            }
            else
            {
                pswix::search_bound_type predictBound;
                #if (MATCH_RATE  == 1)
                if (pswix->within_thread(thread_id, get<1>(task), predictBound))
                {
                    switch (get<0>(task))
                    {
                        case task_status::SEARCH:
                            count += pswix->lookup(thread_id, get<1>(task), get<2>(task), predictBound);
                            break;
                        case task_status::INSERT:
                            pswix->insert(thread_id, get<1>(task), get<2>(task), predictBound);
                            break;
                        default:
                            break;
                    }
                }
                #else
                if (pswix->within_thread(thread_id, get<1>(task), get<3>(task), predictBound))
                {
                    switch (get<0>(task))
                    {
                        case task_status::SEARCH:
                            LOG_INFO("[Thread %u round %i: search %lu start]",thread_id, round, get<1>(task));
                            count += pswix->range_query(thread_id, get<1>(task), get<2>(task), get<3>(task), predictBound);
                            LOG_INFO("[Thread %u round %i: search %lu finish]",thread_id, round, get<1>(task));
                            break;

                        case task_status::INSERT:
                            LOG_INFO("[Thread %u round %i: insert %lu start]",thread_id, round, get<1>(task));
                            pswix->insert(thread_id, get<1>(task), get<2>(task), predictBound);
                            LOG_INFO("[Thread %u round %i: insert %lu finish]",thread_id, round, get<1>(task));
                            break;

                        default:
                            break;
                    }

                    if (pswix::thread_retraining != -1 && pswix::thread_retraining.load()/10 == thread_id) 
                    { 
                        LOG_INFO("[Thread %u round %i: retrain]", thread_id, round);
                        pswix->meta_retrain();
                    }
                }
                #endif
            }
            stopTimer(&cycles);
            thread_param.time += cycles;
        }
    }
}

