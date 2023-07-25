#pragma once
#include <iostream>
#include <mutex>
#include <atomic>
#include <unistd.h>
#include <sys/resource.h>

#include "../src/PSwix.hpp"

#include "../utils/load_concurrent.hpp"
#include "../timer/timer.h"
#include "../timer/rdtsc.h"

#include "../lib/multithread_queues/concurrent_queue.h"
#include "../lib/multithread_queues/reader_writer_queue.h"

using namespace std;

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

//Temporary task queue for each worker thread
moodycamel::ReaderWriterQueue<task_type> task_queue_worker[NUM_THREADS-1];

void prepare_index(pswix_type *&pswix);
void start_benchmark(pswix_type *pswix, uint64_t & totalCycle);
void query_dispatcher();
void *meta_thread(void *param);
void *worker_threads(void *param);

int main(int argc, char **argv)
{
    LOG_INFO("[Bulkloading DALi]");
    double time_s = 0.0;

    sosd_range_query<key_type,time_type>(DATA_DIR FILE_NAME);

    pswix_type *pswix;
    prepare_index(pswix);

    uint64_t totalCycle = 0;
    uint64_t totalCycleWithSync = 0;
    startTimer(&totalCycleWithSync);
    start_benchmark(pswix, totalCycle);
    stopTimer(&totalCycleWithSync);

    if (pswix != nullptr) delete pswix;

    cout << "Algorithm=PSWIX" << ";Threads=" << NUM_THREADS  << ";Data=" << FILE_NAME << ";MatchRate=" << MATCH_RATE << ";SearchPerRound=" << NUM_SEARCH_PER_ROUND;
    cout << ";UpdatePerRound=" << NUM_UPDATE_PER_ROUND <<";TimeWindow=" << TIME_WINDOW << ";TestLength=" << TEST_LEN-TIME_WINDOW;
    cout << ";TotalTime=" << (double)totalCycle/CPU_CLOCK << ";TotalTimeSync=" << (double)totalCycleWithSync/CPU_CLOCK << ";";
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
    pswix = new pswix_type(NUM_THREADS-1, data_initial);
    TIMER_END_S(0,time_s);
    LOG_INFO("%8.1lf s : %.40s", time_s, "bulkloading");
}

/*
Create threads
*/
void start_benchmark(pswix_type *pswix, uint64_t & totalCycle)
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
    int return_code = pthread_create(&threads[0], nullptr, meta_thread,
                            (void *)&thread_params[0]);
    if (return_code) 
    {
        LOG_ERROR("Error gerenerating meta thread: return code = %i \n",return_code);
        abort();
    }
    for(size_t worker_i = 1; worker_i < NUM_THREADS; ++worker_i)
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
    query_dispatcher();

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
        totalCycle += thread_params[i].time;
    }
}

/*
Query dispatcher
*/
void query_dispatcher()
{
    LOG_INFO("[Preparing Dispatcher]");
    auto startIt = benchmark_data.begin();
    auto endIt = benchmark_data.begin() + TIME_WINDOW;

    task_type search_task, insert_task, delete_task, round_end_task, finish_task;
    tuple<uint64_t,uint64_t,uint64_t> searchTuple;

    srand(1); //Change seed if necessary
    volatile int round = 1;
    while (endIt != benchmark_data.begin() + TEST_LEN)
    {
        LOG_INFO("[Dispatching round %i begins]", round);
        for (int i = 0; i < NUM_SEARCH_PER_ROUND; ++i)
        {
            searchTuple = benchmark_data.at((startIt - benchmark_data.begin()) + (rand() % ( (endIt - benchmark_data.begin()) - (startIt - benchmark_data.begin()) + 1 )));
            search_task = make_tuple(task_status::SEARCH, get<0>(searchTuple), get<1>(searchTuple), get<2>(searchTuple));

            for (int worker = 0; worker < NUM_THREADS-1; ++worker)
            {
                task_queue_worker[worker].enqueue(search_task);
            }

        }

        for (int i = 0; i < NUM_UPDATE_PER_ROUND; ++i)
        {   
            insert_task = make_tuple(task_status::INSERT, get<0>(*endIt), get<1>(*endIt), 0);
            delete_task = make_tuple(task_status::DELETE, get<0>(*startIt), get<1>(*startIt), 0);

            for (int worker = 0; worker < NUM_THREADS-1; ++worker)
            {
                task_queue_worker[worker].enqueue(insert_task);
                task_queue_worker[worker].enqueue(delete_task);
            }

            ++startIt;
            ++endIt;

            if (endIt == benchmark_data.begin() + TEST_LEN) { break;}
        }


        round_end_task = make_tuple(task_status::ROUND_END, 0, 0, 0);
        pswix::meta_queue.enqueue(make_tuple(task_status::ROUND_END,0,0,nullptr));

        for (int worker = 0; worker < NUM_THREADS-1; ++worker)
        {
            task_queue_worker[worker].enqueue(round_end_task);
        }

        LOG_INFO("[Dispatching round %i finished]",round);
        ++round;
        LOG_INFO("[Waiting for next round]");
    }
    
    
    while (ready_threads < NUM_THREADS);
    ready_threads = 0;

    finish_task = make_tuple(task_status::FINISH, 0, 0, 0);
    pswix::meta_queue.enqueue(make_tuple(task_status::FINISH,0,0,nullptr));

    for (int worker = 0; worker < NUM_THREADS-1; ++worker)
    {
        task_queue_worker[worker].enqueue(finish_task);
    }

    LOG_INFO("[Dispatcher finished: total rounds = %i]", round-1);
}

/*
Thread processes
*/
void *meta_thread(void *param)
{
    thread_param_t &thread_param = *(thread_param_t *)param;
    uint32_t thread_id = thread_param.thread_id;
    pswix_type *pswix = thread_param.pswix;
    LOG_INFO("[Created meta thread 0]");
    ready_threads++;

    volatile int round = 1;
    volatile int round_end_count = 0;
    volatile int finish_count = 0;

    while (!start_flag);

    while (true)
    {
        pswix::meta_workload_type meta_task;
        bool found = pswix::meta_queue.try_dequeue(meta_task);
        if (found)
        {
            uint64_t cycles = 0;
            startTimer(&cycles);

            if (get<0>(meta_task) == task_status::ROUND_END) {++round_end_count;}
            if (get<0>(meta_task) == task_status::FINISH) {++finish_count;}

            if (round_end_count == NUM_THREADS || finish_count == NUM_THREADS)
            {
                LOG_INFO("Meta thread 0 finished round %i", round);
                round_end_count = 0;
                ++round;
                ++ready_threads;
                
                if (finish_count == NUM_THREADS) 
                { 
                    stopTimer(&cycles);
                    thread_param.time += cycles;
                    return NULL; 
                }
            }
            else
            {
                switch (get<0>(meta_task))
                {
                    case task_status::INSERT:
                        pswix->meta_insert(thread_id,get<2>(meta_task),get<3>(meta_task),get<1>(meta_task));
                        break;
                    
                    case task_status::RETRAIN:
                        pswix->meta_retrain();
                        break;

                    default:
                        break;
                }
            }
            stopTimer(&cycles);
            thread_param.time += cycles;
        }
    }
}

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
        found = task_queue_worker[thread_id-1].try_dequeue(task);
        if (found)
        {
            uint64_t cycles = 0;
            startTimer(&cycles);
            if (get<0>(task) == task_status::ROUND_END || get<0>(task) == task_status::FINISH)
            {
                LOG_INFO("[Thread %u finished round %i: count %i]",thread_id, round, count);
                pswix::meta_queue.enqueue(make_tuple(task_status::ROUND_END,0,0,nullptr));
                count = 0;
                ++round;
                ++ready_threads;

                if (get<0>(task) == task_status::FINISH) 
                {
                    pswix::meta_queue.enqueue(make_tuple(task_status::FINISH,0,0,nullptr));
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
                            count += pswix->range_query(thread_id, get<1>(task), get<2>(task), get<3>(task), predictBound);
                            break;

                        case task_status::INSERT:
                            pswix->insert(thread_id, get<1>(task), get<2>(task), predictBound);
                            break;

                        default:
                            break;
                    }
                }
                #endif
            }
            stopTimer(&cycles);
            thread_param.time += cycles;
        }
    }
}
