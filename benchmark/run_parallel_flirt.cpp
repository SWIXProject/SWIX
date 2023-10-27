#include <iostream>
#include "../src/config_p.hpp"
#include "../lib/parallel_flirt/PPFlirt.hpp"

#include "../utils/load_concurrent.hpp"
#include "../utils/print_util.hpp"
#include "../timer/rdtsc.h"
#include "../timer/timer.h"

#include "../lib/multithread_queues/concurrent_queue.h"
#include "../lib/multithread_queues/reader_writer_queue.h"

/*
FLIRT multithread benchmarks only supports one enqueue thread, 
    one dequeue thread while the rest are search threads (dequeue thread technically modified search thread)
    Only supports sequential workloads
*/
#define LOCAL_CAPACITY TIME_WINDOW/(NUM_THREADS-1)

int enqueue_thread_id = NUM_THREADS-1;
int dequeue_thread_id = (enqueue_thread_id+1)%NUM_THREADS;

atomic<int> enqueue_counter(0);

/*Key and Timestamp Types*/
typedef uint64_t key_type;
typedef uint64_t time_type;

typedef uint64_t val_type;

struct alignas(CACHELINE_SIZE) ThreadParam;

typedef ThreadParam thread_param_t;
typedef flirt::PPFlirt<key_type> flirt_type;
typedef tuple<task_status,uint64_t,uint64_t,uint64_t> task_type;

moodycamel::ReaderWriterQueue<task_type> task_queue_worker[NUM_THREADS];

atomic<bool> start_flag(false);
atomic<size_t> ready_threads(0);

struct alignas(CACHELINE_SIZE) ThreadParam {
    flirt_type *flirt;
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

void prepare_index(flirt_type *&flirt, int id);
void start_benchmark(flirt_type** flirt_partitions,  perf_type & perf);
void query_dispatcher(flirt_type** flirt_partitions, perf_type & perf);
void *worker_threads(void *param);

size_t dataMem;

int main(int argc, char **argv) 
{
    sosd_range_query_sequential<key_type,time_type>(DATA_DIR FILE_NAME);
    flirt_type** flirt_partitions = new flirt_type*[NUM_THREADS];

    perf_type perf;
    perf.totalCycle = 0;
    perf.totalCycleWithSync = 0;
    perf.memoryUsage = 0;

    LOG_INFO("[Bulkloading FLIRT]");
    int total_size_check = 0;
    double time_s = 0.0;
    TIMER_DECLARE(0);
    TIMER_BEGIN(0);
    for (size_t i = 0; i < NUM_THREADS-1; ++i)
    {
        prepare_index(flirt_partitions[i], i);
        total_size_check += flirt_partitions[i]->get_size();
    }
    flirt_partitions[NUM_THREADS-1] = new flirt_type(LOCAL_CAPACITY);
    TIMER_END_S(0,time_s);
    LOG_INFO("%8.1lf s : %.40s", time_s, "bulkloading");

    ASSERT_MESSAGE(total_size_check == TIME_WINDOW, "bulkload size is not equal to TIME_WINDOW");

    startTimer(&perf.totalCycleWithSync);

    start_benchmark(flirt_partitions, perf);

    stopTimer(&perf.totalCycleWithSync);

    if (flirt_partitions != nullptr)
    {
        for (size_t i = 0; i < NUM_THREADS; i++)
        {
            if (flirt_partitions[i] != nullptr)
            {
                delete flirt_partitions[i];
                flirt_partitions[i] = nullptr;
            }
        }
        delete[] flirt_partitions;
        flirt_partitions = nullptr;
    }

    cout << "Algorithm=FLIRT_" << INITIAL_ERROR << ";Threads=" << NUM_THREADS  << ";Data=" << FILE_NAME << ";MatchRate=" << MATCH_RATE << ";SearchPerRound=" << NUM_SEARCH_PER_ROUND;
    cout << ";UpdatePerRound=" << NUM_UPDATE_PER_ROUND <<";TimeWindow=" << TIME_WINDOW << ";TestLength=" << TEST_LEN-TIME_WINDOW;
    cout << ";TotalTime=" << (double)perf.totalCycle/CPU_CLOCK << ";TotalTimeSync=" << (double)perf.totalCycleWithSync/CPU_CLOCK;
    cout << ";MemoryUsage=" << perf.memoryUsage << ";";
    cout << ";DataUsage=" << dataMem << ";";
    cout << ";OverHead=" << ((double)perf.memoryUsage - (double)dataMem) /(double)dataMem *100 << ";";
    cout << endl;

    return 0;
}

/*
Prepare Index (Bulkload)
*/
void prepare_index(flirt_type *&flirt, int id)
{    
    vector<pair<key_type,time_type>> data_initial;
    data_initial.reserve(TIME_WINDOW);

    if (id == NUM_THREADS-2)
    {
        for (auto it = benchmark_data.begin()+LOCAL_CAPACITY*id; 
            it != benchmark_data.begin()+LOCAL_CAPACITY*(id+1)+TIME_WINDOW%(LOCAL_CAPACITY*(NUM_THREADS-1)); ++it)
        {
            data_initial.push_back(make_pair(get<0>(*it),get<1>(*it)));
        }
    }
    else
    {
        for (auto it = benchmark_data.begin()+LOCAL_CAPACITY*id; it != benchmark_data.begin()+LOCAL_CAPACITY*(id+1); ++it)
        {
            data_initial.push_back(make_pair(get<0>(*it),get<1>(*it)));
        }
    }
    ASSERT_MESSAGE(data_initial[0].first <= data_initial[1].first && 
                   data_initial[0].second <= data_initial[1].second, "bulkload data is not key and timestamp sorted");

    flirt = new flirt_type(LOCAL_CAPACITY);
    for (auto it = data_initial.begin(); it != data_initial.end(); ++it)
    {
        flirt->enqueue(get<0>(*it));
    }
}

/*
Start Benchmark
*/
void start_benchmark(flirt_type** flirt_partitions,  perf_type & perf)
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

    LOG_INFO("[Loading flirt into Threads]");
    start_flag = false;
    for(size_t worker_i = 0; worker_i < NUM_THREADS; ++worker_i)
    {
        thread_params[worker_i].flirt = flirt_partitions[worker_i];
        thread_params[worker_i].thread_id = worker_i;
        thread_params[worker_i].time = 0;
    }


    for(size_t worker_i = 0; worker_i < NUM_THREADS; ++worker_i)
    {
        int return_code = pthread_create(&threads[worker_i], nullptr, worker_threads,
                                (void *)&thread_params[worker_i]);
        if (return_code) 
        {
            LOG_ERROR("Error gerenerating worker thread %li: return code = %i \n", worker_i, return_code);
            abort();
        }
    }

    LOG_INFO("[Waiting for all Threads]");
    while (ready_threads < NUM_THREADS);

    start_flag = true;

    query_dispatcher(flirt_partitions, perf);
    
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
        // LOG_DEBUG_SHOW("[Thread %i: %lu]", i, thread_params[i].time);
        perf.totalCycle += thread_params[i].time;
    }
}


void query_dispatcher(flirt_type** flirt_partitions, perf_type & perf)
{
    LOG_INFO("[Preparing Dispatcher]");
    auto startIt = benchmark_data.begin();
    auto endIt = benchmark_data.begin() + TIME_WINDOW;

    task_type search_task, insert_task, delete_task, round_end_task, finish_task;
    tuple<uint64_t,uint64_t,uint64_t> searchTuple;

    srand(1); //Change seed if necessary
    volatile int round = 1;
    int randint = 0;

    size_t total_mem = 0;
    int mem_count = 0;
    
    while (endIt != benchmark_data.begin() + TEST_LEN)
    {
        LOG_INFO("[Dispatching round %i begins]", round);
        for (int i = 0; i < NUM_SEARCH_PER_ROUND; ++i)
        {
            searchTuple = benchmark_data.at((startIt - benchmark_data.begin()) + (rand() % ( (endIt - benchmark_data.begin()) - (startIt - benchmark_data.begin()) + 1 )));
            search_task = make_tuple(task_status::SEARCH, get<0>(searchTuple), get<1>(searchTuple), get<2>(searchTuple));

            //to all worker threads
            for (int worker = 0; worker < NUM_THREADS; ++worker)
            {
                task_queue_worker[worker].enqueue(search_task);
            }

        }

        for (int i = 0; i < NUM_UPDATE_PER_ROUND; ++i)
        {   
            insert_task = make_tuple(task_status::INSERT, get<0>(*endIt), get<1>(*endIt), 0);
            delete_task = make_tuple(task_status::DELETE, get<0>(*startIt), get<1>(*startIt), 0);

            //to all worker threads
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
            for (int i = 0; i < NUM_THREADS; ++i)
            {
                total_mem += flirt_partitions[i]->get_total_size_in_bytes();
                dataMem += flirt_partitions[i]->get_data_size_in_bytes();
            }
            ++mem_count;
            LOG_DEBUG("[Dispatching round %i finished]",round);
        }
        LOG_INFO("[Dispatching round %i finished]",round);
        ++round;

        LOG_INFO("[Waiting for next round]");
    }

    while (ready_threads < NUM_THREADS);
        ready_threads = 0;

    finish_task = make_tuple(task_status::FINISH, 0, 0, 0);

    for (int worker = 0; worker < NUM_THREADS; ++worker)
    {
        task_queue_worker[worker].enqueue(finish_task);
    }

    perf.memoryUsage = total_mem / mem_count;
    dataMem = dataMem / mem_count;

    LOG_INFO("[Dispatcher finished: total rounds = %i]", round-1);
}

void *worker_threads(void *param)
{
    // LOG_INFO("[Initializing Thread]");
    thread_param_t &thread_param = *(thread_param_t *)param;
    uint32_t thread_id = thread_param.thread_id;
    flirt_type *flirt = thread_param.flirt;
    LOG_INFO("[Created thread %u]", thread_id);
    ready_threads++;

    volatile int round = 1;
    volatile int count = 0;

    while (!start_flag)
        ;

    task_type task;
    bool found;

    while(true)
    {
        found = task_queue_worker[thread_id].try_dequeue(task);
        if (found)
        {
            uint64_t cycles = 0;
            startTimer(&cycles);

            std::vector<key_type> result;
            switch (get<0>(task))
            {
                case task_status::ROUND_END:
                case task_status::FINISH:
                    
                    LOG_INFO("[Thread %u finished round %i: count %i]",thread_id, round, count);
                    count = 0;
                    ++round;
                    ready_threads++;

                    if (get<0>(task) == task_status::FINISH) 
                    {
                        stopTimer(&cycles);
                        thread_param.time += cycles;
                        return NULL; 
                    }
                    break;

                case task_status::SEARCH:
                    result.reserve(MATCH_RATE);
                    flirt->range_search(get<1>(task),get<1>(task),get<3>(task), result);
                    count += result.size();
                    break;

                case task_status::INSERT:
                    if (thread_id == enqueue_thread_id)
                    {
                        if (enqueue_counter.load() == 0) {flirt->clear();}
                        
                        flirt->enqueue(get<1>(task));
                        ++enqueue_counter;

                        if (enqueue_counter == LOCAL_CAPACITY)
                        {
                            enqueue_counter = 0;
                            int temp = dequeue_thread_id;
                            temp = (temp+1) % NUM_THREADS;
                            dequeue_thread_id = temp;

                            temp = enqueue_thread_id;
                            temp = (temp+1) % NUM_THREADS;
                            enqueue_thread_id = temp;
                        }
                    }
                    break;

                case task_status::DELETE:
                    if (thread_id == dequeue_thread_id)
                    {
                        result.reserve(MATCH_RATE);
                        flirt->range_search(get<1>(task),get<1>(task),get<3>(task),result,enqueue_counter.load()-1);
                        count += result.size();
                    }
                    break;

                default:
                    LOG_ERROR("Unknown task type");
                    break;
            }

            stopTimer(&cycles);
            thread_param.time += cycles;
        }
    }
}
