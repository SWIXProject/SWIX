#include <iostream>
#include <sys/resource.h>

#include "../lib/finedex/function.h"
#include "../lib/finedex/aidel.h"
#include "../lib/finedex/aidel_impl.h"

#include "../utils/load_concurrent.hpp"
#include "../utils/print_util.hpp"
#include "../timer/rdtsc.h"

#include "../lib/multithread_queues/concurrent_queue.h"
#include "../lib/multithread_queues/reader_writer_queue.h"

// Example Benchmark from findex
/*Key and Timestamp Types*/
typedef uint64_t key_type;
typedef uint64_t time_type;
typedef uint64_t val_type;

enum class task_status { FINISH = -5, ROUND_END = -4, SEARCH = -3, INSERT = -2, DELETE = -1};
struct alignas(CACHELINE_SIZE) ThreadParam;

typedef ThreadParam thread_param_t;
typedef aidel::AIDEL<key_type, time_type> aidel_type;
typedef std::tuple<task_status,uint64_t,uint64_t,uint64_t> task_type;

moodycamel::ReaderWriterQueue<task_type> task_queue_worker[NUM_THREADS];

atomic<bool> start_flag(false);
atomic<size_t> ready_threads(0);
struct alignas(CACHELINE_SIZE) ThreadParam {
    aidel_type *ai;
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

void prepare_index(aidel_type *&ai);
void start_benchmark(aidel_type *ai,  perf_type & perf);
void query_dispatcher(aidel_type *ai, perf_type & perf);
void *worker_threads(void *param);

int main(int argc, char **argv) 
{
    LOG_INFO("[Bulkloading AIDEL]");
    double time_s = 0.0;
    TIMER_DECLARE(0);
    TIMER_BEGIN(0);
    
    sosd_range_query<key_type,time_type>(DATA_DIR FILE_NAME);

    TIMER_END_S(0,time_s);
    LOG_INFO("%8.1lf s : %.40s", time_s, "sosd read");

    aidel_type *ai;

    perf_type perf;
    perf.totalCycle = 0;
    perf.totalCycleWithSync = 0;
    perf.memoryUsage = 0;

    prepare_index(ai);
    startTimer(&perf.totalCycleWithSync);
    start_benchmark(ai, perf);
    stopTimer(&perf.totalCycleWithSync);

    if (ai != nullptr) delete ai;

    cout << "Algorithm=FINEDEX" << ";Threads=" << NUM_THREADS  << ";Data=" << FILE_NAME << ";MatchRate=" << MATCH_RATE << ";SearchPerRound=" << NUM_SEARCH_PER_ROUND;
    cout << ";UpdatePerRound=" << NUM_UPDATE_PER_ROUND <<";TimeWindow=" << TIME_WINDOW << ";TestLength=" << TEST_LEN-TIME_WINDOW;
    cout << ";TotalTime=" << (double)perf.totalCycle/CPU_CLOCK << ";TotalTimeSync=" << (double)perf.totalCycleWithSync/CPU_CLOCK;
    cout << ";MemoryUsage=" << perf.memoryUsage << ";";
    cout << endl;

    return 0;
}

void start_benchmark(aidel_type *ai,  perf_type & perf)
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

    LOG_INFO("[Loading aidel into Threads]");
    start_flag = false;
    for(size_t worker_i = 0; worker_i < NUM_THREADS; ++worker_i)
    {
        thread_params[worker_i].ai = ai;
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

    query_dispatcher(ai, perf);
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

void query_dispatcher(aidel_type *ai, perf_type & perf)
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

            randint = rand() % NUM_THREADS;
            task_queue_worker[randint].enqueue(search_task);
        }

        for (int i = 0; i < NUM_UPDATE_PER_ROUND; ++i)
        {   
            insert_task = make_tuple(task_status::INSERT, get<0>(*endIt), get<1>(*endIt), 0);
            delete_task = make_tuple(task_status::DELETE, get<0>(*startIt), get<1>(*startIt), 0);

            randint = rand() % NUM_THREADS;
            task_queue_worker[randint].enqueue(insert_task);

            randint = rand() % NUM_THREADS;
            task_queue_worker[randint].enqueue(delete_task);

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
            total_mem += ai->memory_usage();
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

    LOG_INFO("[Dispatcher finished: total rounds = %i]", round-1);
}



void *worker_threads(void *param)
{
    thread_param_t &thread_param = *(thread_param_t *)param;
    uint32_t thread_id = thread_param.thread_id;
    aidel_type *ai = thread_param.ai;
    LOG_INFO("[Created thread %u]", thread_id);
    ready_threads++;

    volatile int round = 1;
    volatile int count = 0;

    while (!start_flag);

    task_type task;
    bool found;

    while(true)
    {
        found = task_queue_worker[thread_id].try_dequeue(task);
        if (found)
        {
            uint64_t cycles = 0;
            startTimer(&cycles);

            std::vector<std::pair<key_type, time_type>> result;
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
                    ai->scan(get<1>(task), MATCH_RATE, result);
                    count += result.size();
                    break;

                case task_status::INSERT:
                    ai->insert(get<1>(task), get<2>(task));
                    break;

                case task_status::DELETE:
                    ai->remove(get<1>(task));
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

/*
Prepare Index (Bulkload)
*/
void prepare_index(aidel_type *&ai)
{    
    vector<pair<key_type,time_type>> data_initial;
    for (auto it = benchmark_data.begin(); it != benchmark_data.begin()+TIME_WINDOW; ++it)
    {
        data_initial.push_back(make_pair(get<0>(*it),get<1>(*it)));
    }

    sort(data_initial.begin(),data_initial.end(),sort_based_on_first);

    vector<key_type> data_initial_keys;
    vector<time_type> data_initial_values;

    data_initial_keys.reserve(TIME_WINDOW);
    data_initial_values.reserve(TIME_WINDOW);

    for (auto it = data_initial.begin(); it != data_initial.end(); ++it)
    {
        data_initial_keys.push_back(get<0>(*it));
        data_initial_values.push_back(get<1>(*it));
    }

    ASSERT_MESSAGE(data_initial_keys.size() == TIME_WINDOW, "bulkload size (keys) is not equal to TIME_WINDOW");
    ASSERT_MESSAGE(data_initial_values.size() == TIME_WINDOW, "bulkload size (values) is not equal to TIME_WINDOW");

    LOG_INFO("[Bulkloading aidel]");
    double time_s = 0.0;
    TIMER_DECLARE(0);
    TIMER_BEGIN(0);
    ai = new aidel_type();

    ai->train(data_initial_keys, data_initial_values, 32);

    TIMER_END_S(0,time_s);
    LOG_INFO("%8.1lf s : %.40s", time_s, "bulkloading");

    ai->self_check();
}