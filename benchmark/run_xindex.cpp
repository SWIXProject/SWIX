#include <iostream>
#include <sys/resource.h>

#include "../lib/xindex/benchmark_function.h"
#include "../lib/xindex/xindex.h"
#include "../lib/xindex/xindex_impl.h"

#include "../utils/load_concurrent.hpp"
#include "../utils/print_util.hpp"
#include "../timer/rdtsc.h"

#include "../lib/multithread_queues/concurrent_queue.h"
#include "../lib/multithread_queues/reader_writer_queue.h"

/*
Requires Intel MKL
Using CMake:
    include_directories("include")
    include_directories("/opt/intel/oneapi/mkl/2023.1.0/include")
Using command-line:
    -I/opt/intel/oneapi/mkl/2023.1.0/include
*/

#ifndef LOAD_DATA_METHOD
#define LOAD_DATA_METHOD 0         
#endif

/*Key and Timestamp Types*/
class Key;
typedef uint64_t key_type;
typedef uint64_t time_type;
typedef uint64_t val_type;

struct alignas(CACHELINE_SIZE) ThreadParam;
typedef ThreadParam thread_param_t;
typedef Key index_key_t;
typedef xindex::XIndex<index_key_t, time_type> xindex_t;

std::atomic<bool> start_flag = false;
std::atomic<size_t> ready_threads(0);
std::vector<index_key_t> xindex_bulkload_keys;

struct alignas(CACHELINE_SIZE) ThreadParam {
    xindex_t *xi;
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

void prepare_index(xindex_t *&xi);
void start_benchmark(xindex_t *xi, perf_type & perf);
void query_dispatcher(xindex_t *xi, perf_type & perf);
void *worker_threads(void *param);

enum class task_status { FINISH = -5, ROUND_END = -4, SEARCH = -3, INSERT = -2, DELETE = -1};

class Key {
    typedef std::array<double, 1> model_key_t;

public:
    static constexpr size_t model_key_size() { return 1; }
    static Key max() {
        static Key max_key(std::numeric_limits<uint64_t>::max());
        return max_key;
    }
    static Key min() {
        static Key min_key(std::numeric_limits<uint64_t>::min());
        return min_key;
    }

    Key() : key(0) {}
    Key(uint64_t key) : key(key) {}
    Key(const Key &other) { key = other.key; }
    Key &operator=(const Key &other) {
        key = other.key;
        return *this;
    }

    model_key_t to_model_key() const {
        model_key_t model_key;
        model_key[0] = key;
        return model_key;
    }

    friend bool operator<(const Key &l, const Key &r) { return l.key < r.key; }
    friend bool operator>(const Key &l, const Key &r) { return l.key > r.key; }
    friend bool operator>=(const Key &l, const Key &r) { return l.key >= r.key; }
    friend bool operator<=(const Key &l, const Key &r) { return l.key <= r.key; }
    friend bool operator==(const Key &l, const Key &r) { return l.key == r.key; }
    friend bool operator!=(const Key &l, const Key &r) { return l.key != r.key; }

    uint64_t key;   
} PACKED;

typedef tuple<task_status,uint64_t,uint64_t,uint64_t> task_type;
moodycamel::ReaderWriterQueue<task_type> task_queue_worker[NUM_THREADS];

int main(int argc, char **argv) 
{
    LOG_INFO("Start benchmarking...");
    switch (LOAD_DATA_METHOD)
    {
        case 1:
            sosd_range_query_sequential<key_type,time_type>(DATA_DIR FILE_NAME);
            break;
        default:
            sosd_range_query<key_type,time_type>(DATA_DIR FILE_NAME);
            break;
    }
    LOG_INFO("Data loaded.")

    LOG_INFO("SIZE of benchmark_data: %lu", benchmark_data.size());

    xindex_t *xi;

    perf_type perf;
    perf.totalCycle = 0;
    perf.totalCycleWithSync = 0;
    perf.memoryUsage = 0;

    prepare_index(xi);
    startTimer(&perf.totalCycleWithSync);
    start_benchmark(xi, perf);
    stopTimer(&perf.totalCycleWithSync);

    if (xi != nullptr) delete xi;

    cout << "Algorithm=XINDEX" << ";Threads=" << NUM_THREADS  << ";Data=" << FILE_NAME << ";MatchRate=" << MATCH_RATE << ";SearchPerRound=" << NUM_SEARCH_PER_ROUND;
    cout << ";UpdatePerRound=" << NUM_UPDATE_PER_ROUND <<";TimeWindow=" << TIME_WINDOW << ";TestLength=" << TEST_LEN-TIME_WINDOW;
    cout << ";TotalTime=" << (double)perf.totalCycle/CPU_CLOCK << ";TotalTimeSync=" << (double)perf.totalCycleWithSync/CPU_CLOCK;
    cout << ";MemoryUsage=" << perf.memoryUsage << ";";
    cout << endl;

    return 0;
}


void prepare_index(xindex_t *&xi)
{
    LOG_INFO("Preparing index...");

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

    xindex_bulkload_keys.reserve(data_initial_keys.size());
    for (size_t i = 0; i < data_initial_keys.size(); ++i) {
        xindex_bulkload_keys.push_back(index_key_t(data_initial_keys[i]));
    }
    
    double time_s = 0.0;
    TIMER_DECLARE(0);
    TIMER_BEGIN(0);
    xi = new xindex_t(xindex_bulkload_keys, data_initial_values, NUM_THREADS, 1);
    TIMER_END_S(0,time_s);
    LOG_INFO("Index creation time: %lf", time_s);
}


void start_benchmark(xindex_t *xi,  perf_type & perf)
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
        thread_params[worker_i].xi = xi;
        thread_params[worker_i].thread_id = worker_i;
        thread_params[worker_i].time = 0;
    }

    for(size_t worker_i = 0; worker_i < NUM_THREADS; ++worker_i)
    {
        int return_code = pthread_create(&threads[worker_i], nullptr, worker_threads,
                                (void *)&thread_params[worker_i]);

        LOG_INFO("[Created thread %li]", worker_i);
        if (return_code) 
        {
            LOG_ERROR("Error gerenerating worker thread %li: return code = %i \n", worker_i, return_code);
            abort();
        }
    }

    LOG_INFO("[Waiting for all Threads]");
    while (ready_threads < NUM_THREADS);
    start_flag = true;

    query_dispatcher(xi, perf);
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


void *worker_threads(void *param)
{
    LOG_INFO("[Created thread]");
    thread_param_t &thread_param = *(thread_param_t *)param;
    uint32_t thread_id = thread_param.thread_id;
    xindex_t *xi = thread_param.xi;
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
        if(found) 
        {
            uint64_t cycles = 0;
            startTimer(&cycles);
            std::vector<std::pair<index_key_t, time_type>> result;
            switch(get<0>(task))
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
                    xi->scan(get<1>(task), MATCH_RATE, result, thread_id);
                    count += result.size();
                    break;

                case task_status::INSERT:
                    xi->put(get<1>(task), get<2>(task), thread_id);
                    LOG_INFO("[Thread %u: insert %lu]",thread_id, get<1>(task));
                    break;

                case task_status::DELETE:
                    xi->remove(get<1>(task), thread_id);
                    LOG_INFO("[Thread %u: delete %lu]",thread_id, get<1>(task));
                    break;

                default:
                    LOG_ERROR("Unknown task type");
                    abort();
            }
            stopTimer(&cycles);
            thread_param.time += cycles;
        }
    }
}

void query_dispatcher(xindex_t *xi, perf_type & perf)
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
            total_mem += xi->memory_usage();
            ++mem_count;
            LOG_DEBUG("[Dispatching round %i finished]",round);
        }

        LOG_INFO("[Dispatching round %i finished]",round);
        ++round;

        LOG_INFO("[Waiting for next round]");
        while (ready_threads < NUM_THREADS);
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

