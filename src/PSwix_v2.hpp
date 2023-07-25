#ifndef __PSWIX_META_HPP__
#define __PSWIX_META_HPP__

#pragma once
#include <atomic>
#include <mutex>
#include <condition_variable>
#include "../lib/multithread_queues/concurrent_queue.h"

#include "PSWseg.hpp"

using namespace std;

namespace pswix{

typedef uint64_t key_type;
typedef uint64_t time_type;
typedef tuple<bool,int,int,int> search_bound_type;

mutex thread_lock[NUM_THREADS];
condition_variable thread_cv[NUM_THREADS];
bool thread_occupied[NUM_THREADS];

atomic<int> thread_retraining(-1); //Indicates which thread is retraining

moodycamel::ConcurrentQueue<tuple<key_type,SWseg<key_type,time_type>*,bool>> retrain_insertion_queue(NUM_SEARCH_PER_ROUND + NUM_UPDATE_PER_ROUND*2);

// There is meta thread in this version of Parallel pswix, all threads are worker threads.

template<class Type_Key, class Type_Ts> 
class SWmeta
{
//Variables
private:
    int splitError;

    atomic<int> m_rightSearchBound;
    atomic<int> m_leftSearchBound;
    atomic<int> m_maxSearchError;
    atomic<int> m_numSeg;
    atomic<int> m_numSegExist;
    
    Type_Key m_startKey;
    double m_slope;

    vector<int> m_partitionIndex; //start index of each partition
    vector<Type_Ts> m_parititonMaxTime; //Updated during insertion
    vector<Type_Key> m_partitionStartKey; //starting key of each partition
    vector<int> m_numSegPerPartition; //number of segments in each partition
    vector<int> m_numSegExistsPerPartition; //number of segments exists in each partition
    
    vector<uint64_t> m_bitmap;
    vector<uint64_t> m_retrainBitmap;
    vector<vector<Type_Key>> m_keys;
    vector<vector<SWseg<Type_Key,Type_Ts>*>> m_ptr;

//Functions
public:
    //Constructors & Deconstructors
    SWmeta();
    SWmeta(int numThreads, const pair<Type_Key, Type_Ts> & arrivalTuple);
    SWmeta(int numThreads, const vector<pair<Type_Key, Type_Ts>> & stream);
    ~SWmeta();

    //Bulk Load
    void bulk_load(int numThreads, const vector<pair<Type_Key, Type_Ts>> & stream);
private:
    void split_data(const vector<pair<Type_Key,Type_Ts>> & data, vector<pair<Type_Key, SWseg<Type_Key,Type_Ts> * >> & splitedDataPtr);
    void partition_data_into_threads(int numThreads, vector<Type_Key> & keys, vector<SWseg<Type_Key,Type_Ts>*> & ptr);

public:
    //Multithread Index Operations
    bool within_thread(uint32_t threadID, Type_Key key, tuple<bool,int,int,int> & predictBound);
    bool within_thread(uint32_t threadID, Type_Key lowerBound, Type_Key upperBound, tuple<bool,int,int,int> & predictBound);

    int lookup(uint32_t threadID, Type_Key key, Type_Ts timestamp, tuple<bool,int,int,int> & predictBound);

    int range_query(uint32_t threadID, Type_Key lowerBound, Type_Ts timestamp, Type_Key upperBound, tuple<bool,int,int,int> & predictBound);

    int insert(uint32_t threadID, Type_Key key, Type_Ts timestamp, tuple<bool,int,int,int> & predictBound);

private:
    //Thread Locators
    uint32_t predict_thread(Type_Key key);
    uint32_t predict_thread(Type_Key key, tuple<bool,int,int,int> & predictBound);
    vector<uint32_t> predict_thread(Type_Key lowerBound, Type_Key upperBound, vector<tuple<bool,int,int,int>> & predictBound);
    
    //Search helpers
    int meta_model_search(uint32_t threadID, Type_Key targetKey, tuple<bool,int,int,int> & predictBound);
    int meta_non_model_search(uint32_t threadID, Type_Key targetKey, tuple<bool,int,int,int> & searchBound);

    //Update SWmeta helper functions
    int thread_dispatch_update_seg(uint32_t threadID, Type_Ts expiryTime, 
                                    vector<tuple<pswix::seg_update_type,int,Type_Key>> & updateSeg);
    void seg_retrain(uint32_t threadID, int startIndex, int endIndex, Type_Ts expiryTime,
                    vector<pair<Type_Key,SWseg<Type_Key,Type_Ts>*>> & retrainSeg);
public:
    //Meta insertions
    int thread_insert(uint32_t threadID, Type_Key key, SWseg<Type_Key,Type_Ts>* segPtr, bool currentRetrainStatus);

private:
    //Insertion helpers functions
    int insert_model(uint32_t threadID, int insertionPos, Type_Key key, SWseg<Type_Key,Type_Ts>* segPtr, bool addErrorFlag, bool currentRetrainStatus);
    int insert_model_no_gap(uint32_t threadID, int insertionPos, Type_Key key, SWseg<Type_Key,Type_Ts>* segPtr, bool addErrorFlag, bool currentRetrainStatus);
    int insert_end(uint32_t threadID, Type_Key key, SWseg<Type_Key,Type_Ts>* segPtr, bool currentRetrainStatus); //Only last partition calls
    int insert_append(uint32_t threadID, int insertionPos, Type_Key key, SWseg<Type_Key,Type_Ts>* segPtr, bool currentRetrainStatus); //Only last partition calls

public: 
    // Meta retrain
    void meta_retrain();

private:
    //Retrain helpers
    vector<tuple<key_type,SWseg<key_type,time_type>*,bool>> flatten_partition_retrain(Type_Ts expiryTime);
    void retrain_model_combine_data(Type_Ts expiryTime, vector<Type_Key> & tempKey, vector<SWseg<Type_Key,Type_Ts>*> & tempPtr);
    void retrain_combine_data(Type_Ts expiryTime, vector<Type_Key> & tempKey, vector<SWseg<Type_Key,Type_Ts>*> & tempPtr);
    void retrain_model_insert_segments(Type_Key currentKey, SWseg<key_type,time_type>* currentPtr, bool currentIsRetrain,
                                int &currentInsertionPos, int&lastNonGapIndex, int& dataIndex,
                                vector<Type_Key> & tempKey, vector<SWseg<Type_Key,Type_Ts>*> & tempPtr,
                                vector<uint64_t> & tempBitmap, vector<uint64_t> & tempRetrainBitmap);
    void retrain_insert_segments(Type_Key currentKey, SWseg<key_type,time_type>* currentPtr, bool currentIsRetrain,
                                int &currentInsertionPos, int& dataIndex,
                                vector<Type_Key> & tempKey, vector<SWseg<Type_Key,Type_Ts>*> & tempPtr,
                                vector<uint64_t> & tempBitmap, vector<uint64_t> & tempRetrainBitmap);

private: 
    //Util helper functions
    tuple<int,int,int> find_predict_pos_bound(Type_Key targetKey);
    int exponential_search_right(vector<Type_Key> & keys, Type_Key targetKey, int normalizedStartingPos, int maxSearchBound);
    int exponential_search_right_insert(uint32_t threadID, vector<Type_Key> & keys, Type_Key targetKey, int normalizedStartingPos, int maxSearchBound);
    int exponential_search_left(vector<Type_Key> & keys, Type_Key targetKey, int normalizedStartingPos, int maxSearchBound);
    int exponential_search_left_insert(uint32_t threadID, vector<Type_Key> & keys, Type_Key targetKey, int normalizedStartingPos, int maxSearchBound);

    uint32_t find_index_partition(int index);
    int find_first_segment_in_partition(uint32_t threadID);
    int find_last_segment_in_partition(uint32_t threadID);

    int find_closest_gap(int index);
    int find_closest_gap(int index, int leftBoundary, int rightBoundary);
    int find_closest_gap_within_thread(uint32_t threadID, int index);

    void update_segment_with_neighbours(uint32_t threadID, int index, int startIndex, int endIndex);
    void update_keys_with_previous(uint32_t threadID, int index);

    void flatten_partition_keys(vector<Type_Key> & data);

    bool borrow_gap_position_from_end(uint32_t threadID, int & gapPos);
    bool borrow_gap_position_from_begin(uint32_t threadID, int & gapPos);
    void update_seg_parent_index(uint32_t threadID, int startPos, int endPos, bool incrementFlag);
    void update_seg_parent_index_normalized(uint32_t threadID, int startPos, int endPos, bool incrementFlag);

    void lock_thread(uint32_t threadID, unique_lock<mutex> & lock);
    void unlock_thread(uint32_t threadID, unique_lock<mutex> & lock);

public:
    //Getters & Setters
    size_t get_meta_size();
    size_t get_no_seg();
    int get_split_error();
    void set_split_error(int error);
    void print();
    void print_all();
    uint64_t memory_usage();
    uint64_t get_no_keys(Type_Ts Timestamp);

private:
    //Bitmap functions
    bool bitmap_exists(int index) const;
    bool bitmap_exists(vector<uint64_t> & bitmap, int index) const;
    void bitmap_set_bit(int index);
    void bitmap_set_bit(vector<uint64_t> & bitmap, int index);
    void bitmap_erase_bit(int index);
    void bitmap_erase_bit(vector<uint64_t> & bitmap, int index);

    int bitmap_closest_left_nongap(int index, int leftBoundary);
    int bitmap_closest_right_nongap(int index, int rightBoundary);
    int bitmap_closest_left_gap(int index, int leftBoundary);
    int bitmap_closest_right_gap(int index, int rightBoundary);
    int bitmap_closest_gap(int index);
    int bitmap_closest_gap(int index, int leftBoundary, int rightBoundary);
    pair<int,int> bitmap_retrain_range(int index);
    void bitmap_move_bit_back(int startingIndex, int endingIndex);
    void bitmap_move_bit_back(vector<uint64_t> & bitmap, int startingIndex, int endingIndex);
    void bitmap_move_bit_front(int startingIndex, int endingIndex);
    void bitmap_move_bit_front(vector<uint64_t> & bitmap, int startingIndex, int endingIndex);
};

/*
Constructors & Deconstructors
*/
template<class Type_Key, class Type_Ts> 
SWmeta<Type_Key,Type_Ts>::SWmeta()
:m_rightSearchBound(0), m_leftSearchBound(0), m_numSeg(0), m_numSegExist(0), splitError(0), m_slope(-1), m_maxSearchError(0), 
 m_startKey(numeric_limits<Type_Key>::min()){}

template<class Type_Key, class Type_Ts> 
SWmeta<Type_Key,Type_Ts>::SWmeta(int numThreads, const pair<Type_Key, Type_Ts> & arrivalTuple)
:m_rightSearchBound(0), m_leftSearchBound(0), m_numSeg(0), m_numSegExist(0), m_slope(-1), m_maxSearchError(0), m_startKey(0)
{
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("SWmeta","Constructor(singleData)");
    #endif

    SWseg<Type_Key,Type_Ts> * SWsegPtr = new SWseg<Type_Key,Type_Ts>(arrivalTuple);
    SWsegPtr->m_leftSibling = nullptr;
    SWsegPtr->m_rightSibling = nullptr;
    SWsegPtr->m_parentIndex = 0;

    m_keys.push_back({arrivalTuple.first});
    m_ptr.push_back({SWsegPtr});

    m_startKey = arrivalTuple.first;
    m_numSeg = 1;
    m_numSegExist = 1;
    m_bitmap.push_back(0);
    m_retrainBitmap.push_back(0);
    bitmap_set_bit(0);

    m_partitionIndex.push_back(0);
    m_parititonMaxTime.push_back(0);
    m_partitionStartKey.push_back(m_startKey);

    m_numSegPerPartition.push_back(1);
    m_numSegExistsPerPartition.push_back(1);

    splitError = 64;

    #ifdef DEBUG
    DEBUG_EXIT_FUNCTION("SWmeta","Constructor(singleData)");
    #endif
}

template<class Type_Key, class Type_Ts> 
SWmeta<Type_Key,Type_Ts>::SWmeta(int numThreads, const vector<pair<Type_Key, Type_Ts>> & stream)
:m_rightSearchBound(0), m_leftSearchBound(0), m_numSeg(0), m_numSegExist(0), m_slope(0), m_maxSearchError(0)
{
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("SWmeta","Constructor(data)");
    #endif

    bulk_load(numThreads, stream);
    splitError = 64;

    #ifdef DEBUG
    DEBUG_EXIT_FUNCTION("SWmeta","Constructor(data)");
    #endif
}

template<class Type_Key, class Type_Ts> 
SWmeta<Type_Key,Type_Ts>::~SWmeta()
{
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("SWmeta","Deconstructor");
    #endif

    if (m_ptr.size() == 0)
    {
        return;
    }
    
    for (auto & partition: m_ptr)
    {
        for (auto & seg: partition)
        {
            if (seg != nullptr)
            {
                delete seg;
                seg = nullptr;
            }
        }
    }

    #ifdef DEBUG
    DEBUG_EXIT_FUNCTION("SWmeta","Deconstructor");
    #endif
}

/*
Bulk load & Retrain
*/
template<class Type_Key, class Type_Ts>
void SWmeta<Type_Key,Type_Ts>::bulk_load(int numThreads, const vector<pair<Type_Key, Type_Ts>> & stream)
{
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("SWmeta","bulk_load");
    #endif

    vector<pair<Type_Key, Type_Ts>> data(stream);

    sort(data.begin(), data.end());

    vector<pair<Type_Key, SWseg<Type_Key,Type_Ts> * >> splitedDataPtr;

    split_data(data, splitedDataPtr);

    vector<Type_Key> tempKeys;
    vector<SWseg<Type_Key,Type_Ts> *> tempPtr;

    tempKeys.reserve(splitedDataPtr.size()*1.05);
    tempPtr.reserve(splitedDataPtr.size()*1.05);

    if (m_slope != -1)
    {
        int currentIndex = 1;
        int currentInsertionPos = 0;
        int predictedPos;

        splitedDataPtr[0].second->m_parentIndex = currentInsertionPos;
        ++currentInsertionPos;

        tempKeys.push_back(splitedDataPtr[0].first);
        tempPtr.push_back(splitedDataPtr[0].second);
        m_startKey = splitedDataPtr[0].first;
        m_numSegExist = splitedDataPtr.size();
        m_bitmap.push_back(0);
        m_retrainBitmap.push_back(0);
        bitmap_set_bit(0);

        while (currentIndex < splitedDataPtr.size())
        {
            predictedPos = static_cast<int>(floor(m_slope * ((double)splitedDataPtr[currentIndex].first - (double)m_startKey)));

            if (static_cast<int>(currentInsertionPos >> 6) == m_bitmap.size())
            {
                m_bitmap.push_back(0);
                m_retrainBitmap.push_back(0);
            }

            if(predictedPos == currentInsertionPos)
            {
                splitedDataPtr[currentIndex].second->m_parentIndex = currentInsertionPos;
                
                tempKeys.push_back(splitedDataPtr[currentIndex].first);
                tempPtr.push_back(splitedDataPtr[currentIndex].second);
                bitmap_set_bit(currentInsertionPos);

                ++currentIndex;
            }
            else if (predictedPos < currentInsertionPos)
            {
                splitedDataPtr[currentIndex].second->m_parentIndex = currentInsertionPos;
                
                tempKeys.push_back(splitedDataPtr[currentIndex].first);
                tempPtr.push_back(splitedDataPtr[currentIndex].second);
                bitmap_set_bit(currentInsertionPos);

                m_rightSearchBound = (currentInsertionPos - predictedPos) > m_rightSearchBound.load() 
                                    ? (currentInsertionPos - predictedPos) : m_rightSearchBound.load();
                
                ++currentIndex;
            }
            else
            {
                tempKeys.push_back(tempKeys.back());
                tempPtr.push_back(nullptr);
            }
            ++currentInsertionPos;
        }   
        m_rightSearchBound += 1;
        m_numSeg = tempKeys.size();
        m_maxSearchError = min(8192,(int)ceil(0.6*m_numSeg));
    }
    else //Single Segment in SWmeta
    {        
        int bitmapSize = ceil(MAX_BUFFER_SIZE/64.);
        vector<uint64_t> temp(bitmapSize,0);
        
        m_startKey = splitedDataPtr[0].first;
        m_numSeg = 1;
        m_numSegExist = 1;
        m_bitmap = temp;
        m_retrainBitmap = temp;
        bitmap_set_bit(0);
        
        splitedDataPtr[0].second->m_parentIndex = 0;
        tempKeys.push_back(splitedDataPtr[0].first);
        tempPtr.push_back(splitedDataPtr[0].second);
    }
    tempKeys.shrink_to_fit();
    tempPtr.shrink_to_fit();

    partition_data_into_threads(numThreads, tempKeys, tempPtr);

    #ifdef DEBUG
    DEBUG_EXIT_FUNCTION("SWmeta","bulk_load");
    #endif
}

template<class Type_Key, class Type_Ts>
void SWmeta<Type_Key,Type_Ts>::split_data(const vector<pair<Type_Key,Type_Ts>> & data, 
                                vector<pair<Type_Key, SWseg<Type_Key,Type_Ts> * >> & splitedDataPtr)
{  
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("SWmeta","split_data");
    #endif

    vector<tuple<int,int,double>> splitIndexSlopeVector;
    calculate_split_derivative((int)(TIME_WINDOW * 0.00001), data, splitIndexSlopeVector);

    if (splitIndexSlopeVector.size() != 1)
    {
        Type_Key firstKey = data[get<0>(splitIndexSlopeVector.front())].first;
        double normalizedKey = 0, sumKey = 0, sumIndex = 0, sumKeyIndex = 0, sumKeySquared = 0;
        int cnt = 0;
        
        for (auto it = splitIndexSlopeVector.begin(); it != splitIndexSlopeVector.end()-1; it++)
        {
            SWseg<Type_Key,Type_Ts> * SWsegPtr = new SWseg<Type_Key,Type_Ts>(get<0>(*it), get<1>(*it) ,get<2>(*it), data);
            if(splitedDataPtr.size() > 0)
            {
                SWsegPtr->m_leftSibling = splitedDataPtr[splitedDataPtr.size() - 1].second;
                splitedDataPtr[splitedDataPtr.size() - 1].second->m_rightSibling = SWsegPtr;
            }
            splitedDataPtr.push_back(make_pair( data[get<0>(*it)].first, SWsegPtr));

            normalizedKey = (double)data[get<0>(*it)].first - (double)firstKey;
            sumKey += normalizedKey;
            sumIndex += cnt;
            sumKeyIndex += normalizedKey * cnt;
            sumKeySquared += pow(normalizedKey,2);
            ++cnt;
        }

        //Dealing with last segment with only one point
        if (get<0>(splitIndexSlopeVector.back())  != get<1>(splitIndexSlopeVector.back()))
        {
            SWseg<Type_Key,Type_Ts> * SWsegPtr = new SWseg<Type_Key,Type_Ts>(get<0>(splitIndexSlopeVector.back()), get<1>(splitIndexSlopeVector.back()) ,get<2>(splitIndexSlopeVector.back()), data);
            if(splitedDataPtr.size() > 0)
            {
                SWsegPtr->m_leftSibling = splitedDataPtr[splitedDataPtr.size() - 1].second;
                splitedDataPtr[splitedDataPtr.size() - 1].second->m_rightSibling = SWsegPtr;
            }
            splitedDataPtr.push_back(make_pair( data[get<0>(splitIndexSlopeVector.back())].first, SWsegPtr));

            normalizedKey = (double)data[get<0>(splitIndexSlopeVector.back())].first - (double)firstKey;
            sumKey += normalizedKey;
            sumIndex += cnt;
            sumKeyIndex += normalizedKey * cnt;
            sumKeySquared += pow(normalizedKey,2);
            ++cnt;
        }
        else //Single Point 
        {        
            SWseg<Type_Key,Type_Ts> * SWsegPtr = new SWseg<Type_Key,Type_Ts>(data.back());
            if(splitedDataPtr.size() > 0)
            {
                SWsegPtr->m_leftSibling = splitedDataPtr[splitedDataPtr.size() - 1].second;
                splitedDataPtr[splitedDataPtr.size() - 1].second->m_rightSibling = SWsegPtr;
            }
            splitedDataPtr.push_back(make_pair( data.back().first, SWsegPtr));

            normalizedKey = (double)data.back().first - (double)firstKey;
            sumKey += normalizedKey;
            sumIndex += cnt;
            sumKeyIndex += normalizedKey * cnt;
            sumKeySquared += pow(normalizedKey,2);
            ++cnt;
        }

        m_slope =  (sumKeyIndex - sumKey *(sumIndex/cnt))/(sumKeySquared - sumKey*(sumKey/cnt)) * 1.05;
    }
    else //Single Segment in SWmeta
    {
        if (get<0>(splitIndexSlopeVector[0]) != get<1>(splitIndexSlopeVector[0]))
        {
            SWseg<Type_Key,Type_Ts> * SWsegPtr = new SWseg<Type_Key,Type_Ts>(get<0>(splitIndexSlopeVector[0]), get<1>(splitIndexSlopeVector[0]), get<2>(splitIndexSlopeVector[0]), data);
            splitedDataPtr.push_back(make_pair( data[get<0>(splitIndexSlopeVector[0])].first, SWsegPtr));
        }
        else
        {
            printf("WARNING: entire data is one point \n");
            SWseg<Type_Key,Type_Ts> * SWsegPtr = new SWseg<Type_Key,Type_Ts>(data.back());
            splitedDataPtr.push_back(make_pair( data.back().first, SWsegPtr));
        }
        m_slope = -1;
    }

    if (splitedDataPtr.front().second->m_leftSibling != nullptr)
    {
        printf("WARNING: left sibling of first segment not null \n");
    }

    if (splitedDataPtr.back().second->m_rightSibling != nullptr)
    {
        printf("WARNING: right sibling of last segmentnot null \n");
    }

    #ifdef DEBUG
    DEBUG_EXIT_FUNCTION("SWmeta","split_data");
    #endif
}

/*
Parition data for threads
*/
template<class Type_Key, class Type_Ts>
void SWmeta<Type_Key,Type_Ts>::partition_data_into_threads(int numThreads, vector<Type_Key> & keys, vector<SWseg<Type_Key,Type_Ts>*> & ptr)
{
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("SWmeta","partition_data_into_threads");
    #endif

    if (m_numSeg == 1 || m_slope == -1)
    {
        m_partitionIndex = {0};

        m_parititonMaxTime = {0};
        m_partitionStartKey = {m_startKey};

        m_numSegPerPartition = {1};
        m_numSegExistsPerPartition = {1};
        return;
    }

    //Equal Split Segments into Threads (Simple Partitioning)
    int numSegmentsPerThread = m_numSeg / numThreads;
    int maxPartitionSize = numSegmentsPerThread;

    m_partitionStartKey = vector<Type_Key>(numThreads);
    m_partitionIndex = vector<int>(numThreads);
    for (int i = 0; i < numThreads; ++i)
    {
        int index = numSegmentsPerThread*i; //Starting index must be non-negative (or else the key may be same as max key in previous partition)

        if (ptr[index] == nullptr)
        {
            index = bitmap_closest_gap(index);
            ASSERT_MESSAGE(index != -1, "foundPos is -1, entire partition contains empty segments");
        }

        if (i != 0)
        {
            maxPartitionSize = max(maxPartitionSize, ((i == numThreads-1)? m_numSeg.load(): index)- m_partitionIndex[i-1]);
        }
        m_partitionIndex[i] = index;
        m_partitionStartKey[i] = ptr[index]->m_currentNodeStartKey;
    }

    //Insert into partitioned m_keys and m_ptr
    m_keys = vector<vector<Type_Key>>(numThreads,vector<Type_Key>());
    m_ptr = vector<vector<SWseg<Type_Key,Type_Ts>*>>(numThreads,vector<SWseg<Type_Key,Type_Ts>*>());
    m_parititonMaxTime = vector<Type_Ts>(numThreads,1); //No need to update, since used for retrain
    m_numSegPerPartition = vector<int>(numThreads);
    m_numSegExistsPerPartition = vector<int>(numThreads);

    for (int i = 0; i < numThreads; ++i)
    {
        int startIndex = m_partitionIndex[i];
        int endIndex = (i == numThreads-1)? m_numSeg-1 : m_partitionIndex[i+1]-1;

        int partNumSeg = 0;
        int partNumSegExists = 0;

        m_keys[i].reserve(maxPartitionSize);
        m_ptr[i].reserve(maxPartitionSize);

        for (int currentIndex = startIndex; currentIndex <= endIndex; ++ currentIndex)
        {
            m_keys[i].push_back(keys[currentIndex]);
            m_ptr[i].push_back(ptr[currentIndex]);

            if (ptr[currentIndex] != nullptr) { ++partNumSegExists;}
            ++partNumSeg;
        }

        m_numSegPerPartition[i] = partNumSeg;
        m_numSegExistsPerPartition[i] = partNumSegExists;
    }

    keys.clear();
    ptr.clear();

    #ifdef DEBUG
    DEBUG_EXIT_FUNCTION("SWmeta","partition_data_into_threads");
    #endif
}

/*
Predict (Returns the thread responsible for the query)
*/
template<class Type_Key, class Type_Ts>
uint32_t  SWmeta<Type_Key,Type_Ts>::predict_thread(Type_Key key)
{
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("SWmeta","predict_thread(key)");
    #endif

    if (m_numSeg == 1 || m_slope == -1)
    {
        return 0; 
    }

    int threadID = 0;
    for (int i = 1; i < m_partitionIndex.size(); ++i)
    {
        if (key < m_partitionStartKey[i])
        {
            break;
        }
        ++threadID;
    }

    return threadID;

    #ifdef DEBUG
    DEBUG_EXIT_FUNCTION("SWmeta","predict_thread(key)");
    #endif
}

template<class Type_Key, class Type_Ts>
uint32_t SWmeta<Type_Key,Type_Ts>::predict_thread(Type_Key key, tuple<bool,int,int,int> & predictBound)
{
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("SWmeta","predict_thread(key, predictBound for meta)");
    #endif

    if (m_numSeg == 1 || m_slope == -1)
    {
        predictBound = make_tuple(false,0,0,m_numSeg-1);
        return 0;
    }

    tuple<int,int,int> predictPosBound = find_predict_pos_bound(key);

    int threadID = 0;
    for (int i = 1; i < m_partitionIndex.size(); ++i)
    {
        if (key < m_partitionStartKey[i])
        {
            break;
        }
        ++threadID;
    }

    int startIndex = m_partitionIndex[threadID];
    int endIndex = (threadID == m_partitionIndex.size()-1)? m_numSeg-1 : m_partitionIndex[threadID+1]-1;

    int startPredictBound = max(startIndex,get<1>(predictPosBound));
    int endPredictBound = min(endIndex,get<2>(predictPosBound)); 

     if (endPredictBound <= startPredictBound)
    {
        if (startPredictBound == startIndex)
        {
            predictBound = make_tuple(false, get<0>(predictPosBound), startIndex, startIndex);
        }
        else if (endPredictBound == endIndex)
        {
            predictBound = make_tuple(false, get<0>(predictPosBound), endIndex, endIndex);
        }
        else
        {
            LOG_ERROR("SWmeta [predict_thread] : endPredictBound (%i) <= startPredictBound (%i) but not at start or end of thread \n",
                        endPredictBound, startPredictBound);
            abort();
        }
    }
    else
    {
        bool expSearchFlag = (startIndex <= get<0>(predictPosBound) && get<0>(predictPosBound) <= endIndex);
        predictBound = make_tuple(expSearchFlag, get<0>(predictPosBound), startPredictBound, endPredictBound);
    }

    return threadID;

    #ifdef DEBUG
    DEBUG_EXIT_FUNCTION("SWmeta","predict_thread(key, predictBound for meta)");
    #endif
}

template<class Type_Key, class Type_Ts>
vector<uint32_t>  SWmeta<Type_Key,Type_Ts>::predict_thread(Type_Key lowerBound, Type_Key upperBound, vector<tuple<bool,int,int,int>> & predictBound)
// tuple<expSearchFlag,predictBound,predictBoundMin,predictBoundMax> //If predictBound = -1 (scan thread)
{
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("SWmeta","predict_thread(lowerBound,upperBound,predictBound)");
    #endif

    if (m_numSeg == 1 || m_slope == -1)
    {
        predictBound.push_back(make_tuple(false,0,0,m_numSeg-1));
        return {0};
    }

    tuple<bool,int,int,int> singlePredictBound;
    uint32_t lowerBoundThread = predict_thread(lowerBound,singlePredictBound);
    uint32_t upperBoundThread = predict_thread(upperBound);
    
    vector<uint32_t> threadList = {lowerBoundThread};
    threadList.reserve(upperBoundThread - lowerBoundThread + 1);
    predictBound.reserve(upperBoundThread - lowerBoundThread + 1);
    predictBound.push_back(singlePredictBound);

    for (int i = lowerBoundThread+1; i <= upperBoundThread; ++i)
    {
        predictBound.push_back(make_tuple(false,-1,
                        (i == 0)? 0 : m_partitionIndex[i],
                        (i == m_partitionIndex.size()-1)? m_numSeg-1 : m_partitionIndex[i+1]-1));

        threadList.push_back(i);
    }

    return threadList;

    #ifdef DEBUG
    DEBUG_EXIT_FUNCTION("SWmeta","predict_thread(lowerBound,upperBound,predictBound)");
    #endif
}


/*
Predict if key is within current thread
Used when worker threads recieve queries directly from dispatcher
*/
template<class Type_Key, class Type_Ts>
bool SWmeta<Type_Key,Type_Ts>::within_thread(uint32_t threadID, Type_Key key, tuple<bool,int,int,int> & predictBound)
{
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("SWmeta","within_thread(key)");
    #endif

    if (m_numSeg == 1 || m_slope == -1)
    {
        predictBound = make_tuple(false,0, 0, m_numSeg-1);
        return (threadID == 0);
    }

    int threadIDFound = 0;
    for (int i = 1; i < m_partitionIndex.size(); ++i)
    {
        if (key < m_partitionStartKey[i])
        {
            break;
        }
        ++threadIDFound;
    }

    if (threadID != threadIDFound) {return false;}

    tuple<int,int,int> predictPosBound = find_predict_pos_bound(key);

    int startIndex = m_partitionIndex[threadID];
    int endIndex = (threadID == m_partitionIndex.size()-1)? m_numSeg-1 : m_partitionIndex[threadID+1]-1;

    int startPredictBound = max(startIndex,get<1>(predictPosBound));
    int endPredictBound = min(endIndex,get<2>(predictPosBound)); 

    if (endPredictBound <= startPredictBound)
    {
        if (startPredictBound == startIndex)
        {
            predictBound = make_tuple(false, get<0>(predictPosBound), startIndex, startIndex);
        }
        else if (endPredictBound == endIndex)
        {
            predictBound = make_tuple(false, get<0>(predictPosBound), endIndex, endIndex);
        }
        else
        {
            LOG_ERROR("SWmeta [within_thread] : endPredictBound (%i) <= startPredictBound (%i) but not at start or end of thread \n",
                            endPredictBound, startPredictBound);
            abort();
        }
    }
    else
    {
        bool expSearchFlag = (startIndex <= get<0>(predictPosBound) && get<0>(predictPosBound) <= endIndex);
        predictBound = make_tuple(expSearchFlag, get<0>(predictPosBound), startPredictBound, endPredictBound);
    }
    return true;

    
    #ifdef DEBUG
    DEBUG_EXIT_FUNCTION("SWmeta","within_thread");
    #endif
}

template<class Type_Key, class Type_Ts>
bool SWmeta<Type_Key,Type_Ts>::within_thread(uint32_t threadID, Type_Key lowerBound, Type_Key upperBound, tuple<bool,int,int,int> & predictBound)
//get<1>predictBound == -1 indicates scans
{
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("SWmeta","within_thread(lowerBound,upperBound)");
    #endif

    if (upperBound == numeric_limits<Type_Key>::max()) //Insertion case (must match with dispatcher)
    {
        return within_thread(threadID,lowerBound,predictBound);
    }

    if (m_numSeg == 1 || m_slope == -1)
    {
        predictBound = make_tuple(false,0, 0, m_numSeg-1);
        return (threadID == 0);
    }

    tuple<bool,int,int,int> predictBoundLower;
    uint32_t lowerBoundThread = predict_thread(lowerBound,predictBoundLower);
    uint32_t upperBoundThread = predict_thread(upperBound);
    ASSERT_MESSAGE(lowerBoundThread <= upperBoundThread, "SWmeta [within_thread] : lowerBoundThread > upperBoundThread \n");

    if (lowerBoundThread <= threadID && threadID <= upperBoundThread)
    {
        if (threadID == lowerBoundThread)
        {
            predictBound = predictBoundLower;
        }
        else
        {
            predictBound = make_tuple(false,-1,
                        m_partitionIndex[threadID],
                        (threadID == m_partitionIndex.size()-1)? m_numSeg-1 : m_partitionIndex[threadID+1]-1);
        }
        return true;
    }
    else
    {
        return false;
    }

    #ifdef DEBUG
    DEBUG_EXIT_FUNCTION("SWmeta","within_thread(lowerBound,upperBound)");
    #endif
}


/*
Lookup
*/
template<class Type_Key, class Type_Ts>
int SWmeta<Type_Key,Type_Ts>::lookup(uint32_t threadID, Type_Key key, Type_Ts timestamp, tuple<bool,int,int,int> & predictBound)
{
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("SWmeta","lookup");
    #endif

    #if defined(DEBUG_KEY) || defined(DEBUG_TS) 
    if(DEBUG_KEY == newKey) 
    {
        DEBUG_ENTER_FUNCTION("SWmeta","lookup");
    }
    #endif

    int count = 0;
    int startIndex = m_partitionIndex[threadID];
    tuple<pswix::seg_update_type,int,Type_Key> updateSeg = make_tuple(pswix::seg_update_type::NONE,0,0);
    Type_Ts expiryTime =  ((double)timestamp - TIME_WINDOW < numeric_limits<Type_Ts>::min()) ? numeric_limits<Type_Ts>::min(): timestamp - TIME_WINDOW;
    
    unique_lock<mutex> lock(thread_lock[threadID]);
    lock_thread(threadID, lock);

    if (m_numSeg == 1)
    {
        count = m_ptr[0][0]->lookup(0,m_numSeg-1,key,expiryTime,updateSeg);
    }
    else
    {
        int foundPos;
        if (threadID == 0 && key < m_startKey)
        {
            foundPos = 0; 
        }
        else
        {
            if (m_slope != -1)
            {
                foundPos = meta_model_search(threadID, key, predictBound);
            }
            else
            {
                foundPos = meta_non_model_search(threadID, key, predictBound);
            }
        }

        if (!bitmap_exists(foundPos))
        {
            foundPos = bitmap_closest_left_nongap(foundPos , startIndex);
            if (foundPos == -1)
            {
                unlock_thread(threadID, lock);
                return 0;
            }
        }

        count = m_ptr[threadID][foundPos-startIndex]->lookup(find_first_segment_in_partition(threadID), find_last_segment_in_partition(threadID),
                                        key,expiryTime,updateSeg);
    }

    if (get<0>(updateSeg) != pswix::seg_update_type::NONE)
    {
        vector<tuple<pswix::seg_update_type,int,Type_Key>> updateSegVector = {updateSeg};
        int metaRetrainStatus = thread_dispatch_update_seg(threadID, expiryTime, updateSegVector);
    
        if (metaRetrainStatus > 0 && thread_retraining == -1)
        {
            thread_retraining = threadID*10 + metaRetrainStatus;
        }
    }
    unlock_thread(threadID, lock);

    return count;

    #ifdef DEBUG
    DEBUG_EXIT_FUNCTION("SWmeta","lookup");
    #endif

    #if defined(DEBUG_KEY) || defined(DEBUG_TS) 
    if(DEBUG_KEY == newKey) 
    {
        DEBUG_EXIT_FUNCTION("SWmeta","lookup");
    }
    #endif
}

/*
Range Query
*/
template<class Type_Key, class Type_Ts>
int SWmeta<Type_Key,Type_Ts>::range_query(uint32_t threadID, Type_Key lowerBound, Type_Ts timestamp, Type_Key upperBound, tuple<bool,int,int,int> & predictBound)
{
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("SWmeta","range_query");
    #endif

    #if defined(DEBUG_KEY) || defined(DEBUG_TS) 
    if(DEBUG_KEY == lowerBound) 
    {
        DEBUG_ENTER_FUNCTION("SWmeta","range_query");
    }
    #endif

    int count = 0;
    int startIndex = m_partitionIndex[threadID];
    vector<tuple<pswix::seg_update_type,int,Type_Key>> updateSeg;
    Type_Ts expiryTime =  ((double)timestamp - TIME_WINDOW < numeric_limits<Type_Ts>::min()) ? numeric_limits<Type_Ts>::min(): timestamp - TIME_WINDOW;
    
    unique_lock<mutex> lock(thread_lock[threadID]);
    lock_thread(threadID, lock);

    if (m_numSeg == 1)
    {
        count = m_ptr[0][0]->range_search(0,m_numSeg-1,lowerBound,expiryTime,upperBound,updateSeg);
    }
    else
    {
        int foundPos;

        //Search
        if (get<1>(predictBound) != -1)
        {
            if (threadID == 0 && lowerBound < m_startKey)
            {
                foundPos = 0; 
            }
            else
            {
                if (m_slope != -1)
                {
                    foundPos = meta_model_search(threadID, lowerBound, predictBound);
                }
                else
                {
                    foundPos = meta_non_model_search(threadID, lowerBound, predictBound);
                }
            }

            if (!bitmap_exists(foundPos))
            {
                foundPos = bitmap_closest_left_nongap(foundPos, startIndex);
                if (foundPos == -1)
                {
                    foundPos = bitmap_closest_right_nongap(foundPos, 
                        (threadID == m_partitionIndex.size()-1)? m_numSeg-1:m_partitionIndex[threadID+1]-1);
                    if (foundPos == -1 || m_keys[threadID][foundPos-startIndex] > upperBound) 
                    {
                        unlock_thread(threadID, lock);
                        return 0; 
                    }
                }
            }

            count = m_ptr[threadID][foundPos-startIndex]->range_search(find_first_segment_in_partition(threadID), find_last_segment_in_partition(threadID),
                                        lowerBound,expiryTime,upperBound,updateSeg);
        }
        //Scan (get<1>predictBound == -1 indicates scans)
        else
        {
            foundPos = get<2>(predictBound);
            if (!bitmap_exists(foundPos))
            {
                foundPos = bitmap_closest_right_nongap(foundPos, get<3>(predictBound));
                if (foundPos == -1 || m_keys[threadID][foundPos-startIndex] > upperBound) 
                {
                    unlock_thread(threadID, lock);
                    return 0; 
                }
            }

            count = m_ptr[threadID][foundPos-startIndex]->range_scan(find_first_segment_in_partition(threadID), find_last_segment_in_partition(threadID),
                                        lowerBound,expiryTime,upperBound,updateSeg);
        }
    }

    if (updateSeg.size() > 0)
    {
        int metaRetrainStatus = thread_dispatch_update_seg(threadID, expiryTime, updateSeg);

        if (metaRetrainStatus > 0 && thread_retraining == -1)
        {
            thread_retraining = threadID*10 + metaRetrainStatus;
        }
    }
    unlock_thread(threadID,lock);

    return count;

    #ifdef DEBUG
    DEBUG_EXIT_FUNCTION("SWmeta","range_query");
    #endif

    #if defined(DEBUG_KEY) || defined(DEBUG_TS) 
    if(DEBUG_KEY == lowerBound) 
    {
        DEBUG_EXIT_FUNCTION("SWmeta","range_query");
    }
    #endif
}

/*
Insertion 
*/
template<class Type_Key, class Type_Ts>
int SWmeta<Type_Key,Type_Ts>::insert(uint32_t threadID, Type_Key key, Type_Ts timestamp, tuple<bool,int,int,int> & predictBound)
{
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("SWmeta","insert");
    #endif

    #if defined(DEBUG_KEY) || defined(DEBUG_TS) 
    if(DEBUG_KEY == key) 
    {
        DEBUG_ENTER_FUNCTION("SWmeta","insert");
    }
    #endif

    int startIndex = m_partitionIndex[threadID];

    vector<tuple<pswix::seg_update_type,int,Type_Key>> updateSeg;
    Type_Ts expiryTime =  ((double)timestamp - TIME_WINDOW < numeric_limits<Type_Ts>::min()) ? numeric_limits<Type_Ts>::min(): timestamp - TIME_WINDOW;
    
    m_parititonMaxTime[threadID] = timestamp;

    unique_lock<mutex> lock(thread_lock[threadID]);
    lock_thread(threadID, lock);

    if (m_numSeg == 1)
    {
        m_ptr[0][0]->insert(0,m_numSeg-1,key,timestamp,expiryTime,updateSeg);
    }
    else
    {
        int foundPos;
        if (threadID == 0 && key < m_startKey)
        {
            foundPos = 0; 
        }
        else
        {
            if (m_slope != -1)
            {
                foundPos = meta_model_search(threadID, key, predictBound);
            }
            else
            {
                foundPos = meta_non_model_search(threadID, key, predictBound);
            }
        }

        if (!bitmap_exists(foundPos))
        {
            foundPos = bitmap_closest_left_nongap(foundPos, startIndex);
            if (foundPos == -1)
            {
                foundPos = bitmap_closest_right_nongap(foundPos, 
                    (threadID == m_partitionIndex.size()-1)? m_numSeg-1:m_partitionIndex[threadID+1]-1);

                ASSERT_MESSAGE(foundPos != -1, "SWmeta::insert: entire partition is empty, cannot insert");
            }
        }

        m_ptr[threadID][foundPos-startIndex]->insert(find_first_segment_in_partition(threadID), find_last_segment_in_partition(threadID),
                                key,timestamp,expiryTime,updateSeg);
    }

    if (updateSeg.size() > 0)
    {
        int metaRetrainStatus = thread_dispatch_update_seg(threadID, expiryTime, updateSeg);

        if (metaRetrainStatus > 0 || thread_retraining == -1)
        {
            thread_retraining = threadID*10 + metaRetrainStatus;
        }
    }
    unlock_thread(threadID,lock);

    return 1;

    #ifdef DEBUG
    DEBUG_EXIT_FUNCTION("SWmeta","insert");
    #endif

    #if defined(DEBUG_KEY) || defined(DEBUG_TS) 
    if(DEBUG_KEY == key) 
    {
        DEBUG_EXIT_FUNCTION("SWmeta","insert");
    }
    #endif
}

/*
Search Helpers
*/
template<class Type_Key, class Type_Ts>
inline int SWmeta<Type_Key,Type_Ts>::meta_model_search(uint32_t threadID, Type_Key targetKey, tuple<bool,int,int,int> & predictBound)
{
    //Checking starting segment range
    if (threadID == 0 && targetKey < m_partitionStartKey[0])
    {
        return 0;
    }

    int startIndex = m_partitionIndex[threadID];
    vector<Type_Key> & keys = m_keys[threadID];

    //Single point
    if (get<2>(predictBound) == get<3>(predictBound))
    {
        return get<2>(predictBound);
    }

    //Search range
    if (get<0>(predictBound)) //exponential search
    {
        if (get<2>(predictBound) < get<3>(predictBound))
        {
            int foundPos = get<1>(predictBound);
            foundPos = (foundPos < get<2>(predictBound))? get<2>(predictBound) : foundPos;
            foundPos = (foundPos > get<3>(predictBound))? get<3>(predictBound) : foundPos;

            if (targetKey < keys[foundPos-startIndex])
            {
                return exponential_search_left(keys,targetKey,foundPos-startIndex,foundPos-get<2>(predictBound)) + startIndex;
            }
            else if (targetKey > keys[foundPos-startIndex])
            {
                return exponential_search_right(keys,targetKey,foundPos-startIndex,get<3>(predictBound)-foundPos) + startIndex;
            }
            return foundPos;
        }
        else if (get<2>(predictBound) == get<3>(predictBound))
        {
            if (targetKey < keys[get<1>(predictBound)-startIndex])
            {
                return get<1>(predictBound)-1;
            }
            else
            {
                return get<1>(predictBound);
            }
        }
        else
        {
            return get<3>(predictBound);
        }
    }
    else //binary search
    {
        auto lowerBoundIt = lower_bound(keys.begin()+(get<2>(predictBound)-startIndex),keys.begin()+(get<3>(predictBound)+1-startIndex),targetKey);
 
        if (lowerBoundIt == keys.begin()+(get<3>(predictBound)+1-startIndex) || 
                ((lowerBoundIt != keys.begin()+(get<2>(predictBound)-startIndex)) && (*lowerBoundIt > targetKey)))
        {
            lowerBoundIt--;
        }

        return (lowerBoundIt - keys.begin()) + startIndex;
    }
}

template<class Type_Key, class Type_Ts>
inline int SWmeta<Type_Key,Type_Ts>::meta_non_model_search(uint32_t threadID, Type_Key targetKey, tuple<bool,int,int,int> & searchBound)
{
    //Checking starting segment range
    if (threadID == 0 && targetKey < m_partitionStartKey[0])
    {
        return 0;
    }

    int startIndex = m_partitionIndex[threadID];
    vector<Type_Key> & keys = m_keys[threadID];

    //Single point
    if (get<2>(searchBound) == get<3>(searchBound))
    {
        return get<2>(searchBound);
    }

    //Within range
    auto lowerBoundIt = lower_bound(keys.begin()+(get<2>(searchBound)-startIndex),keys.begin()+(get<3>(searchBound)+1-startIndex),targetKey);

    if (lowerBoundIt == keys.begin()+(get<3>(searchBound)+1-startIndex) || ((lowerBoundIt != keys.begin()+(get<2>(searchBound)-startIndex)) && (*lowerBoundIt > targetKey)))
    {
        lowerBoundIt--;
    }

    return (lowerBoundIt - keys.begin()) + startIndex;
}

/*
Update Segments from Meta and Rendezvous Retrain within thread range
*/
template<class Type_Key, class Type_Ts>
int SWmeta<Type_Key,Type_Ts>::thread_dispatch_update_seg(uint32_t threadID, Type_Ts expiryTime, 
                                                            vector<tuple<pswix::seg_update_type,int,Type_Key>> & updateSeg)
{
    int metaRetrainStatus = 0;
    int startIndex = m_partitionIndex[threadID];
    
    int index;
    vector<int> retrainSegmentIndex;
    retrainSegmentIndex.reserve(updateSeg.size());
    for (auto & segment: updateSeg)
    {
        index = get<1>(segment);
        switch (get<0>(segment))
        {
            case pswix::seg_update_type::RETRAIN: //Rendezvous Retrain (flag first time)
            {
                if (bitmap_exists(m_retrainBitmap,index))
                {
                    retrainSegmentIndex.push_back(index);
                }
                else
                {
                    bitmap_set_bit(m_retrainBitmap,index);
                }
                break;
            }
            case pswix::seg_update_type::REPLACE: //Replace Segment with reinsertion
            {
                bool currentRetrainStatus = (bitmap_exists(m_retrainBitmap,index));
                SWseg<Type_Key,Type_Ts>* segPtr = m_ptr[threadID][index-startIndex];

                m_ptr[threadID][index-startIndex] = nullptr;
                bitmap_erase_bit(index);
                bitmap_erase_bit(m_retrainBitmap,index);
                update_keys_with_previous(threadID, index);
                --m_numSegExist;
                --m_numSegExistsPerPartition[threadID];

                metaRetrainStatus = thread_insert(threadID, get<2>(segment), segPtr, currentRetrainStatus);
                break;
            }
            case pswix::seg_update_type::DELETE: //Delete Segments
            {
                delete m_ptr[threadID][index-startIndex];
                m_ptr[threadID][index-startIndex] = nullptr;
                bitmap_erase_bit(index);
                bitmap_erase_bit(m_retrainBitmap,index);
                update_keys_with_previous(threadID, index);
                --m_numSegExist;
                --m_numSegExistsPerPartition[threadID];

                break;
            }
            default:
            {
                LOG_ERROR("SWmeta::thread_update_seg: Unknown seg_update_type");
                break;
            }
        }
    }

    //Retrain Segments and Update Meta
    vector<pair<Type_Key,SWseg<Type_Key,Type_Ts>*>>  retrainSeg;
    for (auto & index: retrainSegmentIndex)
    {
        if (bitmap_exists(m_retrainBitmap,index))
        {
            #ifdef TUNE
            segNoRetrain++;
            #endif
            
            int startIndex, endIndex;
            tie(startIndex,endIndex) = bitmap_retrain_range(index);
            startIndex = max(startIndex,(threadID == 0)? 0 : m_partitionIndex[threadID]);
            endIndex = min(endIndex,(threadID == m_partitionIndex.size()-1)? m_numSeg-1 : m_partitionIndex[threadID+1]-1);

            seg_retrain(threadID, startIndex, endIndex, expiryTime, retrainSeg);

            //Delete old segments
            if (startIndex == endIndex)
            {
                delete m_ptr[threadID][index-startIndex];
                m_ptr[threadID][index-startIndex] = nullptr;
                bitmap_erase_bit(index);
                bitmap_erase_bit(m_retrainBitmap,index);
                --m_numSegExist;
                --m_numSegExistsPerPartition[threadID];
            }
            else
            {
                for (int i = startIndex; i <= endIndex; ++i)
                {
                    if (bitmap_exists(i))
                    {
                        delete m_ptr[threadID][i-startIndex];
                        m_ptr[threadID][i-startIndex] = nullptr;
                        bitmap_erase_bit(i);
                        bitmap_erase_bit(m_retrainBitmap,i);
                        --m_numSegExist;
                        --m_numSegExistsPerPartition[threadID];
                    }
                }
            }
            update_keys_with_previous(threadID, index);
        }
    }

    for (auto & segment: retrainSeg)
    {
        metaRetrainStatus = max(metaRetrainStatus, thread_insert(threadID, segment.first, segment.second, false));
    }

    return metaRetrainStatus;
}


//Retrain Segments
template<class Type_Key, class Type_Ts>
void SWmeta<Type_Key,Type_Ts>::seg_retrain(uint32_t threadID, int startIndex, int endIndex, Type_Ts expiryTime, vector<pair<Type_Key,SWseg<Type_Key,Type_Ts>*>> & retrainSeg)
{  
    int firstSegmentIndex = m_partitionIndex[threadID];
    
    vector<pair<Type_Key,Type_Ts>> data;
    int firstSegment = find_first_segment_in_partition(threadID);
    int lastSegment = find_last_segment_in_partition(threadID);

    //Retrain Alone
    if (startIndex == endIndex)
    {
        m_ptr[threadID][startIndex-firstSegmentIndex]->merge_data(expiryTime,data,firstSegment,lastSegment);
    }
    else //Retrain with neighbours
    {
        while(startIndex <= endIndex)
        {
            if (bitmap_exists(startIndex))
            { 
                m_ptr[threadID][startIndex-firstSegmentIndex]->merge_data(expiryTime,data,firstSegment,lastSegment);
            }
            ++startIndex;
        }
    }

    vector<tuple<int,int,double>> splitIndexSlopeVector;
    calculate_split_index_one_pass_least_sqaure(data,splitIndexSlopeVector,splitError);

    retrainSeg.reserve(retrainSeg.size() + splitIndexSlopeVector.size());
    for (auto it = splitIndexSlopeVector.begin(); it != splitIndexSlopeVector.end()-1; it++)
    {
        SWseg<Type_Key,Type_Ts>* SWsegPtr = new SWseg<Type_Key,Type_Ts>(get<0>(*it), get<1>(*it) ,get<2>(*it), data);
        retrainSeg.push_back(make_pair(data[get<0>(*it)].first, SWsegPtr));
    }

    //Check last segment if it contains single point
    if (get<0>(splitIndexSlopeVector.back())  != get<1>(splitIndexSlopeVector.back()))
    {
        SWseg<Type_Key,Type_Ts> * SWsegPtr = new SWseg<Type_Key,Type_Ts>(get<0>(splitIndexSlopeVector.back()), get<1>(splitIndexSlopeVector.back()) ,get<2>(splitIndexSlopeVector.back()), data);
        retrainSeg.push_back(make_pair(data[get<0>(splitIndexSlopeVector.back())].first, SWsegPtr));

    }
    else //Single Point 
    {
        SWseg<Type_Key,Type_Ts> * SWsegPtr = new SWseg<Type_Key,Type_Ts>(data.back());
        retrainSeg.push_back(make_pair(data.back().first, SWsegPtr));
    }

    #ifdef TUNE
    segLengthRetrain += data.size();
    #endif
}


/*
Insert into meta by threads
*/
template<class Type_Key, class Type_Ts>
int SWmeta<Type_Key,Type_Ts>::thread_insert(uint32_t threadID, Type_Key key, SWseg<Type_Key,Type_Ts>* segPtr, bool currentRetrainStatus)
{
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("SWmeta","thread_insert");
    #endif

    #if defined(DEBUG_KEY) || defined(DEBUG_TS) 
    if(DEBUG_KEY == key) 
    {
        DEBUG_ENTER_FUNCTION("SWmeta","thread_insert");
    }
    #endif

    if (thread_retraining != -1) //Retrain is in progress (Do not insert)
    {
        retrain_insertion_queue.enqueue(make_tuple(key,segPtr,currentRetrainStatus));
        return 2;
    }
    
    int metaRetrainFlag = 0;
    int startIndex = m_partitionIndex[threadID];

    if (m_slope != -1) //Model Insertion
    {
        if (key < m_startKey) //Insert into front of meta (threadID == 0)
        {
            ASSERT_MESSAGE(threadID == 0, "thread_insert: threadID should be 0 for key < m_startKey");
            metaRetrainFlag = insert_model(threadID, 0, key, segPtr, false, currentRetrainStatus);
        }
        else
        {
            tuple<bool,int,int,int> predictBound;
            uint32_t predictThread = predict_thread(key, predictBound);
            ASSERT_MESSAGE(predictThread == threadID, "thread_insert: threadID should be equal to predictThread");

            int insertPos = get<1>(predictBound);
            if (get<2>(predictBound) < get<3>(predictBound))
            {
                int predictPosMaxUnbounded = (m_rightSearchBound == 0) ? insertPos + 1 : insertPos + m_rightSearchBound;
                
                insertPos = insertPos < get<2>(predictBound) ? get<2>(predictBound) : insertPos;
                insertPos = insertPos > get<3>(predictBound) ? get<3>(predictBound) : insertPos;

                if (key < m_keys[threadID][insertPos-startIndex])
                {
                    insertPos = exponential_search_left_insert(threadID, m_keys[threadID], key, insertPos-startIndex, insertPos-get<2>(predictBound)) + startIndex;
                }
                else if (key > m_keys[threadID][insertPos-startIndex])
                {
                    insertPos = exponential_search_right_insert(threadID, m_keys[threadID], key, insertPos-startIndex, get<3>(predictBound)-insertPos) + startIndex;
                }
                
                if (insertPos < m_numSeg) //Model Insertion
                {
                    metaRetrainFlag = insert_model(threadID, insertPos, key, segPtr, (insertPos > predictPosMaxUnbounded), currentRetrainStatus);
                }
                else //Append to the end (Last thread)
                {
                    ASSERT_MESSAGE(threadID == m_partitionIndex.size()-1, "thread_insert: only last thread should append");
                    
                    if (static_cast<int>(m_numSeg >> 6) == m_bitmap.size())
                    {
                        m_bitmap.push_back(0);
                        m_retrainBitmap.push_back(0);
                    }

                    bitmap_set_bit(m_numSeg);
                    if (currentRetrainStatus)
                    {
                        bitmap_set_bit(m_retrainBitmap,m_numSeg);
                    }

                    m_keys[threadID].push_back(key);
                    m_ptr[threadID].push_back(segPtr);
                    ++m_numSeg;
                    ++m_numSegPerPartition[threadID];

                    segPtr->m_parentIndex = m_numSeg-1;
                    update_segment_with_neighbours(threadID, m_numSeg-1, m_partitionIndex.back(), m_numSeg-1);
                    ++m_numSegExist;
                    ++m_numSegExistsPerPartition[threadID];

                    if (predictPosMaxUnbounded < insertPos) { ++m_rightSearchBound;}

                    if (m_leftSearchBound + m_rightSearchBound > m_maxSearchError || (double)m_numSegExist / m_numSeg > 0.8)
                    {
                        metaRetrainFlag = ((double)m_numSegExist / m_numSeg < 0.5) ? 2 : 1;
                    }
                    metaRetrainFlag = 0;
                }
            }
            else if (get<2>(predictBound) == get<3>(predictBound))
            {
                //Insert to End
                metaRetrainFlag = insert_end(threadID, key, segPtr, currentRetrainStatus);
            }
            else
            {
                //Append (only last segment need lock)
                metaRetrainFlag = insert_append(threadID, insertPos, key, segPtr, currentRetrainStatus);
            }
        }
        m_maxSearchError = min(8192,(int)ceil(0.6*m_numSeg));

        return metaRetrainFlag;
    }
    else //m_slope == -1 (Only thread 1 is running operations except meta retrain)
    {
        ASSERT_MESSAGE(threadID == 0, "m_slope == -1, only thread 0 should be inserting and running");
        ASSERT_MESSAGE(m_keys.size() == 1, "m_slope == -1, m_keys.size() should be 1");
        ASSERT_MESSAGE(m_partitionIndex[0] == 0, "m_slope == -1, m_partitionIndex[0] == 0");

        int insertPos = 0;
        if (m_numSeg > 1)
        {
            if (key < m_startKey)
            {
                insert_model(threadID, 0, key, segPtr, false, currentRetrainStatus);
            }
            else
            {
                //Binary Search
                auto lowerBoundIt = lower_bound(m_keys[0].begin(),m_keys[0].end()-1,key);
                insertPos = lowerBoundIt - m_keys[0].begin();

                if (*lowerBoundIt < key && bitmap_exists(insertPos))
                {
                    ++insertPos;
                }

                if (insertPos < m_numSeg)
                {
                    insert_model(threadID, insertPos, key, segPtr, false, currentRetrainStatus);
                }
                else
                {
                    if (static_cast<int>(m_numSeg >> 6) == m_bitmap.size())
                    {
                        m_bitmap.push_back(0);
                        m_retrainBitmap.push_back(0);
                    }

                    bitmap_set_bit(m_numSeg);
                    if (currentRetrainStatus)
                    {
                        bitmap_set_bit(m_retrainBitmap,m_numSeg);
                    }

                    m_keys[0].push_back(key);
                    m_ptr[0].push_back(segPtr);
                    ++m_numSeg;
                    ++m_numSegPerPartition[0];

                    segPtr->m_parentIndex = m_numSeg-1;
                    update_segment_with_neighbours(0, m_numSeg-1, m_partitionIndex.back(), m_numSeg-1);
                    ++m_numSegExist;
                    ++m_numSegExistsPerPartition[0];
                }
            }
        }
        else
        {
            insert_end(threadID, key, segPtr, currentRetrainStatus);         
        }

        m_startKey = m_keys[0][0];
        m_numSeg = m_keys[0].size();
        m_partitionStartKey[0] = m_startKey;

        return (m_numSeg == MAX_BUFFER_SIZE)? 2 : 0;
    }

    return metaRetrainFlag;

    #ifdef DEBUG
    DEBUG_EXIT_FUNCTION("SWmeta","thread_insert");
    #endif

    #if defined(DEBUG_KEY) || defined(DEBUG_TS) 
    if(DEBUG_KEY == key) 
    {
        DEBUG_EXIT_FUNCTION("SWmeta","thread_insert");
    }
    #endif
}

/*
Insertion Helpers
*/
template<class Type_Key, class Type_Ts>
int SWmeta<Type_Key,Type_Ts>::insert_model(uint32_t threadID, int insertionPos, Type_Key key, SWseg<Type_Key,Type_Ts>* segPtr, bool addErrorFlag, bool currentRetrainStatus)
{
    if (bitmap_exists(insertionPos)) //insertionPos not a gap
    {
        if (m_numSegPerPartition[threadID] == m_numSegExistsPerPartition[threadID]) //No gaps within thread, need to search for gap in neighbours
        {
            return insert_model_no_gap(threadID, insertionPos, key, segPtr, addErrorFlag, currentRetrainStatus);
        }
        else
        {
            int startIndex = m_partitionIndex[threadID];
            int endIndex = (threadID == m_partitionIndex.size()-1) ? m_numSeg.load()-1 : m_partitionIndex[threadID+1]-1;
            
            int gapPos = find_closest_gap_within_thread(threadID, insertionPos);
            ASSERT_MESSAGE(gapPos != -1, "SWmeta::insert_model: gapPos == -1 when there are gaps within thread");

            if (gapPos != m_numSeg) //Shift using move operation
            {
                if (insertionPos < gapPos) //Right Shift (Move)
                {
                    vector<Type_Key> tempKey(m_keys[threadID].begin()+(insertionPos-startIndex),m_keys[threadID].begin()+(gapPos-startIndex));
                    move(tempKey.begin(),tempKey.end(),m_keys[threadID].begin()+(insertionPos+1-startIndex));
                    m_keys[threadID][insertionPos-startIndex] = key;

                    update_seg_parent_index(threadID, insertionPos, gapPos, true);
                    vector<SWseg<Type_Key,Type_Ts>*> tempPtr(m_ptr[threadID].begin()+(insertionPos-startIndex),m_ptr[threadID].begin()+(gapPos-startIndex));
                    move(tempPtr.begin(),tempPtr.end(),m_ptr[threadID].begin()+(insertionPos+1-startIndex));
                    m_ptr[threadID][insertionPos-startIndex] = segPtr;

                    bitmap_move_bit_back(insertionPos,gapPos);
                    bitmap_set_bit(insertionPos);
                    bitmap_move_bit_back(m_retrainBitmap,insertionPos,gapPos);

                    segPtr->m_parentIndex = insertionPos;
                    update_segment_with_neighbours(threadID, insertionPos, startIndex, endIndex);
                    ++m_rightSearchBound;
                }
                else //Left Shift (Move)
                {
                    --insertionPos;
                    vector<Type_Key> tempKey(m_keys[threadID].begin()+(gapPos+1-startIndex),m_keys[threadID].begin()+(insertionPos+1-startIndex));
                    move(tempKey.begin(),tempKey.end(),m_keys[threadID].begin()+(gapPos-startIndex));
                    m_keys[threadID][insertionPos-startIndex] = key;

                    update_seg_parent_index(threadID, gapPos, insertionPos, false);
                    vector<SWseg<Type_Key,Type_Ts>*> tempPtr(m_ptr[threadID].begin()+(gapPos+1-startIndex),m_ptr[threadID].begin()+(insertionPos+1-startIndex));
                    move(tempPtr.begin(),tempPtr.end(),m_ptr[threadID].begin()+(gapPos-startIndex));
                    m_ptr[threadID][insertionPos-startIndex] = segPtr;

                    bitmap_move_bit_front(insertionPos,gapPos);
                    bitmap_set_bit(insertionPos);
                    bitmap_move_bit_front(m_retrainBitmap,insertionPos,gapPos);

                    segPtr->m_parentIndex = insertionPos;
                    update_segment_with_neighbours(threadID, insertionPos, startIndex, endIndex);
                    ++m_leftSearchBound;
                }
            }
            else //Last partition shift to end.
            {
                ASSERT_MESSAGE (threadID == m_partitionIndex.size()-1, "SWmeta::insert_model: shift to end, must be last partition");
                m_keys[threadID].insert(m_keys[threadID].begin()+(insertionPos-startIndex),key);

                update_seg_parent_index(threadID, insertionPos, endIndex, true);
                m_ptr[threadID].insert(m_ptr[threadID].begin()+(insertionPos-startIndex),segPtr);

                bitmap_move_bit_back(insertionPos,m_numSeg);
                bitmap_set_bit(insertionPos);
                bitmap_move_bit_back(m_retrainBitmap,insertionPos,m_numSeg);

                if (currentRetrainStatus)
                {
                    bitmap_set_bit(m_retrainBitmap,insertionPos);
                }
                else
                {
                    bitmap_erase_bit(m_retrainBitmap,insertionPos);
                }

                segPtr->m_parentIndex = insertionPos;
                update_segment_with_neighbours(threadID, insertionPos, startIndex, endIndex);
                ++m_numSeg;
                ++m_rightSearchBound;
            }

            if (currentRetrainStatus)
            {
                bitmap_set_bit(m_retrainBitmap,insertionPos);
            }
            else
            {
                bitmap_erase_bit(m_retrainBitmap,insertionPos);
            }
            ++m_numSegExist;
            ++m_numSegExistsPerPartition[threadID];
        }
    }
    else //insertionPos is a gap
    {
        int startIndex = m_partitionIndex[threadID];
        int endIndex = (threadID == m_partitionIndex.size()-1) ? m_numSeg.load()-1 : m_partitionIndex[threadID+1]-1;
        
        m_keys[threadID][insertionPos-startIndex] = key;
        m_ptr[threadID][insertionPos-startIndex] = segPtr;
        segPtr->m_parentIndex = insertionPos;
        bitmap_set_bit(insertionPos);
        if (currentRetrainStatus) {bitmap_set_bit(m_retrainBitmap, insertionPos);}
        ++m_numSegExist;
        ++m_numSegExistsPerPartition[threadID];
        
        for (int i = insertionPos+1; i <= endIndex; ++i)
        {
            if (m_keys[threadID][i-startIndex] >= key)
            {
                break;
            }
            
            if (bitmap_exists(i))
            {
                LOG_ERROR("Keys are not sorted");
            }
            else
            {
                m_keys[threadID][i-startIndex] = key;
            }
        }
    }

    if (addErrorFlag) { ++m_rightSearchBound;}
    if (m_leftSearchBound + m_rightSearchBound > m_maxSearchError || (double)m_numSegExist / m_numSeg > 0.8)
    {
        return ((double)m_numSegExist / m_numSeg < 0.5) ? 2 : 1;
    }
    return 0;
}

template<class Type_Key, class Type_Ts>
int SWmeta<Type_Key,Type_Ts>::insert_model_no_gap(uint32_t threadID, int insertionPos, Type_Key key, SWseg<Type_Key,Type_Ts>* segPtr, bool addErrorFlag, bool currentRetrainStatus)
{
    if (threadID != m_partitionIndex.size()-1) //Need borrowing gaps
    {
        int gapPos;
        uint32_t gapThreadID;
        bool retrianFlag = false;
        int startIndex = m_partitionIndex[threadID];
        int endIndex = m_partitionIndex[threadID+1]-1;
        
        //Check neighbours for gaps
        int leftNeighbourGaps = (threadID == 0)? 0 : m_numSegExistsPerPartition[threadID-1] - m_numSegPerPartition[threadID-1];
        int rightNeighbourGaps = (threadID == m_partitionIndex.size()-1)? 0 : m_numSegExistsPerPartition[threadID+1] - m_numSegPerPartition[threadID+1];
        if (leftNeighbourGaps == 0 && rightNeighbourGaps == 0) {retrianFlag = true;}

        //Borrow from neighbour with more gaps (need to lock neighbour to secure gap)
        if (leftNeighbourGaps >= rightNeighbourGaps) //Borrow from left neighbour
        {
            if (borrow_gap_position_from_end(threadID-1, gapPos) == false);
            {
                if (rightNeighbourGaps == 0 || borrow_gap_position_from_begin(threadID+1, gapPos) == false)
                {
                    retrianFlag = true;
                }
            }
        }
        else //Borrow from right neighbour
        {
            if (borrow_gap_position_from_begin(threadID+1, gapPos) == false);
            {
                if (leftNeighbourGaps == 0 || borrow_gap_position_from_end(threadID-1, gapPos) == false)
                {
                    retrianFlag = true;
                }
            }
        }

        if (retrianFlag)
        {
            //Add key to queue and insert after retraining.
            retrain_insertion_queue.enqueue(make_tuple(key,segPtr,currentRetrainStatus));
            if (thread_retraining == -1) {thread_retraining = threadID*10+2;}
            return 2;
        }
        if (insertionPos < gapPos) //Mimics Right Shift
        {
            m_keys[threadID].insert(m_keys[threadID].begin()+(insertionPos-startIndex),key);
            update_seg_parent_index_normalized(threadID, insertionPos-startIndex, m_ptr[threadID].size()-1, true);
            m_ptr[threadID].insert(m_ptr[threadID].begin()+(insertionPos-startIndex),segPtr);

            bitmap_move_bit_back(insertionPos,endIndex+1);
            bitmap_set_bit(insertionPos);
            bitmap_move_bit_back(m_retrainBitmap,insertionPos,endIndex+1);

            update_segment_with_neighbours(threadID, insertionPos, startIndex, endIndex+1);
            ++m_rightSearchBound;
        }
        else //Mimics Left Shift
        {
            m_keys[threadID].insert(m_keys[threadID].begin()+(insertionPos+1-startIndex),key);
            update_seg_parent_index_normalized(threadID, 0, insertionPos-startIndex, false);
            m_ptr[threadID].insert(m_ptr[threadID].begin()+(insertionPos+1-startIndex),segPtr);

            bitmap_move_bit_front(insertionPos,startIndex-1);
            bitmap_set_bit(insertionPos);
            bitmap_move_bit_front(m_retrainBitmap,insertionPos,startIndex-1);

            update_segment_with_neighbours(threadID, insertionPos, startIndex-1, endIndex);
            ++m_leftSearchBound;
        }
    }
    else //Last partition (directly insert) right shift
    {
        int startIndex = m_partitionIndex[threadID];

        m_keys[threadID].insert(m_keys[threadID].begin()+(insertionPos-startIndex),key);
        update_seg_parent_index(threadID, insertionPos, m_numSeg-1, true);
        m_ptr[threadID].insert(m_ptr[threadID].begin()+(insertionPos-startIndex),segPtr);

        bitmap_move_bit_back(insertionPos,m_numSeg);
        bitmap_set_bit(insertionPos);
        bitmap_move_bit_back(m_retrainBitmap,insertionPos,m_numSeg);

        update_segment_with_neighbours(threadID, insertionPos, startIndex, m_numSeg);
        ++m_numSeg;
        ++m_numSegPerPartition[threadID];
        ++m_numSegExistsPerPartition[threadID];
        ++m_rightSearchBound;
    }

    if (currentRetrainStatus)
    {
        bitmap_set_bit(m_retrainBitmap,insertionPos);
    }
    else
    {
        bitmap_erase_bit(m_retrainBitmap,insertionPos);
    }

    segPtr->m_parentIndex = insertionPos;
    ++m_numSegExist;

    if (addErrorFlag) { ++m_rightSearchBound;}
    if (m_leftSearchBound + m_rightSearchBound > m_maxSearchError || (double)m_numSegExist / m_numSeg > 0.8)
    {
        return ((double)m_numSegExist / m_numSeg < 0.5) ? 2 : 1;
    }
}

template<class Type_Key, class Type_Ts>
int SWmeta<Type_Key,Type_Ts>::insert_end(uint32_t threadID, Type_Key key, SWseg<Type_Key,Type_Ts>* segPtr, bool currentRetrainStatus)
{
    ASSERT_MESSAGE(threadID == m_partitionIndex.size()-1, "SWmeta::insert_end must be called by last partition");

    if (bitmap_exists(m_numSeg-1))
    {
        if (static_cast<int>(m_numSeg >> 6) == m_bitmap.size())
        {
            m_bitmap.push_back(0);
            m_retrainBitmap.push_back(0);
        }

        if (key < m_keys[threadID].back()) //Insert into numSeg-1
        {
            //Insert and increment m_rightSearchBound
            bitmap_move_bit_back(m_numSeg-1,m_numSeg);
            bitmap_set_bit(m_numSeg-1);
            bitmap_move_bit_back(m_retrainBitmap,m_numSeg-1,m_numSeg);
            if (currentRetrainStatus)
            {
                bitmap_set_bit(m_retrainBitmap,m_numSeg-1);
            }
            else
            {
                bitmap_erase_bit(m_retrainBitmap,m_numSeg-1);
            }

            m_keys[threadID].insert(m_keys[threadID].end()-1,key);

            ++(m_ptr[threadID].back()->m_parentIndex);
            m_ptr[threadID].insert(m_ptr[threadID].end()-1,segPtr);

            segPtr->m_parentIndex = m_numSeg-1;
            update_segment_with_neighbours(threadID, m_numSeg-1, m_partitionIndex.back(), m_numSeg);
            
            ++m_numSeg;
            ++m_numSegPerPartition[threadID];
            ++m_numSegExist;
            ++m_numSegExistsPerPartition[threadID];
            ++m_rightSearchBound;

            if (m_leftSearchBound + m_rightSearchBound > m_maxSearchError || (double)m_numSegExist / m_numSeg > 0.8)
            {
                return ((double)m_numSegExist / m_numSeg < 0.5) ? 2 : 1;
            }
            return 0;
        }
        //Insert into numSeg
        bitmap_set_bit(m_numSeg);
        if (currentRetrainStatus)
        {
            bitmap_set_bit(m_retrainBitmap,m_numSeg);
        }

        m_keys[threadID].push_back(key);
        m_ptr[threadID].push_back(segPtr);
        ++m_numSeg;
        ++m_numSegPerPartition[threadID];

        segPtr->m_parentIndex = m_numSeg-1;
        update_segment_with_neighbours(threadID, m_numSeg-1, m_partitionIndex.back(), m_numSeg-1);
        ++m_numSegExist;
        ++m_numSegExistsPerPartition[threadID];
        
    }
    else //Replace numSeg
    {
        m_keys[threadID].back() = key;
        m_ptr[threadID].back() = segPtr;
        bitmap_set_bit(m_numSeg-1);
        if (currentRetrainStatus)
        {
            bitmap_set_bit(m_retrainBitmap,m_numSeg-1);
        }
        segPtr->m_parentIndex = m_numSeg-1;
        update_segment_with_neighbours(threadID, m_numSeg-1, m_partitionIndex.back(), m_numSeg-1);
        ++m_numSegExist;
        ++m_numSegExistsPerPartition[threadID];
    }

    return 0;
}

template<class Type_Key, class Type_Ts>
int SWmeta<Type_Key,Type_Ts>::insert_append(uint32_t threadID, int insertionPos, Type_Key key, SWseg<Type_Key,Type_Ts>* segPtr, bool currentRetrainStatus)
{
    ASSERT_MESSAGE(threadID == m_partitionIndex.size()-1, "SWmeta::insert_end must be called by last partition");

    Type_Key lastKey = m_keys[threadID].back();
    for (int index = m_numSeg; index <= insertionPos; ++index)
    {
        m_keys[threadID].push_back(lastKey);
        m_ptr[threadID].push_back(nullptr);
        ++m_numSeg;
    }
    m_keys[threadID].back() = key;
    m_ptr[threadID].back() = segPtr;
   
    int bitmapExpandTimes = static_cast<int>(insertionPos >> 6) - (m_bitmap.size()-1);
    for (int i = 0; i < bitmapExpandTimes; i++)
    {
        m_bitmap.push_back(0);
        m_retrainBitmap.push_back(0);
    }
    bitmap_set_bit(insertionPos);
    if (currentRetrainStatus)
    {
        bitmap_set_bit(m_retrainBitmap,insertionPos);
    }

    segPtr->m_parentIndex = insertionPos;
    update_segment_with_neighbours(threadID, insertionPos, m_partitionIndex.back(), insertionPos);
    ++m_numSegExist;
    ++m_numSegExistsPerPartition[threadID];

    if (((double)m_numSegExist) / (insertionPos+1) < 0.5)
    {
        return 2;
    }
    else if ((double)m_numSegExist / (insertionPos+1) > 0.8)
    {
        return 1;
    }
    else
    {
        return 0;
    }
}

/*
Meta Retrain
*/
template<class Type_Key, class Type_Ts>
void SWmeta<Type_Key,Type_Ts>::meta_retrain()
{
    //Load retrain method
    int retrainMethod = thread_retraining % 10; //1 = extend, 2 = retrain
    retrainMethod = ((double)m_numSegExist/(m_numSeg*1.05) < 0.5) ? 2 : retrainMethod;

    //Retrain Meta (calculate slope) without lock
    vector<Type_Key> keys;
    flatten_partition_keys(keys);
    vector<uint64_t> bitmap = m_bitmap;
    int numSeg = m_numSeg;
    
    double slopeTemp = m_slope;
    if (retrainMethod == 1) //Extend
    {
        slopeTemp *= 1.05;
    }
    else //Retrain
    {
        int firstIndex;
        Type_Key firstKey;
        double nomalizedKey;
        double sumKey = 0, sumIndex = 0, sumKeyIndex = 0, sumKeySquared = 0;
        int cnt = 0;
        for (int i = 0; i < numSeg; ++i)
        {
            if (bitmap_exists(bitmap,i))
            {               
                if (!cnt)
                {
                    firstIndex = keys[i];
                    firstIndex = i;
                }
                nomalizedKey = (double)keys[i] - firstKey;
                sumKey += nomalizedKey;
                sumIndex += cnt;
                sumKeyIndex += nomalizedKey * cnt;
                sumKeySquared += (double)pow(nomalizedKey,2);
                ++cnt;
            }
        }
        if (cnt == 1)
        {
            slopeTemp = -1;
        }
        else
        {
            slopeTemp =  (sumKeyIndex - sumKey *(sumIndex/cnt))/(sumKeySquared - sumKey*(sumKey/cnt)) * 1.05;
        }
    }
    keys.clear();
    bitmap.clear();

    vector<Type_Key> tempKey;
    vector<SWseg<Type_Key,Type_Ts>*> tempPtr;

    LOG_INFO("[Thread %u finish retraining model]", thread_retraining.load()/10);

    //Lock all threads
    vector<unique_lock<mutex>> locks(m_partitionIndex.size());
    for (uint32_t threadID = 0; threadID < m_partitionIndex.size(); ++threadID)
    {
        locks[threadID] = unique_lock<mutex>(thread_lock[threadID]);
        lock_thread(threadID+1,locks[threadID]);
    }

    //Find max timestamp 
    Type_Ts maxTimeStamp = *max_element(m_parititonMaxTime.begin(), m_parititonMaxTime.end());
    Type_Ts expiryTime =  ((double)maxTimeStamp - TIME_WINDOW < numeric_limits<Type_Ts>::min()) ? numeric_limits<Type_Ts>::min(): maxTimeStamp - TIME_WINDOW;

    if (slopeTemp != -1)
    {
        m_slope = slopeTemp;
        retrain_model_combine_data(expiryTime, tempKey, tempPtr);
    }
    else
    {
        retrain_combine_data(expiryTime, tempKey, tempPtr);
    }

    LOG_DEBUG_SHOW("[Thread %u finish changing meta]", thread_retraining.load()/10);

    partition_data_into_threads(m_partitionIndex.size(), tempKey, tempPtr);

    m_parititonMaxTime = vector<Type_Ts>(m_partitionIndex.size(),maxTimeStamp);

    thread_retraining = -1;
    
    //Unlock all threads
    for (uint32_t threadID = 0; threadID < m_partitionIndex.size(); ++threadID)
    {
        unlock_thread(threadID+1,locks[threadID]);
    }

    LOG_DEBUG_SHOW("Retrain Partition finished");
}

/*
Retrain helpers
*/
template<class Type_Key, class Type_Ts>
vector<tuple<key_type,SWseg<key_type,time_type>*,bool>> SWmeta<Type_Key,Type_Ts>::flatten_partition_retrain(Type_Ts expiryTime)
{
    //Unload all segments from retrain insertion queue
    vector<tuple<key_type,SWseg<key_type,time_type>*,bool>> insertionQueue;
    insertionQueue.reserve(retrain_insertion_queue.size_approx());

    tuple<key_type,SWseg<key_type,time_type>*,bool> insertionSeg;
    bool found = retrain_insertion_queue.try_dequeue(insertionSeg);
    while (found)
    {
        if (get<1>(insertionSeg)->m_maxTimeStamp >= expiryTime)
        {
            insertionQueue.push_back(insertionSeg);
        }
        else
        {
            delete get<1>(insertionSeg);
        }
        found = retrain_insertion_queue.try_dequeue(insertionSeg);
    }
    insertionQueue.shrink_to_fit();

    sort(insertionQueue.begin(),insertionQueue.end(),
            [](const tuple<Type_Key,SWseg<Type_Key,Type_Ts>*,bool>&a, const tuple<Type_Key,SWseg<Type_Key,Type_Ts>*,bool>&b)
            {
                return get<0>(a) < get<0>(b);
            });

    return insertionQueue;
}

template<class Type_Key, class Type_Ts>
void SWmeta<Type_Key,Type_Ts>::retrain_model_combine_data(Type_Ts expiryTime, vector<Type_Key> & tempKey, vector<SWseg<Type_Key,Type_Ts>*> & tempPtr)
{
    //Get all segments from retrain insertion queue
    vector<tuple<key_type,SWseg<key_type,time_type>*,bool>> insertionQueue = flatten_partition_retrain(expiryTime);

    //Initiatlization 
    vector<uint64_t> tempBitmap;
    vector<uint64_t> tempRetrainBitmap;
    tempKey.reserve(m_numSegExist + insertionQueue.size());
    tempPtr.reserve(m_numSegExist + insertionQueue.size());
    m_numSegExist = 0;
    m_rightSearchBound = 0;
    m_leftSearchBound = 0;

    //Set up temporary variables
    int startIndex = (bitmap_exists(0))? 0 : bitmap_closest_right_nongap(0, m_numSeg-1);
    int partitionID = 0;
    ASSERT_MESSAGE(startIndex < m_partitionIndex[1], "SWmeta::meta_retrain: startIndex should be in first partition");
    
    m_startKey = m_keys[partitionID][startIndex-m_partitionIndex[partitionID]];
    int currentIndex = startIndex;
    int partitionStartIndex = m_partitionIndex[partitionID];

    int currentQueueIndex = 0;
    int currentInsertionPos = 0;
    int lastNonGapIndex = 0;
    
    while (currentIndex < m_numSeg && currentQueueIndex < insertionQueue.size())
    {    
        if (partitionID < m_partitionIndex.size()-1 && currentIndex >= m_partitionIndex[partitionID+1])
        {
            ++partitionID;
            partitionStartIndex = m_partitionIndex[partitionID];
        }

        if (bitmap_exists(currentIndex))
        {
            if (m_ptr[partitionID][currentIndex-partitionStartIndex]->m_maxTimeStamp >= expiryTime)
            {
                
                if (m_keys[partitionID][currentIndex-partitionStartIndex] < get<0>(insertionQueue[currentQueueIndex]))
                {
                    retrain_model_insert_segments(m_keys[partitionID][currentIndex-partitionStartIndex], 
                                                m_ptr[partitionID][currentIndex-partitionStartIndex],
                                                bitmap_exists(m_retrainBitmap,currentIndex), 
                                                currentInsertionPos, lastNonGapIndex, currentIndex, 
                                                tempKey, tempPtr, tempBitmap, tempRetrainBitmap);
                }
                else if (m_keys[partitionID][currentIndex-partitionStartIndex] > get<0>(insertionQueue[currentQueueIndex]))
                {
                    retrain_model_insert_segments(get<0>(insertionQueue[currentQueueIndex]), 
                                                get<1>(insertionQueue[currentQueueIndex]),
                                                get<2>(insertionQueue[currentQueueIndex]), 
                                                currentInsertionPos, lastNonGapIndex, currentQueueIndex, 
                                                tempKey, tempPtr, tempBitmap, tempRetrainBitmap);
                }
                else
                {
                    LOG_DEBUG_SHOW("SWmeta::meta_retrain: two segments have the same key");
                }
            }
            else
            {
                delete m_ptr[partitionID][currentIndex-partitionStartIndex];
                m_ptr[partitionID][currentIndex-partitionStartIndex] = nullptr;
                ++currentIndex;
            }
        }
        else
        {
            ++currentIndex;
        }
    }

    while (currentIndex < m_numSeg)
    {
        if (partitionID < m_partitionIndex.size()-1 && currentIndex >= m_partitionIndex[partitionID+1])
        {
            ++partitionID;
            partitionStartIndex = m_partitionIndex[partitionID];
        }

        if (bitmap_exists(currentIndex))
        {
            if (m_ptr[partitionID][currentIndex-partitionStartIndex]->m_maxTimeStamp >= expiryTime) //Segment has not expired
            {
                retrain_model_insert_segments(m_keys[partitionID][currentIndex-partitionStartIndex], 
                                            m_ptr[partitionID][currentIndex-partitionStartIndex],
                                            bitmap_exists(m_retrainBitmap,currentIndex), 
                                            currentInsertionPos, lastNonGapIndex, currentIndex, 
                                            tempKey, tempPtr, tempBitmap, tempRetrainBitmap);
            }
            else
            {
                delete m_ptr[partitionID][currentIndex-partitionStartIndex];
                m_ptr[partitionID][currentIndex-partitionStartIndex] = nullptr;
                ++currentIndex;
            }
        }
        else
        {
            ++currentIndex;
        }
    }

    while (currentQueueIndex < insertionQueue.size())
    {
        retrain_model_insert_segments(get<0>(insertionQueue[currentQueueIndex]), 
                                    get<1>(insertionQueue[currentQueueIndex]),
                                    get<2>(insertionQueue[currentQueueIndex]), 
                                    currentInsertionPos, lastNonGapIndex, currentQueueIndex, 
                                    tempKey, tempPtr, tempBitmap, tempRetrainBitmap);
    }
    tempPtr.front()->m_leftSibling = nullptr;
    tempPtr[lastNonGapIndex]->m_rightSibling = nullptr;

    m_rightSearchBound += 1;
    m_maxSearchError = min(8192,(int)ceil(0.6*tempKey.size()));
    m_bitmap = tempBitmap;
    m_retrainBitmap = tempRetrainBitmap;
    m_numSeg = tempKey.size();
    tempKey.shrink_to_fit();
    tempPtr.shrink_to_fit();
}

template<class Type_Key, class Type_Ts>
void SWmeta<Type_Key,Type_Ts>::retrain_combine_data(Type_Ts expiryTime, vector<Type_Key> & tempKey, vector<SWseg<Type_Key,Type_Ts>*> & tempPtr)
{
    //Get all segments from retrain insertion queue
    vector<tuple<key_type,SWseg<key_type,time_type>*,bool>> insertionQueue = flatten_partition_retrain(expiryTime);

    int totalSize = m_numSegExist+insertionQueue.size();
    ASSERT_MESSAGE(totalSize < MAX_BUFFER_SIZE, "retrain m_slope = -1, but actual the number of keys is > MAX_BUFFER_SIZE");
    ASSERT_MESSAGE(totalSize != 0, "retrain m_slope = -1, full index is empty");

    int startIndex = (bitmap_exists(0))? 0 : bitmap_closest_right_nongap(0, m_numSeg-1);
    int partitionID = 0;
    
    m_startKey = m_keys[partitionID][startIndex-m_partitionIndex[partitionID]];

    if (totalSize > 1)
    {
        vector<uint64_t> tempBitmap;
        vector<uint64_t> tempRetrainBitmap;
        tempKey.reserve(totalSize);
        tempPtr.reserve(totalSize);
        m_numSegExist = 0;

        int currentIndex = startIndex;
        int partitionStartIndex = m_partitionIndex[partitionID];
        int currentQueueIndex = 0;
        int currentInsertionPos = 0;
        
        while (currentIndex < m_numSeg && currentQueueIndex < insertionQueue.size())
        {    
            if (partitionID < m_partitionIndex.size()-1 && currentIndex >= m_partitionIndex[partitionID+1])
            {
                ++partitionID;
                partitionStartIndex = m_partitionIndex[partitionID];
            }

            if (bitmap_exists(currentIndex))
            {
                if (m_ptr[partitionID][currentIndex-partitionStartIndex]->m_maxTimeStamp >= expiryTime)
                {
                    
                    if (m_keys[partitionID][currentIndex-partitionStartIndex] < get<0>(insertionQueue[currentQueueIndex]))
                    {
                        retrain_insert_segments(m_keys[partitionID][currentIndex-partitionStartIndex], 
                                                m_ptr[partitionID][currentIndex-partitionStartIndex],
                                                bitmap_exists(m_retrainBitmap,currentIndex), 
                                                currentInsertionPos, currentIndex, 
                                                tempKey, tempPtr, tempBitmap, tempRetrainBitmap);
                    }
                    else if (m_keys[partitionID][currentIndex-partitionStartIndex] > get<0>(insertionQueue[currentQueueIndex]))
                    {
                        retrain_insert_segments(get<0>(insertionQueue[currentQueueIndex]), 
                                                get<1>(insertionQueue[currentQueueIndex]),
                                                get<2>(insertionQueue[currentQueueIndex]), 
                                                currentInsertionPos, currentQueueIndex, 
                                                tempKey, tempPtr, tempBitmap, tempRetrainBitmap);
                    }
                    else
                    {
                        LOG_DEBUG_SHOW("SWmeta::meta_retrain: two segments have the same key");
                    }
                }
                else
                {
                    delete m_ptr[partitionID][currentIndex-partitionStartIndex];
                    m_ptr[partitionID][currentIndex-partitionStartIndex] = nullptr;
                    ++currentIndex;
                }
            }
            else
            {
                ++currentIndex;
            }
        }

        while (currentIndex < m_numSeg)
        {
            if (partitionID < m_partitionIndex.size()-1 && currentIndex >= m_partitionIndex[partitionID+1])
            {
                ++partitionID;
                partitionStartIndex = m_partitionIndex[partitionID];
            }

            if (bitmap_exists(currentIndex))
            {
                if (m_ptr[partitionID][currentIndex-partitionStartIndex]->m_maxTimeStamp >= expiryTime) //Segment has not expired
                {
                    retrain_insert_segments(m_keys[partitionID][currentIndex-partitionStartIndex], 
                                            m_ptr[partitionID][currentIndex-partitionStartIndex],
                                            bitmap_exists(m_retrainBitmap,currentIndex), 
                                            currentInsertionPos, currentIndex, 
                                            tempKey, tempPtr, tempBitmap, tempRetrainBitmap);
                }
                else
                {
                    delete m_ptr[partitionID][currentIndex-partitionStartIndex];
                    m_ptr[partitionID][currentIndex-partitionStartIndex] = nullptr;
                    ++currentIndex;
                }
            }
            else
            {
                ++currentIndex;
            }
        }

        while (currentQueueIndex < insertionQueue.size())
        {
            retrain_insert_segments(get<0>(insertionQueue[currentQueueIndex]), 
                                    get<1>(insertionQueue[currentQueueIndex]),
                                    get<2>(insertionQueue[currentQueueIndex]), 
                                    currentInsertionPos, currentQueueIndex, 
                                    tempKey, tempPtr, tempBitmap, tempRetrainBitmap);
        }
        tempPtr.front()->m_leftSibling = nullptr;
        tempPtr.back()->m_rightSibling = nullptr;

        m_bitmap = tempBitmap;
        m_retrainBitmap = tempRetrainBitmap;
        m_numSeg = tempKey.size();
        tempKey.shrink_to_fit();
        tempPtr.shrink_to_fit();
    }
    else
    {
        Type_Key currentKey;
        SWseg<Type_Key,Type_Ts>* currentPtr;
        bool retrainFlag;
        
        if (m_numSegExist == 0)
        {
            currentKey = m_keys[partitionID][startIndex-m_partitionIndex[partitionID]];
            currentPtr = m_ptr[partitionID][startIndex-m_partitionIndex[partitionID]];
            retrainFlag = bitmap_exists(m_retrainBitmap,startIndex);
        }
        else
        {
            currentKey = get<0>(insertionQueue[0]);
            currentPtr = get<1>(insertionQueue[0]);
            retrainFlag = get<2>(insertionQueue[0]);
        }

        int bitmapSize = ceil(MAX_BUFFER_SIZE/64.);
        vector<uint64_t> temp(bitmapSize,0);
        
        m_numSeg = 1;
        m_numSegExist = 1;
        m_bitmap = temp;
        m_retrainBitmap = temp;
        bitmap_set_bit(0);

        if (retrainFlag)
        {
            bitmap_set_bit(m_retrainBitmap,0);
        }
        
        currentPtr->m_parentIndex = 0;
        currentPtr->m_leftSibling = nullptr;
        currentPtr->m_rightSibling = nullptr;

        tempKey.push_back(currentKey);
        tempPtr.push_back(currentPtr);
    }

    m_slope = -1;
    m_rightSearchBound = 0;
    m_leftSearchBound = 0;
}

template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::retrain_model_insert_segments(Type_Key currentKey, SWseg<key_type,time_type>*currentPtr, bool currentIsRetrain,
                                    int & currentInsertionPos, int & lastNonGapIndex, int & insertIndex,
                                    vector<Type_Key> & tempKey, vector<SWseg<Type_Key,Type_Ts>*> & tempPtr,
                                    vector<uint64_t> & tempBitmap, vector<uint64_t> & tempRetrainBitmap)
{
    int  predictedPos = static_cast<int>(floor(m_slope * ((double)currentKey - (double)m_startKey)));
                
    if (static_cast<int>(currentInsertionPos >> 6) == tempBitmap.size())
    {
        tempBitmap.push_back(0);
        tempRetrainBitmap.push_back(0);
    }

    if (predictedPos == currentInsertionPos)
    {
        currentPtr->m_parentIndex = currentInsertionPos;

        tempKey.push_back(currentKey);
        tempPtr.push_back(currentPtr);
        if (lastNonGapIndex > 0)
        {
            tempPtr[lastNonGapIndex]->m_leftSibling = tempPtr.back();
            tempPtr.back()->m_rightSibling = tempPtr[lastNonGapIndex];
        }
        bitmap_set_bit(tempBitmap, currentInsertionPos);

        if (currentIsRetrain)
        {
            bitmap_set_bit(tempRetrainBitmap,currentInsertionPos);
        }

        ++insertIndex;

        ++m_numSegExist;
        lastNonGapIndex = currentInsertionPos;
    }
    else if (predictedPos < currentInsertionPos)
    {
        currentPtr->m_parentIndex = currentInsertionPos;

        tempKey.push_back(currentKey);
        tempPtr.push_back(currentPtr);
        if (lastNonGapIndex > 0)
        {
            tempPtr[lastNonGapIndex]->m_leftSibling = tempPtr.back();
            tempPtr.back()->m_rightSibling = tempPtr[lastNonGapIndex];
        }
        bitmap_set_bit(tempBitmap, currentInsertionPos);

        if (currentIsRetrain)
        {
            bitmap_set_bit(tempRetrainBitmap,currentInsertionPos);
        }

        m_rightSearchBound = (currentInsertionPos - predictedPos) > m_rightSearchBound.load() 
                            ? (currentInsertionPos - predictedPos) : m_rightSearchBound.load();
        
        ++insertIndex;

        ++m_numSegExist;
        lastNonGapIndex = currentInsertionPos;
    }
    else
    {
        tempKey.push_back((tempKey.size() == 0) ? numeric_limits<Type_Key>::min() : tempKey.back());
        tempPtr.push_back(nullptr);
    }
    ++currentInsertionPos;
}

template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::retrain_insert_segments(Type_Key currentKey, SWseg<key_type,time_type>*currentPtr, bool currentIsRetrain,
                                    int & currentInsertionPos, int & insertIndex,
                                    vector<Type_Key> & tempKey, vector<SWseg<Type_Key,Type_Ts>*> & tempPtr,
                                    vector<uint64_t> & tempBitmap, vector<uint64_t> & tempRetrainBitmap)
{                
    if (static_cast<int>(currentInsertionPos >> 6) == tempBitmap.size())
    {
        tempBitmap.push_back(0);
        tempRetrainBitmap.push_back(0);
    }

    currentPtr->m_parentIndex = currentInsertionPos;

    tempKey.push_back(currentKey);
    tempPtr.push_back(currentPtr);
    if (currentInsertionPos > 0)
    {
        tempPtr[currentInsertionPos-1]->m_leftSibling = tempPtr.back();
        tempPtr.back()->m_rightSibling = tempPtr[currentInsertionPos-1];
    }
    bitmap_set_bit(tempBitmap, currentInsertionPos);

    if (currentIsRetrain)
    {
        bitmap_set_bit(tempRetrainBitmap,currentInsertionPos);
    }

    ++insertIndex;
    ++m_numSegExist;
    ++currentInsertionPos;
}

/*
Util functions
*/
template<class Type_Key, class Type_Ts>
inline tuple<int,int,int> SWmeta<Type_Key,Type_Ts>::find_predict_pos_bound(Type_Key targetKey)
{
    int predictPos = static_cast<int>(floor(m_slope * ((double)targetKey - (double)m_startKey)));
    int predictPosMin = predictPos - m_leftSearchBound;
    int predictPosMax = (m_rightSearchBound == 0) ? predictPos + 1 : predictPos + m_rightSearchBound;

    return (make_tuple(predictPos, predictPosMin < 0 ? 0 : predictPosMin, predictPosMax > m_numSeg-1 ? m_numSeg-1: predictPosMax));
}


template<class Type_Key, class Type_Ts>
inline int SWmeta<Type_Key,Type_Ts>::exponential_search_right(vector<Type_Key> & keys, Type_Key targetKey, int normalizedStartingPos, int maxSearchBound)
{
    #ifdef TUNE
    int predictPos = normalizedStartingPos;
    #endif

    int index = 1;
    while (index <= maxSearchBound && keys[normalizedStartingPos + index] < targetKey)
    {
        index *= 2;
    }

    auto startIt = keys.begin() + (normalizedStartingPos + static_cast<int>(index/2));
    auto endIt = keys.begin() + (normalizedStartingPos + min(index,maxSearchBound));

    auto lowerBoundIt = lower_bound(startIt,endIt,targetKey);

    if (lowerBoundIt == keys.end() || ((lowerBoundIt != keys.begin()) && (*lowerBoundIt > targetKey)))
    {
        lowerBoundIt--;
    }


    #ifdef TUNE
    rootNoExponentialSearch++;
    rootLengthExponentialSearch += lowerBoundIt - keys.begin() - predictPos;
    #endif

    return lowerBoundIt - keys.begin();
}

template<class Type_Key, class Type_Ts>
inline int SWmeta<Type_Key,Type_Ts>::exponential_search_right_insert(uint32_t threadID, vector<Type_Key> & keys, Type_Key targetKey, int normalizedStartingPos, int maxSearchBound)
{
    #ifdef TUNE
    int predictPos = normalizedStartingPos;
    #endif

    int index = 1;
    while (index <= maxSearchBound && keys[normalizedStartingPos + index] < targetKey)
    {
        index *= 2;
    }

    auto startIt = keys.begin() + (normalizedStartingPos + static_cast<int>(index/2));
    auto endIt = keys.begin() + (normalizedStartingPos + min(index,maxSearchBound));

    auto lowerBoundIt = lower_bound(startIt,endIt,targetKey);

    int foundPos = lowerBoundIt - keys.begin();

    if (*lowerBoundIt < targetKey && bitmap_exists(foundPos+m_partitionIndex[threadID]))
    {
        ++foundPos;
    }

    #ifdef TUNE
    rootNoExponentialSearch++;
    rootLengthExponentialSearch += foundPos - predictPos;
    #endif

    return foundPos;
}


template<class Type_Key, class Type_Ts>
inline int SWmeta<Type_Key,Type_Ts>::exponential_search_left(vector<Type_Key> & keys, Type_Key targetKey, int normalizedStartingPos, int maxSearchBound)
{
    #ifdef TUNE
    int predictPos = normalizedStartingPos;
    #endif

    int index = 1;
    while (index <= maxSearchBound && keys[normalizedStartingPos - index] >= targetKey)
    {
        index *= 2;
    }

    auto startIt = keys.begin() + (normalizedStartingPos - min(index,maxSearchBound));
    auto endIt = keys.begin() + (normalizedStartingPos - static_cast<int>(index/2));

    auto lowerBoundIt = lower_bound(startIt,endIt,targetKey);
    
    if (lowerBoundIt == keys.end() || ((lowerBoundIt != keys.begin()) && (*lowerBoundIt > targetKey)))
    {
        lowerBoundIt--;
    }


    #ifdef TUNE
    rootNoExponentialSearch++;
    rootLengthExponentialSearch += predictPos - (lowerBoundIt - keys.begin());
    #endif
    
    return lowerBoundIt - keys.begin();
}

template<class Type_Key, class Type_Ts>
inline int SWmeta<Type_Key,Type_Ts>::exponential_search_left_insert(uint32_t threadID, vector<Type_Key> & keys, Type_Key targetKey, int normalizedStartingPos, int maxSearchBound)
{
    #ifdef TUNE
    int predictPos = normalizedStartingPos;
    #endif

    int index = 1;
    while (index <= maxSearchBound && keys[normalizedStartingPos - index] >= targetKey)
    {
        index *= 2;
    }

    auto startIt = keys.begin() + (normalizedStartingPos - min(index,maxSearchBound));
    auto endIt = keys.begin() + (normalizedStartingPos - static_cast<int>(index/2));

    auto lowerBoundIt = lower_bound(startIt,endIt,targetKey);
    
    int foundPos = lowerBoundIt - keys.begin();

    if (*lowerBoundIt < targetKey && bitmap_exists(foundPos+m_partitionIndex[threadID]))
    {
        ++foundPos;
    }

    #ifdef TUNE
    rootNoExponentialSearch++;
    rootLengthExponentialSearch += predictPos - foundPos;
    #endif

    return foundPos;
}
template<class Type_Key, class Type_Ts>
inline uint32_t SWmeta<Type_Key,Type_Ts>::find_index_partition(int index)
{
    uint32_t threadID = 0;
    for (int i = 1; i < m_partitionIndex.size(); ++i)
    {
        if (index >= m_partitionIndex[i])
        {
            ++threadID; 
        }
    }
    return threadID;
}

template<class Type_Key, class Type_Ts>
inline int SWmeta<Type_Key,Type_Ts>::find_first_segment_in_partition(uint32_t threadID)
{
    int firstSegmentIndex = m_partitionIndex[threadID];

    if (!bitmap_exists(firstSegmentIndex))
    {
        firstSegmentIndex = bitmap_closest_right_nongap(firstSegmentIndex,
            (threadID == m_partitionIndex.size()-1)? m_numSeg-1:m_partitionIndex[threadID+1]-1);
        
        if (firstSegmentIndex == -1) return -1;
    }
    return firstSegmentIndex;
}   

template<class Type_Key, class Type_Ts>
inline int SWmeta<Type_Key,Type_Ts>::find_last_segment_in_partition(uint32_t threadID)
{
    int lastSegmentIndex= (threadID == m_partitionIndex.size()-1)? m_numSeg-1 : m_partitionIndex[threadID+1]-1;

    if (!bitmap_exists(lastSegmentIndex))
    {
        lastSegmentIndex = bitmap_closest_left_nongap(lastSegmentIndex,m_partitionIndex[threadID]);
        
        if (lastSegmentIndex == -1) return -1;
    }
    return lastSegmentIndex;
}


template<class Type_Key, class Type_Ts>
inline int SWmeta<Type_Key,Type_Ts>::find_closest_gap(int index)
{
    if (bitmap_exists(index))
    {
        int gapPos = bitmap_closest_gap(index);

        if (gapPos == -1)
        {
            LOG_ERROR("No gap found in bitmap");
            abort();
        }
        return gapPos;
    }
    return index;
}

template<class Type_Key, class Type_Ts>
inline int SWmeta<Type_Key,Type_Ts>::find_closest_gap(int index, int leftBoundary, int rightBoundary)
{
    if (bitmap_exists(index))
    {
        int gapPos = bitmap_closest_gap(index, leftBoundary, rightBoundary);

        return (gapPos == -1)? -1 : gapPos;
    }
    return index;
}

template<class Type_Key, class Type_Ts>
inline int SWmeta<Type_Key,Type_Ts>::find_closest_gap_within_thread(uint32_t threadID, int index)
{
    if (bitmap_exists(index))
    {
        int gapPos = bitmap_closest_gap(index, m_partitionIndex[threadID],
            (threadID == m_partitionIndex.size()-1)? m_numSeg.load():m_partitionIndex[threadID+1]-1);
        //Last partition allow gapPos == m_numSeg;

        return (gapPos == -1)? -1 : gapPos;
    }
    return index;
}


template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::update_segment_with_neighbours(uint32_t threadID, int index, int startIndex, int endIndex)
{
    ASSERT_MESSAGE(bitmap_exists(index), "Segment does not exist in bitmap");

    int leftNeighbour = -1;
    if (startIndex < index)
    {
        leftNeighbour = (bitmap_exists(index-1)) ? index-1: bitmap_closest_left_nongap(index-1,startIndex);
    }

    if (leftNeighbour != -1)
    {
        m_ptr[threadID][index-startIndex]->m_leftSibling = m_ptr[threadID][leftNeighbour-startIndex];
        m_ptr[threadID][leftNeighbour-startIndex]->m_rightSibling = m_ptr[threadID][index-startIndex];
    }

    int rightNeighbour = -1;
    if (index < endIndex)
    {
        rightNeighbour = (bitmap_exists(index+1)) ? index+1: bitmap_closest_right_nongap(index+1,endIndex);
    }

    if (rightNeighbour != -1)
    {
        m_ptr[threadID][index-startIndex]->m_rightSibling = m_ptr[threadID][rightNeighbour-startIndex];
        m_ptr[threadID][rightNeighbour-startIndex]->m_leftSibling = m_ptr[threadID][index-startIndex];
    }

}

template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::update_keys_with_previous(uint32_t threadID, int index)
{
    if (index) // Do not need to replace current key with previous key if it is the first segment
    {
        int startIndex = m_partitionIndex[threadID];
        int endIndex = (threadID == m_partitionIndex.size()-1)? m_numSeg-1 : m_partitionIndex[threadID+1]-1;
        
        Type_Key previousKey = (index == startIndex)? m_partitionStartKey[threadID]: m_keys[threadID][index-startIndex-1];
        
        m_keys[threadID][index-startIndex] = previousKey;
        ++index;
        while (!bitmap_exists(index) && index <= endIndex)
        {
            m_keys[threadID][index-startIndex] = previousKey;
            ++index;
        }
    }
}

template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::flatten_partition_keys(vector<Type_Key> & data)
{
    data.reserve(m_numSeg);
    vector<vector<Type_Key>> tempKeys = m_keys;
    for (auto & partition: tempKeys)
    {
        for (auto & key: partition)
        {
            data.push_back(key);
        }
    }
}

template<class Type_Key, class Type_Ts>
bool SWmeta<Type_Key,Type_Ts>::borrow_gap_position_from_end(uint32_t threadID, int & gapPos) //Both threadID and threadID+1 locked
{
    ASSERT_MESSAGE(threadID != m_partitionIndex.size()-1, "borrow_gap_position_from_end cannot be called by last partition");
    int startIndex = m_partitionIndex[threadID];
    int endIndex = m_partitionIndex[threadID+1]-1;

    unique_lock<mutex> lock(thread_lock[threadID]);
    lock_thread(threadID, lock);
    if (m_numSegExistsPerPartition[threadID] == m_numSegPerPartition[threadID]) {return false;}

    gapPos = bitmap_closest_left_gap(endIndex, startIndex);
    ASSERT_MESSAGE(gapPos != -1, "borrow_gap_position_from_end: gapPos should not be -1 when there is gaps in partition");

    if (gapPos != startIndex)
    {
        m_keys[threadID].erase(m_keys[threadID].begin()+(gapPos-startIndex));
        update_seg_parent_index(threadID, gapPos, endIndex, false);
        m_ptr[threadID].erase(m_ptr[threadID].begin()+(gapPos-startIndex));
        bitmap_move_bit_front(endIndex, gapPos);
        bitmap_move_bit_front(m_retrainBitmap, endIndex, gapPos);
    }
    else
    {
        m_keys[threadID].pop_back();
        m_ptr[threadID].pop_back();
    }
    --m_numSegPerPartition[threadID];
    --m_partitionIndex[threadID+1]; //threadID + 1 calls this function. (no need extra lock)
    ++m_numSegPerPartition[threadID+1];
    ++m_numSegExistsPerPartition[threadID+1];

    unlock_thread(threadID, lock);
    return true;
}

template<class Type_Key, class Type_Ts>
bool SWmeta<Type_Key,Type_Ts>::borrow_gap_position_from_begin(uint32_t threadID, int & gapPos) //Both threadID and threadID-1 locked
{
    ASSERT_MESSAGE(threadID != 0, "borrow_gap_position_from_begin cannot be called by first partition");
    int startIndex = m_partitionIndex[threadID];
    int endIndex = (threadID == m_partitionIndex.size()-1)? m_numSeg.load() :m_partitionIndex[threadID+1]-1;

    unique_lock<mutex> lock(thread_lock[threadID]);
    lock_thread(threadID, lock);
    if (m_numSegExistsPerPartition[threadID] == m_numSegPerPartition[threadID]) {return false;}

    gapPos = bitmap_closest_right_gap(startIndex, endIndex);
    ASSERT_MESSAGE(gapPos != -1, "borrow_gap_position_from_begin: gapPos should not be -1 when there is gaps in partition");

    if (gapPos != endIndex)
    {
        m_keys[threadID].erase(m_keys[threadID].begin()+(gapPos-startIndex));
        update_seg_parent_index(threadID, startIndex, gapPos, true);
        m_ptr[threadID].erase(m_ptr[threadID].begin()+(gapPos-startIndex));
        bitmap_move_bit_back(startIndex,gapPos);
        bitmap_move_bit_back(m_retrainBitmap,startIndex,gapPos);
    }
    else
    {
        m_keys[threadID].erase(m_keys[threadID].begin());
        m_ptr[threadID].erase(m_ptr[threadID].begin());
    }
    --m_numSegPerPartition[threadID];
    ++m_partitionIndex[threadID];
    ++m_numSegPerPartition[threadID-1];
    ++m_numSegExistsPerPartition[threadID-1];

    unlock_thread(threadID, lock);
    return true;
}


template<class Type_Key, class Type_Ts>
void SWmeta<Type_Key,Type_Ts>::update_seg_parent_index(uint32_t threadID, int startPos, int endPos, bool incrementFlag)
//Inclusive: [startPos, endPos]
{
    if (startPos == endPos) {return;}

    int startIndex = m_partitionIndex[threadID];
    int endIndex = (threadID == m_partitionIndex.size()-1)? m_numSeg.load() :m_partitionIndex[threadID+1]-1;

    auto endIt = (endPos == endIndex)? m_ptr[threadID].end() : m_ptr[threadID].begin() + (endPos+1-startIndex);

    int increment = (incrementFlag)? 1 : -1;
    for (auto it = m_ptr[threadID].begin() + (startPos-startIndex); it != endIt; ++it)
    {
        if ((*it) != nullptr)
        {
            (*it)->m_parentIndex += increment;
        }
    }
}

template<class Type_Key, class Type_Ts>
void SWmeta<Type_Key,Type_Ts>::update_seg_parent_index_normalized(uint32_t threadID, int startPos, int endPos, bool incrementFlag)
//Inclusive: [startPos, endPos]: Normalized positions (actual index in m_ptr[threadID])
{
    if (startPos == endPos) {return;}

    auto endIt = (endPos == m_ptr[threadID].size()-1)? m_ptr[threadID].end() : m_ptr[threadID].begin() + endPos+1;

    int increment = (incrementFlag)? 1 : -1;
    for (auto it = m_ptr[threadID].begin() + startPos; it != endIt; ++it)
    {
        if ((*it) != nullptr)
        {
            (*it)->m_parentIndex += increment;
        }
    }
}


template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::lock_thread(uint32_t threadID, unique_lock<mutex> & lock)
{
    while (thread_occupied[threadID])
    {
        thread_cv[threadID].wait(lock);
    }
}

template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::unlock_thread(uint32_t threadID, unique_lock<mutex> & lock)
{
    thread_occupied[threadID] = false;
    lock.unlock();
    thread_cv[threadID].notify_one();
}

/*
Getters & Setters
*/
template<class Type_Key, class Type_Ts>
inline size_t SWmeta<Type_Key,Type_Ts>::get_meta_size()
{
    return m_numSeg;
}

template<class Type_Key, class Type_Ts>
inline size_t SWmeta<Type_Key,Type_Ts>::get_no_seg()
{
    return m_numSegExist;
}

template<class Type_Key, class Type_Ts>
inline int SWmeta<Type_Key,Type_Ts>::get_split_error()
{
    return splitError;
}

template<class Type_Key, class Type_Ts>
inline  void SWmeta<Type_Key,Type_Ts>::set_split_error(int error)
{
    splitError = error;
}

template<class Type_Key, class Type_Ts>
void SWmeta<Type_Key,Type_Ts>::print()
{
    printf("start key: %u, slope: %g \n", m_startKey, m_slope);
    if (bitmap_exists(0))
    {
        printf("%u(1) \n", m_keys[0]);
    }
    else
    {
        printf("%u(0) \n", m_keys[0]);
    }

    for (int i = 1; i < m_numSeg; i++)
    {
        cout << ",";
        if (bitmap_exists(i))
        {
            printf("%u(1) \n", m_keys[i]);
        }
        else
        {
            printf("%u(0) \n", m_keys[i]);
        }
    }
    printf("\n");
}

template<class Type_Key, class Type_Ts>
void  SWmeta<Type_Key,Type_Ts>::print_all()
{
    for (int i = 0; i < m_numSeg; i++)
    {
        if (bitmap_exists(i))
        {
            printf("***************************** \n");
            printf("%u(1) \n", m_keys[i]);
            m_ptr[i]->print();
            printf("***************************** \n");
        }
        else
        {
            printf("***************************** \n");
            printf("%u(0) \n", m_keys[i]);
            printf("***************************** \n");
        }
    }
    printf("\n");
}

template <class Type_Key, class Type_Ts>
inline uint64_t SWmeta<Type_Key,Type_Ts>::memory_usage()
{
    uint64_t segSize = 0;
    for (auto & partition: m_ptr)
    {
        for (auto & seg: partition)
        {
            if (seg != nullptr)
            {
                segSize += seg->memory_usage();
            }
        }
    }

    return sizeof(int) + sizeof(atomic<int>)*5 + 
            sizeof(Type_Key) + sizeof(double) +
            sizeof(vector<int>)*3 + sizeof(int)*m_partitionIndex.size() + 
            sizeof(vector<Type_Ts>) + sizeof(Type_Ts)*m_partitionIndex.size() +
            sizeof(vector<Type_Key>) + sizeof(Type_Key)*m_partitionIndex.size() +
            sizeof(vector<uint64_t>)*2 + sizeof(uint64_t)*(m_bitmap.size()*2) +
            sizeof(vector<vector<Type_Key>>) + sizeof(vector<Type_Key>)*m_keys.size() + sizeof(Type_Key)*m_numSeg +
            sizeof(vector<vector<SWseg<Type_Key,Type_Ts>*>>) + sizeof(vector<SWseg<Type_Key,Type_Ts>*>)*m_ptr.size() 
                                                                + sizeof(SWseg<Type_Key,Type_Ts>*)*m_numSeg + segSize;
}

template <class Type_Key, class Type_Ts>
inline uint64_t SWmeta<Type_Key,Type_Ts>::get_no_keys(Type_Ts Timestamp)
{
    Type_Ts expiryTime =  ((double)Timestamp - TIME_WINDOW < numeric_limits<Type_Ts>::min()) ? numeric_limits<Type_Ts>::min(): Timestamp - TIME_WINDOW;

    uint64_t cnt = 0;
    for (int i = 0; i < m_numSeg; i++)
    {
        if (bitmap_exists(i))
        {
            cnt += m_ptr[i]->get_no_keys(expiryTime);
        }
    }
    
    return cnt;
}

/*
Bitmap Operations
*/
template<class Type_Key, class Type_Ts>
inline bool SWmeta<Type_Key,Type_Ts>::bitmap_exists(int index) const
{
    int bitmapPos = index >> 6;
    int bitPos = index - (bitmapPos << 6);
    return static_cast<bool>(m_bitmap[bitmapPos] & (1ULL << bitPos));
}

template<class Type_Key, class Type_Ts>
inline bool SWmeta<Type_Key,Type_Ts>::bitmap_exists(vector<uint64_t> & bitmap, int index) const
{
    int bitmapPos = index >> 6;
    int bitPos = index - (bitmapPos << 6);
    return static_cast<bool>(bitmap[bitmapPos] & (1ULL << bitPos));
}

template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::bitmap_set_bit(int index)
{
    int bitmapPos = index >> 6;
    int bitPos = index - (bitmapPos << 6);

    m_bitmap[bitmapPos] |= (1ULL << bitPos); 
}

template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::bitmap_set_bit(vector<uint64_t> & bitmap, int index)
{
    int bitmapPos = index >> 6;
    int bitPos = index - (bitmapPos << 6);

    bitmap[bitmapPos] |= (1ULL << bitPos); 
}

template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::bitmap_erase_bit(int index)
{
    int bitmapPos = index >> 6;
    int bitPos = index - (bitmapPos << 6);
    m_bitmap[bitmapPos] &= ~(1ULL << bitPos);
}

template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::bitmap_erase_bit(vector<uint64_t> & bitmap, int index)
{
    int bitmapPos = index >> 6;
    int bitPos = index - (bitmapPos << 6);
    bitmap[bitmapPos] &= ~(1ULL << bitPos);
}


template<class Type_Key, class Type_Ts>
inline int SWmeta<Type_Key,Type_Ts>::bitmap_closest_left_nongap(int index, int leftBoundary)
{
    int bitmapPos = index >> 6;
    int bitPos = index - (bitmapPos << 6);

    int boundaryBitmapPos = leftBoundary >> 6;

    uint64_t currentBitmap = m_bitmap[bitmapPos];
    currentBitmap &= ((1ULL << (bitPos)) - 1); //Erase all bits after the index (set them to 0)

    while(currentBitmap == 0) 
    //If current set of bitmap is empty (find previous bitmap that is not empty)
    {
        --bitmapPos;
        if (bitmapPos < boundaryBitmapPos )
        {
            return -1; //Out of bounds
        }
        currentBitmap = m_bitmap[bitmapPos];
    }

    // __builtin_clzll(currentBitmap) //Non x86instric.h equivalent for gcc.
    int return_index = (bitmapPos << 6) + (63-static_cast<int>(_lzcnt_u64(currentBitmap)));
    return (return_index < leftBoundary) ? -1 : return_index;
}

template<class Type_Key, class Type_Ts>
inline int SWmeta<Type_Key,Type_Ts>::bitmap_closest_right_nongap(int index, int rightBoundary)
{
    int bitmapPos = index >> 6;
    int bitPos = index - (bitmapPos << 6);

    int boundaryBitmapPos = rightBoundary >> 6;

    uint64_t currentBitmap = m_bitmap[bitmapPos];
    currentBitmap &= ~((1ULL << (bitPos)) - 1); //Erase all bits before the index (set them to 0)

    while(currentBitmap == 0) 
    //If current set of bitmap is empty (find next bitmap that is not empty)
    {
        ++bitmapPos;
        if (bitmapPos > boundaryBitmapPos)
        {
            return -1;
        }
        currentBitmap = m_bitmap[bitmapPos];
    }

    int return_index = bit_get_index(bitmapPos,bit_extract_rightmost_bit(currentBitmap));
    return (return_index > rightBoundary) ? -1 : return_index;
}

template<class Type_Key, class Type_Ts>
inline int SWmeta<Type_Key,Type_Ts>::bitmap_closest_left_gap(int index, int leftBoundary)
{
    int bitmapPos = index >> 6;
    int bitPos = index - (bitmapPos << 6);

    uint64_t currentBitmap = m_bitmap[bitmapPos];
    currentBitmap &= ((1ULL << (bitPos)) - 1);
    currentBitmap ^= ((1ULL << (bitPos)) - 1);

    int leftBoundaryBitmapPos = leftBoundary >> 6;
    while(currentBitmap == 0) 
    {
        --bitmapPos;
        if (bitmapPos < leftBoundaryBitmapPos )
        {
            return -1;
        }
        currentBitmap = m_bitmap[bitmapPos] ^ numeric_limits<uint64_t>::max();
    }
    int return_index = (bitmapPos << 6) + (63-static_cast<int>(_lzcnt_u64(currentBitmap)));
    return (return_index < leftBoundary) ? -1 : return_index;
}

template<class Type_Key, class Type_Ts>
inline int SWmeta<Type_Key,Type_Ts>::bitmap_closest_right_gap(int index, int rightBoundary)
{
    int bitmapPos = index >> 6;
    int bitPos = index - (bitmapPos << 6);

    uint64_t currentBitmap = m_bitmap[bitmapPos];
    currentBitmap &= ~((1ULL << (bitPos)) - 1);
    currentBitmap ^= ~((1ULL << (bitPos)) - 1);

    int rightBoundaryBitmapPos = rightBoundary >> 6;
    while(currentBitmap == 0) 
    {
        bitmapPos++;
        if (bitmapPos > rightBoundaryBitmapPos)
        {
           return -1;
        }
        currentBitmap = m_bitmap[bitmapPos] ^ numeric_limits<uint64_t>::max();
    }

    int return_index = bit_get_index(bitmapPos,bit_extract_rightmost_bit(currentBitmap));
    return (return_index > rightBoundary) ? -1 : return_index;
}

template<class Type_Key, class Type_Ts>
inline int SWmeta<Type_Key,Type_Ts>::bitmap_closest_gap(int index)
{
    int bitmapPos = index >> 6;
    int bitPos = index - (bitmapPos << 6);

    //If bitmap block is full (check other blocks)
    if (m_bitmap[bitmapPos] == numeric_limits<uint64_t>::max() || //block full
       (bitmapPos == m_bitmap.size()-1 && _mm_popcnt_u64(m_bitmap[bitmapPos]) == (m_numSeg-((m_bitmap.size()-1)<<6))) //last block full
       )
    {
        int maxLeftBlockDist = bitmapPos;
        int maxRightBlockDist =  ((m_numSeg-1) >> 6) - bitmapPos;

        if (m_bitmap.size() == 1)
        {
            return static_cast<int>(_tzcnt_u64(~m_bitmap[bitmapPos]));
        }

        if (m_numSeg == m_numSegExist)
        {
            return m_numSegExist;
        }

        int blockDist = 1;
        int maxBothBlockDist = min<int>(maxLeftBlockDist,maxRightBlockDist);
        
        while (blockDist <= maxBothBlockDist)
        {
            uint64_t rightBitmap = m_bitmap[bitmapPos + blockDist];
            uint64_t leftBitmap = m_bitmap[bitmapPos - blockDist];

            //Both blocks have gaps (find closest)
            if (leftBitmap != numeric_limits<uint64_t>::max() && rightBitmap != numeric_limits<uint64_t>::max())
            {
                int rightGap = ((bitmapPos + blockDist) << 6) + static_cast<int>(_tzcnt_u64(~rightBitmap));
                int leftGap = ((bitmapPos - blockDist + 1) << 6) - static_cast<int>(_lzcnt_u64(~leftBitmap)) - 1;

                if (rightGap - index < index - leftGap)
                {
                    return rightGap; //Can also be last position (need to check outside to append)
                }
                else
                {
                    return leftGap;
                }
            }
            //Gap in right block only (Also need to check leftblock-1 if bitPos < 32)
            else if (rightBitmap != numeric_limits<uint64_t>::max())
            {
                int rightGap = ((bitmapPos + blockDist) << 6) + static_cast<int>(_tzcnt_u64(~rightBitmap));

                if (bitPos < 32 && bitmapPos - blockDist > 0 &&
                    m_bitmap[bitmapPos - blockDist - 1] != numeric_limits<uint64_t>::max())
                {
                    int leftGap = ((bitmapPos - blockDist) << 6) - static_cast<int>(_lzcnt_u64(~m_bitmap[bitmapPos - blockDist - 1])) - 1;

                    if (rightGap - index < index - leftGap)
                    {
                        return rightGap; //Can also be last position (need to check outside to append)
                    }
                    else
                    {
                        return leftGap;
                    }
                }
                else
                {
                    return rightGap;
                }
            }
            //Gap in left block only (Also need to check rightblock+1 if bitPos > 32)
            else if (leftBitmap != numeric_limits<uint64_t>::max())
            {
                int leftGap = ((bitmapPos - blockDist + 1) << 6) - static_cast<int>(_lzcnt_u64(~leftBitmap)) - 1;

                if (bitPos > 32 && bitmapPos + blockDist + 1 < m_bitmap.size() && 
                    m_bitmap[bitmapPos + blockDist + 1] != numeric_limits<uint64_t>::max())
                {
                    int rightGap = ((bitmapPos + blockDist+1) << 6) + static_cast<int>(_tzcnt_u64(~m_bitmap[bitmapPos + blockDist + 1]));

                    if (rightGap - index < index - leftGap)
                    {
                        return rightGap; //Can also be last position (need to check outside to append)
                    }
                    else
                    {
                        return leftGap;
                    } 
                }
                else
                {
                    return leftGap;
                }
            }
            blockDist++;
        }

        if (maxLeftBlockDist > maxRightBlockDist)
        {
            for (int i = bitmapPos - blockDist; i >= 0; i--)
            {
                if (m_bitmap[i] != numeric_limits<uint64_t>::max())
                {
                    return ((i + 1) << 6) - static_cast<int>(_lzcnt_u64(~m_bitmap[i])) - 1;
                }
            }
        }
        else
        {
            int rightBound = ((m_numSeg-1) >> 6);
            for (int i = bitmapPos + blockDist; i <= rightBound; i++)
            {
                if (m_bitmap[i] != numeric_limits<uint64_t>::max())
                {
                    return (i << 6) + static_cast<int>(_tzcnt_u64(~m_bitmap[i]));
                }
            }
        }
        return -1; //Something went wrong (No Gap)

    }
    //There is a gap (somewhere) in current block
    else
    {
        uint64_t currentBitmap = m_bitmap[bitmapPos];

        int minRightGapDist = 64; //Find right closest gap
        uint64_t rightGaps = ~(currentBitmap | ((1ULL << (bitPos)) - 1));
        if (rightGaps)
        {
            minRightGapDist = static_cast<int>(_tzcnt_u64(rightGaps)) - bitPos;
        }
        else if (bitmapPos + 1 != m_bitmap.size())
        {
            minRightGapDist = static_cast<int>(_tzcnt_u64(~m_bitmap[bitmapPos+1])) + 64 - bitPos;
        }

        int minLeftGapDist = 64; //Find left closest gap
        uint64_t leftGaps = (~currentBitmap) & ((1ULL << bitPos) - 1);
        if (leftGaps)
        {
            minLeftGapDist = bitPos - (63 - static_cast<int>(_lzcnt_u64(leftGaps)));
        }
        else if (bitmapPos)
        {
            minLeftGapDist = bitPos + static_cast<int>(_lzcnt_u64(~m_bitmap[bitmapPos-1])) + 1;
        }

        if (minRightGapDist < minLeftGapDist)
        {
            return index + minRightGapDist;  //Can also be last position (need to check outside to append)
        }
        else
        {
            return index - minLeftGapDist;
        }
    }
}

template<class Type_Key, class Type_Ts>
inline int SWmeta<Type_Key,Type_Ts>::bitmap_closest_gap(int index, int leftBoundary, int rightBoundary)
{
    int bitmapPos = index >> 6;
    int bitPos = index - (bitmapPos << 6);

    int leftIndex;
    uint64_t currentBitmap = m_bitmap[bitmapPos];
    currentBitmap &= ((1ULL << (bitPos)) - 1);
    currentBitmap ^= ((1ULL << (bitPos)) - 1);

    int leftBoundaryBitmapPos = leftBoundary >> 6;
    while(currentBitmap == 0) 
    {
        --bitmapPos;
        if (bitmapPos < leftBoundaryBitmapPos )
        {
            leftIndex = -1; //Out of bounds
            break;
        }
        currentBitmap = m_bitmap[bitmapPos] ^ numeric_limits<uint64_t>::max();
    }
    leftIndex = (bitmapPos << 6) + (63-static_cast<int>(_lzcnt_u64(currentBitmap)));

    if (leftIndex < leftBoundary)
    {
        leftIndex = -1;
    }

    int rightIndex;
    currentBitmap = m_bitmap[bitmapPos];
    currentBitmap &= ~((1ULL << (bitPos)) - 1);
    currentBitmap ^= ~((1ULL << (bitPos)) - 1);

    int rightBoundaryBitmapPos = rightBoundary >> 6;
    while(currentBitmap == 0) 
    {
        bitmapPos++;
        if (bitmapPos > rightBoundaryBitmapPos)
        {
            rightIndex = -1; //Out of bounds
            break;
        }
        currentBitmap = m_bitmap[bitmapPos] ^ numeric_limits<uint64_t>::max();
    }

    rightIndex = bit_get_index(bitmapPos,bit_extract_rightmost_bit(currentBitmap));

    if (rightIndex > rightBoundary)
    {
        rightIndex = -1;
    }

    //Both out of bounds
    if (leftIndex == -1 && rightIndex == -1)
    {
        return -1;
    }

    //One out of bounds
    if (leftIndex == -1 || rightIndex == -1)
    {
        return (leftIndex == -1) ? rightIndex : leftIndex;
    }
    
    //Both in bounds
    if (index - leftIndex < rightIndex - index)
    {
        return leftIndex;
    }
    else
    {
        return rightIndex;
    }
}

template<class Type_Key, class Type_Ts>
inline pair<int,int> SWmeta<Type_Key,Type_Ts>::bitmap_retrain_range(int index)
//Returns [leftExistIndex,rightExistIndex]. 
//If leftExistIndex == rightExistIndex: retrain alone
//Else: retrain with neighbours 
{
    int bitmapPos = index >> 6;
    int bitPos = index - (bitmapPos << 6);

    int tempBitmapPos = bitmapPos;
    uint64_t tempBitmap = (m_retrainBitmap[bitmapPos] & m_bitmap[bitmapPos]);

    int rightIndex = static_cast<int>(_tzcnt_u64(tempBitmap));
    int leftIndex = 63 - static_cast<int>(_lzcnt_u64(tempBitmap));
    
    tempBitmap = (m_retrainBitmap[bitmapPos] ^ m_bitmap[bitmapPos]);
    tempBitmap &= ~((1ULL << (bitPos)) - 1);

    leftIndex = min(leftIndex,static_cast<int>(_tzcnt_u64(tempBitmap))-1);
    if (!static_cast<bool>(m_bitmap[bitmapPos] & (1ULL << leftIndex)))
    {
        uint64_t tempBitmap2 = (m_retrainBitmap[bitmapPos] & m_bitmap[bitmapPos]);
        tempBitmap2 &= ((1ULL << (leftIndex+1)) - 1);
        leftIndex = min(leftIndex, 63 - static_cast<int>(_lzcnt_u64(tempBitmap2)));
    }
    leftIndex += (bitmapPos << 6);

    if (tempBitmap == 0 && bitmapPos != m_bitmap.size()-1)
    {
        tempBitmapPos++;
        tempBitmap = (m_retrainBitmap[tempBitmapPos] ^ m_bitmap[tempBitmapPos]);

        int leftIndexTemp;
        while (tempBitmap == 0 && tempBitmapPos != m_bitmap.size()-1)
        {
            leftIndexTemp = 63 - static_cast<int>(_lzcnt_u64((m_retrainBitmap[tempBitmapPos] & m_bitmap[tempBitmapPos])));

            if (leftIndexTemp != -1)
            {
                leftIndex = (tempBitmapPos << 6) + leftIndexTemp;
            }
            
            tempBitmapPos++;
            tempBitmap = (m_retrainBitmap[tempBitmapPos] ^ m_bitmap[tempBitmapPos]);
        }

        leftIndexTemp = 63 - static_cast<int>(_lzcnt_u64((m_retrainBitmap[tempBitmapPos] & m_bitmap[tempBitmapPos])));

        if (leftIndexTemp != -1 && leftIndexTemp < static_cast<int>(_tzcnt_u64(tempBitmap)))
        {
            leftIndex = (tempBitmapPos << 6) + leftIndexTemp;
        }
    }

    tempBitmap = (m_retrainBitmap[bitmapPos] ^ m_bitmap[bitmapPos]);
    tempBitmap &= ((1ULL << (bitPos)) - 1);
    
    rightIndex = max(rightIndex,64 - static_cast<int>(_lzcnt_u64(tempBitmap)));
    if (!static_cast<bool>(m_bitmap[bitmapPos] & (1ULL << rightIndex)))
    {
        uint64_t tempBitmap2 = (m_retrainBitmap[bitmapPos] & m_bitmap[bitmapPos]);
        tempBitmap2 &= ~((1ULL << (rightIndex)) - 1);
        rightIndex = max(rightIndex,static_cast<int>(_tzcnt_u64(tempBitmap2)));
    }
    rightIndex += (bitmapPos << 6);

    if (tempBitmap == 0 && bitmapPos != 0)
    {
        tempBitmapPos = bitmapPos-1;
        tempBitmap = (m_retrainBitmap[tempBitmapPos] ^ m_bitmap[tempBitmapPos]);

        int rightIndexTemp;
        while (tempBitmap == 0 && tempBitmapPos != 0)
        {
            rightIndexTemp = static_cast<int>(_tzcnt_u64((m_retrainBitmap[tempBitmapPos] & m_bitmap[tempBitmapPos])));

            if (rightIndexTemp != 64)
            {
                rightIndex = (tempBitmapPos << 6) + rightIndexTemp;
            }

            tempBitmapPos--;
            tempBitmap = (m_retrainBitmap[tempBitmapPos] ^ m_bitmap[tempBitmapPos]);
        }

        rightIndexTemp = static_cast<int>(_tzcnt_u64((m_retrainBitmap[tempBitmapPos] & m_bitmap[tempBitmapPos])));

        if (rightIndexTemp != 64 && rightIndexTemp > 63 - static_cast<int>(_lzcnt_u64(tempBitmap)))
        {
            rightIndex = (tempBitmapPos << 6) + rightIndexTemp;
        }
    }

    return make_pair(rightIndex,leftIndex);
}

template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::bitmap_move_bit_back(int startingIndex, int endingIndex) 
//Inclusive: [startingIndex,endingIndex] and startingIndex < endingIndex
{
    if (static_cast<int>(endingIndex >> 6) == m_bitmap.size())
    {
        m_bitmap.push_back(0); //When end exceeds last pos (end == m_numSeg)
    }

    int bitmapPos = startingIndex >> 6;
    int bitPos = startingIndex - (bitmapPos << 6);

    int bitmapPosEnd = endingIndex >> 6;
    int bitPosEnd = endingIndex - (bitmapPosEnd << 6);

    bool previousBitValue = static_cast<bool>(m_bitmap[bitmapPos] & (1ULL << bitPos));

    while (!(bitmapPos == bitmapPosEnd && bitPos == bitPosEnd))
    {
        if (bitPos == 63)
        {
            bitPos = 0;
            bitmapPos++;
        }
        else
        {
            bitPos++;
        }

        bool temp = static_cast<bool>(m_bitmap[bitmapPos] & (1ULL << bitPos));
        if (previousBitValue)
        {
            m_bitmap[bitmapPos] |= (1ULL << bitPos);
        }
        else
        {
            m_bitmap[bitmapPos] &= ~(1ULL << bitPos);
        }
        previousBitValue = temp;
    }
    return;
}

template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::bitmap_move_bit_back(vector<uint64_t> & bitmap, int startingIndex, int endingIndex) 
//Inclusive: [startingIndex,endingIndex] and startingIndex < endingIndex
{
    if (static_cast<int>(endingIndex >> 6) == bitmap.size())
    {
        bitmap.push_back(0); //When end exceeds last pos (end == m_numSeg)
    }

    int bitmapPos = startingIndex >> 6;
    int bitPos = startingIndex - (bitmapPos << 6);

    int bitmapPosEnd = endingIndex >> 6;
    int bitPosEnd = endingIndex - (bitmapPosEnd << 6);

    bool previousBitValue = static_cast<bool>(bitmap[bitmapPos] & (1ULL << bitPos));

    while (!(bitmapPos == bitmapPosEnd && bitPos == bitPosEnd))
    {
        if (bitPos == 63)
        {
            bitPos = 0;
            bitmapPos++;
        }
        else
        {
            bitPos++;
        }

        bool temp = static_cast<bool>(bitmap[bitmapPos] & (1ULL << bitPos));
        if (previousBitValue)
        {
            bitmap[bitmapPos] |= (1ULL << bitPos);
        }
        else
        {
            bitmap[bitmapPos] &= ~(1ULL << bitPos);
        }
        previousBitValue = temp;
    }
    return;
}

template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::bitmap_move_bit_front(int startingIndex, int endingIndex) 
//Inclusive: [startingIndex,endingIndex] and startingIndex > endingIndex
{
    int bitmapPos = startingIndex >> 6;
    int bitPos = startingIndex - (bitmapPos << 6);

    int bitmapPosEnd = endingIndex >> 6;
    int bitPosEnd = endingIndex - (bitmapPosEnd << 6);

    bool previousBitValue = static_cast<bool>(m_bitmap[bitmapPos] & (1ULL << bitPos));

    while (!(bitmapPos == bitmapPosEnd && bitPos == bitPosEnd))
    {
        if (bitPos == 0)
        {
            bitPos = 63;
            bitmapPos--;
        }
        else
        {
            bitPos--;
        }

        bool temp = static_cast<bool>(m_bitmap[bitmapPos] & (1ULL << bitPos));
        if (previousBitValue)
        {
            m_bitmap[bitmapPos] |= (1ULL << bitPos);
        }
        else
        {
            m_bitmap[bitmapPos] &= ~(1ULL << bitPos);
        }
        previousBitValue = temp;
    }
    return;
}

template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::bitmap_move_bit_front(vector<uint64_t> & bitmap, int startingIndex, int endingIndex) 
//Inclusive: [startingIndex,endingIndex] and startingIndex > endingIndex
{
    int bitmapPos = startingIndex >> 6;
    int bitPos = startingIndex - (bitmapPos << 6);

    int bitmapPosEnd = endingIndex >> 6;
    int bitPosEnd = endingIndex - (bitmapPosEnd << 6);

    bool previousBitValue = static_cast<bool>(bitmap[bitmapPos] & (1ULL << bitPos));

    while (!(bitmapPos == bitmapPosEnd && bitPos == bitPosEnd))
    {
        if (bitPos == 0)
        {
            bitPos = 63;
            bitmapPos--;
        }
        else
        {
            bitPos--;
        }

        bool temp = static_cast<bool>(bitmap[bitmapPos] & (1ULL << bitPos));
        if (previousBitValue)
        {
            bitmap[bitmapPos] |= (1ULL << bitPos);
        }
        else
        {
            bitmap[bitmapPos] &= ~(1ULL << bitPos);
        }
        previousBitValue = temp;
    }
    return;
}

}
#endif