#ifndef __SWIX_META_HPP__
#define __SWIX_META_HPP__

#pragma once
#include "SWseg.hpp"

using namespace std;

namespace swix {

template<class Type_Key, class Type_Ts>
class SWmeta
{
private:
    int m_rightSearchBound;
    int m_leftSearchBound;
    int m_numPairExist;
    int m_maxSearchError;
    int splitError;
    
    // Type_Ts m_maxTs;
    Type_Key m_startKey;
    double m_slope;

    vector<uint64_t> m_bitmap;
    vector<uint64_t> m_retrainBitmap;
    vector<Type_Key> m_keys;
    vector<SWseg<Type_Key,Type_Ts>*> m_ptr;

public:
    //Constructors & Deconstructors
    SWmeta(pair<Type_Key, Type_Ts> & arrivalTuple);
    SWmeta(const vector<pair<Type_Key, Type_Ts>> & stream);
    ~SWmeta();

public:
    //Initial Bulk Load
    void bulk_load(const vector<pair<Type_Key, Type_Ts>> & stream);
    
private:
    //Bulk load helpers & retraining SWseg
    void split_data_slope(vector<pair<Type_Key,Type_Ts>> & data, vector<pair<Type_Key, SWseg<Type_Key,Type_Ts> * >> & splitedDataPtr);
    void retrain_seg(pair<int,int> SWsegIndexes, Type_Ts & lowerLimit, vector<pair<Type_Key, SWseg<Type_Key,Type_Ts> * >> & splitedDataPtr);

public:
    //Operations
    void lookup(pair<Type_Key, Type_Ts> & arrivalTuple, Type_Key & resultCount);
    void range_search(tuple<Type_Key, Type_Ts, Type_Key> & arrivalTuple, vector<pair<Type_Key, Type_Ts>> & rangeSearchResult);
    void range_search_ordered_data(tuple<Type_Key, Type_Ts, Type_Key> & arrivalTuple, vector<pair<Type_Key, Type_Ts>> & rangeSearchResult);
    void insert(pair<Type_Key, Type_Ts> & arrivalTuple);

private:
    //Operation Helpers
    void meta_insertion(pair<Type_Key, SWseg<Type_Key,Type_Ts>*> & insertKeyPtr, int & retrainExtendFlag, bool retrainSetBit);
    void meta_insertion_model(int & insertionPos, pair<Type_Key, SWseg<Type_Key,Type_Ts>*> & insertKeyPtr, bool & gapAddError, int & retrainExtendFlag, bool & retrainSetBit);
    void meta_insertion_end(pair<Type_Key, SWseg<Type_Key,Type_Ts>*> & insertKeyPtr, int & retrainExtendFlag, bool & retrainSetBit);
    void meta_insertion_append(int & insertionPos, pair<Type_Key, SWseg<Type_Key,Type_Ts>*> & insertKeyPtr, int & retrainExtendFlag, bool & retrainSetBit);

    void meta_extend_retrain(int & retrainExtendFlag, Type_Ts & lowerLimit);

    void predict_search(Type_Key & targetKey, int & foundPos);
    void exponential_search(Type_Key & targetKey, int & foundPos);
    void exponential_search_insert(Type_Key & targetKey, int & foundPos);

    void find_predict_pos_bound(Type_Key & targetKey, int & predictPos, int & predictPosMin, int & predictPosMax);
    void exponential_search_right(Type_Key & targetKey, int & foundPos, int maxSearchBound);
    void exponential_search_right_insert(Type_Key & targetKey, int & foundPos, int maxSearchBound);
    void exponential_search_left(Type_Key & targetKey, int & foundPos, int maxSearchBound);
    void exponential_search_left_insert(Type_Key & targetKey, int & foundPos, int maxSearchBound);

public:
    //Getters & Setters
    size_t get_meta_size();
    size_t get_no_seg();
    int get_split_error();
    void set_split_error(int error);
    void print();
    void print_all();
    void print_stats();
    uint64_t get_total_size_in_bytes();
    uint64_t get_no_keys(Type_Ts Timestamp);

    #ifdef TUNE_TIME
    int get_meta_error()
    {
       return m_leftSearchBound +  m_rightSearchBound;
    }

    double get_seg_average_error()
    {
        uint64_t error = 0;
        for (int i = 0; i < m_keys.size(); i++)
        {
            if (bitmap_exists(i))
            {
                error += (m_ptr[i]->m_rightSearchBound + m_ptr[i]->m_leftSearchBound);
            }
        }
        return (double)error/m_numPairExist;
    }
    #endif

private:
    //Bitmap helpers
    bool bitmap_exists(int index) const;
    bool bitmap_exists(vector<uint64_t> & bitmap, int index) const;
    void bitmap_set_bit(int index);
    void bitmap_set_bit(vector<uint64_t> & bitmap, int index);
    void bitmap_erase_bit(int index);
    void bitmap_erase_bit(vector<uint64_t> & bitmap, int index); 
    int bitmap_closest_left_nongap(int index);
    int bitmap_closest_right_nongap(int index);
    int bitmap_closest_gap(int index);
    pair<int,int> bitmap_retrain_range(int index);
    void bitmap_move_bit_back(int startingIndex, int endingIndex);
    void bitmap_move_bit_back(vector<uint64_t> & bitmap, int startingIndex, int endingIndex);
    void bitmap_move_bit_front(int startingIndex, int endingIndex);
    void bitmap_move_bit_front(vector<uint64_t> & bitmap, int startingIndex, int endingIndex);

};

template<class Type_Key, class Type_Ts>
SWmeta<Type_Key,Type_Ts>::SWmeta(pair<Type_Key, Type_Ts> & arrivalTuple)
:m_rightSearchBound(0), m_leftSearchBound(0), m_numPairExist(0), m_slope(-1), m_maxSearchError(0), m_startKey(0)
{
    #ifdef DEBUG
    cout << "[Debug Info:] Class {SWmeta} :: Construct Function {SWmeta()}" << endl;
    cout << endl;
    #endif

    SWseg<Type_Key,Type_Ts> * SWsegPtr = new SWseg<Type_Key,Type_Ts>(arrivalTuple);
    SWsegPtr->m_leftSibling = nullptr;
    SWsegPtr->m_rightSibling = nullptr;
    SWsegPtr->m_parentIndex = 0;

    m_keys.push_back(arrivalTuple.first);
    m_ptr.push_back(SWsegPtr);
    m_startKey =arrivalTuple.first;
    // m_maxTs = arrivalTuple.second;
    m_numPairExist = 1;
    m_bitmap.push_back(0);
    m_retrainBitmap.push_back(0);
    bitmap_set_bit(0);

    splitError = INITIAL_ERROR;
}

template<class Type_Key, class Type_Ts>
SWmeta<Type_Key,Type_Ts>::SWmeta(const vector<pair<Type_Key, Type_Ts>> & stream)
:m_rightSearchBound(0), m_leftSearchBound(0), m_numPairExist(0), m_slope(0), m_maxSearchError(0)
{
    #ifdef DEBUG
    cout << "[Debug Info:] Class {SWmeta} :: Construct Function {SWmeta(stream)}" << endl;
    cout << endl;
    #endif

    // m_maxTs = arrivalTuple.second;
    bulk_load(stream);

    splitError = INITIAL_ERROR;
}

template<class Type_Key, class Type_Ts>
SWmeta<Type_Key,Type_Ts>::~SWmeta()
{
    if (m_ptr.size())
    {
        for (auto & it: m_ptr)
        {
            delete it;
            it = nullptr;
        }
    }
}

/*
Bulk Load
*/
template<class Type_Key, class Type_Ts>
void SWmeta<Type_Key,Type_Ts>::bulk_load(const vector<pair<Type_Key, Type_Ts>> & stream)
{
    #ifdef DEBUG
    cout << "[Debug Info:] Class {SWmeta} :: Member Function {bulk_load()} Begin" << endl;
    cout << endl;
    #endif

    vector<pair<Type_Key, Type_Ts>>  data(stream);

    sort(data.begin(), data.end());

    vector<pair<Type_Key, SWseg<Type_Key,Type_Ts> * >> splitedDataPtr;

    split_data_slope(data, splitedDataPtr);

    if (m_slope != -1)
    {
        int currentIndex = 1;
        int currentInsertionPos = 0;
        int predictedPos;

        splitedDataPtr[0].second->m_parentIndex = currentInsertionPos;
        currentInsertionPos++;

        m_keys.push_back(splitedDataPtr[0].first);
        m_ptr.push_back(splitedDataPtr[0].second);
        m_startKey = splitedDataPtr[0].first;
        m_numPairExist = splitedDataPtr.size();
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
                
                m_keys.push_back(splitedDataPtr[currentIndex].first);
                m_ptr.push_back(splitedDataPtr[currentIndex].second);
                bitmap_set_bit(currentInsertionPos);

                currentIndex++;
            }
            else if (predictedPos < currentInsertionPos)
            {
                splitedDataPtr[currentIndex].second->m_parentIndex = currentInsertionPos;
                
                m_keys.push_back(splitedDataPtr[currentIndex].first);
                m_ptr.push_back(splitedDataPtr[currentIndex].second);
                bitmap_set_bit(currentInsertionPos);

                m_rightSearchBound = (currentInsertionPos - predictedPos) > m_rightSearchBound 
                                    ? (currentInsertionPos - predictedPos) : m_rightSearchBound;
                
                currentIndex++;
            }
            else
            {
                m_keys.push_back(m_keys.back());
                m_ptr.push_back(nullptr);
            }
            currentInsertionPos++;
        }   
        m_rightSearchBound += 1;

        m_maxSearchError = min(8192,(int)ceil(0.6*m_keys.size()));
    }
    else //Single Segment in SWmeta
    {        
        int bitmapSize = ceil(MAX_BUFFER_SIZE/64.);
        vector<uint64_t> temp(bitmapSize,0);
        
        m_startKey = splitedDataPtr[0].first;
        m_numPairExist = 1;
        m_bitmap = temp;
        m_retrainBitmap = temp;
        bitmap_set_bit(0);
        
        splitedDataPtr[0].second->m_parentIndex = 0;
        m_keys.push_back(splitedDataPtr[0].first);
        m_ptr.push_back(splitedDataPtr[0].second);
    }

    #ifdef DEBUG
    cout << "[Debug Info:] Class {SWmeta} :: Member Function {bulk_load()} End" << endl;
    cout << endl;
    #endif

}

/*
Split Segment functions
*/
template<class Type_Key, class Type_Ts>
void SWmeta<Type_Key,Type_Ts>::split_data_slope(vector<pair<Type_Key,Type_Ts>> & data, 
                                vector<pair<Type_Key, SWseg<Type_Key,Type_Ts> * >> & splitedDataPtr)
{  
    #ifdef DEBUG
    cout << "[Debug Info:] Class {SWmeta} :: Member Function {split_data_slope()} Begin" << endl;
    cout << endl;
    #endif

    vector<tuple<int,int,double>> splitIndexSlopeVector;

    calculate_split_derivative(128, data, splitIndexSlopeVector);

    if (splitIndexSlopeVector.size() != 1)
    {
        double sumKey = 0, sumIndex = 0, sumKeyIndex = 0, sumKeySquared = 0;
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

            sumKey += (double)data[get<0>(*it)].first;
            sumIndex += cnt;
            sumKeyIndex += (double)data[get<0>(*it)].first * cnt;
            sumKeySquared += (double)pow(data[get<0>(*it)].first,2);
            cnt++;
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

            sumKey += (double)data[get<0>(splitIndexSlopeVector.back())].first;
            sumIndex += cnt;
            sumKeyIndex += (double)data[get<0>(splitIndexSlopeVector.back())].first * cnt;
            sumKeySquared += (double)pow(data[get<0>(splitIndexSlopeVector.back())].first,2);
            cnt++;
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

            sumKey += (double)data.back().first;
            sumIndex += cnt;
            sumKeyIndex += (double)data.back().first * cnt;
            sumKeySquared += (double)pow(data.back().first,2);
            cnt++;
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
            cout << "WARNING: entire data is one point" << endl;
            SWseg<Type_Key,Type_Ts> * SWsegPtr = new SWseg<Type_Key,Type_Ts>(data.back());
            splitedDataPtr.push_back(make_pair( data.back().first, SWsegPtr));
        }
        
        m_slope = -1;
    }

    if (splitedDataPtr.front().second->m_leftSibling != nullptr)
    {
        cout << "first left sibling not null" << endl;
    }

    if (splitedDataPtr.back().second->m_rightSibling != nullptr)
    {
        cout << "last right sibling not null" << endl;
    }

    #ifdef DEBUG
    cout << "[Debug Info:] Class {SWmeta} :: Member Function {split_data_slope()} End" << endl;
    cout << endl;
    #endif
}

/*
Retrain Segments
*/

template<class Type_Key, class Type_Ts>
void SWmeta<Type_Key,Type_Ts>::retrain_seg(pair<int,int> SWsegIndexes, Type_Ts & lowerLimit,
                                vector<pair<Type_Key, SWseg<Type_Key,Type_Ts> * >> & splitedDataPtr)
{  
    #ifdef DEBUG
    cout << "[Debug Info:] Class {SWmeta} :: Member Function {retrain_seg()} Begin" << endl;
    cout << endl;
    #endif
    
    vector<pair<Type_Key,Type_Ts>> data;
    if (SWsegIndexes.first == SWsegIndexes.second) //Retrain Alone
    {
        #ifdef DEBUG
        cout << "[Debug Info:] Retraining Alone" << endl;
        cout << endl;
        #endif

        m_ptr[SWsegIndexes.first]->merge_data(data,lowerLimit);
    }
    else //Retrain with neighbours
    {
        #ifdef DEBUG
        cout << "[Debug Info:] Retraining with Neighbours" << endl;
        cout << endl;
        #endif

        while(SWsegIndexes.first != SWsegIndexes.second+1)
        {
            if (bitmap_exists(SWsegIndexes.first))
            { 
                m_ptr[SWsegIndexes.first]->merge_data(data,lowerLimit);

            }
            SWsegIndexes.first++;
        }
    }

    #ifdef TUNE_TIME
    uint64_t temp = 0;
    startTimer(&temp);
    #endif

    vector<tuple<int,int,double>> splitIndexSlopeVector;

    #ifdef TUNE
    calculate_split_index_one_pass(data,splitIndexSlopeVector,splitError);
    #else
    calculate_split_index_one_pass(data,splitIndexSlopeVector);
    #endif

    for (auto it = splitIndexSlopeVector.begin(); it != splitIndexSlopeVector.end()-1; it++)
    {
        SWseg<Type_Key,Type_Ts> * SWsegPtr = new SWseg<Type_Key,Type_Ts>(get<0>(*it), get<1>(*it) ,get<2>(*it), data);
        
        if(splitedDataPtr.size() > 0)
        {
            SWsegPtr->m_leftSibling = splitedDataPtr[splitedDataPtr.size() - 1].second;
            splitedDataPtr[splitedDataPtr.size() - 1].second->m_rightSibling = SWsegPtr;
        }

        splitedDataPtr.push_back(make_pair( data[get<0>(*it)].first, SWsegPtr));

    }

    //Dealing with last segment with only one point
    if (get<0>(splitIndexSlopeVector.back())  != get<1>(splitIndexSlopeVector.back()))
    {
        SWseg<Type_Key,Type_Ts> * SWsegPtr = new SWseg<Type_Key,Type_Ts>(get<0>(splitIndexSlopeVector.back()), get<1>(splitIndexSlopeVector.back()) ,get<2>(splitIndexSlopeVector.back()), data);
        
        if(splitedDataPtr.size() > 0)
        {
            SWsegPtr->m_leftSibling = splitedDataPtr.back().second;
            splitedDataPtr.back().second->m_rightSibling = SWsegPtr;
        }

        splitedDataPtr.push_back(make_pair( data[get<0>(splitIndexSlopeVector.back())].first, SWsegPtr));

    }
    else //Single Point 
    {

        SWseg<Type_Key,Type_Ts> * SWsegPtr = new SWseg<Type_Key,Type_Ts>(data.back());

        if(splitedDataPtr.size() > 0)
        {
            SWsegPtr->m_leftSibling = splitedDataPtr.back().second;
            splitedDataPtr.back().second->m_rightSibling = SWsegPtr;
        }

        splitedDataPtr.push_back(make_pair( data.back().first, SWsegPtr));
    }

    #ifdef TUNE
    segLengthRetrain += data.size();
    #endif

    #ifdef TUNE_TIME
    stopTimer(&temp);
    segRetrainCycle += temp;
    segLengthRetrain += data.size();
    segRetrainCyclePerLength += (double)temp/data.size();
    #endif

    if (splitedDataPtr.front().second->m_leftSibling != nullptr)
    {
        cout << "first left sibling not null" << endl;
    }

    if (splitedDataPtr.back().second->m_rightSibling != nullptr)
    {
        cout << "last right sibling not null" << endl;
    }

    #ifdef DEBUG
    cout << "[Debug Info:] Class {SWmeta} :: Member Function {retrain_seg()} End" << endl;
    cout << endl;
    #endif
}

/*
Point Lookup
*/

template<class Type_Key, class Type_Ts>
void SWmeta<Type_Key,Type_Ts>::lookup(pair<Type_Key, Type_Ts> & arrivalTuple, Type_Key & resultCount)
{
    #if defined DEBUG 
    cout << "[Debug Info:] Class {SWmeta} :: Member Function {lookup()} Begin" << endl;
    cout << endl;
    #elif defined DEBUG_KEY
    if(DEBUG_KEY == arrivalTuple.first)
    {
        cout << "[Debug Info:] Class {SWmeta} :: Member Function {lookup()} Begin" << endl;
        cout << endl;
    }
    #endif

    //Does not include deletion
    Type_Key newKey = arrivalTuple.first;
    Type_Ts newTimeStamp = arrivalTuple.second;
    // Type_Ts lowerLimit =  ((double)m_maxTs - TIME_WINDOW < numeric_limits<Type_Ts>::min()) ? numeric_limits<Type_Ts>::min(): m_maxTs - TIME_WINDOW;
    Type_Ts lowerLimit =  ((double)newTimeStamp - TIME_WINDOW < numeric_limits<Type_Ts>::min()) ? numeric_limits<Type_Ts>::min(): newTimeStamp - TIME_WINDOW;

    int foundPos = 0;

    if (m_keys.size() > 1)
    {   
        if (newKey > m_startKey)
        {
            if (m_slope != -1)
            {
                predict_search(newKey, foundPos);
            }
            else
            {
                exponential_search(newKey,foundPos);
            }
        }

        if (!bitmap_exists(foundPos))
        {
            foundPos = bitmap_closest_left_nongap(foundPos);

            //Assumption: if it does not exist in left of search key, it does not exist.
            if (foundPos == -1)
            {
                return;
            }
        }
    }
    else
    {
        if (!bitmap_exists(0))
        {
            throw invalid_argument("SWmeta is empty");
        }
    }
    
    m_ptr[foundPos]->lookup(newKey, lowerLimit, resultCount);

    #if defined DEBUG 
    cout << "[Debug Info:] Class {SWmeta} :: Member Function {lookup()} End" << endl;
    cout << endl;
    #elif defined DEBUG_KEY
    if(DEBUG_KEY == arrivalTuple.first)
    {
        cout << "[Debug Info:] Class {SWmeta} :: Member Function {lookup()} End" << endl;
        cout << endl;
    }
    #endif
}

/*
Range Search
*/

template<class Type_Key, class Type_Ts>
void SWmeta<Type_Key,Type_Ts>::range_search(tuple<Type_Key, Type_Ts, Type_Key> & arrivalTuple,
                                                vector<pair<Type_Key, Type_Ts>> & rangeSearchResult)
{
    #if defined DEBUG 
    cout << "[Debug Info:] Class {SWmeta} :: Member Function {range_search()} Begin" << endl;
    cout << endl;
    #elif defined DEBUG_KEY
    if(DEBUG_KEY == get<0>(arrivalTuple))
    {
        cout << "[Debug Info:] Class {SWmeta} :: Member Function {range_search()} Begin" << endl;
        cout << endl;
    }
    #endif

    #if defined(TUNE_TIME) || defined(TIME_BREAKDOWN)
    #ifdef TUNE_TIME
    rootNoSearch++;
    #endif
    uint64_t timer = 0;
    startTimer(&timer);
    #endif

    Type_Key newKey = get<0>(arrivalTuple);
    Type_Ts newTimeStamp = get<1>(arrivalTuple);
    Type_Key upperBound = get<2>(arrivalTuple);
    // Type_Ts lowerLimit =  ((double)m_maxTs - TIME_WINDOW < numeric_limits<Type_Ts>::min()) ? numeric_limits<Type_Ts>::min(): m_maxTs - TIME_WINDOW;
    Type_Ts lowerLimit =  ((double)newTimeStamp - TIME_WINDOW < numeric_limits<Type_Ts>::min()) ? numeric_limits<Type_Ts>::min(): newTimeStamp - TIME_WINDOW;

    int foundPos = 0;
    
    if (m_keys.size() > 1)
    {
        if (newKey > m_startKey)
        {
            if (m_slope != -1)
            {
                predict_search(newKey, foundPos);
            }
            else
            {
                exponential_search(newKey, foundPos);
            }
        }
        
        if (!bitmap_exists(foundPos))
        {
            int leftClosest = bitmap_closest_left_nongap(foundPos);

            if (leftClosest == -1)
            {
                int rightClosest = bitmap_closest_right_nongap(foundPos);

                if (rightClosest == m_keys.size() || m_keys[rightClosest] > upperBound)
                {
                    return;
                }
                else
                {
                    foundPos = rightClosest;
                }
            }
            else
            {
                foundPos = leftClosest;
            }
        }
    }
    else
    {
        if (!bitmap_exists(0))
        {
            throw invalid_argument("SWmeta is empty");
        }
    }

    #if defined(TUNE_TIME) || defined(TIME_BREAKDOWN)
    stopTimer(&timer);
    searchCycle += timer;
    #endif

    vector<pair<Type_Key,int >> updateSeg;
    m_ptr[foundPos]->range_search(newKey, newTimeStamp, lowerLimit, newKey, upperBound, rangeSearchResult, updateSeg);


    #if defined(TUNE_TIME) || defined(TIME_BREAKDOWN)
    timer = 0;
    startTimer(&timer);
    #endif

    //Rendezvous Retrain (Flag first time)
    vector<int> retrainSegIndex;
    retrainSegIndex.reserve(updateSeg.size());
    for (auto & it: updateSeg)
    {
        if (it.first != numeric_limits<Type_Key>::max()) //Retrain or Flag
        {
            if (bitmap_exists(m_retrainBitmap,it.second)) //Retrain (w/ neighbours)
            {
                retrainSegIndex.push_back(it.second);
            }
            else //Flag
            {                
                bitmap_set_bit(m_retrainBitmap,it.second);
            }
        }
        else
        {
            delete m_ptr[it.second];
            m_ptr[it.second] = nullptr;
            bitmap_erase_bit(it.second);
            bitmap_erase_bit(m_retrainBitmap,it.second);
            m_numPairExist--;

            if (it.second) // if it.second == 0 -> first index no need to replace previous keys
            {
                Type_Key previousKey = m_keys[it.second-1];
                m_keys[it.second] = previousKey;
                it.second++;

                while (!bitmap_exists(it.second) && it.second < m_keys.size())
                {
                    m_keys[it.second] = previousKey;
                    it.second++;
                }
            }
        }
    }

    vector<pair<Type_Key, SWseg<Type_Key,Type_Ts> * >> insertionNodes;
    for (auto & it: retrainSegIndex)
    {       
        
        if (bitmap_exists(m_retrainBitmap,it))
        {
            #ifdef TUNE
            segNoRetrain++;
            #endif
            
            #ifdef TUNE_TIME
            segNoRetrain++;
            #endif
            
            vector<pair<Type_Key, SWseg<Type_Key,Type_Ts> * >> tempInsertionNodes;

            pair<int,int> retrainSegmentIndex = bitmap_retrain_range(it);
            retrain_seg(retrainSegmentIndex, lowerLimit, tempInsertionNodes);

            if (m_ptr[retrainSegmentIndex.first]->m_leftSibling)
            {
                m_ptr[retrainSegmentIndex.first]->m_leftSibling->m_rightSibling = tempInsertionNodes.front().second;
                tempInsertionNodes.front().second->m_leftSibling = m_ptr[retrainSegmentIndex.first]->m_leftSibling;
            }

            if (m_ptr[retrainSegmentIndex.second]->m_rightSibling)
            {
                m_ptr[retrainSegmentIndex.second]->m_rightSibling->m_leftSibling = tempInsertionNodes.back().second;
                tempInsertionNodes.back().second->m_rightSibling = m_ptr[retrainSegmentIndex.second]->m_rightSibling;
            }

            insertionNodes.insert(insertionNodes.end(),tempInsertionNodes.begin(),tempInsertionNodes.end());

            if (retrainSegmentIndex.first == retrainSegmentIndex.second)
            {
                    delete m_ptr[retrainSegmentIndex.first];
                    m_ptr[retrainSegmentIndex.first] = nullptr;
                    bitmap_erase_bit(retrainSegmentIndex.first);
                    bitmap_erase_bit(m_retrainBitmap, retrainSegmentIndex.first);
                    m_numPairExist--;
            }
            else
            {
                for (int i = retrainSegmentIndex.first; i < retrainSegmentIndex.second+1; i++)
                {
                    if (bitmap_exists(i))
                    {
                        delete m_ptr[i];
                        m_ptr[i] = nullptr;
                        bitmap_erase_bit(i);
                        bitmap_erase_bit(m_retrainBitmap,i);
                        m_numPairExist--;
                    }
                }
            }

            if (retrainSegmentIndex.first) // if it.second == 0 -> first index no need to replace previous keys
            {
                Type_Key previousKey = m_keys[retrainSegmentIndex.first-1];
                m_keys[retrainSegmentIndex.first] = previousKey;
                retrainSegmentIndex.first++;

                while (!bitmap_exists(retrainSegmentIndex.first) && retrainSegmentIndex.first < m_keys.size())
                {
                    m_keys[retrainSegmentIndex.first] = previousKey;
                    retrainSegmentIndex.first++;
                }
            }
        }

    }

    #if defined(TUNE_TIME) || defined(TIME_BREAKDOWN)
    stopTimer(&timer);
    retrainCycle += timer;
    #endif

    //Insert into keysPtr
    int retrainExtendFlag = 0;
    for (auto & itTemp : insertionNodes)
    {            
        meta_insertion(itTemp,retrainExtendFlag,false);
    }

    #if defined(TUNE_TIME) || defined(TIME_BREAKDOWN)
    timer = 0;
    startTimer(&timer);
    #endif

    if (m_slope != -1 && retrainExtendFlag == 2)
    {
        if ((double)m_numPairExist / m_keys.size() >= 0.5)
        {
            retrainExtendFlag = 0;
        }
        
        if (m_leftSearchBound + m_rightSearchBound > m_maxSearchError || (double)m_numPairExist / m_keys.size() > 0.8)
        {
            retrainExtendFlag = 1;
        }
    }

    if (retrainExtendFlag)
    {               
        #if defined DEBUG 
        cout << "[Debug Info:] Class {SWmeta} :: Member Function {retrain()}" << endl;
        cout << endl;
        #elif defined DEBUG_KEY
        if(DEBUG_KEY == newKey)
        {
            cout << "[Debug Info:] Class {SWmeta} :: Member Function {retrain()}" << endl;
            cout << endl;
        }
        #endif
        
        meta_extend_retrain(retrainExtendFlag, lowerLimit);
    }

    #if defined(TUNE_TIME) || defined(TIME_BREAKDOWN)
    stopTimer(&timer);
    retrainCycle += timer;
    #endif

    #if defined DEBUG 
    cout << "[Debug Info:] Class {SWmeta} :: Member Function {range_search()} End" << endl;
    cout << endl;
    #elif defined DEBUG_KEY
    if(DEBUG_KEY == newKey)
    {
        cout << "[Debug Info:] Class {SWmeta} :: Member Function {range_search()} End" << endl;
        cout << endl;
    }
    #endif
}


//For Append Workload, Remove dangling tuples at the start of the index.
template<class Type_Key, class Type_Ts>
void SWmeta<Type_Key,Type_Ts>::range_search_ordered_data(tuple<Type_Key, Type_Ts, Type_Key> & arrivalTuple,
                                                vector<pair<Type_Key, Type_Ts>> & rangeSearchResult)
{    
    // Type_Ts lowerLimit =  ((double)m_maxTs - TIME_WINDOW < numeric_limits<Type_Ts>::min()) ? numeric_limits<Type_Ts>::min(): m_maxTs - TIME_WINDOW;
    Type_Ts lowerLimit =  ((double)get<1>(arrivalTuple) - TIME_WINDOW < numeric_limits<Type_Ts>::min()) 
                            ? numeric_limits<Type_Ts>::min(): get<1>(arrivalTuple) - TIME_WINDOW;

    for (int i = 0; i < m_ptr.size(); ++i)
    {
        if (bitmap_exists(i))
        {
            if (m_ptr[i]->m_maxTimeStamp < lowerLimit)
            {
                if (m_ptr[i]->m_rightSibling != nullptr) 
                {
                    m_ptr[i]->m_rightSibling->m_leftSibling = nullptr;
                }
                
                delete m_ptr[i];
                m_ptr[i] = nullptr;
                bitmap_erase_bit(i);
                bitmap_erase_bit(m_retrainBitmap,i);
                m_numPairExist--;
            }
            else
            {
                break;
            }
        }
    }

    return range_search(arrivalTuple, rangeSearchResult);
}

template<class Type_Key, class Type_Ts>
void SWmeta<Type_Key,Type_Ts>::insert(pair<Type_Key, Type_Ts> & arrivalTuple)
{
    #if defined DEBUG 
    cout << "[Debug Info:] Class {SWmeta} :: Member Function {insert()} Begin" << endl;
    cout << endl;
    #elif defined DEBUG_KEY
    if(DEBUG_KEY == arrivalTuple.first)
    {
        cout << "[Debug Info:] Class {SWmeta} :: Member Function {insert()} Begin" << endl;
        cout << "[Debug Info:] m_startKey =  " << m_startKey << endl;
        cout << "[Debug Info:] m_numPair =  " << m_keys.size() << endl;
        cout << "[Debug Info:] m_numPairExist =  " << m_numPairExist << endl;
        cout << "[Debug Info:] m_slope =  " << m_slope << endl;
        cout << "[Debug Info:] m_rightSearchBound =  " << m_rightSearchBound << endl;
        cout << "[Debug Info:] m_leftSearchBound =  " << m_leftSearchBound << endl;
        cout << endl;
    }
    #endif

    #if defined(TUNE_TIME) || defined(TIME_BREAKDOWN)
    #ifdef TUNE_TIME
    rootNoSearch++;
    #endif
    uint64_t timer = 0;
    startTimer(&timer);
    #endif

    Type_Key newKey = arrivalTuple.first;
    Type_Ts newTimeStamp = arrivalTuple.second;
    // m_maxTs = (m_maxTs < newTimeStamp)? newTimeStamp: m_maxTs;
    // Type_Ts lowerLimit =  ((double)m_maxTs - TIME_WINDOW < numeric_limits<Type_Ts>::min()) ? numeric_limits<Type_Ts>::min(): m_maxTs - TIME_WINDOW;
    Type_Ts lowerLimit =  ((double)newTimeStamp - TIME_WINDOW < numeric_limits<Type_Ts>::min()) ? numeric_limits<Type_Ts>::min(): newTimeStamp - TIME_WINDOW;

    int foundPos = 0;
    if (m_keys.size() > 1)
    {
        if (newKey > m_startKey)
        {
            if (m_slope != -1)
            {
                predict_search(newKey, foundPos);
            }
            else
            {
                exponential_search(newKey, foundPos);
            }
        }

        if (!bitmap_exists(foundPos))
        {
            int leftClosest = bitmap_closest_left_nongap(foundPos);

            if (leftClosest == -1)
            {
                int rightClosest = bitmap_closest_right_nongap(foundPos);

                if (rightClosest == m_keys.size())
                {
                    cout << "Error: Entire Index is Empty" << endl;
                    return;
                }
                else
                {
                    foundPos = rightClosest;
                }
            }
            else
            {
                foundPos = leftClosest;
            }
        }
    }
    else
    {
        if (!bitmap_exists(0))
        {
            throw invalid_argument("SWmeta is empty");
        }
    }

    #if defined(TUNE_TIME) || defined(TIME_BREAKDOWN)
    stopTimer(&timer);
    searchCycle += timer;
    #endif

    vector<pair<Type_Key,int >> updateSeg;
    m_ptr[foundPos]->insert(newKey, newTimeStamp, lowerLimit, updateSeg);

    //Insert into SWmeta and Retrain if neccessary
    if (updateSeg.size() > 1)
    {
        if (updateSeg[1].first == numeric_limits<Type_Key>::max())
        {
            swap(updateSeg[0], updateSeg[1]);
        }

        if (updateSeg.size() > 2)
        {
            cout << "Error: Insertion No Update Seg > 2" << endl; //Check: Delete later
        }

        if (updateSeg[0].first != numeric_limits<Type_Key>::max() &&  updateSeg[1].first != numeric_limits<Type_Key>::max())
        {
            cout << "Error: Insertion Two Segments and not deleting any" << endl; //Check: Delete later
        }
    }

    for (auto &it : updateSeg)
    {
        if (it.first != numeric_limits<Type_Key>::max())
        {
            if (it.second%10 == 1)
            {               
                it.second /= 10;

                if (bitmap_exists(m_retrainBitmap,it.second))
                {
                    #ifdef TUNE
                    segNoRetrain++;
                    #endif
                    
                    #if defined(TUNE_TIME) || defined(TIME_BREAKDOWN)
                    #ifdef TUNE_TIME
                    segNoRetrain++;
                    #endif
                    timer = 0;
                    startTimer(&timer);
                    #endif
                    
                    vector<pair<Type_Key, SWseg<Type_Key,Type_Ts> * >> tempInsertionNodes;

                    pair<int,int> retrainSegmentIndex = bitmap_retrain_range(it.second);

                    retrain_seg(retrainSegmentIndex, lowerLimit, tempInsertionNodes);
                    
                    if (m_ptr[retrainSegmentIndex.first]->m_leftSibling)
                    {
                        m_ptr[retrainSegmentIndex.first]->m_leftSibling->m_rightSibling = tempInsertionNodes.front().second;
                        tempInsertionNodes.front().second->m_leftSibling = m_ptr[retrainSegmentIndex.first]->m_leftSibling;
                    }

                    if (m_ptr[retrainSegmentIndex.second]->m_rightSibling)
                    {
                        m_ptr[retrainSegmentIndex.second]->m_rightSibling->m_leftSibling = tempInsertionNodes.back().second;
                        tempInsertionNodes.back().second->m_rightSibling = m_ptr[retrainSegmentIndex.second]->m_rightSibling;
                    }

                    #if defined(TUNE_TIME) || defined(TIME_BREAKDOWN)
                    stopTimer(&timer);
                    retrainCycle += timer;
                    #endif

                    if (retrainSegmentIndex.first == retrainSegmentIndex.second)
                    {
                        delete m_ptr[it.second];
                        m_ptr[it.second] = nullptr;
                        bitmap_erase_bit(it.second);
                        bitmap_erase_bit(m_retrainBitmap,it.second);
                        m_numPairExist--;
                    }
                    else
                    {
                        for (int i = retrainSegmentIndex.first; i < retrainSegmentIndex.second+1; i++)
                        {
                            if (bitmap_exists(i))
                            {
                                delete m_ptr[i];
                                m_ptr[i] = nullptr;
                                bitmap_erase_bit(i);
                                bitmap_erase_bit(m_retrainBitmap,i);
                                m_numPairExist--;
                            }
                        }
                    }

                    if (retrainSegmentIndex.first) // if it.second == 0 -> first index no need to replace previous keys
                    {
                        Type_Key previousKey = m_keys[retrainSegmentIndex.first-1];
                        m_keys[retrainSegmentIndex.first] = previousKey;
                        retrainSegmentIndex.first++;

                        while (!bitmap_exists(retrainSegmentIndex.first) && retrainSegmentIndex.first < m_keys.size())
                        {
                            m_keys[retrainSegmentIndex.first] = previousKey;
                            retrainSegmentIndex.first++;
                        }
                    }

                    //Insert into keysPtr
                    int retrainExtendFlag = 0;
                    for (auto & itTemp : tempInsertionNodes)
                    {                                                
                        meta_insertion(itTemp,retrainExtendFlag,false);
                    }

                    #if defined(TUNE_TIME) || defined(TIME_BREAKDOWN)
                    timer = 0;
                    startTimer(&timer);
                    #endif

                    if (retrainExtendFlag)
                    {               
                        #if defined DEBUG 
                        cout << "[Debug Info:] Class {SWmeta} :: Member Function {retrain()}" << endl;
                        cout << endl;
                        #elif defined DEBUG_KEY
                        if(DEBUG_KEY == arrivalTuple.first)
                        {
                            cout << "[Debug Info:] Class {SWmeta} :: Member Function {retrain()}" << endl;
                            cout << endl;
                        }
                        #endif
                        meta_extend_retrain(retrainExtendFlag, lowerLimit);
                    }

                    #if defined(TUNE_TIME) || defined(TIME_BREAKDOWN)
                    stopTimer(&timer);
                    retrainCycle += timer;
                    #endif
                }
                else
                {
                    bitmap_set_bit(m_retrainBitmap,it.second);
                }
            }
            else //Replace SWseg
            {
                it.second /= 10;

                pair<Type_Key, SWseg<Type_Key,Type_Ts>*> tempPair = make_pair(it.first,m_ptr[it.second]);
                bool tempBitmap = (bitmap_exists(m_retrainBitmap,it.second)) ? true : false;

                m_ptr[it.second] = nullptr;
                bitmap_erase_bit(it.second);
                bitmap_erase_bit(m_retrainBitmap,it.second);
                m_numPairExist--;

                if (it.second) // if it.second == 0 -> first index no need to replace previous keys
                {
                    Type_Key previousKey = m_keys[it.second-1];
                    m_keys[it.second] = previousKey;
                    it.second++;

                    while (!bitmap_exists(it.second) && it.second < m_keys.size())
                    {
                        m_keys[it.second] = previousKey;
                        it.second++;
                    }
                }
                
                int retrainExtendFlag = 0;
                meta_insertion(tempPair,retrainExtendFlag,tempBitmap);

                #if defined(TUNE_TIME) || defined(TIME_BREAKDOWN)
                timer = 0;
                startTimer(&timer);
                #endif

                if (retrainExtendFlag)
                {               
                    #if defined DEBUG 
                    cout << "[Debug Info:] Class {SWmeta} :: Member Function {retrain()}" << endl;
                    cout << endl;
                    #elif defined DEBUG_KEY
                    if(DEBUG_KEY == arrivalTuple.first)
                    {
                        cout << "[Debug Info:] Class {SWmeta} :: Member Function {retrain()}" << endl;
                        cout << endl;
                    }
                    #endif

                    meta_extend_retrain(retrainExtendFlag, lowerLimit);

                }

                #if defined(TUNE_TIME) || defined(TIME_BREAKDOWN)
                stopTimer(&timer);
                retrainCycle += timer;
                #endif
            }
        }
        else
        {
            delete m_ptr[it.second];
            m_ptr[it.second] = nullptr;
            bitmap_erase_bit(it.second);
            bitmap_erase_bit(m_retrainBitmap,it.second);
            m_numPairExist--;
            
            if (it.second) // if it.second == 0 -> first index no need to replace previous keys
            {
                Type_Key previousKey = m_keys[it.second-1];
                m_keys[it.second] = previousKey;
                it.second++;

                while (!bitmap_exists(it.second) && it.second < m_keys.size())
                {
                    m_keys[it.second] = previousKey;
                    it.second++;
                }
            }
        }
    }

    #if defined DEBUG 
    cout << "[Debug Info:] Class {SWmeta} :: Member Function {insert()} End" << endl;
    cout << endl;
    #elif defined DEBUG_KEY
    if(DEBUG_KEY == arrivalTuple.first)
    {
        cout << "[Debug Info:] Class {SWmeta} :: Member Function {insert()} End" << endl;
        cout << endl;
    }
    #endif
}


/*
Node Functions
*/
template<class Type_Key, class Type_Ts>
void SWmeta<Type_Key,Type_Ts>::meta_insertion(pair<Type_Key, SWseg<Type_Key,Type_Ts>*> & insertKeyPtr, int & retrainExtendFlag, bool retrainSetBit)
{
    #if defined(TUNE_TIME) || defined(TIME_BREAKDOWN)
    #ifdef TUNE_TIME
    rootNoInsert++;
    rootSizeInsert += m_keys.size();
    #endif
    uint64_t temp = 0;
    uint64_t temp2 = 0;
    startTimer(&temp);
    #endif
    
    int insertionPos = 0;
    bool gapAddError = false;

    if (m_slope != -1)
    {
        if (insertKeyPtr.first < m_startKey)
        {
            meta_insertion_model(insertionPos, insertKeyPtr, gapAddError, retrainExtendFlag, retrainSetBit);
        }
        else
        {
            #if defined(TUNE_TIME) || defined(TIME_BREAKDOWN)
            startTimer(&temp2);
            uint64_t temp3 = 0;
            startTimer(&temp3);
            #ifdef TUNE_TIME
            rootNoSearch++;
            #endif
            #endif

            int predictPos, predictPosMin, predictPosMax;
            find_predict_pos_bound(insertKeyPtr.first, predictPos, predictPosMin, predictPosMax);

            #if defined(TUNE_TIME) || defined(TIME_BREAKDOWN)
            stopTimer(&temp3);
            #endif

            int predictPosMaxUnbounded = (m_rightSearchBound == 0) ? predictPos + 1 : predictPos + m_rightSearchBound;

            if (predictPosMin < predictPosMax)
            {
                predictPos = predictPos < predictPosMin ? predictPosMin : predictPos;
                predictPos = predictPos > predictPosMax ? predictPosMax : predictPos;
                insertionPos = predictPos;

                if (insertKeyPtr.first < m_keys[insertionPos]) // Left of predictedPos
                {
                    exponential_search_left_insert(insertKeyPtr.first, insertionPos, predictPos-predictPosMin);
                }
                else if (insertKeyPtr.first > m_keys[insertionPos]) //Right of predictedPos
                {
                    exponential_search_right_insert(insertKeyPtr.first, insertionPos, predictPosMax-predictPos);
                }

                #if defined(TUNE_TIME) || defined(TIME_BREAKDOWN)
                stopTimer(&temp2);
                searchCycle += temp2 - temp3;
                #endif

                if (insertionPos > predictPosMaxUnbounded) 
                {
                    gapAddError = true;
                }

                if (insertionPos < m_keys.size())
                {
                    meta_insertion_model(insertionPos, insertKeyPtr, gapAddError, retrainExtendFlag, retrainSetBit);
                }
                else
                {               
                    //Append and increment m_rightSearchBound if nessecary
                    insertionPos = m_keys.size();
                    m_keys.push_back(insertKeyPtr.first);
                    m_ptr.push_back(insertKeyPtr.second);
                    insertKeyPtr.second->m_parentIndex = insertionPos;
                    m_numPairExist++;

                    if (static_cast<int>(insertionPos >> 6) == m_bitmap.size())
                    {
                        m_bitmap.push_back(0);
                        m_retrainBitmap.push_back(0);
                    }
                    bitmap_set_bit(insertionPos);
                    if (retrainSetBit)
                    {
                        bitmap_set_bit(m_retrainBitmap,insertionPos);
                    }

                    if (predictPosMaxUnbounded < insertionPos)
                    {
                        m_rightSearchBound++;
                    }

                    if (m_leftSearchBound + m_rightSearchBound > m_maxSearchError || (double)m_numPairExist / m_keys.size() > 0.8)
                    {
                        retrainExtendFlag = max(retrainExtendFlag, ((double)m_numPairExist / m_keys.size() < 0.5) ? 2 : 1);
                    }
                }
            }
            else if (predictPosMin == predictPosMax) // insertPos == m_keys.size()-1
            {    
                #if defined(TUNE_TIME) || defined(TIME_BREAKDOWN)
                stopTimer(&temp2);
                searchCycle += temp2 - temp3;
                #endif
                
                meta_insertion_end(insertKeyPtr,retrainExtendFlag, retrainSetBit);
            }
            else
            {
                #if defined(TUNE_TIME) || defined(TIME_BREAKDOWN)
                stopTimer(&temp2);
                searchCycle += temp2 - temp3;
                #endif

                insertionPos = predictPos;
                meta_insertion_append(insertionPos,insertKeyPtr,retrainExtendFlag, retrainSetBit);
            }
        }

        m_maxSearchError = min(8192,(int)ceil(0.6*m_keys.size()));
    }
    else
    {    
        if (m_keys.size() > 1)
        {
            if (insertKeyPtr.first < m_startKey)
            {
                meta_insertion_model(insertionPos, insertKeyPtr, gapAddError, retrainExtendFlag, retrainSetBit);
            }
            else
            {
                #if defined(TUNE_TIME) || defined(TIME_BREAKDOWN)
                startTimer(&temp2);
                #endif
                
                exponential_search_insert(insertKeyPtr.first, insertionPos);

                #if defined(TUNE_TIME) || defined(TIME_BREAKDOWN)
                stopTimer(&temp2);
                searchCycle += temp2;
                #endif

                if (insertionPos < m_keys.size())
                {
                    meta_insertion_model(insertionPos, insertKeyPtr, gapAddError, retrainExtendFlag, retrainSetBit);
                }
                else
                {
                    //Append
                    insertionPos = m_keys.size();
                    m_keys.push_back(insertKeyPtr.first);
                    m_ptr.push_back(insertKeyPtr.second);
                    insertKeyPtr.second->m_parentIndex = insertionPos;
                    m_numPairExist++;
                    
                    if (static_cast<int>(insertionPos >> 6) == m_bitmap.size())
                    {
                        m_bitmap.push_back(0);
                        m_retrainBitmap.push_back(0);
                    }

                    bitmap_set_bit(insertionPos);
                    if (retrainSetBit)
                    {
                        bitmap_set_bit(m_retrainBitmap,insertionPos);
                    }
                }
            }
        }
        else
        {
            meta_insertion_end(insertKeyPtr,retrainExtendFlag, retrainSetBit);
        }

        m_startKey = m_keys.front();

        if (m_keys.size() == MAX_BUFFER_SIZE)
        {
            retrainExtendFlag = 2;
        }
        else
        {
            retrainExtendFlag = 0;
        }
    }

    #if defined(TUNE_TIME) || defined(TIME_BREAKDOWN)
    stopTimer(&temp);
    insertCycle += temp - temp2;
    #ifdef TUNE_TIME
    rootInsertCycle += temp - temp2;
    rootInsertCyclePerLength += (double)(temp - temp2)/(m_keys.size());
    #endif
    #endif
}

template<class Type_Key, class Type_Ts>
void SWmeta<Type_Key,Type_Ts>::meta_insertion_model(int & insertionPos, pair<Type_Key, SWseg<Type_Key,Type_Ts>*> & insertKeyPtr, bool & gapAddError, int & retrainExtendFlag, bool & retrainSetBit)
{

    //If insertion position is not a gap
    if (bitmap_exists(insertionPos))
    {
        int gapPos = bitmap_closest_gap(insertionPos);

        if (gapPos == -1)
        {
            cout << m_numPairExist << "," << m_keys.size() << endl;
            cout << insertKeyPtr.first << "(" << insertionPos << ")" << endl;
            cout << m_keys[insertionPos-1] << "," << m_keys[insertionPos] << endl;
            throw invalid_argument("Finding Gap Position Error");
        }

        //Shift with Gap
        if (gapPos != m_keys.size())
        {       
            if (insertionPos < gapPos) //Right Shift
            {
                vector<Type_Key> tempKey(m_keys.begin()+insertionPos,m_keys.begin()+gapPos);
                move(tempKey.begin(),tempKey.end(),m_keys.begin()+insertionPos+1);
                m_keys[insertionPos] = insertKeyPtr.first;

                for (auto it = m_ptr.begin() + insertionPos; it != m_ptr.begin() + gapPos; it++)
                //Make this faster later by testing increment_parent_index_until_bound
                {
                    if ((*it) != nullptr)
                    {
                        (*it)->m_parentIndex++;
                    }
                }

                vector<SWseg<Type_Key,Type_Ts>*> tempPtr(m_ptr.begin()+insertionPos,m_ptr.begin()+gapPos);
                move(tempPtr.begin(),tempPtr.end(),m_ptr.begin()+insertionPos+1);
                m_ptr[insertionPos] = insertKeyPtr.second;
                // m_ptr[insertionPos]->increment_parent_index_until_bound(gapPos);

                bitmap_move_bit_back(insertionPos,gapPos);
                bitmap_set_bit(insertionPos);
                bitmap_move_bit_back(m_retrainBitmap,insertionPos,gapPos);

                if (retrainSetBit)
                {
                    bitmap_set_bit(m_retrainBitmap,insertionPos);
                }
                else
                {
                    bitmap_erase_bit(m_retrainBitmap,insertionPos);
                }

                insertKeyPtr.second->m_parentIndex = insertionPos;

                m_numPairExist++;
                m_rightSearchBound++;
            }
            else //Left Shift
            {
                insertionPos--;
                vector<Type_Key> tempKey(m_keys.begin()+gapPos+1,m_keys.begin()+insertionPos+1);
                move(tempKey.begin(),tempKey.end(),m_keys.begin()+gapPos);
                m_keys[insertionPos] = insertKeyPtr.first;

                for (auto it = m_ptr.begin() + insertionPos; it != m_ptr.begin() + gapPos; it--)
                //Make this faster later by testing increment_parent_index_until_bound
                {
                    if ((*it) != nullptr)
                    {
                        (*it)->m_parentIndex--;
                    }
                }

                vector<SWseg<Type_Key,Type_Ts>*> tempPtr(m_ptr.begin()+gapPos+1,m_ptr.begin()+insertionPos+1);
                move(tempPtr.begin(),tempPtr.end(),m_ptr.begin()+gapPos);
                m_ptr[insertionPos] = insertKeyPtr.second;
                // m_ptr[insertionPos]->decrement_parent_index_until_bound(gapPos);

                bitmap_move_bit_front(insertionPos,gapPos);
                bitmap_set_bit(insertionPos);
                bitmap_move_bit_front(m_retrainBitmap,insertionPos,gapPos);
                if (retrainSetBit)
                {
                    bitmap_set_bit(m_retrainBitmap,insertionPos);
                }
                else
                {
                    bitmap_erase_bit(m_retrainBitmap,insertionPos);
                }

                insertKeyPtr.second->m_parentIndex = insertionPos;

                m_numPairExist++;
                m_leftSearchBound++;
            }  
        }
        //Insert (Gap at the end)
        else
        {
            m_keys.insert(m_keys.begin()+insertionPos,insertKeyPtr.first);

            for (auto it = m_ptr.begin() + insertionPos; it != m_ptr.end(); it++)
            //Make this faster later by testing increment_parent_index_until_bound
            {
                if ((*it) != nullptr)
                {
                    (*it)->m_parentIndex++;
                }
            }

            m_ptr.insert(m_ptr.begin()+insertionPos,insertKeyPtr.second);
            // m_ptr[insertionPos]->increment_parent_index_until_bound(gapPos);

            bitmap_move_bit_back(insertionPos,m_keys.size());
            bitmap_set_bit(insertionPos);
            bitmap_move_bit_back(m_retrainBitmap,insertionPos,m_keys.size());
            if (retrainSetBit)
            {
                bitmap_set_bit(m_retrainBitmap,insertionPos);
            }
            else
            {
                bitmap_erase_bit(m_retrainBitmap,insertionPos);
            }

            insertKeyPtr.second->m_parentIndex = insertionPos;

            m_numPairExist++;
            m_rightSearchBound++;
        }

        if (m_leftSearchBound + m_rightSearchBound > m_maxSearchError || (double)m_numPairExist / m_keys.size() > 0.8)
        {
            retrainExtendFlag = max(retrainExtendFlag, ((double)m_numPairExist / m_keys.size() < 0.5) ? 2 : 1);
        }
    }
    //If insertion position is a gap
    else
    {
        m_keys[insertionPos] = insertKeyPtr.first;
        m_ptr[insertionPos] = insertKeyPtr.second;
        insertKeyPtr.second->m_parentIndex = insertionPos;
        bitmap_set_bit(insertionPos);
        if (retrainSetBit)
        {
            bitmap_set_bit(m_retrainBitmap,insertionPos);
        }
        m_numPairExist++;
        
        for (auto it = m_keys.begin()+insertionPos+1; it != m_keys.end(); it++ )
        {
            if (*it >= insertKeyPtr.first)
            {
                break;
            }
            else
            {
                *it = insertKeyPtr.first;
            }
        }

        if (gapAddError)
        {
            m_rightSearchBound++;

            if (m_leftSearchBound + m_rightSearchBound > m_maxSearchError || (double)m_numPairExist / m_keys.size() > 0.8)
            {
                retrainExtendFlag = max(retrainExtendFlag, ((double)m_numPairExist / m_keys.size() < 0.5) ? 2 : 1);
            }
        }
    }
}

template<class Type_Key, class Type_Ts>
void SWmeta<Type_Key,Type_Ts>::meta_insertion_end(pair<Type_Key, SWseg<Type_Key,Type_Ts>*> & insertKeyPtr, int & retrainExtendFlag, bool & retrainSetBit)
{
    if (bitmap_exists(m_keys.size()-1))
    {
        if (static_cast<int>(m_keys.size() >> 6) == m_bitmap.size())
        {
            m_bitmap.push_back(0);
            m_retrainBitmap.push_back(0);
        }

        if (insertKeyPtr.first < m_keys.back())
        {
            //Insert and increment m_rightSearchBound
            bitmap_move_bit_back(m_keys.size()-1,m_keys.size());
            bitmap_set_bit(m_keys.size()-1);
            bitmap_move_bit_back(m_retrainBitmap,m_keys.size()-1,m_keys.size());
            if (retrainSetBit)
            {
                bitmap_set_bit(m_retrainBitmap,m_keys.size()-1);
            }
            else
            {
                bitmap_erase_bit(m_retrainBitmap,m_keys.size()-1);
            }

            m_keys.insert(m_keys.end()-1,insertKeyPtr.first);
            
            m_ptr.back()->m_parentIndex++;
            m_ptr.insert(m_ptr.end()-1,insertKeyPtr.second);

            insertKeyPtr.second->m_parentIndex = m_keys.size()-2;

            m_numPairExist++;
            m_rightSearchBound++;

            if (m_leftSearchBound + m_rightSearchBound > m_maxSearchError || (double)m_numPairExist / m_keys.size() > 0.8)
            {
                retrainExtendFlag = max(retrainExtendFlag, ((double)m_numPairExist / m_keys.size() < 0.5) ? 2 : 1);
            }
        }
        else
        {
            bitmap_set_bit(m_keys.size());
            if (retrainSetBit)
            {
                bitmap_set_bit(m_retrainBitmap,m_keys.size());
            }

            m_keys.push_back(insertKeyPtr.first);
            m_ptr.push_back(insertKeyPtr.second);

            insertKeyPtr.second->m_parentIndex = m_keys.size()-1;

            m_numPairExist++;
        }
    }
    else
    {
        m_keys.back() = insertKeyPtr.first;
        m_ptr.back() = insertKeyPtr.second;
        bitmap_set_bit(m_keys.size()-1);
        if (retrainSetBit)
        {
            bitmap_set_bit(m_retrainBitmap,m_keys.size()-1);
        }
        insertKeyPtr.second->m_parentIndex = m_keys.size()-1;
        m_numPairExist++;
    }

}

template<class Type_Key, class Type_Ts>
void SWmeta<Type_Key,Type_Ts>::meta_insertion_append(int & insertionPos, pair<Type_Key, SWseg<Type_Key,Type_Ts>*> & insertKeyPtr, int & retrainExtendFlag, bool & retrainSetBit)
{

    Type_Key lastKey = m_keys.back();
    for (int index = m_keys.size(); index <= insertionPos; index++)
    {
        m_keys.push_back(lastKey);
        m_ptr.push_back(nullptr);
    }
    m_keys.back() = insertKeyPtr.first;
    m_ptr.back() = insertKeyPtr.second;
    insertKeyPtr.second->m_parentIndex = insertionPos;
    m_numPairExist++;

    int bitmapExpandTimes = static_cast<int>(insertionPos >> 6) - (m_bitmap.size()-1);
    for (int i = 0; i < bitmapExpandTimes; i++)
    {
        m_bitmap.push_back(0);
        m_retrainBitmap.push_back(0);
    }
    bitmap_set_bit(insertionPos);
    if (retrainSetBit)
    {
        bitmap_set_bit(m_retrainBitmap,insertionPos);
    }

    if (((double)m_numPairExist) / (insertionPos+1) < 0.5)
    {
        retrainExtendFlag = 2;
    }
    else if ((double)m_numPairExist / (insertionPos+1) > 0.8)
    {
        retrainExtendFlag = 1;
    }

}


template<class Type_Key, class Type_Ts>
void SWmeta<Type_Key,Type_Ts>::meta_extend_retrain(int & retrainExtendFlag, Type_Ts & lowerLimit)
{
    #ifdef TUNE_TIME
    rootNoRetrain++;
    int tempSize = m_keys.size();
    uint64_t temp = 0;
    startTimer(&temp);
    #endif
    
    retrainExtendFlag = ((double)m_numPairExist/(m_keys.size()*1.05) < 0.5) ? 2 : retrainExtendFlag;
    vector<Type_Key> tempKey;
    vector<SWseg<Type_Key,Type_Ts>*> tempPtr;
    vector<uint64_t> tempBitmap;
    vector<uint64_t> tempRetrainBitmap;
    int firstIndex;
    
    if (retrainExtendFlag == 1) //Extend
    {
        m_slope *= 1.05;
        tempKey.reserve(m_keys.size()*1.05);
        tempPtr.reserve(m_ptr.size()*1.05);
    }
    else //Retrain
    {
        double sumKey = 0, sumIndex = 0, sumKeyIndex = 0, sumKeySquared = 0;
        int cnt = 0;
        for (int i = 0; i < m_keys.size(); i++)
        {
            if (bitmap_exists(i) && m_ptr[i]->m_maxTimeStamp >= lowerLimit)
            {               
                if (!cnt)
                {
                    m_startKey = m_keys[i];
                    firstIndex = i;
                }
                sumKey += (double)m_keys[i];
                sumIndex += cnt;
                sumKeyIndex += (double)m_keys[i] * cnt;
                sumKeySquared += (double)pow(m_keys[i],2);
                cnt++;
            }
        }

        if (cnt == 1)
        {
            m_slope = -1;
        }
        else
        {
            m_slope =  (sumKeyIndex - sumKey *(sumIndex/cnt))/(sumKeySquared - sumKey*(sumKey/cnt)) * 1.05;
        }

        tempKey.reserve(cnt*1.05);
        tempPtr.reserve(cnt*1.05);
    }

    if (m_slope != -1)
    {    
        //Load
        int currentIndex = 0;
        int currentInsertionPos = 0;
        int predictedPos;
        m_numPairExist = 0;
        m_rightSearchBound = 0;
        m_leftSearchBound = 0;

        while (currentIndex < m_keys.size())
        {
            if (bitmap_exists(currentIndex))
            {
                if (m_ptr[currentIndex]->m_maxTimeStamp >= lowerLimit) //Segment has not expired
                {
                    predictedPos = static_cast<int>(floor(m_slope * ((double)m_keys[currentIndex] - (double)m_startKey)));
                    
                    if (static_cast<int>(currentInsertionPos >> 6) == tempBitmap.size())
                    {
                        tempBitmap.push_back(0);
                        tempRetrainBitmap.push_back(0);
                    }

                    if (predictedPos == currentInsertionPos)
                    {
                        m_ptr[currentIndex]->m_parentIndex = currentInsertionPos;

                        tempKey.push_back(m_keys[currentIndex]);
                        tempPtr.push_back(m_ptr[currentIndex]);
                        bitmap_set_bit(tempBitmap, currentInsertionPos);

                        if (bitmap_exists(m_retrainBitmap,currentIndex))
                        {
                            bitmap_set_bit(tempRetrainBitmap,currentInsertionPos);
                        }

                        m_numPairExist++;
                        currentIndex++;
                    }
                    else if (predictedPos < currentInsertionPos)
                    {
                        m_ptr[currentIndex]->m_parentIndex = currentInsertionPos;

                        tempKey.push_back(m_keys[currentIndex]);
                        tempPtr.push_back(m_ptr[currentIndex]);
                        bitmap_set_bit(tempBitmap, currentInsertionPos);

                        if (bitmap_exists(m_retrainBitmap,currentIndex))
                        {
                            bitmap_set_bit(tempRetrainBitmap,currentInsertionPos);
                        }

                        m_rightSearchBound = (currentInsertionPos - predictedPos) > m_rightSearchBound 
                                            ? (currentInsertionPos - predictedPos) : m_rightSearchBound;
                        
                        m_numPairExist++;
                        currentIndex++;
                    }
                    else
                    {
                        tempKey.push_back((tempKey.size() == 0) ? numeric_limits<Type_Key>::min() : tempKey.back());
                        tempPtr.push_back(nullptr);
                    }
                    currentInsertionPos++;
                }
                else //Segment Expired update neighbours
                {
                    if (m_ptr[currentIndex]->m_leftSibling)
                    {
                        m_ptr[currentIndex]->m_leftSibling->m_rightSibling = m_ptr[currentIndex]->m_rightSibling;
                    }

                    if (m_ptr[currentIndex]->m_rightSibling)
                    {
                        m_ptr[currentIndex]->m_rightSibling->m_leftSibling = m_ptr[currentIndex]->m_leftSibling;
                    }
                    
                    delete m_ptr[currentIndex];
                    m_ptr[currentIndex] = nullptr;
                    currentIndex++;
                }
            }
            else
            {
                currentIndex++;
            }
        }

        m_rightSearchBound += 1;

        m_maxSearchError = min(8192,(int)ceil(0.6*m_keys.size()));
    }
    else //Single Segment in SWmeta
    {        
        m_rightSearchBound = 0;
        m_leftSearchBound = 0;

        int bitmapSize = ceil(MAX_BUFFER_SIZE/64.);
        vector<uint64_t> temp(bitmapSize,0);
        tempBitmap = temp;
        tempRetrainBitmap = temp;

        m_numPairExist = 1;
        bitmap_set_bit(tempBitmap,0);

        if (bitmap_exists(m_retrainBitmap,firstIndex))
        {
            bitmap_set_bit(tempRetrainBitmap,0);
        }
        
        m_ptr[firstIndex]->m_parentIndex = 0;
        tempKey.push_back(m_keys[firstIndex]);
        tempPtr.push_back(m_ptr[firstIndex]);
        // m_maxSearchError = 0;
    }

    m_keys = tempKey;
    m_ptr = tempPtr;
    m_bitmap = tempBitmap;
    m_retrainBitmap = tempRetrainBitmap;

    #ifdef TUNE_TIME
    stopTimer(&temp);
    rootRetrainCycle += temp;
    rootLengthRetrain += tempSize;
    rootRetrainCyclePerLength += (double)temp/tempSize;
    #endif
}

template<class Type_Key, class Type_Ts>
void SWmeta<Type_Key,Type_Ts>::predict_search(Type_Key & targetKey, int & foundPos)
{
    //Note: Will return position that is a gap 

    if (targetKey > m_keys[m_keys.size()-1])
    {
        foundPos = m_keys.size()-1;
    }
    else
    {
        int predictPos, predictPosMin, predictPosMax;
        find_predict_pos_bound(targetKey, predictPos, predictPosMin, predictPosMax);

        if (predictPosMin < predictPosMax)
        {
            predictPos = predictPos < predictPosMin ? predictPosMin : predictPos;
            predictPos = predictPos > predictPosMax ? predictPosMax : predictPos;
            foundPos = predictPos;

            if (targetKey <  m_keys[predictPos]) // Left of predictedPos
            {
                exponential_search_left(targetKey, foundPos, predictPos-predictPosMin);
            }
            else if (targetKey > m_keys[predictPos]) //Right of predictedPos
            {
                exponential_search_right(targetKey, foundPos, predictPosMax-predictPos);
            }
        }
        else if (predictPosMin == predictPosMax)
        {     
            foundPos = predictPos;
            
            if (targetKey < m_keys[predictPos])
            {
                foundPos--;
            }
        }
        else
        {
            foundPos = m_keys.size()-1;
        }        
    }
}

/*
Util Functions
*/

template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::find_predict_pos_bound(Type_Key & targetKey, int & predictPos, int & predictPosMin, int & predictPosMax)
{
    #ifdef TUNE_TIME
    uint64_t temp = 0;
    startTimer(&temp);
    #endif

    predictPos = static_cast<int>(floor(m_slope * ((double)targetKey - (double)m_startKey)));
    predictPosMin = predictPos - m_leftSearchBound;
    predictPosMin = predictPosMin < 0 ? 0 : predictPosMin;

    predictPosMax = (m_rightSearchBound == 0) ? predictPos + 1 : predictPos + m_rightSearchBound;
    predictPosMax = predictPosMax > m_keys.size()-1 ? m_keys.size()-1: predictPosMax;

    #ifdef TUNE_TIME
    stopTimer(&temp);
    rootPredictCycle += temp;
    rootNoPredict++;
    #endif
}

template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::exponential_search(Type_Key & targetKey, int & foundPos)
{
    #ifdef TUNE_TIME
    uint64_t temp = 0;
    startTimer(&temp);
    #endif

    int index = 1;
    while (index <= m_keys.size()-1 && m_keys[foundPos + index] < targetKey)
    {
        index *= 2;
    }

    auto startIt = m_keys.begin() + static_cast<int>(index/2);
    auto endIt = m_keys.begin() + min(index,(int)(m_keys.size()-1));

    auto lowerBoundIt = lower_bound(startIt,endIt,targetKey);

    if (lowerBoundIt == m_keys.end() || ((lowerBoundIt != m_keys.begin()) && (*lowerBoundIt > targetKey)))
    {
        lowerBoundIt--;
    }

    foundPos = lowerBoundIt - m_keys.begin();

    #ifdef TUNE
    rootNoExponentialSearch++;
    rootLengthExponentialSearch += foundPos;
    #endif

    #ifdef TUNE_TIME
    stopTimer(&temp);
    rootNoExponentialSearch++;
    rootLengthExponentialSearch += foundPos;
    rootExponentialSearchCycle += temp;
    if (foundPos == 0)
    {
        rootExponentialSearchCyclePerLength += (double)temp;
    }
    else
    {
        rootExponentialSearchCyclePerLength += (double)temp/foundPos;
    }
    #endif
}

template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::exponential_search_insert(Type_Key & targetKey, int & foundPos)
{
    #ifdef TUNE_TIME
    uint64_t temp = 0;
    startTimer(&temp);
    #endif

    int index = 1;
    while (index <= m_keys.size()-1 && m_keys[foundPos + index] < targetKey)
    {
        index *= 2;
    }

    auto startIt = m_keys.begin() + static_cast<int>(index/2);
    auto endIt = m_keys.begin() + min(index,(int)(m_keys.size()-1));

    auto lowerBoundIt = lower_bound(startIt,endIt,targetKey);

    foundPos = lowerBoundIt - m_keys.begin();

    if (*lowerBoundIt < targetKey && bitmap_exists(foundPos))
    {
        foundPos++;
    }

    #ifdef TUNE
    rootNoExponentialSearch++;
    rootLengthExponentialSearch += lowerBoundIt - m_keys.begin();
    #endif

    #ifdef TUNE_TIME
    stopTimer(&temp);
    rootNoExponentialSearch++;
    rootLengthExponentialSearch += lowerBoundIt - m_keys.begin();
    rootExponentialSearchCycle += temp;
    if (lowerBoundIt == m_keys.begin())
    {
        rootExponentialSearchCyclePerLength += (double)temp;
    }
    else
    {
        rootExponentialSearchCyclePerLength += (double)temp/ (lowerBoundIt - m_keys.begin());
    }
    #endif
}


template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::exponential_search_right(Type_Key & targetKey, int & foundPos, int maxSearchBound)
{
    #ifdef TUNE
    int predictPos = foundPos;
    #endif

    #ifdef TUNE_TIME
    int predictPos = foundPos;
    uint64_t temp = 0;
    startTimer(&temp);
    #endif
    
    int index = 1;
    while (index <= maxSearchBound && m_keys[foundPos + index] < targetKey)
    {
        index *= 2;
    }

    auto startIt = m_keys.begin() + (foundPos + static_cast<int>(index/2));
    auto endIt = m_keys.begin() + (foundPos + min(index,maxSearchBound));

    auto lowerBoundIt = lower_bound(startIt,endIt,targetKey);

    if (lowerBoundIt == m_keys.end() || ((lowerBoundIt != m_keys.begin()) && (*lowerBoundIt > targetKey)))
    {
        lowerBoundIt--;
    }

    foundPos = lowerBoundIt - m_keys.begin();

    #ifdef TUNE
    rootNoExponentialSearch++;
    rootLengthExponentialSearch += foundPos - predictPos;
    #endif

    #ifdef TUNE_TIME
    stopTimer(&temp);
    rootNoExponentialSearch++;
    rootLengthExponentialSearch += foundPos - predictPos;
    rootExponentialSearchCycle += temp;
    if (foundPos == predictPos)
    {
        rootExponentialSearchCyclePerLength += (double)temp;
    }
    else
    {
        rootExponentialSearchCyclePerLength += (double)temp/(foundPos - predictPos);
    }
    #endif
}

template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::exponential_search_right_insert(Type_Key & targetKey, int & foundPos, int maxSearchBound)
{
    #ifdef TUNE
    int predictPos = foundPos;
    #endif

    #ifdef TUNE_TIME
    int predictPos = foundPos;
    uint64_t temp = 0;
    startTimer(&temp);
    #endif

    int index = 1;
    while (index <= maxSearchBound && m_keys[foundPos + index] < targetKey)
    {
        index *= 2;
    }

    auto startIt = m_keys.begin() + (foundPos + static_cast<int>(index/2));
    auto endIt = m_keys.begin() + (foundPos + min(index,maxSearchBound));

    auto lowerBoundIt = lower_bound(startIt,endIt,targetKey);

    foundPos = lowerBoundIt - m_keys.begin();

    if (*lowerBoundIt < targetKey && bitmap_exists(foundPos))
    {
        foundPos++;
    }

    #ifdef TUNE
    rootNoExponentialSearch++;
    rootLengthExponentialSearch += foundPos - predictPos;
    #endif

    #ifdef TUNE_TIME
    stopTimer(&temp);
    rootNoExponentialSearch++;
    rootLengthExponentialSearch += foundPos - predictPos;
    rootExponentialSearchCycle += temp;
    if (foundPos == predictPos)
    {
        rootExponentialSearchCyclePerLength += (double)temp;
    }
    else
    {
        rootExponentialSearchCyclePerLength += (double)temp/(foundPos - predictPos);
    }
    #endif
}


template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::exponential_search_left(Type_Key & targetKey, int & foundPos, int maxSearchBound)
{
    #ifdef TUNE
    int predictPos = foundPos;
    #endif

    #ifdef TUNE_TIME
    int predictPos = foundPos;
    uint64_t temp = 0;
    startTimer(&temp);
    #endif

    int index = 1;
    while (index <= maxSearchBound && m_keys[foundPos - index] >= targetKey)
    {
        index *= 2;
    }

    auto startIt = m_keys.begin() + (foundPos - min(index,maxSearchBound));
    auto endIt = m_keys.begin() + (foundPos - static_cast<int>(index/2));

    auto lowerBoundIt = lower_bound(startIt,endIt,targetKey);
    
    if (lowerBoundIt == m_keys.end() || ((lowerBoundIt != m_keys.begin()) && (*lowerBoundIt > targetKey)))
    {
        lowerBoundIt--;
    }

    foundPos = lowerBoundIt - m_keys.begin();

    #ifdef TUNE
    rootNoExponentialSearch++;
    rootLengthExponentialSearch += predictPos - foundPos;
    #endif
    
    #ifdef TUNE_TIME
    stopTimer(&temp);
    rootNoExponentialSearch++;
    rootLengthExponentialSearch += predictPos - foundPos;
    rootExponentialSearchCycle += temp;
    if (foundPos == predictPos)
    {
        rootExponentialSearchCyclePerLength += (double)temp;
    }
    else
    {
        rootExponentialSearchCyclePerLength += (double)temp/(predictPos - foundPos);
    }
    #endif
}

template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::exponential_search_left_insert(Type_Key & targetKey, int & foundPos, int maxSearchBound)
{
    #ifdef TUNE
    int predictPos = foundPos;
    #endif

    #ifdef TUNE_TIME
    int predictPos = foundPos;
    uint64_t temp = 0;
    startTimer(&temp);
    #endif

    int index = 1;
    while (index <= maxSearchBound && m_keys[foundPos - index] >= targetKey)
    {
        index *= 2;
    }

    auto startIt = m_keys.begin() + (foundPos - min(index,maxSearchBound));
    auto endIt = m_keys.begin() + (foundPos - static_cast<int>(index/2));

    auto lowerBoundIt = lower_bound(startIt,endIt,targetKey);
    
    foundPos = lowerBoundIt - m_keys.begin();

    if (*lowerBoundIt < targetKey && bitmap_exists(foundPos))
    {
        foundPos++;
    }

    #ifdef TUNE
    rootNoExponentialSearch++;
    rootLengthExponentialSearch += predictPos - foundPos;
    #endif

    #ifdef TUNE_TIME
    stopTimer(&temp);
    rootNoExponentialSearch++;
    rootLengthExponentialSearch += predictPos - foundPos;
    rootExponentialSearchCycle += temp;
    if (foundPos == predictPos)
    {
        rootExponentialSearchCyclePerLength += (double)temp;
    }
    else
    {
        rootExponentialSearchCyclePerLength += (double)temp/(predictPos - foundPos);
    }
    #endif
}

template<class Type_Key, class Type_Ts>
size_t SWmeta<Type_Key,Type_Ts>::get_meta_size()
{
    return m_keys.size();
}

template<class Type_Key, class Type_Ts>
size_t SWmeta<Type_Key,Type_Ts>::get_no_seg()
{
    return m_numPairExist;
}

template<class Type_Key, class Type_Ts>
int SWmeta<Type_Key,Type_Ts>::get_split_error()
{
    return splitError;
}

template<class Type_Key, class Type_Ts>
void SWmeta<Type_Key,Type_Ts>::set_split_error(int error)
{
    splitError = error;
}

template<class Type_Key, class Type_Ts>
void  SWmeta<Type_Key,Type_Ts>::print()
{
    cout << "start key:" << m_startKey << ", slope:" << m_slope << endl;
    if (bitmap_exists(0))
    {
        cout << m_keys[0] << "(1)";
    }
    else
    {
        cout << m_keys[0] << "(0)";
    }

    for (int i = 1; i < m_keys.size(); i++)
    {
        cout << ",";
        if (bitmap_exists(i))
        {
            cout << m_keys[i] << "(1)";
        }
        else
        {
            cout << m_keys[i] << "(0)";
        }
    }
    cout << endl;
}

template<class Type_Key, class Type_Ts>
void  SWmeta<Type_Key,Type_Ts>::print_all()
{
    for (int i = 0; i < m_keys.size(); i++)
    {
        if (bitmap_exists(i))
        {
            cout << "*****************************" << endl;
            cout << m_keys[i] << "(1)" << endl;
            m_ptr[i]->print();
            cout << "*****************************" << endl;
        }
        else
        {
            cout << "*****************************" << endl;
            cout << m_keys[i] << "(0)" << endl;
            cout << "*****************************" << endl;
        }
    }
    cout << endl;
}

template <class Type_Key, class Type_Ts>
inline uint64_t SWmeta<Type_Key,Type_Ts>::get_total_size_in_bytes()
{
    uint64_t leafSize = 0;
    for (int i = 0; i < m_keys.size(); i++)
    {
        if (bitmap_exists(i))
        {
            leafSize += m_ptr[i]->get_total_size_in_bytes();
        }
    }

    return sizeof(int)*5 + sizeof(double) + sizeof(Type_Key) + sizeof(vector<uint64_t>)*2 + sizeof(uint64_t)*(m_bitmap.size()*2) +
    sizeof(vector<Type_Key>) + sizeof(Type_Key) * m_keys.size() + sizeof(vector<SWseg<Type_Key,Type_Ts>*>) + sizeof(SWseg<Type_Key,Type_Ts>*) * m_keys.size() + leafSize;
}

template <class Type_Key, class Type_Ts>
inline uint64_t SWmeta<Type_Key,Type_Ts>::get_no_keys(Type_Ts Timestamp)
{
    Type_Ts lowerLimit =  ((double)Timestamp - TIME_WINDOW < numeric_limits<Type_Ts>::min()) ? numeric_limits<Type_Ts>::min(): Timestamp - TIME_WINDOW;

    uint64_t cnt = 0;
    for (int i = 0; i < m_keys.size(); i++)
    {
        if (bitmap_exists(i))
        {
            cnt += m_ptr[i]->get_no_keys(lowerLimit);
        }
    }
    
    return cnt;
}

template<class Type_Key, class Type_Ts>
void SWmeta<Type_Key,Type_Ts>::print_stats()
{   
    #ifdef PRINT && TUNE
    printf("##### ROOT STATS ##### \n");
    printf("m_rightSearchBound = %i, m_leftSearchBound = %i \n",m_rightSearchBound,m_leftSearchBound);
    printf("avg rootLengthExponentialSearch = %f \n",(double)print_rootLengthExponentialSearch/(print_rootNoExponentialSearch+0.0001));
    printf("n = %i, m_numPairExist = %i , avg_occupancy = %f\n",m_keys.size(),m_numPairExist,(double)m_numPairExist/(m_keys.size()+0.0001));
    printf("m_maxSearchError = %i, splitError = %i\n",m_maxSearchError,splitError);

    printf("##### SEGMENT STATS ##### \n");
    printf("avg segLengthExponentialSearch = %f\n",(double)print_segLengthExponentialSearch/(print_segNoExponentialSearch+0.0001));
    printf("avg segLengthExponentialSearchAll = %f\n",(double)print_segLengthExponentialSearchAll/(print_segNoExponentialSearchAll+0.0001));
    printf("avg segScanNoSeg = %f\n",(double)print_segScanNoSeg/(print_segNoScan+0.0001));
    printf("segNoRetrain = %u\n",print_segNoRetrain);
    printf("avg merge_length = %f\n",(double)print_segLengthMerge/(print_segNoRetrain+0.0001));
    printf("avg retrain_length = %f\n",(double)print_segLengthRetrain/(print_segNoRetrain+0.0001));

    double avgOccupancy, avgBufferSize, avgRightErrorBound, avgLeftErrorBound = 0;
    for (int i = 0; i < m_keys.size(); i++)
    {
        if (bitmap_exists(i))
        {
            avgOccupancy += m_ptr[i]->m_numPairExist / (m_ptr[i]->m_numPair+0.0001);
            avgBufferSize += m_ptr[i]->m_buffer.size();
            avgRightErrorBound += m_ptr[i]->m_rightSearchBound;
            avgLeftErrorBound += m_ptr[i]->m_leftSearchBound;
        }
    }
    printf("avg occupancy = %f\n",avgOccupancy/(m_keys.size()+0.0001));
    printf("avg buffer size = %f\n",avgBufferSize/(m_keys.size()+0.0001));
    printf("avg right error bound = %f\n",avgRightErrorBound/(m_keys.size()+0.0001));
    printf("avg left error bound = %f\n",avgLeftErrorBound/(m_keys.size()+0.0001));
    printf("\n");
    #endif
    return;
}

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
inline int SWmeta<Type_Key,Type_Ts>::bitmap_closest_right_nongap(int index)
{
    int bitmapPos = index >> 6;
    int bitPos = index - (bitmapPos << 6);

    uint64_t currentBitmap = m_bitmap[bitmapPos];
    currentBitmap &= ~((1ULL << (bitPos)) - 1); //Erase all bits before the index (set them to 0)

    while(currentBitmap == 0) 
    //If current set of bitmap is empty (find next bitmap that is not empty)
    {
        bitmapPos++;
        if (bitmapPos >= m_bitmap.size())
        {
            return m_keys.size(); //Out of bounds
        }
        currentBitmap = m_bitmap[bitmapPos];
    }

    return bit_get_index(bitmapPos,bit_extract_rightmost_bit(currentBitmap));
}

template<class Type_Key, class Type_Ts>
inline int SWmeta<Type_Key,Type_Ts>::bitmap_closest_left_nongap(int index)
{
    int bitmapPos = index >> 6;
    int bitPos = index - (bitmapPos << 6);

    uint64_t currentBitmap = m_bitmap[bitmapPos];
    currentBitmap &= ((1ULL << (bitPos)) - 1); //Erase all bits after the index (set them to 0)

    while(currentBitmap == 0) 
    //If current set of bitmap is empty (find previous bitmap that is not empty)
    {
        bitmapPos--;
        if (bitmapPos < 0 )
        {
            return -1; //Out of bounds
        }
        currentBitmap = m_bitmap[bitmapPos];
    }

    // __builtin_clzll(currentBitmap) //Non x86instric.h equivalent for gcc.
    return (bitmapPos << 6) + (63-static_cast<int>(_lzcnt_u64(currentBitmap)));
}

template<class Type_Key, class Type_Ts>
inline int SWmeta<Type_Key,Type_Ts>::bitmap_closest_gap(int index)
{
    int bitmapPos = index >> 6;
    int bitPos = index - (bitmapPos << 6);

    //If bitmap block is full (check other blocks)
    if (m_bitmap[bitmapPos] == numeric_limits<uint64_t>::max() || //block full
       (bitmapPos == m_bitmap.size()-1 && _mm_popcnt_u64(m_bitmap[bitmapPos]) == (m_keys.size()-((m_bitmap.size()-1)<<6))) //last block full
       )
    {
        int maxLeftBlockDist = bitmapPos;
        int maxRightBlockDist =  ((m_keys.size()-1) >> 6) - bitmapPos;

        if (m_bitmap.size() == 1)
        {
            return static_cast<int>(_tzcnt_u64(~m_bitmap[bitmapPos]));
        }

        if (m_keys.size() == m_numPairExist)
        {
            return m_numPairExist;
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
            int rightBound = ((m_keys.size()-1) >> 6);
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

    // leftIndex = min(leftIndex, static_cast<int>(_tzcnt_u64(tempBitmap))) + (bitmapPos << 6);

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

    // rightIndex = max(rightIndex, 63 - (static_cast<int>(_lzcnt_u64(tempBitmap)))) + (bitmapPos << 6);
    
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
        m_bitmap.push_back(0); //When end exceeds last pos (end == m_keys.size())
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
        bitmap.push_back(0); //When end exceeds last pos (end == m_keys.size())
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