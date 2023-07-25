#ifndef __VECTOR_HELPER_HPP__
#define __VECTOR_HELPER_HPP__

#pragma once
#include <algorithm>
#include "../parameters.hpp"

using namespace std;

/*
Common
*/

template<class Type_Key, class Type_Ts>
inline void vector_point_lookup(  vector<pair<Type_Key,Type_Ts>> & data_no_index,
                                pair<Type_Key, Type_Ts> & arrivalTuple, Type_Key & resultCount)
{
    Type_Key targetKey = arrivalTuple.first;
    Type_Ts newTimeStamp = arrivalTuple.second;

    auto it = find_if(data_no_index.begin(),data_no_index.end(),
            [&targetKey](const pair<Type_Key,Type_Ts>& data)
            {
                return data.first == targetKey;
            });

    if (it != data_no_index.end())
    {
        resultCount++;
    }
}


template<class Type_Key, class Type_Ts>
inline void vector_erase(vector<pair<Type_Key,Type_Ts>> & data_no_index,
                        pair<Type_Key, Type_Ts> & arrivalTuple)
{
    vector<pair<Type_Key,Type_Ts>> deleteKeys;
    deleteKeys.reserve(1000); //Predefined size to prevent vector extending
    Type_Ts lowerLimit =  ((double)arrivalTuple.second - TIME_WINDOW < numeric_limits<Type_Ts>::min()) 
                            ? numeric_limits<Type_Ts>::min(): arrivalTuple.second - TIME_WINDOW;

    data_no_index.erase(remove_if(data_no_index.begin(), data_no_index.end(),
                                [&lowerLimit](pair<Type_Key,Type_Ts> it)
                                {
                                    return it.second < lowerLimit;
                                }),
                        data_no_index.end());
}

/*
Not Sorted Vector
*/
template<class Type_Key, class Type_Ts>
inline void vector_not_sorted_range_search(  vector<pair<Type_Key,Type_Ts>> & data_no_index,
                                pair<Type_Key, Type_Ts> & arrivalTuple, Type_Key & searchRange, vector<pair<Type_Key, Type_Ts>> & searchResult)
{
    Type_Key targetKey = arrivalTuple.first;
    Type_Ts newTimeStamp = arrivalTuple.second;
    
    Type_Key lowerBound = (double)targetKey - (double)searchRange < numeric_limits<Type_Key>::min()?
                           numeric_limits<Type_Key>::min() : targetKey - searchRange; 

    Type_Key upperBound = (double)targetKey + (double)searchRange > numeric_limits<Type_Key>::max()?
                           numeric_limits<Type_Key>::max() : targetKey + searchRange; 


    for (auto &it: data_no_index)
    {
        if (it.first >= lowerBound && it.first <= upperBound)
        {
            searchResult.push_back(it);
        }
    }
}


template<class Type_Key, class Type_Ts>
inline void vector_not_sorted_range_search(  vector<pair<Type_Key,Type_Ts>> & data_no_index,
                                tuple<Type_Key, Type_Ts, Type_Key> & arrivalTuple, vector<pair<Type_Key, Type_Ts>> & searchResult)
{
    Type_Key targetKey = get<0>(arrivalTuple);
    Type_Ts newTimeStamp = get<1>(arrivalTuple);
    Type_Key upperBound = get<2>(arrivalTuple);

    for (auto &it: data_no_index)
    {
        if (it.first >= targetKey && it.first <= upperBound)
        {
            searchResult.push_back(it);
        }
    }
}


/*
Sorted Vector
*/

template<class Type_Key, class Type_Ts>
inline void vector_sorted_range_search(  vector<pair<Type_Key,Type_Ts>> & data_no_index,
                                pair<Type_Key, Type_Ts> & arrivalTuple, Type_Key & searchRange, vector<pair<Type_Key, Type_Ts>> & searchResult)
{
    Type_Key targetKey = arrivalTuple.first;
    Type_Ts newTimeStamp = arrivalTuple.second;
    
    Type_Key lowerBound = (double)targetKey - (double)searchRange < numeric_limits<Type_Key>::min()?
                           numeric_limits<Type_Key>::min() : targetKey - searchRange; 

    Type_Key upperBound = (double)targetKey + (double)searchRange > numeric_limits<Type_Key>::max()?
                           numeric_limits<Type_Key>::max() : targetKey + searchRange; 

    auto it = lower_bound(data_no_index.begin(),data_no_index.end(),lowerBound,
                        [](const pair<Type_Key,Type_Ts>& data, Type_Key value)
                        {
                            return data.first < value;
                        });

    while (it != data_no_index.end() && it->first <= upperBound)
    { 
        if (it->first >= lowerBound)
        {
            searchResult.push_back(*it);
        }
        it++;
    }
}

template<class Type_Key, class Type_Ts>
inline void vector_sorted_range_search(  vector<pair<Type_Key,Type_Ts>> & data_no_index,
                                pair<Type_Key, Type_Ts> & arrivalTuple, vector<pair<Type_Key, Type_Ts>> & searchResult)
{
    Type_Key targetKey = arrivalTuple.first;
    Type_Ts newTimeStamp = arrivalTuple.second;
    
    auto it = lower_bound(data_no_index.begin(),data_no_index.end(),targetKey,
                        [](const pair<Type_Key,Type_Ts>& data, Type_Key value)
                        {
                            return data.first < value;
                        });

    while (searchResult.size() < MATCH_RATE && it != data_no_index.end())
    {
        searchResult.push_back(*it);
        it++;
    }
}

template<class Type_Key, class Type_Ts>
inline void vector_sorted_range_search(  vector<pair<Type_Key,Type_Ts>> & data_no_index,
                                tuple<Type_Key, Type_Ts, Type_Key> & arrivalTuple, vector<pair<Type_Key, Type_Ts>> & searchResult)
{
    Type_Key targetKey = get<0>(arrivalTuple);
    Type_Ts newTimeStamp = get<1>(arrivalTuple);
    Type_Key upperBound = get<2>(arrivalTuple);

    auto it = lower_bound(data_no_index.begin(),data_no_index.end(),targetKey,
                        [](const pair<Type_Key,Type_Ts>& data, Type_Key value)
                        {
                            return data.first < value;
                        });

    while (it != data_no_index.end() && it->first <= upperBound)
    { 
        if (it->first >= targetKey)
        {
            searchResult.push_back(*it);
        }
        it++;
    }
}


template<class Type_Key, class Type_Ts>
inline void vector_sorted_erase(vector<pair<Type_Key,Type_Ts>> & data_no_index, Type_Key key)
{
    auto it = find_if(data_no_index.begin(),data_no_index.end(),
            [&key](const pair<Type_Key,Type_Ts>& data)
            {
                return data.first == key;
            });

    if (it != data_no_index.end())
    {
        data_no_index.erase(it);
    }
    else
    {
        cout << "Key does not exist within data, did not erase" << endl;
    }
}

template<class Type_Key, class Type_Ts>
inline void vector_sorted_insert(vector<pair<Type_Key,Type_Ts>> & data_no_index, pair<Type_Key,Type_Ts> & arrivalTuple)
{
    if (data_no_index.back().first <= arrivalTuple.first) //Less than first key, insert to position 0
    {
        data_no_index.push_back(arrivalTuple);
    }
    else if (arrivalTuple.first <= data_no_index.front().first) //Larger than last key, push_back
    {
        data_no_index.insert(data_no_index.begin(),arrivalTuple);
    }
    else // Between first and last key, determine position and insert
    {
        auto it = lower_bound(data_no_index.begin(),data_no_index.end(),arrivalTuple.first,
                        [](const pair<Type_Key,Type_Ts>& data, Type_Key value)
                        {
                            return data.first < value;
                        });
        data_no_index.insert(it,arrivalTuple);
    }
}

#endif