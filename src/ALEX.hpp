#ifndef __ALEX_HELPER_HPP__
#define __ALEX_HELPER_HPP__

#pragma once
#include "../lib/alex.h"
#include "../parameters.hpp"

using namespace std;

namespace alex {

template<class Type_Key, class Type_Ts>
inline void alex_erase( alex::Alex<Type_Key,Type_Ts> & alex, 
                        pair<Type_Key, Type_Ts> & arrivalTuple)
{
    vector<Type_Key> deleteKeys;
    deleteKeys.reserve(1000); //Predefined size to prevent vector extending
    Type_Ts lowerLimit =  ((double)arrivalTuple.second - TIME_WINDOW < numeric_limits<Type_Ts>::min()) 
                            ? numeric_limits<Type_Ts>::min(): arrivalTuple.second - TIME_WINDOW;

    auto itStart = alex.begin();
    auto itEnd = alex.end();
    while (itStart != itEnd)
    {
        if (itStart.payload() < lowerLimit)
        {
            deleteKeys.push_back(itStart.key());
        }
        itStart++;
    }

    for (auto &key:deleteKeys)
    {
        alex.erase(key);
    }
}

template<class Type_Key, class Type_Ts>
inline void alex_range_search(  alex::Alex<Type_Key,Type_Ts> & alex,
                                pair<Type_Key, Type_Ts> & arrivalTuple, Type_Key & searchRange, vector<pair<Type_Key, Type_Ts>> & searchResult)
{
    Type_Key targetKey = arrivalTuple.first;
    Type_Ts newTimeStamp = arrivalTuple.second;
    
    Type_Key lowerBound = (double)targetKey - (double)searchRange < numeric_limits<Type_Key>::min()?
                           numeric_limits<Type_Key>::min() : targetKey - searchRange; 

    Type_Key upperBound = (double)targetKey + (double)searchRange > numeric_limits<Type_Key>::max()?
                           numeric_limits<Type_Key>::max() : targetKey + searchRange; 

    auto itStart = alex.lower_bound(lowerBound);
    auto itEnd = alex.lower_bound(upperBound);

    while (itStart != itEnd)
    {
        if (itStart.key() >= lowerBound && itStart.key() <= upperBound)
        {
            searchResult.push_back(make_pair(itStart.key(), itStart.payload()));
        }
        itStart++;
    }
}

template<class Type_Key, class Type_Ts>
inline void alex_range_search(  alex::Alex<Type_Key,Type_Ts> & alex,
                                pair<Type_Key, Type_Ts> & arrivalTuple, vector<pair<Type_Key, Type_Ts>> & searchResult)
{
    Type_Key targetKey = arrivalTuple.first;
    Type_Ts newTimeStamp = arrivalTuple.second;
    
    auto it = alex.lower_bound(targetKey);

    while (searchResult.size() < MATCH_RATE && it != alex.end())
    {
        searchResult.push_back(make_pair(it.key(), it.payload()));
        it++;
    }
}

template<class Type_Key, class Type_Ts>
inline void alex_range_search(  alex::Alex<Type_Key,Type_Ts> & alex,
                                tuple<Type_Key, Type_Ts, Type_Key> & arrivalTuple, vector<pair<Type_Key, Type_Ts>> & searchResult)
{
    Type_Key targetKey = get<0>(arrivalTuple);
    Type_Ts newTimeStamp = get<1>(arrivalTuple);
    Type_Key upperBound = get<2>(arrivalTuple);

    auto itStart = alex.lower_bound(targetKey);
    auto itEnd = alex.lower_bound(upperBound);

    while (itStart != itEnd)
    {
        if (itStart.key() >= targetKey && itStart.key() <= upperBound)
        {
            searchResult.push_back(make_pair(itStart.key(), itStart.payload()));
        }
        itStart++;
    }
}

template<class Type_Key, class Type_Ts>
inline void alex_point_lookup(  alex::Alex<Type_Key,Type_Ts> & alex,
                                pair<Type_Key, Type_Ts> & arrivalTuple, Type_Key & resultCount)
{
    Type_Key targetKey = arrivalTuple.first;
    Type_Ts newTimeStamp = arrivalTuple.second;

    auto it = alex.find(targetKey);

    if (it != alex.end())
    {
        resultCount++;
    }
}

template<class Type_Key, class Type_Ts>
inline uint64_t alex_get_total_size_in_bytes(  alex::Alex<Type_Key,Type_Ts> & alex)
{
   return static_cast<uint64_t>(alex.model_size()) + static_cast<uint64_t>(alex.data_size());
}

template<class Type_Key, class Type_Ts>
inline void alex_print_stats(alex::Alex<Type_Key,Type_Ts> & alex)
{
    cout << "num_keys: " << alex.stats_.num_keys << endl;
    cout << "num_model_nodes: " << alex.stats_.num_model_nodes << endl;
    cout << "num_data_nodes: " << alex.stats_.num_data_nodes << endl;
    cout << "num_expand_and_scales: " << alex.stats_.num_expand_and_scales << endl;
    cout << "num_expand_and_retrains: " << alex.stats_.num_expand_and_retrains << endl;
    cout << "num_downward_splits: " << alex.stats_.num_downward_splits << endl;
    cout << "num_sideways_splits: " << alex.stats_.num_sideways_splits << endl;
    cout << "num_model_node_expansions: " << alex.stats_.num_model_node_expansions << endl;
    cout << "num_model_node_splits: " << alex.stats_.num_model_node_splits << endl;
    cout << "num_downward_split_keys: " << alex.stats_.num_downward_split_keys << endl;
    cout << "num_sideways_split_keys: " << alex.stats_.num_sideways_split_keys << endl;
    cout << "num_model_node_expansion_pointers: " << alex.stats_.num_model_node_expansion_pointers << endl;
    cout << "num_model_node_split_pointers: " << alex.stats_.num_model_node_split_pointers << endl;
    cout << "num_node_lookups: " << alex.stats_.num_node_lookups << endl;
    cout << "num_lookups: " << alex.stats_.num_lookups << endl;
    cout << "num_inserts: " << alex.stats_.num_inserts << endl;
    cout << "splitting_time: " << alex.stats_.splitting_time << endl;
    cout << "cost_computation_time: " << alex.stats_.cost_computation_time << endl;
    cout << "model_size: " << alex.model_size()  << ", data_size: " << alex.data_size() << endl;
    cout << endl;
}

}
#endif