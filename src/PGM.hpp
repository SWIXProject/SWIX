#ifndef __PGM_HELPER_HPP__
#define __PGM_HELPER_HPP__

#pragma once
#include "../lib/pgm_index_dynamic.hpp"
#include "config.hpp"

using namespace std;

namespace pgm {

template<class Type_Key, class Type_Ts>
inline void pgm_erase(  pgm::DynamicPGMIndex<Type_Key,Type_Ts,pgm::PGMIndex<Type_Key,FANOUT_BP>> & pgm,
                        pair<Type_Key, Type_Ts> & arrivalTuple)
{
    vector<Type_Key> deleteKeys;
    deleteKeys.reserve(1000); //Predefined size to prevent vector extending
    Type_Ts lowerLimit =  ((double)arrivalTuple.second - TIME_WINDOW < numeric_limits<Type_Ts>::min()) 
                            ? numeric_limits<Type_Ts>::min(): arrivalTuple.second - TIME_WINDOW;
                            
    auto it = pgm.begin();
    while (it != pgm.end())
    {
        if (it->second < lowerLimit)
        {
            deleteKeys.push_back(it->first);
        }
        ++it;
    }

    for (auto &key:deleteKeys)
    {
        pgm.erase(key);
    }
}


template<class Type_Key, class Type_Ts>
inline void pgm_range_search(  pgm::DynamicPGMIndex<Type_Key,Type_Ts,pgm::PGMIndex<Type_Key,FANOUT_BP>> & pgm,
                                pair<Type_Key, Type_Ts> & arrivalTuple, Type_Key & searchRange, vector<pair<Type_Key, Type_Ts>> & searchResult)
{
    Type_Key targetKey = arrivalTuple.first;
    Type_Ts newTimeStamp = arrivalTuple.second;
    
    Type_Key lowerBound = (double)targetKey - (double)searchRange < numeric_limits<Type_Key>::min()?
                           numeric_limits<Type_Key>::min() : targetKey - searchRange; 

    Type_Key upperBound = (double)targetKey + (double)searchRange > numeric_limits<Type_Key>::max()?
                           numeric_limits<Type_Key>::max() : targetKey + searchRange; 

    searchResult = pgm.range(lowerBound,upperBound);
}

template<class Type_Key, class Type_Ts>
inline void pgm_range_search(  pgm::DynamicPGMIndex<Type_Key,Type_Ts,pgm::PGMIndex<Type_Key,FANOUT_BP>> & pgm,
                                pair<Type_Key, Type_Ts> & arrivalTuple, vector<pair<Type_Key, Type_Ts>> & searchResult)
{
    Type_Key targetKey = arrivalTuple.first;
    Type_Ts newTimeStamp = arrivalTuple.second;
    
    auto it = pgm.lower_bound(targetKey);

    while (it != pgm.end() && searchResult.size() < MATCH_RATE)
    {
        searchResult.push_back(make_pair(it->first,it->second));
        ++it;
    }
}

template<class Type_Key, class Type_Ts>
inline void pgm_range_search(  pgm::DynamicPGMIndex<Type_Key,Type_Ts,pgm::PGMIndex<Type_Key,FANOUT_BP>> & pgm,
                                tuple<Type_Key, Type_Ts, Type_Key> & arrivalTuple, vector<pair<Type_Key, Type_Ts>> & searchResult)
{
    Type_Key targetKey = get<0>(arrivalTuple);
    Type_Ts newTimeStamp = get<1>(arrivalTuple);
    Type_Key upperBound = get<2>(arrivalTuple);

    searchResult = pgm.range(targetKey,upperBound);
}

template<class Type_Key, class Type_Ts>
inline void pgm_point_lookup(  pgm::DynamicPGMIndex<Type_Key,Type_Ts,pgm::PGMIndex<Type_Key,FANOUT_BP>> & pgm,
                                pair<Type_Key, Type_Ts> & arrivalTuple, Type_Key & resultCount)
{
    Type_Key targetKey = arrivalTuple.first;
    Type_Ts newTimeStamp = arrivalTuple.second;

    auto it = pgm.find(targetKey);

    if (it != pgm.end())
    {
        resultCount++;
    }
}

template<class Type_Key, class Type_Ts>
inline int pgm_get_total_size_in_bytes(  pgm::DynamicPGMIndex<Type_Key,Type_Ts,pgm::PGMIndex<Type_Key,FANOUT_BP>> & pgm)
{
   return pgm.size_in_bytes();
}

template<class Type_Key, class Type_Ts>
inline void pgm_print_stats(  pgm::DynamicPGMIndex<Type_Key,Type_Ts,pgm::PGMIndex<Type_Key,FANOUT_BP>> & pgm)
{
    vector<tuple<size_t,size_t,size_t>> stats = pgm.find_stats(); //We added .find_stats() function in "pgm_index_dynamic.hpp"
    for (int i = 0; i < stats.size(); ++i)
    {
        cout << "##### level " << i << " #####\n";
        cout << "segment_count: " << get<0>(stats[i]) << "\n";
        cout << "height: " << get<1>(stats[i]) << "\n";
        cout << "size_in_bytes: " << get<2>(stats[i]) << "\n";
    }
    cout << "total_size_in_bytes: " << pgm.size_in_bytes() << "\n";
    cout << endl;
}

}
#endif