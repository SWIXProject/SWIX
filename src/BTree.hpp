#ifndef __BTREE_HELPER_HPP__
#define __BTREE_HELPER_HPP__

#pragma once
#include "../lib/btree_map.h"
#include "config.hpp"

using namespace std;

namespace btree {
    
/*
STX B+Tree Parameters
*/
template<typename Type>
class btree_traits_fanout{
public:
    static const bool selfverify = false;
    static const bool debug = false;
    static const int leafslots = FANOUT_BP;
    static const int innerslots = FANOUT_BP;
    static const size_t binsearch_threshold = 256;
};

template<class Type_Key, class Type_Ts>
inline void bt_erase( stx::btree_map<Type_Key,Type_Ts,less<Type_Key>,btree_traits_fanout<Type_Key>> & btree,
                      pair<Type_Key, Type_Ts> & arrivalTuple)
{
    vector<Type_Key> deleteKeys;
    deleteKeys.reserve(1000); //Predefined size to prevent vector extending
    Type_Ts lowerLimit =  ((double)arrivalTuple.second - TIME_WINDOW < numeric_limits<Type_Ts>::min()) 
                            ? numeric_limits<Type_Ts>::min(): arrivalTuple.second - TIME_WINDOW;

    auto it = btree.begin();
    while (it != btree.end())
    {
        if (it->second < lowerLimit)
        {
            deleteKeys.push_back(it->first);
        }
        it++;
    }

    for (auto &key:deleteKeys)
    {
        btree.erase(key);
    }
}

template<class Type_Key, class Type_Ts>
inline void bt_range_search(  stx::btree_map<Type_Key,Type_Ts,less<Type_Key>,btree_traits_fanout<Type_Key>> & btree,
                                pair<Type_Key, Type_Ts> & arrivalTuple, Type_Key & searchRange, vector<pair<Type_Key, Type_Ts>> & searchResult)
{
    Type_Key targetKey = arrivalTuple.first;
    Type_Ts newTimeStamp = arrivalTuple.second;
    
    Type_Key lowerBound = (double)targetKey - (double)searchRange < numeric_limits<Type_Key>::min()?
                           numeric_limits<Type_Key>::min() : targetKey - searchRange; 

    Type_Key upperBound = (double)targetKey + (double)searchRange > numeric_limits<Type_Key>::max()?
                           numeric_limits<Type_Key>::max() : targetKey + searchRange; 

    auto it = btree.lower_bound(lowerBound);

    while (it != btree.end() && it->first <= upperBound)
    { 
        if (it->first >= lowerBound)
        {
            searchResult.push_back(*it);
        }
        it++;
    }
}

template<class Type_Key, class Type_Ts>
inline void bt_range_search(  stx::btree_map<Type_Key,Type_Ts,less<Type_Key>,btree_traits_fanout<Type_Key>> & btree,
                                pair<Type_Key, Type_Ts> & arrivalTuple, vector<pair<Type_Key, Type_Ts>> & searchResult)
{
    Type_Key targetKey = arrivalTuple.first;
    Type_Ts newTimeStamp = arrivalTuple.second;
    
    auto it = btree.lower_bound(targetKey);

    while (it != btree.end() && searchResult.size() < MATCH_RATE)
    { 
        searchResult.push_back(*it);
        it++;
    }
}

template<class Type_Key, class Type_Ts>
inline void bt_range_search(  stx::btree_map<Type_Key,Type_Ts,less<Type_Key>,btree_traits_fanout<Type_Key>> & btree,
                                tuple<Type_Key, Type_Ts, Type_Key> & arrivalTuple, vector<pair<Type_Key, Type_Ts>> & searchResult)
{
    Type_Key targetKey = get<0>(arrivalTuple);
    Type_Ts newTimeStamp = get<1>(arrivalTuple);
    Type_Key upperBound = get<2>(arrivalTuple);

    auto it = btree.lower_bound(targetKey);

    while (it != btree.end() && it->first <= upperBound)
    { 
        if (it->first >= targetKey)
        {
            searchResult.push_back(*it);
        }
        it++;
    }
}

template<class Type_Key, class Type_Ts>
inline void bt_point_lookup(  stx::btree_map<Type_Key,Type_Ts,less<Type_Key>,btree_traits_fanout<Type_Key>> & btree,
                                pair<Type_Key, Type_Ts> & arrivalTuple, Type_Key & resultCount)
{
    Type_Key targetKey = arrivalTuple.first;
    Type_Ts newTimeStamp = arrivalTuple.second;

    auto it = btree.find(targetKey);

    if (it != btree.end())
    {
        resultCount++;
    }
}

template<class Type_Key, class Type_Ts>
inline uint64_t bt_get_total_size_in_bytes(  stx::btree_map<Type_Key,Type_Ts,less<Type_Key>,btree_traits_fanout<Type_Key>> & btree)
{
    struct node
    {
        unsigned short level;
        unsigned short slotuse;
    };
    
    auto btreeStats = btree.get_stats();
    
    return sizeof(btree) + sizeof(node) * (btreeStats.leaves + btreeStats.innernodes) + 
    sizeof(Type_Key) * btreeStats.innerslots * btreeStats.innernodes + sizeof(node*) * (btreeStats.innerslots+1) * btreeStats.innernodes +
    sizeof(Type_Key) * btreeStats.leafslots * btreeStats.leaves + sizeof(Type_Ts) * btreeStats.leafslots * btreeStats.leaves + sizeof(node*)*2;
}


template<class Type_Key, class Type_Ts>
inline void bt_print_stats(  stx::btree_map<Type_Key,Type_Ts,less<Type_Key>,btree_traits_fanout<Type_Key>> & btree)
{
    struct node
    {
        unsigned short level;
        unsigned short slotuse;
    };
    
    auto btreeStats = btree.get_stats();
    
    cout << "no_items: " << btreeStats.itemcount << endl;
    cout << "no_innernodes: " << btreeStats.innernodes << endl;
    cout << "no_leaves: " << btreeStats.leaves << endl;
    cout << "no_innerslots: " << btreeStats.innerslots << endl;
    cout << "no_leafslots: " << btreeStats.leafslots << endl;
    cout << "no_nodes: " << btreeStats.nodes() << endl;
    cout << "avgfill_leaves: " << btreeStats.avgfill_leaves() << endl;
    cout << "size: " << sizeof(btree) + sizeof(node) * (btreeStats.leaves + btreeStats.innernodes) + 
    sizeof(Type_Key) * btreeStats.innerslots * btreeStats.innernodes + sizeof(node*) * (btreeStats.innerslots+1) * btreeStats.innernodes +
    sizeof(Type_Key) * btreeStats.leafslots * btreeStats.leaves + sizeof(Type_Ts) * btreeStats.leafslots * btreeStats.leaves + sizeof(node*)*2 << endl;
    cout << endl;
}

}
#endif