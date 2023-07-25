#ifndef __IMTREE_HPP__
#define __IMTREE_HPP__

#include<iostream>
#include<vector>
#include<iterator>
#include<algorithm>
#include<chrono>
#include<stdexcept>

#include "../lib/btree_map.h"
#include "config.hpp"

using namespace std;

namespace imtree {
/*
IM Tree Data Structure for time based streaming joins.
Inerstion tree (Ti) consist of an STX B+ Tree.
Search tree (Ts) consist of an custom CSS Tree which we do not have. Therefore, we use a STX B+tree as well.
*/

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
class IMTree
{
    int m_mergeThreshold;
    
    stx::btree_map<Type_Key,Type_Ts,less<Type_Key>,btree_traits_fanout<Type_Key>> m_searchTree;
    stx::btree_map<Type_Key,Type_Ts,less<Type_Key>,btree_traits_fanout<Type_Key>> m_insertionTree;
public:
    IMTree(int mergeThreshold)
    {
        m_mergeThreshold = mergeThreshold;
    }

    IMTree(vector<pair<Type_Key,Type_Ts>> & stream, int mergeThreshold)
    :m_mergeThreshold(mergeThreshold)
    {
        bulk_load(stream);
    }

    void bulk_load(vector<pair<Type_Key,Type_Ts>> & stream);

    void lookup(pair<Type_Key, Type_Ts> & arrivalTuple, Type_Key & resultCount);

    void range_search(pair<Type_Key, Type_Ts> & arrivalTuple, Type_Key & searchRange, vector<pair<Type_Key, Type_Ts>> & searchResult);
    void range_search(pair<Type_Key, Type_Ts> & arrivalTuple, vector<pair<Type_Key, Type_Ts>> & searchResult);
    void range_search(tuple<Type_Key, Type_Ts, Type_Key> & arrivalTuple, vector<pair<Type_Key, Type_Ts>> & searchResult);

    void insert(pair<Type_Key,Type_Ts> & arrivalTuple);

    void merge(Type_Ts & newTimeStamp);

    uint64_t get_total_size_in_bytes();
    uint64_t get_no_keys(Type_Ts Timestamp);
    void print_stats();
};


/*
Bulk Load
*/
template<class Type_Key, class Type_Ts>
void IMTree<Type_Key,Type_Ts>::bulk_load(vector<pair<Type_Key,Type_Ts>> & stream)
{
    sort(stream.begin(),stream.end());
    m_searchTree.bulk_load(stream.begin(),stream.end());
}

/*
Point Lookup
*/
template<class Type_Key, class Type_Ts>
void IMTree<Type_Key,Type_Ts>::lookup(pair<Type_Key, Type_Ts> & arrivalTuple, Type_Key & resultCount)
{
    Type_Key targetKey = arrivalTuple.first;
    Type_Ts newTimeStamp = arrivalTuple.second;
    Type_Ts lowerLimit =  ((double)newTimeStamp - TIME_WINDOW < numeric_limits<Type_Ts>::min()) ? numeric_limits<Type_Ts>::min(): newTimeStamp - TIME_WINDOW;
    
    auto it  = m_searchTree.find(targetKey);
    if (it != m_searchTree.end() && it->second >= lowerLimit)
    {
        resultCount++;
        return;
    }

    it = m_insertionTree.find(targetKey);
    if (it != m_insertionTree.end() && it->second >= lowerLimit)
    {
        resultCount++;
    }
}

/*
Range Search
*/
template<class Type_Key, class Type_Ts>
void IMTree<Type_Key,Type_Ts>::range_search(pair<Type_Key, Type_Ts> & arrivalTuple, Type_Key & searchRange, vector<pair<Type_Key, Type_Ts>> & searchResult)
{
    Type_Key targetKey = arrivalTuple.first;
    Type_Ts newTimeStamp = arrivalTuple.second;
    Type_Ts lowerLimit =  ((double)newTimeStamp - TIME_WINDOW < numeric_limits<Type_Ts>::min()) ? numeric_limits<Type_Ts>::min(): newTimeStamp - TIME_WINDOW;
    
    Type_Key lowerBound = (double)targetKey - (double)searchRange < numeric_limits<Type_Key>::min()?
                           numeric_limits<Type_Key>::min() : targetKey - searchRange; 

    Type_Key upperBound = (double)targetKey + (double)searchRange > numeric_limits<Type_Key>::max()?
                           numeric_limits<Type_Key>::max() : targetKey + searchRange; 

    // Search Search Tree
    auto it = m_searchTree.lower_bound(lowerBound);
    while (it != m_searchTree.end() && it->first <= upperBound)
    { 
        if (it->second >= lowerLimit)
        {
            searchResult.push_back(*it);
        }
        it++;
    }

    //Search Insertion Tree
    it = m_insertionTree.lower_bound(lowerBound);
    while (it != m_insertionTree.end() && it->first <= upperBound)
    { 
        if (it->second >= lowerLimit)
        {
            searchResult.push_back(*it);
        }
        it++;
    }
}

template<class Type_Key, class Type_Ts>
void IMTree<Type_Key,Type_Ts>::range_search(pair<Type_Key, Type_Ts> & arrivalTuple, vector<pair<Type_Key, Type_Ts>> & searchResult)
{
    Type_Key targetKey = arrivalTuple.first;
    Type_Ts newTimeStamp = arrivalTuple.second;
    Type_Ts lowerLimit =  ((double)newTimeStamp - TIME_WINDOW < numeric_limits<Type_Ts>::min()) ? numeric_limits<Type_Ts>::min(): newTimeStamp - TIME_WINDOW;
    
    auto itSearchTree = m_searchTree.lower_bound(targetKey);
    auto itInsertionTree = m_insertionTree.lower_bound(targetKey);

    while (itSearchTree != m_searchTree.end() && itInsertionTree != m_insertionTree.end())
    {
        if (itSearchTree->first < itInsertionTree->first)
        {
            if (itSearchTree->second >= lowerLimit)
            {
                searchResult.push_back(*itSearchTree);
            }
            itSearchTree++;
        }
        else
        {
            if (itInsertionTree->second >= lowerLimit)
            {
                searchResult.push_back(*itInsertionTree);
            }
            itInsertionTree++;
        }

        if(searchResult.size() == MATCH_RATE)
        {
            break;
        }
    }

    if(searchResult.size() < MATCH_RATE)
    {
        if (itSearchTree == m_searchTree.end())
        {
            while (itInsertionTree != m_insertionTree.end() && searchResult.size() < MATCH_RATE)
            {
                if (itInsertionTree->second >= lowerLimit)
                {
                    searchResult.push_back(*itInsertionTree);
                }
                itInsertionTree++;   
            }
        }
        else if (itInsertionTree == m_insertionTree.end())
        {
            while (itSearchTree != m_searchTree.end() && searchResult.size() < MATCH_RATE)
            {
                if (itSearchTree->second >= lowerLimit)
                {
                    searchResult.push_back(*itSearchTree);
                }
                itSearchTree++; 
            }
        }
    }
}

template<class Type_Key, class Type_Ts>
void IMTree<Type_Key,Type_Ts>::range_search(tuple<Type_Key, Type_Ts, Type_Key> & arrivalTuple, vector<pair<Type_Key, Type_Ts>> & searchResult)
{
    Type_Key targetKey = get<0>(arrivalTuple);
    Type_Ts newTimeStamp = get<1>(arrivalTuple);
    Type_Key upperBound = get<2>(arrivalTuple);
    Type_Ts lowerLimit =  ((double)newTimeStamp - TIME_WINDOW < numeric_limits<Type_Ts>::min()) ? numeric_limits<Type_Ts>::min(): newTimeStamp - TIME_WINDOW;

    // Search Search Tree
    auto it = m_searchTree.lower_bound(targetKey);
    while (it != m_searchTree.end() && it->first <= upperBound)
    { 
        if (it->second >= lowerLimit)
        {
            searchResult.push_back(*it);
        }
        it++;
    }

    //Search Insertion Tree
    it = m_insertionTree.lower_bound(targetKey);
    while (it != m_insertionTree.end() && it->first <= upperBound)
    { 
        if (it->second >= lowerLimit)
        {
            searchResult.push_back(*it);
        }
        it++;
    }
}

/*
Insertion
*/
template<class Type_Key, class Type_Ts>
void IMTree<Type_Key,Type_Ts>::insert(pair<Type_Key,Type_Ts> & arrivalTuple)
{
    if (m_insertionTree.size() < m_mergeThreshold)
    {
        m_insertionTree.insert(arrivalTuple);
    }
    else
    {
        m_insertionTree.insert(arrivalTuple);
        merge(arrivalTuple.second);
    }
}

/*
Merge Trees
*/
template<class Type_Key, class Type_Ts>
void IMTree<Type_Key,Type_Ts>::merge(Type_Ts & newTimeStamp)
{
    Type_Ts lowerLimit =  ((double)newTimeStamp - TIME_WINDOW < numeric_limits<Type_Ts>::min()) ? numeric_limits<Type_Ts>::min(): newTimeStamp - TIME_WINDOW;

    vector<pair<Type_Key,Type_Ts>> newSearchTree;
    newSearchTree.reserve(m_insertionTree.size()+m_searchTree.size());

    auto itSearchTree = m_searchTree.begin();
    auto itInsertionTree = m_insertionTree.begin();

    while (itSearchTree != m_searchTree.end() && itInsertionTree != m_insertionTree.end())
    {
        if (itSearchTree->first < itInsertionTree->first)
        {
            if (itSearchTree->second >= lowerLimit)
            {
                newSearchTree.push_back(*itSearchTree);
            }
            itSearchTree++;
        }
        else
        {
            if (itInsertionTree->second >= lowerLimit)
            {
                newSearchTree.push_back(*itInsertionTree);
            }
            itInsertionTree++;
        }
    }

    while (itSearchTree != m_searchTree.end())
    {
        if (itSearchTree->second >= lowerLimit)
        {
            newSearchTree.push_back(*itSearchTree);
        }
        itSearchTree++; 
    }

    while (itInsertionTree != m_insertionTree.end())
    {
        if (itInsertionTree->second >= lowerLimit)
        {
            newSearchTree.push_back(*itInsertionTree);
        }
        itInsertionTree++;   
    }

    m_searchTree.clear();
    m_insertionTree.clear();

    m_searchTree.bulk_load(newSearchTree.begin(),newSearchTree.end());
}

template <class Type_Key, class Type_Ts>
inline uint64_t IMTree<Type_Key,Type_Ts>::get_total_size_in_bytes()
{
    struct node
    {
        unsigned short level;
        unsigned short slotuse;
    };
    
    auto searchTreeStats = m_searchTree.get_stats();
    uint64_t searchTreeSize = sizeof(m_searchTree) + sizeof(node) * (searchTreeStats.leaves + searchTreeStats.innernodes) + 
    sizeof(Type_Key) * searchTreeStats.innerslots * searchTreeStats.innernodes + sizeof(node*) * (searchTreeStats.innerslots+1) * searchTreeStats.innernodes +
    sizeof(Type_Key) * searchTreeStats.leafslots * searchTreeStats.leaves + sizeof(Type_Ts) * searchTreeStats.leafslots * searchTreeStats.leaves + sizeof(node*)*2;

    auto insertionTreeStats = m_insertionTree.get_stats();
    uint64_t insertionTreeSize = sizeof(m_insertionTree) + sizeof(node) * (insertionTreeStats.leaves + insertionTreeStats.innernodes) + 
    sizeof(Type_Key) * insertionTreeStats.innerslots * insertionTreeStats.innernodes + sizeof(node*) * (insertionTreeStats.innerslots+1) * insertionTreeStats.innernodes +
    sizeof(Type_Key) * insertionTreeStats.leafslots * insertionTreeStats.leaves + sizeof(Type_Ts) * insertionTreeStats.leafslots * insertionTreeStats.leaves + sizeof(node*)*2;

    return sizeof(int) + searchTreeSize + insertionTreeSize;
}

template <class Type_Key, class Type_Ts>
inline uint64_t IMTree<Type_Key,Type_Ts>::get_no_keys(Type_Ts Timestamp)
{
    Type_Ts lowerLimit =  ((double)Timestamp - TIME_WINDOW < numeric_limits<Type_Ts>::min()) ? numeric_limits<Type_Ts>::min(): Timestamp - TIME_WINDOW;

    uint64_t cnt = 0;
    for (auto it = m_searchTree.begin(); it != m_searchTree.end(); ++it)
    {
        if (it->second >= lowerLimit)
        {
            ++cnt;
        }
    }

    for (auto it = m_insertionTree.begin(); it != m_insertionTree.end(); ++it)
    {
        if (it->second >= lowerLimit)
        {
            ++cnt;
        }
    }

    return cnt;
}

template <class Type_Key, class Type_Ts>
inline void IMTree<Type_Key,Type_Ts>::print_stats()
{
    struct node
    {
        unsigned short level;
        unsigned short slotuse;
    };
        
    auto btreeStats = m_searchTree.get_stats();
    cout << "##### SEARCH TREE STATS #####" << endl;
    cout << "no_items: " << btreeStats.itemcount << endl;
    cout << "no_innernodes: " << btreeStats.innernodes << endl;
    cout << "no_leaves: " << btreeStats.leaves << endl;
    cout << "no_innerslots: " << btreeStats.innerslots << endl;
    cout << "no_leafslots: " << btreeStats.leafslots << endl;
    cout << "no_nodes: " << btreeStats.nodes() << endl;
    cout << "avgfill_leaves: " << btreeStats.avgfill_leaves() << endl;
    cout << "size: " << sizeof(m_searchTree) + sizeof(node) * (btreeStats.leaves + btreeStats.innernodes) + 
    sizeof(Type_Key) * btreeStats.innerslots * btreeStats.innernodes + sizeof(node*) * (btreeStats.innerslots+1) * btreeStats.innernodes +
    sizeof(Type_Key) * btreeStats.leafslots * btreeStats.leaves + sizeof(Type_Ts) * btreeStats.leafslots * btreeStats.leaves + sizeof(node*)*2 << endl;

    btreeStats = m_insertionTree.get_stats();
    cout << "##### INSERTION TREE STATS #####" << endl;
    cout << "no_items: " << btreeStats.itemcount << endl;
    cout << "no_innernodes: " << btreeStats.innernodes << endl;
    cout << "no_leaves: " << btreeStats.leaves << endl;
    cout << "no_innerslots: " << btreeStats.innerslots << endl;
    cout << "no_leafslots: " << btreeStats.leafslots << endl;
    cout << "no_nodes: " << btreeStats.nodes() << endl;
    cout << "avgfill_leaves: " << btreeStats.avgfill_leaves() << endl;
    cout << "size: " << sizeof(m_insertionTree) + sizeof(node) * (btreeStats.leaves + btreeStats.innernodes) + 
    sizeof(Type_Key) * btreeStats.innerslots * btreeStats.innernodes + sizeof(node*) * (btreeStats.innerslots+1) * btreeStats.innernodes +
    sizeof(Type_Key) * btreeStats.leafslots * btreeStats.leaves + sizeof(Type_Ts) * btreeStats.leafslots * btreeStats.leaves + sizeof(node*)*2 << endl;
    cout << endl;
}

}
#endif