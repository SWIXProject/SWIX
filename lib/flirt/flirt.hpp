#ifndef __FLIRT_HPP__
#define __FLIRT_HPP__

#pragma once
#include "flirt_helper.hpp"

using namespace std;

#ifndef INITIAL_ERROR
#define GLOBAL_ERROR 256
#else
#define GLOBAL_ERROR INITIAL_ERROR
#endif

namespace flirt{

template<class K> class Flirt;

/*
***************** LP Segments *****************
*/
template<class K>
class Segment 
{
private:
	int n;    //current number of keys
	int nDelete; //number of keys deleted from keys
	double slope;
	double slopeLow;  //Low Slope of Segment
	double slopeHigh; //High Slope of Segment

	K keyStart; //starting key
	vector<Key<K>> keys;   //Vector of keys
	Segment<K>* leftSibling = nullptr; //Pointer to the left sibling (leaf only)
	Segment<K>* rightSibling = nullptr; //Pointer to the right sibling (leaf only)

public:
    //Constructor
	Segment(K keyStart, double slopeLow = 0, double slopeHigh = numeric_limits<double>::max());

public:
    //Point Lookup and Range Search
	bool lookup(K key);
    void range_search(K key, K searchRange, vector<K> & searchResults);
    void range_search(K key, int matchRate, vector<K> & searchResults);
    void range_search(K key, K lowerBound, K upperBound, vector<K> & searchResults);
    
private:
    //Search Helpers
    void range_scan(int actualPos, K key, K lowerBound, K upperBound, vector<K> & searchResults);
    void range_scan(int actualPos, int matchRate, vector<K> & searchResults);

public:
	//Enqueue and Dequeue
	Segment<K>* push_back(K key);
    void pop_front();

public:
    //Getters
    uint64_t get_model_size_in_bytes();
    uint64_t get_total_size_in_bytes();

    friend class Flirt<K>;
};

/*
Constructor
*/
template<class K>
Segment<K>::Segment(K keyStart, double slopeLow, double slopeHigh)
{
    this->slopeLow = slopeLow;
    this->slopeHigh = slopeHigh;
    this->slope = 1;
    this->keyStart = keyStart;
    this->keys = { {keyStart,false} };
    this->n = 1;
    this->nDelete = 0;
}

/*
Search
*/
template<class K>
bool Segment<K>::lookup(K key) {
	if (nDelete == n) {
		return false;
	}

	if (n == 0) {
		return (keys[0].key == key);
	}

	int pos = (key - keyStart) * slope;
    return binary_search_vector_key(keys, pos - GLOBAL_ERROR, pos + GLOBAL_ERROR, key);
}

template<class K>
void Segment<K>::range_search(K key, K searchRange, vector<K> & searchResults)
{

    K lowerBound = (double)key - (double)searchRange < numeric_limits<K>::min()?
                           numeric_limits<K>::min() : key - searchRange; 

    K upperBound = (double)key + (double)searchRange > numeric_limits<K>::max()?
                           numeric_limits<K>::max() : key + searchRange; 

    int predictPos = (lowerBound - keyStart) * slope;
    
    int actualPos = binary_search_vector_key_return_index(keys, predictPos-GLOBAL_ERROR,predictPos+GLOBAL_ERROR, lowerBound);
    actualPos = (actualPos == -1)? n : actualPos;
    range_scan(actualPos, key, lowerBound, upperBound, searchResults);
}

template<class K>
void Segment<K>::range_search(K key, int matchRate, vector<K> & searchResults)
{
    int predictPos = (key - keyStart) * slope;
    
    int actualPos = binary_search_vector_key_return_index(keys, predictPos-GLOBAL_ERROR,predictPos+GLOBAL_ERROR, key);
    actualPos = (actualPos == -1)? n : actualPos;
    range_scan(actualPos,matchRate, searchResults);
}

template<class K>
void Segment<K>::range_search(K key, K lowerBound, K upperBound, vector<K> & searchResults)
{
    int predictPos = (lowerBound - keyStart) * slope;
    
    int actualPos = binary_search_vector_key_return_index(keys, predictPos-GLOBAL_ERROR,predictPos+GLOBAL_ERROR, lowerBound);
    actualPos = (actualPos == -1)? n : actualPos;
    range_scan(actualPos, key, lowerBound, upperBound, searchResults);
}

template<class K>
void Segment<K>::range_scan(int actualPos, K key, K lowerBound, K upperBound, vector<K> & searchResults)
{
    while (actualPos < n && keys[actualPos].key <= upperBound)
    {
        if (keys[actualPos].key >= lowerBound)
        {
            searchResults.push_back(keys[actualPos].key);
        }
        actualPos++;
    } 

    if (rightSibling && rightSibling->keyStart <= upperBound)
    {
        rightSibling->range_scan(0,key,lowerBound,upperBound,searchResults);
    }
}

template<class K>
void Segment<K>::range_scan(int actualPos, int matchRate, vector<K> & searchResults)
{
    while (actualPos < n && searchResults.size() < matchRate)
    {
        searchResults.push_back(keys[actualPos].key);
        actualPos++;
    }

    if (rightSibling && searchResults.size() < matchRate)
    {
        rightSibling->range_scan(0,matchRate,searchResults);
    }
}

/*
Push Back
*/
template<class K>
Segment<K>* Segment<K>::push_back(K key) 
{
	if (key == keys[n - 1].key) { return nullptr; }
	if (key < keys[n - 1].key) {
		throw invalid_argument("Key must be largest to be appended");
	}

    //Slope Calculation - ShrinkingCone Algorithm based on FITingTree
    //If slope less than error than just add to leaf
	if (slopeLow <= (double)(n) / (double)(key - keyStart) &&
		(double)(n) / (double)(key - keyStart) <= slopeHigh) {
		double slopeHighTemp = (double)((n + GLOBAL_ERROR)) / (double)(key - keyStart);
		double slopeLowTemp = (double)((n - GLOBAL_ERROR)) / (double)(key - keyStart);

		slopeHigh = min(slopeHigh, slopeHighTemp);
		slopeLow = max(slopeLow, slopeLowTemp);
		slope = (slopeHigh + slopeLow) / 2;
		Key<K> temp = { key,false };
		keys.push_back(temp);
		n++;
		return nullptr;
	}
	//Else create new segment
	else {
		Segment<K>* segPtr = new Segment<K>(key);
		rightSibling = segPtr;
		segPtr->leftSibling = this;
		return segPtr;
	}
}

/*
Pop Front
*/
template<class K>
void Segment<K>::pop_front()
{
    keys[nDelete++].deleted = true;
}

/*
Getters
*/
template<class K>
uint64_t Segment<K>::get_model_size_in_bytes(){
    
    return sizeof(int)*2 + sizeof(double)*3 + sizeof(K) + sizeof(Segment<K>*)*2 + sizeof(vector<Key<K>>);
}

template<class K>
uint64_t Segment<K>::get_total_size_in_bytes(){
        
    return sizeof(int)*2 + sizeof(double)*3 + sizeof(K) + sizeof(Segment<K>*)*2 + sizeof(vector<Key<K>>)
    + sizeof(K)*n + sizeof(bool)*n;
}



/*
***************** FLIRT *****************
*/
template<class K>
class Flirt 
{
private:
    int first_index;  //front index
    int last_index;  //back index
    int capacity; //capacity
    int n; // current size of the queue
    
    pair<K,Segment<K>*>* queue; //queue which is the SummaryList

    Segment<K>* first = nullptr; //first segment in queue
    Segment<K>* last = nullptr; //last segment in queue

public:
    //Constructor & Destructor
    Flirt(int size);
    ~Flirt();

public:
    //Search
    bool search(K target);
    void range_search(K target, K searchRange, vector<K> & searchResults);
    void range_search(K target, int matchRate, vector<K> & searchResults);
    void range_search(K target, K lowerBound, K upperBound, vector<K> & searchResults);

private:
    //Search Helpers
    bool linear_search(K target, int left, int right);
    bool binary_search(K target, int left, int right);

    void linear_range_search(K target, K searchRange, int left, int right, vector<K> & searchResults);
    void linear_range_search(K target, int matchRate, int left, int right, vector<K> & searchResults);
    void linear_range_search(K target, K lowerBound, K upperBound, int left, int right, vector<K> & searchResults);

    void binary_range_search(K target, K searchRange, int left, int right, vector<K> & searchResults);
    void binary_range_search(K target, int matchRate, int left, int right, vector<K> & searchResults);
    void binary_range_search(K target, K lowerBound, K upperBound, int left, int right, vector<K> & searchResults);
    
public:
    //Enqueue & Dequeue
    void enqueue(K key);
    void dequeue();

public:
    //Print, Getters & Setters
    void printQueue();

    uint64_t get_model_size_in_bytes();
    uint64_t get_total_size_in_bytes();

    int get_n(){return this->n;}
};

/*
 Constructor & Destructor
*/
template<class K>
Flirt<K>::Flirt(int size)
{
    queue = new pair<K,Segment<K>*>[size];
    capacity = size;
    first_index = 0;
    last_index = -1;
    n = 0;
};

template<class K>
Flirt<K>::~Flirt()
{
    if (n != 0)
    {
        Segment<K>* temp = first->rightSibling;
        delete first;
        while (temp != NULL)
        {
            Segment<K>* temp2 = temp->rightSibling;
            delete temp;
            temp = temp2;
        }
    }
    delete queue;
    queue = nullptr;
};


/*
Search
*/
template<class K>
bool Flirt<K>::search(K target)
{
    if (n == 0)
    {
        cout << "Empty Queue" << endl;
        return false;
    }
    
    if (n == 1)
    {
        return queue[first_index].second->lookup(target);
    }

    if (n*sizeof(K) < 256) //Less than 256 bytes
    {
        return linear_search(target,first_index,last_index);
    }

    if (last_index < first_index)
    {
        if(target > queue[capacity-1].first) // not in second half arr
        {
            return binary_search(target, 0, last_index);
        }
        else // maybe in second half arr
        {
            return binary_search(target, first_index, capacity-1);
        }
    }
    else
    {
        return binary_search(target, first_index, last_index);
    }
}

template<class K>
void Flirt<K>::range_search(K target, K searchRange, vector<K> & searchResults)
{
    if (n == 0)
    {
        cout << "Empty Queue" << endl;
        return;
    }
    
    if (n == 1)
    {
        return queue[first_index].second->range_search(target,searchRange,searchResults);  
    }

    if (n*sizeof(K) < 256) //Less than 256 bytes
    {
        return linear_range_search(target,searchRange,first_index,last_index,searchResults);
    }

    if (last_index < first_index)
    {
        if(target > queue[capacity-1].first) // not in second half arr
        {
            return binary_range_search(target,searchRange, 0, last_index,searchResults);
        }
        else // maybe in second half arr
        {
            return binary_range_search(target,searchRange, first_index, capacity-1,searchResults);
        }
    }
    else
    {
        return binary_range_search(target,searchRange, first_index, last_index,searchResults);
    }
}

template<class K>
void Flirt<K>::range_search(K target, int matchRate, vector<K> & searchResults)
{
    if (n == 0)
    {
        cout << "Empty Queue" << endl;
        return;
    }
    
    if (n == 1)
    {
        return queue[first_index].second->range_search(target,matchRate,searchResults);  
    }

    if (n*sizeof(K) < 256) //Less than 256 bytes
    {
        return linear_range_search(target,matchRate,first_index,last_index,searchResults);
    }

    if (last_index < first_index)
    {
        if(target > queue[capacity-1].first) // not in second half arr
        {
            return binary_range_search(target, matchRate, 0, last_index,searchResults);
        }
        else // maybe in second half arr
        {
            return binary_range_search(target, matchRate, first_index, capacity-1,searchResults);
        }
    }
    else
    {
        return binary_range_search(target, matchRate, first_index, last_index, searchResults);
    }
}

template<class K>
void Flirt<K>::range_search(K target, K lowerBound, K upperBound, vector<K> & searchResults)
{
    if (n == 0)
    {
        cout << "Empty Queue" << endl;
        return;
    }
    
    if (n == 1)
    {
        return queue[first_index].second->range_search(target,lowerBound,upperBound,searchResults);  
    }

    if (n*sizeof(K) < 256) //Less than 256 bytes
    {
        return linear_range_search(target,lowerBound,upperBound,first_index,last_index,searchResults);
    }

    if (last_index < first_index)
    {
        if(target > queue[capacity-1].first) // not in second half arr
        {
            return binary_range_search(target,lowerBound,upperBound, 0, last_index,searchResults);
        }
        else // maybe in second half arr
        {
            return binary_range_search(target,lowerBound,upperBound, first_index, capacity-1,searchResults);
        }
    }
    else
    {
        return binary_range_search(target,lowerBound,upperBound, first_index, last_index,searchResults);
    }
}

/*
Linear Search Method used in Search Operation
For finding the segment in SummaryList
*/
template<class K>
bool Flirt<K>::linear_search(K target, int left, int right)
{
   for(int i = left;  i < right; i++)
   {
       if (queue[i].first <= target && target < queue[i+1].first)
       {
           return queue[i].second->lookup(target);
       }
   }

    if (queue[right].first <= target)
    {
        return queue[right].second->lookup(target);
    }

   return false;
}

template<class K>
void Flirt<K>::linear_range_search(K target, K searchRange, int left, int right, vector<K> & searchResults)
{
   for(int i = left;  i < right; i++)
   {
       if (queue[i].first <= target && target < queue[i+1].first)
       {
           queue[i].second->range_search(target,searchRange,searchResults);
           return;
       }
   }
    if (queue[right].first <= target)
    {
        queue[right].second->range_search(target,searchRange,searchResults);
        return;
    }
   return;
}

template<class K>
void Flirt<K>::linear_range_search(K target, int matchRate, int left, int right, vector<K> & searchResults)
{
   for(int i = left;  i < right; i++)
   {
       if (queue[i].first <= target && target < queue[i+1].first)
       {
           queue[i].second->range_search(target,matchRate,searchResults);
           return;
       }
   }
    if (queue[right].first <= target)
    {
        queue[right].second->range_search(target,matchRate,searchResults);
        return;
    }
   return;
}

template<class K>
void Flirt<K>::linear_range_search(K target, K lowerBound, K upperBound, int left, int right, vector<K> & searchResults)
{
   for(int i = left;  i < right; i++)
   {
       if (queue[i].first <= lowerBound && lowerBound < queue[i+1].first)
       {
           queue[i].second->range_search(target,lowerBound,upperBound,searchResults);
           return;
       }
   }
    if (queue[right].first <= lowerBound)
    {
        queue[right].second->range_search(target,lowerBound,upperBound,searchResults);
        return;
    }
   return;
}

/*
Binary Search Method used in Search Operation
For finding the segment in SummaryList
*/
template<class K>
bool Flirt<K>::binary_search(K target, int left, int right)
{
    //edge case (left  boundary)
    if (target < queue[left+1].first)
    {
        if (queue[left].first <= target)
        {
            return queue[left].second->lookup(target);
        }
        else
        {
            return false;
        }
    }

    //edge case (right boundary)
    if (target >= queue[right-1].first)
    {
        if (target < queue[right].first)
        {
            return queue[right-1].second->lookup(target);
        }
        else
        {
            return queue[right].second->lookup(target);
        }
    }

    while(left <= right)
    {
        int mid = (left + right) / 2;
        if (queue[mid].first <= target && queue[mid+1].first > target)
        {
            return queue[mid].second->lookup(target);
        }
        if (queue[mid].first < target)
        {
            left = mid + 1;
        }
        else
        {
            right = mid - 1;
        }
    }

    return false;
}

template<class K>
void Flirt<K>::binary_range_search(K target, K searchRange, int left, int right, vector<K> & searchResults)
{
    //edge case (left  boundary)
    if (target < queue[left+1].first)
    {
        if (queue[left].first <= target)
        {
            return queue[left].second->range_search(target,searchRange,searchResults);
        }
        else
        {
            return;
        }
    }

    //edge case (right boundary)
    if (target >= queue[right-1].first)
    {
        if (target < queue[right].first)
        {
            return queue[right-1].second->range_search(target,searchRange,searchResults);
        }
        else
        {
            return queue[right].second->range_search(target,searchRange,searchResults);
        }
    }

    while(left <= right)
    {
        int mid = (left + right) / 2;
        if (queue[mid].first <= target && queue[mid+1].first > target)
        {
            return queue[mid].second->range_search(target,searchRange,searchResults);
        }
        if (queue[mid].first < target)
        {
            left = mid + 1;
        }
        else
        {
            right = mid - 1;
        }
    }
    return;
}

template<class K>
void Flirt<K>::binary_range_search(K target, int matchRate, int left, int right, vector<K> & searchResults)
{
    //edge case (left  boundary)
    if (target < queue[left+1].first)
    {
        if (queue[left].first <= target)
        {
            return queue[left].second->range_search(target,matchRate,searchResults);
        }
        else
        {
            return;
        }
    }

    //edge case (right boundary)
    if (target >= queue[right-1].first)
    {
        if (target < queue[right].first)
        {
            return queue[right-1].second->range_search(target,matchRate,searchResults);
        }
        else
        {
            return queue[right].second->range_search(target,matchRate,searchResults);
        }
    }

    while(left <= right)
    {
        int mid = (left + right) / 2;
        if (queue[mid].first <= target && queue[mid+1].first > target)
        {
            return queue[mid].second->range_search(target,matchRate,searchResults);
        }
        if (queue[mid].first < target)
        {
            left = mid + 1;
        }
        else
        {
            right = mid - 1;
        }
    }
    return;
}

template<class K>
void Flirt<K>::binary_range_search(K target, K lowerBound, K upperBound, int left, int right, vector<K> & searchResults)
{
    //edge case (left  boundary)
    if (lowerBound < queue[left+1].first)
    {
        if (queue[left].first <= lowerBound)
        {
            return queue[left].second->range_search(target,lowerBound,upperBound,searchResults);
        }
        else
        {
            return;
        }
    }

    //edge case (right boundary)
    if (lowerBound >= queue[right-1].first)
    {
        if (lowerBound < queue[right].first)
        {
            return queue[right-1].second->range_search(target,lowerBound,upperBound,searchResults);
        }
        else
        {
            return queue[right].second->range_search(target,lowerBound,upperBound,searchResults);
        }
    }

    while(left <= right)
    {
        int mid = (left + right) / 2;
        if (queue[mid].first <= lowerBound && queue[mid+1].first > lowerBound)
        {
            return queue[mid].second->range_search(target,lowerBound,upperBound,searchResults);
        }
        if (queue[mid].first < lowerBound)
        {
            left = mid + 1;
        }
        else
        {
            right = mid - 1;
        }
    }
    return;
}

/*
Enqueue 
*/
template<class K>
void Flirt<K>::enqueue(K key)
{
    if (n == capacity)
    {
        cout << "The queue is full" << endl;
        return;
    }

    if (n == 0){
        Segment<K>* segPtr = new Segment<K>(key);
        last_index++;
        last_index %= capacity;
        queue[last_index] = make_pair(key,segPtr);
        first = segPtr;
        last = segPtr;
        n++;
        return;
    }

    Segment<K>* newSeg = last->push_back(key);
    if (newSeg != NULL){
        last = newSeg;
        last_index++;
        last_index %= capacity;
        queue[last_index] = make_pair(key,newSeg);
        n++;
    }
    return;
}

/*
Dequeue 
*/
template<class K>
void Flirt<K>::dequeue()
{
    if(n == 0)
    {
        cout << "The queue is empty" << endl;
        return;
    }
    
    if (first->nDelete == first->n-1){
        if (first == last){
            first_index = 0;
            last_index = -1;
            n = 0;
            delete first;
            first = nullptr;
            last = nullptr;
            return;
        }
        first_index ++;
        first_index %= capacity;
        n--;
        Segment<K>*  temp = first->rightSibling;
        delete first;
        first = temp;
        first->leftSibling = nullptr;
        return;
    }

    first->pop_front();
    return;
}

/*
Print Content of Queue
*/
template<class K>
void Flirt<K>::printQueue()
{
    // Print first key
    for(int i = 0; i < n; i++)
    {
        cout << queue[i].first << "\t";
    }
    cout << endl;
}

/*
Getters
*/
template<class K>
uint64_t Flirt<K>::get_model_size_in_bytes()
{
    uint64_t model_size = sizeof(Segment<K>*)*2 + sizeof(int)*4 + sizeof(pair<K,Segment<K>*>*) + 
    sizeof(pair<K,Segment<K>*>)*n;

    for (int i=first_index; i<=last_index;i++)
    {
        model_size += queue[i].second->get_model_size_in_bytes();
    }
    return model_size;
}

template<class K>
uint64_t Flirt<K>::get_total_size_in_bytes()
{
    uint64_t total_size = sizeof(Segment<K>*)*2 + sizeof(int)*4 + sizeof(pair<K,Segment<K>*>*) + 
    sizeof(pair<K,Segment<K>*>)*n;

    for (int i=first_index; i<=last_index;i++)
    {
        total_size += queue[i].second->get_total_size_in_bytes();
    }
    return total_size;
}

}
#endif