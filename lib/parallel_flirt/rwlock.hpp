//From: https://stackoverflow.com/questions/27860685/how-to-make-a-multiple-read-single-write-lock-from-more-basic-synchronization-pr

#pragma once
#include <mutex>
#include <condition_variable>

using namespace std;

namespace flirt{

class SharedMutexRW 
{
private:
    mutex              shared;
    condition_variable readerQ;
    condition_variable writerQ;
    int                     activeReaders;
    int                     waitingWriters;
    int                     activeWriters;

public:
    SharedMutexRW()
    : shared()
    , readerQ(), writerQ()
    , activeReaders(0), waitingWriters(0), activeWriters(0)
    {}

    void lock_shared() {
        unique_lock<mutex> lk(shared);
        while( waitingWriters != 0 )
            readerQ.wait(lk);
        ++activeReaders;
        lk.unlock();
    }

    void unlock_shared() {
        unique_lock<mutex> lk(shared);
        --activeReaders;
        lk.unlock();
        writerQ.notify_one();
    }

    void lock() {
        unique_lock<mutex> lk(shared);
        ++waitingWriters;
        while( activeReaders != 0 || activeWriters != 0 )
            writerQ.wait(lk);
        ++activeWriters;
        lk.unlock();
    }

    void unlock() {
        unique_lock<mutex> lk(shared);
        --waitingWriters;
        --activeWriters;
        if(waitingWriters > 0)
            writerQ.notify_one();
        else
            readerQ.notify_all();
        lk.unlock();
    }
};

}