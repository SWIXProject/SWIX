#ifndef __SWIX_TUNER_HPP__
#define __SWIX_TUNER_HPP__

#pragma once
#include "../utils/tuner_variables.hpp"
#include "config.hpp"

using namespace std;

namespace swix {

class SwixTuner
{
// private:
public:
    bool m_direction;
    int m_splitError;
    int m_searchWeight = 0.32*0.1228; 
    int m_scanWeight = 0.34*1.6424;
    int m_insertWeight = 0.16*0.1274;
    int m_retrainWeight = 0.06*0.8892;

    int m_noTimesInDirection;
    int m_noTimesOppositeDirection;

    float m_adjustment;
    double m_previousCost;

public:
    SwixTuner(int splitError)
    {
        m_splitError = splitError;
        m_previousCost = 0;
        m_adjustment = 2;
        m_direction = 1;
        m_noTimesInDirection = 0;
        m_noTimesOppositeDirection = 0;
    }

    int initial_tune()
    {
        double searchCost = (double)rootLengthExponentialSearch + (double)segLengthExponentialSearchAll;
        double scanCost = (double)segScanNoSeg/segNoScan;
        double retrainCost = (double)segLengthMerge + (double)segLengthRetrain;

        m_previousCost = m_searchWeight*((searchCost == 0)?0:log(searchCost))
                        + m_scanWeight*((scanCost == 0)?0:log(scanCost))
                        + m_insertWeight*log(m_splitError)
                        + m_retrainWeight *((retrainCost == 0)?0:log(retrainCost));

        m_splitError *= m_adjustment;

        rootNoExponentialSearch = 0;
        rootLengthExponentialSearch = 0;
        segNoExponentialSearch = 0;
        segLengthExponentialSearch = 0;
        segNoScan = 0;
        segScanNoSeg = 0;
        segNoRetrain = 0;
        segLengthMerge = 0;
        segLengthRetrain = 0;
        segNoExponentialSearchAll = 0;
        segLengthExponentialSearchAll = 0;

        tuneStage++;

        return m_splitError;
    }

    int tune()
    {            
        double searchCost = (double)rootLengthExponentialSearch + (double)segLengthExponentialSearchAll;
        double scanCost = (double)segScanNoSeg/segNoScan;
        double retrainCost = (double)segLengthMerge + (double)segLengthRetrain;

        double cost = m_searchWeight*((searchCost == 0)?0:log(searchCost))
                        + m_scanWeight*((scanCost == 0)?0:log(scanCost))
                        + m_insertWeight*log(m_splitError)
                        + m_retrainWeight *((retrainCost == 0)?0:log(retrainCost));
       
        if (m_previousCost < cost)
        {
            if (m_noTimesOppositeDirection > 10 && m_adjustment > 1.01)
            {
                m_adjustment = max(m_adjustment - 0.1, 1.01);
                m_noTimesOppositeDirection = 0;
            }

            if (m_direction)
            {
                m_direction = 0;
                if (m_splitError == 128)
                {
                    m_splitError = 256;
                    m_direction = 1;
                }
                else
                {
                    m_splitError = max(128, (int)(m_splitError/m_adjustment));
                }
            }
            else
            {
                // cout << "Change to Increase" << endl;
                m_direction = 1;
                if (m_splitError == 512)
                {
                    m_splitError = 256;
                    m_direction = 0;
                }
                else
                {
                    m_splitError = min(512,(int)(m_splitError*m_adjustment));
                }
            }

            m_noTimesOppositeDirection++;
            m_noTimesInDirection = 0;
        }
        else //Same direction
        {
            
            if (m_noTimesInDirection > 10)
            {
                m_adjustment += 0.1;
                m_noTimesInDirection = 0;
            }
            
            if (m_direction)
            {
                // cout << "Increase" << endl;
                if (m_splitError == 512)
                {
                    m_splitError = 256;
                    m_direction = 0;
                }
                else
                {
                    m_splitError = min(512,(int)(m_splitError*m_adjustment));
                }
            }
            else
            {
                // cout << "Decrease" << endl;
                if (m_splitError == 128)
                {
                    m_splitError = 256;
                    m_direction = 1;
                }
                else
                {
                    m_splitError = max(128, (int)(m_splitError/m_adjustment));
                }
            }

            m_noTimesInDirection++;
            m_noTimesOppositeDirection = 0;
        }
        

        m_previousCost = cost;
        rootNoExponentialSearch = 0;
        rootLengthExponentialSearch = 0;
        segNoExponentialSearch = 0;
        segLengthExponentialSearch = 0;
        segNoScan = 0;
        segScanNoSeg = 0;
        segNoRetrain = 0;
        segLengthMerge = 0;
        segLengthRetrain = 0;
        segNoExponentialSearchAll = 0;
        segLengthExponentialSearchAll = 0;

        tuneStage++;
        
        return m_splitError;
    }
};

}
#endif