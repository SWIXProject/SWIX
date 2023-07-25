#ifndef __TIMER__
#define __TIMER__

#pragma once
#include <time.h>

#define NS_PER_S 1000000000.0
#define TIMER_DECLARE(id) struct timespec start_time##id, end_time##id
#define TIMER_BEGIN(id) clock_gettime(CLOCK_MONOTONIC, &start_time##id)
#define TIMER_END_NS(id,output_t) clock_gettime(CLOCK_MONOTONIC, &end_time##id); \
    (output_t)=(end_time##id.tv_sec-start_time##id.tv_sec)*NS_PER_S+(end_time##id.tv_nsec-start_time##id.tv_nsec)
#define TIMER_END_S(id,output_t) clock_gettime(CLOCK_MONOTONIC, &end_time##id); \
    (output_t)=(end_time##id.tv_sec-start_time##id.tv_sec)+(end_time##id.tv_nsec-start_time##id.tv_nsec)/NS_PER_S

#endif