//For parallel swix
#pragma once
#include "../parameters_p.hpp"
#define MAX_BUFFER_SIZE 256
#define INITIAL_ERROR 64
#define AUTO_TUNE_RATE 0.1
#define AUTO_TUNE_SIZE AUTO_TUNE_RATE * TIME_WINDOW
enum class task_status { FINISH = -6, ROUND_END = -5, SEARCH = -4, INSERT = -3, DELETE = -2, RETRAIN = -1};