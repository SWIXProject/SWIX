#ifndef __THREAD_SAFE_PRINT_UTIL_HPP__
#define  __THREAD_SAFE_PRINT_UTIL_HPP__

#pragma once
#include <stdio.h>
#include <thread>
#include <cstdio>

#define PRINT_MESSAGE(x) printf("%s \n",x)

#define ASSERT_MESSAGE(exp, msg) assert(((void)msg, exp))

#define SMALLER_THREAD_ID (std::hash<std::thread::id>{}(std::this_thread::get_id()) % 10000)

#ifdef LOG_ON
    #define LOG_ERROR(format,...) printf("\033[31mERROR : %s.%d.%s. TID: %lu : \033[0m" format" \n", __FILE__, __LINE__, __PRETTY_FUNCTION__, SMALLER_THREAD_ID,##__VA_ARGS__); 
    #define LOG_INFO(format,...) printf("\033[32mINFO : %s.%d.%s. TID: %lu : \033[0m" format" \n", __FILE__, __LINE__, __PRETTY_FUNCTION__, SMALLER_THREAD_ID,##__VA_ARGS__); 
    #define LOG_WARNING(format,...) printf("\033[33mWARNING : %s.%d.%s. TID: %lu : \033[0m" format" \n", __FILE__, __LINE__, __PRETTY_FUNCTION__, SMALLER_THREAD_ID,##__VA_ARGS__);
    #define LOG_DEBUG(format,...) printf("\033[33mDEBUG : %s.%d.%s. TID: %lu : \033[0m" format" \n", __FILE__, __LINE__, __PRETTY_FUNCTION__, SMALLER_THREAD_ID,##__VA_ARGS__);
    #define LOG_DEBUG_SHOW(format,...) printf("\033[33mDEBUG : %s.%d.%s. TID: %lu : \033[0m" format" \n", __FILE__, __LINE__, __PRETTY_FUNCTION__, SMALLER_THREAD_ID,##__VA_ARGS__);
#else
    #define LOG_ERROR(format,...) printf("\033[31mERROR : %s.%d.%s. TID: %lu : \033[0m" format" \n", __FILE__, __LINE__, __PRETTY_FUNCTION__, SMALLER_THREAD_ID,##__VA_ARGS__); 
    #define LOG_INFO(format,...) do {} while(0);
    #define LOG_WARNING(format,...) do {} while(0);
    #define LOG_DEBUG(format,...) do {} while(0);
    #define LOG_DEBUG_SHOW(format,...) printf("\033[33mDEBUG : %s.%d.%s. TID: %lu : \033[0m" format" \n", __FILE__, __LINE__, __PRETTY_FUNCTION__, SMALLER_THREAD_ID,##__VA_ARGS__);
#endif

#endif