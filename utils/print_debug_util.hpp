#ifndef __PRINT_DEBUG_UTIL_HPP__
#define __PRINT_DEBUG_UTIL_HPP__

#pragma once

#define DEBUG(format,...) printf("DEBUG : %s.%d.%s : ", __FILE__, __LINE__,  __PRETTY_FUNCTION__); printf(format" \n",##__VA_ARGS__)

#define DEBUG_ENTER_FUNCTION(x,y) printf("[Debug Info:] Class {%s} :: Function {%s} @ Line:%d Begin \n",x,y,__LINE__)
#define DEBUG_EXIT_FUNCTION(x,y) printf("[Debug Info:] Class {%s} :: Function {%s} @ Line:%d End \n",x,y,__LINE__)

#endif