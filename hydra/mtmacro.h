#ifndef MTMACRO_H
#define MTMACRO_H

#ifdef __cplusplus
extern "C" {
#endif

#ifdef __SCO_VERSION__
	    #define OS  "SCO"
	    #define IS_SCO 1
#else
#ifdef __APPLE__
	    #define OS  "MAXOS"
	    #define IS_MAC 1
#else
	    #define OS  "LINUX"
	    #define IS_LINUX 1
#endif
#endif

#if IS_MAC
#pragma clang diagnostic ignored "-Wdeprecated"			// Stops complaint about treating 'c' input as 'c++'

#define open64 open
#define off64_t off_t
#define lseek64 lseek
#endif

#define NEW(T, n)			(T*)malloc(sizeof(T)*(n))
#define RENEW(ptr, T, n)	(ptr)=(T*)realloc((ptr), sizeof(T)*(n))

#ifdef __cplusplus
}
#endif

#endif /* MTMACRO_H */

