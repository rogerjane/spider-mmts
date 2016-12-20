
#ifndef MTMACRO_H
#define MTMACRO_H

/**  Easy static vector size calculation        **/

#define ELEMENTS(x)  (sizeof(x)/sizeof(*x))

/**  C++ style memory allocation                **/

#define NEW(T, n)			(T*)malloc(sizeof(T)*(n))
#define DELETE(a)			free((char *)(a))
#define RENEW(ptr, T, n)	(ptr)=(T*)realloc((ptr), sizeof(T)*(n))

/**  ZFILL  - Zero fill a structure             **/
/**  ZFILLP - Zero fill a vector via pointer    **/
/**  ZFILLV - Zero fill a vector                **/

#define ZFILLN(a, n)   memset((char *)(a), 0, (n))
#define ZFILL(a)       ZFILLN(&(a), sizeof(a))
#define ZFILLP(a)      ZFILLN((a),  sizeof(*a))
#define ZFILLV(a)      ZFILLN((a),  sizeof(a))

#endif /* MTMACRO_H */
