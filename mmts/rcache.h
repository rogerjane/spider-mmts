#ifndef _RCACHE_H
#define _RCACHE_H

#include <smap.h>

typedef SSMAP* (*rcache_callback_map)(const char *szKey, SSMAP *OriginalMap);
typedef const char* (*rcache_callback_str)(const char *szKey, const char *szOriginalValue);

void rcache_SetDir(const char *szDir);

void rcache_AddPoolMap(const char *szPool, rcache_callback_map cb, const char *szDir);
void rcache_AddPoolStr(const char *szPool, rcache_callback_str cb, const char *szDir);
void rcache_SetStale(int nBestBefore, int nEatBy, int nRetry);

const char *rcache_Get(const char *szPool, const char *szKey, const char *szName);
const char *rcache_GetValue(const char *szPool, const char *szKey);

#endif
