
#include <errno.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <heapstrings.h>
#include <smap.h>

#include "rcache.h"

#define STATIC //static
#define API

#define RCACHE_BASEDIR	"/usr/mt/cache"				// In case it doesn't get set

// Do we want to be able to set a timeout value when calling the callback function in case it doesn't return?

// We could store and return multiple string responses.  They could be stored in the file either
// using \; or \x00 to separate them and we could always return strings to the caller with a double \0 on the end:
// Single response: "10.123.12.56\0\0"
// Multiple reponse: "L82019\0L82003\0L83102\0\0"
// I.e. the caller can always safely treat it as an sz or an szz.
// How do we represent these in the SSMAP returned from the callback function?
//   We could have a seperator character stored per pool that, if set, is used to split any return values

// Cache algorithm...
// Each cache pool (e.g. dns, ldap) has four parameters:
// callback function, best_before period, eat_by period, retry_period
// The callback function accepts a key and returns a map associated with that key
// value - the return value for the cases where there is only one (e.g. an IP address for a DNS fetch)
// Add a pool to the cache system using cache_AddPool(name, callback_ function);
//
// Attempt to read map from file if the file has changed since it was last read (or it hasn't been read before)
// If successful and now < cache_BestBefore then return map
// If now > cache_TryAgain or failed to fetch from file
//    Try to fetch from wherever we get things from (LDAP, DNS etc.), replacing map if successful
//      If successful:
//        Replace the map with the new one fetched, setting cache_BestBefore and cache_EatBy to future values
//    If not successful, set cache_TryAgain to now + whatever-period
//    Write the map back to the file
// If now < cache_EatBy then return map
// free map
// return NULL
//
// A better way, given we can fork off another process, is to simply return the cached version if we're beyond
// the BestBefore but fetch in the background.  That way the results will always tend to be returned
// immediately.
//
// API:
// Set the base directory for the cache (default is /usr/mt/cache)
// = void rcache_SetDir(const char *szDir);
//
// Add a pool using one of the following two functions, the first is for cases where multiple strings need
// returning for a single key (for example with LDAP calls), the second is for single strings.
// szPool is a tag used to match rcache_Get, "dns" and "ldap" might be good examples.
// cb is the callback function that must be defined as one of:
//   SSMAP* callback(const char *szKey, SSMAP *OriginalMap)
//   const char *callback(const char *szKey, const char *OriginalValue)
// szDir is the directory to contain the cache files or NULL to use the BASEDIR/szPool - i.e. if rcache_SetDir()
//   hasn't been called, szPool is 'dns' and szDir is NULL, /usr/mt/cache/dns will be used.
// = void rcache_AddPoolMap(const char *szPool, rcache_callback_map cb, const char *szDir);
// = void rcache_AddPoolStr(const char *szPool, rcache_callback_str cb, const char *szDir);
//
// Only callable by the callback function, used to set the BestBefore, EatBy and Retry periods in seconds
// If this is not called, a default that's currently 1 day, 7 days and 10 minutes is used.
// = void rcache_SetStale(int nBestBefore, int nEatBy, int nRetry);
//
// Get a single value from the map returned from a cache function.  NULL is returned if the name is not one in
// the map or the cache is stale and no new value can be obtained.
// = const char *rcache_Get(const char *szPool, const char *szKey, const char *szName);
// This is the same call, but uses "value" for szName
// = const char *rcache_GetValue(const char *szPool, const char *szKey);
// This one returns the entire map as returned from the callback function:
// = const char *rcache_GetMap(const char *szPool, const char *szKey);


typedef struct pooldata {
	rcache_callback_map cbmap;			// Callback function that can fetch raw data into a map
	rcache_callback_str cbstr;			// Callback function that can fetch raw data into a const char *
	SIMAP *keys;						// Where to find the keys in memory
	const char *szDir;					// The Directory where we keep this pool
	char separator;						// Separator for when we return multiple results
} pooldata;

typedef struct keydata {
	time_t filetime;					// Time this file was last read in
	char szRefreshed[20];
	char szBestBefore[20];
	char szTryAgain[20];
	char szEatBy[20];
	int nRetry;
	SSMAP *map;							// The values
} keydata;

static const char *szBaseDir = NULL;
static SIMAP *pools = NULL;
static keydata *callback_kd = NULL;		// Points to the current keydata ONLY when a callback is active (else NULL)
static char bSetStaleCalled = 0;		// Set if the callback function calls rcache_SetStale()

STATIC const char *rcache_TimeToText(time_t t)
{
	static char result[20];
	struct tm *tm = gmtime(&t);

	snprintf(result, sizeof(result), "%04d-%02d-%02d %02d:%02d:%02d",
			tm->tm_year+1900, tm->tm_mon+1, tm->tm_mday,
			tm->tm_hour, tm->tm_min, tm->tm_sec);

	return result;
}

// Just for debugging
#if 0

static void pd_Dump(pooldata *pd)
{
	fprintf(stderr, "Pool data at  %p\n", pd);
	fprintf(stderr, "Callback at   %p\n", pd->cb);
	fprintf(stderr, "Key map at    %p\n", pd->keys);
	fprintf(stderr, "Dir          '%s'\n", pd->szDir);
}

static void kd_Dump(keydata *kd, const char *comment)
{
	fprintf(stderr, "Key data at  %p (%s)", kd, comment?comment:"");
	fprintf(stderr, "map is at    %p\n", kd->map);

	fprintf(stderr, "File time   '%s'\n", rcache_TimeToText(kd->filetime));
	fprintf(stderr, "Refreshed   '%s'\n", kd->szRefreshed);
	fprintf(stderr, "Best before '%s'\n", kd->szBestBefore);
	fprintf(stderr, "Try again   '%s' (+%d)\n", kd->szTryAgain, kd->nRetry);
	fprintf(stderr, "Eat by      '%s'\n", kd->szEatBy);
}
#endif

//////////////////////////////////////////////////


STATIC int rcache_MkDir(const char *szDir)
// const in this case means we'll leave it like we found it, but it may be modified during this call
// Ensures that the given path exists
// Returns  1   The path now exists
//          0   Something went really wrong and I couldn't create it
{
	char *chp;

	if (!szDir) return 0;

	if (mkdir(szDir, 0777)) {            // Error
		switch (errno) {
		case EEXIST:                    // it already exists - great!
			break;
		case ENOENT:                    // An ancestor doesn't exist
			chp=strrchr(szDir, '/');

			if (chp) {                  // Good we are not base dir!
				*chp='\0';              // Terminate our parent dir
				int err = !rcache_MkDir(szDir);
				*chp='/';
				if (err) return 0;
				if (!rcache_MkDir(szDir)) return 0;
			}
			break;
		default:
			chmod(szDir, 0777);
			return 1;
		}
	}

	return 1;
}

const char *rcache_Encode(const char *szPlain, int len)
// Encodes as follows:
// \ -> \\, BS -> \b, hichars (and = and ") -> \xhh etc.
// hex 00-31 -> \x00..\x1f
// If len == -1, uses strlen(szPlain)
// Returns a string on the heap
// Creates a buffer on the heap, extending it by 'delta' byte increments
{
	if (!szPlain) return NULL;							// Someone's trying to trick us

	if (len < 0) len=strlen(szPlain);
	char *szResult = strdup("");
	int resultLen = 0;										// Total length of szResult
	const int delta=20;									// How many bytes of this extent used in buffer
	int sublen = delta;									// Size of this buffer segment (init to force alloc 1st time)

	while (len--) {
		char c=*szPlain++;
		const char *str="";
		char buf[5];

		switch (c) {
		case '\a':	str="\\a";	break;
		case '\b':	str="\\b";	break;
		case '\f':	str="\\f";	break;
		case '\n':	str="\\n";	break;
		case '\r':	str="\\r";	break;
		case '\t':	str="\\t";	break;
		case '\v':	str="\\v";	break;
		case '\\':	str="\\\\";	break;
		default:
			if (c < ' ' || c > '~' || c == '=' || c == '"') {	// Need to escape = as it's our separator, " as we may quote
				snprintf(buf, sizeof(buf), "\\x%02x", c & 0xff);
				str=buf;
			}
		}
		if (*str) {
			int slen=strlen(str);
			if (sublen+slen >= delta) {				// We're overflowing
				sublen=0;
				szResult = realloc(szResult, resultLen+delta);
			}
			memcpy(szResult+resultLen, str, slen);
			resultLen+=slen;
			sublen+=slen;
		} else {
			if (sublen == delta) {
				sublen=0;
				szResult = realloc(szResult, resultLen+delta);
			}
			sublen++;
			szResult[resultLen++]=c;
		}
	}
	if (sublen == delta) szResult = realloc(szResult, resultLen+1);
	szResult[resultLen]='\0';

	return szResult;
}

const char *rcache_Decode(char *szCoded)
// Decodes a string, replacing \x with whatever is necessary in situ.
// Always returns the same string passed.
{
	if (!szCoded || !strchr(szCoded, '\\')) return szCoded;		// Null or no escaped chars

	char *chp=szCoded;
	char *src=szCoded;
	char *dest=szCoded;

	while (chp=strchr(src, '\\')) {
		int done=0;

		if (dest != szCoded && chp != src) memmove(dest, szCoded, chp-src);
		dest+=chp-src;
		src=chp+2;
		switch (chp[1]) {
		case 'a': *dest++='\a'; break;
		case 'b': *dest++='\b'; break;
		case 'f': *dest++='\f'; break;
		case 'n': *dest++='\n'; break;
		case 'r': *dest++='\r'; break;
		case 't': *dest++='\t'; break;
		case 'v': *dest++='\v'; break;
		case 'x': {
					done=1;					// Remains set if we don't have two chars after the \x
					int a=chp[2];
					if (a) {
						src++;
						int b=chp[3];
						if (b) {
							src++;
							if (a>='a') a-=('a'-'A');
							if (b>='a') b-=('a'-'A');
							a-='0'; if (a > 9) a-='A'-'9'-1;
							b-='0'; if (b > 9) b-='A'-'9'-1;
							*dest++=(a<<4) | b;
							done=0;
						}
					}
				}
				break;
		case '\0': *dest++='\\';				// There's a \ at the end of the string
				src--;
				done=1;
				break;
		default: *dest++=chp[1]; break;
		}
		if (done) break;						// Drop out as escaping is bad
	}

	if (src != dest) memmove(dest, src, strlen(src)+1);

	return szCoded;
}

STATIC pooldata *rcache_PoolData(const char *szPool)
{
	return (pools && szPool) ? (pooldata*)simap_GetValue(pools, szPool, 0)
							 : NULL;
}

STATIC keydata *rcache_KeyData(const char *szPool, const char *szKey)
{
	pooldata *pd = rcache_PoolData(szPool);

	return pd ? (keydata*)simap_GetValue(pd->keys, szKey, 0)
			  : NULL;
}

STATIC void kd_Init(keydata *kd)
{
	if (kd) {
		kd->filetime=(time_t)0;
		strcpy(kd->szRefreshed, "0000-00-00 00:00:00");
		strcpy(kd->szBestBefore, "0000-00-00 00:00:00");
		strcpy(kd->szTryAgain, "0000-00-00 00:00:00");
		strcpy(kd->szEatBy, "0000-00-00 00:00:00");
		kd->nRetry=0;
		kd->map = ssmap_New();
	}
}

STATIC keydata *rcache_NewKeyData(const char *szPool, const char *szKey)
// NB. Although this is called 'new', it will return an existing one if it's there already
// Returns NULL only if szPool doesn't exist or szKey is NULL
{
	pooldata *pd = rcache_PoolData(szPool);
	keydata *kd = NULL;

	if (pd) {
		kd = (keydata*)simap_GetValue(pd->keys, szKey, 0);
		if (!kd) {
			kd = (keydata*)malloc(sizeof(keydata));
			kd_Init(kd);

			if (!pd->keys) pd->keys=simap_New();
			simap_Add(pd->keys, szKey, (int)kd);
		}
	}

	return kd;
}

STATIC const char *rcache_PoolDir(const char *szPool)
{
	pooldata *pd = rcache_PoolData(szPool);

	return pd ? pd->szDir : szBaseDir;
}

STATIC const char *rcache_KeyFile(const char *szPool, const char *szKey)
// Returns the file that holds the data for this key
{
	static const char *result = NULL;
	szDelete(result);
	char nasty=0;							// 0 means key ok for a filename, 1 means it isn't

	if (strlen(szKey) < 40) {
		const char *chp=szKey;
		char c;

		while (c=*chp++) {
			if (c <= ' ' || c >= '~' || strchr("*/\\[]{}()`'\"", c)) {
				nasty=1;
				break;
			}
		}
	} else {
		nasty=1;
	}

	if (nasty) {
		result = hprintf(NULL, "%s/%s", rcache_PoolDir(szPool), guid_ToText(guid_FromSeed(szKey, -1)));
	} else {
		result = hprintf(NULL, "%s/%s", rcache_PoolDir(szPool), szKey);
	}

	return result;
}

API void rcache_SetDir(const char *szDir)
{
	szDelete(szBaseDir);
	szBaseDir = szDir ? strdup(szDir) : NULL;
}

API void rcache_SetStale(int nBestBefore, int nEatBy, int nRetry)
// Intended to be called by callback functions to set the expiry dates of the cache
// The best before, eat by and retry periods should be specified in seconds.
{
	if (!callback_kd) return;			// Not being called by a callback function

	bSetStaleCalled = 1;

	time_t now=time(NULL);

	strcpy(callback_kd->szBestBefore, rcache_TimeToText(now + nBestBefore));
	strcpy(callback_kd->szTryAgain, callback_kd->szBestBefore);
	strcpy(callback_kd->szEatBy, rcache_TimeToText(now + nEatBy));
	callback_kd->nRetry = nRetry;

	return;
}

STATIC void rcache_AddPool(const char *szPool, rcache_callback_map cbmap, rcache_callback_str cbstr, const char *szDir)
//  void rcache_AddPool(const char *szPool, SSMAP* (*cb)(const char *szKey), const char *szDir)
// Add a new pool with its callback function
// If one already exists with the same name, it'll be overwritten and we'll lose a little memory - not worried.
{
	if (!szPool || (!cbmap && !cbstr)) return;

	if (!pools) {
		pools=simap_New();
	}

	pooldata *pd = (pooldata*)malloc(sizeof(pooldata));
	pd->cbmap = cbmap;
	pd->cbstr = cbstr;
	pd->keys = NULL;
	pd->szDir = (szDir && *szDir == '/') ? strdup(szDir)
										 : hprintf(NULL, "%s/%s", szBaseDir ? szBaseDir
																			: RCACHE_BASEDIR,
												 szDir ? szDir
													   : szPool);
	pd->separator = '\0';

	simap_Add(pools, szPool, (int)pd);

	return;
}

API void rcache_AddPoolMap(const char *szPool, rcache_callback_map cb, const char *szDir)
{
	rcache_AddPool(szPool, cb, NULL, szDir);
}

API void rcache_AddPoolStr(const char *szPool, rcache_callback_str cb, const char *szDir)
{
	rcache_AddPool(szPool, NULL, cb, szDir);
}

API SSMAP *rcache_GetMap(const char *szPool, const char *szKey)
// Returns the current map for the pool/key - using the cache, callback as necessary
{
	// Catch stupid calls and return a blank
	if (!pools || !szPool || !szKey) return NULL;

	pooldata *pd = rcache_PoolData(szPool);
	if (!pd) return NULL;									// Pool doesn't exist

	// Get the keydata block for this key, making a blank one if it doesn't yet exist
	keydata *kd = rcache_NewKeyData(szPool, szKey);

	// Get the name of the file that holds the persistent copy of the cache
	const char *szKeyFile = rcache_KeyFile(szPool, szKey);

	// We will need the current time as a string whatever we do beyond here so set it now
	time_t now = time(NULL);
	char szNow[20];
	strcpy(szNow, rcache_TimeToText(now));

	// Attempt to update the pool from file if the file is newer than the memory copy
	struct stat st;
	if (stat(szKeyFile, &st) || st.st_mtime >= kd->filetime) {	// No file, or file is newer than last time we read it
		FILE *fp = fopen(szKeyFile, "r+");						// Need the + to be able to lock it

		ssmap_Delete(kd->map);
		kd->map=NULL;

		if (fp) {											// It's a new file so update everything from it
			kd->map = ssmap_New();
			int err=lockf(fileno(fp), F_LOCK, 0);			// Maybe this should be F_TLOCK, but what then?
			char *line;

			while (line = (char*)hReadFileLine(fp)) {
				char *eq = strchr(line, '=');
				if (eq) {
					*eq++='\0';
					if (!strncmp(line, "cache_", 6)) {			// cache_ lines go into the keydata struct not the map
						if (!stricmp(line+6, "Refreshed")) {
							strncpy(kd->szRefreshed, eq, sizeof(kd->szRefreshed));
						} else if (!stricmp(line+6, "BestBefore")) {
							strncpy(kd->szBestBefore, eq, sizeof(kd->szBestBefore));
						} else if (!stricmp(line+6, "TryAgain")) {
							strncpy(kd->szTryAgain, eq, sizeof(kd->szTryAgain));
						} else if (!stricmp(line+6, "EatBy")) {
							strncpy(kd->szEatBy, eq, sizeof(kd->szEatBy));
						} else if (!stricmp(line+6, "Retry")) {
							kd->nRetry=atoi(eq);
						}
					} else {
						ssmap_Add(kd->map, rcache_Decode(line), rcache_Decode(eq));
					}
				}
			}
			fclose(fp);

			kd->filetime=now;
		} else {
			kd_Init(kd);		// No file (or we can't read it) so forget anything we have in memory
		}
	}

	// If we're not at cache_BestBefore yet then return map
	if (strcmp(szNow, kd->szBestBefore) <= 0)
		return kd->map;

// If now > cache_TryAgain or failed to fetch from file
//    Try to fetch from wherever we get things from (LDAP, DNS etc.), replacing map if successful
//      If successful:
//        Replace the map with the new one fetched, setting cache_BestBefore and cache_EatBy to future values
//    If not successful, set cache_TryAgain to now + whatever-period
//    Write the map back to the file

// Note that the callback function that's called might fork() for efficiency.  In this case, the parent will
// immediately return with the existing string (if there is one) and the child will go and do whatever it needs
// to do to get the value/map to return.  We process as normal for the parent, but detect the child and only
// write the cache file back then exit.  This cache file will be picked up on the next call.

	if (!kd->map || strcmp(szNow, kd->szTryAgain) >= 0) {
		callback_kd = kd;							// So rcache_SetStale() knows what to look at
		SSMAP *newmap = NULL;
		char bIsChild = 0;							// Special processing if callback forks
		int parent_pid = getpid();					// Remember who we are so we know if a child returns from callback
		bSetStaleCalled = 0;						// Reset by the callback if it calls rcache_SetStale()

		if (pd->cbmap) {							// It's a map-style callback
			newmap = (*pd->cbmap)(szKey, kd->map);
		} else {									// Value-only callback so pull any existing value out...
			const char *szValue = (*pd->cbstr)(szKey, ssmap_GetValue(kd->map, "value"));
			if (szValue) {
				newmap = ssmap_New();				// ... and create a map to put the value into
				ssmap_Add(newmap, "value", szValue);
			}
		}
		bIsChild = getpid() != parent_pid;			// callback forked and we're the child

		if (newmap) {								// Hurrah - we've successfully fetched a fresh map
			if (!bSetStaleCalled)					// Provide default stale parameters if none set by callback
				rcache_SetStale(1*24*60*60, 7*24*60*60, 10*60);		// Default BB=1 day, EB=1 week, RT=10 mins

			strncpy(kd->szRefreshed, rcache_TimeToText(time(NULL)), sizeof(kd->szRefreshed));

			if (kd->map != newmap) {				// Don't switch if callback simply returned the existing map
				ssmap_Delete(kd->map);
				kd->map=newmap;
			}
		} else {
			if (bIsChild) exit(0);					// No result from child callback so nothing to do
			strcpy(kd->szTryAgain, rcache_TimeToText(now+kd->nRetry));
		}
		callback_kd = NULL;

		rcache_MkDir(pd->szDir);					// Ensure the pool directory exists

		FILE *fp = fopen(szKeyFile, "w");			// Write the cache file out
		if (fp) {
			lockf(fileno(fp), 0, 0);

			const char *szEKey = rcache_Encode(szKey, -1);	// Put the key at the top so we recognise the file
			fprintf(fp, "cache_Key=%s\n", szEKey);
			szDelete(szEKey);
			fprintf(fp, "cache_Refreshed=%s\n", kd->szRefreshed);

			if (kd->map) {								// Unfortunately ssmap_GetNextEntry() isn't NULL safe
				const char *szKey;
				const char *szValue;

				while (ssmap_GetNextEntry(kd->map, &szKey, &szValue)) {
					if (strncmp(szKey, "cache_", 6)) {	// In case something sneaks one in, we don't want confusing
						const char *szEKey = rcache_Encode(szKey, -1);
						const char *szEValue = rcache_Encode(szValue, -1);

						fprintf(fp,"%s=%s\n", szEKey, szEValue);
						szDelete(szEKey);
						szDelete(szEValue);
					}
				}
			}

			fprintf(fp, "cache_BestBefore=%s\n", kd->szBestBefore);
			fprintf(fp, "cache_TryAgain=%s\n", kd->szTryAgain);
			fprintf(fp, "cache_EatBy=%s\n", kd->szEatBy);
			fprintf(fp, "cache_Retry=%d\n", kd->nRetry);

			fclose(fp);
			kd->filetime=now;
		}

		if (bIsChild) exit(0);		// We have got a background result from the callback, written in - our work is done.
	}

// If now < cache_EatBy then return map
	if (strcmp(szNow, kd->szEatBy) <= 0) {
		return kd->map;
	}

// return NULL
	return NULL;
}

API const char *rcache_Get(const char *szPool, const char *szKey, const char *szName)
// E.g. rcache_Get("dns", "microtest.co.uk", "ip")
{
	SSMAP *map = rcache_GetMap(szPool, szKey);

	return map ? ssmap_GetValue(map, szName) : NULL;
}

API const char *rcache_GetValue(const char *szPool, const char *szKey)
{
	return rcache_Get(szPool, szKey, "value");
}
