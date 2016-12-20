
// 29-11-09 RJ 0.00 Map strings to data
// 04-12-09 RJ 0.01 Added in 'smap_Count()' as it seems to be an obvious omission
// 15-02-14 RJ 0.02 29422 Added sorting (smap_Sort())
// 15-02-14 RJ 0.03 29423 Added sset functions to support a set of strings
// 10-08-15 RJ 0.04 added in ssmap_Write() functionality

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>

#include "heapstrings.h"

#define STATIC //static
#define API

// A hash mapping of strings onto data.
// The SMAP structure is given in smap.h and handles an arena of keys, which are numbered 1..n for the API.
// Entries in the pKey array are NULL for unallocated, 1 (SM_DELETED) for deleted or a string pointer
// bAllocKeys and bAllocValues indicate whether smap takes copies of the keys and values or not.

// If you want to debug this, look down the bottom for a 'xmain()' function, rename it and #if it back in

// TODO: Consider removing 'nMask' as it is always 'nArena-1' and the cost of calculating this are small
//       Particularly if smap_Hash() was changed to include the arena size as a parameter

#include "smap.h"

#define SM_DELETED	((const char *)1)

STATIC unsigned long smap_Hash(const char *szKey, int ignoreCase)
// Returns a hash value for the key
// If 'ignoreCase', we take a copy of the key and lowercase it before use
{
	if (!szKey || !*szKey) return 0;		// Blank or NULL simply returns 0

	int nLen=strlen(szKey);					// Will never be zero
	unsigned long nHash = nLen;
	unsigned long tmp;
	int nRem;
	const char *freeKey = NULL;				// Set to something to free if we need to

	nRem = nLen & 3;
	nLen >>= 2;

	if (ignoreCase) {
		szKey = strdup(szKey);
		freeKey = szKey;
		strlwr(szKey);
	}

	for (;nLen > 0; nLen--) {
		nHash += (szKey[0]<<16) + szKey[1];
		tmp = (((szKey[2]<<16) + szKey[3]) << 11) ^ nHash;
		nHash = (nHash << 16) ^ tmp;
		szKey += 4;
		nHash += nHash >> 11;
	}

	switch (nRem) {
		case 3: nHash += (szKey[0]<<16) + szKey[1];
				nHash ^= nHash << 16;
				nHash ^= szKey[2] << 18;
				nHash += nHash >> 11;
				break;
		case 2: nHash += (szKey[0]<<16) + szKey[1];
				nHash ^= nHash << 11;
				nHash += nHash >> 17;
				break;
		case 1: nHash += *szKey;
				nHash ^= nHash << 10;
				nHash += nHash >> 1;
	}

	/* Force "avalanching" of final 127 bits */
	nHash ^= nHash << 3;
	nHash += nHash >> 5;
	nHash ^= nHash << 4;
	nHash += nHash >> 17;
	nHash ^= nHash << 25;
	nHash += nHash >> 6;

	if (freeKey)
		free((char*)freeKey);

	return nHash;
}

STATIC void smap_Expand(SMAP *sm)
// Double the arena size, which means re-adding all current values
// NOT NULL SAFE (but only caller, smap_Add(), is)
{
	int nMask;
	int nArena;
	const char **pKey;
	const char **pValue;
	int i;

	nArena=sm->nArena*2;
	nMask=(sm->nMask<<1) | 1;
	pKey=(const char **)calloc(nArena, sizeof(*pKey));
	pValue=(const char **)calloc(nArena, sizeof(*pValue));

	for (i=0;i<sm->nArena;i++) {
		const char *szKey=sm->pKey[i];

		if (szKey && szKey != SM_DELETED) {
			unsigned long nHash=smap_Hash(szKey, sm->bIgnoreCase) & nMask;
			while (pKey[nHash]) {
				nHash++;
				if (nHash >= nArena) nHash=0;
			}
			pKey[nHash]=szKey;
			pValue[nHash]=sm->pValue[i];
		}
	}
	free(sm->pKey);
	free(sm->pValue);
	sm->pKey=pKey;
	sm->pValue=pValue;
	sm->nArena=nArena;
	sm->nMask=nMask;
}

STATIC int smap_GetValueIndex(SMAP *sm, const char *szKey)
// Gets the index (1..nArena) for the key.
// This is the existing slot if it's there already or a blank one
{
	if (!sm || !szKey) return 0;					// Has side effect of making _GetValue and _DeleteKey NULL safe

	unsigned long nHash=smap_Hash(szKey, sm->bIgnoreCase) & sm->nMask;

	int (*cmp)(const char *a, const char *b) = sm->bIgnoreCase ? strcasecmp : strcmp;

	int count=0;

	while (sm->pKey[nHash] && (sm->pKey[nHash] == SM_DELETED || (*cmp)(sm->pKey[nHash], szKey))) {
		nHash++;
		if (nHash >= sm->nArena) nHash=0;
		count++;
		if (count >= sm->nArena) {					// Haven't found, and arena is full
			smap_Expand(sm);
			return smap_GetValueIndex(sm, szKey);
		}
	}

	return sm->pKey[nHash] ? (nHash+1) : 0;
}

STATIC int smap_GetKeyIndex(SMAP *sm, const char *szValue, int nLen)
// Returns the index of a given value - note this is not efficient!
// If AllocValues has been set or nLen > 0 then a content comparison is done, otherwise a location check
// I.e. If not AllocValues and nLen==0, then we search for a matching pointer
// NB. If AllocValues is set and nLen=0, we match the first value!
// NB. This is a linear search of the values and hence is not efficient at all...
{
	int i;
	int bSearchValue = sm->bAllocValues || nLen;

	for (i=0;i<sm->nArena;i++) {
		if (sm->pKey[i]) {
			if (bSearchValue) {
				if (!memcmp(sm->pValue[i], szValue, nLen))
					return i+1;
			} else {
				if (sm->pValue[i] == szValue) {
					return i+1;
				}
			}
		}
	}

	return 0;
}

// Sorting...
// A separate array ('sorted') provides a sort index.  This is only generated when smap_NextIndex() or
// smap_GetKeyAtIndex() are called such that thay returns the next index in sorted order.
// This only occurs if the application has set a sorter (such as 'strcmp') using smap_Sort()
// The sorted index is dropped whenever the map is changed (add/delete)
//
STATIC void smap_Unsort(SMAP *sm)
// Drop any previous sorting
// This is called from any function that changes the map
{
	if (sm->sorted) {
		free((void*)sm->sorted);
		sm->sorted=NULL;
	}
}

static SMAP *_cmp_sm;

STATIC int smap_Cmp(const void *a, const void *b)
{
	if (_cmp_sm->sortValues)
		return _cmp_sm->sorter(_cmp_sm->pValue[*(int*)a], _cmp_sm->pValue[*(int*)b]);
	else
		return _cmp_sm->sorter(_cmp_sm->pKey[*(int*)a], _cmp_sm->pKey[*(int*)b]);
}

STATIC void _smap_Sort(SMAP *sm)
// Sort the map, creating the 'sorted' array if we have a sorter
// The 'sorted' array has nCount entries, pointing to the active entries in the map.
// If 'sm->sorter' is defined then the array is sorted using it, otherwise the order is essentially random.
{
	if (!sm->sorted) {						// We have a sorting method and it's not currently sorted
		sm->sorted = (int*)malloc(sizeof(int)*sm->nCount);
		int i;
		int p = 0;
		for (i=sm->nCount-1;i>=0;i--) {		// Reverse order but it's subtly quicker
			while (sm->pKey[p] == NULL || sm->pKey[p] == SM_DELETED)
				p++;
			sm->sorted[i]=p++;
		}

		if (sm->sorter) {
			_cmp_sm = sm;
			qsort(sm->sorted, sm->nCount, sizeof(int), smap_Cmp);
		}
	}
}

STATIC int smap_DeleteAt(SMAP *sm, int nIndex)
// Returns -1	Index invalid
//			0	Already deleted
//			1	Deleted
{
	if (nIndex >= 1 && nIndex <= sm->nArena) {
		nIndex--;

		if (sm->pKey[nIndex] == NULL) {					// Never been allocated
			return -1;
		}
		if (sm->pKey[nIndex] == SM_DELETED) {			// Already been deleted (perhaps a different key though?)
			return 0;
		}
		if (sm->bAllocKeys) free((void*)sm->pKey[nIndex]);
		if (sm->bAllocValues && sm->pValue[nIndex]) free((void*)sm->pValue[nIndex]);
		sm->pKey[nIndex]=SM_DELETED;
		sm->nCount--;
		smap_Unsort(sm);
		return 1;
	} else {
		return -1;
	}
}

STATIC const char *smap_GetKeyAt(SMAP *sm, int nIndex)
{
	nIndex--;
	if (nIndex < 0 || nIndex >= sm->nArena || sm->pKey[nIndex] == NULL || sm->pKey[nIndex] == SM_DELETED)
		return NULL;
	return sm->pKey[nIndex];
}

STATIC const char *smap_GetValueAt(SMAP *sm, int nIndex, const char *szDefault)
{
	nIndex--;
	if (nIndex < 0 || nIndex >= sm->nArena || sm->pKey[nIndex] == NULL || sm->pKey[nIndex] == SM_DELETED)
		return szDefault;
	return sm->pValue[nIndex];
}

STATIC int smap_GetNextIndex(SMAP *sm)
// Uses the index built by the sort function (even though it may not be sorted)
{
	int nIndex = sm->nIndex++;

	if (nIndex >= sm->nCount) {				// Reached the end of the line (also catches nCount == 0)
		sm->nIndex=0;
		return 0;
	}

	if (!sm->sorted) _smap_Sort(sm);
	return sm->sorted[nIndex]+1;
}

STATIC void smap_Init(SMAP *sm)
{
	sm->nCount=0;
	sm->nArena=8;				// Initial arena size MUST be a power of 2
	sm->nMask=sm->nArena-1;
	sm->nIndex=0;
	sm->bAllocKeys=1;			// Default to allocating keys
	sm->bAllocValues=0;
	sm->bIgnoreCase=0;			// 1 to ignore case on keys
	sm->pKey=(const char **)calloc(sm->nArena, sizeof(*sm->pKey));
	sm->pValue=(const char **)calloc(sm->nArena, sizeof(*sm->pValue));
	sm->sorter=NULL;
	sm->sorted=NULL;
}

STATIC void smap_Uninit(SMAP *sm)
// Undoes the Initialisation by tidying up the structure
{
	int i;
	int bAllocKeys=sm->bAllocKeys;
	int bAllocValues=sm->bAllocValues;

	smap_Unsort(sm);
	if (bAllocKeys || bAllocValues) {
		for (i=0;i<sm->nArena;i++) {
			const char *szKey=sm->pKey[i];

			if (szKey && szKey != SM_DELETED) {
				if (bAllocKeys) free((void*)szKey);
				if (bAllocValues && sm->pValue[i]) free((void*)sm->pValue[i]);
			}
		}
	}
	free((void*)sm->pKey);
	free((void*)sm->pValue);
}

API int smap_IgnoreCase(SMAP *sm, int ignoreCase)
// Sets/resets the ignore case flag which affects key matching, returning the previous state.
// Always call this BEFORE adding anything to the map, otherwise weird stuff will happen.
{
	int result = 0;

	if (sm) {
		result = sm->bIgnoreCase;
		sm->bIgnoreCase = ignoreCase;
	}

	return result;
}

STATIC const char *smap_Encode(const char *szPlain, int len)
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

STATIC const char *smap_Decode(char *szCoded)
// Decodes a string, replacing \x with whatever is necessary in situ.
// Always returns a pointer to the string passed, which has been decoded in situ.
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

STATIC FILE *smap_WriteOpen(const char *filename, const char *comment)
{
	FILE *fp = fopen(filename, "w");
	if (fp) {
		time_t now = time(NULL);
		struct tm *tm = localtime(&now);

		fprintf(fp, "# MAP file written %02d-%02d-%04d at %02d:%02d:%02d\n",
				tm->tm_mday, tm->tm_mon+1, tm->tm_year+1900,
				tm->tm_hour, tm->tm_min, tm->tm_sec);
		if (comment)
			fprintf(fp, "# %s\n", comment);
		fprintf(fp, "\n");
	}

	return fp;
}

STATIC void smap_WriteClose(FILE *fp)
{
	if (fp) fclose(fp);
}

static char *_smap_LineBuffer = NULL;

STATIC int smap_ReadFileEntry(FILE *fp, const char **name, const char **value)
// Reads a name=value line from a file
// Call with a NULL 'fp' to simply clear down the memory used.
// Returns	1	Ok (*name, *value contain the two strings)
//			0	End of file
{
	if (_smap_LineBuffer) {
		free((void*)_smap_LineBuffer);
		_smap_LineBuffer = NULL;
	}

	if (fp) {
		for (;;) {
			if (_smap_LineBuffer)
				free((void*)_smap_LineBuffer);

			_smap_LineBuffer = (char*)hReadFileLine(fp);

			if (_smap_LineBuffer) {
				if (!*_smap_LineBuffer || *_smap_LineBuffer == '#')
					continue;					// Comment line
				char *p = strchr(_smap_LineBuffer, '=');
				if (p) {
					*p = '\0';
					*name = _smap_LineBuffer;
					*value = p+1;
					break;
				} else {
					continue;					// Line with no '='
				}
			} else {
				break;							// End of file
			}
		}
	}

	return !!_smap_LineBuffer;
}

API SSMAP *ssmap_ReadFile(const char *filename)
{
	SSMAP *map = NULL;

	FILE *fp = fopen(filename, "r");
	if (fp) {
		map = ssmap_New();

		const char *name;
		const char *value;
		while (smap_ReadFileEntry(fp, &name, &value)) {
			ssmap_Add(map, name, value);
		}
		smap_ReadFileEntry(NULL, NULL, NULL);
		fclose(fp);
	}

	return map;
}

API int ssmap_Write(SSMAP *map, const char *filename, const char *comment)
{
	FILE *fp = smap_WriteOpen(filename, comment);

	if (fp) {
		const char *key;
		const char *value;

		ssmap_Reset(map);
		while (ssmap_GetNextEntry(map, &key, &value)) {
			const char *encKey = smap_Encode(key, -1);
			const char *encValue = smap_Encode(value, -1);

			fprintf(fp, "%s=%s\n", encKey, encValue);
			free((void*)encKey);
			free((void*)encValue);
		}
		smap_WriteClose(fp);
		return 1;
	} else {
		return 0;
	}
}

API int simap_Write(SIMAP *map, const char *filename, const char *comment)
{
	FILE *fp = smap_WriteOpen(filename, comment);

	if (fp) {
		const char *key;
		int value;

		simap_Reset(map);
		while (simap_GetNextEntry(map, &key, &value)) {
			const char *encKey = smap_Encode(key, -1);

			fprintf(fp, "%s=%d\n", encKey, value);
			free((void*)encKey);
		}
		smap_WriteClose(fp);
		return 1;
	} else {
		return 0;
	}
}

// API functions

API void smap_Delete(SMAP *sm)
{
	if (sm) {
		smap_Uninit(sm);
		free((void*)sm);
	}
}

API SMAP *smap_New()
{
	SMAP *sm=(SMAP*)malloc(sizeof(*sm));
	if (sm) smap_Init(sm);

	return sm;
}

API void smap_Clear(SMAP *sm)
{
	if (sm) {
		int bAllocKeys = sm->bAllocKeys;			// We want to keep these
		int bAllocValues = sm->bAllocValues;

		smap_Uninit(sm);
		smap_Init(sm);

		sm->bAllocKeys = bAllocKeys;
		sm->bAllocValues = bAllocValues;
	}
}

API int smap_CopyKeys(SMAP *sm, int bValue)
// Sets (or unsets) whether allocation of values is taken care of by the map
// Returns the previous value or -1 if an attempt is made to change the value when there is anything in the map
{
	int bOldValue = sm->bAllocKeys;

	if (bOldValue != bValue && sm->nCount) return -1;

	sm->bAllocKeys = bValue;

	return bOldValue;
}

API int smap_CopyValues(SMAP *sm, int bValue)
// Sets (or unsets) whether allocation of values is taken care of by the map
// Returns the previous value or -1 if an attempt is made to change the value when there is anything in the map
{
	int bOldValue = sm->bAllocValues;

	if (bOldValue != bValue && sm->nCount) return -1;

	sm->bAllocValues = bValue;

	return bOldValue;
}

API void smap_Sort(SMAP *sm, int (*sorter)(const char *, const char *))
// Call with a function such as 'strcmp' to have smap_GetNextEntry() and smap_GetKeyAtIndex() return keys in
// the order dictated by the function.  Call with NULL to have them return values in essentially a random order.
{
	sm->sorter = sorter;
	sm->sortValues = 0;
}

API void smap_SortValues(SMAP *sm, int (*sorter)(const char *, const char *))
// Call with a function such as 'strcmp' to have smap_GetNextEntry() and smap_GetKeyAtIndex() return keys in
// the order dictated by the function.  Call with NULL to have them return values in essentially a random order.
{
	sm->sorter = sorter;
	sm->sortValues = 1;
}

API int smap_Add(SMAP *sm, const char *szKey, const char *szValue, int nLen)
// Add a new entry to the map, allocating and copying if necessary.
// Adding an existing item replaces the original.
// nLen is ignored unless bCopyValues(sm, 1) has been called.
// Returns 1 if the entry is new, 0 otherwise
// Returns -999 if sm is NULL (invalid)
{
	unsigned long nHash;
	const char *szFound;
	int nResult;

	if (!sm || !szKey) return -999;									// Returns -999 if passed a NULL map or key

	if (sm->nCount * 2 >= sm->nArena) smap_Expand(sm);

	nHash=smap_Hash(szKey, sm->bIgnoreCase) & sm->nMask;
	int (*cmp)(const char *a, const char *b) = sm->bIgnoreCase ? strcasecmp : strcmp;

	while (szFound=sm->pKey[nHash], szFound && szFound != SM_DELETED && (*cmp)(szFound, szKey)) {
		nHash++;
		if (nHash >= sm->nArena) nHash=0;
	}

	if (!szFound || szFound == SM_DELETED) {						// Need to set a new key
		sm->pKey[nHash]=sm->bAllocKeys ? strdup(szKey) : szKey;
		nResult=1;
		sm->nCount++;
		smap_Unsort(sm);
	} else {														// Adding an existing key
		if (sm->bAllocValues && sm->pValue[nHash]) free((void*)sm->pValue[nHash]);
		nResult=0;
	}

	if (sm->bAllocValues && szValue) {								// We allocate values and this one isn't NULL
		sm->pValue[nHash]=(const char *)malloc(nLen);				// Allocate the space
		memcpy((char *)sm->pValue[nHash], szValue, nLen);			// Copy it in
	} else {
		sm->pValue[nHash]=szValue;									// Simply store a pointer to it
	}

	return nResult;
}

API const char *smap_GetValue(SMAP *sm, const char *szKey)
// Get a value given a key (quick)
{
	unsigned long nHash=smap_GetValueIndex(sm, szKey);

	return nHash ? sm->pValue[nHash-1] : NULL;
}

API const char *smap_GetKey(SMAP *sm, const char *szValue, int nLen)
// Get a key given a value (slow)
// Note that this will compare the pointer to the data if CopyValues is 0 or the content if CopyValues is 1.
// Either way, it can only return the first matching key so if two keys have the same value (or pointer), only
// the first will be returned and this could be any of the ones actually used.
// E.g. smap_Add("abc", "xyz") then smap_Add("def","xyz"), calling smap_GetKey("xyz") could return either "abc" or "def"
{
	int nIndex=smap_GetKeyIndex(sm, szValue, nLen);
	return nIndex ? sm->pKey[nIndex-1] : NULL;
}

API const char *smap_GetKeyAtIndex(SMAP *sm, int index)
{
	if (!sm || index < 1 || index > sm->nCount) return NULL;

	if (!sm->sorted) _smap_Sort(sm);
	return sm->pKey[sm->sorted[index-1]];
}

API const char *smap_GetValueAtIndex(SMAP *sm, int index)
{
	if (!sm || index < 1 || index > sm->nCount) return NULL;

	if (!sm->sorted) _smap_Sort(sm);
	return sm->pValue[sm->sorted[index-1]];
}

API int smap_DeleteKey(SMAP *sm, const char *szKey)
// Deletes an entry given the key
// Returns 1 if the key was deleted, 0 if it wasn't found
{
	int nIndex = smap_GetValueIndex(sm, szKey);

	if (nIndex > 0) smap_DeleteAt(sm, nIndex);

	return !!nIndex;
}

API void smap_Reset(SMAP *sm)
// Reset to the start of the map prior to calling GetNextEntry()
{
	sm->nIndex=0;
}

API int smap_GetNextEntry(SMAP *sm, const char **pKey, const char **pValue)
// Gets the next key and/or value from the map.  If either (or both?) of pKey or pValue are NULL, they're not used.
// The map should be reset to start at the beginning then call this function repeatedly to get each value.
// NB. The order that entries are returned will be essentially random.
// Returns	1	The entry has been returned
//			0	There are no more entries left and it's left in the 'reset' state so the next call will return the first

{
	int nIndex = smap_GetNextIndex(sm);

	if (nIndex) {
		if (pKey) *pKey = sm->pKey[nIndex-1];
		if (pValue) *pValue = sm->pValue[nIndex-1];
	}

	return !!nIndex;
}

API int smap_Count(SMAP *sm)
{
	return sm->nCount;
}

// ssmap - string to string map (values are allocated)

API void ssmap_Delete(SSMAP *sm)									{ smap_Delete((SMAP*)sm); }
API int ssmap_CopyKeys(SSMAP *sm, int bValue)						{ return smap_CopyKeys((SMAP*)sm, bValue); }
API const char *ssmap_GetValue(SSMAP *sm, const char *szKey)		{ return smap_GetValue((SMAP*)sm, szKey); }
API int ssmap_DeleteKey(SSMAP *sm, const char *szKey)				{ return smap_DeleteKey((SMAP*)sm, szKey); }
API void ssmap_Reset(SSMAP *sm)										{ smap_Reset((SMAP*)sm); }
API void ssmap_Clear(SSMAP *sm)										{ smap_Clear((SMAP*) sm); }
API void ssmap_Sort(SSMAP *sm, int (*sorter)(const char *, const char *)) { smap_Sort((SMAP*) sm, sorter); }
API void ssmap_SortValues(SSMAP *sm, int (*sorter)(const char *, const char *)) { smap_SortValues((SMAP*) sm, sorter); }
API const char *ssmap_GetKeyAtIndex(SSMAP *sm, int index)			{ return smap_GetKeyAtIndex((SMAP*) sm, index); }
API const char *ssmap_GetValueAtIndex(SSMAP *sm, int index)			{ return smap_GetValueAtIndex((SMAP*) sm, index); }
API int ssmap_IgnoreCase(SSMAP *sm, int ignoreCase)					{ return smap_IgnoreCase((SMAP*)sm, ignoreCase); }

API int ssmap_GetNextEntry(SSMAP *sm, const char **pKey, const char **pValue)
{
	return smap_GetNextEntry((SMAP*)sm, pKey, pValue);
}

API SSMAP *ssmap_New()
{
	SMAP *sm = smap_New();
	smap_CopyValues(sm, 1);

	return (SSMAP*)sm;
}

API int ssmap_Add(SSMAP *sm, const char *szKey, const char *szValue)
{
	return smap_Add((SMAP*)sm, szKey, szValue, strlen(szValue)+1);
}

API const char *ssmap_GetKey(SSMAP *sm, const char *szValue)
{
	return smap_GetKey((SMAP*)sm, szValue, strlen(szValue)+1);
}

API int ssmap_Count(SSMAP *sm)
{
	return sm->nCount;
}

// spmap - string to pointer map

API void spmap_Delete(SPMAP *sm)									{ smap_Delete((SMAP*)sm); }
API int spmap_CopyKeys(SPMAP *sm, int bValue)						{ return smap_CopyKeys((SMAP*)sm, bValue); }
API void *spmap_GetValue(SPMAP *sm, const char *szKey)				{ return (void*)smap_GetValue((SMAP*)sm, szKey); }
API int spmap_DeleteKey(SPMAP *sm, const char *szKey)				{ return smap_DeleteKey((SMAP*)sm, szKey); }
API void spmap_Reset(SPMAP *sm)										{ smap_Reset((SMAP*)sm); }
API void spmap_Clear(SPMAP *sm)										{ smap_Clear((SMAP*) sm); }
API void spmap_Sort(SPMAP *sm, int (*sorter)(const char *, const char *)) { smap_Sort((SMAP*) sm, sorter); }
API void spmap_SortValues(SPMAP *sm, int (*sorter)(const char *, const char *)) { smap_SortValues((SMAP*) sm, sorter); }
API const char *spmap_GetKeyAtIndex(SPMAP *sm, int index)			{ return smap_GetKeyAtIndex((SMAP*) sm, index); }
API void *spmap_GetValueAtIndex(SPMAP *sm, int index)				{ return (void*)smap_GetValueAtIndex((SMAP*) sm, index); }
API int spmap_IgnoreCase(SPMAP *sm, int ignoreCase)					{ return smap_IgnoreCase((SMAP*)sm, ignoreCase); }

API int spmap_GetNextEntry(SPMAP *sm, const char **pKey, void **pValue)
{
	return smap_GetNextEntry((SMAP*)sm, pKey, (const char **)pValue);
}

API SPMAP *spmap_New()
{
	SMAP *sm = smap_New();
	smap_CopyKeys(sm, 1);
	smap_CopyValues(sm, 0);

	return (SPMAP*)sm;
}

API int spmap_Add(SPMAP *sm, const char *szKey, void *ptr)
{
	return smap_Add((SMAP*)sm, szKey, (const char *)ptr, 0);
}

API const char *spmap_GetKey(SPMAP *sm, void *ptr)
{
	return smap_GetKey((SMAP*)sm, (const char *)ptr, 0);
}

API int spmap_Count(SPMAP *sm)
{
	return sm->nCount;
}

// simap - string to int map

API void simap_Delete(SIMAP *sm)									{ smap_Delete((SMAP*)sm); }
API int simap_CopyKeys(SIMAP *sm, int bValue)						{ return smap_CopyKeys((SMAP*)sm, bValue); }
API int simap_DeleteKey(SIMAP *sm, const char *szKey)				{ return smap_DeleteKey((SMAP*)sm, szKey); }
API void simap_Reset(SIMAP *sm)										{ smap_Reset((SMAP*)sm); }
API void simap_Clear(SIMAP *sm)										{ smap_Clear((SMAP*) sm); }
API void simap_Sort(SIMAP *sm, int (*sorter)(const char *, const char *)) { smap_Sort((SMAP*) sm, sorter); }
API void simap_SortValues(SIMAP *sm, int (*sorter)(const char *, const char *)) { smap_SortValues((SMAP*) sm, sorter); }
API const char *simap_GetKeyAtIndex(SIMAP *sm, int index)			{ return smap_GetKeyAtIndex((SMAP*) sm, index); }
API int simap_GetValueAtIndex(SIMAP *sm, int index)					{ return (int)smap_GetValueAtIndex((SMAP*) sm, index); }

API int simap_GetNextEntry(SIMAP *sm, const char **pKey, int *pValue)
{
	return smap_GetNextEntry((SMAP*)sm, pKey, (const char **)pValue);
}

API SIMAP *simap_New()
{
	SMAP *sm=smap_New();
	smap_CopyValues(sm, 0);

	return (SIMAP*)sm;
}

API int simap_Add(SIMAP *sm, const char *szKey, int nValue)
{
	return smap_Add((SMAP*)sm, szKey, (const char *)nValue, sizeof(int));
}

API int simap_GetValue(SIMAP *sm, const char *szKey, int nDefault)
// Fetches an int from the map, returning nDefault if not found
{
	int nIndex=smap_GetValueIndex((SMAP*)sm, szKey);
	return nIndex ? (int)smap_GetValueAt((SMAP*)sm, nIndex, NULL) : nDefault;
}

API const char *simap_GetKey(SIMAP *sm, int nValue)
{
	return smap_GetKey((SMAP*)sm, (const char *)nValue, 0);
}

API int simap_Count(SIMAP *sm)
{
	return sm->nCount;
}

// sset - A set of strings

API SSET *sset_New()												{ return (SSET*)simap_New(); }
API void sset_Delete(SSET *set)										{ simap_Delete((SIMAP*)set); }
API int sset_Add(SSET *set, const char *string)						{ return simap_Add((SIMAP*)set, string, 1); }
API int sset_Remove(SSET *set, const char *string)					{ return simap_DeleteKey((SIMAP*)set, string); }
API void sset_Reset(SSET *set)										{ simap_Reset((SIMAP*)set); }
API void sset_Clear(SSET *set)										{ simap_Clear((SIMAP*) set); }
API void sset_Sort(SSET *set, int (*sorter)(const char *, const char *)) { simap_Sort((SIMAP*) set, sorter); }
API int sset_GetNextEntry(SSET *set, const char **pKey)				{ return simap_GetNextEntry((SIMAP*)set, pKey, NULL); }
API const char *sset_GetStringAtIndex(SSET *set, int index)			{ return simap_GetKeyAtIndex((SIMAP*)set, index); }
API int sset_IsMember(SSET *set, const char *string)				{ return simap_GetValue((SIMAP*)set, string, 0); }
API int sset_Count(SSET *set)										{ return simap_Count((SIMAP*) set); }

// This just for debugging...

#if 0

void smap_Dump(SMAP *sm)
{
	int i;

	printf("Arena=%d(%d), Strings=%d, Index=%d\n", sm->nArena, sm->nMask, sm->nCount, sm->nIndex);
	for (i=0;i<sm->nArena;i++) {
		if (sm->pKey[i]) {
			if (sm->pKey[i] == SM_DELETED) {
				printf("[%d] Deleted\n", i+1);
			} else {
				printf("[%d] '%s' -> '%s' (%p)\n", i+1, sm->pKey[i], sm->pValue[i], sm->pValue[i]);
			}
		}
	}
	if (sm->sorted) {
		printf("Sort order:");
		for (i=0;i<sm->nCount;i++) {
			printf(" %d", sm->sorted[i]);
		}
		printf("\n");
	} else {
		printf("Not sorted\n");
	}
}

void xmain(int argc, char *argv[])
{
	SIMAP *sim;
	const char *szResult;
	int nResult;
	int i;

	sim=simap_New();
	simap_CopyKeys(sim, 1);
	for (i=1;i<argc;i++) {
		simap_Add(sim, argv[i], i);
	}

	for (i=1;i<argc;i++) {
		nResult=simap_GetValue(sim, argv[i], -1);
		printf("Found '%s' = '%d'\n", argv[i], nResult);
	}

	printf("Key(3) = \"%s\"\n", simap_GetKey(sim, 3));
	simap_DeleteKey(sim, "c");
	printf("Key(3) = \"%s\"\n", simap_GetKey(sim, 3));

	for (i=1;i<argc;i++) {
		nResult=simap_GetValue(sim, argv[i], -1);
		printf("Found '%s' = '%d'\n", argv[i], nResult);
	}

	smap_Dump((SMAP*)sim);
	simap_Add(sim, "c", 16);
	smap_Dump((SMAP*)sim);
	simap_Add(sim, "c", 16);
	smap_Dump((SMAP*)sim);
	simap_Delete(sim);
}

#endif
