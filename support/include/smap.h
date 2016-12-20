
#ifndef __SMAP_H
#define __SMAP_H

// Three 'classes' to map strings to random data, strings or integers.
// They prefix with smap, ssmap or simap respectively and work as follows:
// New()				- returns a structure pointer that must be passed to all the other functions
// Delete(SMAP*)		- Tidies up the structure, releases memory etc.
// CopyKeys(sm, n)		- Call with a 1 to have smap copy Key values, otherwise it'll keep pointers to them
// CopyValues(sm,n)		- Controls whether smap keeps copies of values or pointers (def. is 0 for smap, 1 for others)
// Add(sm, key, value)	- Adds a value to the map (smap also requires a 'len' parameter)
// GetValue(sm, key)	- Returns the value that was added with this key (simap requires a default value)
// GetKey(sm, value)	- Returns the key that was used to add the value (or one of them if not unique)
// DeleteKey(sm, key)	- Deletes a previously added key
// GetNextEntry(sm, pkey, pvalue)	- Returns successive keys and values
// Reset(sm)			- Resets the iterator after GetNextEntry()

// These three structures MUST be identical
// I seemed to have to explicitely declare them three times to achieve the type safety between SMAP, SSMAP and SIMAP

typedef struct SMAP {
	unsigned long nMask;						// Mask for mapping hash onto arena
	int nCount;									// Number of entries
	int nArena;									// Size of arena
	int nIndex;									// Used for iterating
	unsigned char bAllocKeys;					// 1 if keys are malloced
	unsigned char bAllocValues;					// 1 if values are malloced
	unsigned char bIgnoreCase;					// 1 to ignore case of keys
	const char **pKey;
	const char **pValue;
	int *sorted;								// Sorted index of values
	int(*sorter)(const char *, const char *);	// Sort function
	char sortValues;							// When sorting, sort on values instead of keys
} SMAP;

typedef struct SSMAP {
	unsigned long nMask;						// Mask for mapping hash onto arena
	int nCount;									// Number of entries
	int nArena;									// Size of arena
	int nIndex;									// Used for iterating
	unsigned char bAllocKeys;					// 1 if keys are malloced
	unsigned char bAllocValues;					// 1 if values are malloced
	unsigned char bIgnoreCase;					// 1 to ignore case of keys
	const char **pKey;
	const char **pValue;
	int *sorted;								// Sorted index of values
	int(*sorter)(const char *, const char *);	// Sort function
} SSMAP;

typedef struct SPMAP {
	unsigned long nMask;						// Mask for mapping hash onto arena
	int nCount;									// Number of entries
	int nArena;									// Size of arena
	int nIndex;									// Used for iterating
	unsigned char bAllocKeys;					// 1 if keys are malloced
	unsigned char bAllocValues;					// 1 if values are malloced
	unsigned char bIgnoreCase;					// 1 to ignore case of keys
	const char **pKey;
	const char **pValue;
	int *sorted;								// Sorted index of values
	int(*sorter)(const char *, const char *);	// Sort function
} SPMAP;

typedef struct SIMAP {
	unsigned long nMask;						// Mask for mapping hash onto arena
	int nCount;									// Number of entries
	int nArena;									// Size of arena
	int nIndex;									// Used for iterating
	unsigned char bAllocKeys;					// 1 if keys are malloced
	unsigned char bAllocValues;					// 1 if values are malloced
	unsigned char bIgnoreCase;					// 1 to ignore case of keys
	const char **pKey;
	const char **pValue;
	int *sorted;								// Sorted index of values
	int(*sorter)(const void *, const void *);	// Sort function
} SIMAP;

#define SSET	SIMAP

// Functions to map to pointers

void smap_Delete(SMAP *sm);
SMAP *smap_New();
int smap_CopyKeys(SMAP *sm, int bValue);
int smap_CopyValues(SMAP *sm, int bValue);
int smap_Add(SMAP *sm, const char *szKey, const char *szValue, int nLen);
const char *smap_GetValue(SMAP *sm, const char *szKey);
const char *smap_GetKey(SMAP *sm, const char *szValue, int nLen);
int smap_DeleteKey(SMAP *sm, const char *szKey);
void smap_Reset(SMAP *sm);
int smap_GetNextEntry(SMAP *sm, const char **pKey, const char **pValue);
int smap_Count(SMAP *sm);
void smap_Clear(SMAP *sm);
void smap_Sort(SMAP *sm, int (*sorter)(const char *, const char *));
void smap_SortValues(SMAP *sm, int (*sorter)(const char *, const char *));
const char *smap_GetKeyAtIndex(SMAP *sm, int index);

// Corresponding functions to map to strings

void ssmap_Delete(SSMAP *sm);
SSMAP *ssmap_New();
int ssmap_CopyKeys(SSMAP *sm, int bValue);
int ssmap_CopyValues(SSMAP *sm, int bValue);
int ssmap_Add(SSMAP *sm, const char *szKey, const char *szValue);
const char *ssmap_GetValue(SSMAP *sm, const char *szKey);
const char *ssmap_GetKey(SSMAP *sm, const char *szValue);
int ssmap_DeleteKey(SSMAP *sm, const char *szKey);
void ssmap_Reset(SSMAP *sm);
int ssmap_GetNextEntry(SSMAP *sm, const char **pKey, const char **pValue);
int ssmap_Count(SSMAP *sm);
void ssmap_Clear(SSMAP *sm);
void ssmap_Sort(SSMAP *sm, int (*sorter)(const char *, const char *));
void ssmap_SortValues(SSMAP *sm, int (*sorter)(const char *, const char *));
const char *ssmap_GetKeyAtIndex(SSMAP *sm, int index);
const char *ssmap_GetValueAtIndex(SSMAP *sm, int index);

// Corresponding functions to map to pointers
void spmap_Delete(SPMAP *sm);
int spmap_CopyKeys(SPMAP *sm, int bValue);
void *spmap_GetValue(SPMAP *sm, const char *szKey);
int spmap_DeleteKey(SPMAP *sm, const char *szKey);
void spmap_Reset(SPMAP *sm);
void spmap_Clear(SPMAP *sm);
void spmap_Sort(SPMAP *sm, int (*sorter)(const char *, const char *));
void spmap_SortValues(SPMAP *sm, int (*sorter)(const char *, const char *));
const char *spmap_GetKeyAtIndex(SPMAP *sm, int index);
void *spmap_GetValueAtIndex(SPMAP *sm, int index);
int spmap_GetNextEntry(SPMAP *sm, const char **pKey, void **pValue);
SPMAP *spmap_New();
int spmap_Add(SPMAP *sm, const char *szKey, void *ptr);
const char *spmap_GetKey(SPMAP *sm, void *ptr);
int spmap_Count(SPMAP *sm);

// Corresponding functions to map to integers

void simap_Delete(SIMAP *sm);
SIMAP *simap_New();
int simap_CopyKeys(SIMAP *sm, int bValue);
int simap_CopyValues(SIMAP *sm, int bValue);
int simap_Add(SIMAP *sm, const char *szKey, int nValue);
int simap_GetValue(SIMAP *sm, const char *szKey, int nDefault);
const char *simap_GetKey(SIMAP *sm, int nValue);
int simap_DeleteKey(SIMAP *sm, const char *szKey);
void simap_Reset(SIMAP *sm);
int simap_GetNextEntry(SIMAP *sm, const char **pKey, int *pValue);
int simap_Count(SIMAP *sm);
void simap_Clear(SIMAP *sm);
void simap_Sort(SIMAP *sm, int (*sorter)(const char *, const char *));
void simap_SortValues(SIMAP *sm, int (*sorter)(const char *, const char *));
const char *simap_GetKeyAtIndex(SIMAP *sm, int index);
int simap_GetValueAtIndex(SIMAP *sm, int index);

// And functions for a set of strings

SSET *sset_New();
void sset_Delete(SSET *set);
int sset_Add(SSET *set, const char *string);
int sset_Remove(SSET *set, const char *string);
void sset_Reset(SSET *set);
void sset_Clear(SSET *set);
void sset_Sort(SSET *set, int (*sorter)(const char *, const char *));
int sset_GetNextEntry(SSET *set, const char **pKey);
const char *sset_GetStringAtIndex(SSET *set, int index);
int sset_IsMember(SSET *set, const char *string);
int sset_Count(SSET *set);

int ssmap_Write(SSMAP *map, const char *filename, const char *comment);
int simap_Write(SIMAP *map, const char *filename, const char *comment);
SSMAP *ssmap_ReadFile(const char *filename);

#endif
