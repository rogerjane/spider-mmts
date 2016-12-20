
// A library to handle 'szz's, which are normal 'C' style 'sz' strings
// one after the other, allocated on the heap and ending in a blank
// string.

// E.g. "fred" and "jim" would be stored as:
//  f r e d 0 j i m 0 0
// Where '0' represents a zero byte
// These things are inherently read-only, which is why everything tends to
// get returned as 'const'...

#include <malloc.h>
#include <stdio.h>
#include <assert.h>
#include <string.h>

const char *szz_New()
// Returns one consisting of no strings
// On the whole not necessary as any of these functions will do sensible things when passed a NULL pointer
{
	return calloc(2,1);
}

void szz_Delete(const char *szz)
// Safely deletes an szz and safely ignores a NULL ptr
{
	if (szz) free((char*)szz);
}

int szz_IsEmpty(const char *szz)
// Returns true if passed a NULL or an empty szz
{
	return !szz || !*szz;
}

const char *szz_Next(const char *szz)
// Safely returns the next string after the one given or a blank one if there is no more
{
	return (szz && *szz) ? szz+strlen(szz)+1 : szz;
}

int szz_Count(const char *szz)
// Returns the number of strings in the argument
{
	int nCount=0;

	if (szz) {
		while (*szz) {
			szz=szz_Next(szz);
			nCount++;
		}
	}

	return nCount;
}

void szz_Dump(const char *szz, FILE *fp)
// Just in case you want to dump one of these things...
{
	const char *chp;

	if (!fp) fp=stdout;

	if (szz) {
		fprintf(fp, "{");
		for (chp=szz;*chp;chp=szz_Next(chp)) {
			if (chp != szz) fprintf(fp, ", ");
			fprintf(fp, "'%s'", chp);
		}
		fprintf(fp, "}");
	} else {
		fprintf(fp, "<NULL>");
	}
}

const char *szz_Append(const char *szzA, const char *szB)
// Appends an ordinary string to an existing szz
// The first parameter can be a NULL, which acts like an empty szz
{
	const char *chp;
	char *szzResult;					// If not found
	int nOldLen, nNewLen;

	if (!szzA) szzA=szz_New();				// Make it NULL safe

	for (chp=szzA;*chp;chp=szz_Next(chp))	// Find the end
		;

	// Here, 'chp' points to the very last NULL in szzA.
	nOldLen=chp-szzA;					// Without trailing '\0'
	nNewLen=nOldLen+1+strlen(szB)+1;
	szzResult=malloc(nNewLen);
	assert(szzResult != NULL);
	memcpy(szzResult, szzA, nOldLen);	// Don't copy old trailing '\0'
	strcpy(szzResult+nOldLen, szB);		// Add on new string
	szzResult[nNewLen-1]='\0';			// Terminate it
	free((char*)szzA);

	return szzResult;
}

int szz_Find(const char *szzA, const char *szB)
// Returns the index of szB within szzA or 0 if szB isn't there.
{
	if (szzA) {						// Don't bother if NULL and makes the whole thing NULL safe to boot
		const char *chp;
		int nIndex = 0;

		for (chp=szzA;*chp;chp=szz_Next(chp)) {
			nIndex++;
			if (!strcmp((char*)chp, (char*)szB)) {
				return nIndex;
			}
		}
	}

	return 0;
}

int szz_CaseFind(const char *szzA, const char *szB)
// Returns the index of szB within szzA or 0 if szB isn't there.
// This version ignores the case of the string.
{
	if (szzA) {						// Don't bother if NULL and makes the whole thing NULL safe to boot
		const char *chp;
		int nIndex = 0;

		for (chp=szzA;*chp;chp=szz_Next(chp)) {
			nIndex++;
			if (!stricmp((char*)chp, (char*)szB)) {
				return nIndex;
			}
		}
	}

	return 0;
}

const char *szz_UniqueAdd(const char *szzA, const char *szB)
// Adds a string to the szz string A iff it doesn't already exist in it
// Comparisons are NOT case sensitive
// Returns a (possibly new) heap based szz and frees szzA if necessary.
{
	if (!szz_CaseFind(szzA, szB)) {
		return szz_Append(szzA, szB);
	} else {
		return szzA;
	}
}

const char *szz_UniqueCatDelete(const char *szzA, const char *szzB)
// Takes two heap based 'szz' strings and joins them such that the result contains a unique set of codes from each
// Deletes both the originals and returns a new one.
{
	const char *chp;

	if (szzB) {											// NULL safety again
		for (chp=szzB;*chp;chp=szz_Next(chp)) {
			szzA = (const char *)szz_UniqueAdd(szzA, chp);
		}
		szz_Delete(szzB);
	}

	return szzA;
}

int szz_Size(const char *szz)
// Returns the number of bytes used by the szz including the terminating '\0'
{
	const char *chp;

	if (!szz) return 0;

	chp=szz;
	while (*chp)
		chp+=strlen(chp)+1;

	return chp-szz+1;
}

const char *szz_Copy(const char *szz)
// Creates a copy of an szz
{
	const char *szzResult;
	const char *chp;
	int nLen;

	if (!szz) return NULL;

	nLen = szz_Size(szz);

	szzResult = malloc(nLen);
	memcpy((char*)szzResult, szz, nLen);

	return szzResult;
}

const char *szz_Join(const char *szz, const char *szConjunction)
// Returns a string containing each of the strings in szz with 'szConjunction' between them or nothing if it's NULL.
{
	char *szResult;
	char *chp;
	int nCount;
	int nConLen;
	int nSize;

//printf("szz_Join(%x, \"%s\")\n", szz, szConjunction);
	nCount = szz_Count(szz);						// The number of strings
	if (!nCount) return strdup("");					// Deals with NULL and a "\0\0"

	if (!szConjunction) szConjunction="";			// Easier to work with

	nConLen = strlen(szConjunction);
	nSize = szz_Size(szz) - nCount;					// The number of actual characters (plus a trailing '\0')

//printf("Allocated %d+(%d-1)*%d = %d\n", nSize, nCount, nConLen, nSize+(nCount-1)*nConLen);
	szResult = malloc(nSize+(nCount-1)*nConLen);	// nSize included the terminating '\0';

	chp=szResult;
	while (*szz) {
		int nLen=strlen(szz);

		if (nConLen && chp != szResult) {
			memcpy(chp, szConjunction, nConLen);
			chp+=nConLen;
		}
		memcpy(chp, szz, nLen);
		szz+=nLen+1;
		chp+=nLen;
	}
	*chp='\0';

//printf("strlen(%s)=%d = Hopefully = %d\n", szResult, strlen(szResult), chp-szResult);
	return szResult;
}
