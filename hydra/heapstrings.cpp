
// A number of functions all relating to strings held on the heap.

#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <mtmacro.h>

// START HEADER
#include <stdio.h>
// END HEADER

#define STATIC static
#define API

API void szDelete(const char *sz)
// Deletes a string if it isn't NULL.  Will free const strings without generating a warning!
{
	if (sz) free((char*)sz);
}

API const char *strset(const char **pszOriginal, const char *szText)
// The first param MUST point to a heap-allocated string.  If it doesn't, the world will end.
// Enables setting of a string without worrying about whether it has been allocated.  Use as:
// const char *fred=NULL;
// strset(&fred, "whatever");
// strset(&fred, "Something else");
{
	if (pszOriginal) {
		if (*pszOriginal) szDelete(*pszOriginal);
		*pszOriginal = szText ? strdup(szText) : NULL;

		return *pszOriginal;
	} else {
		return NULL;
	}
}

API char *strappend(const char *szOriginal, const char *szAppend)
// Returns a string on the heap consisting of the concatenation of szOriginal and szAppend
// and frees szOriginal.  A NULL szOriginal and/or szAppend work as you'd hope (like blank strings)...
// Always returns a string on the heap even if there is nothing to return (returns strdup("")).
// Will happily return szOriginal if szAppend is blank and szOriginal is non-NULL.
// szText = strappend(szText, "\nThis adds a line");
// szText = strappend(NULL, "This is like strdup");
{
	int nLen;
	char *szResult;

	if (!szAppend) szAppend="";						// Makes next line work nicely
	if (!szOriginal) return strdup(szAppend);		// Nothing to add to
	if (!*szAppend) return (char*)szOriginal;		// Nothing to add

	nLen=strlen(szOriginal);						// Saves calling strlen twice
	szResult = NEW(char, nLen+strlen(szAppend)+1);	// Length of both strings plus the '\0'
	memcpy(szResult, szOriginal, nLen);				// First half
	strcpy(szResult+nLen, szAppend);				// Add the second half on the end
	szDelete(szOriginal);							// Drop the original string

	return szResult;								// Return with our prize
}

API char *strnappend(char *szOriginal, const char *szAppend, int nMaxLen)
// Returns a string on the heap consisting of the concatenation of szOriginal and at most nMaxLen
// characters from szAppend and frees szOriginal.  A NULL szOriginal and/or szAppend work as you'd
// hope (like blank strings)... Always returns a string on the heap even if there is nothing to
// return (returns strdup("")).  Will happily return szOriginal if szAppend is blank and szOriginal
// is non-NULL.
{
	int nLen;
	char *szResult;

	if (!szAppend || !nMaxLen) szAppend="";					// Makes next line work nicely
	if (nMaxLen <= 0 || nMaxLen >= (int)strlen(szAppend))	// Take the easy way out if possible
		return strappend(szOriginal, szAppend);

	if (!szOriginal) szOriginal=strdup("");			// Best to create an original if there wasn't one before
	if (!*szAppend) return szOriginal;				// Nothing to add

	// Here, nMaxLen is between 1 and the length of szAppend, szAppend is a non-blank string
	nLen=strlen(szOriginal);						// Saves calling strlen twice
	szResult = NEW(char, nLen+nMaxLen+1);			// Length of both strings plus the '\0'
	memcpy(szResult, szOriginal, nLen);				// First half
	memcpy(szResult+nLen, szAppend, nMaxLen);		// Add the second half on the end
	szResult[nLen+nMaxLen]='\0';					// Terminate it

	free(szOriginal);								// Drop the original string

	return szResult;								// Return with our prize
}

API char *hprintf(const char *szOrig, const char *szFmt, ...)
// Appends a formatted string to a heap based string
// Pass NULL to create a new one.
// If the expanded szFmt is 1000 chars or less, this is very quick otherwise the 'printf' part will be
// called 3 times to ascertain how big a buffer we need.
// Don't forget to assign back!  E.g. Use as one of the following:
//  szText = hprintf(NULL, "Hello %s", szName);					// Bit like a strdup with formatting
//  szText = hprintf(szText, " and hello %s!", szName);			// Result is now "Hello <a> and hello <b>!"
// Returns something on the heap if everything went ok (might be a blank string of course)
//			NULL if we failed to allocate enough memory somewhere along the line
{
	va_list ap;
	char buf[1000];
	char *szResult;
	int nSent;

//printf("hprintf(\"%s\", \"%s\")\n", szOrig, szFmt);
	va_start(ap, szFmt);
	nSent=vsnprintf(buf, sizeof(buf), szFmt, ap);
	va_end(ap);

	// This following block could be made more sensible, but would then need more testing...

	if (nSent > (int)sizeof(buf)) {				// The LINUX runtime returns the number of bytes actually needed
		char *aim;							// Where we're going to actually write our formatted string

		if (szOrig) {
			int nLen = strlen(szOrig);					// Need this much for the original string
			szResult = (char *)realloc((char*)szOrig, nLen+nSent+1);	// Reallocate to a new, bigger buffer
			if (!szResult) return NULL;					// We needed too much space...!
			aim=szResult+nLen;							// Where we want to write the new bit
		} else {
			aim=szResult = (char *)malloc(nSent+1);		// Give us a buffer just big enough
		}

		va_start(ap, szFmt);
		vsnprintf(aim, nSent+1, szFmt, ap);	// Write to the right place in the buffer
		va_end(ap);
	} else if (nSent >= 0) {				// It fitted into the buffer
		if (szOrig) {
			int nLen = strlen(szOrig);

			szResult = (char *)realloc((char*)szOrig, nLen+nSent+1);
//printf("Simple (nLen=%d): copied %d to %d\n", nLen, nSent+1, nLen);
			if (szResult) memcpy(szResult+nLen, buf, nSent+1);
		} else {
//printf("Simple: Copying buffer\n");
			szResult=strdup(buf);
		}
	} else {								// Not enough space in buf so we need to be sneaky
		char *aim;							// Where we're going to actually write our formatted string
		FILE *fp;							// File pointer to /dev/null, which is quick and safe...

		fp=fopen("/dev/null", "w");			// Open a file to nowhere
		if (!fp) return NULL;				// Weird...
		va_start(ap, szFmt);
		nSent=vfprintf(fp, szFmt, ap);		// Write to this nowhere
		va_end(ap);
		fclose(fp);
		if (nSent < 0) return NULL;			// Don't see how this can ever happen but...

		// Ok, we now know how many characters it uses (nSent does not include the '\0')
		if (szOrig) {
			int nLen = strlen(szOrig);					// Need this much for the original string
			szResult = (char *)realloc((char*)szOrig, nLen+nSent+1);	// Reallocate to a new, bigger buffer
			if (!szResult) return NULL;					// We needed too much space...!
//printf("Nasty: Buffer len = %d, copying into %d\n", nLen+nSent+1, nLen);
			aim=szResult+nLen;							// Where we want to write the new bit
		} else {
//printf("Nasty: Malloc(%d)\n", nSent+1);
			aim=szResult = (char *)malloc(nSent+1);		// Give us a buffer just big enough
		}

		va_start(ap, szFmt);
		vsnprintf(aim, nSent+1, szFmt, ap);	// Write to the right place in the buffer
		va_end(ap);
	}

	return szResult;
}

API const char *strsubst(const char *szStr, const char *szSearch, const char *szReplace)
// Returns a copy of szStr with all occurences of szSearch replaced with szReplace.
// Frees szStr.
{
	const char *chp;
	char *szResult=NULL;
	const char *szOriginal = szStr;

	while ((chp=strstr(szStr, szSearch))) {
		szResult=strnappend(szResult, szStr, chp-szStr);
		szResult=strappend(szResult, szReplace);
		szStr=chp+strlen(szSearch);
	}

	szResult=strappend(szResult, szStr);
	szDelete(szOriginal);

	return szResult;
}

API const char *hReadFileLine(FILE *fp)
// Reads a file from a file, leaving the result on the heap
// There is no indication whether the last (only) line in a file terminates in a linefeed.
// Returns	NULL	Can't read a line (duff file, end of file etc.)
//			cc*		String on the heap
{
	char buf[1000];
	int sofar=0;
	char *result=NULL;
	int done=0;

	while (!done) {
		if (fgets(buf, sizeof(buf), fp)) {
			int len=strlen(buf);

			if (buf[len-1] == '\n') {
				buf[len-1]='\0';
				done=1;
			}
			if (result) {
				result = (char *)realloc(result,sofar+len+1);
				memcpy(result+sofar, buf, len+1);
			} else {
				result=strdup(buf);
			}
			sofar+=len;
		} else {
			done=1;
		}
	}

	return result;
}
