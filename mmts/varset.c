#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

#include "varset.h"

extern char **environ;

#ifdef __SCO_VERSION__
// Seems that LINUX thoughtfully provides this function...
void unsetenv(const char *szName)
// Removes an entry from the environment.
// Note that multiple matching entries are removed even though this is rare.
{
	char **penv=environ;
	int nFound=0;
	int nLen=strlen(szName);
	int nEntries=0;

	// First see if we've got any entry and count them
	while (*penv) {
		if (strlen(*penv)>nLen &&
				!strncmp(*penv, szName, nLen) &&
				(*penv)[nLen]=='=')
			nFound++;
		nEntries++;
		penv++;
	}

	// If there are any, create a new environment consisting of all the
	// entries in the existing one except those matching
	if (nFound) {					// Found something
		// Space is one for each entry, 1 for the NULL less the matches
		char **newenv=(char **)malloc(sizeof(char*)*nEntries+1-nFound);
		char **dest=newenv;
		penv=environ;
		while (*penv) {
			if (strlen(*penv)>nLen &&
					!strncmp(*penv, szName, nLen) &&
					(*penv)[nLen]=='=')
			{} else {				// Seemed easier in an else...!
				*dest++=*penv;
			}
			penv++;
		}
		*dest=NULL;					// Terminate with a NULL pointer
		free((char*)environ);
		environ=newenv;
	}
}
#endif

static int varset_ReadLine(FILE *fp, char *buf, int nBufLen)
// Reads a line from a file, getting rid of '\n's etc. and returning 1 if we got a line, 0 if we didn't.
{
	char *chp=fgets(buf, nBufLen, fp);

	if (chp) {
		chp=strchr(buf, '\n');
		if (chp) *chp='\0';

		return 1;
	} else {
		return 0;
	}
}

static int varset_ReadNonComment(FILE *fp, char *buf, int nBufLen)
// Reads a line from a file, skipping any blanks and comments (lines starting '#')
{
	int nGot;

	for (;;) {
		nGot = varset_ReadLine(fp, buf, nBufLen);
		if (!nGot) break;							// Got nothing
		if (*buf && *buf != '#') break;				// Got something other than a blank or comment
	}

	return nGot;
}

static varval *varval_New(varset *v, const char *szName, const char *szValue)
{
	varval *val = (varval*)malloc(sizeof(varval));
	val->pVarset=v;
	val->szName=strdup(szName);
	val->szValue=strdup(szValue);
	val->szEnv=NULL;

	return val;
}

static void varval_Delete(varval *val)
{
	if (val) {
		varval **pSrc, **pDst;
		varset *v=val->pVarset;

		if (val->szEnv) {
			unsetenv(val->szName);
			free(val->szEnv);
		}
		free(val->szName);
		free(val->szValue);

		pDst=pSrc=v->pVals;
		while (*pSrc) {
			if (*pSrc != val) {
				*pDst++=*pSrc;
				}
			pSrc++;
		}
		*pDst++=NULL;
		v->pVals=(varval**)realloc(v->pVals, (pDst-v->pVals)*sizeof(varval*));

		free((char*)val);
	}
}

static void varval_Export(varval *val)
// Exports this value to the environment
{
	if (val->szEnv) {
		unsetenv(val->szName);
		free(val->szEnv);
	}
	val->szEnv=malloc(strlen(val->szName)+strlen(val->szValue)+2);
	sprintf(val->szEnv, "%s=%s", val->szName, val->szValue);
	putenv(val->szEnv);
}

void varset_Delete(varset *v)
{
	if (v) {
		while (*v->pVals)
			varval_Delete(*v->pVals);
		free((char*)v->pVals);
		free((char*)v);
	}
}

varset *varset_New()
{
	varset *v = (varset*)malloc(sizeof(varset));
	v->pVals=(varval**)malloc(sizeof(varval*));
	v->pVals[0]=NULL;

	return v;
}

int varset_GetCount(varset *v)
{
	int nCount=0;
	varval **pVal = v->pVals;

	while (*pVal++)
		nCount++;

	return nCount;
}

static varval *varset_GetVal(varset *v, int n)
{
	varval **pVal;

	if (n<1) return NULL;			// Number 1..Count

	for (pVal=v->pVals;*pVal&&n>1;pVal++, n--);

	return *pVal;
}

static varval *varset_FindName(varset *v, const char *szName)
{
	varval **pvv = v->pVals;

	while (*pvv) {
		if (!strcasecmp((*pvv)->szName, szName))
			return *pvv;
		pvv++;
	}

	return NULL;
}

void varset_Export(varset *v, const char *szName)
// Export this variable to the environment
{
	varval *val = varset_FindName(v, szName);

	if (val) varval_Export(val);
}

int varset_Set(varset *v, const char *szName, const char *szValue)
// Returns 1 if this is an existing var, otherwise 0.
{
	varval *val=varset_FindName(v, szName);

	if (val) {
		free(val->szValue);
		val->szValue=strdup(szValue);
		if (val->szEnv) varval_Export(val);
		return 1;
	} else {
		int nVars=varset_GetCount(v);
		val=varval_New(v, szName, szValue);
		v->pVals=realloc(v->pVals, (nVars+2)*sizeof(varval*));
		v->pVals[nVars++]=val;
		v->pVals[nVars]=NULL;
		return 0;
	}
}

int varset_Setf(varset *v, const char *szName, const char *szFmt, ...)
{
	va_list ap;
	char buf[1000];

	va_start(ap, szFmt);
	vsnprintf(buf, sizeof(buf), szFmt, ap);
	va_end(ap);

	return varset_Set(v, szName, buf);
}

const char *varset_GetName(varset *v, int n)
{
	varval *val = varset_GetVal(v, n);

	return val ? val->szName : NULL;
}

const char *varset_GetValue(varset *v, int n)
{
	varval *val = varset_GetVal(v, n);

	return val ? val->szValue : NULL;
}

const char *varset_Get(varset *v, const char *szName)
{
	varval *val=varset_FindName(v, szName);

	if (val) {
		return val->szValue;
	} else {
		return NULL;
	}
}

void varset_Del(varset *v, const char *szName)
{
	varval_Delete(varset_FindName(v, szName));
}

void varset_Copy(varset *vFrom, varset *vTo)
// Copies all variables from 'vFrom' to 'vTo'
{
	int nCount=varset_GetCount(vFrom);
	int i;

	for (i=1;i<=nCount;i++)
		varset_Set(vTo, varset_GetName(vFrom, i), varset_GetValue(vFrom, i));

	return;
}

void varset_CopyPrefix(varset *vFrom, varset *vTo, const char *szPrefix)
// Copies all variables from 'vFrom' to 'vTo', prefixing them with szPrefix
{
	int nCount=varset_GetCount(vFrom);
	int i;

	for (i=1;i<=nCount;i++) {
		char buf[500];

		snprintf(buf, sizeof(buf), "%s_%s", szPrefix, varset_GetName(vFrom, i));
		varset_Set(vTo, buf, varset_GetValue(vFrom, i));
	}

	return;
}

int varset_FromFile(varset *v, const char *szFilename)
// Reads variables from a 'name=value' type file.
// Returns the number of values read - note this may not be the number added if any of the names in the file
// match names already in 'v' or are duplicated in the file (only the last is seen).
{
	FILE *fp = fopen(szFilename, "r");
	int nCount=0;

	if (fp) {
		for (;;) {
			char buf[1000];
			char *szName, *szValue, *chp;

			if (!varset_ReadNonComment(fp, buf, sizeof(buf))) break;
			szName=buf;
			while (*szName && (*szName == ' ' || *szName == '\t')) szName++;		// Skip leading blank space
			if (!*szName || *szName=='#') continue;									// Skips blanks and comments
			if (*szName == '[') continue;											// Skip [headers]
			chp=strchr(szName, '=');
			if (!chp) continue;														// Skip lines with no '='
			szValue=chp+1;
			chp--;
			while (chp >= szName && (*chp == ' ' || *chp == '\t')) chp--;			// Skip trailing blank space
			chp[1]='\0';
			while (*szValue && (*szValue == ' ' || *szValue == '\t')) szValue++;	// Skip leading blank space
			chp=szValue+strlen(szValue)-1;
			while (chp >= szValue && (*chp == ' ' || *chp == '\t')) chp--;			// Skip trailing blank space
			chp[1]='\0';
			varset_Set(v, szName, szValue);
			nCount++;
		}
		fclose(fp);
	}

	return nCount;
}

void varset_ExportAll(varset *v)
{
	varval **pVal=v->pVals;
	while (*pVal) {
		if (!(*pVal)->szEnv) varval_Export(*pVal);
		pVal++;
	}
}

void varset_Dump(varset *v)
{
	varval **pVal=v->pVals;
	printf("Dump of vars at %p (%d)\n", v, varset_GetCount(v));
	while (*pVal) {
		if ((*pVal)->szEnv) printf("* ");
		else printf("  ");
		printf("%s=%s\n", (*pVal)->szName, (*pVal)->szValue);
		pVal++;
	}
}
