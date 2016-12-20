
//#include <stdlib.h>
//#include <time.h>
//#include <stdio.h>
//#include <sys/types.h>
//#include <sys/stat.h>
#include <stdlib.h>
#include <string.h>
//
//
#include "mtmacro.h"
//
//#include "eval.h"
//
//#include "rogxml.h"
//
//#include "quadmap.h"


#include "csv.h"

static char *SkipWhiteSpace(const char *szText)
{
	while (*szText == ' ' || *szText == '\t' ||
			*szText == '\r' || *szText == '\n')
		szText++;

	return (char*)szText;
}

static char *TrimRight(char *szText)
{
	char *chp=szText+strlen(szText);

	while (chp > szText && isspace(*(chp-1)))
		chp--;

	*chp='\0';

	return szText;
}

void csv_Delete(CSV *csv)
{
	if (csv) {
		if (csv->fp) fclose(csv->fp);
		if (csv->szLine) free(csv->szLine);
		csv->fp=NULL;
		csv->szLine=NULL;
	}
}

CSV *csv_NewFile(const char *szFilename)
{
	FILE *fp=fopen(szFilename, "r");
	CSV *csv;

	if (!fp) return NULL;

	csv=NEW(CSV, 1);
	csv->fp=fp;
	csv->szLine=NULL;
	csv->szPtr=NULL;
	csv->nLine = 0;

	return csv;
}

int csv_GetLineNumber(CSV *csv) {return csv->nLine;}

int csv_GetRecord(CSV *csv)
{
	char buf[65536];						// Can now handle 64K lines
	char *chp;

	if (csv->szLine) {						// Clear any current line
		free(csv->szLine);
		csv->szLine=csv->szPtr=NULL;
	}

	for (;;) {
		chp=fgets(buf, sizeof(buf), csv->fp);	// Get a new line
		if (chp) {
			csv->nLine++;						// Remember the line number
			chp=SkipWhiteSpace(chp);
			if (!*chp) continue;
			if (*chp == '#') continue;
			csv->szLine=csv->szPtr=strdup(chp);
			break;
		} else {
			return 0;
		}
	}

	return csv->nLine;
}

const char *csv_GetField(CSV *csv)
{
	char *chp=csv->szPtr;
	char cTerm;
	char *szResult;
	int bQuoted;
	int bDoubled = 0;
	if (!chp) return NULL;

	chp=SkipWhiteSpace(chp);
	if (!*chp) return NULL;
	if (*chp == '"') {
		bQuoted=1;
		chp++;
		cTerm='"';
	} else {
		bQuoted=0;
		cTerm=',';
	}

	szResult=chp;
	for (;;) {
		while (*chp && *chp!= cTerm) chp++;
		if (*chp == '"' && chp[1] == '"') {			// Embedded quotes
			chp+=2;
			bDoubled=1;
			continue;
		} else {
			break;
		}
	}
	if (*chp) {									// Normal close
		if (bQuoted) {							// Looking at closing quote
			*chp++='\0';						// Terminate and move on
			chp=SkipWhiteSpace(chp);
		}
		if (*chp == ',') {
			*chp++='\0';
		} else {
			chp=NULL;
		}
	} else {									// Hit end of string
		chp=NULL;
	}
	csv->szPtr=chp;

	if (bDoubled) {								// Need to undouble
		char *szSrc=szResult;
		char *szDest=szResult;
		char c;

		while (c=*szSrc++) {
			if (c == '"' && *szSrc == '"') szSrc++;
			*szDest++=c;
		}
		*szDest='\0';
	}

	return TrimRight(szResult);
}

//----------------------- CSVX

/*
   CSVX uses CSV to manage a file that consists of a header row with field
   followed by any number of rows with data.  These are read into a linked
   list (Names from row 1, Data from successive rows) and an array is
   built for speed of access
*/

static void csvxf_Delete(CSVXF *csvxf) {
	if (csvxf) {
		if (csvxf->szName) free((char *)csvxf->szName);
		if (csvxf->szData) free((char *)csvxf->szData);
	}
}

static CSVXF *csvxf_New(const char *szName, const char *szData)
{
	CSVXF *csvxf=NEW(CSVXF, 1);

	csvxf->pNext=NULL;
	csvxf->szName = szName ? strdup(szName) : NULL;
	csvxf->szData = szData ? strdup(szData) : NULL;

	return csvxf;
}

static void csvxf_SetName(CSVXF *csvxf, const char *szName)
{
	if (csvxf->szName) free((char*)csvxf->szName);
	csvxf->szName=szName?strdup(szName):NULL;
}

static void csvxf_SetData(CSVXF *csvxf, const char *szData)
{
//printf("csvxf_SetData(%x, %x='%s')\n",csvxf,szData,szData);
	if (csvxf->szData) free((char*)csvxf->szData);
	csvxf->szData=szData?strdup(szData):NULL;
}

void csvx_Delete(CSVX *csvx)
{
	if (csvx) {
		csv_Delete(csvx->csv);
		while (csvx->Field) {
			CSVXF	*pNext=csvx->Field->pNext;
			csvxf_Delete(csvx->Field);
			csvx->Field=pNext;
		}

		if (csvx->Array) free((char*)csvx->Array);
		free((char*)csvx);
	}
}

CSVX *csvx_NewFile(const char *szFilename)
{
	CSV		*csv=csv_NewFile(szFilename);
	CSVX	*csvx;
	CSVXF	**ppcsvxf;
	CSVXF	*csvxf;
	const char *chp;
	int		i;
	int		nNames;

	csvx=NEW(CSVX, 1);
	csvx->csv=csv;
	csvx->Field=NULL;
	csvx->Array=NULL;
	csvx->nFields=0;

	ppcsvxf=&(csvx->Field);

//	if (!csv_GetRecord(csv)) Fatal(1, "No header line");

	csv_GetRecord(csv);
	nNames=0;
	while (chp=csv_GetField(csv)) {
		CSVXF	*csvxf=csvxf_New(chp,NULL);
		*ppcsvxf=csvxf;
		ppcsvxf=&(csvxf->pNext);
		nNames++;
	}
	csvx->Array=NEW(CSVXF*, nNames);
	csvxf=csvx->Field;
	for (i=0;i<nNames;i++) {
		csvx->Array[i]=csvxf;
		csvxf=csvxf->pNext;
	}
	csvx->nNames=nNames;

	return csvx;
}

int csvx_GetLineNumber(CSVX *csvx) {return csv_GetLineNumber(csvx->csv);}

void csvx_ClearData(CSVX *csvx)
// Clear all data in CSVX record
{
	CSVXF *csvxf=csvx->Field;

	while (csvxf) {
		csvxf_SetData(csvxf, NULL);
		csvxf=csvxf->pNext;
	}
}

int csvx_GetRecord(CSVX *csvx)
{
	CSV *csv=csvx->csv;
	CSVXF *csvxf=csvx->Field;
	int nLine;
	int nCount=0;

	csvx_ClearData(csvx);
	nLine=csv_GetRecord(csv);
	if (!nLine) return 0;					// Couldn't read record

	while (csvxf) {
		const char *szData=csv_GetField(csv);
//printf("%x: [%x] %s\n", csvxf, szData, szData);
		if (!szData) break;					// Incomplete record!!!!!!!!
		csvxf_SetData(csvxf, szData);
		csvxf=csvxf->pNext;
		nCount++;
	}
	csvx->nFields=nCount;

	return nCount;
}

int csvx_GetFieldNumber(CSVX *csvx, const char *szName)
{
	CSVXF *csvxf=csvx->Field;
	int nField=0;

	while (csvxf) {
		nField++;
		if (!strcasecmp(csvxf->szName, szName)) return nField;
		csvxf=csvxf->pNext;
	}

	return 0;							// Field not found
}

const char *csvx_GetField(CSVX *csvx, const char *szName)
{
	CSVXF *csvxf=csvx->Field;

	while (csvxf) {
		if (!strcasecmp(csvxf->szName, szName)) return csvxf->szData;
		csvxf=csvxf->pNext;
	}

	return NULL;							// Field not found
}

int csvx_GetNameCount(CSVX *csvx) { return csvx->nNames; }
int csvx_GetDataCount(CSVX *csvx) { return csvx->nFields; }

const char *csvx_GetDataByNumber(CSVX *csvx, int n)
{
	if (n<1 || n>csvx->nFields) return NULL;

	return csvx->Array[n-1]->szData;
}

const char *csvx_GetNameByNumber(CSVX *csvx, int n)
{
	if (n<1 || n>csvx->nNames) return NULL;

	return csvx->Array[n-1]->szName;
}

