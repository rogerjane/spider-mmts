
#ifndef __MMTS_SQLITE_H
#define __MMTS_SQLITE_H

#include "sqlite3.h"

typedef struct S3 {
	sqlite3 *sql3;
	const char *query;
} S3;

typedef struct s3it {
	S3 *s3;
	int nCols;						// How many columns in result
	int nRow;						// Which row number we're on
	const char **row;				// Pointers to row text
	sqlite3_stmt *stmt;
} s3it;

S3* s3_Open(const char *szName);
S3* s3_Openf(const char *szFmt, ...);
int s3_Do(S3 *s3, const char *qry);
void s3_Close(S3 *s3);
void s3_Delete(S3 *s3);
int s3_ErrorNo(S3 *s3);
const char *s3_ErrorStr(S3 *s3);

void s3it_Delete(s3it *it);
s3it *s3it_New(S3 *s3, const char *qry);
const char **s3it_Next(s3it *it);
int s3it_Rewind(s3it *it);
int s3it_FindColumn(s3it *it, const char *column);
const char *s3it_GetColumn(s3it *it, const char *column);
int s3it_Cols(s3it *it);
const char **s3it_Row(s3it *it);

#endif
