
#ifndef __MMTS_SQLITE_H
#define __MMTS_SQLITE_H

#include "smap.h"
#include "sqlite3.h"

#define S3LONG(s,x)		long x = 0;s3_VarLong(s,#x,&x)
#define S3STRING(s,x)	const char *x = NULL;s3_VarStr(s,#x,&x)
#define S3DOUBLE(s,x)	double x = 0.00;s3_VarDouble(s,#x,&x)

struct S3;
struct s3it;
typedef void (sl3errorHandler_t)(struct S3 *s, int nErr, const char *szErr, const char *szQuery);

struct S3 {
	sqlite3 *sql3;
	const char *filename;
	SPMAP *field_map;
	sl3errorHandler_t *errorHandler;
	int lastErr;
	struct s3it *it;
};

typedef struct S3 S3;

void s3_Close(S3 *s3);
void s3_Destroy(S3 **ps3);
void s3_Push(S3 *s3);
void s3_Pop(S3 *s3);
void s3_VarLong(S3 *s3, const char *name, long *n);
void s3_VarDouble(S3 *s3, const char *name, double *n);
void s3_VarStr(S3 *s3, const char *name, const char **str);
void s3_OnError(S3 *s, sl3errorHandler_t fn);

int s3_ErrorNo(S3 *s);
const char *s3_ErrorStr(S3 *s);
const char *s3_LastQuery(S3 *s);
const char *s3_Subst(const char *fmt, ...);

S3* s3_Open(const char *szFile);
S3* s3_MustOpen(const char *szFile);
S3* s3_OpenExisting(const char *szFile);
S3* s3_MustOpenExisting(const char *szFile);

S3 *s3_Openf(const char *fmt, ...);
S3 *s3_MustOpenf(const char *fmt, ...);
S3 *s3_OpenExistingf(const char *fmt, ...);
S3 *s3_MustOpenExistingf(const char *fmt, ...);

int s3_Query(S3 *s3, const char *qry);
int s3_MustQuery(S3 *s3, const char *qry);
int s3_Queryf(S3 *s3, const char *fmt, ...);
int s3_MustQueryf(S3 *s3, const char *fmt, ...);

const char **s3_Next(S3 *s3);
int s3_Rewind(S3 *s3);
int s3_Cols(S3 *s3);
const char **s3_Row(S3 *s3);
const char **s3_Tables(S3 *s3);
const char **s3_Columns(S3 *s3, const char *table);

#endif
