
// 26-11-13 RJ 0.00 sqlite access for mmts

#include <stdlib.h>
#include <stdio.h>
#include <strings.h>

#include <mtmacro.h>
#include <heapstrings.h>

#include "mmts-sqlite.h"

#define API
#define STATIC static

const char *s3_vSubst(const char *fmt, va_list ap)
// Substitute into fmt, any %s, %d, %D, %f or %% (string, long, long long, double, %) parameters
// Returns a string on the heap or NULL
// Method:
// * Count the % in the string baulking if naked one at the end
// * Create an array of strings of the right size
// * Allocate into the array a copy of each string needed
// * Build the string using the array, freeing the copies as we go
{
	if (!fmt || !strchr(fmt, '%')) return fmt;

	int percents = 0;
	char **strings;
	const char *chp=fmt;

	while (chp) {
		chp=strchr(chp, '%');
		if (chp) {
			if (!chp[1]) return strdup(fmt);		// Naked % at end of string
			percents++;
			chp+=2;
		}
	}
	strings=malloc(sizeof(char*)*percents);
	int percent=0;

	chp=fmt;
	int len=strlen(fmt)-percents;					// The amount we're going to need
	while (chp) {
		chp=strchr(chp, '%');
		if (chp) {
			char *string = NULL;
			switch(chp[1]) {
			case 's':	{
							char *s = va_arg(ap, char*);
							int count = 0;
							char *chp = s;
							while (*chp) {
								chp=strchr(chp, '\'');
								if (chp) {
									count++;
									chp++;
								}
							}
							string=malloc(strlen(s)+count+2+1);
							*string='\'';
							char *output=string+1;
							chp=s;
							while (*s) {
								chp=strchr(s, '\'');
								if (chp) {
									memcpy(output, s, chp-s);
									output+=chp-s;
									strcpy(output, "''");
									output+=2;
									s=chp+1;
								} else {
									strcpy(output, s);
									s="";
								}
							}
							strcat(output, "'");
						}
						break;
			case 'd':	string=malloc(30); snprintf(string, 30, "%d", va_arg(ap, long)); break;
			case 'D':	string=malloc(30); snprintf(string, 30, "%lld", va_arg(ap, long long)); break;
			case 'f':	string=malloc(30); snprintf(string, 30, "%f", va_arg(ap, double)); break;
			case '%':	string=strdup("%"); break;
			}

			if (!string) {		// Don't worry about stuff already allocated, we're in trouble
				char buf[30];
				snprintf(buf, sizeof(buf), "Bad format character '%c'", chp[1]);
				return strdup(buf);
			}
			strings[percent++]=string;
			len+=strlen(string);
			chp+=2;
		}
	}
	va_end(ap);

	char *result = malloc(len+1);
	char *output = result;
	percent=0;
	chp=fmt;
	while (chp=strchr(fmt, '%')) {
		memcpy(output, fmt, chp-fmt);
		output+=chp-fmt;

		int len = strlen(strings[percent]);
		memcpy(output, strings[percent], len);
		free(strings[percent]);
		output+=len;
		fmt = chp+2;
		percent++;
	}

	strcpy(output, fmt);

	return result;
}

const char *s3_Subst(const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);
	return s3_vSubst(fmt, ap);
}

API S3* s3_Open(const char *szFile)
// bEnv is 1 for an environment related db or 0 for the main system db
// szName is the name of the database or NULL to use the default ("mmts").
{
//printf("s3_Open(\"%s\")\n", szName);
	int err;

	S3 *s3 = NULL;
	sqlite3 *sql3;

	err = sqlite3_open(szFile, &sql3);

	if (!err) {
		s3 = NEW(S3, 1);
		s3->sql3 = sql3;
		s3->query = NULL;
	}

	return s3;
}

STATIC S3 *s3_vOpen(const char *fmt, va_list ap)
// Executes a query and returns 0 for success or an error number
{
	char file[512];
	vsnprintf(file, sizeof(file), fmt, ap);
	return s3_Open(file);
}

API S3 *s3_Openf(const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	return s3_vOpen(fmt, ap);
}

int s3_Do(S3 *s3, const char *qry)
// Executes a query and returns 0 for success or an error number
{
//Log("s3_Do(%p, \"%s\")", s3, qry);
	int err = -1;

	if (s3 && s3->sql3 && qry) {
		const char *szSafe = s3->query;			// In case it's still being referenced somewhere
		s3->query = strdup(qry);
		err = sqlite3_exec(s3->sql3, s3->query, NULL, NULL, NULL);
		szDelete(szSafe);						// Must be finished with now
	}

	return err;
}

int s3_Query(S3 *s3, const char *query)
// Executes a query and returns 0 for success or an error number
{
//Log("s3_Query(%p, \"%s\")", s3, query);
	int err = -1;

	if (s3 && s3->sql3 && query) {
		const char *szSafe = s3->query;			// In case it's still being referenced somewhere
		s3->query = strdup(query);
		err = sqlite3_exec(s3->sql3, s3->query, NULL, NULL, NULL);
		szDelete(szSafe);						// Must be finished with now
	}

	return err;
}

int s3_vQuery(S3 *s3, const char *fmt, va_list ap)
// Executes a query and returns 0 for success or an error number
{
	int err = -1;

	const char *query = s3_vSubst(fmt, ap);

//Log("s3_vQuery(%p, \"%s\" => \"%s\")", s3, fmt, query);

	return s3_Query(s3, query);
}

int s3_Queryf(S3 *s3, const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);
	return s3_vQuery(s3, fmt, ap);
}

void s3_Close(S3 *s3)
// Closes a database
{
	if (s3 && s3->sql3) {
		sqlite3_close(s3->sql3);
		s3->sql3=NULL;

		szDelete(s3->query);
		s3->query=NULL;
	}

	return;
}

void s3_Delete(S3 *s3)
{
	if (s3) {
		s3_Close(s3);
		free((char*)s3);
	}

	return;
}

void s3_Destroy(S3 **ps3)
{
	if (ps3 && *ps3) {
		s3_Delete(*ps3);
		*ps3=NULL;
	}
}

int s3_ErrorNo(S3 *s)
{
	return (s && s->sql3) ? sqlite3_extended_errcode(s->sql3) : -1;
}

const char *s3_ErrorStr(S3 *s)
{
	return (s && s->sql3) ? sqlite3_errmsg(s->sql3) : "No DB connection";
}

const char *s3_LastQuery(S3 *s)
{
	return s ? (s->query ? s->query : "No query") : "NULL handle";
}

void s3it_Delete(s3it *it)
{
	if (it) {
		if (it->row) free((char*)it->row);
		if (it->stmt) {
			sqlite3_finalize(it->stmt);
			it->stmt=NULL;
		}
		free((char*)it);
	}
}

void s3it_Destroy(s3it **pit)
{
	if (pit && *pit) {
		s3it_Delete(*pit);
		*pit=NULL;
	}
}

s3it *s3it_New(S3 *s3, const char *qry)
{
//printf("s3it_New(%p, \"%s\")\n", s3, qry);
	int err;
	sqlite3_stmt *stmt;
	s3it *it = NULL;

	if (s3 && s3->sql3 && qry) {
		err=sqlite3_prepare_v2(s3->sql3, qry, -1, &stmt, NULL);
//printf("%d=sqlite3_prepare_v2(%p, \"%s\", %d, %p, %p)\n", err, s3->sql3, qry, -1, &stmt, NULL);
		if (err == SQLITE_OK) {
			it = NEW(s3it, 1);
			it->stmt=stmt;
			it->s3=s3;
			it->nCols=0;
			it->nRow=0;
			it->row=NULL;
		}
	}

	return it;
}

const char **s3it_Next(s3it *it)
{
//printf("s3it_Next(%p)\n", it);
	const char **row = NULL;

	if (it && it->stmt) {
		sqlite3_stmt *pStmt = it->stmt;
		int rc = sqlite3_step(pStmt);
		int nCol = sqlite3_column_count(pStmt);

		if (rc == SQLITE_ROW) {
			it->nRow++;
			it->nCols=nCol;
			int i;

			if (!it->row) it->row=NEW(const char*, nCol);

			row=it->row;

			for(i=0; i<nCol; i++){
				row[i] = (const char *)sqlite3_column_text(pStmt, i);
			}
		} else {
			if (it->row) {
				free((char*)it->row);
				it->row=NULL;
			}
			it->nRow=0;
			it->nCols=0;
		}
	}

	return row;
}

int s3it_Rewind(s3it *it)
{
	int err=-1;

	if (it && it->stmt) {
		err=sqlite3_reset(it->stmt);
	}

	return err;
}

int s3it_FindColumn(s3it *it, const char *column)
{
}

const char *s3it_GetColumn(s3it *it, const char *column)
{
}

int s3it_Cols(s3it *it)
{
	return it ? it->nCols : 0;
}

const char **s3it_Row(s3it *it)
{
	return it ? (const char **)it->row : NULL;
}
