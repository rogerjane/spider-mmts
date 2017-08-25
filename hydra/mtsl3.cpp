
// 26-11-13 RJ 0.00 Generic sqlite3 access

#include <stdlib.h>
#include <stdio.h>
#include <strings.h>
#include <string.h>
#include <unistd.h>

#include <mtmacro.h>
#include <heapstrings.h>

#include "mtsl3.h"

#define STATIC static
#define API

#if 0
// START HEADER

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

// END HEADER
#endif

// How about in s3_vSubst() or thereabouts, something that will directly embed variables...
// s3_VarInt("age", &age);			// Obviously overload in a C++ environment
// UPDATE patient SET age=$age
// It'd be nice to do 'SELECT age FROM...' and have that load the variables...
// Or, of course, "SELECT patientAge AS age FROM..."
// Easier to be "SELECT patientAge AS $age FROM..."
//
// s3_VarInt(s, "drug_number", &drugno);
// s3_VarStr(s, "drug_name", &drugname);
// s3_VarInt(s, "recno", &recno);
// int err = s3_Sql(s, "SELECT drug_number, drug_name FROM patient_drugs WHERE record_number=$recno");
// int fetched = s3_Rows(s);
// while (s3_Fetch(s)) { ... }

// Column maps and field maps...
//
// The user can set their own variables with names that match 'SELECT' variables.  E.g.
//
// S3STRING(db, name);
// s3_Do(db, "SELECT name FROM mytable WHERE...");
// printf("Name is '%s'\n", name);
//
// Internally, the S3STRING calls s3_VarStr(), which makes an entry in S3.field_map that points to an s3var, which points to the
// variable.  When a query is run, the s3it.col_map maps columns to those s3var entries.  Each time a row is read, the
// corresponding variables are set from the row data.

// Usage paradigm...
// Use s3_Open*(...) to open the datebase
// Use s3_Query*(...) to issue queries
// s3_Query() actually executes the query and internally fetches a row (this so that any variables are set)
// s3_Next() will return each row (the first call after s3_Query() will return the first row again)
// If you want to query another table within a loop that's already querying something in the database there are two options:
// * Re-open the database and query against that
// * Use s3_Push() and s3_Pop() to save the status and restore it around the inner call.

#define _S3VAR_LONG		1
#define _S3VAR_DOUBLE	2
#define _S3VAR_STRING	3

struct s3var_t {
	int type;
	void *data;
};

struct s3it {
	struct s3it *prev;				// Previous entry in the stack
	int nCols;                      // How many columns in result
	int nRow;                       // Which row number we're on
	const char **row;               // Pointers to row text
	struct s3var_t **col_map;       // Pointers to mapped row
	int col_map_len;				// Number of entries in col_map
	sqlite3_stmt *stmt;
	char isCached;                  // 1 if row is cached and is simply returned on next s3it_Next call
	char *query;					// The current query
};

typedef struct s3it s3it;
typedef struct s3var_t s3var_t;

STATIC s3it *s3it_New(s3it *prev)
{
	s3it *it = NEW(s3it, 1);

	it->prev = prev;				// Pointer to previous entry in the stack
	it->stmt = NULL;				// The current statement

	it->nCols = 0;					// The number of columns in the current result
	it->row = NULL;					// Pointers to current results
	it->col_map = NULL;				// Map of column names to variables
	it->col_map_len = 0;			// Ensure we don't assume otherwise

	it->nRow = 0;					// Current row number (starting at 1)
	it->isCached = 0;				// 1 if the row is cached and is simply returned on next _Next() call
	it->query = NULL;				// Text of the current query

	return it;
}

STATIC S3 *s3_New(sqlite3 *sql3, const char *filename)
{
	S3 *s3 = NEW(S3, 1);

	s3->sql3 = sql3;					// sqlite3 structure corresponding to the file
	s3->filename = strdup(filename);	// Name of the file we're referenceing
	s3->field_map = NULL;				// Map of user fields to variables
	s3->errorHandler = NULL;			// Pointer to user error handler
	s3->lastErr = 0;					// Type of last error
	s3->it = s3it_New(NULL);			// Information on current query

	return s3;
}

STATIC s3var_t *s3var_New(int type, void *data)
{
	s3var_t *var = NEW(s3var_t, 1);

	var->type=type;
	var->data=data;

	return var;
}

STATIC s3it *s3it_Clear(s3it *it)
// Clears the content of the iterator apart from the 'prev' link, which it returns
{
	s3it *prev = NULL;

	if (it) {
		prev = it->prev;							// So the caller doesn't lose the chain

		if (it->stmt) {
			sqlite3_finalize(it->stmt);
			it->stmt = NULL;
		}

		if (it->col_map_len) {
			free((char*)it->col_map);
			it->col_map_len = 0;
		}

		if (it->query) {
			free(it->query);
			it->query = NULL;
		}

		if (it->row) {
			free((char*)it->row);
			it->row = NULL;
		}
		it->nCols = 0;

		it->nRow = 0;
		it->isCached = 0;
	}

	return prev;
}

STATIC s3it *s3it_Delete(s3it *it)
// Deletes the s3it, returning the previous one
{
	s3it *prev = NULL;

	if (it) {
		prev = s3it_Clear(it);
		free((char*)it);
	}

	return prev;
}

API void s3_Close(S3 *s3)
{
	if (s3) {
		if (s3->sql3)		sqlite3_close(s3->sql3);
		if (s3->filename)	free((char*)s3->filename);
		if (s3->field_map)	spmap_Delete(s3->field_map);

		s3it *it = s3->it;
		while (it)
			it = s3it_Delete(it);

		free((char*)s3);
	}

	return;
}

API void s3_Destroy(S3 **ps3)
{
	if (ps3 && *ps3) {
		s3_Close(*ps3);
		*ps3=NULL;
	}
}

API void s3_Push(S3 *s3)
{
	if (s3) {
		s3->it = s3it_New(s3->it);
	}
}

API void s3_Pop(S3 *s3)
{
	if (s3 && s3->it->prev) {
		s3->it = s3it_Delete(s3->it);
	}
}

STATIC void s3_Var(S3 *s3, int type, const char *name, void *p)
{
	if (!s3) return;
	if (!s3->field_map) s3->field_map=spmap_New();

	s3var_t *var = s3var_New(type, p);
	spmap_Add(s3->field_map, name, var);
}

API void s3_VarLong(S3 *s3, const char *name, long *n)
{
	s3_Var(s3, _S3VAR_LONG, name, n);
}

API void s3_VarDouble(S3 *s3, const char *name, double *n)
{
	s3_Var(s3, _S3VAR_DOUBLE, name, n);
}

API void s3_VarStr(S3 *s3, const char *name, const char **str)
{
	s3_Var(s3, _S3VAR_STRING, name, str);
}

STATIC sl3errorHandler_t *_errorHandler = NULL;

API void s3_OnError(S3 *s, sl3errorHandler_t fn)
{
	if (s) {
		s->errorHandler = fn;
	} else {
		_errorHandler = fn;
	}
}

API int s3_ErrorNo(S3 *s)
{
	int err = (s && s->sql3) ? sqlite3_extended_errcode(s->sql3) : -1;
	if (err == SQLITE_DONE || err == SQLITE_ROW)
		err = SQLITE_OK;						// Done means it wasn't a query, but a statement, ROW means row fetched

	return err;
}

API const char *s3_ErrorStr(S3 *s)
{
	return (s && s->sql3) ? sqlite3_errmsg(s->sql3) : "No DB connection";
}

API const char *s3_LastQuery(S3 *s)
{
	return s ? (s->it->query ? s->it->query : "No query") : "NULL handle";
}

STATIC const char *s3_vSubst(const char *fmt, va_list ap)
// Substitute into fmt, any %s, %S, %d, %D, %f or %% (string, naked string, long, long long, double, %) parameters
// %S copies the string in verbatim, %s doubles any quotes (') and quotes the result
// Returns a string on the heap or NULL
// Method:
// * Count the % in the string baulking if naked one at the end
// * Create an array of strings of the right size
// * Allocate into the array a copy of each string needed
// * Build the string using the array, freeing the copies as we go
{
	if (!fmt) return NULL;

	if (!strchr(fmt, '%')) return strdup(fmt);		// Quick if there's no substitutions

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

	// strings is going to be an array of replacement strings, we'll create the formatted result once we've collected them
	// as we can count the number of characters needed as we go.
	strings=(char **)malloc(sizeof(char*)*percents);			// Enough for a pointer to each string
	int percent=0;												// Current replacement string number

	chp=fmt;
	int len=strlen(fmt)-percents;								// The amount we're going to need before string insertions
	while (chp) {
		chp=strchr(chp, '%');
		if (chp) {
			char format = chp[1];
			char *string = NULL;
			switch(format) {
			case 's':
				{										// It's a string, so we need to quotify it
					char *s = va_arg(ap, char*);
					if (s) {
						int count = 0;					// Number of ' so number of extra bytes needed
						char *chp = s;
						while (chp) {
							chp=strchr(chp, '\'');
							if (chp) {
								count++;
								chp++;
							}
						}
						string=(char *)malloc(strlen(s)+count+2+1);		// Len + space for extra 's + '' + \0
						char *output=string;
						*output++='\'';
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
								break;
							}
						}
						strcat(output, "'");
					} else {
						string = strdup("NULL");
					}
				}
				break;
			case 'S':	string = strdup(va_arg(ap, char*)); break;
			case 'd':	string = (char *)malloc(30); snprintf(string, 30, "%ld", va_arg(ap, long)); break;
			case 'D':	string = (char *)malloc(30); snprintf(string, 30, "%lld", va_arg(ap, long long)); break;
			case 'f':	string = (char *)malloc(30); snprintf(string, 30, "%f", va_arg(ap, double)); break;
			case '%':	string = strdup("%"); break;
			}

			if (!string) {		// Don't worry about stuff already allocated, we're in trouble
				char buf[30];
				snprintf(buf, sizeof(buf), "Bad format character '%c'", chp[1]);
				return strdup(buf);
			}
			strings[percent++]=string;
			len+=strlen(string);		// Add to total amount needed for result
			chp+=2;
		}
	}

	char *result = (char *)malloc(len+1);
	char *output = result;
	percent=0;
	chp=fmt;
	while ((chp=strchr(fmt, '%'))) {
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

	free((char*)strings);

	return result;
}

API const char *s3_Subst(const char *fmt, ...)
{
	const char *result = NULL;
	if (fmt) {
		va_list ap;

		va_start(ap, fmt);
		result = s3_vSubst(fmt, ap);
		va_end(ap);
	}

	return result;
}

STATIC S3* s3_RawOpen(int must, int existing, const char *szFile)
// This is the only place where we open a file
// must is non-zero if we must open it (if we don't then we call _errorhandler)
// szName is the name of the database
{
//printf("s3_Open(\"%s\")\n", szName);
	S3 *s3 = NULL;

	if (szFile) {
		int err = 0;
		sqlite3 *sql3;

		if (existing && access(szFile, 4)) {			// Want an existing database but can't read a file
			err = 2;
		}

		if (!err) err = sqlite3_open(szFile, &sql3);

		if (err) {
			if (must) {
				static char *buf = NULL;				// Dummy up the 'query' of open(filename) just for error handler
				if (buf) free(buf);
				buf = (char *)malloc(strlen(szFile)+9);

				snprintf(buf, strlen(szFile)+9, "open(\"%s\")", szFile);
				if (_errorHandler) (_errorHandler)(NULL, err, "error opening database", buf);
			}
		} else {
			s3 = s3_New(sql3, szFile);
			sqlite3_busy_timeout(sql3, 5000);
		}
	}

	return s3;
}

API S3* s3_Open(const char *szFile)
{
	return s3_RawOpen(0, 0, szFile);
}

API S3* s3_MustOpen(const char *szFile)
{
	return s3_RawOpen(1, 0, szFile);
}

API S3* s3_OpenExisting(const char *szFile)
{
	return s3_RawOpen(0, 1, szFile);
}

API S3* s3_MustOpenExisting(const char *szFile)
{
	return s3_RawOpen(1, 1, szFile);
}

STATIC S3 *s3_vRawOpen(int must, int existing, const char *fmt, va_list ap)
{
	if (fmt) {
		char file[512];
		vsnprintf(file, sizeof(file), fmt, ap);
		return s3_RawOpen(must, existing, file);
	} else {
		return s3_RawOpen(must, existing, NULL);
	}
}

API S3 *s3_Openf(const char *fmt, ...)
{
	if (fmt) {
		va_list ap;
		va_start(ap, fmt);
		return s3_vRawOpen(0, 0, fmt, ap);
	} else {
		return s3_RawOpen(0, 0, NULL);
	}
}

API S3 *s3_MustOpenf(const char *fmt, ...)
{
	if (fmt) {
		va_list ap;
		va_start(ap, fmt);
		return s3_vRawOpen(1, 0, fmt, ap);
	} else {
		return s3_RawOpen(1, 0, NULL);
	}
}

API S3 *s3_OpenExistingf(const char *fmt, ...)
{
	if (fmt) {
		va_list ap;
		va_start(ap, fmt);
		return s3_vRawOpen(0, 1, fmt, ap);
	} else {
		return s3_RawOpen(0, 1, NULL);
	}
}

API S3 *s3_MustOpenExistingf(const char *fmt, ...)
{
	if (fmt) {
		va_list ap;
		va_start(ap, fmt);
		return s3_vRawOpen(1, 1, fmt, ap);
	} else {
		return s3_RawOpen(1, 1, NULL);
	}
}

STATIC const char **s3it_InternalFetchRow(s3it *it, int caching)
// When a query is executed, this is called once with 'caching=1' so that any
// variables are set up without having to call s3_Next()
// If called with 'caching=0' then it's like s3_Next()
// Hence...
// Call with caching=1, returns first row
// Call with caching=0, returns first row again
// Call with cahcing=0, returns second row
{
	const char **row = NULL;

	if (it && it->stmt) {
		if (it->isCached) {
			it->isCached=0;
			return it->row;
		}

		sqlite3_stmt *pStmt = it->stmt;
		int rc = sqlite3_step(pStmt);

		int nCol = (rc == SQLITE_ROW) ? sqlite3_column_count(pStmt) : 0;

		if (nCol != it->nCols) {
			if (it->row) {
				free((char*)it->row);
				it->row=NULL;
			}

			if (nCol)
				it->row=NEW(const char*, nCol);

			it->nCols=nCol;
		}

		if (rc == SQLITE_ROW) {
			int i;

			it->nRow++;
			row=it->row;

			for(i=0; i<nCol; i++) {
				row[i] = (const char *)sqlite3_column_text(pStmt, i);
			}
			if (it->col_map) {
				for(i=0; i<nCol; i++) {
					if (i < it->col_map_len) {				// safety in case columns returns differently somehow
						s3var_t *var=it->col_map[i];

						if (var) {
							switch(var->type) {
							case _S3VAR_LONG:
								*(int*)(var->data)=row[i]?atol(row[i]):0;
								break;
							case _S3VAR_DOUBLE:
								*(double*)(var->data)=row[i]?atof(row[i]):0.0;
								break;
							case _S3VAR_STRING:
								*(char**)(var->data)=(char*)row[i];
								break;
							}
						}
					}
				}
			}
		} else {
			it->nRow=0;
		}
		it->isCached = caching;
	}

	return row;
}

STATIC int s3_RawQuery(S3 *s3, int must, const char *qry)
// Does all querying in any shape or form.
// If there's an error and 'must == 1' then calls the error handler before returning.
// Returns 0 or an error code
{
//printf("s3_RawQuery(%p, \"%s\")\n", s3, qry);
	int err = -1;

	if (s3 && s3->sql3 && qry) {
		s3it *it = s3->it;
		const char *szSafe = it->query;			// In case it's still being referenced somewhere
		it->query = NULL;

		s3it_Clear(it);
		it->query = strdup(qry);
		s3->lastErr=sqlite3_prepare_v2(s3->sql3, qry, -1, &it->stmt, NULL);

		if (s3->lastErr == SQLITE_ROW) s3->lastErr = SQLITE_OK;		// We treat these equally here

		if (s3->lastErr == SQLITE_OK) {
			if (s3->field_map) {							// We have fields we might want to map
				int i=0;
				int nCols=sqlite3_column_count(it->stmt);

				for (i=0;i<nCols;i++) {						// Each returned column
					s3var_t *var = (s3var_t*)spmap_GetValue(s3->field_map, sqlite3_column_name(it->stmt, i));

					if (var) {								// We have a variable with a name that matches
						if (nCols != it->col_map_len) {		// Prepare row map as we don't have one of the right size
							if (it->col_map_len)
								free((char*)it->col_map);

							it->col_map_len = nCols;
							it->col_map=NEW(s3var_t*, nCols);
							int j;
							for (j=0;j<nCols;j++) {
								it->col_map[j]=NULL;
							}
						}
						it->col_map[i]=var;
					}
				}
// if (it->col_map) { int i; for (i=0;i<nCols;i++) { fprintf(stderr, "Map for %d(%p): %d -> %p\n", i, it->col_map[i], it->col_map[i]->type, it->col_map[i]->data); } }
			}
			s3it_InternalFetchRow(it, 1);
		}

		if (szSafe)
			free((char*)szSafe);							// Must be finished with now
	}

	err = s3_ErrorNo(s3);

	if (err && must) {
		sl3errorHandler_t *handler = (s3 && s3->errorHandler) ? s3->errorHandler : _errorHandler;

		if (handler) (handler)(s3, err, s3_ErrorStr(s3), s3_LastQuery(s3));
	}

	return err;
}

API int s3_Query(S3 *s3, const char *qry)
{
	return s3_RawQuery(s3, 0, qry);
}

API int s3_MustQuery(S3 *s3, const char *qry)
{
	return s3_RawQuery(s3, 1, qry);
}

STATIC int s3_vRawQuery(S3 *s3, int must, const char *fmt, va_list ap)
// Executes a query and returns 0 for success or an error number
{
	const char *query = s3_vSubst(fmt, ap);

	return s3_RawQuery(s3, must, query);
}

API int s3_Queryf(S3 *s3, const char *fmt, ...)
{
	int result;

	va_list ap;

	va_start(ap, fmt);
	result = s3_vRawQuery(s3, 0, fmt, ap);
	va_end(ap);

	return result;
}

API int s3_MustQueryf(S3 *s3, const char *fmt, ...)
{
	int result;

	va_list ap;

	va_start(ap, fmt);
	result = s3_vRawQuery(s3, 1, fmt, ap);
	va_end(ap);

	return result;
}

API const char **s3_Next(S3 *s3)
{
	return s3 ? s3it_InternalFetchRow(s3->it, 0) : NULL;
}

API int s3_Rewind(S3 *s3)
{
	int err=-1;

	if (s3 && s3->it->stmt) {
		err=sqlite3_reset(s3->it->stmt);
		s3->it->nRow = 0;
	}

	return err;
}

API int s3_Cols(S3 *s3)
{
	return s3 ? s3->it->nCols : 0;
}

API const char **s3_Row(S3 *s3)
{
	return s3 ? (const char **)s3->it->row : NULL;
}

API const char *s3_Filename(S3 *s3)
{
	return s3 && s3->filename ? s3->filename : "(NULL)";
}

STATIC const char **addString(const char **argv, const char *string)
{
	int count=0;

	if (argv) {
		const char **p=argv;
		while (*p) {
			p++;
			count++;
		}
		argv = (const char **)realloc(argv, (count+2)*sizeof(const char **));
	} else {
		argv = (const char **)malloc(2*sizeof(const char **));
	}
	argv[count++]=string;
	argv[count]=NULL;

	return argv;
}

API const char **s3_Tables(S3 *s3)
{
	const char **result = NULL;

	if (s3) {
		// Random name so that if user has used S3STRING(name), it won't surprise them!
		s3_Push(s3);
		int err = s3_Query(s3, "SELECT name AS rtguihrtgi FROM sqlite_master WHERE type = 'table'");

		if (!err) {
			const char **names;

			while ((names = s3_Next(s3))) {
				const char *name = names[0];

				if (name) result = addString(result, strdup(name));
			}
		}
		s3_Pop(s3);
	}

	return result;
}

API const char **s3_Columns(S3 *s3, const char *table)
{
	const char **result = NULL;

	if (s3) {
		s3_Push(s3);
		int err = s3_Queryf(s3, "PRAGMA TABLE_INFO(%S)", table);

		if (!err) {
			const char **names;

			while ((names = s3_Next(s3))) {
				const char *name = names[0];

				if (name) result = addString(result, strdup(name));
			}
		}
		s3_Pop(s3);
	}

	return result;
}
