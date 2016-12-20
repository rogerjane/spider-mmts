
#ifndef __VARSET_H
#define __VARSET_H

struct varset;

typedef struct varval {
	struct varset *pVarset;
	char *szName;
	char *szValue;
	char *szEnv;
} varval;

typedef struct varset {
	varval **pVals;
} varset;
#ifdef __SCO_VERSION__
void unsetenv(const char *szName);
#endif 
void varset_Delete(varset *v);
varset *varset_New();
int varset_GetCount(varset *v);
void varset_Export(varset *v, const char *szName);
int varset_Set(varset *v, const char *szName, const char *szValue);
int varset_Setf(varset *v, const char *szName, const char *szFmt, ...);
const char *varset_GetName(varset *v, int n);
const char *varset_GetValue(varset *v, int n);
const char *varset_Get(varset *v, const char *szName);
void varset_Del(varset *v, const char *szName);
void varset_Copy(varset *vFrom, varset *vTo);
void varset_CopyPrefix(varset *vFrom, varset *vTo, const char *szPrefix);
int varset_FromFile(varset *v, const char *szFilename);
void varset_ExportAll(varset *v);
void varset_Dump(varset *v);

#endif
