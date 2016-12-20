#ifndef __szz_h_
#define __szz_h_

const char *szz_New();
void szz_Delete(const char *szz);
int szz_IsEmpty(const char *szz);
const char *szz_Next(const char *szz);
int szz_Count(const char *szz);
void szz_Dump(const char *szz, FILE *fp);
const char *szz_Append(const char *szzA, const char *szB);
int szz_Find(const char *szzA, const char *szB);
int szz_CaseFind(const char *szzA, const char *szB);
const char *szz_UniqueAdd(const char *szzA, const char *szB);
const char *szz_UniqueCatDelete(const char *szzA, const char *szzB);
int szz_Size(const char *szz);
const char *szz_Copy(const char *szz);
const char *szz_Join(const char *szz, const char *szConjunction);

#endif
