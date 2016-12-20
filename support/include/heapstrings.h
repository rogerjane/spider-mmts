void szDelete(const char *sz);
char *strappend(const char *szOriginal, const char *szAppend);
char *strnappend(char *szOriginal, const char *szAppend, int nMaxLen);
char *hprintf(const char *szOrig, const char *szFmt, ...);
const char *strset(const char **pszOriginal, const char *szText);
const char *strsubst(const char *szStr, const char *szSearch, const char *szReplace);
const char *hReadFileLine(FILE *fp);
