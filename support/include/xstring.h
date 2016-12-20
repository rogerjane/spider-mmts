
#ifndef XSTRING_H
#define XSTRING_H

 extern char gt_addr_map[];
 extern char gt_edi_map[];
 extern char gt_upr_map[];
 extern char gt_lwr_map[];

/**  Edifact bit entries for editab            **/

#define   CHK_UPPER     0001
#define   CHK_LOWER     0002
#define   CHK_ALPHA     0003
#define   CHK_NUMERIC   0004
#define   CHK_ALNUM     0007
#define   CHK_SURNAME   0010
#define   CHK_FORENAME  0020
#define   CHK_TITLE     0040
#define   CHK_ADDRESS   0100
#define   CHK_EDI_A     0200

/**  Address parsing bit entries for addrtab   **/

#define   ADR_UPPER     0001
#define   ADR_LOWER     0002
#define   ADR_ALPHA     0003
#define   ADR_NUMERIC   0004
#define   ADR_ALNUM     0007
#define   ADR_SPAN      0010
#define   ADR_BREAK     0020
#define   ADR_WSPACE    0040
#define   ADR_ADDRESS   0100
#define   ADR_SPARE1    0200

// Defines

#define casecmp   stricmp
#define casecmpn  strnicmp

#define strcmpl   stricmp
#define strncmpl  strnicmp

#define strstrf(a, b, c) strstr((a), (b))
#define strcont(a, b, c, d, e) strcontex((a), (b), (c), (d), (e), 0)
#define strcont2(a, b, c, d, e) strcontex((a), (b), (c), (d), (e), 1)
#define strcontx(a, b, c) strcontex((a), (b), (c), 0, 0, 1)
#define strncontx(a, b, c, d, e) strcontex((a), (b), (c), (d), (e), 1)

#define ALLOW_ALPHA             1
#define ALLOW_DIGIT             2

// Typedefs

typedef struct lineSplit {
    int maxn;   /** maximum number of lines **/
    int n;      /** actual number of lines **/
    int maxLen; /** longest allowed line length **/
    int minLen; /** min allowed length (except last line) **/

    char    **lines;    /** dynamically allocated array of lines **/
    char    *splitChars;    /** characters on which we can split **/
    char    hyphen; /** if forced line break use this as a hyphen **/
                    /** if no hypenastion set this to ASCII 0 **/
} lineSplit;

// Padding

char* cpypadl(char* lpszDest, char* lpszSrc, int nWidth, char chFill);
char* cpypadr(char* lpszDest, char* lpszSrc, int nWidth, char chFill);
char* strpadl(char* s, int width, char ch);
char* strpadr(char* s, int width, char ch);

// Trimming

int ltrim(char* lpsz);
int ltrimcpy(char* lpszDest, char* lpszSrc);
char* ltrimp(char* lpsz);

int rtrim(char* lpsz);
int rtrimcpy(char* lpszDest, char* lpszSrc);
char* rtrimp(char* lpsz);

int lrtrim(char* lpsz);
int lrtrimcpy(char* lpszDest, char* lpszSrc);
char* lrtrimp(char* lpsz);

// Comparison

int strcmpa(char* s1, char* s2);
int stricmp(const char* s1, const char* s2);
int stricmpa(char* s1, char* s2);
int stricmpax(char* s1, char* s2);
int strnicmp(const char* a, const char* b, int n);
char* strichr(char* lpsz, char ch);
char* strnchr(char* lpsz, char ch, int nWidth);
char* stristr(const char* lpszText, const char* lpszSub);

// Verification

int strcontex(char* text, char* allowtyp, char* allowchr, int minlen, int maxlen, int zeroinf);
int strkeepx(char* s, char* types, char* allow);

// Sizing

int text_height(char* text);
int text_width(char* text);
int text_width_attr(char* text);

// Case

int strlwr(char* s);
int strupr(char* s);
int cpylwr(char* dest, char* src);
int cpyupr(char* dest, char* src);

// EDIFact

int	stredicmp(char*, char*, int);
int	stredicpy(char*, char*, int);
int	strediicmp(char*, char*, int);


// Misc

int sep2vec(char* sepline, char** vec, char sep, int n);
char* skipwhite(char* s);
char* stradd(char* dx, char* sx);
char* strnadd(char* dx0, char* sx0, int n);
char *strfplace(char* dest, char* overtype, char field_sep, int place);
char* strins(char* s, char* si);
char* strnins(char* s, char* si, int n);
char* strinsc(char* s, char ch);
int strwnk(char* lpsz);
char* ztcpy(char* lpszDest, char* lpszSrc, int nWidth);
int mtStrccpy(char* lpszDest, char* lpszSrc, char ch);
char *krstrncpy(char* dx0, const char* sx0, int n);
int strzapc(char* text, char ch);
int strzaps(char* text, char* zs);

char* centrestr(char *string, int width);
char* format_name(char* title, char* initials, char* forename, char* surname, char* other, char* format);
char* make_filename(char* path, char* file, char* ext, char* dest);
char* stripfile(char* filename);
int setmatch(char *pattern);
int ismatch(char* str);
char* soundex(char *word);
char *soundex_root(char *word);
char *soundex_trim(char *word, int len);
int soundex_ncmp(char *soundex1, char *soundex2, int trimlen);
int soundex_cmp(char *soundex1, char *soundex2);
void rc_make(char *code, int len);
void rc_unmake(char *code, int len);
void CleanStr(char *s, int types, char *allow);
char *initials(char *str, int first_initial);
lineSplit *splitLine(int maxn, int maxlen, int minlen, char *split, char hyphen, char *string);
void freeSplitLine(lineSplit *sp);

#endif /* XSTRING_H */
