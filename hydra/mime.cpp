
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>
#include <stdarg.h>
#include <unistd.h>

#include <heapstrings.h>
#include <hbuf.h>
#include <mtmacro.h>

#include "mime.h"

#define STATIC static
#define API

#if 0
// START HEADER

#define SIG_MIME	0x656d694d		// "MIME"
#define SIG_MIMEB	0x626d694d		// "MIMB"

typedef struct MIMENV {		// A Name/value pair
	char	*name;
	char	*value;
} MIMENV;

typedef struct MIMETEXT {	// Describes the text in body part
	int nLen;				// Length of content
	const char *szContent;	// Perhaps the content or maybe a filename
	char bIsFile;			// 1 if it is a filename
	char bIsHeap;			// 1 if it is on the heap
} MIMETEXT;

typedef struct MIMENVP {	// A Name/value/parameters set
	MIMENV	*nv;
	MIMENV	**params;
} MIMENVP;

typedef struct MIME {		// A MIME message or body part
	MIMENVP		**header;	// Vector of header parts
	MIMETEXT	*content;	// Actual content, containing filename
	struct MIME	**parts;	// Vector of sub-parts where type is 'multipart'
	MIMETEXT	*epilogue;	// Comes after any mime content
} MIME;

typedef int mime_readfn();	// Arg type for mime_ReadFn()

// END HEADER
#endif

// NB. The 'add_' functions here have since been migrated out to be 'hbuf' as they were so useful and
// the hbuf functions have actually been used in here too.  Ideally, the 'add_' functions would be replaced
// by their hbuf_ equivalents but they're not broke so I'm not going to fix them now.
//
// This little 'add_' section handles adding single bytes to a heap-based buffer.  This could be achieved
// by adding a char at a time to the heap and re-allocing on each one but it would be horribly inefficient.
// Here, we use a seperate buffer to build the string and add it to the string whenever it is full.
// Usage:
//		Call add_Init();
//		Repeatedly call add_AddChar(c) to add characters (bytes) or add_AddBuffer() to add bigger bits
//		Call add_GetLength() to get the number of bytes added
//		Call add_GetBuffer() to get a pointer to the buffer (DO NOT free() this!!!)
//		Call add_ReleaseBuffer() to take control of the buffer (adding will then add to a new one)
//		Call add_Finish() to tidy up and free memory.
// Notes:
//		If you never call add_AddChar() then add_GetBuffer() and add_ReleaseBuffer() will return NULL.
//		If you want the resultant buffer to be null terminated, call add_AddChar(0) before the above functions.
//		If you don't call add_Finish() then it will get tidied up anyway on the next add_Init().
//		Call add_GetLength() BEFORE calling add_ReleaseBuffer() or you'll get 0!
//		Freakily, calling add_Init() is optional...

static char *add_buff = NULL;
static int add_len = 0;
static char *add_temp = NULL;
static int add_i=0;
static int add_size=1024;			// Incremental buffer size

STATIC void add_Finish()
{
	szDelete(add_buff);
	szDelete(add_temp);
	add_buff=NULL;
	add_temp=NULL;
	add_len=add_i=0;
}

STATIC void add_Init()
{
	add_Finish();					// Close leaks in case it wasn't called before

	add_temp=NEW(char, add_size);
}

STATIC void add_Flush()
{
	if (add_i) {
		if (add_buff)
			RENEW(add_buff, char, add_len+add_i);	// Make the vector longer (existing entries + new + NULL)
		else
			add_buff=NEW(char, add_i);				// Create a new vector
		memcpy(add_buff+add_len, add_temp, add_i);
		add_len+=add_i;
		add_i=0;
	}
}

STATIC int add_GetLength()				{ add_Flush(); return add_len; }

STATIC const char *add_ReleaseBuffer()
{
	const char *szResult;

	add_Flush();
	szResult=add_buff;
	add_buff=NULL;
	add_len=0;

	return szResult;
}

STATIC void add_AddChar(char c)
{
	if (!add_temp) add_Init();						// Just for safety
	if (add_i == add_size) add_Flush();
	add_temp[add_i++]=c;
//if (c>=' '&&c<127)printf("'%c' ", c);else printf("[%d] ", c);
//{static int x=0;x++;if (x>50)exit(0);}
}

STATIC void add_AddBuffer(int nLen, const char *buf)
// Adds nLen bytes to the buffer from 'buf'.
// If nLen is -1 then strlen(buf) is used.
// Calling with nLen==0 or buf==NULL is perfectly ok, though pointless.
{
	if (nLen < 0 && buf) nLen=strlen(buf);

	if (nLen && buf) {									// Something to add
		if (nLen <= (add_size-add_i)) {					// We can fit it in the small buffer
			memcpy(add_temp+add_i, buf, nLen);
			add_i+=nLen;
		} else {										// Stick it in the main one, flushing the small one first
			add_Flush();								// Ensure there's nothing already buffered up
			if (add_len)
				RENEW(add_buff, char, add_len+nLen);	// Make the vector longer (existing entries + new + NULL)
			else
				add_buff=NEW(char, nLen);				// Create a new vector
			memcpy(add_buff+add_len, buf, nLen);
			add_len+=nLen;
		}
//printf("Added: \"%.*s\"\n", nLen, buf);
	}
}

// Base64 routines - well known stuff.
// mime_Base64Enc() encodes a string and returns it on the heap.
// mime_Base64Dec() decodes a string and returns it on the heap.

static const char _base64[]="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
// In the following table, if the 'base64' char has the top bit set or the corresponding byte
// in the table has then the character is illegal.  Otherwise, it gives the mapping back to
// the original 6 bits.
static const unsigned char _base64D[]={
	255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255,  62, 255, 255, 255,  63,
	 52,  53,  54,  55,  56,  57,  58,  59,		// 0-7
	 60,  61, 255, 255, 255, 255, 255, 255,		// 8-9
	255,   0,   1,   2,   3,   4,   5,   6,		// A-G
	  7,   8,   9,  10,  11,  12,  13,  14,		// H-O
	 15,  16,  17,  18,  19,  20,  21,  22,		// P-W
	 23,  24,  25, 255, 255, 255, 255, 255,		// X-Z
	255,  26,  27,  28,  29,  30,  31,  32,		// a-g
	 33,  34,  35,  36,  37,  38,  39,  40,		// h-o
	 41,  42,  43,  44,  45,  46,  47,  48,		// p-w
	 49,  50,  51, 255, 255, 255, 255, 255		// x-z
};

static int Base64E(int nLen, const char *src, char *dest)
// Encodes up to 3 bytes to base 64, returning the result in the dest buffer
// Returns the number of bytes encoded into the dest buffer (buffer is always padded to 4 with '=')
{
	char a = src[0];
	char b = nLen>1 ? src[1] : 0;
	char c = nLen>2 ? src[2] : 0;

	dest[0]=(nLen>0)?_base64[(a>>2) & 0x3f]:'=';
	dest[1]=(nLen>0)?_base64[((a<<4) & 0x30) | ((b>>4)&0x0f)]:'=';
	dest[2]=(nLen>1)?_base64[((b<<2) & 0x3c) | ((c>>6)&0x03)]:'=';
	dest[3]=(nLen>2)?_base64[c & 0x3f]:'=';

	return nLen ? nLen+1 : nLen;
}

static int Base64D(int nLen, const char *src, char *dest)
// Decodes up to 4 bytes of base 64 to binary.
// If the base64 is shorter (we hit an '=' first) before nLen characters then we only decode that much.
// Returns the number of valid bytes decoded
{
	int i, n;

	for (i=0;i<nLen;i++) {
		int c=*src++;
		if (c=='=') break;
		if (c & 128) break;
		n=_base64D[c];
		switch (i) {
		case 0:		*dest=(n<<2) & 0xfc;	break;
		case 1:		*dest=(*dest | (n>>4));	*++dest=(n<<4) & 0xf0;	break;
		case 2:		*dest=(*dest | (n>>2));	*++dest=(n<<6) & 0xc0;	break;
		case 3:		*dest=(*dest | n);		break;
		}
	}

	return i ? (i-1) : 0;
}

API const char *mime_Base64Enc(int len, const char *szText, int nWrap, const char *szWrap)
// Returns a string that will be approximately 4/3 of the size of the original.
// If 'nWrap' is given, then this will be a bit longer.
// nWrap is the length to which lines will be wrapped (should probably be a multiple of 4) and szWrap
// is the string to wrap with ("LF" or "CRLF") - NULL will be treated as "CRLF".  nWrap=0 disables wrapping.
// If 'nWrap != 0' then szWrap is also tacked onto the end of the string.
// Returns NULL iff there is no memory left.
{
	char *szResult;
	char *szDest;
	int nLen;
	int nWrapLen;
	int nLinePos = 0;												// Position on 'output line'

	if (nWrap &= ~3);												// Must be a multiple of 4
	nLen = (len+3)*4/3+1;											// Length of plain encoding
	nWrapLen = nWrap ? (nLen+nWrap-1)/nWrap*strlen(szWrap) : 0;		// Length added by wrapping

	szResult=NEW(char, nLen+nWrapLen);
	if (!szResult) return NULL;

	szDest=szResult;

	while (len>0) {
		Base64E((len>=3)?3:len,szText, szDest);
		szText+=3;
		szDest+=4;
		len-=3;
		if (nWrap) {
			nLinePos += 4;
			if (nLinePos == nWrap) {
				strcpy(szDest, szWrap);
				szDest+=strlen(szWrap);
				nLinePos=0;
			}
		}
	}

	if (nLinePos != 0) {
		strcpy(szDest, szWrap);
		szDest+=strlen(szWrap);
	}

	*szDest='\0';

	return szResult;
}

API const char *mime_Base64Dec(int *pnLen, const char *szText)
// Takes the string pointed to by 'szText' up to the first '=' or '\0', decoding all
// legal characters.
// If pnLen is NULL then the result will be '\0' terminated as otherwise there is no way of knowing the length f'sure.
{
	char place[4];										// Place to put bytes while we decode them
	const char *szResult;
	int c;
	int i=0;

	add_Init();

	while ((c=*szText++) && (c != '=')) {

		if ((c & 128) || (_base64D[c] & 128))
			continue;									// Skip non-B64 characters
		place[i++]=c;
		if (i==4) {
			char buf[4];

			int got=Base64D(4, place, buf);
			add_AddBuffer(got, buf);
			i=0;
		}
	}

	if (i) {
		char buf[4];

		int got=Base64D(i, place, buf);
		add_AddBuffer(got, buf);
	}

	if (pnLen) *pnLen=add_GetLength();			// Caller wants the length
	else add_AddChar(0);						// Since they don't want the length, they must want an sz

	szResult = add_ReleaseBuffer();

	add_Finish();

	return szResult;
}

// There are one or two 'vectors' used in here.  These are arrays of pointers to the various MIME types and
// ALWAYS end with a NULL pointer.  Think of it like 'argv' if you like - same idea.

static char mime_bParseHeaderValues = 1;		// Set to 1 to inhibit attempt to parse header values

API char mime_ParseHeaderValues(char bSetting)
{
	char bOldSetting = mime_bParseHeaderValues;
	mime_bParseHeaderValues = bSetting;

	return bOldSetting;
}

static int mime_lasterror = 0;
static const char *mime_lasterrorstr = NULL;

API int mime_GetLastError() { return mime_lasterror; }
API const char *mime_GetLastErrorStr() { return mime_lasterrorstr; }

API void mime_SetLastError(int nErr, const char *szFmt, ...)
{
	mime_lasterror = nErr;
	szDelete(mime_lasterrorstr);

	va_list ap;
	char buf[1000];

	if (szFmt) {
		va_start(ap, szFmt);
		vsnprintf(buf, sizeof(buf), szFmt, ap);
		va_end(ap);

		mime_lasterrorstr = strdup(buf);
	} else {
		mime_lasterrorstr = NULL;
	}
}

STATIC const char *SkipSpaces(const char *szText)
{
	while (isspace(*szText)) szText++;

	return szText;
}

STATIC void mime_VAdd(void ***ppv, void *p)
// Adds a new 'thing' to a vector
// Call with the address of the vector and the new pointer to add.
// Note that the vector (*ppv) WILL move during this call
{
	void **pv=*ppv;
	void **pv2=pv;
	int n;						// Number of entries already in the list

	while (*pv2) pv2++;			// Find the NULL pointer at the end
	n=pv2-pv;					// Subtract from start point to get count of USED entries
	RENEW(pv, void*, n+2);		// Make the vector longer (existing entries + new + NULL)
	pv[n]=p;					// Put the new pointer where the NULL was
	pv[n+1]=NULL;				// Put a NULL back on the end
	*ppv=pv;					// Update the original pointer
}

STATIC const char *mime_NewBoundary()
// Creates a unique boundary string on the heap
{
	char buf[100];
	static int count=1;
	time_t now=time(NULL);
	struct tm *tm = localtime(&now);

	sprintf(buf, "----=_%d-%04d%02d%02d-%02d%02d%02d-%d",
			getpid(),
			tm->tm_year+1900, tm->tm_mon+1, tm->tm_mday,
			tm->tm_hour, tm->tm_min, tm->tm_sec,
			count++);

	return strdup(buf);
}

///////////////// MIMENV
// NV is simply a name and value pair.  This is used for the main header entries (including MIME headers in bodies)
// as well as the parameters that associate to those headings.

STATIC void mimenv_Delete(MIMENV *nv)
{
	if (nv) {
		free(nv->name);
		free(nv->value);
		free((char*)nv);
	}
}

STATIC MIMENV *mimenv_New(const char *name, const char *value)
{
	MIMENV *nv=NEW(MIMENV, 1);

	nv->name=strdup(name);
	nv->value=strdup(value);

//{FILE *fp=fopen("/tmp/mimeread","a");fprintf(fp, "mimenv_New('%s', '%s') = %d\n", name, value, nv);fclose(fp);}
	return nv;
}

STATIC MIMENV **mimenv_VNew()
// Creates a new vector of MIMENVs
{
	MIMENV **pnv = NEW(MIMENV*, 1);
	*pnv=NULL;

	return pnv;
}

STATIC void mimenv_VDelete(MIMENV **pnv)
{
	MIMENV **porig=pnv;

	if (pnv) {
		while (*pnv) {
			mimenv_Delete(*pnv);
			pnv++;
		}
		free((char*)porig);
	}
}

STATIC void mimenv_SetValue(MIMENV *nv, const char *value)
{
	if (nv->value) free(nv->value);
	nv->value=strdup(value);
}

STATIC MIMENV *mimenv_VFind(MIMENV **pnv, const char *name)
// Finds a name in a vector
// Returns		MIMENV*		Pointer to NV type
//				NULL		Not found
{
	while (*pnv) {
		if (!strcasecmp((*pnv)->name, name))
			return *pnv;
		pnv++;
	}

	return NULL;
}

STATIC MIMENV *mimenv_VFindAdd(MIMENV ***ppnv, const char *name, const char *value)
// Finds a name in a vector, adding it if it is not there.
// Note that the vector might get longer if the variable needs adding therefore we need to update
// the original pointer to that vector and hence the fact we pass such a de-referenced pointer...
{
	MIMENV **pnv=*ppnv;
	MIMENV *nv=mimenv_VFind(pnv, name);
	if (!nv) {
		nv=mimenv_New(name, value);			// Create a new one
		mime_VAdd((void***)ppnv, nv);		// Add it in
	}

	return nv;
}

STATIC MIMENV *mimenv_VSet(MIMENV ***ppnv, const char *name, const char *value)
// Sets a variable in the vector pointed to by ppnv.
// This will re-use an existing variable of the same name if it exists.
// NB. If there are multiple variables with the same name, only the first will be changed.
{
	MIMENV *nv=mimenv_VFindAdd(ppnv, name, value);

	mimenv_SetValue(nv, value);

	return nv;
}

///////////////// MIMENVP
// NVP is 'Name, Value, Parameters'.  This is the basic representation for a header entry in a MIME (or SMTP)
// email.  The usual representation is:
//		name: value; param1=value1; param2=value2
// Hence there is one main name/value pair and an arbitary number of parameter name/value pairs.

STATIC void mimenvp_Delete(MIMENVP *nvp)
{
	if (nvp) {
		mimenv_Delete(nvp->nv);
		mimenv_VDelete(nvp->params);
		free((char*)nvp);
	}
}

STATIC MIMENVP *mimenvp_New(const char *name, const char *value)
{
	MIMENVP *nvp=NEW(MIMENVP, 1);

	nvp->nv=mimenv_New(name, value);
	nvp->params=mimenv_VNew();

	return nvp;
}

STATIC void mimenvp_VDelete(MIMENVP **pnvp)
{
	MIMENVP **porig=pnvp;

	if (pnvp) {
		while (*pnvp) {
			mimenvp_Delete(*pnvp);
			pnvp++;
		}
		free((char*)porig);
	}
}

STATIC MIMENVP **mimenvp_VNew()
// Creates a new vector of MIMENVPs
{
	MIMENVP **pnvp = NEW(MIMENVP*, 1);
	*pnvp=NULL;

	return pnvp;
}

const char *mimenvp_NameList(MIMENVP **header)
// Returns a szz style list of the names in the vector
// The caller is responsible for freeing it.
{
	MIMENVP **pnv=header;

	int len = 1;
	while (*pnv) {
		len += strlen((*pnv)->nv->name)+1;
		pnv++;
	}

	char *result = (char *)malloc(len);
	char *chp = result;

	pnv=header;
	while (*pnv) {
		strcpy(chp, (*pnv)->nv->name);
		chp+=strlen(chp)+1;
		pnv++;
	}
	*chp='\0';

	return result;
}

STATIC void mimenvp_Dump(MIMENVP *nvp)
{
	MIMENV **pnv=nvp->params;

	printf("Value {%s} = {%s}\n", nvp->nv->name, nvp->nv->value);
	while (*pnv) {
		printf("  Param {%s} = {%s}\n", (*pnv)->name, (*pnv)->value);
		pnv++;
	}
}

STATIC const char *mimenvp_RenderHeap(const char *heap, MIMENVP *nvp)
{
	MIMENV **pnv=nvp->params;

	heap=hprintf(heap, "%s: %s", nvp->nv->name, nvp->nv->value);
	while (*pnv) {				// TODO: See if we really need this to be quoted for NPfIT
		if (!strcasecmp((*pnv)->name, "xxxboundary")) {						// Quote boundary
			heap=hprintf(heap, ";\r\n\t%s=\"%s\"", (*pnv)->name, (*pnv)->value);
		} else {
//			heap=hprintf(heap, ";\r\n\t%s=%s", (*pnv)->name, (*pnv)->value);	// Render on new lines
			heap=hprintf(heap, ";%s=\"%s\"", (*pnv)->name, (*pnv)->value);	// Render on one line
		}
		pnv++;
	}
	return hprintf(heap, "\r\n");
}

STATIC void mimenvp_RenderBuf(HBUF *buf, MIMENVP *nvp)
{
	MIMENV **pnv=nvp->params;
	const char *heap = NULL;

	heap=hprintf(heap, "%s: %s", nvp->nv->name, nvp->nv->value);
	while (*pnv) {
		heap=hprintf(heap, ";%s=\"%s\"", (*pnv)->name, (*pnv)->value);	// Render on one line
		pnv++;
	}
	heap = hprintf(heap, "\r\n");

	hbuf_AddBuffer(buf, -1, heap);
	szDelete(heap);
}

STATIC void mimenvp_VDump(MIMENVP **pnvp)
{
	while (*pnvp) {
		mimenvp_Dump(*pnvp);
		pnvp++;
	}
}

STATIC const char *mimenvp_VRenderHeap(const char *heap, MIMENVP **pnvp)
{
	while (*pnvp) {
		heap=mimenvp_RenderHeap(heap, *pnvp);
		pnvp++;
	}

	return heap;
}

STATIC void mimenvp_VRenderBuf(HBUF *buf, MIMENVP **pnvp)
{
	while (*pnvp) {
		mimenvp_RenderBuf(buf, *pnvp);
		pnvp++;
	}
}

STATIC void mimenvp_SetValue(MIMENVP *nvp, const char *value)	{ mimenv_SetValue(nvp->nv, value); }

void mimenvp_SetParameter(MIMENVP *nvp, const char *param, const char *value)
{
	mimenv_VSet(&nvp->params, param, value);
}

STATIC MIMENVP *mimenvp_VFind(MIMENVP **pnvp, const char *name)
// Finds a name in a vector
// Returns		MIMENVP*	Pointer to NVP type
//				NULL		Not found
{
	while (*pnvp) {
		if (!strcasecmp((*pnvp)->nv->name, name))
			return *pnvp;
		pnvp++;
	}

	return NULL;
}

STATIC void mimenvp_VSetParameter(MIMENVP **pnvp, const char *name, const char *param, const char *value)
// Given a vector of name/value/pairs (usually called 'header'), sets a specific parameter on a
// specific name.  E.g.  "Content-Type: text/plain" - call with "Content-Type", "name", "fred" to
// make it into "Content-Type: text/plain; name=fred".
// Note that if you call this with the 'name' of something that doesn't exist that it will
// quietly not do anything at all.
{
	MIMENVP *nvp = mimenvp_VFind(pnvp, name);

	if (nvp)
		mimenvp_SetParameter(nvp, param, value);
}

STATIC MIMENVP *mimenvp_VFindAdd(MIMENVP ***ppnvp, const char *name, const char *value)
// Finds a name in a vector, adding it if it is not there.
// Note that the vector might get longer if the variable needs adding therefore we need to update
// the original pointer to that vector and hence the fact we pass such a de-referenced pointer...
{
	MIMENVP **pnvp=*ppnvp;
	MIMENVP *nvp=mimenvp_VFind(pnvp, name);
	if (!nvp) {
		nvp=mimenvp_New(name, value);		// Create a new one
		mime_VAdd((void***)ppnvp, nvp);				// Add it in
	}

	return nvp;
}

STATIC MIMENVP *mimenvp_VSet(MIMENVP ***ppnvp, const char *name, const char *value)
// Sets a variable in the vector pointed to by ppnvp.
// This will re-use an existing variable of the same name if it exists.
// NB. If there are multiple variables with the same name, only the first will be changed.
{
	MIMENVP *nvp=mimenvp_VFindAdd(ppnvp, name, value);

	mimenvp_SetValue(nvp, value);

	return nvp;
}

STATIC const char *mimenvp_VGet(MIMENVP **pnvp, const char *name)
{
	MIMENVP *nvp=mimenvp_VFind(pnvp, name);

	if (nvp)
		return nvp->nv->value;
	else
		return NULL;
}

STATIC const char *mimenvp_GetParameter(MIMENVP *nvp, const char *param)
{
	MIMENV *nv=mimenv_VFind(nvp->params, param);

	if (nv)
		return nv->value;
	else
		return NULL;
}

STATIC const char *mimenvp_VGetParameter(MIMENVP **pnvp, const char *name, const char *param)
{
	MIMENVP *nvp=mimenvp_VFind(pnvp, name);

	if (nvp)
		return mimenvp_GetParameter(nvp, param);
	else
		return NULL;
}

STATIC MIMENVP *mimenvp_VAdd(MIMENVP ***ppnvp, const char *name, const char *value)
// Adds a variable to the vector pointed to.
// Always adds one irrespective of whether it already exists.
{
	MIMENVP *nvp=mimenvp_New(name, value);		// Create a new one
	mime_VAdd((void***)ppnvp, nvp);				// Add it in

	return nvp;
}

////////////////////// MIME
// This is the actual MIME 'thing'.  It holds the header lines and a body, which could be a single part
// (which is the bit in 'content') or it could be a load of MIME*s, which constitute the parts of a
// multipart structure.  In this case, there may be a 'preamble' stored in the 'content' and/or an
// epilogue.

API MIMENVP *mime_SetHeader(MIME *m, const char *name, const char *value)
{
	return mimenvp_VSet(&m->header, name, value);
}

API MIMENVP *mime_AddHeader(MIME *m, const char *name, const char *value)
{
	return mimenvp_VAdd(&m->header, name, value);
}

API const char *mime_GetHeader(MIME *m, const char *name)
{
	return mimenvp_VGet(m->header, name);
}

API const char *mime_GetFullHeaderValue(MIME *m, const char *name)
// Returns the full header value on the heap
{
	const char *result = NULL;

	MIMENVP *nvp=mimenvp_VFind(m->header, name);
	if (nvp) {
		MIMENV **pnv=nvp->params;

		result=strdup(nvp->nv->value);
		while (*pnv) {
			result=hprintf(result, ";%s=\"%s\"", (*pnv)->name, (*pnv)->value);	// Render on one line
			pnv++;
		}
	}

	return result;
}

API const char *mime_GetHeaderList(MIME *m)
{
	return mimenvp_NameList(m->header);
}

API void mime_SetParameter(MIME *m, const char *name, const char *param, const char *value)
{
	mimenvp_VSetParameter(m->header, name, param, value);
}

API const char *mime_GetParameter(MIME *m, const char *name, const char *param)
{
	return mimenvp_VGetParameter(m->header, name, param);
}

API void mimetext_Delete(MIMETEXT *t)
{
	if (t) {
		if (t->bIsHeap) szDelete(t->szContent);
		free((char*)t);
	}
}

STATIC MIMETEXT *mimetext_New()
// Not useful on its own - use one of the following _NewXXX(functions)
{
	MIMETEXT *t=NEW(MIMETEXT, 1);
	t->bIsFile=0;
	t->bIsHeap=0;
	t->szContent=NULL;
	t->nLen=0;

	return t;
}

STATIC MIMETEXT *mimetext_NewText(int len, const char *szText)
// New with static text
{
	if (szText) {
		MIMETEXT *t=mimetext_New();
		t->nLen=(len>=0)?len:strlen(szText);
		t->szContent=szText;

		return t;
	} else {
		return NULL;
	}
}

STATIC MIMETEXT *mimetext_NewHeap(int len, const char *szText)
// New with text copied to the heap
{
	MIMETEXT *t=mimetext_NewText(len, szText);
	if (t) {
		t->szContent=strdup(t->szContent);
		t->bIsHeap=1;
	}

	return t;
}

API const char *mimetext_GetContent(MIMETEXT *t)	{ return t ? t->szContent : NULL; }
API int mimetext_GetLength(MIMETEXT *t)				{ return t ? t->nLen : 0; }

API char mimetext_IsFile(MIMETEXT *t)				{ return t ? t->bIsFile : 0; }

////////// Things to do with content types

API const char *mime_GetContentType(MIME *m)	{return mime_GetHeader(m, "Content-Type");}

API void mime_SetContentType(MIME *m, const char *szType)
{
	mime_SetHeader(m, "Content-Type", szType);
}

API int mime_IsMultipart(MIME *m)
{
	return !!m->parts[0];				// Now defined as 'has sub parts'
}

API void mime_SetBoundary(MIME *m, const char *szBoundary)
{
	const char *szContentType = mime_GetContentType(m);

	if (!szContentType)
		mime_SetContentType(m, "multipart/related");
	mime_SetParameter(m, "Content-Type", "boundary", szBoundary);
}

STATIC void mime_SetUniqueBoundary(MIME *m)
{
	const char *szBoundary=mime_NewBoundary();

	mime_SetBoundary(m, szBoundary);

	free((char*)szBoundary);
}

STATIC const char *mime_GetBoundary(MIME *m)
{
	const char *boundary;

	boundary=mime_GetParameter(m, "Content-Type", "boundary");
	if (!boundary) {
		mime_SetUniqueBoundary(m);
		boundary=mime_GetParameter(m, "Content-Type", "boundary");
	}

	return boundary;
}

STATIC void mime_EnsureBoundary(MIME *m)
{
	if (!mime_GetBoundary(m))
		mime_SetUniqueBoundary(m);
}

STATIC void mimetext_Dump(MIMETEXT *t)
{
	if (t) {
		if (t->bIsFile) {
			printf("[FILE %s]\n", t->szContent);
		} else {
			printf("%.*s\n", t->nLen, t->szContent);
		}
	} else {
		printf("<NULL>\n");
	}
}

API void mime_Delete(MIME *m)
{
	if (m) {
		MIME **psub;

		mimenvp_VDelete(m->header);
		mimetext_Delete(m->content);
		mimetext_Delete(m->epilogue);
		psub=m->parts;
		while (*psub) {
			mime_Delete(*psub);
			psub++;
		}
		free((char*)m);
	}
}

STATIC MIME *mime_NewShell()
// Returns a non-useful MIME* with everything set to NULL.
// The only exception is the vector of parts is initialised.
{
	MIME *m=NEW(MIME, 1);

	m->header=NULL;								// Needs to be setup before use
	m->content=NULL;
	m->epilogue=NULL;
	m->parts=NEW(MIME*, 1);
	m->parts[0]=NULL;

	return m;
}

API MIME *mime_New(const char *szType)
// Builds a MIME by putting in the bits that mimeb_NewShell() doesn't.
{
	MIME *m=mime_NewShell();

	m->header=mimenvp_VNew();				// Needs to be a blank list to be useful

	if (szType) {
		mime_SetContentType(m, szType);
	}

	return m;
}

API void mime_Dump(MIME *m)
{
	mimenvp_VDump(m->header);

	printf("Body:\n");

	if (mime_IsMultipart(m)) {				// Multipart so iterate through them
		MIME **pb = m->parts;
		int count=1;

		printf("---- MULTIPART...\n");
		while (*pb) {
			printf("---- Part %d\n", count++);
			mime_Dump(*pb);
			pb++;
		}
		printf("---- END MULTIPART\n");
	} else {								// Easy one - just one part
		printf("---- SINGLEPART...\n");
		mimetext_Dump(m->content);
		printf("---- END SINGLEPART\n");
	}
}

API MIME *mime_NewMultipart()
{
	MIME *m=mime_New("multipart/related");
	mime_SetUniqueBoundary(m);

	return m;
}

API const char *mime_RenderHeapAdd(const char *heap, MIME *m)
{
	heap=mimenvp_VRenderHeap(heap, m->header);
	heap=hprintf(heap, "\r\n");

	if (mime_IsMultipart(m)) {				// Multipart so iterate through them
		MIME **pb = m->parts;

		while (*pb) {
			heap=hprintf(heap, "--%s\r\n", mime_GetBoundary(m));
			heap=mime_RenderHeapAdd(heap, *pb);
			pb++;
		}
		heap=hprintf(heap, "--%s--\r\n", mime_GetBoundary(m));
	} else {
		if (m->content) {
			const char *szText = mimetext_GetContent(m->content);
			if (mimetext_IsFile(m->content)) {	// Need to get the file to render it...!
				heap=hprintf(heap, "Best look in '%s'\n", szText);
			} else {							// Nice and easy...
				heap=hprintf(heap, "%.*s\r\n", mimetext_GetLength(m->content), szText);
			}
		}
	}

	return heap;
}

API const char *mime_RenderHeap(MIME *m)
{
	return mime_RenderHeapAdd(NULL, m);
}

STATIC void mime_RenderBufAdd(HBUF *buf, MIME *m, int bWantHeader)
{
	if (bWantHeader) {
		mimenvp_VRenderBuf(buf, m->header);
		hbuf_AddBuffer(buf, 2, "\r\n");
	}

	if (mime_IsMultipart(m)) {				// Multipart so iterate through them
		MIME **pb = m->parts;

		while (*pb) {
			hbuf_AddBuffer(buf, 2, "--");
			hbuf_AddBuffer(buf, -1, mime_GetBoundary(m));
			hbuf_AddBuffer(buf, 2, "\r\n");
			mime_RenderBufAdd(buf, *pb, 1);
			pb++;
		}
		hbuf_AddBuffer(buf, 2, "--");
		hbuf_AddBuffer(buf, -1, mime_GetBoundary(m));
		hbuf_AddBuffer(buf, 4, "--\r\n");
	} else {
		if (m->content) {
			const char *szText = mimetext_GetContent(m->content);

			if (mimetext_IsFile(m->content)) {	// Need to get the file to render it...!
				hbuf_AddBuffer(buf, -1, "Content in a file (");
				hbuf_AddBuffer(buf, -1, szText);
				hbuf_AddBuffer(buf, -1, ")\n");
			} else {							// Nice and easy...
				int len = mimetext_GetLength(m->content);
				hbuf_AddBuffer(buf, len, szText);
				hbuf_AddBuffer(buf, 2, "\r\n");
			}
		}
	}
}

API const char *mime_RenderBinary(MIME *m, int *pLen)
{
	const char *result = NULL;

	HBUF *buf = hbuf_New();
	mime_RenderBufAdd(buf, m, 1);

	if (pLen) *pLen = hbuf_GetLength(buf);
	result = hbuf_ReleaseBuffer(buf);
	hbuf_Delete(buf);

	return result;
}

API const char *mime_RenderHeaderHeap(MIME *m)
{
	return mimenvp_VRenderHeap(NULL, m->header);
}

API const char *mime_RenderContentBinary(MIME *m, int *pLen)
{
	const char *result = NULL;

	HBUF *buf = hbuf_New();
	mime_RenderBufAdd(buf, m, 0);

	if (pLen) *pLen = hbuf_GetLength(buf);
	hbuf_AddChar(buf, '\0');
	result = hbuf_ReleaseBuffer(buf);
	hbuf_Delete(buf);

	return result;
}

STATIC void mime_AddMime(MIME *m, MIME *sub)
{
	mime_VAdd((void***)&(m->parts), sub);					// Add it in

	return;
}

API void mime_ForceMultipart(MIME *m)
{
	mime_EnsureBoundary(m);							// We're going to need a boundary

	if (!mime_IsMultipart(m)) {						// Needs to be multipart so need a new part with old content
		const char *szOldType;
		MIME *sub;

		sub=mime_New(mime_GetContentType(m));
		sub->content=m->content;
		m->content=NULL;
		mime_AddMime(m, sub);
		szOldType=mime_GetContentType(m);
		if (strncasecmp(szOldType, "multipart/", 10))		// Make sure it has a multi-part type
			mime_SetContentType(m, "multipart/related");	// Use as a default one
	}
}

API MIME *mime_AddBodyPart(MIME *m, const char *szType, int len, const char *szContent)
// Adds in a new body part, making us multipart if necessary.
{
	MIME *sub;

	if (len < 0) len=strlen(szContent);

	if (!m->content && !mime_IsMultipart(m)) {		// Empty shell, just make this the body
		mime_SetContentType(m, szType);
		m->content=mimetext_NewText(len, szContent);
		return NULL;
	}

	mime_EnsureBoundary(m);							// We're going to need a boundary

	if (!mime_IsMultipart(m)) {						// Needs to be multipart so need a new part with old content
		const char *szOldType;

		sub=mime_New(mime_GetContentType(m));
		sub->content=m->content;
		m->content=NULL;
		mime_AddMime(m, sub);
		szOldType=mime_GetContentType(m);
		if (strncasecmp(szOldType, "multipart/", 10))		// Make sure it has a multi-part type
			mime_SetContentType(m, "multipart/related");	// Use as a default one
	}

	// Must now have multipart structure so just create a new part and add it
	sub=mime_New(szType);
	sub->content=mimetext_NewText(len, szContent);
	mime_AddMime(m, sub);

	return sub;
}

STATIC void mime_SetContent(MIME *m, long len, const char *szContent)
{
	mimetext_Delete(m->content);
	if (len < 0) len=strlen(szContent);

	m->content=mimetext_NewText(len, szContent);
}

API void mime_SetContentHeap(MIME *m, long len, const char *szContent)
{
	mimetext_Delete(m->content);
	if (len < 0) len=strlen(szContent);
	m->content=mimetext_NewHeap(len, szContent);
}

API int mime_GetBodyCount(MIME *m)
{
	int nCount=0;

	while (m->parts[nCount])
		nCount++;

	return nCount;
}

API MIME *mime_GetBodyPart(MIME *m, int nPart)
{
	MIME **pm = m->parts;

	while (--nPart && *pm) {
		pm++;
	}

	return *pm;
}

API const char *mime_GetBodyText(MIME *m)
{
	return mimetext_GetContent(m->content);
}

API int mime_GetBodyLength(MIME *m)
{
	return mimetext_GetLength(m->content);
}

static FILE *mimefp = NULL;
static mime_readfn *mimereadfn = NULL;

static int mime_GetChar()
{
	if (mimefp) return getc(mimefp);
	if (mimereadfn) return (*mimereadfn)();

	return EOF;
}

static const char *mime_GetLine()
// Gets a line from wherever input is coming from.
// Lines terminate with \n or EOF, \r is ignored.
// Returns	char*	Line read with no terminator (\n, \r etc)
//			NULL	EOF on a blank line.
{
	static char buf[10240];
	int i=0;
	int c=EOF;

	for (;;) {
		c=mime_GetChar();

		if (c == EOF) break;
		if (c == '\r') continue;
		if (c == '\n') break;
		buf[i++]=c;
		if (i == sizeof(buf)-1) break;
	}

	buf[i]='\0';

	return (i==0 && c==EOF) ? NULL : buf;
}

//{FILE *fp=fopen("/tmp/mimeread","a");fprintf(fp, "Reading body up to '%s'\n", szBoundary);fclose(fp);}

static const void *mime_ReadBodyChunk(int *pnLen, const char **pszName)
// Reads a section of body in 'chunk' format, returning the data on the heap.
// The length is returned in *pnLen.
// If the chunk has a name it is stored in *pszName (ptr to heap), otherwise a NULL is put there
// A chunk is a line with a hex length optionally followed by a space and a name
// Following a linefeed is then 'length' bytes, which are returned.
// Returns		NULL		No chunk (nothing else to read) - ignore pszName and pnLen
//				ptr			All done.  *pszName = NULL if there was no name

{
	const char *szLine;
	int nLen = 0;					// The total we're looking for
	const char *szBody;

	add_Init();

//{FILE *fp=fopen("/tmp/chunking","a");fprintf(fp, "About to chunk...\n");fclose(fp);}
	if ((szLine = mime_GetLine())) {
		int got;					// Number of items from header line, bytes got from later reads
		char szName[201];			// Temporary place for chunk name
		int i;

//{FILE *fp=fopen("/tmp/chunking","a");fprintf(fp, "Chunk line is: '%s'\n", szLine);fclose(fp);}
		got = sscanf(szLine, "%x %200s", &nLen, szName);
		if (got != 1 && got != 2) return NULL;
		if (got == 2) *pszName = strdup(szName);
				 else *pszName = NULL;

//{FILE *fp=fopen("/tmp/chunking","a");fprintf(fp, "Chunk of %d, name '%s' (got=%d)\n", nLen, *pszName, got);fclose(fp);}
		for (i=0;i<nLen;i++) {
			int c=mime_GetChar();

			if (c == EOF) break;			// EOF before end of chunk - just terminate here

			add_AddChar(c);
		}
	}

	mime_GetLine();									// Blank line after every chunk

//{FILE *fp=fopen("/tmp/chunking","a");fprintf(fp, "Read a chunk of length %d\n", add_GetLength());fclose(fp);}
	if (nLen) {										// We got something
		if (pnLen) *pnLen=add_GetLength();			// Caller wants the length
		else add_AddChar(0);						// Since they don't want the length, they must want an sz

		szBody = add_ReleaseBuffer();
	} else {
		szBody = NULL;
	}

	add_Finish();

	return szBody;
}

static const char *mime_ReadBody(int nContentLength, int *pnLen, const char *szBoundary, char *bTerminatesp)
// Reads a body from the source stopping on a boundary or nContentLength.
// Use -1 for nContentLength to ignore it.
// Use NULL for szBoundary to ignore that.
{
	const char *szBody = NULL;
	char bTerminates = 0;							// 1 if terminating boundary
	const char *szFullBoundary = szBoundary ? hprintf(NULL, "\r\n--%s", szBoundary) : NULL;
	const char *szBoundaryStart = szFullBoundary+2;	// Need to act like we've seen a LF on entry here
	const char *szBound = szBoundaryStart;			// Pointer to current place in boundary
	int nGotBoundary = 0;							// 1 once we've seen the boundary
	int bSeenDash = 0;								// 1 if we've seen the boundary and a dash
	int nCount = 0;

	add_Init();

	for (;;) {
		int c;

		if (nCount == nContentLength) {
			c=EOF;
		} else {
			c=mime_GetChar();
		}
		nCount++;

		if (nGotBoundary) {								// Seen boundary, look for '\n' or '--'
			if (c == EOF) c='\n';						// After boundary, treat EOF as '\n'
			if (c == '-') {
				if (bSeenDash) {
					bTerminates=1;
					break;								// Seen '--' so we're done
				}
				if (nGotBoundary == 1)					// First char after boundary
					bSeenDash=1;
				continue;
			}
			if (c == '\n') {
				break;
			}
			nGotBoundary++;
			bSeenDash=0;
			continue;
		}
		if (c == EOF) break;							// EOF in the middle of nowhere...
		if (szFullBoundary) {
			// This fails if there are possible boundary overlaps, e.g. boundary="--ABC-D"
			// If we get '--ABC--ABC-D' it'll fail to match, but this won't happen often!
			if (c != *szBound && szBound > szFullBoundary) {
				add_AddBuffer(szBound-szBoundaryStart, szFullBoundary);
				szBound=szBoundaryStart=szFullBoundary;
			}
			if (c==*szBound) {
				szBound++;
				if (!*szBound) {						// We've found a boundary!
					nGotBoundary=1;
				}
				continue;								// Avoid adding the character to the buffer
			}
			// Didn't match.  Any partial match has aleady been flushed
		}

		add_AddChar(c);
	}
	szDelete(szFullBoundary);

	// If we're here...
	// bTerminates == 1		We've seen the boundary and it was a final one
	// Otherwise, we've reached end of file.

	if (bTerminatesp) *bTerminatesp = bTerminates;

	if (pnLen) *pnLen=add_GetLength();				// Caller wants the length
	add_AddChar(0);									// Useful to ensure \0 terminated anyway

	szBody = add_ReleaseBuffer();

	return szBody;
}

STATIC const char *GetHeaderToken(char **pszBuf, char cDelim)
// NB. Fails where '"' immediately follows a word - e.g. Subject: This"quoted thing
// Accepts 'cDelim' as a delimiter as well as normal characters (used with ':', which is usually a letter)
// cDelim should be 0 if not required.
{
	static char charbuf[2]="?";
	static char cSavedChar = '\0';
	char *buf;
	char *chp;

	charbuf[1]='\0';

	if (cSavedChar) {						// Saved from last call
		*charbuf=cSavedChar;
		cSavedChar='\0';
		buf=charbuf;
	} else {
		buf=*pszBuf;
		while (isspace(*buf)) buf++;
		if (!*buf) return NULL;					// The end of the line...

		switch (*buf) {
			case '"':							// Quoted string is a single token
				chp=strchr(buf+1, '"');
				if (!chp) return NULL;			// Never ending string - treat as blank EOL
				*chp='\0';						// Delimit by zapping the closing '"'
				*pszBuf=chp+1;					// Point just after for next time
				buf++;							// Return the first char of the string
				break;
			case ':': case ';': case '=':		// Punctuation tokens
				*charbuf=*buf;
				*pszBuf=buf+1;
				buf=charbuf;
				break;
			default:							// Treat as bare words
				chp=buf;
				while (*chp && !strchr("\";=", *chp) && *chp!=cDelim)
					chp++;
				if (!*chp) {					// Very easy - end of the line
					*pszBuf=chp;
				} else {						// Trickier - need to save char for next time
					cSavedChar=*chp;			// Save until next time
					*chp='\0';
					*pszBuf=chp+1;
				}
				break;
		}
	}

	return buf;
}

STATIC MIME *mime_ReadX(const char *szOuterBoundary, char *pbFinal, char bWantBody);

STATIC MIME *mime_GetBodyX(MIME *mime, const char *szFinalBoundary, char *pbFinal)
// Given a mime structure that only has a header, fetches the body from the current input
{
	const char *szBoundary = NULL;
	char bMultipart = 0;
	const char *szType = NULL;

	if (!mime) return NULL;

	szType=mime_GetContentType(mime);
	bMultipart = szType && !strncasecmp(szType, "multipart/", 10);
	if (bMultipart) {
		szBoundary=mime_GetBoundary(mime);
		if (!szBoundary) bMultipart = 0;		// No boundary - illegal, treat as non-multipart
	}

	if (bMultipart) {							// Go and get multiple parts
		char bFinal;
		const char *szBody;
		int nLen;

		// First part is preamble, which we can ignore if we want to (spec says we should)
		szBody = mime_ReadBody(-1, &nLen, szBoundary, &bFinal);
		mime_SetContent(mime, nLen, szBody);
		while (!bFinal) {
			MIME *submime = mime_ReadX(szBoundary, &bFinal, 1);
			mime_AddMime(mime, submime);
		}
		if (pbFinal) *pbFinal=bFinal;
	} else {									// Just be happy with a single part
		const char *szTransferEncoding = mime_GetHeader(mime, "Transfer-Encoding");
		const char *szContentLength = mime_GetHeader(mime, "Content-Length");

		// Really, we shouldn't encounter chunked or Content-length unless we're at the outer
		// level of a single-part message.  Hence, both of these will return *pbFinal=1
		if (szTransferEncoding && !strcmp(szTransferEncoding, "chunked")) {		// Deal with chunks
			int nLen = 0;														// Total body length
			const char *szName = NULL;											// Name of body
			char *szBody = NULL;												// Text of body
			const char *szBodyChunk;											// Text of this chunk
			const char *szChunkName;											// Name of this chunk
			int nChunkLen;														// Go figure

			while ((szBodyChunk = (const char *)mime_ReadBodyChunk(&nChunkLen, &szChunkName))) {	// Read a chunk
				if (szBody) {													// Add to the body we already have
					RENEW(szBody, char, nChunkLen+nLen);						// Allocate space for it
					memcpy(szBody+nLen, szBodyChunk, nChunkLen);				// Copy in the new chunk
					nLen+=nChunkLen;											// Remember how big we are
					szDelete(szBodyChunk);										// Lose the original chunk
				} else {
					szBody=(char*)szBodyChunk;									// Starting with this chunk
					nLen=nChunkLen;												// Remember how big it is
				}
				if (szChunkName) {												// We have a name
					if (szName)													// Had one before so ignore this one
						szDelete(szChunkName);
					else
						szName=szChunkName;										// Set the name
				}
			}
			mime_SetContent(mime, nLen, szBody);
			if (szName) {
				mime_SetHeader(mime, "X-ChunkName", szName);
			}
			szDelete(szName);
			if (pbFinal) *pbFinal=1;
		} else if (szContentLength) {								// Got a content length
			int nContentLength = atoi(szContentLength);
			const char *szBody;
			int nLen;

			// TODO: Get a lump of length 'content length' as single body part
			szBody = mime_ReadBody(nContentLength, &nLen, szFinalBoundary, pbFinal);
			mime_SetContent(mime, nLen, szBody);
		} else {													// Non chunked, non contentlengthed
			const char *szBody;
			int nLen;
			szBody = mime_ReadBody(-1, &nLen, szFinalBoundary, pbFinal);
			mime_SetContent(mime, nLen, szBody);
		}
	}

	return mime;
}

STATIC MIME *mime_ReadX(const char *szOuterBoundary, char *pbFinal, char bWantBody)
	// Reads the MIME input, getting a header and body delimited by the boundary passed.
	// Recursively calls itself to get inner 'attachments'.
	// If szBoundary is NULL then only stops on EOF or the final boundary of the internal header
	// if it is multi-part.
	// No longer acceptable...  We need to terminate on 'Content-Length' or 'last chunk'
	// *pbFinal gets 1 if szBoundary was read and it was final (had a trailing '--')
{
	const char *szLine;
	char *szHeader = NULL;
	int nErr=0;
	const char *szBoundary = NULL;
	MIMENVP **pnvpHeader=mimenvp_VNew();		// Vector of header entries
	char bMultipart;
	MIME *mime;
	const char *szType;
	const char *szErrPos = NULL;				// Pointer into line where error is
	const char *szErr = NULL;				// Fixed text part of error
	const char *szName;						// Name of header item being parsed


//extern void Log(const char *fmt, ...);
//{FILE *fp=fopen("/tmp/mimeread","a");fprintf(fp, "mime_ReadX('%s', [%d], %d)\n", szOuterBoundary?szOuterBoundary:"(NULL)", pbFinal, bWantBody);fclose(fp);}
	for (;;) {
		szLine=mime_GetLine();
//Log("Got a line... (%s)", szLine);
//{FILE *fp=fopen("/tmp/mimeread","a");fprintf(fp, "(H=%s) GOT(%d): '%s'\n", szHeader, szLine?*szLine:-1, szLine);fclose(fp);}
		if (szHeader &&							// We have a header being built
				(!*szLine || !isspace(*szLine))) {	// Need to flush it
			MIMENVP *nvp=NULL;					// Current header element
			char *szBuf = szHeader;
			const char *szColon, *szValue;

//			for (;;) {							// I can use 'break' instead of goto...
				szName = GetHeaderToken(&szBuf, ':');
				if (!szName) { nErr=0; break; }		// Nothing in the line - perhaps just a comment
				szColon = GetHeaderToken(&szBuf, '\0');
				if (!szColon || *szColon != ':') { nErr=1; szErr="colon expected"; break; }	// Colon expected
				if ((strcasecmp(szName, "Content-Type") &&	// Special case - always parse content-type so we have boundary
						!mime_bParseHeaderValues) ||	// Don't attempt to parse header values - just use rest of line
					(!strcasecmp(szName, "User-Agent") ||	// Also 'User-Agent: ' is never variable parsed
						!strcasecmp(szName, "Cache-Control") ||	// ... or 'Cache-Control: '
						!strcasecmp(szName, "Keep-Alive") ||		// ... or 'Keep-Alive: '
						!strcasecmp(szName, "Set-Cookie") ||		// ... or 'Set-Cookie: '
						!strcasecmp(szName, "Cookie") ||			// ... or 'Cookie: '
						!strcasecmp(szName, "Range") ||			// ... or 'Range: '
						!strcasecmp(szName, "Referer") ||			// ... or 'Referer: '
						!strcasecmp(szName, "Prefer") ||			// ... or 'Prefer: '
						!strncasecmp(szName, "X-", 2) ||			// ... or anything starting 'X-'
						!strcasecmp(szName, "Accept"))) {			// ... or 'Accept: '
					szBuf = (char*)SkipSpaces(szBuf);
					nvp=mimenvp_VAdd(&pnvpHeader, szName, szBuf);	// Take rest of line
				} else {
					szValue = GetHeaderToken(&szBuf, '\0');
					if (!szValue) { nErr=2; szErr="No value"; break; }	// No value
					nvp=mimenvp_VAdd(&pnvpHeader, szName, szValue);
					for (;;) {
						const char *szSemi, *szName, *szEquals, *szValue;

						szSemi = GetHeaderToken(&szBuf, '\0');
						if (!szSemi) break;				// All done
						if (*szSemi != ';') { nErr=3; szErr="semi-colon expected"; break; }	// Semi-colon expected
						szName=GetHeaderToken(&szBuf, '\0');
						if (!szName) break;				// Semi-colon with nothing after - count as legal
						szEquals=GetHeaderToken(&szBuf, '\0');
						if (!szEquals || *szEquals != '=') { nErr=4; szErr="equals expected"; break;} 	// Equals expected
						szValue=GetHeaderToken(&szBuf, '\0');
						if (!szValue) { nErr=5; szErr="no value"; break; }		// No value
						mimenvp_SetParameter(nvp, szName, szValue);
					}
				}
//				break;
//			}
			szDelete(szHeader);
			szHeader=NULL;
			if (nErr) {
				szErrPos = szBuf;
				break;					// Propogate errors
			}
		}
//{FILE *fp=fopen("/tmp/mimeread","a");fprintf(fp, "Line='%s' (*line=%d)\n", szLine,szLine?*szLine:-1);fclose(fp);}

		if (!*szLine) break;					// Finished the header
		if (!isspace(*szLine)) {				// A new header element
			szHeader=strdup(szLine);			// Start header with this
		} else {
			while (isspace(*szLine))
				szLine++;
			szLine--;
			szHeader=strappend(szHeader, szLine);
		}
	}

	if (nErr) {									// Neater to drop out here if we have an error
//{FILE *fp=fopen("/tmp/mimeread","a");fprintf(fp, "nErr=%d\n", nErr);fclose(fp);}
		mime_SetLastError(nErr, "%s at >>>%s<<< while parsing %s", szErr, szErrPos, szName?szName:"header");
//		nvp_VDelete(pnvp);
		return NULL;
	}

	mime = mime_NewShell();
	mime->header=pnvpHeader;

//{const char *szH=mime_RenderHeap(mime);printf("Header...\n%s", szH);szDelete(szH);}
	if (bWantBody) {
		szType=mime_GetContentType(mime);
		bMultipart = szType && !strncasecmp(szType, "multipart/", 10);
//{FILE *fp=fopen("/tmp/mimeread","a");fprintf(fp, "Fetching body - type='%s', multi=%d\n", szType, bMultipart);fclose(fp);}
		if (bMultipart) {
			szBoundary=mime_GetBoundary(mime);
			if (!szBoundary) bMultipart = 0;		// No boundary - illegal, treat as non-multipart
		}

//{FILE *fp=fopen("/tmp/mimeread","a");fprintf(fp, "Fetching body - multi=%d, boundary = '%s', Outer = '%s'\n", bMultipart, szBoundary, szOuterBoundary);fclose(fp);}
		if (bMultipart) {							// Go and get multiple parts
			char bFinal;
			const char *szBody;
			int nLen;

			// First part is preamble, which we can ignore if we want to (spec says we should)
			szBody = mime_ReadBody(-1, &nLen, szBoundary, &bFinal);
			mime_SetContent(mime, nLen, szBody);
			while (!bFinal) {
				MIME *submime = mime_ReadX(szBoundary, &bFinal, 1);
				mime_AddMime(mime, submime);
			}
			if (pbFinal) *pbFinal=bFinal;
		} else {									// Just be happy with a single part
			const char *szTransferEncoding = mime_GetHeader(mime, "Transfer-Encoding");
			const char *szContentLength = mime_GetHeader(mime, "Content-Length");

			// Really, we shouldn't encounter chunked or Content-length unless we're at the outer
			// level of a single-part message.  Hence, both of these will return *pbFinal=1
			if (szTransferEncoding && !strcmp(szTransferEncoding, "chunked")) {		// Deal with chunks
				int nLen = 0;														// Total body length
				const char *szName = NULL;											// Name of body
				char *szBody = NULL;												// Text of body
				const char *szBodyChunk;											// Text of this chunk
				const char *szChunkName;											// Name of this chunk
				int nChunkLen;														// Go figure

				while ((szBodyChunk = (const char *)mime_ReadBodyChunk(&nChunkLen, &szChunkName))) {	// Read a chunk
					if (szBody) {													// Add to the body we already have
						RENEW(szBody, char, nChunkLen+nLen);						// Allocate space for it
						memcpy(szBody+nLen, szBodyChunk, nChunkLen);				// Copy in the new chunk
						nLen+=nChunkLen;											// Remember how big we are
						szDelete(szBodyChunk);										// Lose the original chunk
					} else {
						szBody=(char*)szBodyChunk;									// Starting with this chunk
						nLen=nChunkLen;												// Remember how big it is
					}
					if (szChunkName) {												// We have a name
						if (szName)													// Had one before so ignore this one
							szDelete(szChunkName);
						else
							szName=szChunkName;										// Set the name
					}
				}
				mime_SetContent(mime, nLen, szBody);
				if (szName) {
					mime_SetHeader(mime, "X-ChunkName", szName);
				}
				szDelete(szName);
				if (pbFinal) *pbFinal=1;
			} else if (szContentLength) {								// Got a content length
				int nContentLength = atoi(szContentLength);
				const char *szBody;
				int nLen;

				// TODO: Get a lump of length 'content length' as single body part
				szBody = mime_ReadBody(nContentLength, &nLen, szOuterBoundary, pbFinal);
				mime_SetContent(mime, nLen, szBody);
			} else {													// Non chunked, non contentlengthed
				const char *szBody;
				int nLen;
				szBody = mime_ReadBody(-1, &nLen, szOuterBoundary, pbFinal);
				mime_SetContent(mime, nLen, szBody);
			}
		}
	}

	return mime;
}

API MIME *mime_Read()
{
	return mime_ReadX(NULL, NULL, 1);
}

API MIME *mime_ReadHeader()
{
	return mime_ReadX(NULL, NULL, 0);
}

API MIME *mime_ReadBody(MIME *mime)
{
	return mime_GetBodyX(mime, NULL, NULL);
}

API MIME *mime_ReadFp(FILE *fp)
{
	mimefp=fp;
	return mime_Read();
}

API MIME *mime_ReadFile(const char *szFilename)
{
	MIME *mime=NULL;
	FILE *fp=fopen(szFilename, "r");

	if (fp) {
		mime=mime_ReadFp(fp);
		fclose(fp);
		return mime;
	}

	return mime;
}

API MIME *mime_ReadFn(mime_readfn fn)
{
	mimefp=NULL;
	mimereadfn = fn;
	return mime_Read();
}

API MIME *mime_ReadHeaderFn(mime_readfn fn)
{
	mimefp=NULL;
	mimereadfn = fn;
	return mime_ReadHeader();
}

