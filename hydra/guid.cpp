
#include <unistd.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>

#include "mtmacro.h"

#include "guid.h"

#define STATIC static
#define API

#if 0
// START HEADER
typedef struct guid {
	unsigned long   time_low;
	unsigned short  time_mid;
	unsigned short  time_hi_and_version;
	unsigned char   clock_seq_hi_and_reserved;
	unsigned char   clock_seq_low;
	unsigned char   node[6];
} guid;
// END HEADER
#endif

static char _szHex[]="0123456789ABCDEF";

// [RJ] The md5 source code was found on the internet

/* Define the state of the MD5 Algorithm. */
#if 0
// START HEADER

typedef unsigned char md5_byte_t; /* 8-bit byte */
typedef unsigned int md5_word_t; /* 32-bit word */

typedef struct md5_state_s {
    md5_word_t count[2];	/* message length in bits, lsw first */
    md5_word_t abcd[4];		/* digest buffer */
    md5_byte_t buf[64];		/* accumulate block */
} MD5;
// END HEADER
#endif

#define T1 0xd76aa478
#define T2 0xe8c7b756
#define T3 0x242070db
#define T4 0xc1bdceee
#define T5 0xf57c0faf
#define T6 0x4787c62a
#define T7 0xa8304613
#define T8 0xfd469501
#define T9 0x698098d8
#define T10 0x8b44f7af
#define T11 0xffff5bb1
#define T12 0x895cd7be
#define T13 0x6b901122
#define T14 0xfd987193
#define T15 0xa679438e
#define T16 0x49b40821
#define T17 0xf61e2562
#define T18 0xc040b340
#define T19 0x265e5a51
#define T20 0xe9b6c7aa
#define T21 0xd62f105d
#define T22 0x02441453
#define T23 0xd8a1e681
#define T24 0xe7d3fbc8
#define T25 0x21e1cde6
#define T26 0xc33707d6
#define T27 0xf4d50d87
#define T28 0x455a14ed
#define T29 0xa9e3e905
#define T30 0xfcefa3f8
#define T31 0x676f02d9
#define T32 0x8d2a4c8a
#define T33 0xfffa3942
#define T34 0x8771f681
#define T35 0x6d9d6122
#define T36 0xfde5380c
#define T37 0xa4beea44
#define T38 0x4bdecfa9
#define T39 0xf6bb4b60
#define T40 0xbebfbc70
#define T41 0x289b7ec6
#define T42 0xeaa127fa
#define T43 0xd4ef3085
#define T44 0x04881d05
#define T45 0xd9d4d039
#define T46 0xe6db99e5
#define T47 0x1fa27cf8
#define T48 0xc4ac5665
#define T49 0xf4292244
#define T50 0x432aff97
#define T51 0xab9423a7
#define T52 0xfc93a039
#define T53 0x655b59c3
#define T54 0x8f0ccc92
#define T55 0xffeff47d
#define T56 0x85845dd1
#define T57 0x6fa87e4f
#define T58 0xfe2ce6e0
#define T59 0xa3014314
#define T60 0x4e0811a1
#define T61 0xf7537e82
#define T62 0xbd3af235
#define T63 0x2ad7d2bb
#define T64 0xeb86d391

STATIC void md5_process(MD5 *pms, const md5_byte_t *data /*[64]*/)
{
    md5_word_t
	a = pms->abcd[0], b = pms->abcd[1],
	c = pms->abcd[2], d = pms->abcd[3];
    md5_word_t t;

#ifndef ARCH_IS_BIG_ENDIAN
# define ARCH_IS_BIG_ENDIAN 1	/* slower, default implementation */
#endif
#if ARCH_IS_BIG_ENDIAN

    /*
     * On big-endian machines, we must arrange the bytes in the right
     * order.  (This also works on machines of unknown byte order.)
     */
    md5_word_t X[16];
    const md5_byte_t *xp = data;
    int i;

    for (i = 0; i < 16; ++i, xp += 4)
	X[i] = xp[0] + (xp[1] << 8) + (xp[2] << 16) + (xp[3] << 24);

#else  /* !ARCH_IS_BIG_ENDIAN */

    /*
     * On little-endian machines, we can process properly aligned data
     * without copying it.
     */
    md5_word_t xbuf[16];
    const md5_word_t *X;

    if (!((data - (const md5_byte_t *)0) & 3)) {
	/* data are properly aligned */
	X = (const md5_word_t *)data;
    } else {
	/* not aligned */
	memcpy(xbuf, data, 64);
	X = xbuf;
    }
#endif

#define ROTATE_LEFT(x, n) (((x) << (n)) | ((x) >> (32 - (n))))

    /* Round 1. */
    /* Let [abcd k s i] denote the operation
       a = b + ((a + F(b,c,d) + X[k] + T[i]) <<< s). */
#define F(x, y, z) (((x) & (y)) | (~(x) & (z)))
#define SET(a, b, c, d, k, s, Ti)\
  t = a + F(b,c,d) + X[k] + Ti;\
  a = ROTATE_LEFT(t, s) + b
    /* Do the following 16 operations. */
    SET(a, b, c, d,  0,  7,  T1);
    SET(d, a, b, c,  1, 12,  T2);
    SET(c, d, a, b,  2, 17,  T3);
    SET(b, c, d, a,  3, 22,  T4);
    SET(a, b, c, d,  4,  7,  T5);
    SET(d, a, b, c,  5, 12,  T6);
    SET(c, d, a, b,  6, 17,  T7);
    SET(b, c, d, a,  7, 22,  T8);
    SET(a, b, c, d,  8,  7,  T9);
    SET(d, a, b, c,  9, 12, T10);
    SET(c, d, a, b, 10, 17, T11);
    SET(b, c, d, a, 11, 22, T12);
    SET(a, b, c, d, 12,  7, T13);
    SET(d, a, b, c, 13, 12, T14);
    SET(c, d, a, b, 14, 17, T15);
    SET(b, c, d, a, 15, 22, T16);
#undef SET

     /* Round 2. */
     /* Let [abcd k s i] denote the operation
          a = b + ((a + G(b,c,d) + X[k] + T[i]) <<< s). */
#define G(x, y, z) (((x) & (z)) | ((y) & ~(z)))
#define SET(a, b, c, d, k, s, Ti)\
  t = a + G(b,c,d) + X[k] + Ti;\
  a = ROTATE_LEFT(t, s) + b
     /* Do the following 16 operations. */
    SET(a, b, c, d,  1,  5, T17);
    SET(d, a, b, c,  6,  9, T18);
    SET(c, d, a, b, 11, 14, T19);
    SET(b, c, d, a,  0, 20, T20);
    SET(a, b, c, d,  5,  5, T21);
    SET(d, a, b, c, 10,  9, T22);
    SET(c, d, a, b, 15, 14, T23);
    SET(b, c, d, a,  4, 20, T24);
    SET(a, b, c, d,  9,  5, T25);
    SET(d, a, b, c, 14,  9, T26);
    SET(c, d, a, b,  3, 14, T27);
    SET(b, c, d, a,  8, 20, T28);
    SET(a, b, c, d, 13,  5, T29);
    SET(d, a, b, c,  2,  9, T30);
    SET(c, d, a, b,  7, 14, T31);
    SET(b, c, d, a, 12, 20, T32);
#undef SET

     /* Round 3. */
     /* Let [abcd k s t] denote the operation
          a = b + ((a + H(b,c,d) + X[k] + T[i]) <<< s). */
#define H(x, y, z) ((x) ^ (y) ^ (z))
#define SET(a, b, c, d, k, s, Ti)\
  t = a + H(b,c,d) + X[k] + Ti;\
  a = ROTATE_LEFT(t, s) + b
     /* Do the following 16 operations. */
    SET(a, b, c, d,  5,  4, T33);
    SET(d, a, b, c,  8, 11, T34);
    SET(c, d, a, b, 11, 16, T35);
    SET(b, c, d, a, 14, 23, T36);
    SET(a, b, c, d,  1,  4, T37);
    SET(d, a, b, c,  4, 11, T38);
    SET(c, d, a, b,  7, 16, T39);
    SET(b, c, d, a, 10, 23, T40);
    SET(a, b, c, d, 13,  4, T41);
    SET(d, a, b, c,  0, 11, T42);
    SET(c, d, a, b,  3, 16, T43);
    SET(b, c, d, a,  6, 23, T44);
    SET(a, b, c, d,  9,  4, T45);
    SET(d, a, b, c, 12, 11, T46);
    SET(c, d, a, b, 15, 16, T47);
    SET(b, c, d, a,  2, 23, T48);
#undef SET

     /* Round 4. */
     /* Let [abcd k s t] denote the operation
          a = b + ((a + I(b,c,d) + X[k] + T[i]) <<< s). */
#define I(x, y, z) ((y) ^ ((x) | ~(z)))
#define SET(a, b, c, d, k, s, Ti)\
  t = a + I(b,c,d) + X[k] + Ti;\
  a = ROTATE_LEFT(t, s) + b
     /* Do the following 16 operations. */
    SET(a, b, c, d,  0,  6, T49);
    SET(d, a, b, c,  7, 10, T50);
    SET(c, d, a, b, 14, 15, T51);
    SET(b, c, d, a,  5, 21, T52);
    SET(a, b, c, d, 12,  6, T53);
    SET(d, a, b, c,  3, 10, T54);
    SET(c, d, a, b, 10, 15, T55);
    SET(b, c, d, a,  1, 21, T56);
    SET(a, b, c, d,  8,  6, T57);
    SET(d, a, b, c, 15, 10, T58);
    SET(c, d, a, b,  6, 15, T59);
    SET(b, c, d, a, 13, 21, T60);
    SET(a, b, c, d,  4,  6, T61);
    SET(d, a, b, c, 11, 10, T62);
    SET(c, d, a, b,  2, 15, T63);
    SET(b, c, d, a,  9, 21, T64);
#undef SET

     /* Then perform the following additions. (That is increment each
        of the four registers by the value it had before this block
        was started.) */
    pms->abcd[0] += a;
    pms->abcd[1] += b;
    pms->abcd[2] += c;
    pms->abcd[3] += d;
}

API MD5 *md5_New()
{
	MD5 *pms = NEW(MD5, 1);

    pms->count[0] = pms->count[1] = 0;
    pms->abcd[0] = 0x67452301;
    pms->abcd[1] = 0xefcdab89;
    pms->abcd[2] = 0x98badcfe;
    pms->abcd[3] = 0x10325476;

	return pms;
}

API void md5_Delete(MD5 *md5)
{
	if (md5)
		free((void*)md5);
}

API void md5_Append(MD5 *pms, int nbytes, const char* data)
{
	if (!nbytes) return;

	if (nbytes < 0) nbytes = strlen(data);

    const md5_byte_t *p = (md5_byte_t*)data;
    int left = nbytes;
    int offset = (pms->count[0] >> 3) & 63;
    md5_word_t nbits = (md5_word_t)(nbytes << 3);

    /* Update the message length. */
    pms->count[1] += nbytes >> 29;
    pms->count[0] += nbits;
    if (pms->count[0] < nbits)
		pms->count[1]++;

    /* Process an initial partial block. */
    if (offset) {
		int copy = (offset + nbytes > 64 ? 64 - offset : nbytes);

		memcpy(pms->buf + offset, p, copy);
		if (offset + copy < 64)
			return;
		p += copy;
		left -= copy;
		md5_process(pms, pms->buf);
    }

    /* Process full blocks. */
    for (; left >= 64; p += 64, left -= 64)
		md5_process(pms, p);

    /* Process a final partial block. */
    if (left)
		memcpy(pms->buf, p, left);
}

API const char *md5_FinishBin(MD5 *pms)
// Returns a pointer to a static buffer of 16 bytes
{
	static md5_byte_t digest[16];

    static const md5_byte_t pad[64] = {
	0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
    };
    md5_byte_t data[8];
    int i;

    /* Save the length before padding. */
    for (i = 0; i < 8; ++i)
	data[i] = (md5_byte_t)(pms->count[i >> 2] >> ((i & 3) << 3));
    /* Pad to 56 bytes mod 64. */
    md5_Append(pms, ((55 - (pms->count[0] >> 3)) & 63) + 1, (const char *)pad);
    /* Append the length. */
    md5_Append(pms, 8, (const char *)data);
    for (i = 0; i < 16; ++i)
	digest[i] = (md5_byte_t)(pms->abcd[i >> 2] >> ((i & 3) << 3));

	return (const char *)digest;
}

API const char *md5_Finish(MD5 *pms)
// Returns a \0 terminated static string of 32 characters being the lower case hex representation
{
	static char result[33];
	static char *hex = "0123456789abcdef";

	const unsigned char *digest = (unsigned char *)md5_FinishBin(pms);
	char *p=result;
	for (int i=0; i<16; i++) {
		unsigned char c = *digest++;
		p[0] = hex[c >> 4];
		p[1] = hex[c & 0x0f];
		p+=2;
	}

	return result;
}

// [RJ] End of md5 code written elsewhere, everything from here on in is my fault!

API const char *md5_Buf(const char *ptr, int nLen)
{
	const char *digest;

	MD5 *state = md5_New();
	md5_Append(state, nLen, ptr);
	digest = md5_FinishBin(state);
	md5_Delete(state);

	return digest;
}

STATIC guid *guid_FromSeedX(const char *szText, int nLen, unsigned int nType)
// Uses the string passed (up to '\0' if nLen=-1, otherwise exactly 'nLen'
// bytes to generate a guid.
// The 'nType' should be 3 for a namespace based UUID or 4 for pseudo-random
{
	static guid g;
	const char *hash;
	unsigned int nTypeMask = (nType << 4) & 0xf0;

	if (nLen == -1) nLen = strlen(szText);

	hash=md5_Buf(szText, nLen);

	memcpy((void*)&g, hash, 16);
	// NB. Although the comments say 'top 2' and 'top 4', I'm actually setting
	// bits in the middle as the spec is inferring a big-endian machine
	g.clock_seq_hi_and_reserved &= 0xff3f;
	g.clock_seq_hi_and_reserved |= 0x0080;		// Set top 2 to be schema (1)
	g.time_hi_and_version &= 0xff0f;
	g.time_hi_and_version |= nTypeMask;			// Set top 4 to be version# (4)

	return &g;
}

API guid *guid_FromSeed(const char *szText, int nLen)
// Create a type 3 GUID from a seed string.
{
	return guid_FromSeedX(szText, nLen, 3);
}

//! \brief Generates a unique guid structure
//! This is the basic guid generating function from which all others
//! get their base information.  It returns a pointer to a static
//! structure so copy the result out the way if you want it.
//! NB. Just in case it's important, this function IS NOT THREAD SAFE
//!     The best way to fix this is probably to wrap the whole lot up as
//!     a critical section.
API guid *guid_Gen()
// Generate a type 4 GUID (pseudo-random)
{
	static int bFirstTime = 1;
	static int count;
	static struct {
		char	sSignature[16];
		short	nPid;
		char	sHost[10];
		time_t	nTime;
		int		nCount;
	} seed;

	if (bFirstTime) {
		count=time(NULL);
		strcpy(seed.sSignature, "RogAndBeckyJane.");
		seed.nPid=getpid();
		gethostname(seed.sHost, 11);	// Will tend to overwrite 1 char of time
		bFirstTime = 0;
	}

	seed.nTime=time(NULL);
	seed.nCount=count++;

	return guid_FromSeedX((char*)&seed, sizeof(seed), 4);
}

STATIC void _guid_QuickHex(char *chp, unsigned char c)
{
	chp[0]=_szHex[c>>4];
	chp[1]=_szHex[c&0x0f];
}

//! \brief Return the textual form of a GUID
//! If 'g' is passed as non-NULL, it should be a pointer to a
//! guid previously generated using guid_Gen().  If it is NULL, a new
//! guid will be generated (using guid_Gen()).
//! \param g	Pointer to a previously created guid or NULL to generate one
API const char *guid_ToText(guid *g)
// Convert a guid* to a string of the form XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX
// Calling this as guid_ToText(NULL) is the easiest way to generate a random GUID string.
// The result is a static string - don't free it!
{
	static char cResult[37] = "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX";
	char *cDst=cResult;
	int i;

	if (!g) g=guid_Gen();
	for (i=0;i<16;i++) {
		_guid_QuickHex(cDst, ((unsigned char *)g)[i]);
		cDst+=2;
		if (i==3 || i==5 || i==7 || i==9)
			cDst++;
	}

	return cResult;
}

//! \brief Returns an internal format GUID from the textual version
//! If the format of the string is anything other than as returned from
//! guid_ToText then NULL is returned.  A terminating NULL on the
//! string passed is not necessary.
API guid *guid_FromText(const char *szText)
// Converts a XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX strng to a guid*
// Must be in the right format or NULL will be returned.  Ignores any rubbish after the string.
{
	static unsigned char g[16];
	int i;

	// First check for dashes and length
	if (strlen(szText) < 36 || szText[8] != '-' ||
			szText[13] != '-' || szText[18] != '-' || szText[23] != '-') {
		return NULL;
	}

	for (i=0;i<16;i++) {
		char a=*szText++;
		char b=*szText++;
		const char *pa, *pb;
		if (a>='a' && a<='f') a-=('a'-'A');
		if (b>='a' && b<='f') b-=('a'-'A');

		pa=strchr(_szHex, a);							// Find the hex char
		pb=strchr(_szHex, b);
		if (!pa || !pb) return NULL;					// Not a hex char
		a=pa-_szHex;									// Map back to index
		b=pb-_szHex;
		g[i]=(a<<4)|b;
		if (i==3 || i==5 || i==7 || i==9) szText++;		// Skip the '-'s
	}

	return (guid*)g;
}

API int guid_Compare(const guid *a, const guid *b)
// Although GUIDs are inherently unordered, returns the comparison between two of them.
{
	const char *ca=(char *)a;
	const char *cb=(char *)b;
	int i;

	for (i=0;i<16;i++) {
		int cmp=*cb++-*ca++;
		if (cmp) return cmp;
	}

	return 0;
}
