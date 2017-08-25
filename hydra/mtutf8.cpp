#if 0
./makeh $0
exit 0
#endif

#include "mtutf8.h"

#define API
#define STATIC static

#include <stdio.h>						// For NULL and heapstrings needs it
#include <string.h>						// strdup()

#include <hbuf.h>
#include <heapstrings.h>

API const char *utf8_FromCodepoint(long code, int *plen)
// Returns a string that is the UTF-8 representation of the code point passed
// The string will have a terminating \0, but if plen != NULL then it'll receive the length (without the trailing \0) too.
{
	static char buf[6];
	int len=0;

	if (code <= 0x7f) {					// 1 byte - up to 7 bits
		len = 1;
		buf[0] = (char)code;
		buf[1] = '\0';
	} else if (code <= 0x7ff) {			// 2 bytes - up to 11 bits
		len = 2;
		buf[0] = 0xc0 | ((code >> 6) & 0x3f);
		buf[1] = 0x80 | (code & 0x3f);
		buf[2] = '\0';
	} else if (code <= 0xffff) {		// 3 bytes - up to 16 bits
		len = 3;
		buf[0] = 0xe0 | ((code >> 12) & 0x3f);
		buf[1] = 0x80 | ((code >> 6) & 0x3f);
		buf[2] = 0x80 | (code & 0x3f);
		buf[3] = '\0';
	} else if (code <= 0x1fffffL) {		// 4 bytes - up to 21 bits
		len = 4;
		buf[0] = 0xf0 | ((code >> 18) & 0x3f);
		buf[1] = 0x80 | ((code >> 12) & 0x3f);
		buf[2] = 0x80 | ((code >> 6) & 0x3f);
		buf[3] = 0x80 | (code & 0x3f);
		buf[4] = '\0';
	} else if (code <= 0x3ffffffL) {	// 5 bytes - up to 26 bits
		len = 5;
		buf[0] = 0xf8 | ((code >> 24) & 0x3f);
		buf[1] = 0x80 | ((code >> 18) & 0x3f);
		buf[2] = 0x80 | ((code >> 12) & 0x3f);
		buf[3] = 0x80 | ((code >> 6) & 0x3f);
		buf[4] = 0x80 | (code & 0x3f);
		buf[5] = '\0';
	}

	if (plen) *plen = len;

	return buf;
}

API long utf8_Codepoint(const char **ptext)
// Returns a unicode codepoint from a single character that text points at.
// Returns		0...		Codepoint (*ptext incremented past last byte)
//				-1..-6		Error at byte 1..4 (*ptext unchanged)
{
	const unsigned char *text = *(const unsigned char **)ptext;

	int len;
	int mask;
	int i;
	long min;				// Minimum allowed codepoint for number of bytes used

	unsigned char c = *text;
	if ((c & 0x80) == 0)			{ len = 1; mask=0x7f; min = 0; }
	else if ((c & 0xe0) == 0xc0)	{ len = 2; mask=0x1f; min = 1 << 7; }
	else if ((c & 0xf0) == 0xe0)	{ len = 3; mask=0x0f; min = 1 << 11; }
	else if ((c & 0xf8) == 0xf0)	{ len = 4; mask=0x07; min = 1 << 16; }
	else if ((c & 0xfc) == 0xf8)	{ len = 5; mask=0x03; min = 1 << 21; }
	else if ((c & 0xfe) == 0xfc)	{ len = 6; mask=0x01; min = 1 << 26; }
	else return -1;								// Illegal first byte

	for (i = 1; i < len; i++) {					// Continuation bytes must be 0b10xxxxxx
		if ((text[i] & 0xc0) != 0x80) {
			return -i-1;
		}
	}

	long codepoint = 0;
	for (i = 0; i < len; i++) {
		codepoint = (codepoint << 6) | (text[i] & mask);
		mask = 0x3f;
	}

	*ptext += len;

	return codepoint >= min ? codepoint : -1;
}

API const char *utf8_IsValidString(int len, const char *string)
// Checks a string for validity.  Len is the length in bytes or -1 to stop at the first '\0'
// Returns	NULL			String is valid
//			const char *	Pointer to first invalid byte
{
	if (string) {
		if (len < 0) len = strlen(string);

		const char *end = string+len;
		while (string < end) {
			long code = utf8_Codepoint(&string);

			if ((code >= 0xd800 && code <= 0xdfff) || code > 0x10ffff)			// Invalid codepoints
				code = -1;

			if (code < 0) {
				return string-code-1;
			}
		}
	}

	return NULL;
}
