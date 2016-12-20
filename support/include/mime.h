
#ifndef __MIME_H
#define __MIME_H

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

#include "mime.proto"

#endif
