#ifndef _HBUF_H
#define _HBUF_H

typedef struct HBUF {
	char *buff;					// Main buffer
	int len;					// length of main buffer
	char *temp;					// Temporary buffer
	int i;						// Index into temporary buffer
	int size;					// Size of temporary buffer
} HBUF;

typedef struct HLIST {
	struct HLIST	*prev;		// Previous item (or last if this is the header)
	struct HLIST	*next;		// Next item (or first if this is the header)
	const void		*data;		// data (or name if this is the header)
	long long		len;		// Length of item (or total if this is the header
} HLIST;

#include <hbuf.proto>
#endif
