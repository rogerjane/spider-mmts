#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/fcntl.h>

#include <mtmacro.h>
#include <hbuf.h>

// 26-06-16 RJ 0.01 Added hbuf_AddHeap() and slight tidy of hbuf_AddBuffer

#define STATIC static

// This little 'hbuf_' section handles adding single bytes to a heap-based buffer.  This could be achieved
// by adding a char at a time to the heap and re-allocing on each one but it would be horribly inefficient.
// Here, we use a seperate buffer to build the string and add it to the string whenever it is full.
// Usage:
//		HBUF *hbuf=hbuf_New();		// Get yourself a handle
//		hbuf_SetBufferSize()		// Optionally call to set a specific buffer size (default is 1024)
//		Repeatedly call hbuf_AddChar() to add characters (bytes) or hbuf_AddBuffer() to add bigger bits
//		Call hbuf_GetLength() to get the number of bytes added so far
//		Call hbuf_GetBuffer() to get a pointer to the buffer (DO NOT free() this!!!)
//		Call hbuf_ReleaseBuffer() to take control of the buffer (adding will then add to a new one)
//		Call hbuf_Delete() to tidy up and free memory.
// Notes:
//		If you never call hbuf_AddChar() then hbuf_GetBuffer() and hbuf_ReleaseBuffer() will return NULL.
//		If you want the resultant buffer to be null terminated, call hbuf_AddChar(0) before the above functions.
//		Call hbuf_GetLength() BEFORE calling hbuf_ReleaseBuffer() or you'll get 0!

// Functions in here are NULL-safe in relation to secondary arguments, but passing a NULL HBUF will usually cause the world to end.

HBUF *hbuf_New()
// Create a new hbuf and return a pointer to it
{
	HBUF *h=NEW(HBUF, 1);

	if (h) {
		h->buff=NULL;
		h->len=0;
		h->temp=NULL;
		h->i=0;
		h->size=1024;
	}

	return h;
}

void hbuf_Delete(HBUF *h)
{
	if (h) {
		szDelete(h->buff);
		szDelete(h->temp);
		free((char*)h);
	}
}

STATIC int hbuf_Flush(HBUF *h)
// Flush anything added so that the main buffer is complete
{
	if (h->i) {
		if (h->buff)
			RENEW(h->buff, char, h->len+h->i);	// Make the vector longer (existing entries + new + NULL)
		else
			h->buff=NEW(char, h->i);				// Create a new vector
		memcpy(h->buff+h->len, h->temp, h->i);
		h->len+=h->i;
		h->i=0;
	}
}

void hbuf_SetBufferSize(HBUF *h, int size)
// The default buffer size is 1024 but that can be tuned here if necessary.
// It can be called at any time.
{
	hbuf_Flush(h);
	szDelete(h->temp);
	h->temp=NULL;
	h->size=size>0?size:1024;
}

const char *hbuf_GetBuffer(HBUF *h)
{
	hbuf_Flush(h);
	return h->buff;
}

int hbuf_GetLength(HBUF *h)
{
	return h->len+h->i;
}

const char *hbuf_ReleaseBuffer(HBUF *h)
{
	const char *szResult;

	hbuf_Flush(h);
	szResult=h->buff;
	h->buff=NULL;
	h->len=0;

	return szResult;
}

const char *hbuf_SetBuffer(HBUF *h, int len, const char *buf)
// Initialises to a given buffer.
// Returns	Previous buffer (now in your control) or NULL if there was none.
{
	const char *result = hbuf_ReleaseBuffer(h);		// Take control of any previous buffer

	h->buff=(char*)buf;
	h->len=len;

	return result;
}

void hbuf_AddChar(HBUF *h, char c)
{
	if (!h->temp) h->temp=NEW(char, h->size);			// Delay allocation to here
	if (h->i == h->size) hbuf_Flush(h);
	h->temp[h->i++]=c;
}

void hbuf_AddBuffer(HBUF *h, int nLen, const char *buf)
// Adds nLen bytes to the buffer from 'buf'.
// If nLen is -1 then strlen(buf) is used.
// Calling with nLen==0 or buf==NULL is perfectly ok, though pointless.
{
	if (buf) {
		if (nLen < 0) nLen=strlen(buf);

		if (nLen) {									// Something to add

			if (nLen <= (h->size-h->i)) {					// We can fit it in the small buffer
				if (!h->temp) h->temp=NEW(char, h->size);	// Delay allocation to here
				memcpy(h->temp+h->i, buf, nLen);
				h->i+=nLen;
			} else {										// Stick it in the main one, after anything in 'temp'
				int nNeeded = h->len+h->i+nLen;				// Current size+temp content+new stuff
				if (h->len)
					RENEW(h->buff, char, nNeeded);			// Make the vector longer (existing entries + temp + new)
				else
					h->buff=NEW(char, nNeeded);				// Create a new vector
				if (h->i) memcpy(h->buff+h->len, h->temp, h->i);	// Copy temp buffer if used
				memcpy(h->buff+h->len+h->i, buf, nLen);
				h->len=nNeeded;
				h->i=0;
			}
		}
	}
}

void hbuf_AddHeap(HBUF *h, int nLen, const char *heap)
// As for hbuf_AddBuffer() but frees the thing added afterwards
{
	hbuf_AddBuffer(h, nLen, heap);
	szDelete(heap);
}

// HLIST - a list of heap-based data
// There is a header record and a number (0...n) of data records
// Header:
//		prev	Points to the last item in the list (or NULL if there are none)
//		next	Points to the first item in the list (or NULL if there are none)
//		data	name of the list or NULL if it doesn't have one
//		len		Total length of all items in the list
//
// Item:
//		prev	Previous item in the list (NULL if this is the first item)
//		next	Next item in the list (NULL if this is the last item)
//		data	Pointer to the data (never NULL)
//		len		Length of this data
//
// Zero length data is never added to the list so each item always points to data and has non-zero len
// The 'name' of the list may be a filename

HLIST *hlist_New()
{
	HLIST *h=NEW(HLIST, 1);

	if (h) {
		h->prev = NULL;
		h->next = NULL;
		h->data = NULL;
		h->len  = 0;
	}

	return h;
}

static HLIST *hlist_NewData(int len, const void *data)
// Only used for creating internal elements
{
	HLIST *h = hlist_New();

	if (len == -1) len = strlen(data);
	h->data = data;
	h->len = len;

	return h;
}

HLIST *hhlist_SetName(HLIST *h, const char *name)
// NB. By passing in a NULL hlist, this can effectively be a 'New(name)' call.
{
	if (!h) h = hlist_New();

	szDelete(h->data);
	h->data = (void*)(name ? strdup(name) : name);

	return h;
}

const char *hlist_Name(HLIST *h)
{
	return (const char *)h->data;
}

long long hlist_Length(HLIST *h)
{
	return h->len;
}

static int hlist_Coalesce(HLIST *h)
// Coalesce the first two blocks in the heap
// Returns	1	Coalesced
//			0	There were only 0 or 1 blocks (or h was NULL)
{
	int result = 0;

	if (h) {
		HLIST *head = h->next;
		if (head) {							// Got one
			HLIST *next = head->next;

			if (next) {						// Got two
				int headLen = head->len;	// Used a lot!
				char *buf = (char*)malloc(headLen+next->len);
				memcpy(buf, head->data, headLen);
				memcpy(buf+headLen, next->data, next->len);
				free((char*)head->data);
				free((char*)next->data);

				next->data = buf;
				next->len += headLen;		// We're going to drop the old head
				next->prev = NULL;
				h->next = next;
				free((char*)head);
				result = 1;
			}
		}
	}

	return result;
}

HLIST *hlist_AddHeap(HLIST *h, int len, const void *data)
// NB. NULL can be passed in here for the hlist, in which case a new one will be created and returned
// otherwise the hlist passed in is returned
// 'data' MUST be on the heap or bad things will happen later
// Management (free'ing) of the data passed in is taken by the HLIST.

{
	if (!h) h = hlist_New();

	if (data) {
		HLIST *newItem = hlist_NewData(len, data);

		h->len += newItem->len;

		if (h->prev) {						// Already has something in the list
			newItem->prev = h->prev;
			h->prev->next = newItem;
		} else {
			h->next = newItem;
		}
		h->prev = newItem;
	}

	return h;
}

HLIST *hlist_Add(HLIST *h, int len, const void *data)
// Same as hlist_Add() but uses a copy of the thing added (if it isn't NULL)
{
	if (data) {
		if (len == -1) len = strlen((const char*)data);

		void *buf = malloc(len);
		memcpy(buf, data, len);
		h = hlist_AddHeap(h, len, buf);
	}

	return h;
}

HLIST *hlist_PushHeap(HLIST *h, int len, const void *data)
// NB. NULL can be passed in here for the hlist, in which case a new one will be created and returned
// otherwise the hlist passed in is returned
// 'data' MUST be on the heap or bad things will happen later
// Management (free'ing) of the data passed in is taken by the HLIST.

{
	if (!h) h = hlist_New();

	if (data) {
		HLIST *newItem = hlist_NewData(len, data);

		h->len += newItem->len;

		if (h->next) {						// Already has something in the list
			newItem->next = h->next;
			h->next->prev = newItem;
		} else {
			h->prev = newItem;
		}
		h->next = newItem;
	}

	return h;
}

HLIST *hlist_Push(HLIST *h, int len, const void *data)
// Same as hlist_Add() but uses a copy of the thing added (if it isn't NULL)
{
	if (data) {
		if (len == -1) len = strlen((const char*)data);

		void *buf = malloc(len);
		memcpy(buf, data, len);
		h = hlist_PushHeap(h, len, buf);
	}

	return h;
}

void hlist_Delete(HLIST *h)
{
	if (h) {
		HLIST *rover = h->prev;					// Might be best to work backwards

		while (rover) {
			free((char*)rover->data);
			rover = rover->prev;
		}
	}
}

HBUF *hlist_ToHBuf(HLIST *h)
// Convert an hlist into an HBUF.
// NB. Passing in a NULL is ok here, it'll just return an empty hbuf.
{
	HBUF *hbuf = hbuf_New();

	if (h) {
		h = h->next;
		while (h) {
			hbuf_AddBuffer(hbuf, h->len, h->data);
			h = h->next;
		}
	}

	return hbuf;
}

int hlist_WriteFile(HLIST *h, const char *filename)
// Writes the HLIST to a file.  If filename is NULL, uses the name of the HBUF
// NB. Passing in a NULL hlist is harmless, it just means the file will be empty (or -1 returned if no name is given).
// Returns	-1	No name available
//			-2	Failed to open file (see errno)
//			0	All went ok
{
	if (!filename) {
		filename = hlist_Name(h);
		if (!filename) return -1;
	}

	int fd = open64(filename, O_WRONLY | O_CREAT);
	if (fd >= 0) {
		fchmod(fd, 0644);
		if (h) {
			h = h->next;
			while (h) {
				write(fd, h->data, h->len);
				h = h->next;
			}
		}
		close(fd);

		return 0;
	}

	return -2;
}

const void *hlist_GetBlock(HLIST *h, int *plen)
// Returns a single block from the list
// If plen is non-NULL, it receives the length of the block
// Returns	void*	The block of data
//			NULL	There was none (or h was NULL)
{
	const void *data = NULL;

	if (h && h->next) {
		HLIST *block = h->next;

		if (plen) *plen = block->len;
		data = block->data;

		if (block->next) {			// It wasn't the only one
			block->next->prev = NULL;
		} else {
			h->prev = NULL;
		}
		h->next = block->next;		// This will correctly be NULL if there was none
		h->len -= block->len;

		free((char*)block);
	} else {
		if (plen) *plen = 0;
	}

	return data;
}

const void *hlist_Peek(HLIST *h, int len, int *plen)
// Returns a pointer to a block of memory at the front of the queue of at least len bytes.
// If plen is non-NULL, receives the actual length.
// If there is not len bytes in the entire queue and plen==NULL, returns NULL.
// If there is not len bytes in the entire queue and plen!=NULL, makes the queue a single block and returns its length in plen
// If len=0, simply returns a pointer to the first block if any
{
	const void *result = NULL;

	if (h) {
		if (h->len >= len || plen != NULL) {			// We've enough data or the caller wants to know how much he can get
			while (h->next && h->next->len < len) {
				if (!hlist_Coalesce(h))
					break;
			}

			if (h->next) {
				result = h->next->data;
				if (plen) *plen = h->next->len;
			} else if (plen) {
				*plen = 0;
			}
		}
	}

	return result;
}

const void *hlist_GetData(HLIST *h, int len)
// Returns a heap-based block of the size given, or NULL if there isn't that many bytes
{
	const void *result = NULL;

	if (h && h->len >= len) {
		hlist_Peek(h, len, NULL);						// Ensures first block has enough data

		int got;
		result = hlist_GetBlock(h, &got);
		if (got > len) {
			hlist_Push(h, got-len, (char*)result+len);
			result = realloc((void*)result, len);
		}
	}

	return result;
}

int hlist_GetDataToBuffer(HLIST *h, int len, void *buffer)
// Removes 'len' bytes from the queue, placing it in the buffer given.
// If there aren't 'len' bytes total available, does nothing.
// Returns	1	The bytes are now in 'buffer'
//			0	There weren't that many bytes in the queue
{
	if (h && h->len >= len) {
		hlist_Peek(h, len, NULL);						// Ensures first block has enough data

		int got;
		const char *block = hlist_GetBlock(h, &got);
		if (got > len) {
			hlist_Push(h, got-len, (char*)block+len);
		}
		memcpy(buffer, block, len);
		free((char*)block);

		return 1;
	}

	return 0;
}

