#if 0
./makeh $0
exit 0
#endif

// Channels
// A channel describes a connection through which websocket messages can travel.
// A channelList is used for an event loop that monitors those channels and coordinates data flow
// A channel can only be in one channelList at a time

#define STATIC static
#define API

#include <sys/stat.h>
#include <stdio.h>
#include <poll.h>

#include <heapstrings.h>
#include <mtmacro.h>

#include "mtwebsocket.h"

#if 0
// START HEADER

#include <openssl/bio.h>

#include "hbuf.h"
#include "smap.h"

#define WS_IN		1				// This event receives stuff from io/fd
#define WS_OUT		2				// This event feeds it's output queue to io/fd
#define WS_DIRTY	4				// Indication that incoming data has been received in poll() loop
#define WS_CLOSE	8				// We are requesting that this channel closes

struct WS;
typedef int (*WSCB_Receiver)(struct WS *channel, int len, const char *text);

typedef struct WSL {
	WSCB_Receiver cb_utf8;			// Callback Handler for UTF8 messages (opcode = 1)
	WSCB_Receiver cb_binary;		// Callback Handler for BINARY messages (opcode = 2)
	SPMAP *map;						// Map of channels
} WSL;

typedef struct WS {
	WSL *list;			// The list this channel is in, if any
	char *id;						// A unique ID for this channel (currntly the 'fd' as a string)
	int fd;							// File descriptor (valid even if io is non-NULL)
	BIO *io;						// io structure (NULL if we're using a plain fd)
	HLIST *in;						// incoming queue (never NULL)
	HLIST *out;						// Outgoing queue (never NULL)
	unsigned char opcode;			// Current opcode (used to handle continution frames)
	HBUF *inbuf;					// Used if we have fragments
	int flags;						// Bit mask of WS_*
} WS;

// END HEADER
#endif

STATIC void ws_ApplyMask(long mask, int len, const void *data)
{
	if (mask) {												// If there is a mask, store it and adjust our copied data
		unsigned long *p = (unsigned long *)data;

		int endian=1;										// we're little endian so we need to flip the mask around
		if (*(char*)&endian) {
			mask = ((unsigned char *)&mask)[0] << 24
					| ((unsigned char *)&mask)[1] << 16
					| ((unsigned char *)&mask)[2] << 8
					| ((unsigned char *)&mask)[3];
		}

		while (len >= 4) {
			*p ^= mask;
			p++;
			len -= 4;
		}
		if (len >= 1) ((char*)p)[0] ^= mask;
		if (len >= 2) ((char*)p)[1] ^= (mask >> 8);
		if (len >= 3) ((char*)p)[2] ^= (mask >> 16);
	}
}

API const char *ws_MakeFragment(int fin, int opcode, long mask, int len, const char *data, int *plen)
// Returns a heap-based fragment ready to send as a WebSocket fragment, with the length in *plen
// There is precious little error-checking/argument checking in here as the caller should take care.
// The arguments should be:
// fin		0 - there will be another fragment, non-0 this is the last
// opcode	0, 1, 2, 8, 9, 10
// mask		0 to not mask, otherwise the mask
// len		The length of 'data' (-1 to be strlen(data))
// data		The data
// plen		non-NULL to receive the resultent length

{
//Log("ws_MakeFragment(%d, %d, %x, %d, %p, %p)", fin, opcode, mask, len, data, plen);
	if (len == -1) len = strlen(data);
	int headlen = mask ? 6 : 2;
	if (len > 125) headlen += 2;
	if (len > 65535) headlen += 2;

	if (plen) *plen=headlen+len;

	char *result = malloc(headlen+len);
	result[0] = (fin ? 128 : 0) + opcode;
	int l = len;
	if (len > 65535) {
		l = 127;
		*(int*)(result+2) = len;
	} else if (len > 125) {
		l = 126;
		*(short*)(result+2) = len;
	}
	result[1] = (mask ? 128 : 0) + l;
	memcpy(result+headlen, data, len);

	if (mask) {												// If there is a mask, store it and adjust our copied data
		*(int*)(result+headlen-4)=mask;
		ws_ApplyMask(mask, len, data+headlen);
	}

	return result;
}

API int ws_WriteHeap(WS *channel, int len, const char *data)
// Just like ws_Write() but deletes the data afterwards
{
	int result = 0;

	if (channel && len && data) {
//LogBuffer("Adding to heap:", len, data);
		hlist_AddHeap(channel->out, len, data);
		result = len;
	}

	return result;
}

API int ws_Write(WS *channel, int len, const char *data)
// Put some data on the WebSocket (by adding it to its output queue)
{
	int result = 0;

	if (channel && len && data) {
		if (len == -1) len = strlen(data);

		char *heapCopy = malloc(len);
		memcpy(heapCopy, data, len);
		result = ws_Write(channel, len, heapCopy);
	}

	return result;
}

API void ws_SetUtf8Handler(WSL *wsl, WSCB_Receiver fn)
{
	if (wsl) wsl->cb_utf8 = fn;
}

API void ws_SetBinaryHandler(WSL *wsl, WSCB_Receiver fn)
{
	if (wsl) wsl->cb_binary = fn;
}

API WSL * ws_NewChannelList()
{
	WSL *cl = NEW(WSL, 1);

	cl->map = spmap_New();

	return cl;
}

API int ws_RemoveChannel(WS *c)
// Removes the channel from its list but DOES NOT delete it
// Returns 1 if it was removed, 0 if it wasn't in a list
{
	int result = 0;

	if (c && c->list) {
		WS *channel = spmap_GetValue(c->list->map, c->id);
		if (channel) {
			spmap_DeleteKey(c->list->map, c->id);
			result = 1;
		}
		c->list = NULL;
	}

	return result;
}

API void ws_AddChannel(WSL *cl, WS *c)
// Adds a channel to a list or, if cl == NULL, removes it from its list
{
	if (!c) return;

	if (cl != c->list) {										// Something to do
		if (c->list) ws_RemoveChannel(c);						// Remove it from any existing list

		if (cl) {												// It's in a list already
			spmap_Add(cl->map, c->id, (void*)c);
			c->list = cl;
		} else {
			ws_RemoveChannel(c);
		}
	}
}

API WS *ws_NewChannel(WSL *cl, int fd, BIO *io, int flags)
// Creates a new channel and adds it to the list (if non NULL)
// fd = 0..., io = NULL		// Plain 'fd' connection
// fd = -1, io = BIO*		// BIO * connection (fd is ascertained from io)
// flags					// a combination of WS_* values
{
	WS *channel = NEW(WS, 1);

	if (fd == -1 && io != NULL)						// Get fd if not provided
		BIO_get_fd(io, &fd);

	channel->id = hprintf(NULL, "%d", fd);
	channel->list = NULL;
	channel->fd = fd;
	channel->io = io;
	channel->flags = flags;
	channel->in = hlist_New();
	channel->out = hlist_New();
	channel->inbuf = hbuf_New();
	channel->opcode = 0;

	if (cl) ws_AddChannel(cl, channel);

	return channel;
}

API void ws_DeleteChannel(WS *c)
// Deleting a channel will automatically remove it from its list (if it's in one)
{
	if (c) {
		ws_RemoveChannel(c);

		szDelete(c->id);
		hlist_Delete(c->in);
		hlist_Delete(c->out);
		hbuf_Delete(c->inbuf);
		free((char*)c);
	}
}

API WS *ws_FindChannelByFd(WSL *cl, int fd)
// Given an fd, finds a channel in a list or NULL if it's not there
{
	if (!cl) return NULL;

	char key[20];

	sprintf(key, "%d", fd);
	return spmap_GetValue(cl->map, key);
}

API void ws_PushMessage(HLIST *h, int immediate, int opcode, int len, const char *data)
{
//Log("ws_Pushmessage(%x, %d, %d, %d, %p)",h,immediate,opcode,len,data);
	int fraglen;
	const char *fragment = ws_MakeFragment(1, opcode, 0, len, data, &fraglen);

	if (immediate)
		hlist_Push(h, fraglen, fragment);
	else
		hlist_Add(h, fraglen, fragment);

	free((char*)fragment);
}

STATIC void ws_CloseToBuffer(WS *channel, int code, const char *fmt, ...)
{
//Log("ws_CloseToBuffer(%p, %d, %s, ...)", h, code, fmt);
	if (channel) {
		char buf[256];
		char *p = buf;

		if (code != 0 || fmt != NULL) {
			p[0]=code >> 8;
			p[1]=code;
			p+=2;
		}

		if (fmt) {
			va_list ap;

			va_start(ap, fmt);
			vsnprintf(p, sizeof(buf)-(p-buf), fmt, ap);
			va_end(ap);
			p+=strlen(p);
		}

		ws_PushMessage(channel->out, 1, 8, p-buf, buf);
	}
}

API int ws_HandleIncoming(WS *channel)
// Returns 1 if we successfully removed something from the queue
// TODO: Need a way of handling an error here...
{
	int got;
	HLIST *h = channel->in;
	const unsigned char *header = (const unsigned char*)hlist_Peek(h, 10, &got);

	if (got < 2) return 0;			// Not enough to do anything with
	unsigned char fin = header[0] & 0x80;
	unsigned char op = header[0] & 0x0f;
	unsigned char masked = header[1] & 0x80;
	unsigned char len = header[1] & 0x7f;
	int needed = (len < 126) ? 2 : len == 126 ? 4 : 6;
	if (masked) needed += 4;
	if (got < needed) return 0;		// Not enough for length and mask

	long length;					// Length of block
	unsigned long mask = 0;			// Mask if it is masked
	if (len < 126) {
		length = len;
	} else if (len == 126) {
		length = header[2] << 8 | header[3];
	} else {
		length = header[2] << 24 | header[3] << 16 | header[4] << 8 | header[5];
	}
	if (masked) {
		mask = header[needed-4] << 24 | header[needed-3] << 16 | header[needed-2] << 8 | header[needed-1];
	}

	if (hlist_Length(h) < needed+length)
		return 0;					// After all that, there wasn't that much data there

	const char *data = malloc(needed+length+1);		// +1 as we'll plonk a nice '\0' on the end
	hlist_GetDataToBuffer(h, needed+length, (void*)data);

	header = (const unsigned char *)data;
	data = (char*)(header+needed);	// header is now only so we can properly free later, data is the actual data (len = length)
	((char*)data)[length]='\0';				// Terminate it to make parsing etc. so much easier

	if (mask) {						// Need exclusive OR our way over the data with the mask
		ws_ApplyMask(mask, length, data);
	}

	int dataAllocated = 0;			// 1 if data is a separate heap buffer that'll want freeing

	if (!fin) {						// This is only part of a message
		if (op) {
		   if (hbuf_GetLength(channel->inbuf)) {// Error - opcode should be 0 for fragments 2...
			   ws_CloseToBuffer(channel, 1002, "Non-zero opcode in continuation frame");
			   return 1;
		   } else {
				channel->opcode = op;
		   }
		}
		hbuf_AddBuffer(channel->inbuf, length, data);
		free((char*)header);
		return 1;
	} else {
		if (hbuf_GetLength(channel->inbuf)) {		// We have some prefixing data
			if (op) {
				ws_CloseToBuffer(channel, 1002, "Non-zero opcode in final frame of multi-frame message");
				return 1;
			}
			op = channel->opcode;
			hbuf_AddBuffer(channel->inbuf, length, data);
			length = hbuf_GetLength(channel->inbuf);
			data = hbuf_ReleaseBuffer(channel->inbuf);
			dataAllocated = 1;
			header = (const unsigned char *)realloc((void*)header, needed);
		}
	}
	// NB. At this point we may have two layouts for header and data
	// A single block allocated as header+data	- Message was a single fragment (dataAllocated = 0)
	// Separate header and data blocks			- Message was in fragments (dataAllocated = 1)

	switch (op) {
		case 0:									// Error, opcode should not be 0 at this stage
			ws_CloseToBuffer(channel, 1002, "Zero opcode in message");
			break;
		case 1:									// UTF-8 text
			if (channel->list && channel->list->cb_utf8)
				(*channel->list->cb_utf8)(channel, length, data);		// Assuming WAMP at this point
			break;
		case 2:									// Binary data
			break;
		case 8:									// Close
			ws_CloseToBuffer(channel, 1000, "Goodbye");
			break;
		case 9:									// Ping
LogBuffer("PingPong", length, data);
			ws_PushMessage(channel->out, 0, 10, length, data);	// Send a pong
			break;
		case 10:								// Pong
			break;
		default:
			break;
	}

	free((char*)header);
	if (dataAllocated) free((char*)data);

	return 1;
}

API int ws_EventLoop(WSL *cl)
{
	if (!cl) return 0;

	struct pollfd *fds = NULL;
	int nfds = 0;

	for (;;) {			// We'll be doing this until something makes us drop out
		int timeout = 500;						// 500 ms

		int entries = spmap_Count(cl->map);
		if (!entries) {
			Log("We have no channels to watch - dropping out of event loop");
			break;
		}

		if (nfds != entries) {								// Need to (re-)allocate the array if its changed size
			if (fds) free((char*)fds);
			fds = NULL;

			if (entries) {
				fds = (struct pollfd*)malloc(sizeof(*fds) * entries);
			}
		}

		nfds = 0;
		WS *channel;

		spmap_Reset(cl->map);
		while (spmap_GetNextEntry(cl->map, NULL, (void**)&channel)) {
			fds[nfds].fd = channel->fd;
			fds[nfds].events = 0;
			if (channel->flags & WS_IN) fds[nfds].events |= POLLIN;
			if ((channel->flags & WS_OUT) && hlist_Length(channel->out)) fds[nfds].events |= POLLOUT;	// It's O and we have stuff
			if (channel->flags & WS_DIRTY) timeout = 0;

			nfds++;
		}

		// We have our array of 'fd's so now poll them...
		int haveReceived = 0;									// Count of channels we've received something on
		int n = poll(fds, nfds, timeout);
		if (n > 0) {				// There is something coming in or going out
			int e;

			for (e = 0; e < nfds; e++) {
				WS *channel = NULL;
				int fd = fds[e].fd;
				int revents = fds[e].revents;

				if (revents & (POLLIN | POLLOUT)) {
					channel = ws_FindChannelByFd(cl, fd);
				}

				if (revents & POLLIN) {
					size_t got = 0;
					if (channel->io) {							// Using a BIO and we can't find number of pending bytes
						char buf[10240];						// Read into a 10K buffer for now

						got = BIO_read(channel->io, buf, sizeof(buf));
						hlist_Add(channel->in, got, buf);			// Add it to the buffer
					} else {
						int len;
#ifdef __SCO_VERSION__
						struct stat st;							// Method a works on both, but is documented as being unreliable
						fstat(fd, &st);
						len = st.st_size;
#else
						ioctl(fd, FIONREAD, &len);		// This method works only on LINUX
#endif

						if (len) {
							void *buf = (void*)malloc(len);

							got = read(fd, buf, len);	// Read what we can
							hlist_AddHeap(channel->in, got, buf);	// Add it to the buffer
						}
					}
					if (got) channel->flags |= WS_DIRTY;
					haveReceived++;
				} else if( revents & POLLHUP ) {			// Other program terminated
																// Here means that the other end has gone away and nothing to read
Log("Channel on %d has gone away", fd);
					channel = ws_FindChannelByFd(cl, fd);
					ws_DeleteChannel(channel);			// Channel has hung up so remove it from the loop
					// NB. channel is NULL here currently
					break;
				}

				if (revents & POLLOUT ) {				// We can write
					int len;

					if (hlist_Length(channel->out)) {				// Must be non-zero or POLLOUT wouldn't set, but just in case...
						const char *data = (const char *)hlist_GetBlock(channel->out, &len);
						int written = write(fd, data, len);	// Try to write it all
						if (written != len) {						// Didn't write it all
							hlist_Push(channel->out, len-written, data+written);	// Push any remainder
						}
						// TODO: Slightly kudgey close detection - needs to be better
						if (((unsigned char *)data)[0] == 0x88 && ((unsigned char *)data)[1] == len-2) {
Log("On sending %x %d (len=%d) - closing channel", ((unsigned char *)data)[0], ((unsigned char *)data)[1], len);
							// It looks like a ws close and the length matches
							ws_DeleteChannel(channel);			// Make it go away
						}
						free((char*)data);
					}
				}
			}
		} // if (poll)

		// Done the polling part to play with the outside world, now to deal with anything that came in
		spmap_Reset(cl->map);

		while (spmap_GetNextEntry(cl->map, NULL, (void**)&channel)) {
			if (channel->flags & WS_DIRTY) {

				int handled = ws_HandleIncoming(channel);
				int empty = !hlist_Length(channel->in);

				if (!handled || empty)
					channel->flags &= ~WS_DIRTY;		// Don't immediately process next time if we failed or there is data
			}
		}

{ static int count = 0;count++; if (count > 15) { Log("Dropping out due to debug limit of loops"); break; } }

	} // Main loop (;;)
}
