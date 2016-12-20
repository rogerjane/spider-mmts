#ifndef __MTWEBSOCKET_H
#define __MTWEBSOCKET_H


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


const char *ws_MakeFragment(int fin, int opcode, long mask, int len, const char *data, int *plen);
// Returns a heap-based fragment ready to send as a WebSocket fragment, with the length in *plen
// There is precious little error-checking/argument checking in here as the caller should take care.
// The arguments should be:
// fin		0 - there will be another fragment, non-0 this is the last
// opcode	0, 1, 2, 8, 9, 10
// mask		0 to not mask, otherwise the mask
// len		The length of 'data' (-1 to be strlen(data))
// data		The data
// plen		non-NULL to receive the resultent length

int ws_WriteHeap(WS *channel, int len, const char *data);
// Just like ws_Write() but deletes the data afterwards

int ws_Write(WS *channel, int len, const char *data);
// Put some data on the WebSocket (by adding it to its output queue)

void ws_SetUtf8Handler(WSL *wsl, WSCB_Receiver fn);
void ws_SetBinaryHandler(WSL *wsl, WSCB_Receiver fn);
WSL * ws_NewChannelList();
int ws_RemoveChannel(WS *c);
// Removes the channel from its list but DOES NOT delete it
// Returns 1 if it was removed, 0 if it wasn't in a list

void ws_AddChannel(WSL *cl, WS *c);
// Adds a channel to a list or, if cl == NULL, removes it from its list

WS *ws_NewChannel(WSL *cl, int fd, BIO *io, int flags);
// Creates a new channel and adds it to the list (if non NULL)
// fd = 0..., io = NULL		// Plain 'fd' connection
// fd = -1, io = BIO*		// BIO * connection (fd is ascertained from io)
// flags					// a combination of WS_* values

void ws_DeleteChannel(WS *c);
// Deleting a channel will automatically remove it from its list (if it's in one)

WS *ws_FindChannelByFd(WSL *cl, int fd);
// Given an fd, finds a channel in a list or NULL if it's not there

void ws_PushMessage(HLIST *h, int immediate, int opcode, int len, const char *data);
int ws_HandleIncoming(WS *channel);
// Returns 1 if we successfully removed something from the queue
// TODO: Need a way of handling an error here...

int ws_EventLoop(WSL *cl);

#endif
