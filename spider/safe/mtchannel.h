#ifndef __MTCHANNEL_H
#define __MTCHANNEL_H


#include <openssl/bio.h>

#include "hbuf.h"
#include "smap.h"

#define CHAN_IN		1				// This event receives stuff from io/fd
#define CHAN_OUT		2				// This event feeds it's output queue to io/fd
#define CHAN_DIRTY	4				// Indication that incoming data has been received in poll() loop
#define CHAN_CLOSE	8				// We are requesting that this channel closes

struct CHAN;
typedef int (*CHANCB_Receiver)(struct CHAN *channel);

typedef struct CHANPOOL {
	SPMAP *map;						// Map of channels
} CHANPOOL;

typedef struct CHAN {
	CHANPOOL *pool;					// The pool this channel is in, if any
	CHANCB_Receiver cb_receiver;	// Callback Handler for incoming data
	char *id;						// A unique ID for this channel (currntly the 'fd' as a string)
	int fd;							// File descriptor (valid even if io is non-NULL)
	BIO *io;						// io structure (NULL if we're using a plain fd)
	HLIST *inq;						// incoming queue (never NULL)
	HLIST *outq;					// Outgoing queue (never NULL)
	int flags;						// Bit mask of CHAN_*
} CHAN;


int chan_WriteHeap(CHAN *channel, int len, const char *data);
// Just like chan_Write() but takes ownership of a heap based data block

int chan_Write(CHAN *channel, int len, const char *data);
// Put a copy of some data on the WebSocket (by adding it to its output queue)

CHANPOOL * chan_PoolNew();
// Creates a new, empty, channel pool

int chan_RemoveFromPool(CHAN *c);
// Removes the channel from its pool but DOES NOT delete it
// Returns 1 if it was removed, 0 if it wasn't in a pool

void chan_PoolAdd(CHANPOOL *cp, CHAN *c);
// Adds a channel to a pool or, if cp == NULL, removes it from its pool

void chan_RegisterReceiver(CHAN *channel, CHANCB_Receiver cb);
CHAN *chan_New(CHANPOOL *cp, int fd, BIO *io, int flags);
// Creates a new channel and adds it to the pool (if non NULL)
// fd = 0..., io = NULL		// Plain 'fd' connection
// fd = -1, io = BIO*		// BIO * connection (fd is ascertained from io)
// flags					// a combination of CHAN_* values

void chan_Delete(CHAN *c);
// Deleting a channel will automatically remove it from its pool (if it's in one)

CHAN *chan_FindByFd(CHANPOOL *cp, int fd);
// Given an fd, finds a channel in a pool or NULL if it's not there

int chan_EventLoop(CHANPOOL *cp);
// 'Runs' the event loop for a channel pool.
// This will fetch any waiting data from inputs and deliver data in output queues.
// Only returns if/when there are no longer any channels in the pool.

#endif
