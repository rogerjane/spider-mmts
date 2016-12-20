#ifndef __MTCHANNEL_H
#define __MTCHANNEL_H


#include <openssl/bio.h>

#include "hbuf.h"
#include "smap.h"

// Flags used in chan_New();
#define CHAN_IN			1				// This channel receives stuff from bio/fd
#define CHAN_OUT		2				// This channel feeds its output queue to bio/fd

struct CHAN;
typedef int (*CHANCB_Receiver)(struct CHAN *channel);
typedef int (*CHANCB_Pseudo)(int type, struct CHAN *channel);
typedef void (*CHANCB_DeleteCallback)(struct CHAN *channel);

struct CHANPOOL;
typedef void (*CHANCB_Idler)(struct CHANPOOL *pool);

typedef struct CHANPOOL {
	SPMAP *map;						// Map of channels
	CHANCB_Idler cb_idler;			// Function to call while idling
	int idlePeriod;					// How often to idle
} CHANPOOL;

typedef struct CHAN {
	CHANPOOL *pool;					// The pool this channel is in, if any
	CHANCB_Receiver cb_receiver;	// Callback Handler for incoming data
	CHANCB_Receiver cb_closed;		// Called when a channel is closed, just before it's deleted
	CHANCB_Receiver cb_error;		// Called when there is an error on the channel
	CHANCB_DeleteCallback cb_ondelete;	// Called just before we destruct the channel (allows info clearup etc.)
	int minBytes;					// Minimum bytes received before receiver is called
	void *info;						// Miscellaneous info for the use of the caller
	char *name;						// A unique ID for this channel (currntly the 'fd' as a string)
	int fd;							// File descriptor (valid even if bio is non-NULL)
	BIO *bio;						// bio structure (NULL if we're using a plain fd)
	CHANCB_Pseudo fn;				// Function for communication
	HLIST *inq;						// incoming queue (never NULL)
	HLIST *outq;					// Outgoing queue (never NULL)
	int flags;						// Bit mask of CHAN_*
	char deleting;					// 1 if we are currently physically deleting this object
} CHAN;

#define chan_IncomingQueue(chan)	(chan->inq)


CHANPOOL* chan_Pool(CHAN *chan);
int chan_Fd(CHAN *chan);
BIO* chan_Bio(CHAN *chan);
HLIST* chan_Inq(CHAN *chan);
HLIST* chan_Outq(CHAN *chan);
void* chan_Info(CHAN *chan);
const char *chan_Name(CHAN *chan);
void chan_Logger(void (*logger)(const char *, va_list));
// Sets a logging function to receive all debug output from the channel library

CHAN *chan_SetInfo(CHAN *chan, void *info);
// Sets the 'info' data for this channel.  This is not used by the channel itself but is available for the registered
// receiver.  E.g. for websocket it should be set to be the WS* that is using this channel

int chan_Write(CHAN *chan, int len, const char *data);
// Write data to the chan after any pending data.  The data is copied before being sent.

int chan_WriteHeap(CHAN *chan, int len, const char *data);
// Write data to the chan after any pending data.  The data must be on the heap and this function will take ownership.

int chan_WriteFront(CHAN *chan, int len, const char *data);
// Write data to the chan before any pending data.  The data is copied before being sent.

int chan_WriteFrontHeap(CHAN *chan, int len, const char *data);
// Write data to the chan before any pending data.  The data must be on the heap and this function will take ownership.

CHAN *chan_ByName(const char *name);
// Retrieve a channel by its name.

CHANPOOL *chan_PoolNew();
// Creates a new, empty, channel pool

int chan_RemoveFromPool(CHAN *c);
// Removes the channel from its pool but DOES NOT delete it
// Returns 1 if it was removed, 0 if it wasn't in a pool

void chan_PoolDeleteAllChannels(CHANPOOL *cp);
// Delete all the channels belonging to the pool, which will empty it and cause any event loop to exit next time around.

void chan_PoolAdd(CHANPOOL *cp, CHAN *c);
// Adds a channel to a pool or, if cp == NULL, removes it from its pool

void chan_PoolRegisterIdler(CHANPOOL *pool, int period, CHANCB_Idler idler);
// Register a function that will be called every 'period' milliseconds when chan_EventLoop() is invoked
// Call with idler=NULL to stop calling it.
// If period = 0 then it will be called as often as possible (you probably don't want to do this)

void chan_RegisterReceiver(CHAN *chan, CHANCB_Receiver cb, int minBytes);
// Register a callback that will be called whenever data is received on this channel

void chan_RegisterClosed(CHAN *chan, CHANCB_Receiver cb);
// Register a callback that will be called when the channel closes, just before it's deleted

void chan_RegisterError(CHAN *chan, CHANCB_Receiver cb);
// Register a callback that will be called when an error occurs on the channel

CHANCB_DeleteCallback chan_OnDelete(CHAN *chan, CHANCB_DeleteCallback cb);
// Register a function to be called when this object is deleted.
// Returns the previous function.

CHAN *chan_NewFd(CHANPOOL *cp, int fd, int flags);
// Create a new channel given a file descriptor (may be network or pipe)

CHAN *chan_NewBio(CHANPOOL *cp, BIO *bio, int flags);
// Create a new channel given a 'BIO' from the openssl library

CHAN *chan_NewPseudo(CHANPOOL *cp, CHANCB_Pseudo fn, int flags);
// Create a new channel controlled by internal functions

void chan_Delete(CHAN *chan);
// Deleting a channel will automatically remove it from its pool (if it's in one)
// and close the fd/BIO

CHAN *chan_FindByFd(CHANPOOL *cp, int fd);
// Given an fd, finds a channel in a pool or NULL if it's not there

void chan_CloseOnEmpty(CHAN *chan);
// Sets the channel to close as soon as it has no pending output
// You would generally call chan_Write*() then call this to have the channel output what you want then close.

int chan_EventLoop(CHANPOOL *cp);
// 'Runs' the event loop for a channel pool.
// This will fetch any waiting data from inputs and deliver data in output queues.
// Only returns if/when there are no longer any channels in the pool.

#endif
