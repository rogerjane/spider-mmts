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

#include <mtmacro.h>

#ifdef IS_LINUX
    #include <sys/ioctl.h>
    #include <linux/sockios.h>
#endif

#include <errno.h>
#include <limits.h>
#include <poll.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

#include <heapstrings.h>

#include "mtchannel.h"

										// NB. Flags used internally for various purposes - should only affect bits 8 upwards
#define CHAN_DIRTY		0x0100			// Indication that incoming data has been received in poll() loop
#define CHAN_CLOSE		0x0200			// We are requesting that this channel closes when empty
#define CHAN_NOCLOSE	0x0400			// Don't close the underlying fd on deleting the channel

#if 0
// START HEADER

#include <openssl/bio.h>

#include "hbuf.h"
#include "smap.h"

// Flags used in chan_New();			// NB. These flags should only occupy bits 0-7 as they share with CHAN_DIRTY etc. above
#define CHAN_IN			0x0001			// This channel receives stuff from bio/fd
#define CHAN_OUT		0x0002			// This channel feeds its output queue to bio/fd
#define CHAN_LISTEN		0x0004			// This channel is a listening network port

struct CHAN;
typedef int (*CHANCB_Receiver)(struct CHAN *channel);
typedef int (*CHANCB_Pseudo)(int type, struct CHAN *channel);
typedef void (*CHANCB_DeleteCallback)(struct CHAN *channel);

struct CHANPOOL;
typedef void (*CHANCB_Idler)(void *data);

typedef struct CHANIDLER {
	struct CHANIDLER	*next;
	struct CHANIDLER	*prev;

	long long		due;			// Due time in microseconds after epoch (01-01-1970)
	CHANCB_Idler	fn;				// Function to be called
	void			*data;			// Payload passed to 'fn'
	long			repeatPeriod;	// Microseconds between calls
	bool			beingCalled;	// True while 'fn' is being called
} CHANIDLER;

typedef struct CHANPOOL {
	SPMAP *nameMap;					// Map of name->channels
	SPMAP *fdMap;					// Map of fd->channels
	CHANIDLER *idleQueue;			// Queue of timed events
} CHANPOOL;

typedef struct CHAN {
	CHANPOOL *pool;					// The pool this channel is in, if any
	CHANCB_Receiver cb_receiver;	// Callback Handler for incoming data
	CHANCB_DeleteCallback cb_ondelete;	// Called just before we destruct the channel (allows info clearup etc.)
	pthread_mutex_t queue_mutex;	// Mutex protecting access to the data queues
	int minBytes;					// Minimum bytes received before receiver is called
	void *owner;					// Pointer to owner (this would usually be a websocket)
	void *info;						// Miscellaneous info for the use of the caller
	char *name;						// A unique ID for this channel (currntly the 'fdin' as a string)
	int fdin;						// File descriptor for input (valid even if bio is non-NULL)
	int fdout;						// File descriptor for output (valid even if bio is non-NULL)
	BIO *bio;						// bio structure (NULL if we're using a plain fdin/fnout)
	CHANCB_Pseudo fn;				// Function for communication
	HLIST *inq;						// incoming queue (never NULL)
	HLIST *outq;					// Outgoing queue (never NULL)
	CHAN *connectFrom;				// Channel that uses our outq as its inq
	CHAN *connectTo;				// Channel whose outq we used as our inq
	int flags;						// Bit mask of CHAN_*
	char deleting;					// 1 if we are currently physically deleting this object
} CHAN;

//#define chan_IncomingQueue(chan)	(chan->inq)

// END HEADER
#endif

STATIC SPMAP *allChannels = NULL;			// All channels by name

API CHANPOOL* chan_Pool(CHAN *chan)			{ return chan ? chan->pool : NULL; }
API int chan_FdIn(CHAN *chan)				{ return chan ? chan->fdin : -1; }
API int chan_FdOut(CHAN *chan)				{ return chan ? chan->fdout : -1; }
API BIO* chan_Bio(CHAN *chan)				{ return chan ? chan->bio : NULL; }
API void* chan_Info(CHAN *chan)				{ return chan ? chan->info : NULL; }
API void* chan_Owner(CHAN *chan)			{ return chan ? chan->owner : NULL; }
API const char *chan_Name(CHAN *chan)		{ return chan ? chan->name : "NULL_CHANNEL"; }

static long long MicrosecondsNow()
// Returns the number of microseconds since time began (on 1st Jan 1970)
{
	struct timeval tp;

	gettimeofday(&tp, NULL);									// Get those little bits of seconds

	return ((long long)tp.tv_sec)*1000000 + (long long)tp.tv_usec;
}

static void (*_Logger)(const char *fmt, va_list ap) = NULL;

static void Log(const char *fmt, ...)
{
	if (_Logger) {
		va_list ap;

		va_start(ap, fmt);
		(*_Logger)(fmt, ap);
		va_end(ap);
	}
}

API void chan_Logger(void (*logger)(const char *, va_list))
// Sets a logging function to receive all debug output from the channel library
{
	_Logger = logger;
}

API void *chan_SetInfo(CHAN *chan, void *info)
// Sets the 'info' data for this channel.  This is not used by the channel itself but is available for the registered
// receiver.  E.g. for websocket it should be set to be the WS* that is using this channel
{
	void *previous = NULL;
	if (chan) {
		previous = chan->info;
		chan->info = info;
//Log("CH: *** Changed info for channel %s from %p to %p", chan->name, previous, info);
	}

	return previous;
}

API void *chan_SetOwner(CHAN *chan, void *owner)
// Sets the 'owner' for this channel.  This would almost always be a websocker pointer
{
	void *previous = NULL;
	if (chan) {
		previous = chan->owner;
		chan->owner = owner;
	}

	return previous;
}

STATIC void chan_Lock(CHAN *chan)
// Locks access to the channel's output buffer
{
	if (chan) {
		pthread_mutex_lock(&chan->queue_mutex);
	}
}

STATIC void chan_Unlock(CHAN *chan)
// Unlocks access to the channel's output buffer
{
	if (chan) {
		pthread_mutex_unlock(&chan->queue_mutex);
	}
}

API unsigned int chan_Available(CHAN *chan)
// Returns the number of bytes available in the input queue
{
	unsigned int result;

	if (chan) {
		chan_Lock(chan);
		result = hlist_Length(chan->inq);
		chan_Unlock(chan);
	}

	return result;
}

API const char *chan_Peek(CHAN *chan, int bytes, int *pgot)
// Returns a pointer to a number of bytes in the input buffer up to 'bytes' long
// DOES NOT remove the data from the buffer.
{
	const char *result = NULL;

	if (chan) {
		chan_Lock(chan);
		result = (const char *)hlist_Peek(chan->inq, bytes, pgot);
		chan_Unlock(chan);
	}

	return result;
}

API int chan_GetData(CHAN *chan, int bytes, char *buffer)
// Fetches a number of bytes from the channel's input buffer
{
	int result = 0;

	if (chan) {
		chan_Lock(chan);
		result = hlist_GetDataToBuffer(chan->inq, bytes, buffer);
		chan_Unlock(chan);
	}

	return result;
}

STATIC int chan_DataOut(CHAN *chan, int front, int heap, int len, const char *data)
// All functions that add to a queue come here
// front	0 - Add to the tail of the queue, 1 - Add to the front of the queue
// heap		0 - Data is static and needs to be copied, 1 data is on the heap and we'll take ownership
{
	int result = 0;

	chan_Lock(chan);

	if (chan && len && data) {
		if (len == -1) len = strlen(data);

		if (!heap) {
			char *heapCopy = (char*)malloc(len);
			memcpy(heapCopy, data, len);
			data = heapCopy;
		}
		if (front) {
			hlist_PushBackHeap(chan->outq, len, data);
		} else {
			hlist_AddHeap(chan->outq, len, data);
Log("CH: Added %d bytes to queue for %s", len, chan ? chan->name : "NULL");
		}
	}

	chan_Unlock(chan);

	return result;
}

API int chan_Write(CHAN *chan, int len, const char *data)
// Write data to the chan after any pending data.  The data is copied before being sent.
{
	return chan_DataOut(chan, 0, 0, len, data);
}

API int chan_WriteHeap(CHAN *chan, int len, const char *data)
// Write data to the chan after any pending data.  The data must be on the heap and this function will take ownership.
{
	return chan_DataOut(chan, 0, 1, len, data);
}

API int chan_WriteFront(CHAN *chan, int len, const char *data)
// Write data to the chan before any pending data.  The data is copied before being sent.
{
	return chan_DataOut(chan, 1, 0, len, data);
}

API int chan_WriteFrontHeap(CHAN *chan, int len, const char *data)
// Write data to the chan before any pending data.  The data must be on the heap and this function will take ownership.
{
	return chan_DataOut(chan, 1, 1, len, data);
}

API CHAN *chan_ByName(const char *name)
// Retrieve a channel by its name.
{
	return (CHAN*)spmap_GetValue(allChannels, name);
}

API CHANPOOL *chan_PoolNew()
// Creates a new, empty, channel pool
{
	CHANPOOL *cp = NEW(CHANPOOL, 1);

	cp->nameMap = spmap_New();
	cp->fdMap = spmap_New();
	cp->idleQueue = NULL;

	return cp;
}

API int chan_RemoveFromPool(CHAN *c)
// Removes the channel from its pool but DOES NOT delete it
// Returns 1 if it was removed, 0 if it wasn't in a pool
{
	int result = 0;

	if (c && c->pool) {
		CHANPOOL *pool = c->pool;

		CHAN *channel = (CHAN*)spmap_GetValue(pool->nameMap, c->name);
		if (channel) {

			spmap_DeleteKey(pool->nameMap, c->name);
			char buf[20];

			if (c->fdin != -1) {
				snprintf(buf, sizeof(buf), "%d", c->fdin);
				spmap_DeleteKey(pool->fdMap, buf);
			}
			if (c->fdout != -1 && c->fdout != c->fdin) {
				snprintf(buf, sizeof(buf), "%d", c->fdout);
				spmap_DeleteKey(pool->fdMap, buf);
			}
			result = 1;
		}
		c->pool = NULL;
	}

	return result;
}

API void chan_PoolDeleteAllChannels(CHANPOOL *cp)
// Delete all the channels belonging to the pool, which will empty it and cause any event loop to exit next time around
// if the idle queue is empty.
{
Log("Deleting all channels in %p", cp);
	if (cp) {
		// It's a bit tricky deleting things when they're in the map of all channels so we make an array of the
		// channels first, then whip through the array deleting the ones that are not alread being deleted.


		if (1) {							// This simple method should work now as we don't remove from pool within chan_Delete()
			CHAN *chan;

			spmap_Reset(cp->nameMap);
			while (spmap_GetNextEntry(cp->nameMap, NULL, (void**)&chan)) {
				chan_Delete(chan);
			}
		} else {
			int nChannels = spmap_Count(cp->nameMap);

			CHAN **channels = NEW(CHAN *, nChannels);

			spmap_Reset(cp->nameMap);
			int i = 0;
			while (i < nChannels && spmap_GetNextEntry(cp->nameMap, NULL, (void**)&channels[i++]))
				;

			for (i = 0; i < nChannels; i++) {
				CHAN *c = channels[i];

Log("CH: Deleting %s", c->name);

				chan_Delete(c);
			}
			free((void*)channels);
		}
	}
}

API void chan_PoolAdd(CHANPOOL *cp, CHAN *c)
// Adds a channel to a pool or, if cp == NULL, removes it from its pool
{
	if (!c) return;

	if (cp != c->pool) {										// Something to do
		if (c->pool) chan_RemoveFromPool(c);					// Remove it from any existing pool

		if (cp) {												// It's in a pool already
			spmap_Add(cp->nameMap, c->name, (void*)c);
			char buf[20];

			if (c->fdin != -1) {
				snprintf(buf, sizeof(buf), "%d", c->fdin);
				spmap_Add(cp->fdMap, buf, c);
			}
			if (c->fdout != -1) {
				snprintf(buf, sizeof(buf), "%d", c->fdout);
				spmap_Add(cp->fdMap, buf, c);
			}
			c->pool = cp;
		}
	}
}

STATIC void chan_PoolInsertQueueEntry(CHANPOOL *pool, CHANIDLER *entry)
// Insert the entry in the right position in the queue to keep the 'due' times in order
{
	if (pool) {
		long long due = entry->due;
		CHANIDLER *cursor = pool->idleQueue;

		if (!cursor || cursor->due > due) {						// Need to insert at head
			entry->next = cursor;
			entry->prev = NULL;
			pool->idleQueue = entry;
		} else {												// Else find the entry we need to sit after
			while (cursor->next && cursor->next->due < due)
				cursor = cursor->next;

			entry->next = cursor->next;
			entry->prev = cursor;
			cursor->next = entry;
		}
		if (entry->next) entry->next->prev = entry;
	}
}

STATIC CHANIDLER *chan_PoolRemoveQueueEntry(CHANPOOL *pool, CHANIDLER *entry)
// Removes queue entry from the queue
{
	if (pool && entry) {
		if (pool->idleQueue == entry)
			pool->idleQueue = entry->next;
		if (entry->prev) entry->prev->next = entry->next;
		if (entry->next) entry->next->prev = entry->prev;

		entry->next = NULL;
		entry->prev = NULL;
	}

	return entry;
}

STATIC void chan_PoolDeleteQueueEntry(CHANPOOL *pool, CHANIDLER *entry)
// Removes queue entry from the queue and deletes it
{
	chan_PoolRemoveQueueEntry(pool, entry);
	if (entry) free((void*)entry);
}

#if 0
STATIC void chan_PoolDeleteQueue(CHANPOOL *pool)
{
	if (pool) {
		while (pool->idleQueue)
			chan_PoolDeleteQueueEntry(pool, pool->idleQueue);
	}
}
#endif

STATIC CHANIDLER *chanidler_New(CHANCB_Idler fn, void *data)
{
	CHANIDLER *entry = NEW(CHANIDLER, 1);

	entry->next = NULL;
	entry->prev = NULL;
	entry->due = LLONG_MAX;
	entry->fn = fn;
	entry->data = data;
	entry->repeatPeriod = -1;
	entry->beingCalled = false;

	return entry;
}

API void chan_PoolStopIdler(CHANPOOL *pool, CHANIDLER *entry)
// Be wary, this may be called by the very function that it invokes so we need to be carefull when it returns from that function
{
	if (pool && entry) {
		if (entry->beingCalled) {						// Currently being called so just stop any repeats and it'll die itself
			entry->repeatPeriod = -1;
		} else {										// Not being called to safe to simply delete
			chan_PoolDeleteQueueEntry(pool, entry);
		}
	}
}

API CHANIDLER *chan_PoolCallAfter(CHANPOOL *pool, int period, CHANCB_Idler idler, void *data)
// Calls idler once after period milliseconds
{
	CHANIDLER *entry = NULL;

	if (pool && idler) {
		CHANIDLER *entry = chanidler_New(idler, data);

		entry->due = MicrosecondsNow()+(long long)period*1000;

		chan_PoolInsertQueueEntry(pool, entry);
	}

	return entry;
}

API CHANIDLER *chan_PoolCallEvery(CHANPOOL *pool, int period, CHANCB_Idler idler, void *data)
// Calls idler every period milliseconds
{
	CHANIDLER *entry = NULL;

	if (pool && idler) {
		CHANIDLER *entry = chanidler_New(idler, data);

		entry->due = MicrosecondsNow()+(long long)period*1000;
		entry->repeatPeriod = (long long)period*1000;

		chan_PoolInsertQueueEntry(pool, entry);
	}

	return entry;
}

API CHANIDLER *chan_CallAfter(CHAN *channel, int period, CHANCB_Idler idler, void *data)
{
	return channel ? chan_PoolCallAfter(channel->pool, period, idler, data) : NULL;
}

API CHANIDLER *chan_CallEvery(CHAN *channel, int period, CHANCB_Idler idler, void *data)
{
	return channel ? chan_PoolCallEvery(channel->pool, period, idler, data) : NULL;
}

API void chan_RegisterReceiver(CHAN *chan, CHANCB_Receiver cb, int minBytes)
// Register a callback that will be called whenever data is received on this channel
{
	if (chan) {
		chan->cb_receiver = cb;
		chan->minBytes = minBytes;
	}
}

API CHANCB_DeleteCallback chan_OnDelete(CHAN *chan, CHANCB_DeleteCallback cb)
// Register a function to be called when this object is deleted.
// Returns the previous function.
{
	CHANCB_DeleteCallback previous = NULL;

	if (chan) {
		previous = chan->cb_ondelete;
		chan->cb_ondelete = cb;
	}

	return previous;
}

API bool chan_CloseOnDelete(CHAN *chan, int state /* = 1 */)
// state	1	Close on delete (default state)
//			0	Don't close on delete
//			-1	No change
// Returns previous state
{
	bool oldState = true;

	if (chan) {
		oldState = !(chan->flags & CHAN_NOCLOSE);

		if (state >= 0) {
			chan->flags &= ~CHAN_NOCLOSE;					// Reset
			if (!state) chan->flags |= CHAN_NOCLOSE;		// Set if we DON'T want to close on delete
		}
	}

	return oldState;
}

STATIC CHAN *chan_New(CHANPOOL *cp, int fdin, int fdout, BIO *bio, CHANCB_Pseudo fn, int flags)
// Creates a new channel and adds it to the pool (if non NULL)
// fd = 0..., bio = NULL	// Plain 'fd' connection
// fd = -1, bio = BIO*		// BIO * connection (fd is ascertained from bio)
// fd = -1, fn = callback	// Pseudo connection
// flags					// a combination of CHAN_* values
{
	CHAN *chan = NEW(CHAN, 1);

	if (fdin == -1 && bio != NULL) {			// Get fds if not provided
		BIO_get_fd(bio, &fdin);
		fdout = fdin;
	}

	chan->name = hprintf(NULL, "%d", fdin);
	chan->pool = NULL;
	chan->cb_receiver = NULL;
	chan->minBytes = 0;
	chan->cb_ondelete = NULL;
	chan->info = NULL;
	chan->owner = NULL;
	chan->fdin = fdin;
	chan->fdout = fdout;
	chan->bio = bio;
	chan->fn = fn;
	chan->flags = flags;
	chan->inq = hlist_New();
	chan->outq = hlist_New();
	chan->connectFrom = NULL;
	chan->connectTo = NULL;
	chan->deleting = 0;
	pthread_mutex_init(&chan->queue_mutex, NULL);

	if (cp) chan_PoolAdd(cp, chan);

	if (!allChannels) allChannels = spmap_New();
	spmap_Add(allChannels, chan->name, (void*)chan);

	return chan;
}

API CHAN *chan_NewListen(CHANPOOL *cp, int fd, CHANCB_Receiver cb)
// Create a new channel given a file descriptor (may be network or pipe)
// The 'fd' provided is used for both input and output
{
	CHAN *chan = chan_New(cp, fd, fd, NULL, NULL, CHAN_IN | CHAN_LISTEN);
	if (chan)
		chan_RegisterReceiver(chan, cb, 0);

	return chan;
}

API CHAN *chan_NewFd(CHANPOOL *cp, int fd, int flags)
// Create a new channel given a file descriptor (may be network or pipe)
// The 'fd' provided is used for both input and output
{
	return chan_New(cp, fd, fd, NULL, NULL, flags);
}

API CHAN *chan_NewFd2(CHANPOOL *cp, int fdin, int fdout, int flags)
// Create a new channel given a pair of file descriptors (may be network or pipe)
{
	return chan_New(cp, fdin, fdout, NULL, NULL, flags);
}

API CHAN *chan_NewBio(CHANPOOL *cp, BIO *bio, int flags)
// Create a new channel given a 'BIO' from the openssl library
{
	return chan_New(cp, -1, -1, bio, NULL, flags);
}

API CHAN *chan_NewPseudo(CHANPOOL *cp, CHANCB_Pseudo fn, int flags)
// Create a new channel controlled by internal functions
{
	return chan_New(cp, -1, -1, NULL, fn, flags);
}

API void chan_Disconnect(CHAN *in, CHAN *out)
// Disconnects any connection from IN -> OUT
// NB. Either in or out can be NULL, in which case it's picked up from the non-NULL one
{
	if (!in && !out) return;				// Two NULLs - nothing to do
	if (!in) in = out->connectFrom;
	if (!out) out = in->connectTo;
	if (!in || !out) return;				// There isn't a connection

	in->connectTo = NULL;
	out->connectFrom = NULL;
	in->inq = hlist_New();
}

API bool chan_Connect(CHAN *in, CHAN *out)
// Connect two channels together such that anything coming in on 'in' immediately goes out on 'out'
// This effectively proxies the incoming channel to the outgoing one
{
	if (!in || !out) return false;

	chan_Lock(in);
	chan_Lock(out);

	if (hlist_Length(in->inq)) {							// There is something in on in(!) - we need to transfer this first
		int len = 1;

		while (len) {										// Transfer it across
			const void *data = hlist_GetBlock(in->inq, &len);
			if (len)
				hlist_PushBackHeap(out->outq, len, data);
		}
	}
	if (!in->connectTo)										// Don't mess up any existing connection
		hlist_Delete(in->inq);
	in->inq=out->outq;
	in->connectTo = out;
	out->connectFrom = in;

	chan_Unlock(out);
	chan_Unlock(in);

	return true;
}

STATIC void chan_Dealloc(CHAN *chan)
// This is where the memory and any other resources owned by the channel are deallocated
// This is called within chan_EventLoop() when everything is quiet, not generally from chan_Delete().
// NB. The _Delete() function only calls this if the channel is NOT in a pool.
{
	if (chan) {
		chan_Disconnect(chan, NULL);						// Make sure we don't have any proxy connections from/to us
		chan_Disconnect(NULL, chan);

Log("CH: Deallocating channel %s", chan_Name(chan));
		if (allChannels)
			spmap_DeleteKey(allChannels, chan->name);

		if (!(chan->flags & CHAN_NOCLOSE)) {
			if (chan->fn) {										// Nothing to do with function-based channels
			} else if (chan->bio) {
				int biofd = -1;
				BIO_get_fd(chan->bio, &biofd);
				if (biofd > -1) {
					shutdown(biofd, SHUT_RDWR);					// Seems to require this to inform the far end of closure
				}
				BIO_free_all(chan->bio);
			} else {
				if (chan->fdin != -1) {
					shutdown(chan->fdin, SHUT_RDWR);			// Harmless on non-network connections
					close(chan->fdin);
				}
				if (chan->fdout != chan->fdin && chan->fdout != -1) {
					shutdown(chan->fdout, SHUT_RDWR);
					close(chan->fdout);
				}
			}
		}
		szDelete(chan->name);
		hlist_Delete(chan->inq);
		hlist_Delete(chan->outq);
		free((char*)chan);
	}
}

API void chan_Delete(CHAN *chan)
// Deleting a channel will automatically remove it from its pool (if it's in one)
// and close the fds/BIO
{
	if (chan) {
		if (chan->deleting) return;						// Already deleting this object
		chan->deleting = 1;

Log("CH: Deleting channel %s", chan_Name(chan));
		if (chan->cb_ondelete)
			(chan->cb_ondelete)(chan);

		if (!chan->pool)								// Only actually deallocate if it's not in a pool
			chan_Dealloc(chan);
	}
}

API CHAN *chan_FindByFd(CHANPOOL *cp, int fd)
// Given an fd (input or output), finds a channel in a pool or NULL if it's not there
{
	if (!cp) return NULL;

	char key[20];

	snprintf(key, sizeof(key), "%d", fd);
	return (CHAN*)spmap_GetValue(cp->fdMap, key);
}

API void chan_CloseOnEmpty(CHAN *chan)
// Sets the channel to close as soon as it has no pending output
// You would generally call chan_Write*() then call this to have the channel output what you want then close.
{
	if (chan)
		chan->flags |= CHAN_CLOSE;
}

API int chan_EventLoop(CHANPOOL *cp, bool flushout /*=false*/)
// 'Runs' the event loop for a channel pool.
// This will fetch any waiting data from inputs and deliver data in output queues.
// Only returns if/when there are no longer any channels in the pool.
// If 'flushout' is true, only flushes outgoing queues, returning when there is nothing left in them.
{
	if (!cp) return 0;

	struct pollfd *fds = NULL;
	int nfds = 0;

//	int count = spmap_Count(cp->nameMap);
//Log("CH: Entering event loop with %d channel%s", count, count==1?"":"s");
	for (;;) {			// We'll be doing this until something makes us drop out
		int timeout = -1;
		if (cp->idleQueue && !flushout) {
			long long due = cp->idleQueue->due;
			long long now = MicrosecondsNow();
			if (due < now) due = now;
			long long delay = (due-now)/1000;
			if (delay > INT_MAX) delay = INT_MAX;
			timeout = (long)delay;
		}

		int entries = spmap_Count(cp->fdMap);

		if (nfds != entries) {								// Need to (re-)allocate the array if its changed size
Log("CH: Entries changed from %d to %d", nfds, entries);
			if (fds) free((char*)fds);
			fds = NULL;

			if (entries) {
				fds = (struct pollfd*)malloc(sizeof(*fds) * entries);
			}
		}

		nfds = 0;
		CHAN *chan;

		SSET *deadones = sset_New();						// Collect up a set of channels that are pending deletion

		spmap_Reset(cp->nameMap);
		while (spmap_GetNextEntry(cp->nameMap, NULL, (void**)&chan)) {
			if (chan->deleting) {							// This channel has had chan_Delete() called on it
//Log("CH: Spotted channel %s is on Charon's boat", chan_Name(chan));
				sset_Add(deadones, chan->name);
			}
		}

		const char *rip;
		while (sset_GetNextEntry(deadones, &rip)) {				// Delete thc channels that should be dead
			CHAN *chan = (CHAN*)spmap_GetValue(cp->nameMap, rip);
			chan_RemoveFromPool(chan);
			chan_Dealloc(chan);
		}
		sset_Delete(deadones);

		spmap_Reset(cp->nameMap);
		while (spmap_GetNextEntry(cp->nameMap, NULL, (void**)&chan)) {
			if (chan->fdin != -1) {
				fds[nfds].fd = chan->fdin;
				fds[nfds].events = 0;

				if (chan->flags & CHAN_IN && !flushout)					// Waiting for input and it's actually wanted
					fds[nfds].events |= POLLIN;	// We're an incoming queue

				if (chan->fdin == chan->fdout) {						// Bi-directional fd
					chan_Lock(chan);
					if ((chan->flags & CHAN_OUT) && hlist_Length(chan->outq)) {
Log("Have data (%d) waiting for channel %s", hlist_Length(chan->outq), chan_Name(chan));
						fds[nfds].events |= POLLOUT;							// We are an outgoing queue and have stuff to send
					}
					chan_Unlock(chan);
				}
//Log("Channel %s (in) to poll with %d", chan_Name(chan), fds[nfds].events);

				nfds++;
			}

			if (chan->fdout != -1 && chan->fdout != chan->fdin) {		// fdout is separate output
				fds[nfds].fd = chan->fdout;
				fds[nfds].events = 0;

				chan_Lock(chan);
				if ((chan->flags & CHAN_OUT) && hlist_Length(chan->outq)) {
Log("Have data (%d) waiting for channel %s", hlist_Length(chan->outq), chan_Name(chan));
					fds[nfds].events |= POLLOUT;							// We are an outgoing queue and have stuff to send
				}
				chan_Unlock(chan);
//Log("Channel %s (out) to poll with %d", chan_Name(chan), fds[nfds].events);

				nfds++;
			}
//Log("CH: Channel in loop: %s (%d - %d) q=%d", chan->name, chan->flags, fds[nfds].events, hlist_Length(chan->outq));
		}

		if (!nfds) {
			Log("CH: We have no channels to watch - dropping out of event loop");
			break;
		}

		entries = spmap_Count(cp->fdMap);						// Purely as a check for sanity
		if (nfds != entries) {									// Something has gone badly awry
			Log("IMPOSSIBLE: nfds = %d, entries = %d...", nfds, entries);
			break;
		}

//if (timeout == -1) { for (int i = 0; i<nfds; i++) { Log("fds[%d]: Waiting on %d with flags %d", i, fds[i].fd, fds[i].events); } }

		// We have our array of 'fd's so now poll them...
		int haveReceived = 0;									// Count of channels we've received something on
		int n = poll(fds, nfds, timeout);
		if (!flushout) {
			CHANIDLER *entry = cp->idleQueue;

			if (entry && MicrosecondsNow() >= entry->due) {			// We have an idler function to execute
				chan_PoolRemoveQueueEntry(cp, entry);

				entry->beingCalled = true;
				(*entry->fn)(entry->data);
				entry->beingCalled = false;

				if (entry->repeatPeriod >= 0) {
					entry->due = MicrosecondsNow()+entry->repeatPeriod;
					chan_PoolInsertQueueEntry(cp, entry);
				} else {
					chan_PoolDeleteQueueEntry(cp, entry);
				}
			}
		}

		if (n > 0) {				// There is something coming in or going out
//Log("CH: %d = poll(%p, %d, %d)", n, fds, nfds, timeout);
			int e;

			for (e = 0; e < nfds; e++) {
				CHAN *chan = NULL;
				int fd = fds[e].fd;
				int revents = fds[e].revents;
				size_t got = 0;

				if (!revents) continue;

				chan = chan_FindByFd(cp, fd);
//Log("CH: Channel %s has events set as %d", chan_Name(chan), revents);

				struct stat st;
				fstat(fd, &st);

				if (revents & POLLERR) {						// An error on the channel - we deem this fatal
					Log("CH: Error on channel %s - deleting it", chan_Name(chan));
					chan_Delete(chan);
					break;
				}

				if (revents & POLLIN) {
					if (chan->fn) {							// We're function driven - this should never happen!
					} else if (chan->flags & CHAN_LISTEN) {	// Incoming network connection
Log("Poked on network port on channel %d (%d >= %d?)", fd, hlist_Length(chan->inq), chan->minBytes);
						// Nothing to do - no data to receive and we'll mark it dirty lower down
					} else if (chan->bio) {					// Using a BIO and we can't find number of pending bytes
						char buf[10240];						// Read into a 10K buffer for now

//Log("Reading from BIO(%p)...", chan->bio);
						got = BIO_read(chan->bio, buf, sizeof(buf));
//Log("CH: Read %d byte%s from BIO of %d: %s", got, got==1?"":"s", fd, Printable(got, buf));
						if (got < 1 && !BIO_should_retry(chan->bio)) {	// We performed a read and got error - EOF
							Log("CH: Channel %s has nothing more to say", chan_Name(chan));
							revents |= POLLHUP;			// Treat it like a HUP
							revents &= ~POLLIN;
						}
						if (got >= 1) {
							chan_Lock(chan);
							hlist_Add(chan->inq, got, buf);			// Add it to the buffer
							chan_Unlock(chan);
						}
					} else {
						int len;
#ifdef IS_SCO
						struct stat st;				// Method documented as works on both, but unreliable (linux: len=0 on stdin)
						int err = fstat(fd, &st);
						len = st.st_size;
						if (isatty(fd) && len == 0)
							len = 1;				// Kludge we don't seem to be able to ascertain the available input on stdin
#else
						ioctl(fd, FIONREAD, &len);	// This method works only on LINUX
#endif

						if (len) {
							void *buf = (void*)malloc(len);

							got = read(fd, buf, len);	// Read what we can
							chan_Lock(chan);
//Log("CH: Adding %d byte%s to %s->inq", got, got==1?"":"s", chan_Name(chan));
							hlist_AddHeap(chan->inq, got, buf);	// Add it to the buffer
							chan_Unlock(chan);
						} else {
							revents |= POLLHUP;			// Treat it like a HUP
						}
					}
					chan_Lock(chan);
					if (hlist_Length(chan->inq) >= chan->minBytes)
						chan->flags |= CHAN_DIRTY;
					chan_Unlock(chan);

					haveReceived++;
				}

				if (revents & POLLHUP) {				// Here means that the other end has gone away and nothing to read
					chan = chan_FindByFd(cp, fd);
Log("CH: Channel %s has gone away", chan->name);
					chan_Delete(chan);			// Channel has hung up so remove it from the loop
					// NB. channel is NULL here currently
					continue;
				}

				if (revents & POLLOUT ) {				// We can write
					int len;

					chan_Lock(chan);
					if (hlist_Length(chan->outq)) {				// Must be non-zero or POLLOUT wouldn't set, but just in case...
						const char *data = (const char *)hlist_GetBlock(chan->outq, &len);
						int written = write(fd, data, len);			// Try to write it all - the only place we write
//Log("CH: Written %d byte%s to %s: %s", written, written==1?"":"s", chan->name, Printable(written, data));
						if (written == len) {					// We're done
							if (chan->flags & CHAN_CLOSE) {
Log("CH: Closing channel %s because the owner no longer wants it", chan->name);
								chan_Delete(chan);					// Make it go away
							}
						} else if (written >= 0) {				// Wrote some of it
							hlist_PushBack(chan->outq, len-written, data+written);	// Push the remainder back
						} else {								// -1 means an error or unable to write
							if (errno == EAGAIN) {
								hlist_PushBackHeap(chan->outq, len, data);	// Push the remainder back
								data = NULL;					// Don't delete it!
							} else {							// An actual error so drop the stuff we were writing and report it
								Log("CH: ERROR - errno %d writing to %s", errno, chan->name);
							}
						}
						if (data) free((char*)data);
					}
					chan_Unlock(chan);	// Although chan may be deleted a couple lines up, it actually stays until the next loop
				}
			}
		} // if (poll)

//Log("CH: Done the poll, checking for activity on all channels (%d)", spmap_Count(cp->nameMap));
		// Done the polling part to play with the outside world, now to deal with anything that came in
		spmap_Reset(cp->nameMap);

		while (spmap_GetNextEntry(cp->nameMap, NULL, (void**)&chan)) {
			if (chan->flags & CHAN_DIRTY) {
//Log("CH: Channel %s - flags = %d", chan->name, chan->flags);

// TODO: I'm not happy with this...  It decides that the cb_receiver has done something depending on whether the queue
// size has changed.  It's possible that it would take something off it and add something and it would look unchanged when
// it actually has.
				for (;;) {								// Call the handler until it doesn't remove anything from the queue
					chan_Lock(chan);
					long long beforeLen = hlist_Length(chan->inq);
					chan_Unlock(chan);

//Log("CH: Chan %s: length = %lld, calling %p", chan_Name(chan), beforeLen, chan->cb_receiver);
					if (chan->cb_receiver)
						(chan->cb_receiver)(chan);

					chan_Lock(chan);
					long long afterLen = hlist_Length(chan->inq);
					chan_Unlock(chan);
//Log("CH: Chan %s: Before = %lld, after = %lld, called %p", chan_Name(chan), beforeLen, afterLen, chan->cb_receiver);

					if (afterLen == beforeLen || beforeLen == 0)		// Stop if queue changed or is empty
						break;
				}
				chan->flags &= ~CHAN_DIRTY;
			}
		}
	} // Main loop (;;)

	return 1;
}
