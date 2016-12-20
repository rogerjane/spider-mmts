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

#ifdef __SCO_VERSION__
    #define OS  "SCO"
    #define IS_SCO 1
#else
    #define OS  "LINUX"
    #define IS_LINUX 1
#endif

#ifdef IS_LINUX
    #include <sys/ioctl.h>
    #include <linux/sockios.h>
#endif

#include <sys/stat.h>
#include <stdio.h>
#include <poll.h>

#include <heapstrings.h>
#include <mtmacro.h>

#include "mtchannel.h"

#define CHAN_DIRTY		4			// Indication that incoming data has been received in poll() loop
#define CHAN_CLOSE		8			// We are requesting that this channel closes when empty
#define CHAN_DELETE		16			// We are requesting that this channel is deleted when next looped


#if 0
// START HEADER

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

// END HEADER
#endif

STATIC SPMAP *allChannels = NULL;			// All channels by name

API CHANPOOL* chan_Pool(CHAN *chan)			{ return chan ? chan->pool : NULL; }
API int chan_Fd(CHAN *chan)					{ return chan ? chan->fd : -1; }
API BIO* chan_Bio(CHAN *chan)				{ return chan ? chan->bio : NULL; }
API HLIST* chan_Inq(CHAN *chan)				{ return chan ? chan->inq : NULL; }
API HLIST* chan_Outq(CHAN *chan)			{ return chan ? chan->outq : NULL; }
API void* chan_Info(CHAN *chan)				{ return chan ? chan->info : NULL; }
API const char *chan_Name(CHAN *chan)		{ return chan ? chan->name : NULL; }

API CHAN *chan_SetInfo(CHAN *chan, void *info)
// Sets the 'info' data for this channel.  This is not used by the channel itself but is available for the registered
// receiver.  E.g. for websocket it should be set to be the WS* that is using this channel
{
	CHAN *previous = NULL;
	if (chan) {
		previous = chan->info;
		chan->info = info;
//Log("CH: *** Changed info for channel %s from %p to %p", chan->name, previous, info);
	}

	return previous;
}

STATIC int chan_DataOut(CHAN *chan, int front, int heap, int len, const char *data)
// All functions that add to a queue come here
// front	0 - Add to the tail of the queue, 1 - Add to the front of the queue
// heap		0 - Data is static and needs to be copied, 1 data is on the heap and we'll take ownership
{
	int result = 0;

	if (chan && len && data) {
		if (len == -1) len = strlen(data);

		if (!heap) {
			char *heapCopy = malloc(len);
			memcpy(heapCopy, data, len);
			data = heapCopy;
		}
		if (front) {
			hlist_PushHeap(chan->outq, len, data);
		} else {
			hlist_AddHeap(chan->outq, len, data);
//Log("CH: Added %d bytes to queue for %s", len, chan ? chan->name : "NULL");
		}
	}
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

	cp->map = spmap_New();
	cp->cb_idler = NULL;
	cp->idlePeriod = -1;

	return cp;
}

API int chan_RemoveFromPool(CHAN *c)
// Removes the channel from its pool but DOES NOT delete it
// Returns 1 if it was removed, 0 if it wasn't in a pool
{
	int result = 0;

	if (c && c->pool) {
		CHAN *channel = spmap_GetValue(c->pool->map, c->name);
		if (channel) {
			spmap_DeleteKey(c->pool->map, c->name);
			result = 1;
		}
		c->pool = NULL;
	}

	return result;
}

API void chan_PoolDeleteAllChannels(CHANPOOL *cp)
// Delete all the channels belonging to the pool, which will empty it and cause any event loop to exit next time around.
{
	if (cp) {
		while (spmap_Count(cp->map)) {
			CHAN *c = spmap_GetValueAtIndex(cp->map, 1);

			chan_Delete(c);										// This will remove the channel from the map
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
			spmap_Add(cp->map, c->name, (void*)c);
			c->pool = cp;
		}
	}
}

API void chan_PoolRegisterIdler(CHANPOOL *pool, int period, CHANCB_Idler idler)
// Register a function that will be called every 'period' milliseconds when chan_EventLoop() is invoked
// Call with idler=NULL to stop calling it.
// If period = 0 then it will be called as often as possible (you probably don't want to do this)
{
	if (pool) {
		pool->cb_idler = idler;

		if (idler) {
			pool->idlePeriod = period;
		} else {
			pool->idlePeriod = -1;
		}
	}
}

API void chan_RegisterReceiver(CHAN *chan, CHANCB_Receiver cb, int minBytes)
// Register a callback that will be called whenever data is received on this channel
{
	if (chan) {
		chan->cb_receiver = cb;
		chan->minBytes = minBytes;
	}
}

API void chan_RegisterClosed(CHAN *chan, CHANCB_Receiver cb)
// Register a callback that will be called when the channel closes, just before it's deleted
{
	if (chan)
		chan->cb_closed = cb;
}

API void chan_RegisterError(CHAN *chan, CHANCB_Receiver cb)
// Register a callback that will be called when an error occurs on the channel
{
	if (chan)
		chan->cb_error = cb;
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

STATIC CHAN *chan_New(CHANPOOL *cp, int fd, BIO *bio, CHANCB_Pseudo fn, int flags)
// Creates a new channel and adds it to the pool (if non NULL)
// fd = 0..., bio = NULL		// Plain 'fd' connection
// fd = -1, bio = BIO*		// BIO * connection (fd is ascertained from bio)
// fd = -1, fn = callback	// Pseudo connection
// flags					// a combination of CHAN_* values
{
	CHAN *chan = NEW(CHAN, 1);

	if (fd == -1 && bio != NULL)						// Get fd if not provided
		BIO_get_fd(bio, &fd);

	chan->name = hprintf(NULL, "%d", fd);
	chan->pool = NULL;
	chan->cb_receiver = NULL;
	chan->minBytes = 0;
	chan->cb_closed = NULL;
	chan->cb_error = NULL;
	chan->cb_ondelete = NULL;
	chan->info = NULL;
	chan->fd = fd;
	chan->bio = bio;
	chan->fn = fn;
	chan->flags = flags;
	chan->inq = hlist_New();
	chan->outq = hlist_New();
	chan->deleting = 0;

	if (cp) chan_PoolAdd(cp, chan);

	if (!allChannels) allChannels = spmap_New();
	spmap_Add(allChannels, chan->name, (void*)chan);

	return chan;
}

API CHAN *chan_NewFd(CHANPOOL *cp, int fd, int flags)
// Create a new channel given a file descriptor (may be network or pipe)
{
	return chan_New(cp, fd, NULL, NULL, flags);
}

API CHAN *chan_NewBio(CHANPOOL *cp, BIO *bio, int flags)
// Create a new channel given a 'BIO' from the openssl library
{
	return chan_New(cp, -1, bio, NULL, flags);
}

API CHAN *chan_NewPseudo(CHANPOOL *cp, CHANCB_Pseudo fn, int flags)
// Create a new channel controlled by internal functions
{
	return chan_New(cp, -1, NULL, fn, flags);
}

API void chan_Delete(CHAN *chan)
// Deleting a channel will automatically remove it from its pool (if it's in one)
// and close the fd/BIO
{
	if (chan) {
		if (chan->pool && (chan->flags & CHAN_DELETE) == 0) {		// If we're in a pool, don't delete until we safely loop again
//Log("CH: Marking channel %s for deletion", chan_Name(chan));
			chan->flags |= CHAN_DELETE;
			return;
		}

		if (chan->deleting) return;						// Alread deleting this object
		chan->deleting = 1;

//Log("CH: Deleting channel %s fatally", chan_Name(chan));
		if (chan->cb_ondelete)
			(chan->cb_ondelete)(chan);

		chan_RemoveFromPool(chan);
		if (allChannels)
			spmap_DeleteKey(allChannels, chan->name);

		if (chan->fn) {
			(chan->fn)(CHAN_DELETE, chan);				// TODO: This shouldn't be needed
		} else if (chan->bio) {
			BIO_free_all(chan->bio);
		} else {
			close(chan->fd);
		}
		szDelete(chan->name);
		hlist_Delete(chan->inq);
		hlist_Delete(chan->outq);
		free((char*)chan);
	}
}

API CHAN *chan_FindByFd(CHANPOOL *cp, int fd)
// Given an fd, finds a channel in a pool or NULL if it's not there
{
	if (!cp) return NULL;

	char key[20];

	sprintf(key, "%d", fd);
	return spmap_GetValue(cp->map, key);
}

API void chan_CloseOnEmpty(CHAN *chan)
// Sets the channel to close as soon as it has no pending output
// You would generally call chan_Write*() then call this to have the channel output what you want then close.
{
	if (chan)
		chan->flags |= CHAN_CLOSE;
}

STATIC const char *NiceBuf(int len, const char *buf)
// Returns a string (treat it as static) that has a printable form of the buffer passed (-1 means strlen)
{
	static char *result = NULL;

	if (result) free(result);
	if (len < 0) len = strlen(buf);

	HBUF *hbuf = hbuf_New();

	char *dest = result;
	while (len > 0) {
		if (*buf >= ' ' && *buf <= '~') {
			hbuf_AddChar(hbuf, *buf);
		} else {
			char tmp[10];
			snprintf(tmp, sizeof(tmp), "<%02x>", (unsigned char)*buf);
			hbuf_AddBuffer(hbuf, -1, tmp);
		}
		buf++;
		len--;
	}
	hbuf_AddChar(hbuf, '\0');
	result = (char*)hbuf_ReleaseBuffer(hbuf);
	hbuf_Delete(hbuf);

	return result;
}

API int chan_EventLoop(CHANPOOL *cp)
// 'Runs' the event loop for a channel pool.
// This will fetch any waiting data from inputs and deliver data in output queues.
// Only returns if/when there are no longer any channels in the pool.
{
	if (!cp) return 0;

	struct pollfd *fds = NULL;
	int nfds = 0;

Log("CH: Entering event loop with channels numbering %d", spmap_Count(cp->map));
	for (;;) {			// We'll be doing this until something makes us drop out
		int timeout = cp->idlePeriod;

		int entries = spmap_Count(cp->map);

		if (nfds != entries) {								// Need to (re-)allocate the array if its changed size
			if (fds) free((char*)fds);
			fds = NULL;

			if (entries) {
				fds = (struct pollfd*)malloc(sizeof(*fds) * entries);
			}
		}

		nfds = 0;
		CHAN *chan;

		SSET *deadones = sset_New();						// Collect up a set of channels that are pending deletion

		spmap_Reset(cp->map);
		while (spmap_GetNextEntry(cp->map, NULL, (void**)&chan)) {
			if (chan->flags & CHAN_DELETE) {
//Log("CH: Spotted channel %s is on Charon's boat", chan_Name(chan));
				sset_Add(deadones, chan->name);
			}
		}

		const char *rip;
		while (sset_GetNextEntry(deadones, &rip)) {				// Delete thc channels that should be dead
			CHAN *chan = (CHAN*)spmap_GetValue(cp->map, rip);
//Log("CH: Deleting doomed channel %s", chan_Name(chan));
			chan_Delete(chan);
		}
		sset_Delete(deadones);

		spmap_Reset(cp->map);
		while (spmap_GetNextEntry(cp->map, NULL, (void**)&chan)) {
			fds[nfds].fd = chan->fd;
			fds[nfds].events = 0;
			if (chan->flags & CHAN_IN) fds[nfds].events |= POLLIN;	// We're an incoming queue
			if ((chan->flags & CHAN_OUT) && hlist_Length(chan->outq))
				fds[nfds].events |= POLLOUT;							// We are an outgoing queue and have stuff to send

//Log("CH: Channel in loop: %s (%d - %d) q=%d", chan->name, chan->flags, fds[nfds].events, hlist_Length(chan->outq));
			nfds++;
		}

		if (!nfds) {
			Log("CH: We have no channels to watch - dropping out of event loop");
			break;
		}

		// We have our array of 'fd's so now poll them...
		int haveReceived = 0;									// Count of channels we've received something on
		int n = poll(fds, nfds, timeout);
		if (n > 0) {				// There is something coming in or going out
			int e;

			for (e = 0; e < nfds; e++) {
				CHAN *chan = NULL;
				int fd = fds[e].fd;
				int revents = fds[e].revents;
				size_t got = 0;

				if (!revents) continue;

				chan = chan_FindByFd(cp, fd);
//Log("CH: Event on channel %s: %d", chan_Name(chan), revents);

				if (revents & POLLERR) {						// An error on the channel...
					if (chan->cb_error) {
Log("CH: Error on channel %s - calling error handler", chan_Name(chan));
						(*chan->cb_error)(chan);
					} else {
Log("CH: Error on channel %s - dropping", chan_Name(chan));
						chan_Delete(chan);
					}
Log("CH: I've got to break free....");
					break;
				}

				if (revents & POLLIN) {
					if (chan->fn) {							// We're function driven - this should never happen!
					} else if (chan->bio) {					// Using a BIO and we can't find number of pending bytes
						char buf[10240];						// Read into a 10K buffer for now

						got = BIO_read(chan->bio, buf, sizeof(buf));
//Log("CH: Channel %s: Got %d (%s)", chan_Name(chan), got, NiceBuf(got, buf));
						if (!got && !BIO_should_retry(chan->bio)) {	// We performed a read and got error - EOF
//Log("CH: Channel %s: Nothing read, no retry", chan_Name(chan));
							revents |= POLLHUP;			// Treat it like a HUP
						}
						hlist_Add(chan->inq, got, buf);			// Add it to the buffer
					} else {
						int len;
#ifdef __SCO_VERSION__
						struct stat st;							// Method a works on both, but is documented as being unreliable
						fstat(fd, &st);
						len = st.st_size;
#else
						ioctl(fd, FIONREAD, &len);		// This method works only on LINUX
#endif
//Log("CH: Input available on %s - len = %d", chan->name, len);

						if (len) {
							void *buf = (void*)malloc(len);

							got = read(fd, buf, len);	// Read what we can
							hlist_AddHeap(chan->inq, got, buf);	// Add it to the buffer
						}
					}
					if (hlist_Length(chan->inq) >= chan->minBytes)
						chan->flags |= CHAN_DIRTY;
//Log("CH: Chan %s: If %Ld + %d >= %d -> %d", chan_Name(chan), hlist_Length(chan->inq), got,  chan->minBytes, chan->flags);
					haveReceived++;
				}

				if (!got && revents & POLLHUP) {		// Other program terminated and there is no input
														// Here means that the other end has gone away and nothing to read
					chan = chan_FindByFd(cp, fd);
Log("CH: Channel %s has gone away", chan->name);
					if (chan->cb_closed)
						(*chan->cb_closed)(chan);
//Log("CH: Any parent has been informed about the demise of channel %s", chan->name);
					chan_Delete(chan);			// Channel has hung up so remove it from the loop
					// NB. channel is NULL here currently
					continue;
				}

				if (revents & POLLOUT ) {				// We can write
					int len;

					if (hlist_Length(chan->outq)) {				// Must be non-zero or POLLOUT wouldn't set, but just in case...
						const char *data = (const char *)hlist_GetBlock(chan->outq, &len);
						int written = write(fd, data, len);			// Try to write it all
//{const char *file=hprintf(NULL, "/tmp/fd%d.txt", fd);FILE *fp=fopen(file, "a"); fwrite(data, 1, len, fp); fclose(fp);}
//Log("CH: Written to %s: %d - %s", chan->name, written, NiceBuf(written, data));
						if (written != len) {						// Didn't write it all
							hlist_Push(chan->outq, len-written, data+written);	// Push any remainder
						} else if (chan->flags & CHAN_CLOSE) {
//Log("CH: Closing channel %s because the owner no longer wants it", chan->name);
							chan_Delete(chan);					// Make it go away
						}
						free((char*)data);
					}
				}
			}
		} // if (poll)

//Log("CH: Done the poll, checking for activity on all channels (%d)", spmap_Count(cp->map));
		// Done the polling part to play with the outside world, now to deal with anything that came in
		spmap_Reset(cp->map);

		while (spmap_GetNextEntry(cp->map, NULL, (void**)&chan)) {
//Log("CH: Channel %s - flags = %d", chan->name, chan->flags);
			if (chan->flags & CHAN_DIRTY) {

				for (;;) {								// Call the handler until it doesn't remove anything from the queue
					int handled;

					long long beforeLen = hlist_Length(chan->inq);
//Log("CH: Chan %s: length = %Ld, calling %p", chan_Name(chan), beforeLen, chan->cb_receiver);
					if (chan->cb_receiver)
						handled = (chan->cb_receiver)(chan);
					long long afterLen = hlist_Length(chan->inq);
//Log("CH: Chan %s: Before = %Ld, after = %Ld, called %p", chan_Name(chan), beforeLen, afterLen, chan->cb_receiver);

					if (afterLen == beforeLen || beforeLen == 0)		// Stop if queue changed or is empty
						break;
				}
				chan->flags &= ~CHAN_DIRTY;

			}
		}

//{ static int count = 0;count++; if (count > 40) { Log("CH: Dropping out due to debug limit of loops"); break; } }

		if (cp->cb_idler)
			(cp->cb_idler)(cp);

	} // Main loop (;;)
}
