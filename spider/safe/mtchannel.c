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

#include "mtchannel.h"

#if 0
// START HEADER

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

// END HEADER
#endif

API int chan_WriteHeap(CHAN *channel, int len, const char *data)
// Just like chan_Write() but takes ownership of a heap based data block
{
	int result = 0;

	if (channel && len && data) {
		hlist_AddHeap(channel->outq, len, data);
		result = len;
	}

	return result;
}

API int chan_Write(CHAN *channel, int len, const char *data)
// Put a copy of some data on the WebSocket (by adding it to its output queue)
{
	int result = 0;

	if (channel && len && data) {
		if (len == -1) len = strlen(data);

		char *heapCopy = malloc(len);
		memcpy(heapCopy, data, len);
		result = chan_WriteHeap(channel, len, heapCopy);
	}

	return result;
}

API CHANPOOL * chan_PoolNew()
// Creates a new, empty, channel pool
{
	CHANPOOL *cp = NEW(CHANPOOL, 1);

	cp->map = spmap_New();

	return cp;
}

API int chan_RemoveFromPool(CHAN *c)
// Removes the channel from its pool but DOES NOT delete it
// Returns 1 if it was removed, 0 if it wasn't in a pool
{
	int result = 0;

	if (c && c->pool) {
		CHAN *channel = spmap_GetValue(c->pool->map, c->id);
		if (channel) {
			spmap_DeleteKey(c->pool->map, c->id);
			result = 1;
		}
		c->pool = NULL;
	}

	return result;
}

API void chan_PoolAdd(CHANPOOL *cp, CHAN *c)
// Adds a channel to a pool or, if cp == NULL, removes it from its pool
{
	if (!c) return;

	if (cp != c->pool) {										// Something to do
		if (c->pool) chan_RemoveFromPool(c);					// Remove it from any existing pool

		if (cp) {												// It's in a pool already
			spmap_Add(cp->map, c->id, (void*)c);
			c->pool = cp;
		}
	}
}

API void chan_RegisterReceiver(CHAN *channel, CHANCB_Receiver cb)
// Register a callback that will be called whenever data is received on this channel
{
	channel->cb_receiver = cb;
}

API CHAN *chan_New(CHANPOOL *cp, int fd, BIO *io, int flags)
// Creates a new channel and adds it to the pool (if non NULL)
// fd = 0..., io = NULL		// Plain 'fd' connection
// fd = -1, io = BIO*		// BIO * connection (fd is ascertained from io)
// flags					// a combination of CHAN_* values
{
	CHAN *channel = NEW(CHAN, 1);

	if (fd == -1 && io != NULL)						// Get fd if not provided
		BIO_get_fd(io, &fd);

	channel->id = hprintf(NULL, "%d", fd);
	channel->pool = NULL;
	channel->cb_receiver = NULL;
	channel->fd = fd;
	channel->io = io;
	channel->flags = flags;
	channel->inq = hlist_New();
	channel->outq = hlist_New();

	if (cp) chan_PoolAdd(cp, channel);

	return channel;
}

API void chan_Delete(CHAN *c)
// Deleting a channel will automatically remove it from its pool (if it's in one)
{
	if (c) {
		chan_RemoveFromPool(c);

		szDelete(c->id);
		hlist_Delete(c->inq);
		hlist_Delete(c->outq);
		free((char*)c);
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

API int chan_EventLoop(CHANPOOL *cp)
// 'Runs' the event loop for a channel pool.
// This will fetch any waiting data from inputs and deliver data in output queues.
// Only returns if/when there are no longer any channels in the pool.
{
	if (!cp) return 0;

	struct pollfd *fds = NULL;
	int nfds = 0;

	for (;;) {			// We'll be doing this until something makes us drop out
		int timeout = 500;						// 500 ms

		int entries = spmap_Count(cp->map);
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
		CHAN *channel;

		spmap_Reset(cp->map);
		while (spmap_GetNextEntry(cp->map, NULL, (void**)&channel)) {
			fds[nfds].fd = channel->fd;
			fds[nfds].events = 0;
			if (channel->flags & CHAN_IN) fds[nfds].events |= POLLIN;
			if ((channel->flags & CHAN_OUT) && hlist_Length(channel->outq)) fds[nfds].events |= POLLOUT;	// It's O and we have stuff
			if (channel->flags & CHAN_DIRTY) timeout = 0;

			nfds++;
		}

		// We have our array of 'fd's so now poll them...
		int haveReceived = 0;									// Count of channels we've received something on
		int n = poll(fds, nfds, timeout);
		if (n > 0) {				// There is something coming in or going out
			int e;

			for (e = 0; e < nfds; e++) {
				CHAN *channel = NULL;
				int fd = fds[e].fd;
				int revents = fds[e].revents;

				if (revents & (POLLIN | POLLOUT)) {
					channel = chan_FindByFd(cp, fd);
				}

				if (revents & POLLIN) {
					size_t got = 0;
					if (channel->io) {							// Using a BIO and we can't find number of pending bytes
						char buf[10240];						// Read into a 10K buffer for now

						got = BIO_read(channel->io, buf, sizeof(buf));
						hlist_Add(channel->inq, got, buf);			// Add it to the buffer
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
							hlist_AddHeap(channel->inq, got, buf);	// Add it to the buffer
						}
					}
					if (got) channel->flags |= CHAN_DIRTY;
					haveReceived++;
				} else if( revents & POLLHUP ) {			// Other program terminated
																// Here means that the other end has gone away and nothing to read
Log("Channel on %d has gone away", fd);
					channel = chan_FindByFd(cp, fd);
					chan_Delete(channel);			// Channel has hung up so remove it from the loop
					// NB. channel is NULL here currently
					break;
				}

				if (revents & POLLOUT ) {				// We can write
					int len;

					if (hlist_Length(channel->outq)) {				// Must be non-zero or POLLOUT wouldn't set, but just in case...
						const char *data = (const char *)hlist_GetBlock(channel->outq, &len);
						int written = write(fd, data, len);	// Try to write it all
						if (written != len) {						// Didn't write it all
							hlist_Push(channel->outq, len-written, data+written);	// Push any remainder
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
		spmap_Reset(cp->map);

		while (spmap_GetNextEntry(cp->map, NULL, (void**)&channel)) {
			if (channel->flags & CHAN_DIRTY) {

				int handled = 0;
				if (channel->cb_receiver)
					handled = (channel->cb_receiver)(channel);
				int empty = !hlist_Length(channel->inq);

				if (!handled || empty)
					channel->flags &= ~CHAN_DIRTY;		// Don't immediately process next time if we failed or there is data
			}
		}

{ static int count = 0;count++; if (count > 15) { Log("Dropping out due to debug limit of loops"); break; } }

	} // Main loop (;;)
}
