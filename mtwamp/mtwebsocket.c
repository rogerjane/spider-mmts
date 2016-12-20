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
#include <string.h>

#include <heapstrings.h>
#include <mtmacro.h>

#include "mtwebsocket.h"

#if 0
// START HEADER

#include <openssl/bio.h>

#include "mtchannel.h"
#include "hbuf.h"
#include "smap.h"

struct WS;

typedef int (*WSCB_Receiver)(struct WS *channel, int len, const char *text);
typedef int (*WSCB_Closed)(struct WS *channel);
typedef void (*WSCB_DeleteCallback)(struct WS *channel);

typedef struct WS {
	char *name;
	CHAN *channel;					// Channel used for comms
	WSCB_Receiver cb_utf8;			// Callback Handler for UTF8 messages (opcode = 1)
	WSCB_Receiver cb_binary;		// Callback Handler for BINARY messages (opcode = 2)
	WSCB_Closed cb_closed;			// Called just prior to deleting this websocket
	WSCB_DeleteCallback cb_ondelete;	// Called just before we destruct the websocket
	void *info;						// Miscellaneous info for the use of the caller
	unsigned char opcode;			// Current opcode (used to handle continution frames)
	HBUF *inbuf;					// Used if we have fragments
	char wantMask;					// Set mask on outgoing fragments (should be set where connection is client->server)
	char deleting;					// 1 if we are currently physically deleting this object
} WS;

// END HEADER
#endif

STATIC SPMAP *allWebsockets = NULL;		// Map of all websockets by name

STATIC void ws_ApplyMask(long mask, int len, const void *data)
{
	if (mask && len && data) {								// If there is a mask, store it and adjust our copied data
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

API void ws_Logger(void (*logger)(const char *, va_list))
// Sets a logging function to receive all debug output from the websocket library
{
	_Logger = logger;
}

API const char *ws_MakeFragment(int fin, int opcode, long mask, int len, const char *data, int *plen)
// Returns a heap-based fragment ready to send as a WebSocket fragment, with the length in *plen
// There is precious little error-checking/argument checking in here as the caller should take care.
// The arguments should be:
// fin		0 - there will be another fragment, non-0 this is the last
// opcode	0, 1, 2, 8, 9, 10
// mask		0 to not mask, otherwise the mask (special case, -1 means use a mask of '0')
// len		The length of 'data' (-1 means strlen(data))
// data		The data
// plen		non-NULL to receive the resultent length

{
	if (!data) return NULL;									// I said 'little error checking' but this would be careless

//Log("WS: ws_MakeFragment(%d, %d, %x, %d, %p, %p)", fin, opcode, mask, len, data, plen);
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
		result[2] = len >> 24;
		result[3] = len >> 16;
		result[4] = len >> 8;
		result[5] = len >> 0;
	} else if (len > 125) {
		l = 126;
		result[2] = len >> 8;
		result[3] = len >> 0;
	}
	result[1] = (mask ? 128 : 0) + l;
	memcpy(result+headlen, data, len);

	if (mask) {												// If there is a mask, store it and adjust our copied data
		if (mask == -1) mask = 0;							// Makes it much easier to see what's going on!
		*(int*)(result+headlen-4)=mask;
		ws_ApplyMask(mask, len, data+headlen);
	}

	return result;
}

API int ws_WriteHeap(WS *ws, int len, const char *data)
// Just like ws_Write() but deletes the data afterwards
{
	int result = 0;

	if (ws && ws->channel && len && data) {
		return chan_WriteHeap(ws->channel, len, data);
	}

	return result;
}

API int ws_Write(WS *ws, int len, const char *data)
// Put some data on the WebSocket (by adding it to its output queue)
{
	int result = 0;

	if (ws && ws->channel && len && data) {
		return chan_Write(ws->channel, len, data);
	}

	return result;
}

STATIC void ws_Close(WS *ws, int code, const char *fmt, ...)
// Sends a close packet with an optional error message formatted in the data segment
// Note that this will result in a callback from the channel when it closes, which will call ws_OnChannelClosed()
// which in turn will delete this websocket.
{
//Log("WS: ws_Close(%p, %d, %s, ...)", h, code, fmt);
	if (ws && ws->channel) {
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

		ws_PushMessage(ws, 1, 8, p-buf, buf);
		chan_CloseOnEmpty(ws->channel);
		ws->channel = NULL;
	}
}

API void ws_SetUtf8Handler(WS *ws, WSCB_Receiver fn)
// Sets a handler for incoming UTF-8 formatted data
{
	if (ws) ws->cb_utf8 = fn;
}

API void ws_SetBinaryHandler(WS *ws, WSCB_Receiver fn)
// Sets a handler for incoming binary formatted data
{
	if (ws) ws->cb_binary = fn;
}

STATIC int ws_HandleIncoming(CHAN *channel)
// Figure what sort of data it is and call sensible function
// Returns 1 if we successfully removed something from the queue
// TODO: Need a way of handling an error here...
{
	WS *ws = chan_Info(channel);
	if (!ws) return 0;				// Channel is NULL (closed?) or no info (must be corrupt)

	int got;
	HLIST *h = chan_IncomingQueue(channel);
	const unsigned char *header = (const unsigned char*)hlist_Peek(h, 10, &got);

//Log("WS: ws_HandleIncoming(%s) - got = %d", chan_Name(channel), got);
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
		length = (header[2] << 8) | header[3];
	} else {
		length = (header[2] << 24) | (header[3] << 16) | (header[4] << 8) | header[5];
	}
	if (masked) {
		mask = header[needed-4] << 24 | header[needed-3] << 16 | header[needed-2] << 8 | header[needed-1];
	}

//Log("WS: h[0] = %x, h[1] = %x, h[2] = %x, h[3] = %x", header[0], header[1], header[2], header[3]);
//Log("WS: h[0] = %d, h[1] = %d, h[2] = %d, h[3] = %d", header[0], header[1], header[2], header[3]);
//Log("WS: Point B (hlist_Length = %Ld, needed = %d, length = %d", hlist_Length(h), needed, length);
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
		   if (hbuf_GetLength(ws->inbuf)) {		// Error - opcode should be 0 for fragments 2...
			   ws_Close(ws, 1002, "Non-zero opcode in websocket continuation frame");
			   return 1;
		   } else {
				ws->opcode = op;
		   }
		}
		hbuf_AddBuffer(ws->inbuf, length, data);
		free((char*)header);
		return 1;
	} else {
		if (hbuf_GetLength(ws->inbuf)) {			// We have some prefixing data
			if (op) {
				ws_Close(ws, 1002, "Non-zero opcode in final frame of multi-frame message");
				return 1;
			}
			op = ws->opcode;
			hbuf_AddBuffer(ws->inbuf, length, data);
			length = hbuf_GetLength(ws->inbuf);
			data = hbuf_ReleaseBuffer(ws->inbuf);
			dataAllocated = 1;
			header = (const unsigned char *)realloc((void*)header, needed);
		}
	}
	// NB. At this point we may have two layouts for header and data
	// A single block allocated as header+data	- Message was a single fragment (dataAllocated = 0)
	// Separate header and data blocks			- Message was in fragments (dataAllocated = 1)

	switch (op) {
		case 0:									// Error, opcode should not be 0 at this stage
			ws_Close(ws, 1002, "Zero opcode in message");
			break;
		case 1:									// UTF-8 text
//Log("WS: UTF received on %s, calling handler at %p", ws_Name(ws), ws->cb_utf8);
			if (ws->cb_utf8)
				(*ws->cb_utf8)(ws, length, data);
			break;
		case 2:									// Binary data
			if (ws->cb_binary)
				(*ws->cb_binary)(ws, length, data);
			break;
		case 8:									// Close
			ws_Close(ws, 1000, "Goodbye");
			break;
		case 9:									// Ping
Log("WS: We have a ping, I'll send a pong...");
			ws_Pong(ws, length, data);			// Send a pong with the same data
			break;
		case 10:								// Pong
			// We should perhaps check that a pong received in response to a ping of ours contains the same data but
			// I have no reason to care and don't see a need to return any error to anyone if this should fail.
			break;
		default:
			break;
	}

	free((char*)header);
	if (dataAllocated) free((char*)data);

	return 1;
}

API WSCB_DeleteCallback ws_OnDelete(WS *ws, WSCB_DeleteCallback cb)
// Register a function to be called when this object is deleted.
// Returns the previous function.
{
	WSCB_DeleteCallback previous = NULL;
	if (ws) {
		previous = ws->cb_ondelete;
		ws->cb_ondelete = cb;
	}

	return previous;
}

API void ws_Delete(WS *ws)
// Deleting a channel will automatically remove it from its list (if it's in one)
{
	if (ws) {
//Log("WS: Contemplating deletion of %s", ws->name);
		if (ws->deleting) return;				// Already deleting
		ws->deleting = 1;
Log("WS: Actually deleting %s (%p)", ws->name, ws->cb_ondelete);

		if (ws->cb_ondelete)
			(ws->cb_ondelete)(ws);

		if (ws->channel) {
			chan_RegisterReceiver(ws->channel, NULL, 0);
			chan_Delete(ws->channel);
		}

		spmap_DeleteKey(allWebsockets, ws->name);
		hbuf_Delete(ws->inbuf);
		szDelete(ws->name);
		free((char*)ws);
	}
}

STATIC void ws_OnChannelDeleted(CHAN *channel)
{
	if (channel) {
		WS *ws = chan_Info(channel);

		Log("WS: Websocket saying goodbye to channel %s (ws=%s)", chan_Name(channel), ws_Name(ws));

		ws_Delete(ws);
	}
}

STATIC int ws_OnChannelClosed(CHAN *channel)
// Our channel has closed and is about to be deleted so we need to tidy ourselves up similarly
{
	WS *ws = chan_Info(channel);
	if (ws) {				// This should not happen...
Log("WS: Websocket: Channel %s has closed so I'm deleting myself (%s)", chan_Name(channel), ws_Name(ws));
if (ws->cb_closed) Log("WS: Calling up to warn of closure - %p", ws->cb_closed);
		if (ws->cb_closed)
			(*ws->cb_closed)(ws);
		ws->channel = NULL;				// Channel is about to close itself
		ws_Delete(ws);
	}
}

API void ws_SetChannel(WS *ws, CHAN *channel)
// Forcibly sets the channel to which this websocket is connected, returning the previous one.
// This is intended for internal use but provided in case an adventurous caller needs it.
{
	if (ws) {
		if (ws->channel) {
			chan_SetInfo(ws->channel, NULL);
			chan_RegisterReceiver(ws->channel, NULL, 0);
			chan_RegisterClosed(ws->channel, NULL);
			chan_OnDelete(ws->channel, NULL);
		}

		ws->channel = channel;
		if (channel) {
			chan_SetInfo(channel, ws);
			chan_RegisterReceiver(channel, ws_HandleIncoming, 2);
			chan_RegisterClosed(channel, ws_OnChannelClosed);
			chan_OnDelete(channel, ws_OnChannelDeleted);
		}
	}
}

API const char *ws_Name(WS *ws)
// Returns the name of the websocket
{
	return ws ? ws->name : NULL;
}

API WS *ws_ByName(const char *name)
// Returns the websocket matching the name given
{
	return name ? (WS*)spmap_GetValue(allWebsockets, name) : NULL;
}

API WS *ws_SetInfo(WS *ws, void *info)
// Sets the 'info' data for this websocket.  This is not used by the websocket itself but is available for the registered
// receiver.  E.g. for wamp it should be set to be the WAMP* that is using this websocket
{
	WS *previous = NULL;
	if (ws) {
		previous = ws->info;
		ws->info = info;
	}

	return previous;
}

API void *ws_Info(WS *ws)
// Returns the 'info' pointer previously set using ws_SetInfo()
{
	return ws ? ws->info : NULL;
}

API void ws_EnableMask(WS *ws, char state)
// Enables or disables mask on sending.
// This should be called after ws_NewOnChannel() where the connection is client -> server
// In reality, I doubt any server will be picky about this... (Apparently I'm wrong!)
{
	if (ws)
		ws->wantMask = state;
}

API WS *ws_NewOnChannel(CHAN *channel)
// Creates a new websocket and connects it to the channel if specified
// fd = 0..., io = NULL		// Plain 'fd' connection
// fd = -1, io = BIO*		// BIO * connection (fd is ascertained from io)
// flags					// a combination of WS_* values
{
	WS *ws = NEW(WS, 1);

	ws->name = hprintf(NULL, "ws_%s", chan_Name(channel));
	ws->cb_utf8 = NULL;
	ws->cb_binary = NULL;
	ws->cb_closed = NULL;
	ws->cb_ondelete = NULL;
	ws->inbuf = hbuf_New();
	ws->info = NULL;
	ws->opcode = 0;
	ws->channel = NULL;
	ws->wantMask = 0;
	ws->deleting = 0;

	ws_SetChannel(ws, channel);

	if (!allWebsockets) allWebsockets = spmap_New();
	spmap_Add(allWebsockets, ws->name, (void*)ws);

	return ws;
}

API void ws_PushMessage(WS *ws, int immediate, int opcode, int len, const char *data)
// Push a message to the outgoing channel
// The data is copied so the caller is still responsible for it.
{
	if (ws && ws->channel) {
		int fraglen;
		long mask = 0;

		if (ws->wantMask) {
			static char inited = 0;
			if (!inited++)
				    srand48(time(NULL));		// We don't care enough about having completely distinct masks across processes

			mask = lrand48();
		}

		const char *fragment = ws_MakeFragment(1, opcode, mask, len, data, &fraglen);

		if (immediate)
			chan_WriteFrontHeap(ws->channel, fraglen, fragment);
		else
			chan_WriteHeap(ws->channel, fraglen, fragment);
	}
}

API void ws_Ping(WS *ws, int len, const char *data)
// Sends a 'ping' through the websocket
{
	ws_PushMessage(ws, 0, 9, len, data);		// Send a ping
}

API void ws_Pong(WS *ws, int len, const char *data)
// Sends a 'ping' through the websocket
{
	ws_PushMessage(ws, 0, 10, len, data);	// Send a pong
}
