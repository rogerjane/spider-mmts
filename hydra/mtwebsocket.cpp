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
#include <time.h>

#include <heapstrings.h>
#include <mtmacro.h>

#include "mtutf8.h"
#include "mtwebsocket.h"

#define LENIENT		0			// Make this 1 to be more lax on acceptable formats

#define WS_STATUS_OK			1000
#define WS_STATUS_GOING			1001
#define WS_STATUS_PROTOCOL		1002
#define WS_STATUS_UNACCEPTABLE	1003
#define WS_STATUS_1004			1004
#define WS_STATUS_NONE			1005
#define WS_STATUS_ABNORMAL		1006
#define WS_STATUS_BADDATA		1007
#define WS_STATUS_POLICY		1008
#define WS_STATUS_TOOBIG		1009
#define WS_STATUS_URTHICK		1010
#define WS_STATUS_CONFUSED		1011
#define WS_STATUS_1012			1012
#define WS_STATUS_1013			1013
#define WS_STATUS_1014			1014
#define WS_STATUS_TLS			1015

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
typedef void (*WSCB_Idler)(void *data);

typedef void WSIDLER;				// An opaque structure (really a CHANIDLER)

typedef struct WS {
	char *name;
	CHAN *channel;					// Channel used for comms
	WSCB_Receiver cb_utf8;			// Callback Handler for UTF8 messages (opcode = 1)
	WSCB_Receiver cb_binary;		// Callback Handler for BINARY messages (opcode = 2)
	WSCB_DeleteCallback cb_ondelete;	// Called just before we destruct the websocket
	void *info;						// Miscellaneous info for the use of the caller
	void *owner;					// Owner of this websocket - usually a WAMP*
	unsigned char opcode;			// Current opcode (used to handle continution frames)
	HBUF *inbuf;					// Used if we have fragments
	char wantMask;					// Set mask on outgoing fragments (should be set where connection is client->server)
	char deleting;					// 1 if we are currently physically deleting this object
} WS;

// END HEADER
#endif

STATIC SPMAP *allWebsockets = NULL;		// Map of all websockets by name

API void* ws_Info(WS *ws)				{ return ws ? ws->info : NULL; }
API void* ws_Owner(WS *ws)				{ return ws ? ws->owner : NULL; }

STATIC void ws_ApplyMask(long mask, int len, const void *data)
{
	if (mask && len && data) {								// If there is a mask, store it and adjust our copied data
		unsigned int *p = (unsigned int *)data;

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

API CHANPOOL *ws_Pool(WS *ws)
{
	return ws ? chan_Pool(ws->channel) : NULL;
}

STATIC const char *ws_OpName(unsigned char op)
{
	static char buf[10];

	switch (op) {
		case 0:	return "CONT";
		case 1: return "UTF8";
		case 2: return "BIN";
		case 8: return "CLOSE";
		case 9: return "PING";
		case 10: return "PONG";
	}

	snprintf(buf, sizeof(buf), "OP=%u", op);
	return buf;
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
	if (len > 65535) headlen += 6;

	if (plen) *plen=headlen+len;

	char *result = (char *)malloc(headlen+len);
	result[0] = (fin ? 128 : 0) + opcode;
	int l = len;
	if (len > 65535) {
		l = 127;
		result[2] = 0;
		result[3] = 0;
		result[4] = 0;
		result[5] = 0;
		result[6] = len >> 24;
		result[7] = len >> 16;
		result[8] = len >> 8;
		result[9] = len >> 0;
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

//Log("WS: ws_Close(%s, %d, %s)", ws->name, code, Printable(-1, buf+(code?2:0)));
		ws_PushMessage(ws, 0, 8, p-buf, buf);
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
// TODO: Consider allowing massive fragments (over 31 bits)?
{
//Log("WS: ws_HandleIncoming(%s)", chan_Name(channel));
	WS *ws = (WS *)chan_Info(channel);
	if (!ws) return 0;				// Channel is NULL (closed?) or no info (must be corrupt)
//Log("WS: chan %s -> ws %s, utf8 callback = %p", chan_Name(channel), ws_Name(ws), ws->cb_utf8);

	int got;
	const unsigned char *header = (const unsigned char*)chan_Peek(channel, 10, &got);

	if (got < 2) return 0;			// Not enough to do anything with
	unsigned char fin = header[0] & 0x80;
	unsigned char rsv = (header[0] >> 4) & 0x07;
	unsigned char op = header[0] & 0x0f;
	unsigned char masked = header[1] & 0x80;
	unsigned char len = header[1] & 0x7f;
	int needed = (len < 126) ? 2 : len == 126 ? 4 : 10;
	if (masked) needed += 4;
	if (got < needed) return 0;		// Not enough for length and mask

//Log("WS: OPCODE=%d, FIN=%s, RSV=%d, LEN=%d, MASKED=%s", op, fin?"True":"False", rsv, len, masked?"Yes":"No");
	if (!LENIENT && rsv) {
		ws_Close(ws, WS_STATUS_PROTOCOL, "fragment received with RSV != 0");
		return 1;
	}

	if ((op > 2 && op < 8) || op > 10) {			// Valid op codes are 0, 1, 2, 8, 9 and 10
		ws_Close(ws, WS_STATUS_PROTOCOL, "Unrecognised OP code (%d) received", op);
	}

	long length;					// Length of block
	unsigned long mask = 0;			// Mask if it is masked
	if (len < 126) {
		length = len;
	} else if (len == 126) {
		length = (header[2] << 8) | header[3];
	} else {
		if (header[2] || header[3] || header[4] || header[5] || (header[6] & 0x80))
			ws_Close(ws, WS_STATUS_TOOBIG, "Fragment too large - can only handle up to 31 bits");

		length = (header[6] << 24) | (header[7] << 16) | (header[8] << 8) | header[9];
	}

	if (masked) {					// Should only occur client to server but we won't be picky (autobahn tests don't test this!)
		mask = header[needed-4] << 24 | header[needed-3] << 16 | header[needed-2] << 8 | header[needed-1];
	}

	if (chan_Available(channel) < needed+length)
		return 0;					// The entire fragment hasn't been received yet

	const char *data = (const char *)malloc(needed+length+1);	// +1 as we'll plonk a nice '\0' on the end
	chan_GetData(channel, needed+length, (char*)data);

	header = (const unsigned char *)data;
	data = (char*)(header+needed);	// header is now only so we can properly free later, data is the actual data (len = length)
	((char*)data)[length]='\0';		// Terminate it to make parsing etc. so much easier

	if (mask) {						// Need exclusive OR our way over the data with the mask
		ws_ApplyMask(mask, length, data);
	}

	// Here we have the fragment nicely decoded.

	// Deal with control op codes, which can occur in the middle of data
	if (op >= 8) {
		if (!LENIENT) {						// Check things that would be ok, but disallowed by the spec
			if (length >= 126) {			// Control messages cannot be over 125 bytes
				ws_Close(ws, WS_STATUS_PROTOCOL, "Control frame (%s) receeived with length > 125 (%d)", ws_OpName(op), length);
				free((char*)header);
				return 1;
			}
		}

		if (!fin) {
			ws_Close(ws, WS_STATUS_PROTOCOL, "Fragmented %s message not allowed", ws_OpName(op));
				free((char*)header);
			return 1;
		}

		if (op == 8) {
			if (length == 1) {
				ws_Close(ws, WS_STATUS_PROTOCOL, "Invalid close message");
			} else {
				if (length > 0) {
					const char *message = data + 2;
					unsigned int reason = (((unsigned char*)data)[0] << 8) + ((unsigned char *)data)[1];
					if (reason < 1000
							|| reason == WS_STATUS_1004
							|| reason == WS_STATUS_NONE
							|| reason == WS_STATUS_ABNORMAL
							|| reason == WS_STATUS_1012
							|| reason == WS_STATUS_1013
							|| reason == WS_STATUS_1014
							|| reason == WS_STATUS_TLS
							|| (reason > 1015 && reason < 3000)
							|| reason >= 5000) {
						ws_Close(ws, WS_STATUS_PROTOCOL, "Invalid close reason (%u)", reason);
					} else if (utf8_IsValidString(length-2, message)) {
						ws_Close(ws, WS_STATUS_BADDATA, "Close message isn't UTF8");
					} else {
						ws_Close(ws, WS_STATUS_OK, "Goodbye");
					}
				} else {
					ws_Close(ws, WS_STATUS_OK, "Goodbye");
				}
			}
		} else if (op == 9) {
//			Log("WS: We have a ping on %s (len=%d: %s), I'll send a pong...", ws->name, length, Printable(length <= 25 ? length : 25, data));
			ws_Pong(ws, length, data);			// Send a pong with the same data
		} else if (op == 10) {
			Log("WS: We have been ponged on %s (len=%d: %s) - ignoring it...", ws->name, length, Printable(-1, data));
			// We should perhaps check that a pong received in response to a ping of ours contains the same data but
			// I have no reason to care and don't see a need to return any error to anyone if this should fail.
		}

		free((char*)header);
		return 1;
	}

	// At this point, we only have data op codes - 0 (continuation) or 1/2 (text/binary)

	if (!op && !ws->opcode) {
		ws_Close(ws, WS_STATUS_PROTOCOL, "Non-zero opcode in final frame of multi-frame message");
		free((char*)header);
		return 1;
	}

	if (op && ws->opcode) {
		ws_Close(ws, WS_STATUS_PROTOCOL, "Non-zero opcode in websocket continuation fragment");
		free((char*)header);
		return 1;
	}

	if (!fin) {									// This is only part of a message, add to buffer and return (or error)
		if (op) {								// First fragment in message
			ws->opcode = op;
		}
#if 0			/// Check UTF8 validity early, but there are boundary conditions...
		// We can be clever here by checking the return value if it fails near the end of the data and re-checking from that
		// point when we get the next fragment (if we're due another fragment)
		if (ws->opcode == 1) {					// UTF-8 so we check it here
			if (length && utf8_IsValidString(length, data)) {
				ws_Close(ws, WS_STATUS_BADDATA, "Invalid UTF8");
				return 1;
			}
		}
#endif
//Log("WS: Fragment received (op=%d), adding it to existing data at %p", op, ws->inbuf);
		hbuf_AddBuffer(ws->inbuf, length, data);
		free((char*)header);
		return 1;
	}

	int dataAllocated = 0;			// 1 if data is a separate heap buffer that'll want freeing

	// We have a complete message as this is the final fragment
	if (ws->opcode) {							// We have some data from previous fragments
		op = ws->opcode;
		hbuf_AddBuffer(ws->inbuf, length, data);
		length = hbuf_GetLength(ws->inbuf);
		data = hbuf_ReleaseBuffer(ws->inbuf);
		dataAllocated = 1;
		header = (const unsigned char *)realloc((void*)header, needed);
//Log("Assembled data at %p, header chopped to %d bytes at %p", data, needed, header);
	}
	ws->opcode = 0;								// Reset op code for next time

	// NB. At this point we may have two layouts for header and data
	// A single block allocated as header+data	- Message was a single fragment (dataAllocated = 0)
	// Separate header and data blocks			- Message was in fragments (dataAllocated = 1)

	if (op == 0) {											// UTF text
		ws_Close(ws, WS_STATUS_PROTOCOL, "Zero opcode in message");
	} else if (op == 1) {
//Log("Callback for UTF8 on %s = %p", ws_Name(ws), ws->cb_utf8);
		if (ws->cb_utf8) {
			(*ws->cb_utf8)(ws, length, data);
		} else {							// No handler specified so we'll echo data back (this passes the autobahn tests!)
			if (length && utf8_IsValidString(length, data)) {
				ws_Close(ws, 1007, "Invalid UTF8");
			} else {
//Log("ws_PushMessage(%s, 0, %d, %d, %s)", ws->name, op, length, Printable(length,data));
				ws_PushMessage(ws, 0, op, length, data ? data : "");
			}
		}
	} else if (op == 2) {									// Binary data
		if (ws->cb_binary) {
			(*ws->cb_binary)(ws, length, data);
		} else {							// No handler specified so we'll echo data back (this passes the autobahn tests!)
			ws_PushMessage(ws, 0, op, length, data ? data : "");
		}
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
		if (ws->deleting) return;				// Already deleting
		ws->deleting = 1;

		Log("WS: Deleting %s (channel = %s)", ws_Name(ws), chan_Name(ws->channel));

		if (ws->cb_ondelete)
			(ws->cb_ondelete)(ws);

		if (ws->channel) {
			chan_RegisterReceiver(ws->channel, NULL, 0);
			chan_Delete(ws->channel);
		}

//Log("WS: Deleting our name (%s) from the list of websockets", ws->name);
		spmap_DeleteKey(allWebsockets, ws->name);
		hbuf_Delete(ws->inbuf);
		szDelete(ws->name);
		free((char*)ws);
	}
}

STATIC void ws_OnChannelDelete(CHAN *channel)
// Our channel is being deleted so we need to tidy ourselves up similarly
{
	if (channel) {
		WS *ws = (WS*)chan_Owner(channel);

		if (ws) {
			ws_Delete(ws);
		}
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
			chan_OnDelete(ws->channel, NULL);
		}

		ws->channel = channel;
		if (channel) {
			chan_SetInfo(channel, ws);
			chan_RegisterReceiver(channel, ws_HandleIncoming, 2);
			chan_OnDelete(channel, ws_OnChannelDelete);
		}
	}
}

API const char *ws_Name(WS *ws)
// Returns the name of the websocket
{
	return ws ? ws->name : "NULL_WEBSOCKET";
}

API WS *ws_ByName(const char *name)
// Returns the websocket matching the name given
{
	return name ? (WS*)spmap_GetValue(allWebsockets, name) : NULL;
}

API void *ws_SetInfo(WS *ws, void *info)
// Sets the 'info' data for this websocket.  This is not used by the websocket itself but is available for the registered
// receiver.  E.g. for wamp it should be set to be the WAMP* that is using this websocket
{
	void *previous = NULL;
	if (ws) {
		previous = ws->info;
		ws->info = info;
	}

	return previous;
}

API void *ws_SetOwner(WS *ws, void *owner)
// Sets the 'owner' for this websocket.  This would generally be a WAMP*
{
	void *previous = NULL;
	if (ws) {
		previous = ws->owner;
		ws->owner = owner;
	}

	return previous;
}

API void ws_EnableMask(WS *ws, char state)
// Enables or disables mask on sending.
// This should be called after ws_NewOnChannel() where the connection is client -> server
// In reality, I doubt any server will be picky about this... (Apparently I'm wrong!)
{
	if (ws)
		ws->wantMask = state;
}

// TODO - I am not needed
//STATIC void ws_EchoHandler(CHAN *channel)
// Used as the callback for data connections before a 'real' handler is assigned.
// Simply echos back any packets received.
//{
//}

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
	ws->cb_ondelete = NULL;
	ws->inbuf = hbuf_New();
	ws->info = NULL;
	ws->owner = NULL;
	ws->opcode = 0;
	ws->channel = NULL;
	ws->wantMask = 0;
	ws->deleting = 0;

	chan_SetOwner(channel, ws);
	chan_OnDelete(channel, ws_OnChannelDelete);
	ws_SetChannel(ws, channel);

	if (!allWebsockets) allWebsockets = spmap_New();
	spmap_Add(allWebsockets, ws->name, (void*)ws);

	return ws;
}

API void ws_PushMessage(WS *ws, int immediate, int opcode, int len, const char *data)
// Push a message to the outgoing channel
// The data is copied so the caller is still responsible for it.
{
//Log("WS: PushMessage(%s, %d, %s, %d, %s)", ws_Name(ws), immediate, ws_OpName(opcode), len, Printable(len < 50 ? len : 50, data));
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

API WSIDLER *ws_CallAfter(WS *ws, int period, WSCB_Idler idler, void *data)
{
    return ws ? chan_CallAfter(ws->channel, period, idler, data) : NULL;
}

API WSIDLER *ws_CallEvery(WS *ws, int period, WSCB_Idler idler, void *data)
{
    return ws ? chan_CallEvery(ws->channel, period, idler, data) : NULL;
}

