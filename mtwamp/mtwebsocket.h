#ifndef __MTWEBSOCKET_H
#define __MTWEBSOCKET_H


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


void ws_Logger(void (*logger)(const char *, va_list));
// Sets a logging function to receive all debug output from the websocket library

const char *ws_MakeFragment(int fin, int opcode, long mask, int len, const char *data, int *plen);
// Returns a heap-based fragment ready to send as a WebSocket fragment, with the length in *plen
// There is precious little error-checking/argument checking in here as the caller should take care.
// The arguments should be:
// fin		0 - there will be another fragment, non-0 this is the last
// opcode	0, 1, 2, 8, 9, 10
// mask		0 to not mask, otherwise the mask (special case, -1 means use a mask of '0')
// len		The length of 'data' (-1 means strlen(data))
// data		The data
// plen		non-NULL to receive the resultent length

int ws_WriteHeap(WS *ws, int len, const char *data);
// Just like ws_Write() but deletes the data afterwards

int ws_Write(WS *ws, int len, const char *data);
// Put some data on the WebSocket (by adding it to its output queue)

void ws_SetUtf8Handler(WS *ws, WSCB_Receiver fn);
// Sets a handler for incoming UTF-8 formatted data

void ws_SetBinaryHandler(WS *ws, WSCB_Receiver fn);
// Sets a handler for incoming binary formatted data

WSCB_DeleteCallback ws_OnDelete(WS *ws, WSCB_DeleteCallback cb);
// Register a function to be called when this object is deleted.
// Returns the previous function.

void ws_Delete(WS *ws);
// Deleting a channel will automatically remove it from its list (if it's in one)

void ws_SetChannel(WS *ws, CHAN *channel);
// Forcibly sets the channel to which this websocket is connected, returning the previous one.
// This is intended for internal use but provided in case an adventurous caller needs it.

const char *ws_Name(WS *ws);
// Returns the name of the websocket

WS *ws_ByName(const char *name);
// Returns the websocket matching the name given

WS *ws_SetInfo(WS *ws, void *info);
// Sets the 'info' data for this websocket.  This is not used by the websocket itself but is available for the registered
// receiver.  E.g. for wamp it should be set to be the WAMP* that is using this websocket

void *ws_Info(WS *ws);
// Returns the 'info' pointer previously set using ws_SetInfo()

void ws_EnableMask(WS *ws, char state);
// Enables or disables mask on sending.
// This should be called after ws_NewOnChannel() where the connection is client -> server
// In reality, I doubt any server will be picky about this... (Apparently I'm wrong!)

WS *ws_NewOnChannel(CHAN *channel);
// Creates a new websocket and connects it to the channel if specified
// fd = 0..., io = NULL		// Plain 'fd' connection
// fd = -1, io = BIO*		// BIO * connection (fd is ascertained from io)
// flags					// a combination of WS_* values

void ws_PushMessage(WS *ws, int immediate, int opcode, int len, const char *data);
// Push a message to the outgoing channel
// The data is copied so the caller is still responsible for it.

void ws_Ping(WS *ws, int len, const char *data);
// Sends a 'ping' through the websocket

void ws_Pong(WS *ws, int len, const char *data);
// Sends a 'ping' through the websocket

#endif
