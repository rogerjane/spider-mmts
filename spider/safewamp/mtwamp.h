#ifndef __MTWAMP_H
#define __MTWAMP_H


#include "smap.h"

#include "mtwebsocket.h"
#include "mtjson.h"

#define IDMAP	SPMAP

// Typedefs for WAMP callback functions
struct WAMP;
typedef int (*WAMPCB_NoCallee)(const char *name);
typedef void (*WAMPCB_Invokee)(struct WAMP *wamp, long long requestId, const char *procedure, JSON *list, JSON *dict);
typedef void (*WAMPCB_Data)(struct WAMP *wamp, JSON *json);
typedef int (*WAMPCB_Handler)(struct WAMP *wamp, int type, JSON *json);
typedef void (*WAMPCB_DeleteCallback)(struct WAMP *wamp);

typedef struct WAMP {
	const char *name;				// Unique ID for this wamp
	int connectionType;				// 1 = websocket, 2 = rawsocket, 3 = code (not currently implemented)
	union {
		WS *ws;						// The websocket this WAMP lives on
		WAMPCB_Data fn;				// Call back routine for code-based WAMPS
	} connection;
	IDMAP *handlers;
	HLIST *saveOutput;				// If non-null, receives a copy of all output
	HLIST *saveInput;				// If non-null, receives a copy of all input
	void *info;								// Miscellaneous info owned by the thing creating this WAMP
	struct WAMPREALM *realm;
	struct WAMPSESSION *session;
	WAMPCB_DeleteCallback cb_ondelete;		// Called just before object deleted
	char deleting;							// 1 while actually deleting
	char masked;							// Non-0 if we want to send outgoing masks
} WAMP;

#define WAMP_EXACT			1		// Used in wamp_RegisterCallee
#define WAMP_PREFIX			2
#define WAMP_WILDCARD		3

#define WAMP_HELLO			1
#define WAMP_WELCOME		2
#define WAMP_ABORT			3
#define WAMP_GOODBYE		6

#define WAMP_ERROR			8

#define WAMP_PUBLISH		16
#define WAMP_PUBLISHED		17

#define WAMP_SUBSCRIBE		32
#define WAMP_SUBSCRIBED		33
#define WAMP_UNSUBSCRIBE	34
#define WAMP_UNSUBSCRIBED	35
#define WAMP_EVENT			36

#define WAMP_CALL			48
#define WAMP_RESULT			50

#define WAMP_REGISTER		64
#define WAMP_REGISTERED		65
#define WAMP_UNREGISTER		66
#define WAMP_UNREGISTERED	67
#define WAMP_INVOCATION		68
#define WAMP_YIELD			70


const char *wamp_Name(WAMP *w);
// Returns the name of a WAMP.  This can later be used in wamp_ByName().
// If you want to refer to a WAMP later, using a name is safer as WAMPs can be deleted...
// WAMP *savedWamp = myWamp; ...... time passes .......  wamp_WriteJson(savedWamp, ...) can fail if it's been deleted in the middle
// const char *savedWamp = wamp_Name(myWamp); .....  WAMP *wamp = wamp_ByName(savedWamp); if (wamp) wamp_WriteJson(... is safe.

const char *wamp_TypeName(int type);
// Given a WAMP message type number, returns the name

long long wamp_RandomId();
// Returns a random number 1..2^53-1

WAMP *wamp_SetInfo(WAMP *w, void *info);
// Sets the 'info' data for this wamp.  This is not used by the wamp itself but is available for the caller

void *wamp_Info(WAMP *w);
// Returns the 'info' pointer previously set using wamp_SetInfo()

HLIST *wamp_SaveOutput(WAMP *wamp, HLIST *hlist);
// Save a copy of all output from the WAMP in hlist.
// Returns the previous value

HLIST *wamp_SaveInput(WAMP *wamp, HLIST *hlist);
// Save a copy of all input to the WAMP in hlist.
// Returns the previous value

void wamp_SetMasked(WAMP *wamp, int masked);
WAMP *wamp_NewPseudo(WAMPCB_Data fn, const char *name);
WAMP *wamp_NewOnWebsocket(WS *ws, int masked);
WAMP *wamp_ByName(const char *name);
void wamp_WriteJson(WAMP *wamp, JSON *json);
void wamp_WriteJsonHeap(WAMP *wamp, JSON *json);
JSON *wamp_NewMessage(int type, long long id);
// Returns a new JSON array with the type filled in and the id if it's non-0

void wamp_SendError(WAMP *wamp, int type, long long id, SPMAP *details, const char *uri, JSON *argList, JSON *argDict);
// If details, argList and/or argDict are non-NULL, they become owned, then deleted here.
// Generic error sending
// wamp			- Where we're sending the error
// type			- E.g. WAMP_INVOCATION, WAMP_CALL etc.
// id			- The identifier which will (hopefully) allow the receipient to tie up the error with the message that caused it
// details		- NULL or a details MAP
// uri			- A WAMP error uri
// argList	- NULL or some JSON containing the argument list array
// argDict	- NULL or some JSON containing the argument list dictionary

void wamp_SendErrorStr(WAMP *wamp, int type, long long id, const char *err, const char *uri, JSON *argList, JSON *argDict);
// If argList and/or argDict are non-NULL, they become owned, then deleted here.

void wamp_SendResult(WAMP *wamp, long long requestId, JSON *details, JSON *argList, JSON *argDict);
// If details, argList and/or argDict are non-NULL, they become owned, then deleted here.

void wamp_CloseAbort(WAMP *wamp, int type, const char *uri, const char *reason);
// Close and Abort formats are identical apart from the type

void wamp_Abort(WAMP *wamp, const char *uri, const char *reason);
void wamp_Close(WAMP *wamp, const char *uri, const char *reason);
// Sends a WAMP close message with the given reason

void wamp_Finish(WAMP *wamp);
// We have finished with this WAMP so close the associated channel and delete it.

int wamp_SetRealm(WAMP *w, const char *name);
void wamp_CallbackNoCallee(WAMPCB_NoCallee cb);
WAMPCB_DeleteCallback wamp_OnDelete(WAMP *w, WAMPCB_DeleteCallback cb);
void wamp_RegisterInvokee(WAMPCB_Invokee cb);
void wamp_Delete(WAMP *w);
int wamp_Hello(WAMP *wamp, const char *realm, JSON *opts);
// Does the work of 'helloing' a wamp connection, which is useful if we want to create an already registered
// channel.  E.g. Connecting a legacy API handler.
// Returns	0	Everything went swimmingly
//			1	Was already in a realm
//			2	Realm didn't accept the connection (no such realm)
//			3	Was already in a session

const char *wamp_RegisterWampCallee(WAMP *wamp, const char *match, const char *invoke, long long priority, const char *procedure, long long *registrationIdp);
const char *wamp_RegisterInternalCallee(const char *realmName, const char *match, const char *procedure, WAMPCB_Invokee invokee, long long *registrationIdp);
// Registers an internal invokee for a realm
// Returns		NULL	All went OK
//				char*	Error message (treat as static)

void wamp_RegisterRedirect(const char *realmName, int type, WAMPCB_Handler fn);
// Sets a function that will be called for all messages of the type given for the given realm.
// If the function returns -1, the normal processing will be performed instead.
// Any other value will be returned as the result of wamp_Dispatch() - 0=ok, 1...=error

void wamp_RegisterHandler(WAMP *wamp, int type, WAMPCB_Handler fn);
void wamp_Ping(WAMP *wamp, int len, const char *data);
void wamp_Pong(WAMP *wamp, int len, const char *data);
int wamp_SetForwardSubscriptions(const char *realmName, int enable, WAMP *wamp);
int wamp_Publish(WAMP *wamp, const char *topic, SPMAP *options, JSON *argList, JSON *argDict);
// Publishes the topic
// Takes ownership of options, argList and argDict.

int wamp_Dispatch(WAMP *wamp, int type, JSON *json);
// Does whatever needs to be done to deal with a message of type 'type'
// The JSON* provided is definitely an array with at least one element.
// Returns 0 or an error code

#endif
