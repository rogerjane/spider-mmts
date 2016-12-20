#ifndef __MTWAMP_H
#define __MTWAMP_H


#include <stdarg.h>
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

typedef void (*WAMPCB_Subscribed)(struct WAMP *wamp, long long reqId, long long subId, JSON *details, const char *uri);
typedef void (*WAMPCB_Published)(struct WAMP *wamp, long long reqId, long long pubId, JSON *details, const char *uri);
typedef void (*WAMPCB_Event)(struct WAMP *wamp, long long subId, const char *topic, JSON *details, JSON *list, JSON *dict);
typedef void (*WAMPCB_Unsubscribed)(struct WAMP *wamp, long long reqId, long long subId, JSON *details, const char *uri);

typedef void (*WAMPCB_Registered)(struct WAMP *wamp, long long reqId, long long regId, JSON *details, const char *uri);
typedef void (*WAMPCB_Result)(struct WAMP *wamp, long long reqId, const char *procedure, const char *uri, JSON *details, JSON *list, JSON *dict);
typedef JSON *(*WAMPCB_Invoke)(struct WAMP *wamp, long long reqId, long long regId, const char *procedure, JSON *details, JSON *list, JSON *dict);
typedef void (*WAMPCB_Unregistered)(struct WAMP *wamp, long long reqId, long long regId, JSON *details, const char *uri);
typedef void (*WAMPCB_Welcome)(struct WAMP *wamp, long long sessionId, const char *uri, JSON *details);

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
	WAMPCB_Welcome cbWelcome;				// Called when a welcome message is received
	char deleting;							// 1 while actually deleting
	char masked;							// Non-0 if we want to send outgoing masks

	IDMAP *clientSubscriptionsReq;				// Things we're subscribed to by requestID
	IDMAP *clientSubscriptionsSub;				// Things we're subscribed to by subscriptionID
	IDMAP *clientSubscriptionsUnreq;			// Things we're subscribed to by unrequestID

	IDMAP *clientPublishes;						// Things we've published by requestID

	IDMAP *clientCalls;							// Map of currently outstanding outgoing client calls

	IDMAP *clientRegistrationsReq;				// Map of registered client invokees by requestId
	IDMAP *clientRegistrationsReg;				// Map of registered client invokees by registrationId
	IDMAP *clientRegistrationsUnreq;			// Map of registered client invokees by unrequestId

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


#define WAMP_INVOKE_SINGLE		1
#define WAMP_INVOKE_ROUNDROBIN	2
#define WAMP_INVOKE_RANDOM		3
#define WAMP_INVOKE_FIRST		4
#define WAMP_INVOKE_LAST		5

void wamp_Logger(void (*logger)(const char *, va_list));
// Sets a logging function to receive all debug output from the WAMP library

void (*wamp_GetLogger())(const char *, va_list);
const char *wamp_Name(WAMP *w);
// Returns the name of a WAMP.  This can later be used in wamp_ByName().
// If you want to refer to a WAMP later, using a name is safer as WAMPs can be deleted...
// WAMP *savedWamp = myWamp; ...... time passes .......  wamp_SendJson(savedWamp, ...) can fail if it's been deleted in the middle
// const char *savedWamp = wamp_Name(myWamp); .....  WAMP *wamp = wamp_ByName(savedWamp); if (wamp) wamp_SendJson(... is safe.

const char *wamp_TypeName(int type);
// Given a WAMP message type number, returns the name

const char *wamp_MatchName(int type);
// Returns the textual representation of WAMP_EXACT, WAMP_PREFIX or WAMP_WILDCARD

const char *wamp_PolicyName(int type);
// Returns the textual representation of WAMP_EXACT, WAMP_PREFIX or WAMP_WILDCARD

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
void wamp_SendJson(WAMP *wamp, JSON *json);
void wamp_SendJsonHeap(WAMP *wamp, JSON *json);
JSON *wamp_NewMessage(int type, long long id);
// Returns a new JSON array with the type filled in and the id if it's non-0
// Passing id as -1, uses a random ID.

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

void wamp_SendInternalError(WAMP *wamp, int type, long long id, const char *uri, const char *mesg);
// Uses the uri and message to send an error
// mesg MUST be on the heap and becomes owned by this function

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
long long wamp_PublishIn(WAMP *wamp, const char *topic, JSON *opts, JSON *list, JSON *dict);
// Receive a publish message as if it been received from a publisher
// The opts, list and dict become owned by the function.

long long wamp_Publish(WAMP *wamp, const char *topic, JSON *opts, JSON *list, JSON *dict, WAMPCB_Published cbPublished);
// Publish the event through the broker
// If cbPublished is given, it is called once the published message returns (also sets 'acknowledge:true' in options)
// The opts, list and dict become owned by the function.

long long wamp_Subscribe(WAMP *wamp, int type, const char *topic, JSON *opts, WAMPCB_Subscribed cbSubscribed, WAMPCB_Event cbEvent);
// Subscribes to a topic
// type			WAMP_EXACT, WAMP_PREFIX or WAMP_WILDCARD (0 defaults to WAMP_EXACT)
// topic		The topic or pattern to which to subscribe
// opts			Options regarding the subscription (the "match" element will be filled if type is specified)
// cbSubscribed	Called when the 'SUBSCRIBED' (or error) message is returned from the broker
// cbEvent		Called whenever this topic is published
// Returns the requestId (can be matched up in cbSubscribed)

long long wamp_Unsubscribe(WAMP *wamp, long long subscriptionId, WAMPCB_Unsubscribed cbUnsubscribed);
// Unsubscribe from a topic given either the subscriptionId or requestId.
// Returns 0 if we were not subscribed on this subscription ID or are already unsubscribing
// subcriptionId	Either the subscriptionId or the original requestId
// cbUnsubscribed	Called when the UNSUBSCRIBED (or error) is returned from the broker
// Returns the requestId or 0 if we're not subscribed or there is a pending unsubscription
// NB. cbUnsubscribed will only be called if we DO NOT return a 0.

int wamp_Call(WAMP *wamp, const char *procedure, JSON *options, JSON *list, JSON *dict, WAMPCB_Result cbResult);
// Calls a procedure, with the result being returned via the callback procedure given.
// If no result is expected (or wanted), cbResult can be NULL.

long long wamp_Register(WAMP *wamp, int policy, int match, int priority, const char *procedure, WAMPCB_Registered cbRegistered, WAMPCB_Invoke cbInvoke);
long long wamp_Unregister(WAMP *wamp, long long registrationId, WAMPCB_Unregistered cbUnregistered);
// Unregister a callee given either the subscriptionId or the requstId.
// Returns 0 if we were not registered on this registration ID or are already unregistering

void wamp_Hello(WAMP *wamp, const char *realm, JSON *caller, JSON *callee, JSON *publisher, JSON *subscriber, WAMPCB_Welcome cbWelcome);
// Sends a WAMP_HELLO message for the realm given.
// opts can be NULL or json_NewObject() to have no options, otherwise should be a JSON_OBJECT
// Each of caller, callee, publisher and subscriber can be one of the following:
// NULL				- We are not offering this role
// json_NewObject()	- We are offering the role, but adding no options
// JSON*			- The options we wish to use for this role

JSON *wamp_ResultStringz(const char *string);
int wamp_Result(WAMP *wamp, long long requestId, JSON *options, JSON *list, JSON *dict);
int wamp_Dispatch(WAMP *wamp, int type, JSON *json);
// Does whatever needs to be done to deal with a message of type 'type'
// The JSON* provided is definitely an array with at least one element.
// Returns 0 or an error code

#endif
