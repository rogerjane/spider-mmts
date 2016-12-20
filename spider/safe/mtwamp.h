#ifndef __MTWAMP_H
#define __MTWAMP_H


#include "smap.h"

#include "mtwebsocket.h"
#include "mtjson.h"

typedef struct WAMPREALM {
	const char *name;
	SPMAP *calleeMapExact;						// Map of known callees with exact call
	SPMAP *calleeMapPrefix;						// Map of known callees	with prefix call
	SPMAP *calleeMapWildcard;					// Map of known callees with wildcard call
} WAMPREALM;

typedef struct WAMPSESSION {
	long long id;
	WAMPREALM *realm;
} WAMPSESSION;

typedef struct WAMP {
	WS *channel;					// The websocket channel this WAMP lives on
	WAMPREALM *realm;
	WAMPSESSION *session;
} WAMP;

// Typedefs for WAMP callback functions
typedef int (*WAMPCB_NoCallee)(const char *name);
typedef void (*WAMPCB_Invokee)(WAMP *wamp, long long requestId, const char *procedure, JSON *list, JSON *dict);

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


long long wamp_RandomId();
void wamp_Delete(WAMP *w);
WAMP *wamp_GetForChannel(WS *channel);
const char *wamp_Name(WAMP *w);
int wamp_Write(WAMP *wamp, const char *message);
int wamp_WriteHeap(WAMP *wamp, const char *message);
void wamp_WriteJson(WAMP *wamp, JSON *json);
void wamp_WriteJsonHeap(WAMP *wamp, JSON *json);
void wamp_CloseAbort(WAMP *wamp, int type, const char *uri, const char *reason);
void wamp_Abort(WAMP *wamp, const char *uri, const char *reason);
void wamp_Close(WAMP *wamp, const char *uri, const char *reason);
int wamp_SetRealm(WAMP *w, const char *name);
void wamp_CallbackNoCallee(WAMPCB_NoCallee cb);
void wamp_RegisterInvokee(WAMPCB_Invokee cb);
int wamp_Hello(WAMP *wamp, const char *realm, JSON *opts);
int wamp_Dispatch(WAMP *wamp, int type, JSON *json);

#endif
