#if 0
./makeh $0
exit 0
#endif

extern void Log(const char *, ...);

#include "mtwamp.h"

#include <string.h>

#include <mtmacro.h>
#include <heapstrings.h>

#define STATIC static
#define API

#define IDMAP	SPMAP

#if 0
// START HEADER

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

// END HEADER
#endif

typedef struct wamp_call_t {								// Holds information about an actice call
	const char *callerWampName;								// The name of the calling wamp
	long long callerRequestId;
} wamp_call_t;

typedef struct wamp_callee_t {
	WAMP *wamp;												// WAMP handling this call
	long long registrationId;								// Id under which callee registered
} wamp_callee_t;

STATIC SPMAP *allWampChannels = NULL;						// All active channels
STATIC IDMAP *callMap = NULL;								// Calls (wamp_call_t) that have been invoked but not yet yielded

void wamp_NoInvokee(WAMP*, long long, const char*, JSON*, JSON*);
STATIC WAMPCB_NoCallee		wampcb_NoCallee = NULL;			// Callback function if there is no callee
STATIC WAMPCB_Invokee		wampcb_Invokee = wamp_NoInvokee;		// Callback function when a procedure is invoked

STATIC struct wamp_code_t {
	int code;
	const char *name;
} wamp_codes[] = {
	{WAMP_HELLO,			"Hello"},
	{WAMP_WELCOME,			"Welcome"},
	{WAMP_ABORT,			"Abort"},
	{WAMP_GOODBYE,			"Goodbye"},

	{WAMP_ERROR,			"Error"},

	{WAMP_PUBLISH,			"Publish"},
	{WAMP_PUBLISHED,		"Published"},

	{WAMP_SUBSCRIBE,		"Subscribe"},
	{WAMP_SUBSCRIBED,		"Subscribed"},
	{WAMP_UNSUBSCRIBE,		"Unsubscribe"},
	{WAMP_UNSUBSCRIBED,		"Unsubscribed"},
	{WAMP_EVENT,			"Event"},

	{WAMP_CALL,				"Call"},
	{WAMP_RESULT,			"Result"},

	{WAMP_REGISTER,			"Register"},
	{WAMP_REGISTERED,		"Registered"},
	{WAMP_UNREGISTER,		"Unregister"},
	{WAMP_UNREGISTERED,		"Unregistered"},
	{WAMP_INVOCATION,		"Invocation"},
	{WAMP_YIELD,			"Yield"},

	{0,NULL}
};

// IDMAP - maps long long to pointer

IDMAP *idmap_New()												{ return (IDMAP*)spmap_New(); }
void idmap_Delete(IDMAP *im)									{ spmap_Delete((SPMAP*)im); }
void idmap_Reset(IDMAP *im)										{ spmap_Reset((SPMAP*)im); }
void idmap_Clear(IDMAP *im)										{ spmap_Clear((SPMAP*)im); }
int idmap_Count(IDMAP *im)										{ return spmap_Count((SPMAP*)im); }
void idmap_Sort(IDMAP *im, int (*sorter)(const char *, const char *)) { spmap_Sort((SPMAP*)im, sorter); }
void idmap_SortValues(IDMAP *im, int (*sorter)(const char *, const char *)) { spmap_SortValues((SPMAP*)im, sorter); }
void *idmap_GetValueAtIndex(IDMAP *im, int index)                 { return spmap_GetValueAtIndex((SPMAP*)im, index); }
int idmap_GetNextEntry(IDMAP *im, const char **pKey, void **pValue) { return spmap_GetNextEntry((SPMAP*)im, pKey, pValue); }

int idmap_DeleteKey(IDMAP *im, long long id)
{
	char key[18];

	sprintf(key, "%Lx", id);
	return spmap_DeleteKey((SPMAP*)im, key);
}

long long idmap_GetKeyAtIndex(IDMAP *im, int index)
{
	long long result = 0;
	const char *key = spmap_GetKeyAtIndex((SPMAP*)im, index);

	if (key) sscanf(key, "%Lx", &result);

	return result;
}

int idmap_Add(IDMAP *im, long long id, void *value)
{
	char key[18];

	sprintf(key, "%Lx", id);

	return spmap_Add((SPMAP*)im, key, value);
}

void *idmap_GetValue(IDMAP *im, long long id)
{
	char key[18];

	sprintf(key, "%Lx", id);

	return spmap_GetValue((SPMAP*)im, key);
}

long long idmap_GetKey(SIMAP *im, void *value)
{
	long long result = 0;
	const char *key = spmap_GetKey((SPMAP*)im, value);

	if (key) sscanf(key, "%Lx", &result);

	return result;
}

//////////////////////

const char *wamp_TypeName(int type)
{
	struct wamp_code_t *rover = wamp_codes;

	while (rover->code) {
		if (rover->code == type)
			return rover->name;
		rover++;
	}

	return "Unknown wamp message type";
}

API long long wamp_RandomId()
// Returns a random number 1..2^53-1
{
	static int seeded = 0;

	if (!seeded) {
		seeded = 1;
		srand48(time(NULL));
	}

	long long result = 0;
	while (result == 0) {					// Dangerous but if it doesn't exit, the random number generator has gone weird
		long long a = lrand48();			// Unsigned
		long long b = lrand48();			// Unsigned
		result = a ^ (b<<5);
	}

	return result;
}

API void wamp_Delete(WAMP *w)
{
	if (w) {
		if (w->channel)
			spmap_DeleteKey(allWampChannels, w->channel->id);

		free((char*)w);
	}
}

STATIC WAMP *wamp_New()
{
	WAMP *w = NEW(WAMP, 1);

	w->channel = NULL;
	w->session = NULL;
	w->realm = NULL;

	return w;
}

API WAMP *wamp_GetForChannel(WS *channel)
// Returns the wamp structure associated with a websocket
// If there isn't one already, creates one.
// One is always returned (unless channel is NULL)
{
	WAMP *w = NULL;

	if (channel) {
		if (!allWampChannels) allWampChannels = spmap_New();

		const char *id = channel->id;
		w = (WAMP*)spmap_GetValue(allWampChannels, id);
		if (!w) {
			w = wamp_New();

			spmap_Add(allWampChannels, id, (void*)w);
			w->channel = channel;
		}
	}

	return w;
}

API const char *wamp_Name(WAMP *w)
{
	const char *name = NULL;

	if (w && w->channel) {
		name = w->channel->id;
	}

	return name;
}

STATIC WAMP *wamp_FindByName(const char *name)
{
	WAMP *w = NULL;

	if (name)
		w = (WAMP*)spmap_GetValue(allWampChannels, name);

	return w;
}

API int wamp_Write(WAMP *wamp, const char *message)
// Write a string to this WAMP's output channel
// Returns the number of bytes sent
{
	int result = 0;

	if (wamp && message) {
		int fragLen;
		const char *frag = ws_MakeFragment(1, 1, 0, -1, message, &fragLen);

Log("WAMP OUT(%s, %s)", wamp->channel->id, message);
		result = ws_WriteHeap(wamp->channel, fragLen, frag);
	}

	return result;
}

API int wamp_WriteHeap(WAMP *wamp, const char *message)
// Exactly like wamp_Write() but deletes the message afterwards
{
	int result = wamp_Write(wamp, message);
	szDelete(message);

	return result;
}

API void wamp_WriteJson(WAMP *wamp, JSON *json)
{
	const char *message = json_Render(json);
	if (message) {
//Log("wamp_WriteJson(%p, %s)", wamp, message);
		wamp_WriteHeap(wamp, message);
	} else {
		//Log("Duff JSON passed to wamp_SendJson");
	}
}

API void wamp_WriteJsonHeap(WAMP *wamp, JSON *json)
{
	wamp_WriteJson(wamp, json);
	json_Delete(json);
}

API void wamp_CloseAbort(WAMP *wamp, int type, const char *uri, const char *reason)
// Close and Abort formats are identical apart from the type
{
	JSON *j = json_NewArray();
	json_ArrayAddInteger(j, type);
	JSON *jreason = json_ArrayAdd(j, json_NewObject());
	if (reason)
		json_ObjectAddString(jreason, "reason", -1, reason);
	json_ArrayAddString(j, -1, uri);

	wamp_WriteJsonHeap(wamp, j);
}

API void wamp_Abort(WAMP *wamp, const char *uri, const char *reason)
{
	wamp_CloseAbort(wamp, WAMP_ABORT, uri, reason);
}

API void wamp_Close(WAMP *wamp, const char *uri, const char *reason)
// Sends a WAMP close message with the given reason
{
	wamp_CloseAbort(wamp, WAMP_GOODBYE, uri, reason);
}

STATIC SPMAP *realm_Map()
{
	static SPMAP *realm_map = NULL;

	if (!realm_map) {
		realm_map = spmap_New();

		WAMPREALM *r = NEW(WAMPREALM, 1);
		r->name = strdup("spider");
		r->calleeMapExact = NULL;
		r->calleeMapPrefix = NULL;
		r->calleeMapWildcard = NULL;
		spmap_Add(realm_map, "spider", r);
	}

	return realm_map;
}

API int wamp_SetRealm(WAMP *w, const char *name)
{
	WAMPREALM *realm = (WAMPREALM*)spmap_GetValue(realm_Map(), name);
	if (realm) w->realm = realm;

	return !!realm;
}

STATIC WAMPREALM *wamp_GetRealmByName(const char *name)
{
	return (WAMPREALM*)spmap_GetValue(realm_Map(), name);
}

STATIC WAMPSESSION *wamp_NewSession(WAMPREALM *realm, long long id)
{
	WAMPSESSION *s = NEW(WAMPSESSION, 1);

	if (!id) id = wamp_RandomId();
	s->id = id;
	s->realm = realm;

	return s;
}

STATIC long long session_Id(WAMPSESSION *session)
{
	return session ? session->id : 0;
}

STATIC WAMPSESSION *wamp_GetSession(long long id)
{
	return NULL;
}

// Connection provided an incorrect URI for any URI-based attribute of WAMP message,
// such as realm, topic or procedure.
STATIC const char *ErrInvalidUri = "wamp.error.invalid_uri";

// A Dealer could not perform a call, since no procedure is currently
// registered under the given URI.
STATIC const char *ErrNoSuchDomain = "wamp.error.no_such_procedure";

// A procedure could not be registered, since a procedure with the given URI
// is already registered.
STATIC const char *ErrDomainAlreadyExists = "wamp.error.procedure_already_exists";

// A Dealer could not perform an unregister, since the given registration is
// not active.
STATIC const char *ErrNoSuchRegistration = "wamp.error.no_such_registration";

// A Broker could not perform an unsubscribe, since the given subscription is
// not active.
STATIC const char *ErrNoSuchSubscription = "wamp.error.no_such_subscription";

// A call failed, since the given argument types or values are not acceptable
// to the called procedure - in which case the Callee may throw this error. Or
// a Node performing payload validation checked the payload (args / kwargs)
// of a call, call result, call error or publish, and the payload did not
// conform - in which case the Node may throw this error.
STATIC const char *ErrInvalidArgument = "wamp.error.invalid_argument";

// The Connection is shutting down completely - used as a GOODBYE (or aBORT) reason.
STATIC const char *ErrSystemShutdown = "wamp.error.system_shutdown";

// The Connection wants to leave the realm - used as a GOODBYE reason.
STATIC const char *ErrCloseRealm = "wamp.error.close_realm";

// A Connection acknowledges ending of a session - used as a GOOBYE reply reason.
STATIC const char *ErrGoodbyeAndOut = "wamp.error.goodbye_and_out";

// A join, call, register, publish or subscribe failed, since the Connection is not
// authorized to perform the operation.
STATIC const char *ErrNotAuthorized = "wamp.error.not_authorized";

// A Dealer or Broker could not determine if the Connection is authorized to perform
// a join, call, register, publish or subscribe, since the authorization
// operation itself failed. E.g. a custom authorizer ran into an error.
STATIC const char *ErrAuthorizationFailed = "wamp.error.authorization_failed";

// Connection wanted to join a non-existing realm (and the Node did not allow to
// auto-create the realm)
STATIC const char *ErrNoSuchRealm = "wamp.error.no_such_realm";

// A Connection was to be authenticated under a Role that does not (or no longer)
// exists on the Node. For example, the Connection was successfully authenticated,
// but the Role configured does not exists - hence there is some
// misconfiguration in the Node.
STATIC const char *ErrNoSuchRole = "wamp.error.no_such_role";

STATIC int wamp_Validate(WAMP *wamp, int type, JSON *json)
// Checks the validity of the message, sending an error back if there is a problem
// Returns	0		Ok
//			1...	Problem
{
	int result = 0;

	typedef struct {
		int type;
		int min;
		int max;
		const char *signature;				// I=int, F=Float, N=number, B=bool, O=object, U=Uri, A=Array, S=String
	} valid_t;

	static valid_t valids[] = {
	//	Message type			min	max	Types
		{WAMP_HELLO,			3,	3,	"ISO"},
		{WAMP_WELCOME,			3,	3,	"IIO"},
		{WAMP_ABORT,			3,	3,	"IOU"},
		{WAMP_GOODBYE,			3,	3,	"IOU"},
		{WAMP_ERROR,			5,	7,	"IIIOUAO"},
		{WAMP_PUBLISH,			4,	6,	"IIOUAO"},
		{WAMP_PUBLISHED,		3,	3,	"III"},
		{WAMP_SUBSCRIBE,		4,	4,	"IIOU"},
		{WAMP_SUBSCRIBED,		3,	3,	"III"},
		{WAMP_UNSUBSCRIBE,		3,	3,	"III"},
		{WAMP_UNSUBSCRIBED,		2,	2,	"II"},
		{WAMP_EVENT,			4,	6,	"IIIOAO"},
		{WAMP_REGISTER,			4,	4,	"IIOU"},
		{WAMP_REGISTERED,		3,	3,	"III"},
		{WAMP_UNREGISTER,		3,	3,	"III"},
		{WAMP_UNREGISTERED,		2,	2,	"II"},

		{WAMP_CALL,				4,	6,	"IIOUAO"},
		{WAMP_INVOCATION,		4,	6,	"IIIOAO"},
		{WAMP_YIELD,			3,	5,	"IIOAO"},
		{WAMP_RESULT,			3,	5,	"IIUAO"},

		{0,0,0,0}
	};

	valid_t *v = valids;
	while (v->type) {
		if (v->type == type) {
			int argc = json_ArrayCount(json);

			if (argc < v->min || argc > v->max) {
				const char *msg;

				if (v->min == v->max) {
					msg = hprintf(NULL, "A %s message must have %d elements", wamp_TypeName(type), v->min);
				} else {
					msg = hprintf(NULL, "A %s message must have %d-%d elements", wamp_TypeName(type), v->min, v->max);
				}
				wamp_Abort(wamp, "wamp.error.invalid_argument", msg);
				szDelete(msg);
				result = 1;
				break;
			}

			int i=0;
			const char *expected = NULL;
			for (i=0;i<argc;i++) {
				char check = v->signature[i];
				char actual = json_Type(json_ArrayAt(json, i));

				if (check == 'I' && actual != JSON_INTEGER) {
					expected = "INTEGER";
				} else if (check == 'N' && actual != JSON_FLOAT && actual != JSON_INTEGER) {
					expected = "NUMBER";
				} else if (check == 'B' && actual != JSON_BOOL) {
					expected = "true or false";
				} else if (check == 'O' && actual != JSON_OBJECT) {
					expected = "object";
				} else if (check == 'A' && actual != JSON_ARRAY) {
					expected = "array";
				} else if (check == 'U' && actual != JSON_STRING) {
					expected = "URI";
				} else if (check == 'S' && actual != JSON_STRING) {
					expected = "string";
				}

				if (expected) {
					const char *found = "something else";
					if (actual == JSON_INTEGER)	found = "integer";
					else if (actual == JSON_FLOAT) found = "float";
					else if (actual == JSON_BOOL) found = "boolean";
					else if (actual == JSON_NULL) found = "null";
					else if (actual == JSON_OBJECT) found = "object";
					else if (actual == JSON_ARRAY) found = "array";
					else if (actual == JSON_STRING) found = "string";

					const char *msg = hprintf(NULL, "Message type %d (%s), element %d should be %s not %s", type, wamp_TypeName(type), i+1, expected, found);
					wamp_Abort(wamp, "wamp.error.invalid_argument", msg);
					szDelete(msg);
					result = 2;
					break;
				}
			}

			break;
		}
		v++;
	}

	return result;
}

API void wamp_CallbackNoCallee(WAMPCB_NoCallee cb)
{
	wampcb_NoCallee = cb;
}

API void wamp_RegisterInvokee(WAMPCB_Invokee cb)
{
	wampcb_Invokee = cb ? cb : wamp_NoInvokee;
}

STATIC int wamp_WildcardMatch(const char *wildcard, const char *match)
// Taking a wildcard such as api..thing and a match such as api.specific.thing, returns 1 for a match or 0 for not
{
	return 0;
}

STATIC int wamp_AddCaller(long long invokeRequestId, WAMP *wamp, long long callRequestId)
// Adds a caller to those currently known.
// Returns	0	Ok
//			1	requestId was already known
{
	if (!callMap)
		callMap = idmap_New();

	wamp_call_t *caller = idmap_GetValue(callMap, invokeRequestId);
Log("wamp_AddCaller(%Ld, %p (%s), %Ld)", invokeRequestId, wamp,wamp_Name(wamp), callRequestId);

	if (caller)
		return 1;

	caller = NEW(wamp_call_t, 1);
	caller->callerWampName = strdup(wamp_Name(wamp));
	caller->callerRequestId = callRequestId;

Log("idmap_Add(%p, %Ld, %p)", callMap, invokeRequestId, caller);
	idmap_Add(callMap, invokeRequestId, caller);
wamp_call_t *testcaller = idmap_GetValue(callMap, invokeRequestId);
Log("AAA Caller %p = idmap_GetValue(%p, %Ld)", testcaller, callMap, invokeRequestId);

	return 0;
}

STATIC const char *wamp_FindAndRemoveCaller(long long requestId, long long *pcallerRequestId)
// Returns a string on the heap
{
	const char *result = NULL;

Log("wamp_FindAndRemoveCaller(%Ld) - callMap = %p", requestId, callMap);
	if (callMap) {
		wamp_call_t *caller = idmap_GetValue(callMap, requestId);
Log("BBB Caller %p = idmap_GetValue(%p, %Ld)", caller, callMap, requestId);

Log("Found it as %p", caller);
		if (caller) {
			idmap_DeleteKey(callMap, requestId);
			result = caller->callerWampName;
			if (pcallerRequestId) *pcallerRequestId = caller->callerRequestId;
			free((char*)caller);
		}
	}

Log("Returning '%s'", result);
	return result;
}

STATIC wamp_callee_t *wamp_FindCallee(WAMP *wamp, const char *procedure)
{
Log("wamp_FindCallee(%p, '%s')", wamp, procedure);
	wamp_callee_t *result = NULL;
	WAMPREALM *realm = wamp->realm;

	if (realm->calleeMapExact) {												// Check for an exact match
		result = (wamp_callee_t*)spmap_GetValue(realm->calleeMapExact, procedure);
	}

	if (!result && realm->calleeMapPrefix) {									// Not found, Check for a prefix match
		const char *name;
		void *value;

		spmap_Reset(realm->calleeMapPrefix);
		while(spmap_GetNextEntry(realm->calleeMapPrefix, &name, &value)) {
			if (!strncmp(procedure, name, strlen(name))) {
				result = (wamp_callee_t*)value;
				break;
			}
		}
	}

	if (!result && realm->calleeMapWildcard) {									// Not found, Check for a wildcard match
		const char *name;
		void *value;

		spmap_Reset(realm->calleeMapWildcard);
		while(spmap_GetNextEntry(realm->calleeMapWildcard, &name, &value)) {
			if (wamp_WildcardMatch(procedure, name)) {
				result = (wamp_callee_t*)value;
				break;
			}
		}
	}

	if (!result && wampcb_NoCallee) {
		int whatToDo = (*wampcb_NoCallee)(procedure);							// Give the callback chance to register one

		if (whatToDo) {															// It says it might work if you try again
			WAMPCB_NoCallee temp = wampcb_NoCallee;								// Stop us repeatedly calling
			wampcb_NoCallee = NULL;

			result = wamp_FindCallee(wamp, procedure);							// Try again

			wampcb_NoCallee = temp;
		}
	}

	return result;
}

STATIC void wamp_NoInvokee(WAMP *wamp, long long requestId, const char *procedure, JSON *list, JSON *dict)
{
	JSON *invErr = json_NewArray();
	json_ArrayAddInteger(invErr, WAMP_ERROR);
	json_ArrayAddInteger(invErr, WAMP_INVOCATION);
	json_ArrayAddInteger(invErr, requestId);
	JSON *err = json_NewArray();
	json_ArrayAddString(err, -1, "callee is unable to handle anything");
	json_ArrayAdd(invErr, err);

	wamp_WriteJsonHeap(wamp, invErr);
}

API int wamp_Hello(WAMP *wamp, const char *realm, JSON *opts)
// Does the work of 'helloing' a wamp connection, which is useful if we want to create an already registered
// channel.  E.g. Connecting a legacy API handler.
// Returns	0	Everything went swimmingly
//			1	Was already in a realm
//			2	Realm didn't accept the connection (no such realm)
//			3	Was already in a session
{
	if (wamp->realm) return 1;
	wamp->realm = wamp_GetRealmByName(realm);
	if (!wamp->realm) return 2;

	if (wamp->session) return 3;

	wamp->session = wamp_NewSession(wamp->realm, 0);
	long long id = session_Id(wamp->session);
}

API int wamp_Dispatch(WAMP *wamp, int type, JSON *json)
// Does whatever needs to be done to deal with a message of type 'type'
// The JSON* provided is definitely an array with at least one element.
// Returns 0 or an error code
{
{const char *text = json_Render(json); Log("wamp_Dispatch(%p, %d, %s)", wamp, type, text); szDelete(text);}

	int err = wamp_Validate(wamp, type, json);
	if (err) return err;

	int argc = json_ArrayCount(json);
	// We now know that we have a good number of arguments and that their types match what we expect

	if (type != WAMP_HELLO && type != WAMP_WELCOME && !wamp->session) {			// Trying to say something other than hello and we've not shaken hands yet
		wamp_Abort(wamp, "wamp.error.invalid_argument", "You have not been welcomed");
		return 1;
	}

	switch (type) {
	case WAMP_HELLO:
	{
		const char *realm = json_ArrayStringAt(json, 1);
		JSON *opts = json_ArrayAt(json, 2);

		int err = wamp_Hello(wamp, realm, opts);
		switch (err) {
		case 1:					// Already in a realm (these two should always be the same)
		case 3:					// Already in a session
			wamp_Abort(wamp, "wamp.error.invalid_argument", "WAMP HELLO received within a session");
			return 1;
		case 2:
			wamp_Abort(wamp, "wamp.error.no_such_realm", "I can only accept \"spider\" currently");
			return 2;
		}

		long long id = session_Id(wamp->session);

		JSON *json = json_NewArray();
		json_ArrayAddInteger(json, WAMP_WELCOME);
		json_ArrayAddInteger(json, id);
		JSON *replyOpts = json_NewObject();
		json_ObjectAddString(replyOpts, "agent", -1, "SPIDER");
		json_ObjectAddString(replyOpts, "roles", -1, "broker");
		json_ArrayAdd(json, replyOpts);

		wamp_WriteJsonHeap(wamp, json);
		return 0;
	}
	case WAMP_WELCOME:
		wamp->session = wamp_NewSession(wamp->realm, json_ArrayIntegerAt(json, 1));
		return 0;

	case WAMP_GOODBYE:
	{
		const char *uri = json_ArrayStringAt(json, 2);

		if (strcmp(uri, ErrGoodbyeAndOut)) {					// Don't respond if they're saying 'goodbye and out'
			wamp_Close(wamp, ErrGoodbyeAndOut, NULL);
		}
		return 0;
	}

	case WAMP_REGISTER:
	{
		long long requestId = json_ArrayIntegerAt(json, 1);
		SPMAP *options = json_ArrayObjectAt(json, 2);
		const char *procedure = json_ArrayStringAt(json, 3);

		const char *match = "exact";
		JSON *jMatch = (JSON*)spmap_GetValue(options, "match");
		if (jMatch) {
			if (json_Type(jMatch) != JSON_STRING) {
				wamp_Close(wamp, "wamp.error.invalid_argument", "OPTION value must be a string");
			}
			match = json_AsString(jMatch);
		}

		WAMPREALM *realm = wamp->realm;
		SPMAP **pmap;
		if (!strcmp(match, "exact"))
			pmap = &realm->calleeMapExact;
		else if (!strcmp(match, "prefix"))
			pmap = &realm->calleeMapPrefix;
		else if (!strcmp(match, "wildcard"))
			pmap = &realm->calleeMapWildcard;
		else {
			wamp_Close(wamp, "wamp.error.invalid_argument", "match type must be exact, prefix or wildcard");
			return 1;
		}

		long long registrationId = wamp_RandomId();
		wamp_callee_t *callee = NEW(wamp_callee_t, 1);
		callee->registrationId = registrationId;
		callee->wamp = wamp;
		if (!*pmap)
			*pmap = spmap_New();
		spmap_Add(*pmap, procedure, (void*)callee);

		JSON *j = json_NewArray();
		json_ArrayAddInteger(j, WAMP_REGISTERED);
		json_ArrayAddInteger(j, requestId);
		json_ArrayAddInteger(j, registrationId);

		wamp_WriteJsonHeap(callee->wamp, j);
		return 1;
	}

	case WAMP_REGISTERED:
		return 1;

	case WAMP_CALL:
	{
		long long callRequestId = json_ArrayIntegerAt(json, 1);
		SPMAP *options = json_ArrayObjectAt(json, 2);
		const char *procedure = json_ArrayStringAt(json, 3);
		JSON *argumentList = json_ArrayTakeAt(json, 4);
		JSON *argumentDict = json_ArrayTakeAt(json, 4);

		wamp_callee_t *callee = wamp_FindCallee(wamp, procedure);
Log("Looked for callee for %s, found %p", procedure, callee);
		if (!callee) {
			wamp_Close(wamp, "wamp.error.no_such_procedure", NULL);
			return 1;
		}
		long long requestId = wamp_RandomId();

		wamp_AddCaller(requestId, wamp, callRequestId);

		JSON *j = json_NewArray();
		json_ArrayAddInteger(j, WAMP_INVOCATION);
		json_ArrayAddInteger(j, requestId);
		json_ArrayAddInteger(j, callee->registrationId);
		JSON *callDetails = json_NewObject();
		json_ObjectAddString(callDetails, "procedure", -1, procedure);
		json_ArrayAdd(j, callDetails);
		if (argumentList) json_ArrayAdd(j, argumentList);
		if (argumentDict) json_ArrayAdd(j, argumentDict);

		wamp_WriteJsonHeap(callee->wamp, j);
	}
		return 0;
	case WAMP_INVOCATION:
	{
		long long requestId = json_ArrayIntegerAt(json, 1);
		long long registrationId = json_ArrayIntegerAt(json, 2);
		JSON *details = json_ArrayAt(json, 3);
		JSON *argumentList = json_ArrayTakeAt(json, 4);
		JSON *argumentDict = json_ArrayTakeAt(json, 4);

		const char *procedure = NULL;
		JSON *jProcedure = json_ObjectValue(details, "procedure");
		if (jProcedure && json_Type(jProcedure) == JSON_STRING)
			procedure = json_AsString(jProcedure);

		(*wampcb_Invokee)(wamp, requestId, procedure, argumentList, argumentDict);
		return 0;
	}
	case WAMP_YIELD:
	{
		WAMP *callerWamp = NULL;

		long long requestId = json_ArrayIntegerAt(json, 1);
		long long callerRequestId;
		const char *callerName = wamp_FindAndRemoveCaller(requestId, &callerRequestId);
Log("Name of caller = '%s'", callerName);
		if (callerName) {									// Should always find one
			callerWamp = wamp_FindByName(callerName);	// May not find one if the wamp has 'gone away' since the call

Log("Calling wamp = %p", callerWamp);
			szDelete(callerName);
		}

		if (callerWamp) {									// The caller still exists
			JSON *details = json_ArrayTakeAt(json, 2);
			JSON *resultList = json_ArrayTakeAt(json, 2);
			JSON *resultDict = json_ArrayTakeAt(json, 2);

			JSON *result = json_NewArray();
			json_ArrayAddInteger(result, WAMP_RESULT);
			json_ArrayAddInteger(result, callerRequestId);
			json_ArrayAdd(result, details);
			if (resultList) json_ArrayAdd(result, resultList);
			if (resultDict) json_ArrayAdd(result, resultDict);

			wamp_WriteJsonHeap(callerWamp, result);
		}
		return 0;
	}
	default:
		{
			const char *msg = hprintf(NULL, "I can't handle a '%s' message (yet)", wamp_TypeName(type));
			wamp_Close(wamp, "wamp.error.invalid_argument", msg);
			szDelete(msg);
		}
	}

//	Log("Dispatching WAMP(%d)", type);
	return 0;
}
