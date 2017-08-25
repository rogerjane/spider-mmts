#if 0
./makeh $0
exit 0
#endif

#include "mtwamp.h"

#include <limits.h>
#include <regex.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include <mtmacro.h>
#include <heapstrings.h>
#include <mtstrings.h>

#define STATIC static
#define API

#define WAMP_CONN_WEBSOCKET		1
#define WAMP_CONN_RAWSOCKET		2
#define WAMP_CONN_CODE			3

// TODO:
// * Call timeout: (page 67)
//		{"timeout":123}		-- In milliseconds
//	To implement, need to have a queue of timed events added to the event loop and handle it from there...
//	Each timed event needs to carry a structure with it - this can be just a heap pointer.
//
// * Event history: (page 102)
//		wamp.topic.history.last(subscriptionId, count)
//		wamp.topic.history.since(subscriptionId, timestamp)
//		wamp.topic.history.after(subscriptionId, publicationId)
// The crossbar implementation only holds events in memory according to specific topics and the above functions return
// an array of event objects.
//
// I consider a better implementation is to make the history persistent across restarts with the topics defined as either
// the exact/prefix/wildcard method or as a regex, optionally across restarts and have the function simply return the
// number of events with the events being sent afterwards as if they were being generated afresh.  In that way, the client
// can handle the events in the same way as it usually does.
//
// A slight hiccup would be this problem...
// Subscribe to a topic
// Ask for events on this topic since timestamp
//   The problem is that if an event fires before asking for the history, that one would be out of order.
//   Need to subscribe to a topic with a 'wait for history' flag so that no events are fired until history has been sent.
//   [SUBSCRIBE, 1000,{"wait_for_history":true,"match":"prefix"},"appointment."] => [SUBSCRIBED,1000,5678]
//   [CALL, 1001, "wamp.topic.history.since",[],{"subscription":5678,"timestamp":"2017-01-03T14:04:00.000","delivery":"events"}]
// No events would be delivered between the subscribe and the call, but once the call has delivered everything they'd be enabled

#if 0
// START HEADER

#include <stdarg.h>
#include "smap.h"

#include "mtwebsocket.h"
#include "mtjson.h"

#include <string>
#include <vector>
#include <map>
#include <set>

typedef std::vector<JSON *> testamentBucket;							// A vector of testaments
typedef std::map<std::string, testamentBucket> testamentCollection;		// Buckets of testaments

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
typedef void (*WAMPCB_Event)(struct WAMP *wamp, long long pubId, long long subId, const char *topic, JSON *details, JSON *list, JSON *dict);
typedef void (*WAMPCB_Unsubscribed)(struct WAMP *wamp, long long reqId, long long subId, JSON *details, const char *uri);

typedef void (*WAMPCB_Registered)(struct WAMP *wamp, long long reqId, long long regId, JSON *details, const char *uri);
typedef void (*WAMPCB_Result)(struct WAMP *wamp, long long reqId, const char *procedure, const char *uri, JSON *details, JSON *list, JSON *dict, void *data);
typedef JSON *(*WAMPCB_Invoke)(struct WAMP *wamp, long long reqId, long long regId, const char *procedure, JSON *details, JSON *list, JSON *dict, void *data);
typedef void (*WAMPCB_Unregistered)(struct WAMP *wamp, long long reqId, long long regId, JSON *details, const char *uri);
typedef void (*WAMPCB_Welcome)(struct WAMP *wamp, long long sessionId, const char *uri, JSON *details);
typedef int (*WAMPCB_Modifier)(WAMP *wamp, const char **pname, JSON **details, JSON **list, JSON **dict);

typedef void (*WAMPCB_Idler)(void *data);

typedef void WAMPIDLER;				// An opaque structure (really a CHANIDLER)

typedef void (*WAMP_Logger)(const char *fmt, va_list);

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
	long long sessionId;
	bool authenticated;				// true once we're willing to talk beyond 'HELLO, AUTHENTICATE'

	const char *peer;						// ID of peer (e.g. IP address and port)

	const char *authId;
	const char *authRole;
	const char *authMethod;
	const char *authProvider;

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

	testamentCollection *testaments;			// May contain either or both of 'detached', 'destroyed'

} WAMP;

#define WAMP_HELLO			1
#define WAMP_WELCOME		2
#define WAMP_ABORT			3
#define WAMP_CHALLENGE		4
#define WAMP_AUTHENTICATE	5
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
#define WAMP_CANCEL			49
#define WAMP_RESULT			50

#define WAMP_REGISTER		64
#define WAMP_REGISTERED		65
#define WAMP_UNREGISTER		66
#define WAMP_UNREGISTERED	67
#define WAMP_INVOCATION		68
#define WAMP_INTERRUPT		69
#define WAMP_YIELD			70

// END HEADER
#endif

// Start at 1, WAMP_INVOKE_COUNT must be right
#if 0
// START HEADER
#define WAMP_INVOKE_SINGLE		1
#define WAMP_INVOKE_ROUNDROBIN	2
#define WAMP_INVOKE_RANDOM		3
#define WAMP_INVOKE_FIRST		4
#define WAMP_INVOKE_LAST		5
// END HEADER
#endif

#define WAMP_INVOKE_COUNT		5

// Start at 1, WAMP_MATCH_COUNT must be right
#if 0
// START HEADER
#define WAMP_MATCH_EXACT		1
#define WAMP_MATCH_PREFIX	2
#define WAMP_MATCH_WILDCARD	3
#define WAMP_MATCH_REGEX		4
// END HEADER
#endif

#define WAMP_MATCH_COUNT		4

typedef struct WAMPREALM {
	const char *name;
	SSET *wamps;								// Set of WAMPs connected to this realm

	IDMAP *allSessions;							// All active wamps by session

	SPMAP *calleeMapExact;						// Map of known callees with exact call
	SPMAP *calleeMapPrefix;						// Map of known callees	with prefix call
	SPMAP *calleeMapWildcard;					// Map of known callees with wildcard call
	SPMAP *calleeMapRegex;						// Map of known callees with regex call
	IDMAP *redirect;							// Re-direction map (see. wamp_RegisterRedirect())

	SPMAP *subMapExact;							// Three maps to handle subsscriptions
	SPMAP *subMapPrefix;
	SPMAP *subMapWildcard;
	SPMAP *subMapRegex;
	IDMAP *allSubscriptions;					// All subscriptions by subscription ID

// wantForwardSubscriptions = 0									We are handling subscriptions
// wantForwardSubscriptions = 1, forwardSubscriptions != NULL	We can't handle them, but we can forward them
// wantForwardSubscriptions = 1, forwardSubscriptions = NULL	We can't handle them at all
	int wantForwardSubscriptions;				// 1 if we want to forward subscriptions
	const char *forwardSubscriptions;			// WAMP to which to forward subscriptions or NULL not to forward
	SSMAP *fwdSubscriptionMap;					// Records who is subscribing to messages
	SSMAP *fwdUnsubscriptionMap;				// Records who is unsubscribing to messages
	SSMAP *fwdEventMap;							// Records who wants to know about events
	SSMAP *fwdPublishMap;						// Records who has published
} WAMPREALM;

/* WAMP ROUTING


# Do we want / need to be able to advertise a callee and address it from anywhere?
# E.g. snomed.register might be provided on a specific server, but we don't want the caller to need to know that.

# Would we, therefore, want a list of 'advertised' callees on each node, which gets distributed when a node sends
# a discovery message?  Also, presumably, if a node sees one of these come or go.
# Don't want to have a specific 'global' realm for this as callers register with only one.

	For this to work, each node is going to have to know which neighbours it has and will need a unique name.

	Message format is:

		[MTPOST, messageID|id, destination|String, source|String, visited|array, details|Dict, [normal message content]]

		MTPOST		- Our own message type for routing

		messageId		- Unique message ID generated by sender
		destination		- Unique ID of the destination node or "*" for a broadcast
		source			- The original sender of the message

		visited			- Array of nodes this message has visited (unsure whether to include sender)

		details:
			Expiry		- Date				When this message can be forgotten about (default to +1h)
			PleaseAck	- bool				True if sender requires delivery acknowledgement
			Hops		- integer			How many hops this message has taken (only in discovery)
			Aliases		- Array of Strings	Aliases under which this node can be referred

		message			- normal message	Omitted for a discovery message

	On startup, node sends 'discovery' broadcast via every neighbour
		Contains no content

	On receiving any message:
		If not seen before (only needs to be since startup?):
			Add to 'seen' list (ID, expiry)
		else
			flag as 'duplicate'
		Process message

	On receiving a broadcast (destination = "*"):
		flag as 'broadcast'
		If not duplicate OR is discovery:
			Process message
		If not duplicate:			(Needs to be last as a response might need the above processing to be done to work properly)
			Send to all neighbours not on visited list

	Processing 'discovery':
		Record in route table (sender, hops, neighbour)
		If it was a broadcast:
			Send 'discovery' message direct to sender
		If it was direct:
			Forward as a broadcast to other neighbours

	Processing anything else:
		if duplicate, forget it (mentioned above).

	On receiving any direct message to me:
		If I am sender, process as 'failed to deliver'
		If 'PleaseAck' send 'ack' to sender
		Process the message

	On receiving any direct message to another (X):
		If it is discovery, increment 'details.hops'
		Forward on to a neighbour as for 'On sending to X' below

	On originating a message to X:
		Set sender to self
		Send to X as below:

	On Sending to another (X):
		Add self to 'visited' array
		Find 'X' in route table
		Send to matching neighbour with smallest hop count, not visited

		If there is no matching neighbour, not visited:
			If forwarding (came from neighbour)
				Send back to previous neighbour
			else
				Send to self

		If cannot deliver to neighbour:
			Set its hop count to MAX
			Try next in table

	Failed to deliver:
		This means the message has failed to reach the recipient.

Questions:
	Q: If we receive a message with no visited and the sender is not on our neighbours table, do we add it?
		A: Should never happen, but if already known and hop count in route table == MAX, then reset the hops in the route table?

	Q: How often should we send 'reminder' discovery messages
	Q: When should we 'ping' our neighbours?
		A: Depends on the reliability - let's see how it works out

	Q: Not sure of the use-case for 'PleaseAck' but I'm sure there is one

	Q: Can this be done using external connections to this wamp handling?
		A: I think so!

	Q: When receiving a discovery we may choose not to reply if we have no services to offer, or ignore it if we have no use for it.
		A: If we don't do this, every route table will have all known nodes.  Not sure if this is needed or no.

	Q: Where do we configure neighbours?
		A simple text file somewhere.  Distinct from spider.conf but needs to be one per node and there are several per server.

What achieves this?
	Call from outside WAMP to add handler for message type: 100 = MTPOST
		This should be done by adding an internal WAMP* and saying it should receive all MTPOST messages
	Call WAMP to make connection to each known neighbour
	Send the initial discovery message as above
	When we receive MTPOST, if it's a discovery then reply only if it's a broadcast.
	If it's not a broadcast, re-transmit it as a broadcast, keeping hops and visited intact.

*/

typedef struct wamp_invokee_t {
	int	priority;
	const char *wampName;
	WAMPCB_Invokee cb_invokee;
} wamp_invokee_t;

typedef struct wamp_call_t {							// Holds information about an active call
	const char *callerWampName;							// The name of the calling wamp
	long long callerRequestId;
	const char *calleeWampName;							// The name of the called wamp
	long long calleeRequestId;
	wamp_invokee_t *invokee;
	bool wantsYield;									// true if this is a redirected call, that needs to be returned as yield
} wamp_call_t;

typedef struct wamp_callee_t {
	const char *procedure;								// The name of the procedure that these callees handle
	const char *timestamp;
	int matchType;
	long long registrationId;							// Id under which callee registered
	int roundRobinCount;
	int nInvokees;										// Number of invokees servicing this call
	wamp_invokee_t **invokee;
	int invokePolicy;									// WAMP_INVOKE_SINGLE ... WAMP_INVOKE_LAST
	int highestPriority;								// Highest priority of any invokees
} wamp_callee_t;

typedef struct subscription_history_t {
	subscription_history_t *prev;
	subscription_history_t *next;

	const char *timestamp;								// YYYY-MM-DDTHH:MM:SS.sss
	long long publicationId;
	const char *topic;
	JSON *details;
	JSON *argList;
	JSON *argDict;
} subscription_history_t;

typedef struct subscription_t {
	WAMPREALM *realm;
	const char *topic;									// The topic to which this is a subscription
	const char *timestamp;
	int matchType;
	int count;											// Number of active subscriptions (= spmap_Count(wamps))
	SPMAP *map;											// Map within the realm into which this fits (e.g. subMapExact)
	long long subscriptionId;
	SPMAP *wamps;										// Maps the subscribed WAMPS (by name) to their subscription_info_t
	char saveHistory;									// non-0 to save history
	subscription_history_t *history;					// In-memory list of history of events
} subscription_t;

typedef struct subscription_info_t {
	subscription_t *sub;
	const char *wampname;
	long long requestId;
	char wait_for_history;								// 1 if the client wants history before receiving this publications
} subscription_info_t;

typedef struct client_subscription_t {					// For recording subscriptions for the client
	long long requestId;								// Request ID from wamp_Subscribe()
	long long subscriptionId;							// Subscription ID
	long long unrequestId;								// Request ID from wamp_Unsubscribe()
	WAMPCB_Subscribed cbSubscribed;						// Called when subscribed
	WAMPCB_Event cbEvent;								// Called on each event
	WAMPCB_Unsubscribed cbUnsubscribed;					// Called when unsubscribed
	int type;											// WAMP_MATCH_EXACT, WAMP_MATCH_PREFIX or WAMP_MATCH_WILDCARD
	const char *topic;									// Our topic
} client_subscription_t;

typedef struct client_publish_t {
	long long requestId;
	const char *topic;									// The original published topic
	WAMPCB_Published cbPublished;						// A function to call back
} client_publish_t;

typedef struct client_call_t {
	long long requestId;
	const char *procedure;								// The original published topic
	WAMPCB_Result cbResult;								// A function to call back
	void *data;											// Some data tagging along from the wamp_Call()
} client_call_t;

typedef struct client_registration_t {
	long long requestId;
	long long registrationId;
	long long unrequestId;								// Id used to unregister
	const char *procedure;								// The original registered procedure
	WAMPCB_Registered cbRegistered;						// Called back when we received WAMP_REGISTERED
	WAMPCB_Invoke cbInvoke;								// A function to call back
	WAMPCB_Unregistered cbUnregistered;					// Called when unregistered
	void *data;											// A pointer that is passed in at registration stage
} client_registration_t;

typedef struct sublist_t {								// A list of subscription IDs
	struct sublist_t *prev;
	subscription_t *sub;
} sublist_t;

typedef struct wamp_redirect_info_t {
	WAMPCB_Modifier cbModifier;
} wamp_redirect_info_t;

STATIC SPMAP *allWamps = NULL;							// All active wamps by name

STATIC IDMAP *activeCallMap = NULL;						// Active calls - points to wamp_invokee_t
STATIC WAMPCB_NoCallee wampcb_NoCallee = NULL;			// Callback function if there is no callee
STATIC WAMPCB_Invokee wampcb_Invokee = NULL;			// Callback function when a procedure is invoked

STATIC struct wamp_code_t {
	int code;
	const char *name;
} wamp_codes[] = {
	{WAMP_HELLO,			"Hello"},
	{WAMP_WELCOME,			"Welcome"},
	{WAMP_ABORT,			"Abort"},
	{WAMP_CHALLENGE,		"Challenge"},
	{WAMP_AUTHENTICATE,		"Authenticate"},
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
	{WAMP_CANCEL,			"Cancel"},
	{WAMP_RESULT,			"Result"},

	{WAMP_REGISTER,			"Register"},
	{WAMP_REGISTERED,		"Registered"},
	{WAMP_UNREGISTER,		"Unregister"},
	{WAMP_UNREGISTERED,		"Unregistered"},
	{WAMP_INVOCATION,		"Invocation"},
	{WAMP_INTERRUPT,		"Interrupt"},
	{WAMP_YIELD,			"Yield"},

	{0,NULL}
};

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

// IDMAP - maps long long to pointer
// The map is really string to pointer but the long long keys are converted to strings

API IDMAP *idmap_New()												{ return (IDMAP*)spmap_New(); }
API void idmap_Delete(IDMAP *im)									{ spmap_Delete((SPMAP*)im); }
API void idmap_Reset(IDMAP *im)										{ spmap_Reset((SPMAP*)im); }
API void idmap_Clear(IDMAP *im)										{ spmap_Clear((SPMAP*)im); }
API int idmap_Count(IDMAP *im)										{ return spmap_Count((SPMAP*)im); }
API void idmap_Sort(IDMAP *im, int (*sorter)(const char *, const char *)) { spmap_Sort((SPMAP*)im, sorter); }
API void idmap_SortValues(IDMAP *im, int (*sorter)(const char *, const char *)) { spmap_SortValues((SPMAP*)im, sorter); }
API void *idmap_GetValueAtIndex(IDMAP *im, int index)                 { return spmap_GetValueAtIndex((SPMAP*)im, index); }

API int idmap_GetNextEntry(IDMAP *im, long long *pKey, void **pValue)
{
	const char *key = "0";

	int result = spmap_GetNextEntry((SPMAP*)im, &key, pValue);
	if (pKey) *pKey = atoll(key);

	return result;
}

API int idmap_DeleteKey(IDMAP *im, long long id)
{
	char key[28];

	sprintf(key, "%lld", id);
	return spmap_DeleteKey((SPMAP*)im, key);
}

API long long idmap_GetKeyAtIndex(IDMAP *im, int index)
{
	long long result = 0;
	const char *key = spmap_GetKeyAtIndex((SPMAP*)im, index);

	if (key) result = atoll(key);

	return result;
}

API int idmap_Add(IDMAP *im, long long id, void *value)
{
	char key[28];

	sprintf(key, "%lld", id);

	return spmap_Add((SPMAP*)im, key, value);
}

API void *idmap_GetValue(IDMAP *im, long long id)
{
	char key[18];

	sprintf(key, "%lld", id);

	void *result = spmap_GetValue((SPMAP*)im, key);

	return result;
}

API long long idmap_GetKey(SIMAP *im, void *value)
{
	long long result = 0;
	const char *key = spmap_GetKey((SPMAP*)im, value);

	if (key) result = atoll(key);

	return result;
}

//API void wamp_Logger(void (*logger)(const char *, va_list))
API void wamp_Logger(WAMP_Logger logger)
// Sets a logging function to receive all debug output from the WAMP library
{
	_Logger = logger;
}

API WAMP_Logger wamp_GetLogger()
{
	return _Logger;
}

//////////////////////

// Connection provided an incorrect URI for any URI-based attribute of WAMP message,
// such as realm, topic or procedure.
//STATIC const char *errInvalidUri = "wamp.error.invalid_uri";

// A Dealer could not perform a call, since no procedure is currently
// registered under the given URI.
STATIC const char *errNoSuchProcedure = "wamp.error.no_such_procedure";

// A procedure could not be registered, since a procedure with the given URI
// is already registered.
STATIC const char *errDomainAlreadyExists = "wamp.error.procedure_already_exists";

// A Dealer could not perform an unregister, since the given registration is
// not active.
STATIC const char *errNoSuchRegistration = "wamp.error.no_such_registration";

// A Broker could not perform an unsubscribe, since the given subscription is
// not active.
STATIC const char *errNoSuchSubscription = "wamp.error.no_such_subscription";

// A router could not return information on a session as the given session isn't found
STATIC const char *errNoSuchSession = "wamp.error.no_such_session";

// A call failed, since the given argument types or values are not acceptable
// to the called procedure - in which case the Callee may throw this error. Or
// a Node performing payload validation checked the payload (args / kwargs)
// of a call, call result, call error or publish, and the payload did not
// conform - in which case the Node may throw this error.
STATIC const char *errInvalidArgument = "wamp.error.invalid_argument";

// The Connection is shutting down completely - used as a GOODBYE (or aBORT) reason.
//STATIC const char *errSystemShutdown = "wamp.error.system_shutdown";

// The Connection wants to leave the realm - used as a GOODBYE reason.
//STATIC const char *errCloseRealm = "wamp.error.close_realm";

// A Connection acknowledges ending of a session - used as a GOOBYE reply reason.
STATIC const char *errGoodbyeAndOut = "wamp.error.goodbye_and_out";

// A join, call, register, publish or subscribe failed, since the Connection is not
// authorized to perform the operation.
//STATIC const char *errNotAuthorized = "wamp.error.not_authorized";

// A Dealer or Broker could not determine if the Connection is authorized to perform
// a join, call, register, publish or subscribe, since the authorization
// operation itself failed. E.g. a custom authorizer ran into an error.
//STATIC const char *errAuthorizationFailed = "wamp.error.authorization_failed";

// Connection wanted to join a non-existing realm (and the Node did not allow to
// auto-create the realm)
STATIC const char *errNoSuchRealm = "wamp.error.no_such_realm";

// A call has been canceled (or timed out)
STATIC const char *errCanceled = "wamp.error.canceled";

// A Connection was to be authenticated under a Role that does not (or no longer)
// exists on the Node. For example, the Connection was successfully authenticated,
// but the Role configured does not exists - hence there is some
// misconfiguration in the Node.
//STATIC const char *errNoSuchRole = "wamp.error.no_such_role";

API CHANPOOL *wamp_Pool(WAMP *wamp)
{
	return wamp && wamp->connectionType == 1 ? ws_Pool(wamp->connection.ws) : NULL;
}

STATIC const char *TimeNow()
{
	static char result[25];

	time_t now = time(NULL);
	struct tm *tm = gmtime(&now);

	struct timeval tp;
	int msecs;

	gettimeofday(&tp, NULL);
	msecs=tp.tv_usec / 1000;

	snprintf(result, sizeof(result), "%04d-%02d-%02dT%02d:%02d:%02d.%03d",
			tm->tm_year+1900, tm->tm_mon+1, tm->tm_mday,
			tm->tm_hour, tm->tm_min, tm->tm_sec,
			msecs);

	return result;
}

#if 0			// In fact we never delete a subscription once it's created so commenting this out stops the compiler whinging
STATIC void subscription_t_Delete(subscription_t *sub)
// Deletes a subscription and removes it from the realm
{
	if (sub) {
		spmap_DeleteKey(sub->map, sub->topic);
		idmap_DeleteKey(sub->realm->allSubscriptions, sub->subscriptionId);

		szDelete(sub->topic);
		spmap_Delete(sub->wamps);
	}
}
#endif

STATIC subscription_t *subscription_t_New(WAMPREALM *realm, SPMAP *map, int matchType, const char *topic)
{
	subscription_t *sub = NEW(subscription_t, 1);

	sub->realm = realm;
	sub->topic = strdup(topic);
	sub->timestamp = strdup(TimeNow());
	sub->matchType = matchType;
	sub->map = map;
	sub->subscriptionId = wamp_RandomId();
	sub->wamps = spmap_New();
	sub->history = NULL;
	sub->saveHistory = 1;													// TODO: From config somehow!
	sub->count = 0;
	if (!strncmp(topic, "wamp.", 5))
	sub->saveHistory = 0;
	spmap_Add(map, topic, sub);
	idmap_Add(realm->allSubscriptions, sub->subscriptionId, sub);

	return sub;
}

STATIC subscription_info_t *subscription_t_AddSubscription(subscription_t *sub, const char *wampname, long long requestId, int wait_for_history)
// Creates a wamp to an existing subscription
{
	subscription_info_t *subinfo = NULL;

	if (sub && wampname) {
		subinfo = NEW(subscription_info_t, 1);
		subinfo->sub = sub;
		subinfo->wampname = strdup(wampname);
		subinfo->requestId = requestId;
		subinfo->wait_for_history = wait_for_history;
		spmap_Add(sub->wamps, wampname, subinfo);

		sub->count++;
	}

	return subinfo;
}

STATIC int subscription_t_RemoveSubscription(subscription_info_t *subinfo)
// Deletes the subinfo structure and removes it from the containing sub
// Returns 1 if it did it (basically if subinfo is non-null), 0 otherwise
{
	if (subinfo) {
		spmap_DeleteKey(subinfo->sub->wamps, subinfo->wampname);

		WAMP *wamp = wamp_ByName(subinfo->wampname);

		if (wamp) {
			long long subscriptionId = subinfo->sub->subscriptionId;

			JSON *list = json_NewArray();
			json_ArrayAddInteger(list, wamp->sessionId);
			json_ArrayAddInteger(list, subscriptionId);
			wamp_PublishIn(wamp, "wamp.subscription.on_unsubscribe", NULL, list);

			if (subinfo->sub->count == 1) {
				JSON *list = json_NewArray();
				json_ArrayAddInteger(list, wamp->sessionId);
				json_ArrayAddInteger(list, subscriptionId);
				wamp_PublishIn(wamp, "wamp.subscription.on_delete", NULL, list);
			}
		}

		subinfo->sub->count--;
		szDelete(subinfo->wampname);
		free((char*)subinfo);

		return 1;
	}

	return 0;

	// Note that we may end up here with a subscription with no wamps subsribing to it.  This is fine as we want
	// to retain it so that a new subscription to the same topic definition will get the same subscription ID
	// otherwise wamp.topic.history.* calls won't work.
}

#if 0			// Actually never used, but left here for the moment in case we suddenly need it!
STATIC int subscription_t_RemoveSubscription(WAMPREALM *realm, const char *wampname, long long subscriptionId)
// Returns 1 if the realm held this subscription for the given wamp, 0 otherwise.
{
	int result = 0;

	if (realm && wampname) {
		subscription_t *sub = (subscription_t *)idmap_GetValue(realm->allSubscriptions, subscriptionId);

		if (sub) {
			subscription_info_t *subinfo = (subscription_info_t *)spmap_GetValue(sub->wamps, wampname);

			if (subinfo) {
				result = subscription_t_RemoveSubscription(subinfo);
			}
		}
	}

	return result;
}
#endif

STATIC subscription_t *wamp_SubscriptionById(WAMP *wamp, long long subscriptionId)
{
	subscription_t *sub = NULL;

	if (wamp) {
		sub = (subscription_t *)idmap_GetValue(wamp->realm->allSubscriptions, subscriptionId);
	}

	return sub;
}

STATIC int wamp_RemoveWampFromSubscription(subscription_t *sub, const char *wampname)
// Removes the wamp from the subscription, given a subscription structure.
// Returns 1 if is was actually removed, 0 if it wasn't or the arguments were bad.
{
	int result = 0;

	if (sub && wampname) {
		subscription_info_t *subinfo = (subscription_info_t *)spmap_GetValue(sub->wamps, wampname);

		if (subinfo) {
			result = subscription_t_RemoveSubscription(subinfo);
		}
	}


	return result;
}

STATIC int wamp_IsSubscriptionType(int type)
{
	return type >= WAMP_PUBLISH && type < WAMP_CALL;
}

STATIC int wamp_IsCallType(int type)
{
	return type >= WAMP_CALL && type <= WAMP_YIELD;
}

API void wamp_SetPeer(WAMP *wamp, const char *peer)
// Sets the identity of the peer, which is not 'used' as such but can be used to better identify the connection
{
	if (wamp) {
		szDelete(wamp->peer);
		wamp->peer = peer ? strdup(peer) : NULL;
	}
}

API const char *wamp_ConnectionName(WAMP *w)
{
	static const char *name = NULL;
	szDelete(name);

	if (!w) return "NULL_WAMP";

	if (w->connectionType == WAMP_CONN_WEBSOCKET) {
		return ws_Name(w->connection.ws);
	} else if (w->connectionType == WAMP_CONN_RAWSOCKET) {
		return "rawsocket???";
	} else if (w->connectionType == WAMP_CONN_CODE) {
		name = hprintf(NULL, "code(%p)", w->connection.fn);
	} else {
		name = hprintf(NULL, "Invalid connection type %d", w->connectionType);
	}

	return name;
}

API const char *wamp_Name(WAMP *w)
// Returns the name of a WAMP.  This can later be used in wamp_ByName().
// If you want to refer to a WAMP later, using a name is safer as WAMPs can be deleted...
// WAMP *savedWamp = myWamp; ...... time passes .......  wamp_SendJson(savedWamp, ...) can fail if it's been deleted in the middle
// const char *savedWamp = wamp_Name(myWamp); .....  WAMP *wamp = wamp_ByName(savedWamp); if (wamp) wamp_SendJson(... is safe.
{
	return w ? w->name : "NULL_WAMP";
}

API const char *wamp_RealmName(WAMP *w)
{
	if (w) {
		if (w->realm)
			return w->realm->name;
		else
			return "NULL REALM";
	} else {
		return "NULL_WAMP_REALM";
	}
}

API long long wamp_Session(WAMP *w)
{
	return w ? w->sessionId : 0;
}

API bool wamp_Authenticated(WAMP *w)
{
	return w ? w->authenticated : false;
}

API const char *wamp_Peer(WAMP *w)
{
	return w ? w->peer : NULL;
}

API const char *wamp_AuthId(WAMP *w)
{
	return w ? w->authId : NULL;
}

API const char *wamp_AuthRole(WAMP *w)
{
	return w ? w->authRole : NULL;
}

API const char *wamp_AuthMethod(WAMP *w)
{
	return w ? w->authMethod : NULL;
}

API const char *wamp_AuthProvider(WAMP *w)
{
	return w ? w->authProvider : NULL;
}

API const char *wamp_TypeName(int type)
// Given a WAMP message type number, returns the name
{
	struct wamp_code_t *rover = wamp_codes;

	while (rover->code) {
		if (rover->code == type)
			return rover->name;
		rover++;
	}

	if (type == 999) return "MTPost";					// Just to make logs look less daft

	return "Unknown wamp message type";
}

API const char *wamp_MatchName(int type)
// Returns the textual representation of WAMP_MATCH_EXACT, WAMP_MATCH_PREFIX or WAMP_MATCH_WILDCARD
{
	if (type == WAMP_MATCH_EXACT)		return "exact";
	if (type == WAMP_MATCH_PREFIX)		return "prefix";
	if (type == WAMP_MATCH_WILDCARD)	return "wildcard";
	if (type == WAMP_MATCH_REGEX)		return "regex";

	return "unknown";
}

API const char *wamp_PolicyName(int type)
// Returns the textual representation of WAMP_SINGLE, WAMP_ROUNDROBIN, WAMP_RANDOM, WAMP_FIRST or WAMP_LAST
{
	if (type == WAMP_INVOKE_SINGLE)		return "single";
	if (type == WAMP_INVOKE_ROUNDROBIN)	return "roundrobin";
	if (type == WAMP_INVOKE_RANDOM)		return "random";
	if (type == WAMP_INVOKE_FIRST)		return "first";
	if (type == WAMP_INVOKE_LAST)		return "last";

	return "unknown";
}

STATIC int wamp_MatchTypeFromUri(const char* uri, char **presult)
// Given a uri with possible '*' in it, attempts to ascertain a match type.
// If presult is non-NULL, a heap-based copy of the modified (without '*') uri is placed there,
// otherwise it'll over-write the passed string.
// If there is no match type found then uri and *result are left alone.
// Returns	0				uri passed was NULL
//			WAMP_MATCH_*	The figured out type (WAMP_MATCH_EXACT if nothing else)
{
	int matchType = 0;

	if (uri) {
		char *copy = strdup(uri);
		matchType = WAMP_MATCH_EXACT;

		if (*copy == '.'											// . at the front
				|| (*copy == '*' && copy[1])						// * at the front, but not the only character
				|| strstr(copy, "..")								// .. anywhere in the string
				|| strstr(copy, ".*.")) {							// .*. anywhere in the string
			matchType = WAMP_MATCH_WILDCARD;
		} else if (*uri) {											// Check for ending with wildcard ('.' or '*')
			char c = uri[strlen(uri)-1];

			if (c == '.' || c == '*') {
				matchType = WAMP_MATCH_PREFIX;
			}
		}

		if (strchr(copy, '*')) {				// Remove any '*'
			char *dest = copy;
			char *src = copy;
			char c;

			while ((c=*src++)) {
				if (c != '*')
					*dest++ = c;
			}
			*dest = '\0';
		}

		if (presult) {
			*presult = copy;
		} else {
			strcpy((char*)uri, copy);
			szDelete(copy);
		}
	}

	return matchType;
}

STATIC time_t DecodeTimeStamp(const char *szDt, int *pms)
// Turns a YYYY-MM-DDTHH:MM:SS:mmm thing into a unix time
// pms (if non-NULL) will receive milliseconds or 0 if none are given.
// As a special case, pass NULL to return the current time
{
	if (!szDt) {
		if (pms) {
			struct timeval tp;
			gettimeofday(&tp, NULL);
			*pms=tp.tv_usec / 1000;
		}

		return time(NULL);
	}

	int nLen = strlen(szDt);
	struct tm tm;
	memset(&tm, 0, sizeof(tm));
	tm.tm_isdst=-1;				// Trust this not to be timezoned

	if (nLen >= 10) {			// YYYY-MM-DD	(assume 00:00:00)
		tm.tm_year=atoi(szDt)-1900;
		tm.tm_mon=atoi(szDt+5)-1;
		tm.tm_mday=atoi(szDt+8);
	}

	if (nLen >= 16) {			// YYYY-MM-DDTHH:MM
		tm.tm_hour=atoi(szDt+11);
		tm.tm_min=atoi(szDt+14);
	}

	if (nLen >= 19) {			// YYYY-MM-DDTHH:MM:SS
		tm.tm_sec=atoi(szDt+17);
	}

	if (nLen == 23 && pms) {	// YYYY-MM-DDTHH:MM:SS:mmm
		*pms = atoi(szDt+20);
	}
//Log("y=%d,m=%d,d=%d,h=%d,m=%d,s=%d",tm.tm_year,tm.tm_mon,tm.tm_mday,tm.tm_hour,tm.tm_min,tm.tm_sec);

	return mktime(&tm);
}

STATIC void LogString(WAMP *wamp, const char *prefix, const char *string)
{
	const char *name = "?";
	if (string && *string == '[') {
		int type = atoi(string+1);
		const char *comma = strchr(string, ',');

		if (type == 8 && comma) {							// Error
			static char buf[40];

			int errType = atoi(comma+1);

			snprintf(buf, sizeof(buf), "Error(%s)", wamp_TypeName(errType));
			name = buf;
		} else {
			name = wamp_TypeName(type);
		}
	}
	const char *wampName = wamp_Name(wamp);
	if (!wampName) wampName = "NULL";

	Log("WA: %-8s(%s): %s - %s", prefix, wampName, name, Printable(-1, string));
}

API long long wamp_RandomId()
// Returns a random number 1..2^53-1
{
	static int seeded = 0;

	if (!seeded) {
		seeded = 1;
		srand48(time(NULL) * (getpid()+1));
	}

	long long result = 0;
	while (result == 0) {	// Dangerous but if it doesn't exit, the random number generator has gone weird, which would be spooky
		long long a = lrand48();						// Unsigned
		long long b = lrand48();						// Unsigned
		result = (a ^ (b<<32)) & 0x1fffffffffffffL;		// XOR them and take the bottom 53 bits
	}

	return result;
}

API int wamp_Receive(WAMP *wamp, JSON *message)
// Receives a JSON message on the given WAMP and processes it as an incoming thing
// May simply return an error on the WAMP
{
	const char *err = json_Error(message);
	if (err) {
		wamp_SendErrorStr(wamp, 0, 0, "Invalid JSON", errInvalidArgument, NULL, NULL);
		return 1;
	}

	if (json_Type(message) != JSON_ARRAY) {
		wamp_SendErrorStr(wamp, 0, 0, "JSON message must be an array", errInvalidArgument, NULL, NULL);
		return 1;
	}

	if (json_ArrayCount(message) < 1) {
		wamp_SendErrorStr(wamp, 0, 0, "Message must have at least one element", errInvalidArgument, NULL, NULL);
		return 1;
	}

	JSON *element1 = json_ArrayAt(message, 0);
	if (json_Type(element1) != JSON_INTEGER) {
		wamp_SendErrorStr(wamp, 0, 0, "Message type must be an integer", errInvalidArgument, NULL, NULL);
		return 1;
	}

	int messageType = json_AsInteger(element1);

	int result = wamp_Dispatch(wamp, messageType, message);
	json_Delete(message);

	return result;
}

int wamp_HandleIncomingData(WS *ws, int len, const char *text)
// This function handles UTF8 data coming in on the WebSocket channel.
// Currently this can only be destined for the WAMP system and this is effectively the interface function.
// Returns	0		All went well and was handled
//			1...	Fatal error - close connection
{
//Log("WA: WS-IN(%s): %.*s", ws->name, len, text);

	WAMP *wamp = (WAMP*)ws_Info(ws);

	if (wamp && wamp->saveInput) {
		hlist_Add(wamp->saveInput, len, text);
		hlist_Add(wamp->saveInput, 1, "\n");
	}

	JSON *j = json_ParseText(text);

	return wamp_Receive(wamp, j);
}

API void *wamp_SetInfo(WAMP *w, void *info)
// Sets the 'info' data for this wamp.  This is not used by the wamp itself but is available for the caller
{
	void *previous = NULL;
	if (w) {
		previous = w->info;
		w->info = info;
	}

	return previous;
}

API void *wamp_Info(WAMP *w)
// Returns the 'info' pointer previously set using wamp_SetInfo()
{
	return w ? w->info : NULL;
}

STATIC WAMP *wamp_New(const char *name)
// This function is used only by other wamp_NewXxx() functions.
// name must be on the heap
{
	WAMP *w = NEW(WAMP, 1);

	w->name = name;
	w->sessionId = 0;
	w->realm = NULL;
	w->handlers = idmap_New();
	w->info = NULL;
	w->cb_ondelete = NULL;
	w->cbWelcome = NULL;
	w->deleting = 0;
	w->masked = 0;
	w->saveOutput = NULL;
	w->saveInput = NULL;
	w->clientSubscriptionsReq = idmap_New();				// Things we're subscribed to by requestID
	w->clientSubscriptionsSub = idmap_New();				// Things we're subscribed to by subscriptionID
	w->clientSubscriptionsUnreq = idmap_New();				// Things we're unsubscribed to by requestID
	w->clientPublishes = idmap_New();						// Things we've published by requestID
	w->clientCalls = idmap_New();							// Calls we're in the middle of making
	w->clientRegistrationsReq = idmap_New();				// Registrations we have made by requestID
	w->clientRegistrationsReg = idmap_New();				// Registrations we have made by registerationID
	w->clientRegistrationsUnreq = idmap_New();				// Registrations we have unregistered by registerationID

	w->peer = NULL;

	w->authenticated = false;
	w->authId = hprintf(NULL, "auth-%lld", wamp_RandomId());
	w->authRole = strdup("anonymous");
	w->authMethod = strdup("anonymous");
	w->authProvider = strdup("static");

	w->testaments = new testamentCollection;

	if (!allWamps) allWamps = spmap_New();

	spmap_Add(allWamps, w->name, (void*)w);			// If it's already there then the world is probably already ending

	return w;
}

API HLIST *wamp_SaveOutput(WAMP *wamp, HLIST *hlist)
// Save a copy of all output from the WAMP in hlist.
// Returns the previous value
{
	HLIST *result = NULL;

	if (wamp) {
		result = wamp->saveOutput;
		wamp->saveOutput = hlist;
	}

	return result;
}

API HLIST *wamp_SaveInput(WAMP *wamp, HLIST *hlist)
// Save a copy of all input to the WAMP in hlist.
// Returns the previous value
{
	HLIST *result = NULL;

	if (wamp) {
		result = wamp->saveInput;
		wamp->saveInput = hlist;
	}

	return result;
}

API void wamp_SetMasked(WAMP *wamp, int masked)
{
	if (wamp)
		wamp->masked = masked;
}


API WAMP *wamp_NewPseudo(WAMPCB_Data fn, const char *name)
{
	WAMP *w = wamp_New(hprintf(NULL, "pseudo_%s", name));

	w->connectionType = WAMP_CONN_CODE;
	w->connection.fn = fn;

	return w;
}

STATIC void OnSocketDeleted(WS *ws)
{
	WAMP *wamp = (WAMP*)ws_Info(ws);

//Log("WA: Socket belonging to %s (%s) being deleted", wamp_Name(wamp), ws_Name(ws));
	wamp_Delete(wamp);
//HERE!! - need to ensure the wamp is deleted and anything relying on it is removed from lists etc.
}

API WAMP *wamp_NewOnWebsocket(WS *ws, int masked)
{
	WAMP *w = wamp_New(hprintf(NULL, "wamp_%s", ws_Name(ws)));

	w->connectionType = WAMP_CONN_WEBSOCKET;
	w->connection.ws = ws;
	w->masked = masked;

	ws_SetInfo(ws, w);
	ws_SetUtf8Handler(ws, wamp_HandleIncomingData);
	ws_OnDelete(ws, OnSocketDeleted);

	return w;
}

STATIC JSON *wamp_SessionObject(WAMP *wamp)
{
	JSON *json = json_NewObject();
	if (wamp) {
		json_ObjectAddInteger(json, "session", wamp->sessionId);
		json_ObjectAddStringz(json, "authid", wamp->authId);
		json_ObjectAddStringz(json, "authrole", wamp->authRole);
		json_ObjectAddStringz(json, "authmethod", wamp->authMethod);
		json_ObjectAddStringz(json, "authprovider", wamp->authProvider);

		JSON *transport = json_NewObject();
		switch (wamp->connectionType) {
			case 1:						// Websocket
				json_ObjectAddStringz(transport, "type", "websocket");
				json_ObjectAddStringz(transport, "name", ws_Name(wamp->connection.ws));
				break;
			case 2:						// Rawsocket
				json_ObjectAddStringz(transport, "type", "rawsocket");
				break;
			case 3:						// Code
				json_ObjectAddStringz(transport, "type", "code");
				break;
		}
		json_ObjectAdd(json, "transport", transport);

	}

	return json;
}

STATIC long long wamp_AddToSession(WAMP *wamp)
// Adds the wamp to the session, creating a new sessionId in the process.
{
	long long sessionId = 0;

	if (wamp) {
		WAMPREALM *realm = wamp->realm;

		long long sessionId = wamp_RandomId();
		idmap_Add(realm->allSessions, sessionId, (void*)wamp);
		wamp->sessionId = sessionId;

		wamp_PublishIn(wamp, "wamp.session.join", NULL, wamp_SessionObject(wamp));
	}

	return sessionId;
}

STATIC void wamp_RemoveFromSession(WAMP *wamp)
{
	if (wamp) {
		if (wamp->realm)
			idmap_DeleteKey(wamp->realm->allSessions, wamp->sessionId);

		wamp_PublishIn(wamp, "wamp.session.leave", NULL, json_NewInteger(wamp->sessionId));

		wamp->sessionId = 0;
	}
}

API WAMP *wamp_BySession(WAMPREALM *realm, long long sessionId)
{
	WAMP *wamp = NULL;
	if (realm) {
		wamp = (WAMP*)idmap_GetValue(realm->allSessions, sessionId);
	}

	return wamp;
}

API WAMP *wamp_ByName(const char *name)
{
	return (name && allWamps) ? (WAMP*)spmap_GetValue(allWamps, name) : NULL;
}

API int wamp_Send(WAMP *wamp, const char *message)
// Write a string to this WAMP's output channel
// The string will always be ASCII (JSON) so we don't need a length
// Returns the number of bytes sent
{
	int result = 0;

	if (wamp && message) {
LogString(wamp, "WAMP OUT", message);
		if (wamp->connectionType == WAMP_CONN_WEBSOCKET) {	// Means it's a WebSocket, which is the only thing we know at the moment
			int fragLen;
			const char *frag = ws_MakeFragment(1, 1, wamp->masked ? -1 : 0, -1, message, &fragLen);

			result = ws_WriteHeap(wamp->connection.ws, fragLen, frag);
		} else if (wamp->connectionType == WAMP_CONN_RAWSOCKET) {		// Raw socket
		} else if (wamp->connectionType == WAMP_CONN_CODE) {			// Pseudo WAMP
			// We should never get here as this case is picked up in wamp_SendJson()
		} else {										// TODO: An error really...
		}
		if (wamp->saveOutput) {
			hlist_Add(wamp->saveOutput, -1, message);
			hlist_Add(wamp->saveOutput, 1, "\n");
		}
	}

	return result;
}

#if 0				// Never actually called so removed to keep the compilers content
STATIC int wamp_SendHeap(WAMP *wamp, const char *message)
// Exactly like wamp_Send() but deletes the message afterwards
{
	int result = wamp_Send(wamp, message);
	szDelete(message);

	return result;
}
#endif

API void wamp_SendJson(WAMP *wamp, JSON *json)
{
//Log("WA: wamp_SendJson(%s, %s)", wamp_Name(wamp), json_Render(json));
	if (json) {
		if (wamp->connectionType == WAMP_CONN_CODE) {
			(wamp->connection.fn)(wamp, json);
		} else {
			wamp_Send(wamp, json_Render(json));
		}
	} else {
		Log("WA: NULL JSON passed to wamp_SendJson");
	}
}

API void wamp_SendJsonHeap(WAMP *wamp, JSON *json)
{
	wamp_SendJson(wamp, json);
	json_Delete(json);
}

API JSON *wamp_NewMessage(int type, long long id)
// Returns a new JSON array with the type filled in and the id if it's non-0
// Passing id as -1, uses a random ID.
{
	JSON *j = json_NewArray();
	json_ArrayAddInteger(j, type);
	if (id) json_ArrayAddInteger(j, id == -1 ? wamp_RandomId() : id);

	return j;
}

STATIC void AddArguments(JSON *json, JSON *argList=NULL, JSON *argDict=NULL)
// Add the argList and argDict to the message.
// Note that an empty argList array will be inserted if it is NULL and argDict is not.
// If argList is given, but is not an array then it'll be put into one.  E.g. "Hi" will become ["Hi"]
// argList and argDict become owned by the message so don't go deleting them later.
{
	if (argDict && !argList) argList = json_NewArray();		// Need to make an argList if we don't have one, but have an argDict
	if (argList && !json_IsArray(argList)) {
		JSON *tmp = json_NewArray();
		json_ArrayAdd(tmp, argList);
		argList = tmp;
	}
	if (argList) json_ArrayAdd(json, argList);
	if (argDict) json_ArrayAdd(json, argDict);
}

API void wamp_SendError(WAMP *wamp, int type, long long id, SPMAP *details, const char *uri, JSON *argList/*=NULL*/, JSON *argDict/*=NULL*/)
// If details, argList and/or argDict are non-NULL, they become owned, then deleted here.
// Generic error sending
// wamp			- Where we're sending the error
// type			- E.g. WAMP_INVOCATION, WAMP_CALL etc.
// id			- The identifier which will (hopefully) allow the receipient to tie up the error with the message that caused it
// details		- NULL or a details MAP
// uri			- A WAMP error uri
// argList	- NULL or some JSON containing the argument list array
// argDict	- NULL or some JSON containing the argument list dictionary
{
	JSON *j = wamp_NewMessage(WAMP_ERROR, 0);
	json_ArrayAddInteger(j, type);
	json_ArrayAddInteger(j, id);
	JSON *callDetails = json_NewObjectWith(details);
	json_ArrayAdd(j, callDetails);
	json_ArrayAddStringz(j, uri);
	AddArguments(j, argList, argDict);

	wamp_SendJsonHeap(wamp, j);
}

API void wamp_SendErrorStr(WAMP *wamp, int type, long long id, const char *err, const char *uri, JSON *argList, JSON *argDict)
// If argList and/or argDict are non-NULL, they become owned, then deleted here.
{
	SPMAP *details = spmap_New();
	if (err) spmap_Add(details, "error", json_NewStringz(err));

	wamp_SendError(wamp, type, id, details, uri, argList, argDict);
}

API void wamp_SendInternalError(WAMP *wamp, int type, long long id, const char *uri, const char *mesg)
// Uses the uri and message to send an error
// mesg MUST be on the heap and becomes owned by this function
{
	JSON *list = json_NewArray();

	json_ArrayAddStringz(list, mesg);

	wamp_SendError(wamp, type, id, NULL, uri, list, NULL);
}

API void wamp_SendResult(WAMP *wamp, long long requestId, JSON *details, JSON *argList/*=NULL*/, JSON *argDict/*=NULL*/)
// If details, argList and/or argDict are non-NULL, they become owned, then deleted here.
{
	JSON *j = wamp_NewMessage(WAMP_RESULT, requestId);
	json_ArrayAdd(j, details ? details : json_NewObject());
	AddArguments(j, argList, argDict);

Log("Sending to %s: %s", wamp_Name(wamp), json_Render(j));
	wamp_SendJsonHeap(wamp, j);
}

API void wamp_CloseAbort(WAMP *wamp, int type, const char *uri, const char *reason)
// Close and Abort formats are identical apart from the type
{
	JSON *j = wamp_NewMessage(type, 0);
	JSON *jreason = json_ArrayAdd(j, json_NewObject());
	if (reason)
		json_ObjectAddStringz(jreason, "reason", reason);
	json_ArrayAddStringz(j, uri);

	wamp_SendJsonHeap(wamp, j);
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

API void wamp_Finish(WAMP *wamp)
// We have finished with this WAMP so close the associated channel and delete it.
{

}

API const char *realm_Name(WAMPREALM *realm)
{
	return realm ? realm->name : "NULL_REALM";
}

STATIC WAMPREALM *realm_New(const char *name)
{
	WAMPREALM *realm = NEW(WAMPREALM, 1);

	realm->name = strdup(name);
	realm->allSessions = idmap_New();
	realm->wamps = sset_New();
	realm->calleeMapExact = spmap_New();;
	realm->calleeMapPrefix = spmap_New();;
	realm->calleeMapWildcard = spmap_New();;
	realm->calleeMapRegex = spmap_New();;
	realm->redirect = idmap_New();
	realm->subMapExact = NULL;
	realm->subMapPrefix = NULL;
	realm->subMapWildcard = NULL;
	realm->subMapRegex = NULL;
	realm->allSubscriptions = NULL;

	realm->wantForwardSubscriptions = 0;
	realm->forwardSubscriptions = NULL;				// Reset forwarding
	realm->fwdSubscriptionMap = ssmap_New();
	realm->fwdUnsubscriptionMap = ssmap_New();
	realm->fwdEventMap = ssmap_New();
	realm->fwdPublishMap = ssmap_New();

	return realm;
}

STATIC SPMAP *realm_Map()
{
	static SPMAP *realm_map = NULL;

	if (!realm_map) {
		realm_map = spmap_New();

		spmap_Add(realm_map, "spider", realm_New("spider"));
	}

	return realm_map;
}

STATIC WAMPREALM *wamp_RealmByName(const char *name, bool create=false)
{
	WAMPREALM *realm = (WAMPREALM*)spmap_GetValue(realm_Map(), name);

	if (!realm && create) {
		realm = realm_New(name);
		spmap_Add(realm_Map(), name, (void*)realm);
	}

	return realm;
//	return (WAMPREALM*)spmap_GetValue(realm_Map(), "spider");				/// TODO: KLUDGE!!!
//	return (WAMPREALM*)spmap_GetValue(realm_Map(), name);
}

API int wamp_SetRealm(WAMP *wamp, const char *name)
{
	WAMPREALM *realm = NULL;

	if (wamp) {
		realm = wamp_RealmByName(name, 1);

		if (realm) {
			if (wamp->realm)
				sset_Remove(wamp->realm->wamps, wamp->name);
			wamp->realm = realm;
			sset_Add(realm->wamps, wamp->name);
		}
	}

	return !!realm;
}

STATIC int wamp_Validate(WAMP *wamp, int type, JSON *json)
// Checks the validity of the message, sending an error back if there is a problem
// If there is a problem, an error will have been sent to 'wamp' before returning.
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
		{WAMP_PUBLISHED,		3,	4,	"IIII"},
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
		{WAMP_RESULT,			3,	5,	"IIOAO"},

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
				wamp_SendInternalError(wamp, 0, 0, errInvalidArgument, msg);
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
					wamp_SendInternalError(wamp, 0, 0, errInvalidArgument, msg);
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

API WAMPCB_DeleteCallback wamp_OnDelete(WAMP *w, WAMPCB_DeleteCallback cb)
{
	WAMPCB_DeleteCallback previous = NULL;

	if (w) {
		previous = w->cb_ondelete;
		w->cb_ondelete = cb;
	}

	return previous;
}

API void wamp_RegisterInvokee(WAMPCB_Invokee cb)
{
	wampcb_Invokee = cb;
}

char *mystrtok_r(char *s, char c, char **next)
// strtok_r doesn't seem to want to return empty components and we need that functionality
// It's slightly different as 's2' is actually just a char as that's all I need
{
	char *result = s;
	if (!result) result = *next;
	if (result) {
		char *end = strchr(result, c);

		if (end) *end++ = '\0';
		*next = end;
	}

	return result;
}

STATIC int wamp_WildcardMatch(const char *wildcard, const char *match)
// Taking a wildcard such as mtxml..thing and a match such as mtxml.specific.thing, returns 1 for a match or 0 for not
{
	int matched = 1;

	char *next1;
	char *next2;

	char *wildcardCopy = strdup(wildcard);
	char *matchCopy = strdup(match);
	char *w = wildcardCopy;
	char *m = matchCopy;
	for (;;) {
		const char *wildcardComponent = mystrtok_r(w, '.', &next1);
		const char *matchComponent = mystrtok_r(m, '.', &next2);

		if (!wildcardComponent && !matchComponent)									// We've matched
			break;

		if (!wildcardComponent || !matchComponent) {								// One has ended early
			matched = 0;
			break;
		}

		if (*wildcardComponent && strcmp(wildcardComponent, matchComponent)) {		// A component differs
			matched = 0;
			break;
		}

		w = NULL;
		m = NULL;
	}

	free(wildcardCopy);
	free(matchCopy);

	return matched;
}

STATIC int wamp_RegexMatch(const char *expression, const char *match)
// Taking a regex such as appointment\..*\.booked and a match such as mtxml.specific.thing, returns 1 for a match or 0 for not
// Calling with a NULL expression will used the previous expression.  This can be used to avoid re-compiling an expression
// several times when comparing against a number of strings.
{
	int matched = 0;
	int reti = 0;
	static int firstTime = 1;				// 1 only on the first call
	static regex_t regex;

	if (expression) {
		if (!firstTime) regfree(&regex);
		else firstTime = 0;

		reti = regcomp(&regex, expression, REG_ICASE);
	}

	if (!reti) {
		reti = regexec(&regex, match, 0, NULL, 0);
		if (!reti) {
			matched = 1;
		}
	}

	return matched;
}

STATIC int wamp_AddCaller(const char *callerWampName, long long callerRequestId, const char *calleeWampName, long long calleeRequestId, wamp_invokee_t *invokee, bool wantsYield=false)
// Adds a caller to those currently known.
//
// callerWampName	- WAMP name of the caller
// callerRequestId	- request ID passed by the caller
// calleeWampName	- WAMP name of the callee
// calleeRequestId	- request ID passed to the callee
// invokee			- Invokee information (wamp_invokee_t)
//
// Returns	0	Ok
//			1	requestId was already known
{
	if (!activeCallMap)
		activeCallMap = idmap_New();

	wamp_call_t *callInfo = (wamp_call_t *)idmap_GetValue(activeCallMap, calleeRequestId);

	if (callInfo)
		return 1;

	callInfo = NEW(wamp_call_t, 1);
	callInfo->callerWampName = strdup(callerWampName);
	callInfo->callerRequestId = callerRequestId;
	callInfo->calleeWampName = strdup(calleeWampName);
	callInfo->calleeRequestId = calleeRequestId;
	callInfo->invokee = invokee;
	callInfo->wantsYield = wantsYield;

	idmap_Add(activeCallMap, calleeRequestId, callInfo);
//Log("WA: Added caller %s->%s - Current callers = %d", callerWampName, calleeWampName, idmap_Count(activeCallMap));

	return 0;
}

STATIC void wamp_DeleteCallInfo(wamp_call_t *callInfo)
// Safely deletes an active call info structure
{
	if (callInfo) {
		idmap_DeleteKey(activeCallMap, callInfo->calleeRequestId);

		szDelete(callInfo->callerWampName);
		szDelete(callInfo->calleeWampName);

		free((char*)callInfo);
	}
}

STATIC void wamp_CallWampGone(const char *wampName)
// A wamp has gone away (been deleted, channel gone etc.) so we need to tidy up the caller information for any current
// calls where this wamp is either the caller or callee.
{
//Log("Checking for %s existing in activeCallMap (%p)", wampName, activeCallMap);
	if (activeCallMap && wampName) {
		idmap_Reset(activeCallMap);
		long long calleeRequestId;
		wamp_call_t *callInfo;

		while (idmap_GetNextEntry(activeCallMap, &calleeRequestId, (void**)&callInfo)) {
			if (!strcmp(callInfo->callerWampName, wampName)) {				// Caller has gone - do we inform the callee somehow?
//Log("Caller '%s' has gone", wampName);
				if (!strcmp(callInfo->calleeWampName, wampName)) {			// Both have gone (call was back to itself)
//Log("Caller/callee '%s' has gone", wampName);
				}
				wamp_DeleteCallInfo(callInfo);
			} else if (!strcmp(callInfo->calleeWampName, wampName)) {		// Callee has gone - send error to caller
				WAMP *caller = wamp_ByName(callInfo->callerWampName);

				if (caller) {
					wamp_SendInternalError(caller, WAMP_CALL, callInfo->callerRequestId, "wamp.error.call.failed", strdup("Callee has gone away"));
				}
				wamp_DeleteCallInfo(callInfo);
			}
		}
	}
}

STATIC const char *wamp_FindCaller(long long requestId, long long *pcallerRequestId, bool *pisYield, bool del=true)
// Finds the callee information in the active call map and returns the caller ID, removing the information at the same time.
// Returns a copy of the caller Wamp name on the heap
// If pcallerRequestId is Non-NULL, it receives a copy of the callerRequestID
// pisYield isn't so bad as it sounds - it receives whether the original invoke was called with 'wantsYield' set
// If del, then removes it as well
{
	const char *result = NULL;

	if (activeCallMap) {
		wamp_call_t *callInfo = (wamp_call_t *)idmap_GetValue(activeCallMap, requestId);

		if (callInfo) {
			result = strdup(callInfo->callerWampName);
			if (pcallerRequestId)
				*pcallerRequestId = callInfo->callerRequestId;
			if (pisYield)
				*pisYield = callInfo->wantsYield;

			if (del)
				wamp_DeleteCallInfo(callInfo);
		}
	}

	return result;
}

STATIC void wamp_RemoveInvokee(WAMP *wamp, wamp_callee_t *callee, int index)
// Removes the indexed invokee from the callee structure
// NB. The callee may be deleted by this call!
{
	if (!wamp || !callee || index < 0 || index >= callee->nInvokees)
		return;

	WAMPREALM *realm = wamp->realm;
	long long registrationId = callee->registrationId;

	JSON *publishArgs = json_NewObject();
	json_ObjectAddStringz(publishArgs, "uri", callee->procedure);
	json_ObjectAddInteger(publishArgs, "session", wamp->sessionId);
	json_ObjectAddInteger(publishArgs, "registration", registrationId);
	wamp_PublishIn(wamp, "wamp.registration.on_unregister", NULL, publishArgs);

//Log("Removing invokee %d for %s", index, callee->procedure);
	if (index < callee->nInvokees-1) {
		memmove(callee->invokee+index+1, callee->invokee+index, (callee->nInvokees-1-index)*sizeof(*callee->invokee));
	}
	callee->nInvokees--;

	if (callee->nInvokees) {
		// Reset the highest priority
		int highestPriority = INT_MIN;
		int i;
		for (i=0;i<callee->nInvokees;i++) {
			if (callee->invokee[i]->priority > highestPriority)
				highestPriority = callee->invokee[i]->priority;
		}
		callee->highestPriority = highestPriority;
	} else {												// No invokees so we want to forget the procedure completely
		// Look for the callee in the three maps
		const char *procedure = callee->procedure;
		SPMAP *map = NULL;

		if (!map) {
			if (callee == spmap_GetValue(realm->calleeMapExact, procedure))
				map = realm->calleeMapExact;
		}
		if (!map) {
			if (callee == spmap_GetValue(realm->calleeMapPrefix, procedure))
				map = realm->calleeMapPrefix;
		}
		if (!map) {
			if (callee == spmap_GetValue(realm->calleeMapWildcard, procedure))
				map = realm->calleeMapWildcard;
		}
		if (!map) {
			if (callee == spmap_GetValue(realm->calleeMapRegex, procedure))
				map = realm->calleeMapRegex;
		}

		JSON *publishArgs = json_NewObject();
		json_ObjectAddStringz(publishArgs, "uri", procedure);
		json_ObjectAddInteger(publishArgs, "session", wamp->sessionId);
		json_ObjectAddInteger(publishArgs, "registration", registrationId);
		wamp_PublishIn(wamp, "wamp.registration.on_delete", NULL, publishArgs);

		// At this point, we should have the map set correctly
		if (map) {
			spmap_DeleteKey(map, procedure);

			szDelete(callee->procedure);
			szDelete(callee->timestamp);
			free((char*)callee);
		}
	}
}

STATIC int wamp_ServerUnregister(WAMP *wamp, long long registrationId)
// Removes any callee registration with the given ID for the wamp
// Returns 1 if the subscription was removed, 0 if it wasn't there
{
	int done = 0;

	if (wamp) {
		WAMPREALM *realm = wamp->realm;

		SPMAP *maps[WAMP_MATCH_COUNT];
		maps[0] = realm->calleeMapExact;
		maps[1] = realm->calleeMapPrefix;
		maps[2] = realm->calleeMapWildcard;
		maps[3] = realm->calleeMapRegex;

		int i;
		for (i=0; i<3; i++) {
			SPMAP *map = maps[i];

			if (map) {
				const char *name;
				wamp_callee_t *callee;
				spmap_Reset(map);

				while(!done && spmap_GetNextEntry(map, &name, (void**)&callee)) {
					int j;

					if (callee->registrationId == registrationId) {
						for (j=0;j<callee->nInvokees;j++) {
							wamp_invokee_t *invokee = callee->invokee[j];

							if (!strcmp(invokee->wampName, wamp->name)) {

								wamp_RemoveInvokee(wamp, callee, j);
								done = 1;
								break;
							}
						}
					}
				}
			}
		}
	}

	return done;
}

STATIC void wamp_RemoveFromCalleeMap(WAMP *wamp, SPMAP *map)
// If the wamp appears as the value in the map passed, remove it.
{
//Log("Looking for %s in map %p", wamp->name, map);
	if (wamp && map) {
		const char *name;
		wamp_callee_t *callee;

		// Keep looking for registration IDs that belong to this caller and remove them.
		int foundRegistrationId = 1;					// Pretend we've found one to start the loop off
		while (foundRegistrationId) {
			foundRegistrationId = 0;
			spmap_Reset(map);
			while(spmap_GetNextEntry(map, &name, (void**)&callee)) {
				int i;

				for (i=0;i<callee->nInvokees;i++) {
					wamp_invokee_t *invokee = callee->invokee[i];
					if (!strcmp(wamp->name, invokee->wampName)) {
						wamp_RemoveInvokee(wamp, callee, i);
						foundRegistrationId=1;
						break;
					}
				}
			}
		}
	}
}

STATIC void wamp_RemoveFromMaps(WAMP *wamp)
// Removes the wamp from any callee or subscription maps that it's in
{
	if (wamp) {
//Log("Removing %s from maps", wamp->name);
		if (wamp->realm) {
			WAMPREALM *realm = wamp->realm;
			const char *wampName = wamp->name;

			sset_Remove(realm->wamps, wampName);

			// Callees is quite straightforward, look for it in each of the three maps and delete any found
			wamp_RemoveFromCalleeMap(wamp, realm->calleeMapExact);
			wamp_RemoveFromCalleeMap(wamp, realm->calleeMapPrefix);
			wamp_RemoveFromCalleeMap(wamp, realm->calleeMapWildcard);
			wamp_RemoveFromCalleeMap(wamp, realm->calleeMapRegex);

			// Note that we don't have a list of subscriptions that this wamp has so we need to go through the
			// entire list in the realm and try to remove it from each one.  This shouldn't be too bad as it
			// only happens infrequently but perhaps we should hold a list of subsrciptions for each wamp to
			// make it nicer.

			if (realm->allSubscriptions) {
				idmap_Reset(realm->allSubscriptions);
				long long subscriptionId;
				subscription_t *sub;
				while (idmap_GetNextEntry(realm->allSubscriptions, &subscriptionId, (void**)&sub)) {
					wamp_RemoveWampFromSubscription(sub, wampName);
				}
			}
		}

		wamp_CallWampGone(wamp->name);							// Remove from any active calls
	}
}

STATIC wamp_callee_t *wamp_CalleeByRegistrationId(WAMP *wamp, long long registrationId)
// Finds a callee by the registration ID or NULL if not found
{
	int done = 0;

	if (wamp) {
		WAMPREALM *realm = wamp->realm;

		SPMAP *maps[WAMP_MATCH_COUNT];
		maps[0] = realm->calleeMapExact;
		maps[1] = realm->calleeMapPrefix;
		maps[2] = realm->calleeMapWildcard;
		maps[3] = realm->calleeMapRegex;

		int i;
		for (i=0; i<3; i++) {
			SPMAP *map = maps[i];

			if (map) {
				const char *name;
				wamp_callee_t *callee;
				spmap_Reset(map);

				while(!done && spmap_GetNextEntry(map, &name, (void**)&callee)) {
					if (callee->registrationId == registrationId) {
						return callee;
					}
				}
			}
		}
	}

	return NULL;
}

STATIC wamp_callee_t *wamp_FindCallee(WAMP *wamp, const char *procedure)
{
	wamp_callee_t *result = NULL;
	WAMPREALM *realm = wamp->realm;

	result = (wamp_callee_t*)spmap_GetValue(realm->calleeMapExact, procedure);

	if (!result) {																// Not found, Check for a prefix match
		const char *name;
		void *value;

		spmap_Reset(realm->calleeMapPrefix);
		while(spmap_GetNextEntry(realm->calleeMapPrefix, &name, &value)) {
//Log("Comparing '%s' against '%s' for %d", procedure, name, strlen(name));
			if (!strncmp(procedure, name, strlen(name))) {
				result = (wamp_callee_t*)value;
				break;
			}
		}
	}

	if (!result) {																// Not found, Check for a wildcard match
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

	if (!result) {																// Not found, Check for a regex match
		const char *name;
		void *value;
		const char *copyProc = procedure;

		spmap_Reset(realm->calleeMapRegex);
		while(spmap_GetNextEntry(realm->calleeMapRegex, &name, &value)) {
			if (wamp_RegexMatch(copyProc, name)) {
				result = (wamp_callee_t*)value;
				break;
			}
			copyProc = NULL;
		}
	}

	if (!result && wampcb_NoCallee) {
		int whatToDo = (*wampcb_NoCallee)(procedure);							// Give the callback chance to register one
//Log("WA: NO-CALLEE returned %d", whatToDo);

		if (whatToDo) {															// It says it might work if you try again
			WAMPCB_NoCallee temp = wampcb_NoCallee;								// Stop us repeatedly calling
			wampcb_NoCallee = NULL;

//Log("WA: trying to find callee for %s again", procedure);
			result = wamp_FindCallee(wamp, procedure);							// Try again
//Log("WA: Second attempt at finding callee = %d", result);

			wampcb_NoCallee = temp;
		}
	}

	return result;
}

STATIC wamp_invokee_t *wamp_FindInvokee(WAMP *wamp, const char *procedure, long long *registrationIdp)
// Returns an invokee given the requested call.  This takes into account the invocation policy (roundrobin etc.)
{
	wamp_callee_t *callee = wamp_FindCallee(wamp, procedure);

//Log("Looking for invokee for '%s' - found %p", procedure, callee);
	if (!callee) return NULL;							// A procedure has not been registered for this at all

	long long registrationId = callee->registrationId;
	int highestPriority = callee->highestPriority;		// We only want something matching this
	int nInvokees = callee->nInvokees;
	int breakPlease = 0;								// Logic says to stop looking
	int roundRobinCount = callee->roundRobinCount;		// The current robin
	int randomCount = 0;								// The number of possibilities
	int i;
	wamp_invokee_t *firstInvokee = NULL;				// Used in round-robin logic

	wamp_invokee_t *invokee = NULL;						// This will be the one we eventually pick

	for (i=0; i<nInvokees; i++) {
		wamp_invokee_t *thisInvokee = callee->invokee[i];
		if (thisInvokee->priority != highestPriority)
			continue;

		invokee=thisInvokee;							// Potentially the one we want

		switch (callee->invokePolicy) {
			case WAMP_INVOKE_SINGLE:						// Take the only one we'll find
				breakPlease = 1;
				break;
			case WAMP_INVOKE_ROUNDROBIN:
				roundRobinCount--;
				if (roundRobinCount == 0) {				// We've found the one we want
					breakPlease = 1;
					break;
				}
				if (!firstInvokee) firstInvokee=invokee;	// In case we wrap around
				break;
			case WAMP_INVOKE_RANDOM:
				randomCount++;
				break;
			case WAMP_INVOKE_FIRST:						// We want the first we find
				breakPlease = 1;
				break;
			case WAMP_INVOKE_LAST:						// Keep going, we'll take the last we found
				break;
		}
		if (breakPlease)
			break;
	}

	if (callee->invokePolicy == WAMP_INVOKE_ROUNDROBIN) {
		if (roundRobinCount) {							// Didn't find one
			invokee = firstInvokee;
			callee->roundRobinCount = 1;
		}
		callee->roundRobinCount++;
	} else if (callee->invokePolicy == WAMP_INVOKE_RANDOM) {
		if (randomCount) {
			long choice = ((mrand48() & 0xffff) * randomCount) >> 16;		// Random(0..randomCount)
//Log("WA: Have invokees(%d), chosen %d", randomCount, choice+1);

			for (i=0; i<nInvokees; i++) {
				invokee = callee->invokee[i];

				if (invokee->priority == highestPriority) {
					if (!choice)
						break;
					choice--;
				}
			}
		}
	}

	if (invokee && registrationIdp) *registrationIdp = registrationId;

	return invokee;
}

STATIC void DeleteClientSubscription(WAMP *wamp, client_subscription_t *sub)
{
	if (wamp && sub) {
		idmap_DeleteKey(wamp->clientSubscriptionsReq, sub->requestId);
		idmap_DeleteKey(wamp->clientSubscriptionsSub, sub->subscriptionId);
		idmap_DeleteKey(wamp->clientSubscriptionsUnreq, sub->unrequestId);

		szDelete(sub->topic);
		free((char*)sub);
	}
}

STATIC void DeleteClientRegistration(WAMP *wamp, client_registration_t *reg)
{
	if (wamp && reg) {
		idmap_DeleteKey(wamp->clientRegistrationsReq, reg->requestId);
		idmap_DeleteKey(wamp->clientRegistrationsReg, reg->registrationId);
		idmap_DeleteKey(wamp->clientRegistrationsUnreq, reg->requestId);

		szDelete(reg->procedure);
		free((char*)reg);
	}
}

STATIC void DeleteClientPublish(WAMP *wamp, client_publish_t *pub)
{
	if (wamp && pub) {
		idmap_DeleteKey(wamp->clientPublishes, pub->requestId);
		szDelete(pub->topic);
		free((char*)pub);
	}
}

STATIC void DeleteClientCall(WAMP *wamp, client_call_t *call)
{
	if (wamp && call) {
		idmap_DeleteKey(wamp->clientCalls, call->requestId);
		szDelete(call->procedure);
		free((char*)call);
	}
}

STATIC void wamp_NoInvokee(WAMP *wamp, long long requestId, const char *procedure, JSON *list, JSON *dict)
// Called when we've been invoked but we haven't a function registered to handle it
{
	wamp_SendError(wamp, WAMP_INVOCATION, requestId, NULL, errNoSuchProcedure, list, dict);
}

STATIC void wamp_ReadTestaments(WAMP *w, std::string bucket, bool publish)
// Reads over (and removes) the testaments
// If 'publish' then sends publishes them as well
{
//Log("reading testaments for %s", bucket.c_str());
	testamentCollection::iterator it = w->testaments->find(bucket);
	if (it != w->testaments->end()) {
//Log("We have something to say...");
		testamentBucket bucket = it->second;

		for (unsigned int i=0; i<bucket.size(); i++) {
//Log("Speaker %d...", i);
			JSON *testament = bucket[i];
			if (publish) {
				const char *topic = json_ArrayStringzAt(testament, 0);
				JSON *list = json_ArrayAt(testament, 1);
				JSON *dict = json_ArrayAt(testament, 2);
				JSON *opts = json_ArrayAt(testament, 3);

//Log("Publishing topic %s", topic);
				wamp_PublishIn(w, topic, opts, list, dict);
			}

			json_Delete(testament);
		}

		w->testaments->erase(it);
	}
}

API void wamp_Delete(WAMP *w)
{
	if (w) {
		if (w->deleting) return;
		w->deleting = 1;

		Log("WA: Deleting %s (socket = %s)", wamp_Name(w), wamp_ConnectionName(w));
		wamp_ReadTestaments(w, "detached", true);
		wamp_ReadTestaments(w, "destroyed", true);

		if (w->cb_ondelete)
			(w->cb_ondelete)(w);

		wamp_RemoveFromMaps(w);

		switch (w->connectionType) {
			case WAMP_CONN_WEBSOCKET:							// Websocket
				ws_Delete(w->connection.ws);
				break;
			case WAMP_CONN_RAWSOCKET:							// Rawsocket
				break;
			case WAMP_CONN_CODE:								// Code
				break;
		}

		// Remove any subscriptions we have
		client_subscription_t *sub = (client_subscription_t *)idmap_GetValueAtIndex(w->clientSubscriptionsReq, 0);
		while (sub) {
			DeleteClientSubscription(w, sub);
			sub = (client_subscription_t *)idmap_GetValueAtIndex(w->clientSubscriptionsReq, 0);
		}
		idmap_Delete(w->clientSubscriptionsReq);
		idmap_Delete(w->clientSubscriptionsSub);
		idmap_Delete(w->clientSubscriptionsUnreq);

		client_publish_t *pub = (client_publish_t *)idmap_GetValueAtIndex(w->clientPublishes, 0);
		while (pub) {
			DeleteClientPublish(w, pub);
			pub = (client_publish_t *)idmap_GetValueAtIndex(w->clientPublishes, 0);
		}

		if (w->name)
			spmap_DeleteKey(allWamps, w->name);

		wamp_RemoveFromSession(w);

		szDelete(w->peer);

		szDelete(w->authId);
		szDelete(w->authRole);
		szDelete(w->authMethod);
		szDelete(w->authProvider);

		delete w->testaments;

		idmap_Delete(w->handlers);
		szDelete(w->name);
		free((char*)w);
	}
}

STATIC int wamp_HelloResponse(WAMP *wamp, const char *realm, JSON *opts)
// Does the work of 'helloing' a wamp connection, which is useful if we want to create an already registered
// channel.  E.g. Connecting a legacy API handler.
// Returns	0	Everything went swimmingly
//			1	Was already in a realm
//			2	Realm didn't accept the connection (no such realm)
//			3	Was already in a session
{
	if (wamp->realm) return 1;
	wamp->realm = wamp_RealmByName(realm, 1);			// The '1' means it'll auto-create new realms, 0 means spider only
	if (!wamp->realm) return 2;

	if (wamp->sessionId) return 3;

	wamp_AddToSession(wamp);

	return 0;
}

STATIC int wamp_MatchType(const char *match)
// returns	WAMP_MATCH_EXACT		match == "exact"
// returns	WAMP_MATCH_PREFIX	match == "prefix"
// returns	WAMP_MATCH_WILDCARD	match == "wildcard"
// returns	WAMP_MATCH_REGEX		match == "regex"
// returns	0	anything else
{
	int result = 0;

	if (match) {
		if (!strcmp(match, "exact"))			{ result = WAMP_MATCH_EXACT;
		} else if (!strcmp(match, "prefix"))	{ result = WAMP_MATCH_PREFIX;
		} else if (!strcmp(match, "wildcard"))	{ result = WAMP_MATCH_WILDCARD;
		} else if (!strcmp(match, "regex"))		{ result = WAMP_MATCH_REGEX;
		}
	}

	return result;
}

STATIC subscription_t *wamp_FindSubscriptionForType(WAMPREALM *realm, int matchType, const char *topic, SPMAP **pmap)
// Looks for a subscription for the match type (1..4) and topic name.
// *pmap gets a spmap*, which will always be set to a valid map unless we're returning NULL
// Returns	NULL			The topic isn't registered (under this type)
//			subscription_t*	A Pointer to the relevant subscription structure
{
	subscription_t *result = NULL;
//Log("wamp_FindSubscriptionForType(%p, %d, %s, %p)", realm, matchType, topic, pmap);

	if (realm && topic) {
		SPMAP **my_pmap = &realm->subMapExact;

		switch (matchType) {
			case WAMP_MATCH_EXACT:		my_pmap = &realm->subMapExact;		break;
			case WAMP_MATCH_PREFIX:		my_pmap = &realm->subMapPrefix;		break;
			case WAMP_MATCH_WILDCARD:	my_pmap = &realm->subMapWildcard;	break;
			case WAMP_MATCH_REGEX:		my_pmap = &realm->subMapRegex;		break;
		}

		if (my_pmap) {
			if (!*my_pmap)
				*my_pmap = spmap_New();

			result = (subscription_t*)spmap_GetValue(*my_pmap, topic);
			if (pmap) *pmap = *my_pmap;
		}
	}

	return result;
}

STATIC wamp_callee_t *wamp_FindCalleeForType(WAMPREALM *realm, int matchType, const char *procedure, SPMAP **pmap)
// Looks for a callee for the match type (1..4) and procedure name.
// *pmap gets a spmap*, which will always be set to a valid map unless we're returning NULL
// Returns	NULL			The procedure isn't registered (under this type)
//			wamp_callee_t*	A Pointer to the relevant callee structure
{
	wamp_callee_t *result = NULL;

	if (realm && procedure) {
		SPMAP **my_pmap = NULL;

		switch (matchType) {
			case WAMP_MATCH_EXACT:		my_pmap = &realm->calleeMapExact;		break;
			case WAMP_MATCH_PREFIX:		my_pmap = &realm->calleeMapPrefix;		break;
			case WAMP_MATCH_WILDCARD:	my_pmap = &realm->calleeMapWildcard;	break;
			case WAMP_MATCH_REGEX:		my_pmap = &realm->calleeMapRegex;		break;
		}

		if (my_pmap) {
			if (!*my_pmap)
				*my_pmap = spmap_New();

			result = (wamp_callee_t*)spmap_GetValue(*my_pmap, procedure);
			if (pmap) *pmap = *my_pmap;
		}
	}

	return result;
}
API const char *wamp_DecodeMatchInvokePriority(char *name, const char *match, const char *invoke, const char *priority, int *pmatch, int *pinvoke, int *ppriority)
// Decodes the name and match/invoke/priority strings to integers.
// Returns string with an error message on the heap, or NULL on success.
// If an error string is returned, pmatch, pinvoke and ppriority are undefined.
// If called for topics, you should check that *pinvoke and *ppriority are 0 on return meaning they were not given.
// NB. name may be modified by the call if match is NULL and name is, for example, "appt.*.drdare"
{
	int invokePolicy = 0;
	if (invoke) {
		if (!strcmp(invoke, "single"))
			invokePolicy = WAMP_INVOKE_SINGLE;
		else if (!strcmp(invoke, "roundrobin"))
			invokePolicy = WAMP_INVOKE_ROUNDROBIN;
		else if (!strcmp(invoke, "random"))
			invokePolicy = WAMP_INVOKE_RANDOM;
		else if (!strcmp(invoke, "first"))
			invokePolicy = WAMP_INVOKE_FIRST;
		else if (!strcmp(invoke, "last"))
			invokePolicy = WAMP_INVOKE_LAST;
		else {
			return hprintf(NULL, "Invoke type '%s' invalid (must be \"single\", \"roundrobin\", \"random\", \"first\" or \"last\")", invoke);
		}
	}
	if (pinvoke) *pinvoke = invokePolicy;

	int matchType = WAMP_MATCH_EXACT;
	if (match) {
		matchType = wamp_MatchType(match);
		if (!matchType)
			return hprintf(NULL, "Match type '%s' invalid (must be \"exact\", \"prefix\", \"wildcard\" or \"regex\"", match);
	} else {
		matchType = wamp_MatchTypeFromUri(name, NULL);
	}
	if (pmatch) *pmatch = matchType;

	long long priorityLevel = 0;
	if (priority) {
		if (!sscanf(priority, "%lld", &priorityLevel)) {
			return hprintf(NULL, "Invalid priority '%s' (must be an integer)", priority);
		}
	}
	if (ppriority) *ppriority = priorityLevel;

	return NULL;
}

STATIC const char *wamp_RegisterAnyCallee(WAMPREALM *realm, WAMP *wamp, const char *match, const char *invoke, long long priority, const char *procedure, WAMPCB_Invokee invokee, long long *registrationIdp)
// To register an internal invokee...
// realm		realm as named in calling wamp_RegisterInternalCallee()
// wamp			NULL
// match		"exact", "prefix" or "wildcard"
// invoke		"single", "roundrobin", "random", "first" or "last"
// priority		-INF .. 0 .. +INF
// procedure	Named procedure (or prefix etc)
// invokee		Callback procedure to handle
//
// To register an invokee on a WAMP
// realm		realm from the wamp
// wamp			The wamp as registered
// match		"exact", "prefix" or "wildcard"
// invoke		"single", "roundrobin", "random", "first" or "last"
// priority		-INF .. 0 .. +INF
// procedure	NULL
// invokee		Callback procedure to handle
//
// Returns	NULL	All OK
//			char*	Error message (treat as static)

{
Log("WA: wamp_RegisterAnyCallee(%p, %p, %s, %s, %lld, %s, %p, %p)", realm, wamp, match, invoke, priority, procedure, invokee,  registrationIdp);
	if (!realm) return "Realm not known";

	int invokePolicy = 0;
	if (!strcmp(invoke, "single"))
		invokePolicy = WAMP_INVOKE_SINGLE;
	else if (!strcmp(invoke, "roundrobin"))
		invokePolicy = WAMP_INVOKE_ROUNDROBIN;
	else if (!strcmp(invoke, "random"))
		invokePolicy = WAMP_INVOKE_RANDOM;
	else if (!strcmp(invoke, "first"))
		invokePolicy = WAMP_INVOKE_FIRST;
	else if (!strcmp(invoke, "last"))
		invokePolicy = WAMP_INVOKE_LAST;
	else {
		return "Invoke type not known (must be \"single\", \"roundrobin\", \"random\", \"first\" or \"last\"";
	}

	int matchType = wamp_MatchType(match);
	if (!matchType)
		return "Match type not known (must be \"exact\", \"prefix\", \"wildcard\" or \"regex\")";

	SPMAP *map;
	wamp_callee_t *callee = wamp_FindCalleeForType(realm, matchType, procedure, &map);

	if (invokePolicy == WAMP_INVOKE_SINGLE && callee != NULL)		// Policy is single, but we've already got one
		return "Invoke type is \"single\" and the procedure is already registered";

	long long registrationId = 0L;
	if (!callee) {													// Not had registration for this before
		registrationId = wamp_RandomId();
		callee = NEW(wamp_callee_t, 1);
		callee->procedure = strdup(procedure);						// So we can find the map entry for deletion
		callee->timestamp = strdup(TimeNow());						// Just for admin purposes
		callee->matchType = matchType;								// Only so we can report what this is
		callee->nInvokees = 0;
		callee->invokePolicy = invokePolicy;
		callee->highestPriority = priority;
		callee->invokee = NEW(wamp_invokee_t *, 1);
		callee->roundRobinCount = 1;
		callee->registrationId = registrationId;

		spmap_Add(map, procedure, (void*)callee);					// Add it so we'll find it next time

		JSON *publishArgs = json_NewObject();

		JSON *registrationDetails = json_NewObject();
		json_ObjectAddInteger(registrationDetails, "id", registrationId);
		json_ObjectAddStringz(registrationDetails, "created", TimeNow());
		json_ObjectAddStringz(registrationDetails, "uri", procedure);
		json_ObjectAddStringz(registrationDetails, "match", match);
		json_ObjectAddStringz(registrationDetails, "invoke", invoke);

		json_ObjectAddInteger(publishArgs, "session", wamp ? wamp->sessionId : 0);
		json_ObjectAdd(publishArgs, "registrationDetails", registrationDetails);
		wamp_PublishIn(wamp, "wamp.registration.on_create", NULL, publishArgs);
	} else {
		if (callee->invokePolicy != invokePolicy) {
			return "Invocation policy does not match that of the existing registration";
		}
		registrationId = callee->registrationId;
	}

	if (priority > callee->highestPriority)
		callee->highestPriority=priority;

	wamp_invokee_t *invokeeInfo = NEW(wamp_invokee_t, 1);			// Invokee specific information
	invokeeInfo->priority = priority;
	invokeeInfo->wampName = strdup(wamp ? wamp_Name(wamp) : "internal");
	invokeeInfo->cb_invokee = invokee;

	RENEW(callee->invokee, wamp_invokee_t*, ++callee->nInvokees);	// Add to our list
	callee->invokee[callee->nInvokees-1] = invokeeInfo;

	if (registrationIdp) *registrationIdp = registrationId;

	return NULL;
}

API const char *wamp_RegisterWampCallee(WAMP *wamp, const char *match, const char *invoke, long long priority, const char *procedure, long long *registrationIdp)
{
	return wamp_RegisterAnyCallee(wamp->realm, wamp, match, invoke, priority, procedure, NULL, registrationIdp);
}

API const char *wamp_RegisterInternalCallee(const char *realmName, const char *match, const char *procedure, WAMPCB_Invokee invokee, long long *registrationIdp)
// Registers an internal invokee for a realm
// Returns		NULL	All went OK
//				char*	Error message (treat as static)
{
	WAMPREALM *realm = wamp_RealmByName(realmName, 1);

	return wamp_RegisterAnyCallee(realm, NULL, match, "single", 0, procedure, invokee, registrationIdp);
}

API void wamp_RegisterRedirect(const char *realmName, int type, WAMPCB_Handler fn)
// Sets a function that will be called for all messages of the type given for the given realm.
// If the function returns -1, the normal processing will be performed instead.
// Any other value will be returned as the result of wamp_Dispatch() - 0=ok, 1...=error
{
	WAMPREALM *realm = wamp_RealmByName(realmName, 1);

	if (realm) {
//Log("WA: Adding %p as handler for %s on \"%s\" (%p)", fn, wamp_TypeName(type), realmName, realm);
		idmap_Add(realm->redirect, (long long)type, (void*)fn);
	}
}

API void wamp_RegisterHandler(WAMP *wamp, int type, WAMPCB_Handler fn)
{
	idmap_Add(wamp->handlers, (long long)type, (void*)fn);
}

API void wamp_Ping(WAMP *wamp, int len, const char *data)
{
	if (wamp->connectionType == WAMP_CONN_WEBSOCKET)			// Only know how to ping if we are connected via a websocket
		ws_Ping(wamp->connection.ws, len, data);
}

API void wamp_Pong(WAMP *wamp, int len, const char *data)
{
	if (wamp->connectionType == WAMP_CONN_WEBSOCKET)			// Only know how to pong if we are connected via a websocket
		ws_Pong(wamp->connection.ws, len, data);
}

STATIC void wamp_SaveEvent(subscription_t *sub, long long publicationId, const char *topic, JSON *details, JSON *argList, JSON *argDict)
{
	subscription_history_t *item = NEW(subscription_history_t, 1);
	item->timestamp = strdup(TimeNow());
	item->publicationId = publicationId;
	item->topic = strdup(topic);
	item->details = json_Copy(details);
	item->argList = json_Copy(argList);
	item->argDict = json_Copy(argDict);
const char *d = json_RenderHeap(item->details, 0);
const char *al = json_RenderHeap(item->argList, 0);
const char *ad = json_RenderHeap(item->argDict, 0);
//Log("*** Setting ITEM(%p:%lld) = %s(%s, %s, %s)", item, sub->subscriptionId, topic, d, al, ad);
szDelete(ad);
szDelete(al);
szDelete(d);
//Log("*** Setting history(%p) = %p, item->prev=%p", sub, item, sub->history);

	item->next = NULL;
	item->prev = sub->history;
	if (item->prev) {
//Log("*** Setting item->prev(%p)->next(%p) = %p", item->prev, item->prev->next, item);
		item->prev->next = item;
	}
	sub->history = item;

	Log("Saving subscription ");
}

STATIC subscription_history_t *wamp_FindEventHistoryByTimestamp(subscription_t *sub, const char *timestamp)
// Returns the oldest entry in the event history list that is later than or equal to the timestamp or NULL if there are none
{
	subscription_history_t *item = NULL;

	if (sub && timestamp) {
		subscription_history_t *cursor = sub->history;

		while (cursor && strcmp(cursor->timestamp, timestamp) >= 0) {
			item = cursor;
			cursor = cursor->prev;
		}
	}

	return item;
}

STATIC subscription_history_t *wamp_FindEventHistoryByCount(subscription_t *sub, int count)
// Returns the count'th entry back in the history list or NULL if there are no entries (or count == 0)
{
	subscription_history_t *item = NULL;

	if (sub && count > 0) {
		item = sub->history;

		while (item && item->prev && --count) {
			item = item->prev;
		}
	}

	return item;
}

STATIC subscription_history_t *wamp_FindEventHistoryByPublication(subscription_t *sub, long long publicationId)
// Returns the entry in the history list with the given publicationId or NULL
{
	subscription_history_t *item = NULL;

	if (sub) {
		item = sub->history;

		while (item && item->publicationId != publicationId)
			item = item->prev;
	}

	return item;
}



/*
   If we're to add wampcra challenge authenticate, it goes like this...
   We generate a nonce value and return this...
   [ WAMP_CHALLENGE, "wampcra", {"challenge":"see below"}]
   The 'see below' is a JSON encoded string made up as follows:
   {"nonce":our_nonce_string,
    "authprovider":usernamedb,
	"authid":username,
	"timestamp":TimeNow(),
	"authrole":role,
	"authmethod":"wampcra",
	"session":sessionid}
	All the values in there are strings apart from the session id.
	The entire string is the challenge string.  The client should receive this and perform the following (as should we of course):
	K' = Pad the key (this is essentially the user password) to 256 bits with zeros
				or, if it is more than 256 bits(!), take the sha-256 hash of it
	o_key_pad = K' xor with 5c5c5c5c5c5c...
	i_key_pad = K' xor with 363636363636...
	H1 = sha-256 hash of (i_key_pad + challenge)
	H2 = sha-256 hash of (o_key_pad + H1)
	AUTH = base64 encoding of H2

	We then expect back a WAMP_AUTHENTICATE with the calculated AUTH above.

	May be...

		#include <openssl/evp.h>
		#include <openssl/hmac.h>

		unsigned char* hmac_sha256(const void *key, int keylen,
									const unsigned char *data, int datalen,
									unsigned char *result, unsigned int* resultlen)
		{
			return HMAC(EVP_sha256(), key, keylen, data, datalen, result, resultlen);
		}
 */
int wamp_HandleHello(WAMP *wamp, JSON *json)
{
	const char *realmName = json_ArrayStringzAt(json, 1);
	JSON *opts = json_ArrayAt(json, 2);

	int err = wamp_HelloResponse(wamp, realmName, opts);
	if (err) {
		switch (err) {
			case 1:					// Already in a realm (these two should always be the same)
			case 3:					// Already in a session
				wamp_Abort(wamp, errInvalidArgument, "WAMP HELLO received within a session");
				break;
			case 2:
				wamp_Abort(wamp, errNoSuchRealm, "I can only accept \"spider\" currently");
				break;
		}

		wamp_Delete(wamp);			// This will drop the connection
		return err;
	}

	long long id = wamp->sessionId;
	wamp->authenticated = true;				// Around here is where we place authentication messages...

	WAMPREALM *realm = wamp->realm;
	int canDealer = 1;
	int canBroker = !realm->wantForwardSubscriptions || (realm->wantForwardSubscriptions && realm->forwardSubscriptions);

	JSON *jResponse = wamp_NewMessage(WAMP_WELCOME, id);
	JSON *replyOpts = json_NewObject();
	char hostname[50];
	if (gethostname(hostname, sizeof(hostname)))
		strcpy(hostname, "unknown server");


	const char *agentName = hprintf(NULL, "MTWAMP on %s", hostname);
	json_ObjectAddStringz(replyOpts, "agent", agentName);
	JSON *jRoles = json_NewObject();
	if (canDealer) {
		JSON *jObj = json_ObjectAdd(jRoles, "dealer", json_NewObject());
		JSON *jFeatures = json_ObjectAdd(jObj, "features", json_NewObject());
		json_ObjectAdd(jFeatures, "progressive_call_results", json_NewBool(1));
//		json_ObjectAdd(jFeatures, "progressive_calls", json_NewBool(1));
		json_ObjectAdd(jFeatures, "call_timeout", json_NewBool(1));
		json_ObjectAdd(jFeatures, "call_cancelling", json_NewBool(1));
		json_ObjectAdd(jFeatures, "caller_identification", json_NewBool(1));
//		json_ObjectAdd(jFeatures, "call_trustlevels", json_NewBool(1));
		json_ObjectAdd(jFeatures, "registration_meta_api", json_NewBool(1));
		json_ObjectAdd(jFeatures, "pattern_based_registration", json_NewBool(1));
		json_ObjectAdd(jFeatures, "shared_registration", json_NewBool(1));
//		json_ObjectAdd(jFeatures, "sharded_registration", json_NewBool(1));
//		json_ObjectAdd(jFeatures, "registration_revocation", json_NewBool(1));
//		json_ObjectAdd(jFeatures, "procedure_reflection", json_NewBool(1));

		json_ObjectAdd(jFeatures, "session_meta_api", json_NewBool(1));

		json_ObjectAdd(jFeatures, "testament_meta_api", json_NewBool(1));

//		json_ObjectAdd(jFeatures, "payload_encryption_cryptobox", json_NewBool(1));
//		json_ObjectAdd(jFeatures, "payload_transparency", json_NewBool(1));
	}
	if (canBroker) {
		JSON *jObj = json_ObjectAdd(jRoles, "broker", json_NewObject());
		JSON *jFeatures = json_ObjectAdd(jObj, "features", json_NewObject());
		json_ObjectAdd(jFeatures, "subscriber_blackwhite_listing", json_NewBool(1));
		json_ObjectAdd(jFeatures, "publisher_exclusion", json_NewBool(1));
		json_ObjectAdd(jFeatures, "publisher_identification", json_NewBool(1));
//		json_ObjectAdd(jFeatures, "publication_trustlevels", json_NewBool(1));
		json_ObjectAdd(jFeatures, "session_meta_api", json_NewBool(1));
		json_ObjectAdd(jFeatures, "subscription_meta_api", json_NewBool(1));
		json_ObjectAdd(jFeatures, "pattern_based_subscription", json_NewBool(1));
//		json_ObjectAdd(jFeatures, "sharded_subscription", json_NewBool(1));
		json_ObjectAdd(jFeatures, "event_history", json_NewBool(1));
//		json_ObjectAdd(jFeatures, "topic_reflection", json_NewBool(1));

//		json_ObjectAdd(jFeatures, "subscription_revocation", json_NewBool(1));
//		json_ObjectAdd(jFeatures, "payload_transparency", json_NewBool(1));
//		json_ObjectAdd(jFeatures, "payload_encryption_cryptobox", json_NewBool(1));
	}

	// These wouldn't be included in the features list but are here just to complete the feature list from section 14.2 of the spec
//	json_ObjectAdd(jFeatures, "challenge_response_authentication", json_NewBool(1));
//	json_ObjectAdd(jFeatures, "cookie_authentication", json_NewBool(1));
//	json_ObjectAdd(jFeatures, "ticket_authentication", json_NewBool(1));
//	json_ObjectAdd(jFeatures, "rawsocket_transport", json_NewBool(1));
//	json_ObjectAdd(jFeatures, "batched_ws_transport", json_NewBool(1));
//	json_ObjectAdd(jFeatures, "longpoll_transport", json_NewBool(1));

	json_ObjectAdd(replyOpts, "roles", jRoles);

	json_ObjectAddStringz(replyOpts, "realm", realmName);
	json_ObjectAddStringz(replyOpts, "authid", wamp->authId);
	json_ObjectAddStringz(replyOpts, "authrole", wamp->authRole);
	json_ObjectAddStringz(replyOpts, "authmethod", wamp->authMethod);
	json_ObjectAddStringz(replyOpts, "authprovider", wamp->authProvider);

	json_ArrayAdd(jResponse, replyOpts);

	wamp_SendJsonHeap(wamp, jResponse);

	return 0;
}

STATIC int wamp_HandleWelcome(WAMP *wamp, JSON *json)
{
	long long sessionId = json_ArrayIntegerAt(json, 1);

	wamp->sessionId = sessionId;
	JSON *details = json_ArrayAt(json, 2);
	const char *authId = json_ObjectStringzCalled(details, "authid");
	const char *authRole = json_ObjectStringzCalled(details, "authrole");
	const char *authMethod = json_ObjectStringzCalled(details, "authmethod");
	const char *authProvider = json_ObjectStringzCalled(details, "authprovider");

	szDelete(wamp->authId);
	szDelete(wamp->authRole);
	szDelete(wamp->authMethod);
	szDelete(wamp->authProvider);

	wamp->authId = authId ? strdup(authId) : NULL;
	wamp->authRole = authRole ? strdup(authRole) : NULL;
	wamp->authMethod = authMethod ? strdup(authMethod) : NULL;
	wamp->authProvider = authProvider ? strdup(authProvider) : NULL;

	if (wamp->cbWelcome) {
		(*wamp->cbWelcome)(wamp, sessionId, NULL, json_ArrayAt(json, 2));
	}

	return 0;
}

STATIC int wamp_HandleGoodbye(WAMP *wamp, JSON *json)
{
	const char *uri = json_ArrayStringzAt(json, 2);

	if (strcmp(uri, errGoodbyeAndOut)) {					// Don't respond if they're saying 'goodbye and out'
		wamp_Close(wamp, errGoodbyeAndOut, NULL);
	}
	return 0;
}

STATIC int wamp_HandleRegister(WAMP *wamp, JSON *json)
{
	long long requestId = json_ArrayIntegerAt(json, 1);
	SPMAP *options = json_ArrayObjectAt(json, 2);
	const char *procedure = json_ArrayStringzAt(json, 3);

	if (!procedure || !*procedure) {
		wamp_SendErrorStr(wamp, WAMP_REGISTER, requestId, "Must have non-blank procedure name", "wamp.error.cannot_register", NULL, NULL);
		return 1;
	}

	const char *match = "exact";
	JSON *jMatch = (JSON*)spmap_GetValue(options, "match");
	if (jMatch) {
		if (!json_IsString(jMatch)) {
			wamp_SendErrorStr(wamp, WAMP_REGISTER, requestId, "Match value must be a string", "wamp.error.cannot_register", NULL, NULL);
			return 1;
		}
		match = json_AsStringz(jMatch);
	} else {
		int matchType = wamp_MatchTypeFromUri(procedure, NULL);
		if (matchType) {
			match = wamp_MatchName(matchType);
		}
	}

	const char *invoke = "single";
	JSON *jInvoke = (JSON*)spmap_GetValue(options, "invoke");
	if (jInvoke) {
		if (!json_IsString(jInvoke)) {
			wamp_SendErrorStr(wamp, WAMP_REGISTER, requestId, "Invoke value must be a string", "wamp.error.cannot_register", NULL, NULL);
			return 1;
		}
		invoke = json_AsStringz(jInvoke);
	}

	long long priority = 0;
	JSON *jPriority = (JSON*)spmap_GetValue(options, "_priority");
	if (jPriority) {
		if (!json_IsInteger(jPriority)) {
			wamp_SendErrorStr(wamp, WAMP_REGISTER, requestId, "Priority value must be an integer", "wamp.error.cannot_register", NULL, NULL);
			return 1;
		}
		priority = json_AsInteger(jPriority);
	}

	long long registrationId;
	const char *err = wamp_RegisterWampCallee(wamp, match, invoke, priority, procedure, &registrationId);
	if (err) {
		wamp_SendErrorStr(wamp, WAMP_REGISTER, requestId, err, errDomainAlreadyExists, NULL, NULL);
		return 1;
	}

	JSON *j = wamp_NewMessage(WAMP_REGISTERED, requestId);
	json_ArrayAddInteger(j, registrationId);

	wamp_SendJsonHeap(wamp, j);

	JSON *publishArgs = json_NewObject();
	json_ObjectAddStringz(publishArgs, "uri", procedure);
	json_ObjectAddInteger(publishArgs, "session", wamp->sessionId);
	json_ObjectAddInteger(publishArgs, "registration", registrationId);
	wamp_PublishIn(wamp, "wamp.registration.on_register", NULL, publishArgs);

	return 1;
}

STATIC int wamp_HandleRegistered(WAMP *wamp, JSON *json)
{
	if (wamp && json) {
		long long requestId = json_ArrayIntegerAt(json, 1);
		long long registrationId = json_ArrayIntegerAt(json, 2);

		client_registration_t *reg = (client_registration_t *)idmap_GetValue(wamp->clientRegistrationsReq, requestId);
		if (reg) {
			reg->registrationId = registrationId;
			idmap_Add(wamp->clientRegistrationsReg, registrationId, reg);

			if (reg->cbRegistered)
				(*reg->cbRegistered)(wamp, requestId, registrationId, NULL, reg->procedure);
		}
	}

	return 1;
}

STATIC int wamp_HandleUnregister(WAMP *wamp, JSON *json)
{
	long long requestId = json_ArrayIntegerAt(json, 1);
	long long registrationId = json_ArrayIntegerAt(json, 2);

	int ok = wamp_ServerUnregister(wamp, registrationId);

	if (ok) {
		JSON *j = wamp_NewMessage(WAMP_UNREGISTERED, requestId);
		wamp_SendJsonHeap(wamp, j);
	} else {
		wamp_SendErrorStr(wamp, WAMP_UNSUBSCRIBE, requestId, "You were not registered", errNoSuchRegistration, NULL, NULL);
	}

	return 1;
}

STATIC int wamp_HandleUnregistered(WAMP *wamp, JSON *json)
{
	return 1;
}

STATIC sublist_t *wamp_FindSubscribers(WAMPREALM *realm, const char *topic)
{
	if (!realm || !topic) return NULL;

	sublist_t *subs = NULL;

	if (realm->subMapExact) {											// Check for an exact match
		subscription_t *result = (subscription_t*)spmap_GetValue(realm->subMapExact, topic);
		if (result) {
			sublist_t *sub = NEW(sublist_t, 1);
			sub->sub = result;
			sub->prev = subs;
			subs = sub;
		}
	}

	if (realm->subMapPrefix) {											// Check for a prefix match
		const char *name;
		void *value;

		spmap_Reset(realm->subMapPrefix);
		while(spmap_GetNextEntry(realm->subMapPrefix, &name, &value)) {
			if (!strncmp(topic, name, strlen(name))) {
				subscription_t *result = (subscription_t*)value;

				sublist_t *sub = NEW(sublist_t, 1);
				sub->sub = result;
				sub->prev = subs;
				subs = sub;
			}
		}
	}

	if (realm->subMapWildcard) {										// Check for a wildcard match
		const char *name;
		void *value;

		spmap_Reset(realm->subMapWildcard);
		while(spmap_GetNextEntry(realm->subMapWildcard, &name, &value)) {
			if (wamp_WildcardMatch(name, topic)) {
				subscription_t *result = (subscription_t*)value;

				sublist_t *sub = NEW(sublist_t, 1);
				sub->sub = result;
				sub->prev = subs;
				subs = sub;
			}
		}
	}

	if (realm->subMapRegex) {										// Check for a regex match
		const char *name;
		void *value;
		const char *copyTopic = topic;

		spmap_Reset(realm->subMapRegex);
		while(spmap_GetNextEntry(realm->subMapRegex, &name, &value)) {
			if (wamp_RegexMatch(name, copyTopic)) {
				subscription_t *result = (subscription_t*)value;

				sublist_t *sub = NEW(sublist_t, 1);
				sub->sub = result;
				sub->prev = subs;
				subs = sub;
			}
			copyTopic = NULL;
		}
	}

	return subs;
}

STATIC int wamp_EventHistorySinceItem(WAMP *wamp, long long subscriptionId, subscription_history_t *item, long long callRequestId, JSON *options, JSON *argList, JSON *argDict)
{
	if (!item) return 1;

	const char *delivery = json_ObjectStringzCalled(argDict, "delivery");
	int asEvents = !stricmp(delivery, "events");

	if (asEvents) {						// Needs events re-sending
		JSON *event = wamp_NewMessage(WAMP_EVENT, subscriptionId);

		while (item) {
			json_ArraySetIntegerAt(event, 2, item->publicationId);
			JSON *details = json_NewObject();
			json_ObjectAddStringz(details, "topic", item->topic);
			json_ArrayAdd(event, item->details);
			AddArguments(event, item->argList, item->argDict);

			wamp_SendJson(wamp, event);

			// This bit of magic needs explanation...
			// The details, argList and argDict came from the item but currently 'belong' to the event
			// If they're replaced in the event, or the event is deleted, they'll be deleted so here we remove them from
			// the event so they return to being 'owned' by the item.
			json_ArrayTakeAt(event, 3);
			json_ArrayTakeAt(event, 3);
			json_ArrayTakeAt(event, 3);

			item=item->next;
		}

		json_Delete(event);
	} else {							// Needs returning as an array
		JSON *result = wamp_NewMessage(WAMP_RESULT, callRequestId);
		json_ArrayAdd(result, json_NewObject());
		JSON *events = json_NewArray();
		while (item) {
			JSON *event = wamp_NewMessage(WAMP_EVENT, item->publicationId);
			JSON *details = json_NewObject();
			json_ObjectAddStringz(details, "topic", item->topic);
			json_ArrayAdd(event, item->details);
			AddArguments(event, item->argList, item->argDict);

			json_ArrayAdd(events, event);

			item = item->next;
		}
		json_ArrayAdd(result, events);

		wamp_SendJson(wamp, result);
	}

	return 0;
}

STATIC int wamp_DoWampTopicHistoryLast(WAMP *wamp, long long callRequestId, JSON *options, JSON *argList, JSON *argDict)
{
	long long subscriptionId = json_ObjectIntegerCalled(argDict, "subscription", 0);
	long long limit = json_ObjectIntegerCalled(argDict, "limit", 0);

	if (!subscriptionId || limit <= 0) {
		wamp_SendInternalError(wamp, WAMP_CALL, callRequestId, errInvalidArgument, "Must have subscription and positive limit");
		return 1;
	}

	if (limit > INT_MAX) {
		wamp_SendInternalError(wamp, WAMP_CALL, callRequestId, errInvalidArgument, "limit is stupidly high");
		return 1;
	}
	int count = (int)limit;

	WAMPREALM *realm = wamp->realm;
	subscription_t *sub = (subscription_t *)idmap_GetValue(realm->allSubscriptions, subscriptionId);

	subscription_history_t *item = wamp_FindEventHistoryByCount(sub, count);

	return wamp_EventHistorySinceItem(wamp, subscriptionId, item, callRequestId, options, argList, argDict);
}

STATIC int wamp_DoWampTopicHistorySince(WAMP *wamp, long long callRequestId, JSON *options, JSON *argList, JSON *argDict)
{
	long long subscriptionId = json_ObjectIntegerCalled(argDict, "subscription", 0);
	const char *timestamp = json_ObjectStringzCalled(argDict, "timestamp");

	if (!subscriptionId || !timestamp) {
		wamp_SendInternalError(wamp, WAMP_CALL, callRequestId, errInvalidArgument, "Must have subscription and timestamp");
		return 1;
	}

	time_t test = DecodeTimeStamp(timestamp, NULL);

	if (!test) {
		wamp_SendInternalError(wamp, WAMP_CALL, callRequestId, errInvalidArgument, "Timestamp is invalid");
		return 1;
	}

	WAMPREALM *realm = wamp->realm;
	subscription_t *sub = (subscription_t *)idmap_GetValue(realm->allSubscriptions, subscriptionId);

	subscription_history_t *item = wamp_FindEventHistoryByTimestamp(sub, timestamp);

	return wamp_EventHistorySinceItem(wamp, subscriptionId, item, callRequestId, options, argList, argDict);
}

STATIC int wamp_DoWampTopicHistoryAfter(WAMP *wamp, long long callRequestId, JSON *options, JSON *argList, JSON *argDict)
{
	long long subscriptionId = json_ObjectIntegerCalled(argDict, "subscription", 0);
	long long publicationId = json_ObjectIntegerCalled(argDict, "publication", 0);

	if (!subscriptionId || !publicationId) {
		wamp_SendInternalError(wamp, WAMP_CALL, callRequestId, errInvalidArgument, "Must have subscription and publication");
		return 1;
	}

	WAMPREALM *realm = wamp->realm;
	subscription_t *sub = (subscription_t *)idmap_GetValue(realm->allSubscriptions, subscriptionId);

	subscription_history_t *item = wamp_FindEventHistoryByPublication(sub, publicationId);

	return wamp_EventHistorySinceItem(wamp, subscriptionId, item, callRequestId, options, argList, argDict);
}

STATIC int wamp_DoWampRegistrationList(WAMP *wamp, long long callRequestId, JSON *options, JSON *argList, JSON *argDict)
{
	if (!wamp) return 0;
	WAMPREALM *realm = wamp->realm;

	JSON *registrationLists = json_NewObject();
	JSON *exactList = json_NewArray();
	JSON *prefixList = json_NewArray();
	JSON *wildcardList = json_NewArray();
	JSON *regexList = json_NewArray();

	const char *name;
	wamp_callee_t *callee;
	int i;

	SPMAP *maps[4];
	maps[0] = realm->calleeMapExact;
	maps[1] = realm->calleeMapPrefix;
	maps[2] = realm->calleeMapWildcard;
	maps[3] = realm->calleeMapRegex;

	JSON *list[4];
	list[0] = exactList;
	list[1] = prefixList;
	list[2] = wildcardList;
	list[2] = regexList;

	for (i=0; i<3; i++) {
		SPMAP *map = maps[i];

		if (map) {
			spmap_Reset(map);
			while (spmap_GetNextEntry(map, &name, (void**)&callee)) {
				json_ArrayAddInteger(list[i], callee->registrationId);
			}
		}
	}

	json_ObjectAdd(registrationLists, "exact", exactList);
	json_ObjectAdd(registrationLists, "prefix", prefixList);
	json_ObjectAdd(registrationLists, "wildcard", wildcardList);
	if (spmap_Count(realm->calleeMapRegex))
		json_ObjectAdd(registrationLists, "regex", regexList);

	wamp_SendResult(wamp, callRequestId, NULL, registrationLists);

	return 1;
}

STATIC int wamp_DoWampRegistrationLookup(WAMP *wamp, long long callRequestId, JSON *options, JSON *argList, JSON *argDict)
{
	const char *procedure = json_ArrayStringzAt(argList, 0);

	if (!procedure) {
		wamp_SendInternalError(wamp, WAMP_CALL, callRequestId, errInvalidArgument, "Must have procedure");
		return 1;
	}

	const char *match = json_ObjectStringzCalled(argDict, "match");
	int matchType = wamp_MatchType(match);

	if (match && !matchType)
		wamp_SendInternalError(wamp, WAMP_CALL, callRequestId, errInvalidArgument, "Invalid match argument");

	SPMAP *map;
	WAMPREALM *realm = wamp->realm;
	wamp_callee_t *callee = wamp_FindCalleeForType(realm, matchType, procedure, &map);

	wamp_SendResult(wamp, callRequestId, NULL, callee ? json_NewInteger(callee->registrationId) : json_NewNull());

	return 0;
}

STATIC int wamp_DoWampRegistrationMatch(WAMP *wamp, long long callRequestId, JSON *options, JSON *argList, JSON *argDict)
{
	const char *procedure = json_ArrayStringzAt(argList, 0);

	if (!procedure) {
		wamp_SendInternalError(wamp, WAMP_CALL, callRequestId, errInvalidArgument, "Must have procedure");
		return 1;
	}

	long long registrationId = 0;
	wamp_invokee_t *invokee = wamp_FindInvokee(wamp, procedure, &registrationId);

	wamp_SendResult(wamp, callRequestId, NULL, invokee ? json_NewInteger(registrationId) : json_NewNull());

	return 0;
}

STATIC int wamp_DoWampRegistrationGet(WAMP *wamp, long long callRequestId, JSON *options, JSON *argList, JSON *argDict)
{
	long long registrationId = json_ArrayIntegerAt(argList, 0);

	if (!registrationId) {
		wamp_SendInternalError(wamp, WAMP_CALL, callRequestId, errInvalidArgument, "Must have registration identifier");
		return 1;
	}

	wamp_callee_t *callee = wamp_CalleeByRegistrationId(wamp, registrationId);

	if (!callee) {
		wamp_SendInternalError(wamp, WAMP_CALL, callRequestId, errNoSuchRegistration, "Registration not found");
		return 1;
	}

	JSON *result = json_NewObject();
	json_ObjectAddInteger(result, "id", callee->registrationId);
	json_ObjectAddStringz(result, "created", callee->timestamp);
	json_ObjectAddStringz(result, "uri", callee->procedure);
	json_ObjectAddStringz(result, "match", wamp_MatchName(callee->matchType));
	json_ObjectAddStringz(result, "invoke", wamp_PolicyName(callee->invokePolicy));

	wamp_SendResult(wamp, callRequestId, NULL, result);

	return 0;
}

STATIC int wamp_DoWampRegistrationListCallees(WAMP *wamp, long long callRequestId, JSON *options, JSON *argList, JSON *argDict)
{
	long long registrationId = json_ArrayIntegerAt(argList, 0);

	if (!registrationId) {
		wamp_SendInternalError(wamp, WAMP_CALL, callRequestId, errInvalidArgument, "Must have registration identifier");
		return 1;
	}

	wamp_callee_t *callee = wamp_CalleeByRegistrationId(wamp, registrationId);

	if (!callee) {
		wamp_SendInternalError(wamp, WAMP_CALL, callRequestId, errNoSuchRegistration, "Registration not found");
		return 1;
	}

	JSON *sessionList = json_NewArray();

	for (int i=0;i<callee->nInvokees;i++) {
		wamp_invokee_t *invokee = callee->invokee[i];
		const char *wampName = invokee->wampName;

		WAMP *calleeWamp = wamp_ByName(wampName);

		if (calleeWamp) {
			json_ArrayAddInteger(sessionList, calleeWamp->sessionId);
		}
	}

	JSON *result = json_NewArray();
	json_ArrayAdd(result, sessionList);

	wamp_SendResult(wamp, callRequestId, NULL, result, NULL);

	return 0;
}

STATIC int wamp_DoWampRegistrationCountCallees(WAMP *wamp, long long callRequestId, JSON *options, JSON *argList, JSON *argDict)
{
	long long registrationId = json_ArrayIntegerAt(argList, 0);

	if (!registrationId) {
		wamp_SendInternalError(wamp, WAMP_CALL, callRequestId, errInvalidArgument, "Must have registration identifier");
		return 1;
	}

	wamp_callee_t *callee = wamp_CalleeByRegistrationId(wamp, registrationId);

	if (!callee) {
		wamp_SendInternalError(wamp, WAMP_CALL, callRequestId, errNoSuchRegistration, "Registration not found");
		return 1;
	}

	JSON *result = json_NewArray();
	json_ArrayAddInteger(result, callee->nInvokees);

	wamp_SendResult(wamp, callRequestId, NULL, result, NULL);

	return 0;
}

STATIC int wamp_DoWampSubscriptionList(WAMP *wamp, long long callRequestId, JSON *options, JSON *argList, JSON *argDict)
{
	if (!wamp) return 0;
	WAMPREALM *realm = wamp->realm;

	JSON *subscriptionLists = json_NewObject();
	JSON *exactList = json_NewArray();
	JSON *prefixList = json_NewArray();
	JSON *wildcardList = json_NewArray();
	JSON *regexList = json_NewArray();

	const char *name;
	subscription_t *sub;
	int i;

	SPMAP *maps[4];
	maps[0] = realm->subMapExact;
	maps[1] = realm->subMapPrefix;
	maps[2] = realm->subMapWildcard;
	maps[3] = realm->subMapRegex;

	JSON *list[4];
	list[0] = exactList;
	list[1] = prefixList;
	list[2] = wildcardList;
	list[2] = regexList;

	for (i=0; i<3; i++) {
		SPMAP *map = maps[i];

		if (map) {
			spmap_Reset(map);
			while (spmap_GetNextEntry(map, &name, (void**)&sub)) {
				json_ArrayAddInteger(list[i], sub->subscriptionId);
			}
		}
	}

	json_ObjectAdd(subscriptionLists, "exact", exactList);
	json_ObjectAdd(subscriptionLists, "prefix", prefixList);
	json_ObjectAdd(subscriptionLists, "wildcard", wildcardList);
	if (spmap_Count(realm->calleeMapRegex))
		json_ObjectAdd(subscriptionLists, "regex", regexList);

	wamp_SendResult(wamp, callRequestId, NULL, NULL, subscriptionLists);

	return 1;
}

STATIC int wamp_DoWampSubscriptionLookup(WAMP *wamp, long long callRequestId, JSON *options, JSON *argList, JSON *argDict)
{
	const char *topic = json_ArrayStringzAt(argList, 0);

	if (!topic) {
		wamp_SendInternalError(wamp, WAMP_CALL, callRequestId, errInvalidArgument, "Must have topic");
		return 1;
	}

	const char *match = json_ObjectStringzCalled(argDict, "match");
	int matchType = wamp_MatchType(match);

	if (match && !matchType)
		wamp_SendInternalError(wamp, WAMP_CALL, callRequestId, errInvalidArgument, "Invalid match argument");

	WAMPREALM *realm = wamp->realm;
	subscription_t *sub = wamp_FindSubscriptionForType(realm, matchType, topic, NULL);

	JSON *result = json_NewArray();
	if (sub)
		json_ArrayAddInteger(result, sub->subscriptionId);
	else
		json_ArrayAddNull(result);

	wamp_SendResult(wamp, callRequestId, NULL, result, NULL);

	return 0;
}

STATIC int wamp_DoWampSubscriptionMatch(WAMP *wamp, long long callRequestId, JSON *options, JSON *argList, JSON *argDict)
{
	const char *topic = json_ArrayStringzAt(argList, 0);

	if (!topic) {
		wamp_SendInternalError(wamp, WAMP_CALL, callRequestId, errInvalidArgument, "Must have topic");
		return 1;
	}

	WAMPREALM *realm = wamp->realm;
	sublist_t *subs = wamp_FindSubscribers(realm, topic);

	JSON *result = json_NewArray();
	if (subs) {
		JSON *sublist = json_NewArray();

		while (subs) {
			json_ArrayAddInteger(sublist, subs->sub->subscriptionId);

			subs=subs->prev;
		}

		json_ArrayAdd(result, sublist);
	} else {
		json_ArrayAddNull(result);
	}

	wamp_SendResult(wamp, callRequestId, NULL, result, NULL);

	return 0;
}

STATIC int wamp_DoWampSubscriptionGet(WAMP *wamp, long long callRequestId, JSON *options, JSON *argList, JSON *argDict)
{
	long long subscriptionId = json_ArrayIntegerAt(argList, 0);

	if (!subscriptionId) {
		wamp_SendInternalError(wamp, WAMP_CALL, callRequestId, errInvalidArgument, "Must have subscription identifier");
		return 1;
	}

	subscription_t *sub = wamp_SubscriptionById(wamp, subscriptionId);

	if (!sub) {
		wamp_SendInternalError(wamp, WAMP_CALL, callRequestId, errNoSuchRegistration, "Subscription not found");
		return 1;
	}

	JSON *result = json_NewObject();
	json_ObjectAddInteger(result, "subscription", sub->subscriptionId);
	json_ObjectAddStringz(result, "created", sub->timestamp);
	json_ObjectAddStringz(result, "uri", sub->topic);
	json_ObjectAddStringz(result, "match", wamp_MatchName(sub->matchType));

	wamp_SendResult(wamp, callRequestId, NULL, result);

	return 0;
}

STATIC int wamp_DoWampSubscriptionListSubscribers(WAMP *wamp, long long callRequestId, JSON *options, JSON *argList, JSON *argDict)
{
	long long subscriptionId = json_ArrayIntegerAt(argList, 0);

	if (!subscriptionId) {
		wamp_SendInternalError(wamp, WAMP_CALL, callRequestId, errInvalidArgument, "Must have subscription identifier");
		return 1;
	}

	subscription_t *sub = wamp_SubscriptionById(wamp, subscriptionId);

	if (!sub) {
		wamp_SendInternalError(wamp, WAMP_CALL, callRequestId, errNoSuchRegistration, "Subscription not found");
		return 1;
	}

	JSON *sessionList = json_NewArray();

	const char *wampName;
	subscription_info_t *subInfo;

	spmap_Reset(sub->wamps);
	while (spmap_GetNextEntry(sub->wamps, &wampName, (void**)&subInfo)) {
		WAMP *calleeWamp = wamp_ByName(wampName);

		if (calleeWamp) {
			json_ArrayAddInteger(sessionList, calleeWamp->sessionId);
		}
	}

	JSON *result = json_NewArray();
	json_ArrayAdd(result, sessionList);

	wamp_SendResult(wamp, callRequestId, NULL, result);

	return 0;
}

STATIC int wamp_DoWampSubscriptionCountSubscribers(WAMP *wamp, long long callRequestId, JSON *options, JSON *argList, JSON *argDict)
{
	long long subscriptionId = json_ArrayIntegerAt(argList, 0);

	if (!subscriptionId) {
		wamp_SendInternalError(wamp, WAMP_CALL, callRequestId, errInvalidArgument, "Must have subscription identifier");
		return 1;
	}

	subscription_t *sub = wamp_SubscriptionById(wamp, subscriptionId);

	if (!sub) {
		wamp_SendInternalError(wamp, WAMP_CALL, callRequestId, errNoSuchRegistration, "Subscription not found");
		return 1;
	}

	wamp_SendResult(wamp, callRequestId, NULL, json_NewInteger(spmap_Count(sub->wamps)));

	return 0;
}

STATIC int wamp_DoWampSessionCount(WAMP *wamp, long long callRequestId, JSON *options, JSON *argList, JSON *argDict)
{
	wamp_SendResult(wamp, callRequestId, NULL, json_NewInteger(idmap_Count(wamp->realm->allSessions)));

	return 0;
}

STATIC int wamp_DoWampSessionList(WAMP *wamp, long long callRequestId, JSON *options, JSON *argList, JSON *argDict)
{
	JSON *result = json_NewArray();
	JSON *sessions = json_NewArray();

	idmap_Reset(wamp->realm->allSessions);
	long long sessionId;
	while (idmap_GetNextEntry(wamp->realm->allSessions, &sessionId, NULL)) {
		json_ArrayAddInteger(sessions, sessionId);
	}

	json_ArrayAdd(result, sessions);

	wamp_SendResult(wamp, callRequestId, NULL, result);

	return 0;
}

STATIC int wamp_DoWampSessionGet(WAMP *wamp, long long callRequestId, JSON *options, JSON *argList, JSON *argDict)
{
	long long sessionId = json_ArrayIntegerAt(argList, 0);

	if (!sessionId) {
		wamp_SendInternalError(wamp, WAMP_CALL, callRequestId, errInvalidArgument, "Must have session identifier");
		return 1;
	}

	WAMP *sessionWamp = wamp_BySession(wamp->realm, sessionId);
	if (!sessionWamp) {
		wamp_SendInternalError(wamp, WAMP_CALL, callRequestId, errNoSuchSession, "Session not found");
		return 1;
	}

	wamp_SendResult(wamp, callRequestId, NULL, wamp_SessionObject(sessionWamp));

	return 0;
}

STATIC int wamp_DoWampSessionAddTestament(WAMP *wamp, long long callRequestId, JSON *options, JSON *argList, JSON *argDict)
{
	const char *topic = json_ArrayStringzAt(argList, 0);
	JSON *list = json_ArrayAt(argList, 1);
	JSON *dict = json_ArrayAt(argList, 2);

	JSON *opts = json_ObjectElementCalled(argDict, "publish_options");
	const char *scope = json_ObjectStringzCalled(argDict, "scope");
	if (!scope) scope = "destroyed";

	if (strcmp(scope, "detached") && strcmp(scope, "destroyed")) {
		wamp_SendInternalError(wamp, WAMP_CALL, callRequestId, errInvalidArgument, "scope must be 'detached' or 'destroyed'");
		return 1;
	}

	testamentCollection::iterator it = wamp->testaments->find(scope);

	if (it == wamp->testaments->end()) {			// Bucket doesn't exist yet
		testamentBucket b;

		wamp->testaments->insert(std::make_pair(scope, b));		// Put it in the map

		it = wamp->testaments->find(scope);						// And look again
	}

	JSON *testament = json_NewArray();
	json_ArrayAddStringz(testament, topic);
	json_ArrayAdd(testament, list);
	json_ArrayAdd(testament, dict);
	json_ArrayAdd(testament, opts);

	it->second.push_back(testament);							// Add it to the bucket

	// The spec (14.4.11.2.1) says this doesn't return anything

	return 0;
}

STATIC int wamp_DoWampSessionFlushTestaments(WAMP *wamp, long long callRequestId, JSON *options, JSON *argList, JSON *argDict)
{
	const char *scope = json_ObjectStringzCalled(argDict, "scope");
	if (!scope) scope = "destroyed";

	if (strcmp(scope, "detached") && strcmp(scope, "destroyed")) {
		wamp_SendInternalError(wamp, WAMP_CALL, callRequestId, errInvalidArgument, "scope must be 'detached' or 'destroyed'");
		return 1;
	}

	wamp_ReadTestaments(wamp, scope, false);

	// The spec (14.4.11.2.2) says this doesn't return anything

	return 0;
}

STATIC int wamp_HandleWampCall(WAMP *wamp, long long callRequestId, JSON *options, const char *procedure, JSON *argList, JSON *argDict)
{
	if (!strcmp(procedure, "wamp.registration.list")) {
		return wamp_DoWampRegistrationList(wamp, callRequestId, options, argList, argDict);
	} else if (!strcmp(procedure, "wamp.registration.lookup")) {
		return wamp_DoWampRegistrationLookup(wamp, callRequestId, options, argList, argDict);
	} else if (!strcmp(procedure, "wamp.registration.match")) {
		return wamp_DoWampRegistrationMatch(wamp, callRequestId, options, argList, argDict);
	} else if (!strcmp(procedure, "wamp.registration.get")) {
		return wamp_DoWampRegistrationGet(wamp, callRequestId, options, argList, argDict);
	} else if (!strcmp(procedure, "wamp.registration.list_callees")) {
		return wamp_DoWampRegistrationListCallees(wamp, callRequestId, options, argList, argDict);
	} else if (!strcmp(procedure, "wamp.registration.count_callees")) {
		return wamp_DoWampRegistrationCountCallees(wamp, callRequestId, options, argList, argDict);

	} else if (!strcmp(procedure, "wamp.subscription.list")) {
		return wamp_DoWampSubscriptionList(wamp, callRequestId, options, argList, argDict);
	} else if (!strcmp(procedure, "wamp.subscription.lookup")) {
		return wamp_DoWampSubscriptionLookup(wamp, callRequestId, options, argList, argDict);
	} else if (!strcmp(procedure, "wamp.subscription.match")) {
		return wamp_DoWampSubscriptionMatch(wamp, callRequestId, options, argList, argDict);
	} else if (!strcmp(procedure, "wamp.subscription.get")) {
		return wamp_DoWampSubscriptionGet(wamp, callRequestId, options, argList, argDict);
	} else if (!strcmp(procedure, "wamp.subscription.list_subscribers")) {
		return wamp_DoWampSubscriptionListSubscribers(wamp, callRequestId, options, argList, argDict);
	} else if (!strcmp(procedure, "wamp.subscription.count_subscribers")) {
		return wamp_DoWampSubscriptionCountSubscribers(wamp, callRequestId, options, argList, argDict);

	} else if (!strcmp(procedure, "wamp.session.count")) {
		return wamp_DoWampSessionCount(wamp, callRequestId, options, argList, argDict);
	} else if (!strcmp(procedure, "wamp.session.list")) {
		return wamp_DoWampSessionList(wamp, callRequestId, options, argList, argDict);
	} else if (!strcmp(procedure, "wamp.session.get")) {
		return wamp_DoWampSessionGet(wamp, callRequestId, options, argList, argDict);
	} else if (!strcmp(procedure, "wamp.session.add_testament")) {
		return wamp_DoWampSessionAddTestament(wamp, callRequestId, options, argList, argDict);
	} else if (!strcmp(procedure, "wamp.session.flush_testaments")) {
		return wamp_DoWampSessionFlushTestaments(wamp, callRequestId, options, argList, argDict);

	} else if (!strcmp(procedure, "wamp.topic.history.last")) {
		return wamp_DoWampTopicHistoryLast(wamp, callRequestId, options, argList, argDict);
	} else if (!strcmp(procedure, "wamp.topic.history.since")) {
		return wamp_DoWampTopicHistorySince(wamp, callRequestId, options, argList, argDict);
	} else if (!strcmp(procedure, "wamp.topic.history.after")) {
		return wamp_DoWampTopicHistoryAfter(wamp, callRequestId, options, argList, argDict);
	}

	wamp_SendInternalError(wamp, WAMP_CALL, callRequestId, errNoSuchProcedure, procedure);

	return 1;
}

STATIC int wamp_DoMtWampCalleesList(WAMP *wamp, long long callRequestId, JSON *options, JSON *argList, JSON *argDict)
{
	return 0;
}

STATIC int wamp_HandleMtWampCall(WAMP *wamp, long long callRequestId, JSON *options, const char *procedure, JSON *argList, JSON *argDict)
{
	if (!strcmp(procedure, "mtwamp.callees.list")) {
		return wamp_DoMtWampCalleesList(wamp, callRequestId, options, argList, argDict);
	}

	const char *err = hprintf(NULL, "Can't do mtwamp call '%s' yet", procedure);
	wamp_SendInternalError(wamp, WAMP_CALL, callRequestId, errNoSuchProcedure, err);
	szDelete(err);

	return 0;
}

API WAMPIDLER *wamp_CallAfter(WAMP *wamp, int period, WAMPCB_Idler idler, void *data)
{
    return (wamp && wamp->connectionType == 1) ? ws_CallAfter(wamp->connection.ws, period, idler, data) : NULL;
}

API WAMPIDLER *wamp_CallEvery(WAMP *wamp, int period, WAMPCB_Idler idler, void *data)
{
    return (wamp && wamp->connectionType == 1) ? ws_CallEvery(wamp->connection.ws, period, idler, data) : NULL;
}

STATIC void wamp_CancelCall(long long requestId, const char *reason)
{
	if (activeCallMap) {
		wamp_call_t *callInfo = (wamp_call_t *)idmap_GetValue(activeCallMap, requestId);

		if (callInfo) {
			// Send appropriate error back to the caller (looks as if call has been canceled)
			WAMP *callerWamp = wamp_ByName(callInfo->callerWampName);

			if (callerWamp) {
				wamp_SendErrorStr(callerWamp, WAMP_CALL, callInfo->callerRequestId, reason, errCanceled, NULL, NULL);
			}

			// Send appropriate message to the callee to suggest they don't bother
			WAMP *calleeWamp = wamp_ByName(callInfo->calleeWampName);

			if (calleeWamp) {
				JSON *interrupt = wamp_NewMessage(WAMP_INTERRUPT, callInfo->calleeRequestId);
				json_ArrayAdd(interrupt, json_NewObject());

				wamp_SendJsonHeap(calleeWamp, interrupt);
			}

			wamp_DeleteCallInfo(callInfo);
		}
	}
}

STATIC void wamp_CallTimeout(void *info)
{
	long long requestId = *(long long*)info;

	wamp_CancelCall(requestId, "Timed out");

	free((void*)info);
}

STATIC int wamp_HandleCall(WAMP *wamp, JSON *json, bool wantsYield = false)
// Handles an incoming call, but will also re-direct an incoming invoke if wantsYield is true
// wantsYield simply means that when the yield (or error) comes in from the outgoing invocation, the response will be
// passed back as a yield rather than as a result.
{
	long long callRequestId = json_ArrayIntegerAt(json, 1);
	JSON *options = json_ArrayAt(json, 2);
	const char *procedure = json_ArrayStringzAt(json, 3);
	JSON *argList = json_ArrayTakeAt(json, 4);
	JSON *argDict = json_ArrayTakeAt(json, 4);

	if (!strncmp(procedure, "wamp.", 5)) {
		return wamp_HandleWampCall(wamp, callRequestId, options, procedure, argList, argDict);
	}

	if (!strncmp(procedure, "mtwamp.", 5)) {
		return wamp_HandleMtWampCall(wamp, callRequestId, options, procedure, argList, argDict);
	}

	long long registrationId = 0;
	wamp_invokee_t *invokee = wamp_FindInvokee(wamp, procedure, &registrationId);
	if (!invokee) {
		wamp_SendError(wamp, WAMP_CALL, callRequestId, NULL, errNoSuchProcedure, NULL, NULL);
		return 1;
	}

	long long timeout= json_ObjectIntegerCalled(options, "timeout", 0);
	bool disclose_me = json_ObjectBoolCalled(options, "disclose_me") == 1;
	bool receive_progress = json_ObjectBoolCalled(options, "receive_progress") == 1;

	// Here, we have someone available to handle the call
	long long invokeRequestId = wamp_RandomId();

	if (timeout) {
		long long *info = NEW(long long, 1);
		*info = invokeRequestId;

		if (timeout > INT_MAX) timeout=INT_MAX;		// Even this high is stupid - around 24 days.

		wamp_CallAfter(wamp, (int)timeout, wamp_CallTimeout, info);
	}

	if (invokee->cb_invokee) {								// Handled by a function
		(invokee->cb_invokee)(wamp, callRequestId, procedure, argList, argDict);
		json_Delete(argList);
		json_Delete(argDict);
	} else {												// Handled in the normal way
		JSON *j = wamp_NewMessage(WAMP_INVOCATION, invokeRequestId);
		json_ArrayAddInteger(j, registrationId);
		JSON *callDetails = json_NewObject();
		json_ObjectAddStringz(callDetails, "procedure", procedure);
		if (disclose_me) json_ObjectAddInteger(callDetails, "caller", wamp->sessionId);
		if (receive_progress) json_ObjectAddBool(callDetails, "receive_progress", true);
		json_ArrayAdd(j, callDetails);
		AddArguments(j, argList, argDict);

		WAMP *invokeeWamp = wamp_ByName(invokee->wampName);
		if (invokeeWamp) {
			wamp_AddCaller(wamp_Name(wamp), callRequestId, invokee->wampName, invokeRequestId, invokee, wantsYield);

			wamp_SendJsonHeap(invokeeWamp, j);
		}
	}

	return 0;
}

STATIC int wamp_HandleInvocation(WAMP *wamp, JSON *json)
{
	long long requestId = json_ArrayIntegerAt(json, 1);
	long long registrationId = json_ArrayIntegerAt(json, 2);
	JSON *details = json_ArrayAt(json, 3);
	JSON *list = json_ArrayAt(json, 4);
	JSON *dict = json_ArrayAt(json, 5);

	const char *procedure = json_ObjectStringzCalled(details, "procedure");

	client_registration_t *reg = (client_registration_t *)idmap_GetValue(wamp->clientRegistrationsReg, registrationId);

//Log("WA: INVOCATION: requestId = %lld", requestId);
//Log("WA: INVOCATION: registrationId = %lld", registrationId);
//Log("WA: INVOCATION: details = %s", json_Render(details));
//Log("WA: INVOCATION: list = (%p) %s", list, json_Render(list));
//Log("WA: INVOCATION: dict = (%p) %s", dict, json_Render(dict));
//Log("WA: INVOCATION: wamp_Invokee = (%p)", wampcb_Invokee);
//Log("WA: INVOCATION: procedure = %s", procedure ? procedure : "NONE");
//Log("WA: INVOCATION: reg = (%p)", reg);

	if (reg) {
		if (reg->cbInvoke) {
			if (!procedure) procedure = reg->procedure;
			JSON *yield = (*reg->cbInvoke)(wamp, requestId, registrationId, procedure, details, list, dict, reg->data);

			if (yield) {
				const char *uri = json_ArrayStringzAt(yield, 0);

				if (uri) {	// It's an error
					SPMAP *details = json_ArrayObjectAt(yield, 1);
					JSON *list = json_ArrayAt(yield, 2);
					JSON *dict = json_ArrayAt(yield, 3);

					if (!details) details = spmap_New();
					wamp_SendError(wamp, WAMP_INVOCATION, requestId, details, uri, list, dict);
				} else {										// Success of some sort
					JSON *options = json_ArrayAt(yield, 0);
					JSON *list = json_ArrayAt(yield, 1);
					JSON *dict = json_ArrayAt(yield, 2);
					if (!options) options = json_NewObject();

					JSON *json = wamp_NewMessage(WAMP_YIELD, requestId);
					json_ArrayAdd(json, options);
					AddArguments(json, list, dict);

					wamp_SendJsonHeap(wamp, json);
				}
				json_Delete(yield);
			}
		}
	} else if (wampcb_Invokee) {
		(*wampcb_Invokee)(wamp, requestId, procedure, list, dict);
	} else {
		wamp_NoInvokee(wamp, requestId, procedure, list, dict);
	}

	return 0;
}

STATIC int wamp_HandleYield(WAMP *wamp, JSON *json)
{
	WAMP *callerWamp = NULL;

	long long requestId = json_ArrayIntegerAt(json, 1);
	JSON *details = json_ArrayAt(json, 2);

	long long callerRequestId;
	bool wantsYield;

	int progressive = json_ObjectBoolCalled(details, "progress") == 1;

	const char *callerName = wamp_FindCaller(requestId, &callerRequestId, &wantsYield, !progressive);
	//Log("WA: Name of caller = '%s'", callerName);
	if (callerName) {									// Should always find one
		callerWamp = wamp_ByName(callerName);			// May not find one if the wamp has 'gone away' since the call

		//Log("WA: Calling wamp = %p", callerWamp);
		szDelete(callerName);
	}

	if (callerWamp) {									// The caller still exists
		JSON *resultList = json_ArrayAt(json, 3);
		JSON *resultDict = json_ArrayAt(json, 4);

		if (wantsYield)
			wamp_SendYield(callerWamp, callerRequestId, details, resultList, resultDict);
		else
			wamp_SendResult(callerWamp, callerRequestId, details, resultList, resultDict);
	}
	return 0;
}

STATIC int wamp_HandleResult(WAMP *wamp, JSON *json)
{
	long long requestId = json_ArrayIntegerAt(json, 1);
	JSON *details = json_ArrayAt(json, 2);
	JSON *list = json_ArrayAt(json, 3);
	JSON *dict = json_ArrayAt(json, 4);

	client_call_t *call = (client_call_t *)idmap_GetValue(wamp->clientCalls, requestId);
	if (call) {
		if (call->cbResult) {
			const char *procedure = json_ObjectStringzCalled(details, "procedure");
			if (!procedure) procedure = call->procedure;

			(*call->cbResult)(wamp, requestId, call->procedure, NULL, details, list, dict, call->data);
		}

		DeleteClientCall(wamp, call);
	}

	return 0;
}

STATIC int wamp_HandleCancel(WAMP *wamp, JSON *json)
{
	long long requestId = json_ArrayIntegerAt(json, 1);

	wamp_CancelCall(requestId, "Canceled");

	return 0;
}

STATIC int wamp_HandleSubscribe(WAMP *wamp, JSON *json)
{
	long long requestId = json_ArrayIntegerAt(json, 1);
	SPMAP *options = json_ArrayObjectAt(json, 2);
	const char *topic = json_ArrayStringzAt(json, 3);
	WAMPREALM *realm = wamp->realm;

//Log("WA: Subscribing topic %s into realm %p", topic, realm);
	int matchType = WAMP_MATCH_EXACT;
	JSON *jMatch = (JSON*)spmap_GetValue(options, "match");
	if (jMatch) {
		if (json_Type(jMatch) != JSON_STRING) {
			wamp_SendErrorStr(wamp, WAMP_REGISTER, requestId, "OPTION value must be a string", errInvalidArgument, NULL, NULL);
			return 0;
		}
		matchType = wamp_MatchType(json_AsStringz(jMatch));
	} else {
		matchType = wamp_MatchTypeFromUri(topic, NULL);
	}

	if (!matchType) {
		wamp_SendErrorStr(wamp, WAMP_REGISTER, requestId, "match must be exact, prefix, wildcard or regex", errInvalidArgument, NULL, NULL);
		return 0;
	}

	// See if this entry wants to hold off receiving events until history has been sent
	int wait_for_history = json_AsBool((JSON*)spmap_GetValue(options, "wait_for_history"));
	if (wait_for_history == -1) wait_for_history = 0;

	SPMAP *map;
	subscription_t *sub = wamp_FindSubscriptionForType(realm, matchType, topic, &map);

	if (!realm->allSubscriptions)	realm->allSubscriptions = idmap_New();		// Initialise the map of all subscriptions if nec.

	if (!sub) {
		sub = subscription_t_New(realm, map, matchType, topic);
	}

	long long subscriptionId = sub->subscriptionId;

	if (sub->count == 0) {
		JSON *list = json_NewArray();

		json_ArrayAddInteger(list, wamp->sessionId);
		JSON *info = json_NewObject();
		json_ObjectAddStringz(info, "uri", topic);
		json_ObjectAddStringz(info, "match", wamp_MatchName(matchType));
		json_ObjectAddInteger(info, "id", subscriptionId);
		json_ObjectAddStringz(info, "timestamp", sub->timestamp);
		json_ArrayAdd(list, info);

		wamp_PublishIn(wamp, "wamp.subscription.on_create", NULL, list);
	}

	JSON *list = json_NewArray();

	json_ArrayAddInteger(list, wamp->sessionId);
	json_ArrayAddInteger(list, subscriptionId);
	wamp_PublishIn(wamp, "wamp.subscription.on_subscribe", NULL, list);

	subscription_t_AddSubscription(sub, wamp_Name(wamp), requestId, wait_for_history);

	JSON *j = wamp_NewMessage(WAMP_SUBSCRIBED, requestId);
	json_ArrayAddInteger(j, subscriptionId);

	wamp_SendJsonHeap(wamp, j);

	return 0;
}

STATIC int wamp_HandleSubscribed(WAMP *wamp, JSON *json)
{
	long long requestId = json_ArrayIntegerAt(json, 1);
	long long subscriptionId = json_ArrayIntegerAt(json, 2);

	client_subscription_t *sub = (client_subscription_t *)idmap_GetValue(wamp->clientSubscriptionsReq, requestId);
	if (sub) {
		sub->subscriptionId = subscriptionId;
		idmap_Add(wamp->clientSubscriptionsReq, requestId, sub);
		idmap_Add(wamp->clientSubscriptionsSub, subscriptionId, sub);

//Log("Subscription topic = '%s'", sub->topic);
		if (sub->cbSubscribed)
			(*sub->cbSubscribed)(wamp, requestId, subscriptionId, NULL, sub->topic);
	} else {
		Log("WA: Unhandled subscribed message: %s", json_Render(json));
	}

	return 1;
}

STATIC int wamp_HandleUnsubscribe(WAMP *wamp, JSON *json)
{
	long long requestId = json_ArrayIntegerAt(json, 1);
	long long subscriptionId = json_ArrayIntegerAt(json, 2);
	WAMPREALM *realm = wamp->realm;
	int removed = 0;

	if (realm->allSubscriptions) {
		subscription_t *sub = (subscription_t *)idmap_GetValue(realm->allSubscriptions, subscriptionId);
		const char *wampName = wamp_Name(wamp);

		removed = wamp_RemoveWampFromSubscription(sub, wampName);
	}

	if (removed) {
		JSON *j = wamp_NewMessage(WAMP_UNSUBSCRIBED, requestId);
		wamp_SendJsonHeap(wamp, j);
	} else {
		wamp_SendErrorStr(wamp, WAMP_UNSUBSCRIBE, requestId, "You were not subscribed", errNoSuchSubscription, NULL, NULL);
	}

	return 1;
}

STATIC int wamp_HandleUnsubscribed(WAMP *wamp, JSON *json)
{
	long long requestId = json_ArrayIntegerAt(json, 1);

	client_subscription_t *sub = (client_subscription_t *)idmap_GetValue(wamp->clientSubscriptionsUnreq, requestId);
	if (sub) {
		if (sub->cbUnsubscribed)
			(*sub->cbUnsubscribed)(wamp, requestId, sub->subscriptionId, NULL, sub->topic);

		DeleteClientSubscription(wamp, sub);
	} else {
		Log("WA: Unhandled unsubscribed message: %s", json_Render(json));
	}

	return 1;
}

STATIC int wamp_HandlePublish(WAMP *wamp, JSON *json)
// We need to publish a topic
// Look through each of the three maps (exact, prefix, wildcard) for matching topics
// For each one that we find, add it to a list of subscriptions
// When we've done that, run through the list, sending a message to each WAMP.
{
	long long requestId = json_ArrayIntegerAt(json, 1);

	JSON *options = json_ArrayAt(json, 2);
	const char *topic = json_ArrayStringzAt(json, 3);
	JSON *argList = json_ArrayTakeAt(json, 4);
	JSON *argDict = json_ArrayTakeAt(json, 4);
	WAMPREALM *realm = wamp->realm;
	if (!realm) {
		Log("WAMP %s has no realm - we can't publish '%s'", wamp_Name(wamp), topic);
		return 1;
	}
	int subscriberCount = 0;

	sublist_t *subs = wamp_FindSubscribers(realm, topic);

	JSON *exclude = json_ObjectElementCalled(options, "exclude");
	JSON *exclude_authid = json_ObjectElementCalled(options, "exclude_authid");
	JSON *exclude_authrole = json_ObjectElementCalled(options, "exclude_authrole");
	JSON *eligible = json_ObjectElementCalled(options, "eligible");
	JSON *eligible_authid = json_ObjectElementCalled(options, "eligible_authid");
	JSON *eligible_authrole = json_ObjectElementCalled(options, "eligible_authrole");

	std::set<long long> exclude_set;
	std::set<std::string> exclude_authid_set;
	std::set<std::string> exclude_authrole_set;
	std::set<long long> eligible_set;
	std::set<std::string> eligible_authid_set;
	std::set<std::string> eligible_authrole_set;

	// All the error checking for the various black/white lists
	if (exclude && !json_IsIntegerArray(exclude)) {

		wamp_SendErrorStr(wamp, WAMP_PUBLISH, requestId, "exclude must be an array of integer", errInvalidArgument, NULL, NULL);
		return 0;
	}

	if (exclude_authid && !json_IsStringArray(exclude_authid)) {
		wamp_SendErrorStr(wamp, WAMP_PUBLISH, requestId, "exclude_authid must be an array of strings", errInvalidArgument, NULL, NULL);
		return 0;
	}

	if (exclude_authrole && !json_IsStringArray(exclude_authrole)) {
		wamp_SendErrorStr(wamp, WAMP_PUBLISH, requestId, "exclude_authrole must be an array of strings", errInvalidArgument, NULL, NULL);
		return 0;
	}

	if (eligible && !json_IsIntegerArray(eligible)) {
		wamp_SendErrorStr(wamp, WAMP_PUBLISH, requestId, "eligible must be an array of integer", errInvalidArgument, NULL, NULL);
		return 0;
	}

	if (eligible_authid && !json_IsStringArray(eligible_authid)) {
		wamp_SendErrorStr(wamp, WAMP_PUBLISH, requestId, "eligible_authid must be an array of strings", errInvalidArgument, NULL, NULL);
		return 0;
	}

	if (eligible_authrole && !json_IsStringArray(eligible_authrole)) {
		wamp_SendErrorStr(wamp, WAMP_PUBLISH, requestId, "eligible_authrole must be an array of strings", errInvalidArgument, NULL, NULL);
		return 0;
	}

	if (exclude) exclude_set = json_SetFromIntegerArray(exclude);
	if (exclude_authid) exclude_authid_set = json_SetFromStringArray(exclude_authid);
	if (exclude_authrole) exclude_authrole_set = json_SetFromStringArray(exclude_authrole);
	if (eligible) eligible_set = json_SetFromIntegerArray(eligible);
	if (eligible_authid) eligible_authid_set = json_SetFromStringArray(eligible_authid);
	if (eligible_authrole) eligible_authrole_set = json_SetFromStringArray(eligible_authrole);

	if (json_ObjectBoolCalled(options, "exclude_me") != 0) {			// Basically excluding ourselves
		exclude = options;												// Has to be something non-NULL
		exclude_set.insert(wamp->sessionId);
	}

	// restricted means that we need to be careful when publishing...
	bool restricted = exclude_set.size() || exclude_authid_set.size() || exclude_authid_set.size()
					|| eligible_set.size() || eligible_authid_set.size() || eligible_authid_set.size();

	int bDiscloseMe = json_ObjectBoolCalled(options, "disclose_me");
	if (bDiscloseMe == -1) bDiscloseMe = 0;

	long long publicationId = wamp_RandomId();						// May be needed for 'ack' even if no recipients

	// We now have a linked list of subscribers that match this topic, so we'll set about sending events...
	if (subs) {
		JSON *event = wamp_NewMessage(WAMP_EVENT, 0);
		json_ArrayAddInteger(event, 0);								// This will be replaced by the relevant subscription ID
		json_ArrayAddInteger(event, publicationId);
		JSON *details = json_NewObject();
		json_ObjectAddStringz(details, "topic", topic);
		json_ObjectAddStringz(details, "timestamp", TimeNow());
		if (bDiscloseMe) json_ObjectAddInteger(details, "publisher",wamp->sessionId);
		json_ArrayAdd(event, details);
		AddArguments(event, argList, argDict);

		while (subs) {
			sublist_t *subStub = subs;

			long long subscriptionId = subStub->sub->subscriptionId;
			spmap_Reset(subStub->sub->wamps);
			const char *wampName;
			subscription_info_t *subInfo;

			json_ArraySetAt(event, 1, json_NewInteger(subscriptionId));
			while (spmap_GetNextEntry(subStub->sub->wamps, &wampName, (void**)&subInfo)) {
				if (subInfo->wait_for_history)
					continue;

				WAMP *wRecip = wamp_ByName(wampName);
				if (!wRecip)
					continue;

				if (restricted) {										// There may be some reason to exclude this recipient
					if (eligible && eligible_set.find(wRecip->sessionId) == eligible_set.end())
						continue;
					if (eligible_authid && eligible_authid_set.find(wRecip->authId) == eligible_authid_set.end())
						continue;
					if (eligible_authrole && eligible_authrole_set.find(wRecip->authRole) == eligible_authrole_set.end())
						continue;
					if (exclude && exclude_set.find(wRecip->sessionId) != exclude_set.end())
						continue;
					if (exclude_authid && exclude_authid_set.find(wRecip->authId) != exclude_authid_set.end())
						continue;
					if (exclude_authrole && exclude_authrole_set.find(wRecip->authRole) != exclude_authrole_set.end())
						continue;
				}

				wamp_SendJson(wRecip, event);
				subscriberCount++;
			}

			if (subStub->sub->saveHistory)
				wamp_SaveEvent(subStub->sub, publicationId, topic, details, argList, argDict);

			subs=subStub->prev;
			free((char*)subStub);
		}

		json_Delete(event);
	}

	int bAcknowledge = json_ObjectBoolCalled(options, "acknowledge");
	if (bAcknowledge == 1) {
		JSON *json = wamp_NewMessage(WAMP_PUBLISHED, requestId);
		json_ArrayAddInteger(json, publicationId);
//		json_ArrayAddInteger(json, subscriberCount);				// There is no room to put this...
		wamp_SendJsonHeap(wamp, json);
	}


	// If our publisher has subscribed to 'topic.report', send them a report back
	if (realm->subMapExact) {
		const char *reportTopic = hprintf(NULL, "%s.report", topic);

		subscription_t *sub = (subscription_t*)spmap_GetValue(realm->subMapExact, reportTopic);
		if (sub) {
			SPMAP *map = sub->wamps;
			spmap_Reset(map);

			const char *wampName;
			subscription_info_t *subInfo;

			while (spmap_GetNextEntry(map, &wampName, (void**)&subInfo)) {

				if (!strcmp(wampName, wamp_Name(wamp))) {			// This is our caller
					JSON *event = wamp_NewMessage(WAMP_EVENT, sub->subscriptionId);
					json_ArrayAddInteger(event, wamp_RandomId());
					JSON *details = json_NewObject();
					json_ObjectAddStringz(details, "topic", reportTopic);
					json_ObjectAddStringz(details, "timestamp", TimeNow());
					json_ArrayAdd(event, details);

					JSON *list = json_NewArray();
					json_ArrayAddInteger(list, subscriberCount);

					AddArguments(event, list, NULL);

					wamp_SendJsonHeap(wamp, event);
					break;
				}
			}
		}
		szDelete(reportTopic);
	}

	return 1;
}


STATIC int TellPublished(WAMP *wamp, long long requestId, long long publishId, JSON *details, const char *uri)
// Does the actual callback to the publisher on either a published message or a publish error
// Deletes the publish entry from clientPublishes if it exists
// Returns 1 if we called the callback
{
	int result = 0;

	client_publish_t *pub = (client_publish_t *)idmap_GetValue(wamp->clientPublishes, requestId);
	if (pub) {
		if (pub->cbPublished)
			(*pub->cbPublished)(wamp, requestId, publishId, details, uri ? uri : pub->topic);
		DeleteClientPublish(wamp, pub);

		result = 1;
	}

	return result;
}

STATIC int wamp_HandlePublished(WAMP *wamp, JSON *json)
// If we have a callback set from a wamp_Publish() call, call it back.
{
	if (wamp && json) {
		long long requestId = json_ArrayIntegerAt(json, 1);
		long long publishId = json_ArrayIntegerAt(json, 2);

		if (!TellPublished(wamp, requestId, publishId, NULL, NULL)) {
			Log("WA: Unhandled published message: %s", json_Render(json));
		}
	}

	return 1;
}

STATIC int wamp_HandleEvent(WAMP *wamp, JSON *json)
{
	int handled = 0;

	if (wamp) {
		long long subscriptionId = json_ArrayIntegerAt(json, 1);
		long long publicationId = json_ArrayIntegerAt(json, 2);
		JSON *details = json_ArrayAt(json, 3);
		JSON *list = json_ArrayAt(json, 4);
		JSON *dict = json_ArrayAt(json, 5);

		client_subscription_t *sub = (client_subscription_t *)idmap_GetValue(wamp->clientSubscriptionsSub, subscriptionId);
		if (sub) {
			if (sub->cbEvent) {
				const char *topic = json_ObjectStringzCalled(details, "topic");
				if (!topic) topic = sub->topic;
				(*sub->cbEvent)(wamp, subscriptionId, publicationId, topic, details, list, dict);
				handled = 1;
			}
		}
	}

	if (!handled)
		Log("WA: Unhandled event message: %s", json_Render(json));

	return 1;
}

STATIC int wamp_HandleError(WAMP *wamp, JSON *json)
{
	long long errorNumber = json_ArrayIntegerAt(json, 1);

	if (errorNumber == WAMP_INVOCATION) {					// An invocation error, need to pass back to the caller
		WAMP *callerWamp = NULL;

		long long requestId = json_ArrayIntegerAt(json, 2);
		long long callerRequestId;
		bool wantsYield;
		const char *callerName = wamp_FindCaller(requestId, &callerRequestId, &wantsYield);

		if (callerName) {									// Should always find one
			callerWamp = wamp_ByName(callerName);			// May not find one if the wamp has 'gone away' since the call

			szDelete(callerName);
		}

		if (callerWamp) {									// The caller still exists
			SPMAP *details = json_ArrayTakeObjectAt(json, 3);
			const char *uri = json_ArrayStringzAt(json, 3);
			JSON *resultList = json_ArrayTakeAt(json, 4);
			JSON *resultDict = json_ArrayTakeAt(json, 4);

			wamp_SendError(callerWamp, wantsYield ? WAMP_INVOCATION : WAMP_CALL, callerRequestId, details, uri, resultList, resultDict);
		}
	} else if (errorNumber == WAMP_PUBLISH) {
		long long requestId = json_ArrayIntegerAt(json, 2);
		JSON *details = json_ArrayAt(json, 3);
		const char *uri = json_ArrayStringzAt(json, 4);

		TellPublished(wamp, requestId, 0, details, uri);
	} else if (errorNumber == WAMP_SUBSCRIBE) {
		long long requestId = json_ArrayIntegerAt(json, 2);
		JSON *details = json_ArrayAt(json, 3);
		const char *uri = json_ArrayStringzAt(json, 4);

		client_subscription_t *sub = (client_subscription_t *)idmap_GetValue(wamp->clientSubscriptionsReq, requestId);
		if (sub) {
			if (sub->cbSubscribed)
				(*sub->cbSubscribed)(wamp, requestId, 0, details, uri);

			DeleteClientSubscription(wamp, sub);
		}
	} else if (errorNumber == WAMP_UNSUBSCRIBE) {
		long long requestId = json_ArrayIntegerAt(json, 2);
		JSON *details = json_ArrayAt(json, 3);
		const char *uri = json_ArrayStringzAt(json, 4);

		client_subscription_t *sub = (client_subscription_t *)idmap_GetValue(wamp->clientSubscriptionsUnreq, requestId);
		if (sub) {
			if (sub->cbUnsubscribed)
				(*sub->cbUnsubscribed)(wamp, requestId, 0, details, uri);

			DeleteClientSubscription(wamp, sub);
		}
	} else if (errorNumber == WAMP_REGISTER) {
		long long requestId = json_ArrayIntegerAt(json, 2);
		JSON *details = json_ArrayAt(json, 3);
		const char *uri = json_ArrayStringzAt(json, 4);

		client_registration_t *reg = (client_registration_t *)idmap_GetValue(wamp->clientRegistrationsReq, requestId);
		if (reg) {
			if (reg->cbRegistered)
				(*reg->cbRegistered)(wamp, requestId, 0, details, uri);

			DeleteClientRegistration(wamp, reg);
		}
	} else if (errorNumber == WAMP_UNREGISTER) {
		long long requestId = json_ArrayIntegerAt(json, 2);
		JSON *details = json_ArrayAt(json, 3);
		const char *uri = json_ArrayStringzAt(json, 4);

		client_registration_t *sub = (client_registration_t *)idmap_GetValue(wamp->clientRegistrationsUnreq, requestId);
		if (sub) {
			if (sub->cbUnregistered)
				(*sub->cbUnregistered)(wamp, requestId, 0, details, uri);

			DeleteClientRegistration(wamp, sub);
		}
	} else if (errorNumber == WAMP_CALL) {
		long long requestId = json_ArrayIntegerAt(json, 2);
		JSON *details = json_ArrayAt(json, 3);
		const char *uri = json_ArrayStringzAt(json, 4);
		JSON *list = json_ArrayAt(json, 5);
		JSON *dict = json_ArrayAt(json, 6);

		client_call_t *call = (client_call_t *)idmap_GetValue(wamp->clientCalls, requestId);
		if (call) {
			if (call->cbResult)
				(*call->cbResult)(wamp, requestId, call->procedure, uri, details, list, dict, call->data);

			DeleteClientCall(wamp, call);
		}
	} else {
		Log("WA: We've received an error message that nobody will handle: %s", json_Render(json));
	}

	return 0;
}

// This set of functions is used if we're forwarding subscription handling to another broker
STATIC int wamp_ForwardSubscribe(WAMP *wamp, WAMP *fwdWamp, JSON *json)
{
	long long requestId = json_ArrayIntegerAt(json, 1);
	char buf[30];
	sprintf(buf, "%lld", requestId);

	ssmap_Add(wamp->realm->fwdSubscriptionMap, buf, wamp_Name(wamp));       // Remember where this came from
	wamp_SendJson(fwdWamp, json);

	return 0;
}

STATIC int wamp_ForwardSubscribed(WAMP *wamp, WAMP *fwdWamp, JSON *json)
{
	long long requestId = json_ArrayIntegerAt(json, 1);
	long long subscriptionId = json_ArrayIntegerAt(json, 2);
	char buf[30];
	sprintf(buf, "%lld", requestId);
	const char *sender = ssmap_GetValue(wamp->realm->fwdSubscriptionMap, buf);
	if (sender) {
		char buf[30];
		sprintf(buf, "%lld", subscriptionId);
		ssmap_Add(wamp->realm->fwdEventMap, buf, sender);                   // Remember who asked for this

		WAMP *wsender = wamp_ByName(sender);
		if (wsender) {                                      // If no wsender then it's gone away since subscribing
			wamp_SendJson(wsender, json);                  // Send the SUBSCRIBED message back to the subscriber
		}
	} else {                                                // TODO: Case of a subscribed message when we didn't subscribe...
		Log("WA: SUBSRIBED(%s) not found", buf);
	}
	return 0;
}

STATIC int wamp_ForwardUnsubscribe(WAMP *wamp, WAMP *fwdWamp, JSON *json)
{
	long long requestId = json_ArrayIntegerAt(json, 1);
	char buf[30];
	sprintf(buf, "%lld", requestId);

	ssmap_Add(wamp->realm->fwdUnsubscriptionMap, buf, wamp_Name(wamp));       // Remember where this came from
	wamp_SendJson(fwdWamp, json);

	return 0;
}

STATIC int wamp_ForwardUnsubscribed(WAMP *wamp, WAMP *fwdWamp, JSON *json)
{
	long long subscriptionId = json_ArrayIntegerAt(json, 1);
	char buf[30];
	sprintf(buf, "%lld", subscriptionId);
	const char *sender = ssmap_GetValue(wamp->realm->fwdUnsubscriptionMap, buf);
	if (sender) {
		WAMP *wsender = wamp_ByName(sender);
		if (wsender) {                                      // If no wsender then it's gone away since subscribing
			wamp_SendJson(wsender, json);                  // Send the UNSUBSCRIBED message back to the subscriber
		}
	} else {                                                // TODO: Case of a subscribed message when we didn't subscribe...
		Log("WA: UNSUBSRIBED(%s) not found", buf);
	}
	return 0;
}

STATIC int wamp_ForwardPublish(WAMP *wamp, WAMP *fwdWamp, JSON *json)
{
	long long requestId = json_ArrayIntegerAt(json, 1);
	char buf[30];
	sprintf(buf, "%lld", requestId);

	ssmap_Add(wamp->realm->fwdPublishMap, buf, wamp_Name(wamp));            // Remember where this came from
	wamp_SendJson(fwdWamp, json);

	return 0;
}

STATIC int wamp_ForwardPublished(WAMP *wamp, WAMP *fwdWamp, JSON *json)
{
	long long requestId = json_ArrayIntegerAt(json, 1);
	char buf[30];
	sprintf(buf, "%lld", requestId);
	const char *sender = ssmap_GetValue(wamp->realm->fwdPublishMap, buf);

	if (sender) {
		WAMP *wsender = wamp_ByName(sender);
		if (wsender) {                                      // If no wsender then it's gone away since subscribing
			wamp_SendJson(wsender, json);                  // Send the SUBSCRIBED message back to the subscriber
		}
	} else {                                                // TODO: Case of a subscribed message when we didn't subscribe...
		Log("WA: SUBSRIBED(%s) not found", buf);
	}
	return 0;
}

STATIC int wamp_ForwardEvent(WAMP *wamp, WAMP *fwdWamp, JSON *json)
{
	long long subscriptionId = json_ArrayIntegerAt(json, 1);
	char buf[30];
	sprintf(buf, "%lld", subscriptionId);
	const char *sender = ssmap_GetValue(wamp->realm->fwdEventMap, buf);
//Log("WA: Event found (%p, %s) = %s", wamp->realm->fwdEventMap, buf, sender?sender:"NULL");

	if (sender) {
		WAMP *wsender = wamp_ByName(sender);
		if (wsender) {                                      // If no wsender then it's gone away since subscribing
			wamp_SendJson(wsender, json);                  // Send the SUBSCRIBED message back to the subscriber
		}
	} else {                                                // TODO: Case of a subscribed message when we didn't subscribe...
		Log("WA: In event: SUBSRIBED(%s) not found", buf);
	}
	return 0;
}

API int wamp_SetForwardSubscriptions(const char *realmName, int enable, WAMP *wamp)
{
	if (realmName) {
		WAMPREALM *realm = wamp_RealmByName(realmName, 1);

		if (realm) {
			realm->forwardSubscriptions = wamp ? wamp_Name(wamp) : NULL;
			realm->wantForwardSubscriptions = enable;
			return 0;
		}
	}

	return 1;
}

API JSON *wamp_MessagePublish(long long requestId, const char *topic, JSON *opts, JSON *list/*=NULL*/, JSON *dict/*=NULL*/)
{
	if (!topic) return NULL;

	JSON *message = wamp_NewMessage(WAMP_PUBLISH, requestId);

	json_ArrayAdd(message, opts ? opts : json_NewObject());
	json_ArrayAddStringz(message, topic);
	AddArguments(message, list, dict);

	return message;
}

API long long wamp_PublishIn(WAMP *wamp, const char *topic, JSON *opts, JSON *list/*=NULL*/, JSON *dict/*=NULL*/)
// Receive a publish message as if it been received from a publisher
// The opts, list and dict become owned by the function.
{
	if (!wamp || !topic) return 0;

	long long requestId = wamp_RandomId();
	JSON *json = wamp_MessagePublish(requestId, topic, opts, list, dict);

	wamp_Dispatch(wamp, WAMP_PUBLISH, json);
	json_Delete(json);

	return requestId;
}

API long long wamp_Publish(WAMP *wamp, const char *topic, JSON *opts, JSON *list, JSON *dict, WAMPCB_Published cbPublished)
// Publish the event through the broker
// If cbPublished is given, it is called once the published message returns (also sets 'acknowledge:true' in options)
// The opts, list and dict become owned by the function.
{
	if (!wamp || !topic) return 0;

	long long requestId = wamp_RandomId();
	if (!opts) opts = json_NewObject();

	if (cbPublished) {
		client_publish_t *pub = NEW(client_publish_t, 1);
		pub->requestId = requestId;
		pub->cbPublished = cbPublished;
		pub->topic = strdup(topic);
		idmap_Add(wamp->clientPublishes, requestId, pub);

		json_ObjectAddBool(opts, "acknowledge",1);
	}

	JSON *json = wamp_MessagePublish(requestId, topic, opts, list, dict);

	wamp_SendJsonHeap(wamp, json);

	return requestId;
}

API long long wamp_Subscribe(WAMP *wamp, int type, const char *topic, JSON *opts, WAMPCB_Subscribed cbSubscribed, WAMPCB_Event cbEvent)
// Subscribes to a topic
// type			WAMP_EXACT, WAMP_PREFIX or WAMP_WILDCARD (defaults depending on topic format)
// topic		The topic or pattern to which to subscribe
// opts			Options regarding the subscription (the "match" element will be filled if type is specified)
// cbSubscribed	Called when the 'SUBSCRIBED' (or error) message is returned from the broker
// cbEvent		Called whenever this topic is published
// Returns the requestId (can be matched up in cbSubscribed)
{
	if (!wamp || !topic) return 0;

	long long requestId = wamp_RandomId();

	char *newtopic = NULL;
	if (!type) {
		type = wamp_MatchTypeFromUri(topic, &newtopic);
		if (newtopic)
			topic = newtopic;
	}

	client_subscription_t *sub = NEW(client_subscription_t, 1);
	sub->requestId = requestId;
	sub->subscriptionId = 0;
	sub->unrequestId = 0;
	sub->cbSubscribed = cbSubscribed;
	sub->cbEvent = cbEvent;
	sub->cbUnsubscribed = NULL;
	sub->type = type;
	sub->topic = strdup(topic);
	idmap_Add(wamp->clientSubscriptionsReq, requestId, sub);

	JSON *json = wamp_NewMessage(WAMP_SUBSCRIBE, requestId);
	JSON *options = json_NewObject();
	if (type)
		json_ObjectAddStringz(options, "match", wamp_MatchName(type));
	json_ArrayAdd(json, options);
	json_ArrayAddStringz(json, topic);

	wamp_SendJsonHeap(wamp, json);

	szDelete(newtopic);

	return requestId;
}

API long long wamp_Unsubscribe(WAMP *wamp, long long subscriptionId, WAMPCB_Unsubscribed cbUnsubscribed)
// Unsubscribe from a topic given either the subscriptionId or requestId.
// Returns 0 if we were not subscribed on this subscription ID or are already unsubscribing
// subcriptionId	Either the subscriptionId or the original requestId
// cbUnsubscribed	Called when the UNSUBSCRIBED (or error) is returned from the broker
// Returns the requestId or 0 if we're not subscribed or there is a pending unsubscription
// NB. cbUnsubscribed will only be called if we DO NOT return a 0.
{
	if (!wamp) return 0;

	client_subscription_t *sub = (client_subscription_t *)idmap_GetValue(wamp->clientSubscriptionsSub, subscriptionId);
	if (!sub)
		sub = (client_subscription_t *)idmap_GetValue(wamp->clientSubscriptionsReq, subscriptionId);

	long long requestId;

	if (sub && !sub->unrequestId) {
		long long requestId = wamp_RandomId();

		sub->unrequestId = requestId;
		sub->cbUnsubscribed = cbUnsubscribed;
		idmap_Add(wamp->clientSubscriptionsUnreq, requestId, sub);

		JSON *json = wamp_NewMessage(WAMP_UNSUBSCRIBE, requestId);
		json_ArrayAddInteger(json, subscriptionId);
		wamp_SendJsonHeap(wamp, json);
	} else {
		requestId = 0;
	}

	return requestId;
}

API long long wamp_CallIn(WAMP *wamp, const char *procedure, JSON *opts, JSON *list/*=NULL*/, JSON *dict/*=NULL*/)
// Receive a call as if it been received from a caller
// The opts, list and dict become owned by the function.
{
	if (!wamp || !procedure) return 0;

	long long requestId = wamp_RandomId();
	JSON *json = wamp_MessageCall(requestId, procedure, opts, list, dict);

	wamp_Dispatch(wamp, WAMP_CALL, json);

	json_Delete(json);

	return requestId;
}

API JSON *wamp_MessageCall(long long requestId, const char *procedure, JSON *options, JSON *list/*=NULL*/, JSON *dict/*=NULL*/)
{
	if (!procedure) return 0;

	JSON *message = wamp_NewMessage(WAMP_CALL, requestId);
	json_ArrayAdd(message, options ? options : json_NewObject());
	json_ArrayAddStringz(message, procedure);
	AddArguments(message, list, dict);

	return message;
}

API long long wamp_Call(WAMP *wamp, const char *procedure, JSON *options, JSON *list, JSON *dict, WAMPCB_Result cbResult, void *data/*= NULL */)
// Calls a procedure, with the result being returned via the callback procedure given.
// If no result is expected (or wanted), cbResult can be NULL.
{
	if (!wamp || !procedure) return 0;

//const char *o = json_RenderHeap(options, 0);
//const char *l = json_RenderHeap(list, 0);
//const char *d = json_RenderHeap(dict, 0);
//Log("wamp_Call(%s, %p(%s), %p(%s), %p(%s), %p(%s), %p)", wamp_Name(wamp), procedure, procedure, o, o, l, l, d, d, cbResult);
	long long requestId = wamp_RandomId();

	// Remember this call
	client_call_t *call = NEW(client_call_t, 1);
	call->requestId = requestId;
	call->procedure = strdup(procedure);
	call->cbResult = cbResult;
	call->data = data;
	idmap_Add(wamp->clientCalls, requestId, call);

	if (!options) options = json_NewObject();

	JSON *json = wamp_MessageCall(requestId, procedure, options, list, dict);

	wamp_SendJsonHeap(wamp, json);

	return requestId;
}

API long long wamp_Register(WAMP *wamp, int policy, int match, int priority, const char *procedure, WAMPCB_Registered cbRegistered, WAMPCB_Invoke cbInvoke, void *data /*=NULL*/)
// Registers a callee function
// policy		WAMP_INVOKE_SINGLE, WAMP_INVOKE_ROUNDROBIN, WAMP_INVOKE_RANDOM, WAMP_INVOKE_FIRST, WAMP_INVOKE_LAST
// match		WAMP_EXACT, WAMP_PREFIX or WAMP_WILDCARD (defaults depending on procedure format)
// procedure	The procedure or pattern which we're registering
// opts			Options regarding the subscription (the "match" element will be filled if type is specified)
// cbRegistered	Called when the 'REGISTERED' (or error) message is returned from the caller
// cbInvoke		Called whenever this procedure is called
// Returns the requestId (can be matched up in cbSubscribed)
{
	if (!wamp || !procedure || !cbInvoke) return 0;

	long long requestId = wamp_RandomId();

//Log("Registering %d %s", match, procedure);
	char *newprocedure = NULL;
	if (!match) {
		match = wamp_MatchTypeFromUri(procedure, &newprocedure);
		if (newprocedure)
			procedure = newprocedure;
	}
//Log("Now %d %s", match, procedure);

	client_registration_t *reg = NEW(client_registration_t, 1);
	reg->requestId = requestId;
	reg->registrationId = 0;
	reg->cbRegistered = cbRegistered;
	reg->cbUnregistered = NULL;
	reg->unrequestId = 0;
	reg->procedure = strdup(procedure);
	reg->cbInvoke = cbInvoke;
	reg->data = data;
	idmap_Add(wamp->clientRegistrationsReq, requestId, reg);

	JSON *json = wamp_NewMessage(WAMP_REGISTER, requestId);
	JSON *options = json_NewObject();
	if (policy)
		json_ObjectAddStringz(options, "invoke", wamp_PolicyName(policy));
	if (match)
		json_ObjectAddStringz(options, "match", wamp_MatchName(match));
	if (priority)
		json_ObjectAddInteger(options, "_priority", priority);
	json_ArrayAdd(json, options);
	json_ArrayAddStringz(json, procedure);

	wamp_SendJsonHeap(wamp, json);

	szDelete(newprocedure);

	return requestId;
}

static JSON *wamp_RedirectHelper(WAMP *wamp, long long requestId, long long registrationId, const char *procedure, JSON *details, JSON *list, JSON *dict, void *data)
// Called when a redirected call is invoked
{
	wamp_redirect_info_t *redirectInfo = (wamp_redirect_info_t *)data;

	int ok = 1;

	if (redirectInfo->cbModifier)
		ok = (*redirectInfo->cbModifier)(wamp, &procedure, &details, &list, &dict);

	if (ok) {
		JSON *json = wamp_NewMessage(WAMP_CALL, requestId);
		json_ArrayAdd(json, details ? details : json_NewObject());
		json_ArrayAddStringz(json, procedure);
		AddArguments(json, list, dict);
		wamp_HandleCall(wamp, json, true);
	}

	return NULL;
}

API long long wamp_RedirectCall(WAMP *wamp, int policy, int match, int priority, const char *procedure, WAMPCB_Registered cbRegistered, WAMPCB_Modifier cbModifier)
{
	wamp_redirect_info_t *redirectInfo = NEW(wamp_redirect_info_t, 1);
	redirectInfo->cbModifier = cbModifier;

	long long result = wamp_Register(wamp, policy, match, priority, procedure, cbRegistered, wamp_RedirectHelper, redirectInfo);

	return result;
}

API long long wamp_Unregister(WAMP *wamp, long long registrationId, WAMPCB_Unregistered cbUnregistered)
// Unregister a callee given either the subscriptionId or the requstId.
// Returns 0 if we were not registered on this registration ID or are already unregistering
{
	if (!wamp) return 0;

	client_registration_t *reg = (client_registration_t *)idmap_GetValue(wamp->clientRegistrationsReg, registrationId);
	if (!reg)
		reg = (client_registration_t *)idmap_GetValue(wamp->clientRegistrationsReq, registrationId);

	long long requestId;

	if (reg && !reg->unrequestId) {
		long long requestId = wamp_RandomId();

		reg->unrequestId = requestId;
		reg->cbUnregistered = cbUnregistered;
		idmap_Add(wamp->clientRegistrationsUnreq, requestId, reg);

		JSON *json = wamp_NewMessage(WAMP_UNREGISTER, requestId);
		json_ArrayAddInteger(json, registrationId);
		wamp_SendJsonHeap(wamp, json);
	} else {
		requestId = 0;
	}

	return requestId;
}

API JSON *wamp_MessageHello(const char *realmName, JSON *caller, JSON *callee, JSON *publisher, JSON *subscriber)
{
	JSON *message = wamp_NewMessage(WAMP_HELLO, 0);
	json_ArrayAddStringz(message, realmName);

	JSON *roles = json_NewObject();
	if (caller)			json_ObjectAdd(roles, "caller", caller);
	if (callee)			json_ObjectAdd(roles, "callee", callee);
	if (publisher)		json_ObjectAdd(roles, "publisher", publisher);
	if (subscriber)		json_ObjectAdd(roles, "subscriber", subscriber);

	JSON *opts = json_NewObject();
	json_ObjectAddStringz(opts, "agent", "MTWAMP");
	json_ObjectAdd(opts, "roles", roles);

	json_ArrayAdd(message, opts);

	return message;
}

API void wamp_Hello(WAMP *wamp, const char *realmName, JSON *caller, JSON *callee, JSON *publisher, JSON *subscriber, WAMPCB_Welcome cbWelcome)
// Sends a WAMP_HELLO message for the realm given.
// opts can be NULL or json_NewObject() to have no options, otherwise should be a JSON_OBJECT
// Each of caller, callee, publisher and subscriber can be one of the following:
// NULL				- We are not offering this role
// json_NewObject()	- We are offering the role, but adding no options
// JSON*			- The options we wish to use for this role
{
	if (!wamp || !realmName) return;

	wamp->cbWelcome = cbWelcome;
	wamp_SetRealm(wamp, realmName);

	JSON *hello = wamp_MessageHello(realmName, caller, callee, publisher, subscriber);

	wamp_SendJsonHeap(wamp, hello);
}

API void wamp_HelloIn(WAMP *wamp, const char *realmName, JSON *caller, JSON *callee, JSON *publisher, JSON *subscriber)
// Receives a WAMP_HELLO message for the realm given.
{
	if (!wamp || !realmName) return;

	JSON *hello = wamp_MessageHello(realmName, caller, callee, publisher, subscriber);

	wamp_Receive(wamp, hello);
}

// Some helper functions for returning results from invoked procedures
API JSON *wamp_ResultStringz(const char *string)
{
	JSON *result = json_NewArray();
	json_ArrayAdd(result, json_NewObject());

	JSON *list = json_NewArray();
	json_ArrayAddStringz(list, string);
	json_ArrayAdd(result, list);

	return result;
}

API int wamp_SendYield(WAMP *wamp, long long requestId, JSON *options, JSON *list/*=NULL*/, JSON *dict/*=NULL*/)
{
	if (wamp) {
		if (!options) options = json_NewObject();
		JSON *yield = wamp_NewMessage(WAMP_YIELD, requestId);
		json_ArrayAdd(yield, options);
		AddArguments(yield, list, dict);

		wamp_SendJsonHeap(wamp, yield);
	}

	return 0;
}

API int wamp_Dispatch(WAMP *wamp, int type, JSON *json)
// Does whatever needs to be done to deal with a message of type 'type'
// The JSON* provided is definitely an array with at least one element.
// Returns 0 or an error code
{
LogString(wamp, "WAMP IN", json_Render(json));

	int err = wamp_Validate(wamp, type, json);
	if (err) return err;

	if (type == WAMP_ABORT) {							// We sent a HELLO, the far end didn't like it
		if (wamp->cbWelcome) {
			const char *uri = json_ArrayStringzAt(json, 2);

			(*wamp->cbWelcome)(wamp, 0, uri, json_ArrayAt(json, 1));
		}
		return 1;
	}

	if (type != WAMP_HELLO && type != WAMP_WELCOME && !wamp->sessionId) {			// Trying to say something other than hello and we've not shaken hands yet
		wamp_Abort(wamp, errInvalidArgument, "You have not been welcomed");
		wamp_Delete(wamp);
		return 1;
	}

	WAMP *fwdWamp = NULL;							// Assume we're not forwarding this message

	WAMPCB_Handler fn = (WAMPCB_Handler)idmap_GetValue(wamp->handlers, (long long)type);
//Log("WA: Specific handler = %p", fn);
	if (fn) {
		int result = (*fn)(wamp, type, json);
		if (result != -1) return result;
	}

	if (wamp->realm) {
		WAMPCB_Handler fn = (WAMPCB_Handler)idmap_GetValue(wamp->realm->redirect, (long long)type);

		if (fn) {
			int result = (*fn)(wamp, type, json);
			if (result != -1) return result;
		}

//Log("WA: Have realm '%s', wantForward = %d, to %s", wamp->realm->name, wamp->realm->wantForwardSubscriptions, wamp->realm->forwardSubscriptions ? wamp->realm->forwardSubscriptions : NULL);
		if (wamp_IsSubscriptionType(type)) {
			if (wamp->realm->wantForwardSubscriptions) {
				fwdWamp = wamp_ByName(wamp->realm->forwardSubscriptions);		// Will be NULL if it's not valid
				if (!fwdWamp) {													// Asked to forward but our wamp's gone...
					wamp_SendError(wamp, 0, 0, NULL, "wamp.cannot_handle_subscriptions", NULL, NULL);
					return 2;
				}
			}
		}

		if (wamp_IsCallType(type)) {
		}
	} else {
//Log("WA: WAMP \"%s\"(%p) with no realm...", wamp_Name(wamp), wamp);
	}

//Log("WA: Dispatching type %s", wamp_TypeName(type));
	switch (type) {
	case WAMP_HELLO:		return wamp_HandleHello(wamp, json);
	case WAMP_WELCOME:		return wamp_HandleWelcome(wamp, json);
	case WAMP_GOODBYE:		return wamp_HandleGoodbye(wamp, json);
	case WAMP_ERROR:		return wamp_HandleError(wamp, json);
	case WAMP_REGISTER:		return wamp_HandleRegister(wamp, json);
	case WAMP_REGISTERED:	return wamp_HandleRegistered(wamp, json);
	case WAMP_UNREGISTER:	return wamp_HandleUnregister(wamp, json);
	case WAMP_UNREGISTERED:	return wamp_HandleUnregistered(wamp, json);
	case WAMP_CALL:			return wamp_HandleCall(wamp, json);
	case WAMP_INVOCATION:	return wamp_HandleInvocation(wamp, json);
	case WAMP_YIELD:		return wamp_HandleYield(wamp, json);
	case WAMP_RESULT:		return wamp_HandleResult(wamp, json);
	case WAMP_CANCEL:		return wamp_HandleCancel(wamp, json);
	case WAMP_SUBSCRIBE:	return fwdWamp ? wamp_ForwardSubscribe(wamp, fwdWamp, json)
											  : wamp_HandleSubscribe(wamp, json);
	case WAMP_SUBSCRIBED:	return fwdWamp ? wamp_ForwardSubscribed(wamp, fwdWamp, json)
											  : wamp_HandleSubscribed(wamp, json);
	case WAMP_UNSUBSCRIBE:	return fwdWamp ? wamp_ForwardUnsubscribe(wamp, fwdWamp, json)
											  : wamp_HandleUnsubscribe(wamp, json);
	case WAMP_UNSUBSCRIBED:	return fwdWamp ? wamp_ForwardUnsubscribed(wamp, fwdWamp, json)
											  : wamp_HandleUnsubscribed(wamp, json);
	case WAMP_PUBLISH:		return fwdWamp ? wamp_ForwardPublish(wamp, fwdWamp, json)
											  : wamp_HandlePublish(wamp, json);
	case WAMP_PUBLISHED:	return fwdWamp ? wamp_ForwardPublished(wamp, fwdWamp, json)
											  : wamp_HandlePublished(wamp, json);
	case WAMP_EVENT:		return fwdWamp ? wamp_ForwardEvent(wamp, fwdWamp, json)
											  : wamp_HandleEvent(wamp, json);
	default:
		{
			const char *msg = hprintf(NULL, "I can't handle a '%s' message (yet)", wamp_TypeName(type));
			wamp_SendErrorStr(wamp, type, 0, msg, errInvalidArgument, NULL, NULL);
			szDelete(msg);
		}
	}

	return 0;
}
