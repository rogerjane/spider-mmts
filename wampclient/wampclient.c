
#include <stdio.h>
#include <time.h>

#include <mtwamp.h>
#include <mtwamputil.h>

void vLog(const char *szFmt, va_list ap)
// Logging is purely optional, but a function like this can be passed to wamp_Logger(), ws_Logger(), chan_Logger() or json_Logger()
// and receives any logging that those classes output.
{
	time_t now=time(NULL);
	struct tm *tm = gmtime(&now);

	printf("%02d-%02d-%02d %02d:%02d:%02d ",
			tm->tm_mday, tm->tm_mon+1, tm->tm_year % 100,
			tm->tm_hour, tm->tm_min, tm->tm_sec);

	vprintf(szFmt, ap);
	printf("\n");
}

void Log(const char *szFmt, ...)
// Use the same logging function ourselves
{
	va_list ap;

	va_start(ap, szFmt);
	vLog(szFmt, ap);
	va_end(ap);
}

void OnUnsubscribed(WAMP *wamp, long long requestId, long long subscriptionId, JSON *details, const char *uri)
// We have been successfully unsubscribed from a topic or an error has occurred
// requestId is the ID that was returned from wamp_Unsubscribe()
// subscriptionId is the iD that the broker had assigned to the subscription or 0 if this is an error
// details is only set on an error and is the error detail returned by the broker
// uri is the error URI if this is an error
{
	if (subscriptionId) {
		printf("*** Unsubscribed from %Ld:%Ld (%s)\n", requestId, subscriptionId, uri);
	} else {
		printf("*** Unsubscription %Ld of %s failed: %s\n", requestId, uri, json_Render(details));
	}
}

void OnEvent(WAMP *wamp, long long subscriptionId, const char *topic, JSON *details, JSON *list, JSON *dict)
// An event that we subcribe to has been triggered.
// subscriptionId is the subscription ID (we get informed of this in the OnSubscribed function)
// topic is the topic that's been published
// details is any details passed by the broker
// list and dict at the payload as sent by the publisher
{
	printf("======================== - Event occurred\n");
	printf("Event:   %Ld (%s)\n", subscriptionId, topic);
	printf("Details: %s\n", json_Render(details));
	printf("List:    %s\n", json_Render(list));
	printf("Dict:    %s\n", json_Render(dict));
	printf("------------------------\n");
}

void OnSubscribed(WAMP *wamp, long long requestId, long long subscriptionId, JSON *details, const char *uri)
// We have been successfully subscribed to a topic or an error has occurred
// requestId is the ID that was returned from wamp_Subscribe()
// subscriptionId is the iD that the broker has assigned to the subscription or 0 if this is an error
// details is only set on an error and is the error detail returned by the broker
// uri is the error URI if this is an error
{
	if (subscriptionId) {
		printf("*** Subscribed to %Ld:%Ld\n", requestId, subscriptionId);
	} else {
		printf("*** Subscription %Ld for %s failed: %s\n", requestId, uri, json_Render(details));
	}
}

void OnPublished(WAMP *wamp, long long requestId, long long publicationId, JSON *details, const char *uri)
// We have successfully published a topic or an error has occurred
// requestId is the ID thw was returned from wamp_Publish()
// publicationID is the ID assigned to this instance of the publication or 0 if this is an error
// details is only set on an error and is the error detail returned by the broker
// uri is the published topic or the error URI
{
	if (publicationId) {
		printf("*** Published %Ld:%Ld (%s)\n", requestId, publicationId, uri);
	} else {
		printf("*** Publication %Ld of %s failed: %s\n", requestId, uri, json_Render(details));
	}
}

void OnResult(WAMP *wamp, long long requestId, const char *procedure, const char *uri, JSON *details, JSON *list, JSON *dict)
// We've received a result from a call we've made.
// If uri != NULL then it's an error
{
	printf("===================== - Result has been returned\n");
	printf("Result:  %Ld (%s)\n", requestId, procedure);
	if (uri) printf("Error:   %s\n", uri);
	printf("Details: %s\n", json_Render(details));
	printf("List:    %s\n", json_Render(list));
	printf("Dict:    %s\n", json_Render(dict));
	printf("---------------------\n");
}

void OnRegistered(WAMP *wamp, long long requestId, long long registrationId, JSON *details, const char *uri)
// We have successfully registeded a procedure or an error has occurred
// requestId is the ID that was returned from wamp_Register()
// registrationId is the ID assigned to this callee or 0 if this is an error
// details is only set on an error and is the error detail returned by the router
// uri is the published topic or the error URI
{
	if (registrationId) {
		printf("*** Registered %Ld:%Ld (%s)\n", requestId, registrationId, uri);
	} else {
		printf("*** Registration %Ld failed: %s - %s\n", requestId, uri, json_Render(details));
	}
}

void OnUnregistered(WAMP *wamp, long long requestId, long long registrationId, JSON *details, const char *uri)
// We have been successfully unregistered from a topic or an error has occurred
// requestId is the ID that was returned from wamp_Unregister()
// registrationId is the iD that the dealer had assigned to the registration or 0 if this is an error
// details is only set on an error and is the error detail returned by the dealer
// uri is the error URI if this is an error
{
	if (registrationId) {
		printf("*** Unregistered from %Ld:%Ld (%s)\n", requestId, registrationId, uri);
	} else {
		printf("*** Unregistered %Ld of %s failed: %s\n", requestId, uri, json_Render(details));
	}
}

JSON *OnInvoke(WAMP *wamp, long long requestId, long long registrationId, const char *procedure, JSON *details, JSON *list, JSON *dict)
// We are a callee being invoked.
// Sucess:	return [] or [{details}] or [{details},[list]] or [{details},[list],{dict}]
// Nothing:	return NULL to have nothing returned.  wamp_Result() should be called later
// Error:	return ["uri" ["uri",{details}] or ["uri",{details},[list]] or ["uri",{details},[list],{dict}]
// If it's more convenient, this function can call wamp_Result() and then return NULL.
{
	printf("===================== - I have been called\n");
	printf("Invoked:      %Ld (%s)\n", requestId, procedure);
	printf("Registration: %Ld\n", registrationId);
	printf("Details:      %s\n", json_Render(details));
	printf("List:         %s\n", json_Render(list));
	printf("Dict:         %s\n", json_Render(dict));
	printf("---------------------\n");

	JSON *resultList = json_ParseText("[\"This is a string\"]");

	wamp_Result(wamp, requestId, NULL, resultList, NULL);
	return NULL;

	return wamp_ResultStringz("Welcome World!");			// Builds a result as [{},[string]]
}

void OnWelcome(WAMP *wamp, long long sessionId, const char *uri, JSON *details)
// Called on welcome or abort
// It's an abort if sessionId = 0 and 'uri' is non-null, in which case it'll contain the error URI
{
	if (!sessionId) {
		printf("*** I am not welcome: %s\n", uri);
		printf("*** Details:          %s\n", json_Render(details));
		wamp_Delete(wamp);									// This will result in use dropping out from the event loop
		return;
	}

	printf("*** I have been welcomed as session %Ld\n", sessionId);
	printf("*** Server details: %s\n", json_Render(details));

	// Subscribe to every topic (any topic prefixed with "")
	// OnSubscribed() will be called once the subscription is accepted (or rejected)
	// OnEvent() is called every time the topic is published
	wamp_Subscribe(wamp, WAMP_PREFIX, "", NULL, OnSubscribed, OnEvent);

	// Publish a topic with no options, list or dict.  When it has been published (or on failure), call OnPublished()
	wamp_Publish(wamp, "I.am.alive", NULL, NULL, NULL, OnPublished);

	// Register a functino we'll handle here
	// OnRegistered will be called when it's registered (or if there is an error in doing so)
	// OnInvoke will be called when it is invoked
	wamp_Register(wamp, WAMP_EXACT, WAMP_INVOKE_SINGLE, 0, "hello.world", OnRegistered, OnInvoke);

	// Call our function with no options, list or dict
	wamp_Call(wamp, "hello.world", NULL, NULL, NULL, OnResult);
}

int OnWampConnected(WAMP *wamp, const char *mesg)
// Called when an outgoing WAMP connection succeeds (wamp != NULL) or fails (wamp == NULL)
{
	printf("*** Connected - wamp = %s\n", wamp_Name(wamp));

	if (wamp) {
		// Send a hello message to the router, saying we're a caller, callee, publisher and subscriber
		wamp_Hello(wamp, "gpconnect", json_NewObject(), json_NewObject(), json_NewObject(), json_NewObject(), OnWelcome);
	} else {
		fprintf(stderr, "wampclient: Could not connect - %s\n", mesg);
	}
}

void main(int argc, const char *argv[])
{
	wamp_Logger(vLog);											// Catch log entries from the wamp_ library
	chan_Logger(vLog);											// Catch log entries from the chan_ library
	ws_Logger(vLog);											// Catch log entries from the ws_ library (websocket)
	json_Logger(vLog);											// Catch log entries from the json_ library

	CHANPOOL *pool = chan_PoolNew();							// Create a new channel pool

	// Connect to an instance of spider.  OnWampConnected() will ALWAYS be called either on success or failure
	wamp_Connect(pool, "localhost:4512", OnWampConnected);
//	wamp_Connect(pool, "10.198.165.88:8080/ws", OnWampConnected);

	chan_EventLoop(pool);										// Pass control to the event loop

	printf("*** Loop ended\n");									// Event loop has exited - no wamp channels left
}
