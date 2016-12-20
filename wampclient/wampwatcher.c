
#include <stdio.h>
#include <time.h>

#include <mtwamp.h>
#include <mtwamputil.h>

const char *g_host = NULL;				// The host and realm to which we're connecting
const char *g_realm = NULL;
const char *g_topic = "";
int g_type = 0;

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

void OnEvent(WAMP *wamp, long long subscriptionId, const char *topic, JSON *details, JSON *list, JSON *dict)
{
	printf("\n");
	printf("Event:   %Ld (%s)\n", subscriptionId, topic);
	if (json_ObjectCount(details))	printf("Details: %s\n", json_Render(details));
	if (json_ArrayCount(list))		printf("List:    %s\n", json_Render(list));
	if (json_ObjectCount(dict))		printf("Dict:    %s\n", json_Render(dict));
}

void OnSubscribed(WAMP *wamp, long long requestId, long long subscriptionId, JSON *details, const char *uri)
{
	if (subscriptionId) {
		printf("*** Subscribed to %Ld:%Ld\n", requestId, subscriptionId);
	} else {
		printf("*** Subscription %Ld for %s failed: %s\n", requestId, uri, json_Render(details));
	}
}

void OnWelcome(WAMP *wamp, long long sessionId, const char *uri, JSON *details)
{
	if (!sessionId) {
		printf("*** I am not welcome: %s\n", uri);
		printf("*** Details:          %s\n", json_Render(details));
		wamp_Delete(wamp);									// This will result in use dropping out from the event loop
		return;
	}

	printf("Welcomed as session %Ld: %s\n", sessionId, json_Render(details));

	long long reqId = wamp_Subscribe(wamp, g_type, g_topic, NULL, OnSubscribed, OnEvent);
}

int OnWampConnected(WAMP *wamp, const char *mesg)
// Called when an outgoing WAMP connection succeeds (wamp != NULL) or fails (wamp == NULL)
{
	printf("Connected on wamp(%s)\n", wamp_Name(wamp));

	if (wamp) {
		wamp_Hello(wamp, g_realm, json_NewObject(), json_NewObject(), json_NewObject(), json_NewObject(), OnWelcome);
	} else {
		fprintf(stderr, "Could not connect: %s\n", mesg);
	}
}

void main(int argc, const char *argv[])
{
	if (argc >= 2 && !strcmp(argv[1], "-v")) {
		wamp_Logger(vLog);											// Catch log entries from the wamp_ library
		chan_Logger(vLog);											// Catch log entries from the chan_ library
		ws_Logger(vLog);											// Catch log entries from the ws_ library (websocket)
		json_Logger(vLog);											// Catch log entries from the json_ library

		argv++;
	}

	if (argc < 3 || argc > 5) {
		fprintf(stderr, "Usage: wampwatcher server realm [topic] [type]\n");
		exit(1);
	}

	g_host = argv[1];
	g_realm = argv[2];
	g_topic = argc > 3 ? argv[3] : NULL;
	const char *sType = argc > 4 ? argv[4] : NULL;

	if (sType) {
		if (!strcasecmp(sType, "exact")) g_type = WAMP_EXACT;
		else if (!strcasecmp(sType, "prefix")) g_type = WAMP_PREFIX;
		else if (!strcasecmp(sType, "wildcard")) g_type = WAMP_WILDCARD;
		else {
			fprintf(stderr, "wampwatcher: Type must be exact, prefix or wildcard\n");
			exit(1);
		}
	}
	CHANPOOL *pool = chan_PoolNew();							// Create a new channel pool

	wamp_Connect(pool, g_host, OnWampConnected);

	chan_EventLoop(pool);										// Pass control to the event loop

	printf("Disconnected\n");									// Event loop has exited - no wamp channels left
}
