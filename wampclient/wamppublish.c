
#include <stdio.h>
#include <time.h>

#include <mtwamp.h>
#include <mtwamputil.h>

const char *g_host = NULL;				// The host and realm to which we're connecting
const char *g_realm = NULL;
const char *g_topic = NULL;
JSON *g_list = NULL;
JSON *g_dict = NULL;
JSON *g_opts = NULL;

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
	wamp_Delete(wamp);
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

	wamp_Publish(wamp, g_topic, g_opts, g_list, g_dict, OnPublished);
}

int OnWampConnected(WAMP *wamp, const char *mesg)
// Called when an outgoing WAMP connection succeeds (wamp != NULL) or fails (wamp == NULL)
{
	if (wamp) {
		wamp_Hello(wamp, g_realm, json_NewObject(), json_NewObject(), json_NewObject(), json_NewObject(), OnWelcome);
	} else {
		fprintf(stderr, "wamppublish: Failed to connect to %s: %s\n", g_host, mesg);
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

	if (argc < 4 || argc > 7) {
		fprintf(stderr, "Usage: wamppublish server realm topic [list] [dict] [opts]\n");
		exit(1);
	}

	g_host = argv[1];
	g_realm = argv[2];
	g_topic = argv[3];
	if (argc > 4) {
		g_list = json_Parse(argv+4);
		if (json_Error(g_list)) {
			fprintf(stderr, "wamppublish: list is not valid - %s\n", json_Render(g_list));
		}
	}

	if (argc > 5) {
		g_dict = json_Parse(argv+5);
		if (json_Error(g_dict)) {
			fprintf(stderr, "wamppublish: dict is not valid - %s\n", json_Render(g_dict));
		}
	}

	if (argc > 6) {
		g_opts = json_Parse(argv+6);
		if (json_Error(g_opts)) {
			fprintf(stderr, "wamppublish: opts is not valid - %s\n", json_Render(g_opts));
		}
	}

	CHANPOOL *pool = chan_PoolNew();							// Create a new channel pool

	wamp_Connect(pool, g_host, OnWampConnected);

	chan_EventLoop(pool);										// Pass control to the event loop
}
