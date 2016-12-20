
#include <stdio.h>
#include <time.h>

#include <mtwamp.h>
#include <mtwamputil.h>

const char *g_host = NULL;				// The host and realm to which we're connecting
const char *g_realm = NULL;
const char *g_procedure = NULL;
JSON *g_list = NULL;
JSON *g_dict = NULL;
JSON *g_opts = NULL;

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

	wamp_Call(wamp, g_procedure, g_opts, g_list, g_dict, OnResult);
}

int OnWampConnected(WAMP *wamp, const char *mesg)
// Called when an outgoing WAMP connection succeeds (wamp != NULL) or fails (wamp == NULL)
{
	if (wamp) {
		wamp_Hello(wamp, g_realm, json_NewObject(), json_NewObject(), json_NewObject(), json_NewObject(), OnWelcome);
	} else {
		fprintf(stderr, "wampcall: Failed to connect to %s: %s\n", g_host, mesg);
	}
}

void main(int argc, const char *argv[])
{
	if (argc < 4 || argc > 7) {
		fprintf(stderr, "Usage: wampcall server realm procedure [list] [dict] [opts]\n");
		exit(1);
	}

	g_host = argv[1];
	g_realm = argv[2];
	g_procedure = argv[3];
	if (argc > 4) {
		g_list = json_Parse(argv+4);
		if (json_Error(g_list)) {
			fprintf(stderr, "wampcall: list is not valid - %s\n", json_Render(g_list));
		}
	}

	if (argc > 5) {
		g_dict = json_Parse(argv+5);
		if (json_Error(g_dict)) {
			fprintf(stderr, "wampcall: dict is not valid - %s\n", json_Render(g_dict));
		}
	}

	if (argc > 6) {
		g_opts = json_Parse(argv+6);
		if (json_Error(g_opts)) {
			fprintf(stderr, "wampcall: opts is not valid - %s\n", json_Render(g_opts));
		}
	}

	CHANPOOL *pool = chan_PoolNew();							// Create a new channel pool

	wamp_Connect(pool, g_host, OnWampConnected);

	chan_EventLoop(pool);										// Pass control to the event loop
}
