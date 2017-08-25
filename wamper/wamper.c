
// 30-01-17 RJ 0.00 wamper - a utility to allow someone to 'talk' to a wamp server
// 01-08-17 RJ 0.04 Added better (getopt) command line processing

#include <ctype.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <termio.h>
#include <time.h>
#include <unistd.h>

#include <heapstrings.h>
#include <mtmacro.h>
#include <mtwamp.h>
#include <mtwamputil.h>
#include <mtstrings.h>
#include <mime.h>

#define	VERSION	"0.04"

// Little test suite to check that a WAMP server is doing what we'd like it to do.

// TODO:
//	Understand progressive results

static const char *g_host = NULL;
static const char *g_realm = NULL;

static CHANPOOL *g_pool = NULL;

static int g_connection = 0;					// Which WAMP we'll use
static int g_defaultConnection = 0;				// Default if not told otherwise
static int g_numConnections = 0;				// The number of connections we have
static const char **g_connections;				// Our full list of wamps

static CHAN *chStdin = NULL;					// Channel for our command input

static char *input = NULL;						// Command input buffer
static int inlen=0;
static int inpos=0;								// Cursor position in line

static int inputTty = 1;						// 1 when input is from a terminal

static bool g_Prompting = false;
static const char *prompt = "WAMP> ";

static bool quiet = false;						// Be chatty about things
static bool pretty = false;						// Display JSON prettily

static bool quitting = false;					// True once we've entered a quit command

static void history_Save(void);
static void Unprompt(void);
static int OnStdin(CHAN *chan);
static void OnStdinClosed(CHAN *chan);
static void ProcessCommand(const char *buf);

typedef struct calldata_t {
	long long requestId;
	const char *procedure;
	long long start;
} calldata_t;

static IDMAP *calleeRequestIdMap = NULL;		// When registering, this is a map to information on the registration against reqId
static IDMAP *calleeRegistrationIdMap = NULL;	// This is the same information as in calleeRequestIdMap, but against regId
static IDMAP *outstandingCallsMap = NULL;		// map of outstanding call IDs

static JSON *g_options = NULL;					// Options where it succeeds a command name (e.g. "publish {...} my.event")

static bool snomedMode = false;

static HLIST *g_commands = NULL;				// Commands we'll execute instead of stdin

static const char *StopwatchRender(long long period)
{
	static char buf[30];

	long long msecs = period / 1000;
	long long secs = msecs / 1000;
	long long mins = secs / 60;
	msecs %= 1000;
	secs %= 60;
	snprintf(buf, sizeof(buf), "%lld:%02lld.%03lld", mins, secs, msecs);

	return buf;
}

static long long StopwatchNow()
{
	struct timeval tv;

	gettimeofday(&tv, NULL);

	return (long long)tv.tv_sec * 1000000 + (long long)tv.tv_usec;
}

static int ScreenWidth()
{
	static int width = 0;

	if (!width) {
		const char *cols = getenv("COLUMNS");

		if (cols)
			width = atoi(cols);
		if (!width)
			width = 80;
	}

	return width;
}

static void Colour(int colour, int bright)
{
	if (inputTty)
		printf("\033[%d;%dm", colour, bright);
}

static void GotoXy(int x, int y)
{
	printf("\033[%d;%dH", y, x);
}

static void ClearScreen()
{
	printf("\033[2J");
	GotoXy(1,1);
}

static void ClearEol()
{
	printf("\033[K");
}

void Black()		{ Colour(30, 0); }
void DarkRed()		{ Colour(31, 0); }
void DarkGreen()	{ Colour(32, 0); }
void Brown()		{ Colour(33, 0); }
void DarkBlue()		{ Colour(34, 0); }
void DarkMagenta()	{ Colour(35, 0); }
void DarkCyan()		{ Colour(36, 0); }
void LightGrey()	{ Colour(37, 0); }
void DarkGrey()		{ Colour(30, 1); }
void Red()			{ Colour(31, 1); }
void Green()		{ Colour(32, 1); }
void Yellow()		{ Colour(33, 1); }
void Blue()			{ Colour(34, 1); }
void Magenta()		{ Colour(35, 1); }
void Cyan()			{ Colour(36, 1); }
void White()		{ Colour(37, 1); }

SSMAP *varMap = NULL;

void VarSet(const char *name, const char *value)
{
	if (!varMap) varMap = ssmap_New();

	if (value)
		ssmap_Add(varMap, name, value);
	else
		ssmap_DeleteKey(varMap, name);
}

void VarSet(const char *name, long long value)
{
	char buf[28];

	snprintf(buf, sizeof(buf), "%lld", value);

	return VarSet(name, buf);
}

const char *VarValue(const char *name)
{
	if (!varMap) varMap = ssmap_New();

	return ssmap_GetValue(varMap, name);
}

int DoVars(const char *args)
{
	ssmap_Sort(varMap, strcmp);
	ssmap_Reset(varMap);
	const char *name;
	const char *value;
	while (ssmap_GetNextEntry(varMap, &name, &value)) {
		printf("%s = %s\n", name, value);
	}

	return 0;
}

static void cbreak(bool state)
// Turns cbreak mode on or off (needs to be on for our prompting)
{

	static struct termio orig_ioctl;
	static struct termio cbreak_ioctl;
	static bool ioctlSaved = false;

	if (!ioctlSaved) {
		int n = 100;						// Timeout on character get

		ioctl(0, TCGETA, &orig_ioctl);		// Get initial state safe

		cbreak_ioctl = orig_ioctl;			// Set up a new one, mangled to our needs
		cbreak_ioctl.c_lflag = 0;
		cbreak_ioctl.c_cc[4] = 0;             /* VMIN */
		cbreak_ioctl.c_cc[5] = n;             /* VTIME */

		cbreak_ioctl.c_iflag = 0x0c06;
		cbreak_ioctl.c_oflag = 0x0005;
		cbreak_ioctl.c_cflag = 0x04be;
		cbreak_ioctl.c_lflag = 0x0031;
		cbreak_ioctl.c_line = 0x0000;

		ioctlSaved = true;
	}

	if (state) {
		ioctl(0, TCSETA, &cbreak_ioctl);
	} else {
		ioctl(0, TCSETA, &orig_ioctl);				// Not quite sure why this makes sense, but it seems to work
		ioctl(1, TCSETA, &orig_ioctl);
	}
}

static void ExitNicely()
{
	history_Save();

	cbreak(false);

	Unprompt();									// Likely not at left side
	LightGrey();

	exit(0);
}

static int NewConnection(WAMP *wamp)
{
	int connection = 0;

	if (wamp) {
		const char *wampName = wamp_Name(wamp);

		for (int i=0;i<g_numConnections;i++) {
			if (g_connections[i] == NULL) {
				connection = i+1;
				break;
			}
		}

		if (!connection) {
			RENEW(g_connections, const char*, ++g_numConnections);
			connection = g_numConnections;
		}

		g_connections[connection-1] = strdup(wampName);
	}

	return connection;
}

static void SetDefaultConnection(int i)
{
	static const char *connectionPrompt = NULL;

	if (i >= 1 && i<=g_numConnections && g_connections[i-1]) {
		g_defaultConnection = i;

		szDelete(connectionPrompt);
		connectionPrompt = hprintf(NULL, "WAMP %d>", g_defaultConnection);
		prompt = connectionPrompt;
	} else {
		g_defaultConnection = 0;
		prompt = "WAMP>";
	}
}

static bool DeleteConnection(int conn)
{
	if (conn >= 1 && conn<=g_numConnections) {
		int i = conn-1;											// Index into array of connections

		WAMP *wamp = wamp_ByName(g_connections[i]);

		wamp_Delete(wamp);
		szDelete(g_connections[i]);
		g_connections[i] = NULL;
		if (g_defaultConnection == conn) {					// We've closed the default channel, look for...
			int j;

			for (j=i-1;j>=0;j--)							// ... The next lowest
				if (g_connections[j])
					break;

			if (j < 0)										// ... Or failing that, the next highest
				for (j=i+1;j<g_numConnections;j++)
					if (g_connections[j])
						break;

			if (j < 0 || j >= g_numConnections)
				j=-1;

			SetDefaultConnection(j+1);						// Set to newly found wamp if we've found one, otherwise 0
		}

		return true;
	} else {
		return false;
	}
}

static int SetConnection(int conn)
{
	if (conn == -1) conn = g_defaultConnection;

	if (conn >= 1 && conn<=g_numConnections && g_connections[conn-1]) {
		g_connection = conn;
		if (!g_defaultConnection)
			SetDefaultConnection(conn);
		VarSet("connection", conn);

		return g_connection;
	}

	return 0;
}

static int ConnectionNumber(WAMP *wamp)
{
	for (int i=0;i<g_numConnections;i++) {
		if (wamp_ByName(g_connections[i]) == wamp) {
			return i+1;
		}
	}

	return 0;
}

static WAMP *CurrentWamp()
{
	WAMP *wamp = NULL;

	if (g_connection >= 1 && g_connection <= g_numConnections)
		wamp = wamp_ByName(g_connections[g_connection-1]);

	return wamp;
}

static void ListConnections()
{
	for (int connection = 0; connection < g_numConnections; connection++) {
		if (g_connections[connection]) {
			WAMP *wamp = wamp_ByName(g_connections[connection]);

			if (wamp) {
				printf("%d: %16Ld is %s on %s", connection+1, wamp_Session(wamp), wamp_RealmName(wamp), wamp_Peer(wamp));
				if (0) {
					const char *authid = wamp_AuthId(wamp);
					const char *authrole = wamp_AuthRole(wamp);
					const char *authmethod = wamp_AuthMethod(wamp);
					const char *authprovider = wamp_AuthProvider(wamp);

					printf(" (%s,%s,%s,%s)", authid, authrole,authmethod,authprovider);
				}
				printf("\n");
			} else {
				printf("%d: Disconnected (%s)\n", connection+1, g_connections[connection]);
				DeleteConnection(connection+1);
			}
		}
	}
}

static const char *SkipSpaces(const char *text)
{
	while (isspace(*text))
		text++;

	return text;
}

static void righttrim(char *text)
{
	if (text && *text) {
		char *chp = text+strlen(text)-1;

		while (chp >= text && isspace(*chp))
			chp--;

		chp[1] = '\0';
	}
}

static void Prompt()
{
	if (!inputTty) return;

	if (!g_Prompting) {
		Green();
		printf("%s", prompt);
		White();
		fflush(stdout);
		g_Prompting = true;
	}
}

static void Unprompt()
{
	if (!inputTty) return;

	if (g_Prompting) {
		int i;

		for (i = inlen+strlen(prompt); i>0; i--) {
			printf("\b \b");
		}
		Yellow();
		fflush(stdout);
		g_Prompting = false;
	}
}

static void Reprompt()
{
	if (!inputTty) return;

	if (!g_Prompting) {

		Prompt();
		if (inlen) {
			printf("%.*s", inlen, input);
			int i;

			for (i=inlen; i>inpos; i--)				// Put the cursor in the right place
				printf("\b");
		}
		fflush(stdout);
	}
}

static void Say(const char *fmt, ...)
{
	if (inputTty) {
		va_list ap;

		va_start(ap, fmt);
		vprintf(fmt, ap);
		va_end(ap);
	}
}

static const char *TimeNow()
{
	static const char *result = NULL;
	szDelete(result);

	time_t now=time(NULL);
	struct tm *tm = gmtime(&now);

	struct timeval tp;
	int msecs;

	gettimeofday(&tp, NULL);
	msecs=tp.tv_usec / 1000;

	result = hprintf(NULL, "%02d:%02d:%02d.%03d", tm->tm_hour, tm->tm_min, tm->tm_sec, msecs);

	return result;
}

static void vLog(const char *szFmt, va_list ap)
// Logging is purely optional, but a function like this can be passed to wamp_Logger(), ws_Logger(), chan_Logger() or json_Logger()
// and receives any logging that those classes output.
{
	Unprompt();

	time_t now=time(NULL);
	struct tm *tm = gmtime(&now);

	if (inputTty) {
		printf("%02d-%02d-%02d %s ", tm->tm_mday, tm->tm_mon+1, tm->tm_year % 100, TimeNow());

		vprintf(szFmt, ap);
		printf("\n");
	}

	Reprompt();
}

static void Log(const char *szFmt, ...)
// Use the same logging function ourselves
{
	va_list ap;

	va_start(ap, szFmt);
	vLog(szFmt, ap);
	va_end(ap);
}

static int Error(const char *fmt, ...)
{
	va_list ap;

	printf("Error: ");
	va_start(ap, fmt);
	vprintf(fmt, ap);
	va_end(ap);
	printf("\n");

	return 0;
}

#if 0			// Ready for implementing unsubscribe!

static void OnUnsubscribed(WAMP *wamp, long long requestId, long long subscriptionId, JSON *details, const char *uri)
// We have been successfully unsubscribed from a topic or an error has occurred
// requestId is the ID that was returned from wamp_Unsubscribe()
// subscriptionId is the iD that the broker had assigned to the subscription or 0 if this is an error
// details is only set on an error and is the error detail returned by the broker
// uri is the error URI if this is an error
{
	Unprompt();

	if (subscriptionId) {
		Green();
		printf("*** Unsubscribed from %lld:%lld (%s)\n", requestId, subscriptionId, uri);
	} else {
		Red();
		printf("*** Unsubscription %lld of %s failed: %s\n", requestId, uri, json_Render(details, pretty));
	}

	Reprompt();
}

static void OnUnregistered(WAMP *wamp, long long requestId, long long registrationId, JSON *details, const char *uri)
// We have been successfully unregistered from a topic or an error has occurred
// requestId is the ID that was returned from wamp_Unregister()
// registrationId is the iD that the dealer had assigned to the registration or 0 if this is an error
// details is only set on an error and is the error detail returned by the dealer
// uri is the error URI if this is an error
{
	Unprompt();

	if (registrationId) {
		Green();
		printf("*** Unregistered from %lld:%lld (%s)\n", requestId, registrationId, uri);
	} else {
		Red();
		printf("*** Unregistered %lld of %s failed: %s\n", requestId, uri, json_Render(details, pretty));
	}

	Reprompt();
}

#endif

static void OnEvent(WAMP *wamp, long long subscriptionId, long long publicationId, const char *topic, JSON *details, JSON *list, JSON *dict)
// An event that we subcribe to has been triggered.
// subscriptionId is the subscription ID (we get informed of this in the OnSubscribed function)
// topic is the topic that's been published, json_Render(list)
// details is any details passed by the broker
// list and dict at the payload as sent by the publisher
{
	Unprompt();

	VarSet("eventDetails", (char*)NULL);
	VarSet("eventList", (char*)NULL);
	VarSet("eventDict", (char*)NULL);
	VarSet("event1", (char*)NULL);
	VarSet("event2", (char*)NULL);
	VarSet("event3", (char*)NULL);

	Magenta();
	printf("Event %lld for %lld (%s) on connection %d\n", publicationId, subscriptionId, topic, ConnectionNumber(wamp));
	Yellow();

	VarSet("eventDetails", json_Render(details));
	VarSet("eventList", json_Render(list));
	VarSet("eventDict", json_Render(dict));

	if (json_IsArray(list)) {
		if (json_ArrayCount(list) >= 1) VarSet("event1", json_Render(json_ArrayAt(list, 0)));
		if (json_ArrayCount(list) >= 2) VarSet("event2", json_Render(json_ArrayAt(list, 1)));
		if (json_ArrayCount(list) >= 3) VarSet("event3", json_Render(json_ArrayAt(list, 2)));
	}

	if (json_Count(details))	printf("%s\n", json_Render(details, pretty));
								printf("%s\n", json_Render(list, pretty));
	if (json_Count(dict))		printf("%s\n", json_Render(dict, pretty));

	Reprompt();
}

static void OnSubscribed(WAMP *wamp, long long requestId, long long subscriptionId, JSON *details, const char *uri)
// We have been successfully subscribed to a topic or an error has occurred
// requestId is the ID that was returned from wamp_Subscribe()
// subscriptionId is the iD that the broker has assigned to the subscription or 0 if this is an error
// details is only set on an error and is the error detail returned by the broker
// uri is the error URI if this is an error
{
	Unprompt();

	if (subscriptionId) {
		Green();
		if (!quiet) printf("*** Subscribed to %lld (%s)\n", subscriptionId, uri);
	} else {
		Red();
		printf("*** Subscription %lld for %s failed: %s\n", requestId, uri, json_Render(details, pretty));
	}

	Reprompt();
}

static void OnPublished(WAMP *wamp, long long requestId, long long publicationId, JSON *details, const char *uri)
// We have successfully published a topic or an error has occurred
// requestId is the ID thw was returned from wamp_Publish()
// publicationID is the ID assigned to this instance of the publication or 0 if this is an error
// details is only set on an error and is the error detail returned by the broker
// uri is the published topic or the error URI
{
	Unprompt();

	if (publicationId) {
		Green();
		printf("*** Published %lld:%lld (%s)\n", requestId, publicationId, uri);
	} else {
		Red();
		printf("*** Publication %lld of %s failed: %s\n", requestId, uri, json_Render(details, pretty));
	}

	Reprompt();
}

static void OnResult(WAMP *wamp, long long requestId, const char *procedure, const char *uri, JSON *details, JSON *list, JSON *dict, void *data)
// We've received a result from a call we've made.
// If uri != NULL then it's an error
{
	VarSet("resultDetails", (char*)NULL);
	VarSet("resultList", (char*)NULL);
	VarSet("resultDict", (char*)NULL);
	VarSet("result1", (char*)NULL);
	VarSet("result2", (char*)NULL);
	VarSet("result3", (char*)NULL);

	idmap_DeleteKey(outstandingCallsMap, requestId);
	calldata_t *call = (calldata_t*)data;
	long long now = StopwatchNow();

	if (uri) {
		Unprompt();

		Say("Error returned from call to %s on connection %d after %s\n", procedure, ConnectionNumber(wamp), StopwatchRender(now-call->start));
		printf("Error: %s\n", uri);
		if (details && json_ObjectCount(details))	Say("Details: %s\n", json_Render(details, pretty));
		if (list)									Say("List:    %s\n", json_Render(list, pretty));
		if (dict)									Say("Dict:    %s\n", json_Render(dict, pretty));

		Reprompt();
	} else {
		VarSet("resultDetails", json_Render(details));
		VarSet("resultList", json_Render(list));
		VarSet("resultDict", json_Render(dict));

		if (json_IsArray(list)) {
			if (json_ArrayCount(list) >= 1) VarSet("result1", json_Render(json_ArrayAt(list, 0)));
			if (json_ArrayCount(list) >= 2) VarSet("result2", json_Render(json_ArrayAt(list, 1)));
			if (json_ArrayCount(list) >= 3) VarSet("result3", json_Render(json_ArrayAt(list, 2)));
		}

		if (inputTty) {							// We weren't given things to do
			Unprompt();

		printf("List = %p\n", list);
			Cyan();
			Say("Result: %s returned after %s\n", procedure, StopwatchRender(now-call->start));
			Yellow();

			if (details && json_ObjectCount(details))	Say("Details: %s\n", json_Render(details, pretty));
			if (list)									Say("List:    %s\n", json_Render(list, pretty));
			if (dict)									Say("Dict:    %s\n", json_Render(dict, pretty));
			Reprompt();
		} else {
			if (details && json_ObjectCount(details))	printf("%s\n", json_Render(details));
			if (list)									printf("%s\n", json_Render(list));
			if (dict)									printf("%s\n", json_Render(dict));
		}
	}
}

static void OnRegistered(WAMP *wamp, long long requestId, long long registrationId, JSON *details, const char *uri)
// We have successfully registeded a procedure or an error has occurred
// requestId is the ID that was returned from wamp_Register()
// registrationId is the ID assigned to this callee or 0 if this is an error
// details is only set on an error and is the error detail returned by the router
// uri is the published topic or the error URI
{
	Unprompt();

	if (registrationId) {
		void *command = idmap_GetValue(calleeRequestIdMap, requestId);

		if (command) {
			idmap_DeleteKey(calleeRequestIdMap, requestId);
			idmap_Add(calleeRegistrationIdMap, registrationId, command);
		}
		if (!quiet) {
			Green();
			if (command) {
				printf("*** Registered %lld:%lld (%s) to do '%s'\n", requestId, registrationId, uri, (char*)command);
			} else {
				printf("*** Registered %lld:%lld (%s)\n", requestId, registrationId, uri);
			}
		}
	} else {
		Red();
		printf("*** Registration %lld failed: %s - %s\n", requestId, uri, json_Render(details, pretty));
	}

	Reprompt();
}

static JSON *OnInvoke(WAMP *wamp, long long requestId, long long registrationId, const char *procedure, JSON *details, JSON *list, JSON *dict, void *data)
// We are a callee being invoked.
// Sucess:	return [] or [{details}] or [{details},[list]] or [{details},[list],{dict}]
// Nothing:	return NULL to have nothing returned.  wamp_SendYield() should be called later
// Error:	return ["uri" ["uri",{details}] or ["uri",{details},[list]] or ["uri",{details},[list],{dict}]
// If it's more convenient, this function can call wamp_SendYield() and then return NULL.
{
	Unprompt();

	int connection = ConnectionNumber(wamp);

	const char *command = (const char *)idmap_GetValue(calleeRegistrationIdMap, registrationId);

	if (command) {
		if (*command == '!') {
			int argc = json_Count(list);

			const char *myCommand = strdup(command+1);

			for (int i=0;i<argc;i++) {
				myCommand = hprintf(myCommand, " %s", json_ArrayStringzAt(list, i));
			}

			FILE *fp = popen(myCommand, "r");
			szDelete(myCommand);

			if (fp) {
				char buf[10240];
				HBUF *hbuf = hbuf_New();
				size_t got;

				while ((got = fread(buf, 1, sizeof(buf), fp))) {
					hbuf_AddBuffer(hbuf, got, buf);
				}
				pclose(fp);

				int len = hbuf_GetLength(hbuf);
				char *data = (char *)hbuf_ReleaseBuffer(hbuf);
				hbuf_Delete(hbuf);

				bool binary=false;
				for (int i=0;i<len;i++) {
					if (data[i] & 0x80) {
						binary = true;
						break;
					}
				}

				if (binary) {
					const char *b64 = mime_Base64Enc(len, data, 0, NULL);
					szDelete(data);
					len = strlen(b64)+1;							// A NULL at the beginning...

					data = (char *)malloc(len+1);
					*data = '\0';
					memcpy(data+1, b64, len);
					szDelete(b64);
				}

				JSON *list = json_NewArray();
				json_ArrayAddString(list, len, data);
				wamp_SendYield(wamp, requestId, NULL, list, NULL);

				return NULL;
			}
		} else if (*command == '>') {
			command++;

			printf("I'd be redirecting to '%s'\n", command);
			Log("Redirecting %s to %s", procedure, command);
			wamp_Call(wamp, command, details, list, dict, OnResult);
		}
	}

	int delay = json_ObjectIntegerCalled(dict, "delay");

	Magenta();
	printf("Invoked:      %lld (%s) on connection %d\n", requestId, procedure, connection);
	Yellow();
	printf("Registration: %lld\n", registrationId);
	if (json_Count(details))	printf("Details:      %s\n", json_Render(details, pretty));
	if (json_Count(list))		printf("List:         %s\n", json_Render(list, pretty));
	if (json_Count(dict))		printf("Dict:         %s\n", json_Render(dict, pretty));
	if (delay) {
		printf("Delay:        %d\n", delay);
		sleep(delay);
	}

	char hostname[50];
	if (gethostname(hostname, sizeof(hostname)))
		strcpy(hostname, "unknown");

	time_t now = time(NULL);
	struct tm *tm = gmtime(&now);
	char dateNow[20];

	snprintf(dateNow, sizeof(dateNow), "%d-%.3s-%04d",
			tm->tm_mday, "JanFebMarAprMayJunJulAugSepOctNovDec"+tm->tm_mon*3, tm->tm_year+1900);

	JSON *yieldDict = json_NewObject();
	json_ObjectAddInteger(yieldDict, "connection", connection);
	json_ObjectAddInteger(yieldDict, "pid", getpid());
	json_ObjectAddStringz(yieldDict, "hostname", hostname);
	json_ObjectAddStringz(yieldDict, "date", dateNow);
	json_ObjectAddStringz(yieldDict, "time", TimeNow());

#if 0
	const char *result = hprintf(NULL, "Wamper connection %d, pid %d on %s at %d:%02d:%02d on %d-%.3s-%04d",
			connection, getpid(), hostname,
			tm->tm_hour, tm->tm_min, tm->tm_sec,
			tm->tm_mday, "JanFebMarAprMayJunJulAugSepOctNovDec"+tm->tm_mon*3, tm->tm_year+1900);
	JSON *resultList = json_NewArray();
	json_ArrayAddStringz(resultList, result);

	szDelete(result);
#endif

	if (!quiet) printf("Sending back a result at %s on %s\n", TimeNow(), dateNow);

	wamp_SendYield(wamp, requestId, NULL, NULL, yieldDict);

	Reprompt();

	return NULL;
}

typedef struct commandHistory_t {
	struct commandHistory_t *next;
	struct commandHistory_t *prev;

	const char *command;
} commandHistory_t;

commandHistory_t *commandHistory = NULL;			// Points to the last entry in the history
commandHistory_t *historyCursor = NULL;				// Current position in command history list

static void history_Add(const char *line)
{
	if (!line || !*line) return;									// Don't save blank lines

	if (commandHistory && !strcmp(commandHistory->command, line))	// Don't save the same line twice in a row
		return;

	commandHistory_t *hist = NEW(commandHistory_t, 1);
	hist->next = NULL;
	hist->prev = commandHistory;
	hist->command = strdup(line);
	if (hist->prev)
		hist->prev->next = hist;

	historyCursor = NULL;

	commandHistory = hist;
}

static const char *history_Prev()
// Returns the previous thing back in the command history or NULL if there is nothing (else)
{
	if (historyCursor)
		historyCursor = historyCursor->prev;
	else
		historyCursor = commandHistory;

	return historyCursor ? historyCursor -> command : NULL;
}

static const char *history_Next()
// Returns the next thing back in the command history or NULL if there is nothing (else)
{
	if (historyCursor)
		historyCursor = historyCursor->next;

	return historyCursor ? historyCursor -> command : NULL;
}

static void history_Catchup()
// Puts us at the end of history
{
	historyCursor = NULL;
}

static void history_Load()
{
	const char *home = getenv("HOME");
	if (home) {
		const char *filename = hprintf(NULL, "%s/.wamper_history", home);

		FILE *fd = fopen(filename, "r");
		if (fd) {
			const char *line;

			while ((line = hReadFileLine(fd))) {
				history_Add(line);
				szDelete(line);
			}
			fclose(fd);
		}
		szDelete(filename);
	}
}

static void history_Save()
{
	commandHistory_t *cursor = commandHistory;

	if (cursor) {
		while (cursor->prev)
			cursor = cursor->prev;

		const char *home = getenv("HOME");
		if (home) {
			const char *filename = hprintf(NULL, "%s/.wamper_history", home);

			FILE *fd = fopen(filename, "w");
			if (fd) {
				while (cursor) {
					fprintf(fd, "%s\n", cursor->command);
					cursor = cursor->next;
				}
				fclose(fd);
			}
			szDelete(filename);
		}
	}
}

void OnIdle(void *data)
{
	const char *command = (const char *)hlist_GetBlock(g_commands, NULL);

	if (command) {
		ProcessCommand(command);
	} else {
		int pending = idmap_Count(outstandingCallsMap);

//		printf("\rDone now (%d)", pending); fflush(stdout);

		if (!pending) exit(0);								// No outstanding calls
	}
}

static void StartInput()
// Add stdin to our input possibilities
{
	if (g_commands) {										// We have some commands to execute
		chan_PoolCallEvery(g_pool, 0, OnIdle, NULL);
	} else {
		if (chStdin) return;								// Already started
		history_Load();

		chStdin = chan_NewFd(g_pool, 0, CHAN_IN);			// Connect our stdin to the pool
		chan_RegisterReceiver(chStdin, OnStdin, 1);
		chan_CloseOnDelete(chStdin, false);
		chan_OnDelete(chStdin, OnStdinClosed);

		cbreak(true);

		Prompt();											// Set initial prompt
	}
}

static void OnWelcome(WAMP *wamp, long long sessionId, const char *uri, JSON *details)
// Called on welcome or abort
// It's an abort if sessionId = 0 and 'uri' is non-null, in which case it'll contain the error URI
{
	StartInput();										// Connect up our stdin now if now done before

	Unprompt();

	if (!sessionId) {
		Say("*** I am not welcome: %s\n", uri);
		Say("*** Details:          %s\n", json_Render(details, pretty));
		wamp_Delete(wamp);									// This will result in use dropping out from the event loop
		return;
	}

	int connection = NewConnection(wamp);
	SetDefaultConnection(connection);

	Say("*** @%d = %s on %s (%lld)\n",
			connection, wamp_RealmName(wamp), wamp_Peer(wamp), sessionId);

	const char *name = hprintf(NULL, "session%d", connection);
	VarSet(name, sessionId);
	szDelete(name);

	VarSet("session", sessionId);
//	if (!quiet)
//		Say("*** Server details: %s\n", json_Render(details, pretty));

	Reprompt();

	return;
}

static int OnWampConnected(WAMP *wamp, const char *mesg, void *data)
// Called when an outgoing WAMP connection succeeds (wamp != NULL) or fails (wamp == NULL)
{
	Unprompt();

	if (wamp) {
		Say("*** Connected via %s - saying hello to %s on %s...\n", wamp_Name(wamp), g_realm, wamp_Peer(wamp));

		// Send a hello message to the router, saying we're a caller, callee, publisher and subscriber
		wamp_Hello(wamp, g_realm, json_NewObject(), json_NewObject(), json_NewObject(), json_NewObject(), OnWelcome);
	} else {
		g_host = NULL;
		fprintf(stderr, "wamptest: Could not connect - %s\n", mesg);
	}

	Reprompt();

	return 0;
}

static const char *GetToken(const char **ptext)
// Fetches a token from the text given, returning it on the heap and moving 'ptext' after it.
{
	const char *result = NULL;

	if (!ptext || !*ptext || !**ptext) return result;		// Duff argument, NULL string or blank string

	const char *cursor = SkipSpaces(*ptext);
	char ch = *cursor;

	if (ch == '"') {
		const char *close = strchr(cursor+1, '"');

		while (close) {
			if (close[-1] != '\\')
				break;
			close = strchr(close+1, '"');
		}

		if (!close) {
			Error("No closing \"");
			return result;
		}

		result = strsubst(strnappend(NULL, cursor+1, close-cursor-1), "\\\"", "\"");
		*ptext = close+1;
	} else if (*cursor == '[' || *cursor == '{') {
		const char *end = cursor;
		JSON *parsed = json_Parse(&end);

		const char *error = json_Error(parsed);
		if (error) {
			Error("JSON Error: %s", error);
			json_Delete(parsed);
			return NULL;
		}
		json_Delete(parsed);

		result = strnappend(NULL, cursor, end-cursor);
		*ptext = end;
	} else if (ispunct(*cursor) || *cursor == '@') {
		result = strnappend(NULL, cursor, 1);
		*ptext = cursor+1;
	} else {
		const char *space = cursor+1;
		while (isalnum(*space) || *space == '.' || *space == '_')
			space++;
		if (!space) space=cursor+strlen(cursor);

		result = strnappend(NULL, cursor, space-cursor);
		*ptext = space;
	}

	return result;
}

static long long GetInteger(const char **ptext, long long def = 0)
{
	long long result = def;

	const char *token = GetToken(ptext);

	if (token) {
		result = atoll(token);
		szDelete(token);
	}

	return result;
}

static JSON *GetJson(const char **ptext)
// Not efficient as we're parsing the text twice but heck, huh?
{
	const char *cursor = SkipSpaces(*ptext);

	JSON *json = json_Parse(&cursor);

	const char *error = json_Error(json);
	if (error) {
		json_Delete(json);
		return NULL;
	}

	*ptext = cursor;
	return json;
}

static char *VarSubst(const char *text)
// Return a heap-based copy of text with variables substituted
{
	char *buf = strdup(text);

	char *cursor = buf;
	for (;;) {
		char *dollar = strchr(cursor, '$');
		if (!dollar) break;

		cursor = dollar+1;
		const char *end = dollar+1;
		if (isalpha(*end)) {
			const char *name = GetToken(&end);

			if (name) {
				const char *value = VarValue(name);

				if (value) {
					char *newbuf = (char*)hprintf(NULL, "%.*s%s%s", dollar-buf, buf, value, end);
					cursor = newbuf+(dollar-buf)+strlen(value);
					szDelete(buf);
					buf = newbuf;
				}
				szDelete(name);
			}
		}
	}


	return buf;
}

static int DoHello(const char *args)
{
	g_host = GetToken(&args);
	g_realm = GetToken(&args);

	if (!g_host)
		return Error("I need a host address");

	if (!g_realm) g_realm = "spider";

	Say("Saying hello to %s...\n", g_host);

	wamp_Connect(g_pool, g_host, OnWampConnected);

	return 0;
}

static int DoBye(const char *args)
{
	int connection = GetInteger(&args, -1);
	if (connection == -1) connection = g_connection;

	if (connection) {
		if (DeleteConnection(connection)) {
			Say("Connection %d closed\n", connection);
		} else {
			Say("Connection %d was not open\n", connection);
		}
	} else {
		Say("Not connected");
	}

	return 0;
}

static int DoSubscribe(const char *args)
{
	WAMP *wamp = CurrentWamp();

	if (!wamp) return Error("We're not connected");

	const char *topic = GetToken(&args);

	wamp_Subscribe(wamp, 0, topic, NULL, OnSubscribed, OnEvent);

	szDelete(topic);

	return 0;
}

static void GetArgs(const char **pargs, JSON **plist, JSON **pdict)
// Fetches the list and dict for use in 'call' or 'publish'
{
	JSON *list;
	JSON *first = GetJson(pargs);					// Might be list or something else, which we'll listify
	JSON *dict = GetJson(pargs);

	if (first && !json_IsArray(first)) {				// If the first thing isn't a list, listify it anyway
		list = json_NewArray();
		json_ArrayAdd(list, first);
	} else {
		list = first;
	}

	if (plist) *plist = list;
	else json_Delete(list);

	if (pdict) *pdict = dict;
	else json_Delete(dict);
}

static int DoCall(const char *args)
{
	WAMP *wamp = CurrentWamp();

	if (!wamp) return Error("We're not connected");

	const char *procedure = GetToken(&args);
	JSON *argList;
	JSON *argDict;
	GetArgs(&args, &argList, &argDict);

	calldata_t *data = NEW(calldata_t, 1);

//	Log("Making call to %s", procedure);
	long long requestId = wamp_Call(wamp, procedure, g_options, argList, argDict, OnResult, (void*)data);
	if (requestId) {
		struct timeval tp;

		gettimeofday(&tp, NULL);
		data->requestId = requestId;
		data->procedure = strdup(procedure);
		data->start = StopwatchNow();
		idmap_Add(outstandingCallsMap, requestId, data);
	} else {
		free((void*)data);
	}
	g_options = NULL;

	szDelete(procedure);

	return 0;
}

static int DoRegister(const char *args)
{
	WAMP *wamp = CurrentWamp();

	if (!wamp) return Error("We're not connected");

	const char *procedure = GetToken(&args);

	const char *extra = GetToken(&args);

	const char *command = NULL;
	bool err = false;

	if (extra) {
		if (!strcmp(extra, "!")) {
			const char *spec = GetToken(&args);

			if (!spec) {
				Error("Register ! must be followed by a command");
				err = true;
			} else {
				command = hprintf(NULL, "!%s", spec);
			}
			szDelete(spec);
		} else if (!strcmp(extra, "call") || !strcmp(command, "publish")) {
			command = hprintf(NULL, ">%s %s", extra, SkipSpaces(args));
		} else {
			Error("Register can be followed by call, publish or !command");
			err = true;
		}

		szDelete(extra);
	}

	if (err) {
		szDelete(command);
		command=NULL;
	} else {
		const char *invoke = json_ObjectStringzCalled(g_options, "invoke");
		int nInvoke = WAMP_INVOKE_SINGLE;
		if (invoke) {
			if (!stricmp(invoke, "single")) {
				nInvoke = WAMP_INVOKE_SINGLE;
			} else if (!stricmp(invoke, "first")) {
				nInvoke = WAMP_INVOKE_FIRST;
			} else if (!stricmp(invoke, "last")) {
				nInvoke = WAMP_INVOKE_LAST;
			} else if (!stricmp(invoke, "random")) {
				nInvoke = WAMP_INVOKE_RANDOM;
			} else if (!stricmp(invoke, "roundrobin")) {
				nInvoke = WAMP_INVOKE_ROUNDROBIN;
			} else {
				Error("Invoke must be one of single, first, last, random or roundrobin");
			}
		}
		long long requestId = wamp_Register(wamp, nInvoke, 0, 0, procedure, OnRegistered, OnInvoke);

		if (command) {
			printf("Registering %s (%lld) to do %s\n", procedure, requestId, command);
			idmap_Add(calleeRequestIdMap, requestId, (void*)command);
		}
	}

	szDelete(procedure);

	return 0;
}

static int DoPublish(const char *args)
{
	bool metoo = false;

	WAMP *wamp = CurrentWamp();

	if (!wamp) return Error("We're not connected");

	const char *topic = GetToken(&args);
	if (!strcmp(topic, "!")) {
		metoo = 1;
		topic = GetToken(&args);
	}
	JSON *argList;
	JSON *argDict;
	GetArgs(&args, &argList, &argDict);

	if (metoo) json_ObjectAddBool(g_options, "exclude_me", false);

	json_ObjectAddBool(g_options, "disclose_me", true);

	wamp_Publish(wamp, topic, g_options, argList, argDict, OnPublished);
	g_options = NULL;

	szDelete(topic);

	return 0;
}

static void OnRegistrationGet(WAMP *wamp, long long requestId, const char *procedure, const char *uri, JSON *details, JSON *list, JSON *dict, void *data)
// [{"registration":8735518509006636,"invoke":"single","uri":"my.proc","match":"exact","created":"2017-03-28T12:39:59.458"}]
{
	Unprompt();

	JSON *result = json_ArrayAt(list, 0);
	long long id = json_ObjectIntegerCalled(result, "id");
	const char *myuri = json_ObjectStringzCalled(result, "uri");
	const char *invoke = json_ObjectStringzCalled(result, "invoke");
	const char *match = json_ObjectStringzCalled(result, "match");
	const char *created = json_ObjectStringzCalled(result, "created");

	printf("%16lld: %s %-8s %-10s %s\n", id, created, match, invoke, myuri);

	Reprompt();
}

static void OnRegistrationList(WAMP *wamp, long long requestId, const char *procedure, const char *uri, JSON *details, JSON *list, JSON *dict, void *data)
{
	JSON *result = json_ArrayAt(list, 0);
	JSON *exact = json_ObjectElementCalled(result, "exact");
	JSON *prefix = json_ObjectElementCalled(result, "prefix");
	JSON *wildcard = json_ObjectElementCalled(result, "wildcard");

	for (int i=0;i<json_ArrayCount(exact);i++) {
		long long registrationId = json_ArrayIntegerAt(exact, i, -1);

		if (registrationId >= 0) {
			JSON *list = json_NewArray();
			json_ArrayAddInteger(list, registrationId);
			wamp_Call(wamp, "wamp.registration.get", json_NewObject(), list, NULL, OnRegistrationGet, (void*)"exact");
		}
	}

	for (int i=0;i<json_ArrayCount(prefix);i++) {
		long long registrationId = json_ArrayIntegerAt(prefix, i, -1);

		if (registrationId >= 0) {
			JSON *list = json_NewArray();
			json_ArrayAddInteger(list, registrationId);
			wamp_Call(wamp, "wamp.registration.get", json_NewObject(), list, NULL, OnRegistrationGet, (void*)"prefix");
		}
	}

	for (int i=0;i<json_ArrayCount(wildcard);i++) {
		long long registrationId = json_ArrayIntegerAt(wildcard, i, -1);

		if (registrationId >= 0) {
			JSON *list = json_NewArray();
			json_ArrayAddInteger(list, registrationId);
			wamp_Call(wamp, "wamp.registration.get", json_NewObject(), list, NULL, OnRegistrationGet, (void*)"wildcard");
		}
	}

}

static int DoProcedures(const char *args)
// List the procedures defined on a router
{
	WAMP *wamp = CurrentWamp();

	if (!wamp) return Error("We're not connected");

	wamp_Call(wamp, "wamp.registration.list", json_NewObject(), NULL, NULL, OnRegistrationList);

	return 0;
}

static void DoQuit(const char *args)
{
	chan_PoolDeleteAllChannels(g_pool);
	quitting = true;
}

static int DoSource(const char *args)
{
	const char *filename = GetToken(&args);

	if (!filename) return Error("I need a filename");

	FILE *fd = fopen(filename, "r");
	const char *line;

	if (fd) {
		while ((line = hReadFileLine(fd))) {
			ProcessCommand(line);
			szDelete(line);
		}
		fclose(fd);
	}

	szDelete(filename);

	return 0;
}

static int DoHelp(const char *args)
{
	printf("Commands:\n");
	printf("help                             This text\n");
	printf("hello server[:port] [realm]      Connect to a realm (default port=4512, realm = spider)\n");
	printf("call procedure [list] {dict}     Call a procedure\n");
	printf("register procedure               Register a procedure (the procedure returns a nice string)\n");
	printf("subscribe topic                  Subscribe to a topic (* can be used to make this prefix or wildcard)\n");
	printf("publish topic [list] {dict}      Publish a topic\n");
	printf("publish! topic [list] {dict}     Publish a topic, including self in recipients\n");
	printf("echo anything                    Simply echoes the text back\n");
	printf("quiet                            Be less chatty\n");
	printf("loud                             Be more chatty\n");
	printf("quit or q                        Leave wamper\n");
	printf("pretty                           Render JSON prettily\n");
	printf("ugly                             Render JSON on one line\n");
	printf("vars                             List all defined variables\n");
	printf("source filename                  Read commands from a file\n");
	printf("!command                         Execute shell command\n");
	printf("\n");
	printf("[list] amd {dict} are always optional.\n");
	printf("Results of calls, events etc. will be displayed asynchronously.\n");
	printf("Commands entered are saved in ~/.wamper_history\n");

	return 0;
}

static void OnSnomed(WAMP *wamp, long long requestId, const char *procedure, const char *uri, JSON *details, JSON *list, JSON *dict, void *data)
{
	int lines = 12;
	char **line = NEW(char *, lines);
	for (int i=0;i<lines;i++)
		line[i]=NULL;

	if (uri) {
		line[0] = hprintf(NULL, "Error: %s\n", uri);
		if (details && json_ObjectCount(details))
			line[1] = hprintf(NULL, "%-.78s", json_Render(details, 0));
		line[2] = hprintf(NULL, json_Render(list));
		line[3] = hprintf(NULL, json_Render(dict));

		Magenta();
	} else {
		Cyan();

		for (int i=0; i<lines; i++) {
			const char *resultLine = json_ArrayStringzAt(list, i);

			JSON *obj = json_ParseText(resultLine);

			int rank = json_ObjectIntegerCalled(obj, "rank", 99);
			int words = (rank >> 16) & 0xff;
			int score = (rank >> 8) & 0xff;
			int len = rank & 0xff;
			const char *text = json_ObjectStringzCalled(obj, "text");

			line[i] = hprintf(NULL, "(%d,%d,%d): %s", words,score,len, text);
		}

	}
	Unprompt();
	GotoXy(1,2);

	for (int i=0; i<lines; i++) {
		printf("%.*s", ScreenWidth()-2, line[i] ? line[i] : "");
		ClearEol();
		printf("\n");

		szDelete(line[i]);
	}

	GotoXy(1,1);
	Reprompt();
}

void CheckSnomed()
{
//	static const char *text = NULL;

	if (snomedMode) {
		JSON *list = json_NewArray();
		json_ArrayAddString(list, inlen, input);

		wamp_Call(CurrentWamp(), "mtcode.searchterm", json_NewObject(), list, json_ParseText("{\"limit\":\"12\"}"), OnSnomed);
	}
}

static int DoSnomed(const char *args)
{
	snomedMode = !snomedMode;

	if (snomedMode)
		ClearScreen();

	return 0;
}

static void ProcessCommand(const char *buf)
{
	if (!buf) return;						// Called with no buffer

	buf = SkipSpaces(buf);

	if (*buf == '\0' || *buf == '#') return;	// Blank line or comment

	// If the command starts @nnn then we want to use connection nnn, either for this command or for all subsequent
	if (*buf == '@') {										// Aiming at a specific connection
		buf = SkipSpaces(buf+1);

		if (!*buf) {
			ListConnections();
			return;
		}

		int connection = GetInteger(&buf, -1);

		if (!SetConnection(connection)) {
			if (g_numConnections) {
				Error("Connection number must be 1..%d", g_numConnections);
			} else {
				Error("You have no connections");
			}
			return;
		} else {
			buf = SkipSpaces(buf);

			if (!*buf) {
				SetDefaultConnection(connection);			// Just '@nnn' to set default connection
			}
		}
	} else {
		SetConnection(-1);
	}

	char *copyBuffer = VarSubst(buf);
	const char *args = copyBuffer;

	char *command = (char*)GetToken(&args);
	if (!command || !*command) {
		szDelete(copyBuffer);
		return;
	}

	righttrim(command);
	strlwr(command);

	bool done = false;

	args = SkipSpaces(args);
	if (isalpha(*command) && args && *args == '=') {			// Assignment...
		const char *v = args+1;
		const char *value = GetToken(&v);
		VarSet(command, value);
		szDelete(value);
		done = true;
	}

	json_Delete(g_options);
	if (*args == '{') {
		const char *options = GetToken(&args);
		g_options = json_ParseText(options);
		szDelete(options);
	} else {
		g_options = json_NewObject();
	}


	if (!done && command) {
		if (*command == '!') {
			cbreak(false);
			if (args && *args) {
				system(args);
			} else {
				const char *shell = getenv("SHELL");
				if (shell) {
					printf("Entering sub-shell - use ^D or exit to return\n");
					system(shell);
				} else {
					printf("No SHELL defined\n");
				}
			}
			cbreak(true);
		} else if (!strcmp(command, "help")) {
			DoHelp(args);
		} else if (!strcmp(command, "hello")) {
			DoHello(args);
		} else if (!strcmp(command, "bye")) {
			DoBye(args);
		} else if (!strcmp(command, "call")) {
			DoCall(args);
		} else if (!strcmp(command, "register")) {
			DoRegister(args);
		} else if (!strcmp(command, "subscribe")) {
			DoSubscribe(args);
		} else if (!strcmp(command, "publish")) {
			DoPublish(args);
		} else if (!strcmp(command, "procedures")) {
			DoProcedures(args);
		} else if (!strcmp(command, "quiet")) {
			quiet=1;
		} else if (!strcmp(command, "loud")) {
			quiet=0;
		} else if (!strcmp(command, "echo")) {
			printf("%s\n", args);
		} else if (!strcmp(command, "pretty")) {
			pretty=true;
		} else if (!strcmp(command, "ugly")) {
			pretty=false;
		} else if (!strcmp(command, "vars")) {
			DoVars(args);
		} else if (!strcmp(command, "quit")) {
			DoQuit(args);
		} else if (!strcmp(command, "q")) {
			DoQuit(args);
		} else if (!strcmp(command, "source")) {
			DoSource(args);
		} else if (!strcmp(command, "snomed")) {
			DoSnomed(args);
		} else {
			printf("Command '%s' not recognised\n", command);
		}
	}

	szDelete(command);
	szDelete(copyBuffer);
}

static char *ResetInput()
{
	char *result = input;

	if (input) {
		input=NULL;
		inlen=0;
		inpos=0;
	}

	return result;
}

#define IN_BUF_INC	16			// The increments by which we'll increase the input buffer

static void AddInput(char ch)
{
	if (inlen % IN_BUF_INC == 0) {
		if (input) {
			RENEW(input, char, inlen+16);
		} else {
			input = NEW(char, inlen+16);
		}
	}

	if (!ch)							// \0 is the terminating character, always goes at the end
		inpos=inlen;

//printf("Before: (%d,%d)'%.*s'\n", inpos, inlen, inlen, input);
	if (inpos <= inlen) {				// Inserting so make space
		memmove(input+inpos+1, input+inpos, inlen-inpos);
	}

	input[inpos]=ch;
//printf("After:  (%d,%d)'%.*s'\n", inpos, inlen, inlen, input);
	if (ch) {
		printf("%c", ch);
		if (inpos < inlen) {
			printf("%.*s", inlen-inpos, input+inpos+1);
			int i;
			for (i=inpos; i<inlen; i++)
				printf("\b");
		}
		fflush(stdout);
	}
	inlen++;
	inpos++;

	CheckSnomed();
}

static void Backspace()
{
	if (input && *input && inpos) {
		printf("\b");

		memmove(input+inpos-1, input+inpos, inlen-inpos);
		printf("%.*s ", inlen-inpos, input+inpos-1);
		int i;

		for (i=inlen; i>=inpos; i--)
			printf("\b");
		fflush(stdout);
		inlen--;
		inpos--;
		CheckSnomed();
	}
}

static void DelForward()
// Like Backspace() but deletes to the right
{
	if (input && *input && inpos < inlen) {
		memmove(input+inpos, input+inpos+1, inlen-inpos-1);
		printf("%.*s ", inlen-inpos-1, input+inpos);
		int i;

		for (i=inlen; i>inpos; i--)
			printf("\b");
		fflush(stdout);
		inlen--;
	}
	CheckSnomed();
}

static void SetInput(const char *line)
{
	Unprompt();

	szDelete(ResetInput());

	if (line) {							// Need to be careful here due to the way allocation of the buffer works
		int len = strlen(line)+1;
		len = (len / IN_BUF_INC + 1) * IN_BUF_INC;		// Might be slightly generous but that's ok
		input = NEW(char, len);
		strcpy(input, line);
		inpos = inlen = strlen(line);
//printf("\n(%d,%d) '%s'\n", inpos, inlen, line);
	}

	Reprompt();
}

static void OnStdinClosed(CHAN *chan)
{
	ExitNicely();
}

static int OnStdin(CHAN *chan)
// Something has been received on stdin
{
	static char escBuf[10];							// To store escape sequences
	static int escLen = 0;

	char ch;
	int got = chan_GetData(chan, 1, &ch);
	if (!got) return 0;								// Odd - this shouldn't really happen...

	if (escLen) {
		escBuf[escLen++]=ch;
		if (escLen == 2 && ch != '[') {				// Second character is expected to be '[' in our simple minds
			escLen = 0;								// Throw the sequence so far away
		} else if (escLen == 3) {
			escLen = 0;

			switch (ch) {							// We're only interested in the last character
				case 'A':							// Up...
					{
						const char *prev = history_Prev();
						if (prev) {
							SetInput(prev);
						}
					}
					break;
				case 'B':							// Down...
					{
						const char *next = history_Next();
						SetInput(next);
					}
					break;
				case 'C':							// Right...
					if (inpos < inlen) {
						inpos++;
						printf("\033[C"); fflush(stdout);
					}
					break;
				case 'D':							// Left...
					if (inpos) {
						inpos--;
						printf("\033[D"); fflush(stdout);
					}
					break;
				case 'H':							// Home...
					while (inpos) {
						inpos--;
						printf("\033[D");
					}
					fflush(stdout);
					break;
				case 'F':							// End...
					while (inpos < inlen) {
						inpos++;
						printf("\033[C");
					}
					fflush(stdout);
					break;
				case '3':							// Ubuntu terminal emulator thing sends "ESC [ 3 ~" on DEL
					escLen = 3;						// Put us back to expecting more characters
					break;
				default:
					Unprompt();
					printf("Unrecognised key ESC+[+%c\n", ch);
					szDelete(ResetInput());
					Reprompt();
					break;
			}
		} else if (escLen == 4) {
			escLen = 0;

			switch (ch) {
				case '~':							// ESC [ 3 ~ means DEL on the ubuntu box
					DelForward();
					break;
				default:
					Unprompt();
					printf("Unrecognised key ESC+[+%c+%c\n", escBuf[2], ch);
					szDelete(ResetInput());
					Reprompt();
					break;
			}
		}
	} else if (ch == '\n' || ch == '\r') {
		printf("\r\n");
		g_Prompting = false;					// Tells prompt logic we're at start of line

		if (input) {
			AddInput(0);

			char* entry = ResetInput();
			ProcessCommand(entry);
			if (!quitting) {
				history_Add(entry);
				history_Catchup();
			}
			szDelete(entry);
		}
		Prompt();
	} else if (ch == '\b' || ch == 127) {			// Linux thinks DEL is backspace
		Backspace();
	} else if (ch == '\033') {						// Escape sequence...
		escBuf[0]=ch;
		escLen=1;
	} else if (ch == '\004' && inlen == 0) {		// ^D = EOF
		DoQuit("");
	} else if ((unsigned char)ch >= 32) {
		AddInput(ch);
	}

	return 0;
}

static void HandleSignal(int n)
{
	if (n == SIGINT) {
		signal(SIGINT, HandleSignal);

		printf("\n");
		ResetInput();
		Reprompt();
		return;
	}

	printf("\nSignal %d - bye\n", n);
	ExitNicely();
}

void LoadList(HLIST *hlist, FILE *fp)
// Loads a list of commands from a file pointer into the list
{
	if (!hlist || !fp) return;

	const char *line = NULL;

	for (;;) {
		line = hReadFileLine(fp);
		if (!line) break;

		hlist_AddHeap(hlist, -1, line);
	}
}

int main(int argc, char * const* argv)
{
	const char *host = "localhost";
	const char *realm = "spider";
	int verbose = 0;

	int nErr = 0;
	int c;
	while((c=getopt(argc,argv,"h:r:v"))!=-1){
		switch(c){
			case 'h': host = optarg;				break;	// Host
			case 'r': realm = optarg;				break;	// Realm
			case 'v': verbose++;					break;	// verbose
			case '?': nErr++;						break;	// Something wasn't understood
		}
	}

	if (verbose >= 1) wamp_Logger(vLog);							// Catch log entries from the wamp_ library
	if (verbose >= 2) ws_Logger(vLog);								// Catch log entries from the ws_ library
	if (verbose >= 3) chan_Logger(vLog);							// Catch log entries from the chan_ library
	if (verbose >= 4) json_Logger(vLog);							// Catch log entries from the json_ library

	calleeRequestIdMap = idmap_New();
	calleeRegistrationIdMap = idmap_New();
	outstandingCallsMap = idmap_New();

	g_host = host;
	g_realm = realm;

	inputTty = isatty(0);								// Need to get it here as it becomes untrue after the ioctl call.

	g_commands = hlist_New();
	const char *command = argv[optind++];				// Command given on the command line?
	if (command) {
		if (*command == '@') {							// Take input from a file
			FILE *fp = fopen(command+1, "r");
			if (!fp) {
				fprintf(stderr, "Cannot read file '%s'\n", command+1);
				exit(2);
			}
			LoadList(g_commands, fp);
			fclose(fp);
		} else {										// One or more discrete commands to execute
			const char *cmd = NULL;

			while (command) {
				cmd = hprintf(cmd, "%s%s", cmd?" ":"", command);
				command = argv[optind++];
			}
			hlist_Add(g_commands, -1, cmd);
			szDelete(cmd);
		}
	}

	if (!inputTty) {									// Something piped into us
		LoadList(g_commands, stdin);
	}

	if (!hlist_Length(g_commands)) {					// No commands so we'll NULL the commands list
		hlist_Delete(g_commands);
		g_commands = NULL;
	}

	if (g_commands) {
		inputTty = 0;									// We'll be quiet
	}

	if (inputTty) {
		Cyan();
		printf("WAMPER - version " VERSION " (built " __DATE__ " at " __TIME__ ")\n");
	}

	g_pool = chan_PoolNew();							// Create a new channel pool

	if (g_host)
		wamp_Connect(g_pool, g_host, OnWampConnected);
	else
		StartInput();

	signal(SIGHUP, HandleSignal);						// We're setting cbreak mode so we'll need to tidy up if things go wrong
	signal(SIGINT, HandleSignal);
	signal(SIGQUIT, HandleSignal);
	signal(SIGTERM, HandleSignal);

	chan_EventLoop(g_pool);								// Pass control to the event loop

	ExitNicely();

	return 0;
}
