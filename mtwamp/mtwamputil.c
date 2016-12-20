#if 0
./makeh $0
exit 0
#endif

#include <netdb.h>
#include <setjmp.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include "hbuf.h"
#include "heapstrings.h"
#include "mime.h"
#include "mtmacro.h"

#include "mtwamputil.h"
#define API
#define STATIC static

#if 0
// START HEADER

//#include "mtjson.h"
#include "mtchannel.h"
#include "mtwebsocket.h"
#include "mtwamp.h"

typedef int (*INTWAMPFN)(WAMP *wamp, const char *mesg);

// END HEADER
#endif

typedef struct outward_connection_t {
	const char *nonce;
	INTWAMPFN	onDone;
} outward_connection_t;

static jmp_buf jmpbuf_alarm;

static void Log(const char *fmt, ...)
{
	void (*_Logger)(const char *fmt, va_list ap) = wamp_GetLogger();

	if (_Logger) {
		va_list ap;

		va_start(ap, fmt);
		(*_Logger)(fmt, ap);
		va_end(ap);
	}
}

STATIC const char *memmem(const char *haystack, size_t hlen, const char *needle, size_t nlen)
{
	int needle_first;
	const char *p = haystack;
	size_t plen = hlen;

	if (nlen < 0) nlen=strlen(needle);
	if (!nlen) return haystack;

	needle_first = *(unsigned char *)needle;

	while (plen >= nlen && (p = memchr(p, needle_first, plen - nlen + 1))) {
		if (!memcmp(p, needle, nlen))
			return p;

		p++;
		plen = hlen - (p - haystack);
	}

	return NULL;
}

static const char *wamp_HttpResponseText(int code)
// I don't like having to include this here - it should be in a library
{
	const char *s;

	switch (code) {
		case 100: s="Continue"; break;
		case 101: s="Switching Protocols"; break;
		case 200: s="OK"; break;
		case 201: s="Created"; break;
		case 202: s="Accepted"; break;
		case 203: s="Non-Authoritative Information"; break;
		case 204: s="No Content"; break;
		case 205: s="Reset Content"; break;
		case 206: s="Patient Content"; break;
		case 207: s="Multi-Status"; break;
		case 300: s="Multiple Choices"; break;
		case 301: s="Moved Permanently"; break;
		case 302: s="Found"; break;
		case 303: s="See Other"; break;
		case 304: s="Not Modified"; break;
		case 305: s="Use Proxy"; break;
		case 306: s="Unused"; break;
		case 307: s="Temporary Redirect"; break;
		case 400: s="Bad Request"; break;
		case 401: s="Unauthorized"; break;
		case 402: s="Payment Required"; break;
		case 403: s="Forbidden"; break;
		case 404: s="Not Found"; break;
		case 405: s="Method Not Allowed"; break;
		case 406: s="Not Acceptable"; break;
		case 407: s="Proxy Authentication Required"; break;
		case 408: s="Request Timeout"; break;
		case 409: s="Conflict"; break;
		case 410: s="Gone"; break;
		case 411: s="Length Required"; break;
		case 412: s="Precondition Failed"; break;
		case 413: s="Request Entity Too Large"; break;
		case 414: s="Request-URI Too long"; break;
		case 415: s="Unsupported Media Type"; break;
		case 416: s="Requested Range Not Satisfiable"; break;
		case 417: s="Expectation Failed"; break;
		case 422: s="Unprocessable Entity"; break;
		case 423: s="Locked"; break;
		case 424: s="Failed Dependency"; break;
		case 500: s="Internal Server Error"; break;
		case 501: s="Not Implemented"; break;
		case 502: s="Bad gateway"; break;
		case 503: s="Service Unavailable"; break;
		case 504: s="Gateway Timeout"; break;
		case 505: s="HTTP Version Not Supported"; break;
		case 507: s="Insufficient Storage"; break;
		default:  s="UNKNOWN CODE"; break;
	}

	return s;
}

static const char *wamp_HttpHeader(nCode)
// I don't like having to include this here - it should be in a library
{
	const char *result = NULL;

	szDelete(result);

	result = hprintf(NULL, "HTTP/1.1 %d %s\r\n", nCode, wamp_HttpResponseText(nCode));

	return result;
}

static int tcp_Connect(const char *szHost, int nPort)
// Creates an outgoing connection to the given host and port
// Returns	0		Failed (0 can't be valid outgoing as it would already have been used for an incoming...!)
//			1...	The socket number
{
	struct hostent *hp;
	struct sockaddr_in addr;
	int nSock;
	time_t nStarted, nFinished;

//Log("WA: tcp_Connect(%s:%d)", szHost, nPort);
	memset(&addr,0,sizeof(addr));
	addr.sin_family=AF_INET;
	addr.sin_port=htons(nPort);

//Log("WA: DNS Lookup");
	// This call takes 75 seconds if there is no DNS...
	nStarted=time(NULL);
	hp=gethostbyname(szHost);
	nFinished=time(NULL);
	if (nFinished - nStarted > 5) {
		Log("WA: Lookup of '%s' was slow (%d seconds) - DNS Problem?", szHost, nFinished-nStarted);
	}

//Log("WA: hp=%d", hp);
	if (hp) {
		addr.sin_addr=*(struct in_addr*)hp->h_addr_list[0];
	} else {
		unsigned int ad=inet_addr(szHost);
		addr.sin_addr=*(struct in_addr*)&ad;
	}

//Log("WA: Opening socket");
	if((nSock=socket(AF_INET,SOCK_STREAM, IPPROTO_TCP))<0) {
		Log("WA: Error: Couldn't create socket to connect to %s", szHost);
		return 0;
	}

	//fcntl(nSock, F_SETFL, O_NONBLOCK);
	if (setjmp(jmpbuf_alarm)) {
		Log("WA: Timed out waiting for connection to %s:%d", szHost, nPort);
		return 0;
	} else {
		alarm(5);
	}

//Log("WA: Connecting to socket");
	if(connect(nSock,(struct sockaddr *)&addr, sizeof(addr))<0) {
		alarm(0);
		Log("WA: Couldn't connect socket to '%s:%d' (%s)", szHost, nPort, inet_ntoa(addr.sin_addr));
		return 0;
	}

	alarm(0);

//Log("WA: Returning...");
	return nSock;
}

STATIC void wamp_OnChannelDeleteOutward(CHAN *chan)
{
Log("WA: Wamp outward channel %s is being deleted", chan_Name(chan));
	outward_connection_t *out = (outward_connection_t*)chan_Info(chan);

	if (out) {							// Must be non-NULL but being safe is cheap
		szDelete(out->nonce);
		free((char*)out);

		chan_SetInfo(chan, NULL);
	}
}

STATIC const char *CheckWampHttpResponse(SSMAP *headerMap, const char *nonce)
// Returns	NULL	Ok
//			char*	Error message (treat as static string)
{
	const char *connection = ssmap_GetValue(headerMap, "connection");
	const char *upgrade = ssmap_GetValue(headerMap, "upgrade");
	const char *protocol = ssmap_GetValue(headerMap, "sec-websocket-protocol");
	const char *accept = ssmap_GetValue(headerMap, "sec-websocket-accept");

//Log("WA: connection='%s'", connection ? connection : "NULL");
//Log("WA: upgrade='%s'", upgrade ? upgrade : "NULL");
//Log("WA: protocol='%s'", protocol ? protocol : "NULL");
//Log("WA: accept='%s'", accept ? accept : "NULL");

	if (!connection || strcasecmp(connection, "upgrade")) return "Connection header must be 'Upgrade'";
	if (!upgrade || strcasecmp(upgrade, "websocket")) return "Upgrade header must be 'WebSocket'";
	if (!protocol || strcasecmp(protocol, "wamp.2.json")) return "Sec-Websocket-Protocol header must be 'wamp.2.json'";

	if (!accept || strlen(accept) != 28) return "sec-Websocket-Accept header must be 28 chars";

	const char *key = hprintf(NULL, "%s%s", nonce, "258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
	unsigned char buf[20];
	SHA1((const unsigned char *)key, strlen(key), buf);
	const char *expected = mime_Base64Enc(20, (const char *)buf, 0, NULL);
	szDelete(key);
//Log("WA: OUT NONCE:    '%s'", nonce);
//Log("WA: OUT EXPECTED: '%s'", accept);

	int ok = !strcmp(accept, expected);
	if (ok) {
		szDelete(expected);
		return NULL;
	}

	static const char *result = NULL;
	szDelete(result);
	result = hprintf(NULL, "sec-Websocket-Accept doesn't match - I expect it to be '%s'", expected);
	szDelete(expected);

	return result;

}

STATIC int wamp_OnClientChannelError(CHAN *chan)
//HERE - need to catch the two places this is references, adding Info() to the channel then using that here to delete the WAMP, which will in turn delete the channel and deal with the other effects of the demise of a WAMP.
{
Log("WA: WAMP channel error on %s - closing connection", chan ? chan_Name(chan) : "NULL");
	chan_Delete(chan);												// We won't be hearing from them again...
}

STATIC int wamp_OnHttpResponse(CHAN *chan)
// We arrive here when we have made an outgoing connection to a WAMP server and a reply has come in
//
// 1. Make sure we have the full header (i.e. we have a blank line) 'crlfcrlf'
// 2. Check the first line starts with "HTTP/1.1 101"
// 3. Interpret the header into a SSMAP and check it's valid
// 4. Connect the channel to a websocketed wamp
//
// Note that 'technically' there might be continuation lines in a header so it might look like:
// sec-Websocket-Protocol:
//  wamp.2.json
// But nobody's going to split a header line so short and we're simply not going to deal with it here (we drop the continuation)
// Returns an error code...
//	0	Everything went fine
//	1	Bad header (too short)
//	2	Bad header (is not a 101 - Protocol Changed)
//	3	Bad header (there is no CRLF)
//	4	Bad response from peer
// The outwardInfo->onDone callback will ALWAYS be called - if there is no error then it's called with a valid WAMP, otherwise NULL
{
	outward_connection_t *outwardInfo = chan_Info(chan);
	INTWAMPFN cb_onDone = outwardInfo->onDone;
	HLIST *inq = chan_Inq(chan);
	int err = 0;											// Optimistically assume we won't get an error
	static const char *mesg = NULL;							// An error message to be passed to cb_onDone()
	WAMP *wamp = NULL;										// cb_OnDone will be called with this

	szDelete(mesg);
	mesg = NULL;

	const int maxpeek = 10240;								// 10k is very ample for a WAMP connection response message
	int got;
	const char *peek = hlist_Peek(inq, maxpeek, &got);

	const char *crlfcrlf = memmem(peek, got, "\r\n\r\n", 4);	// We're going to need a linefeed
	if (!crlfcrlf) return 1;								// Have not got a full header yet so go back and wait

	got = crlfcrlf-peek+4;									// The number of bytes we want
	char *header = (char *)malloc(got);						// Take the line from the input
	hlist_GetDataToBuffer(inq, got, header);

	header[got-2] = '\0';									// We KNOW that the last four chars are CRLFCRLF so we terminate at CRLF
	if (got < 13) {											// ERR - cannot possibly be a good header
		err = 1;
		mesg = hprintf(NULL, "Response too short to be HTTP status line (%d char%s)", got, got==1?"":"s");
	}

	if (!err && strncmp(header, "HTTP/1.1 101 ", 13)) {		// ERR - MUST return a 101 response
		err = 2;
		mesg = hprintf(NULL, "Expecting status to start 'HTTP/1.1 101 ', not '%.13s'", header);
	}

	if (!err) {
		SSMAP *headerMap = ssmap_New();
		char *line = strchr(header, '\n');
		if (!line) {							// ERR - No line ending in header...
			err = 3;
			mesg = hprintf(NULL, "No end of line in HTTP response '%.13s'", header);
		}

		if (!err) {
			line++;									// Point to first line of actual header
			while (*line) {
				char *eol = strchr(line, '\n');		// We know the string ends with '\n' so this MUST always return a pointer
				char *nextLine = eol+1;
				if (eol > line && line[eol-line-1] == '\r')		// We're going to replace the eol with '\0', but it should be \r\n
					eol--;										// It is, so we'll replace the '\r'
				*eol='\0';

				char *value = strchr(line, ':');

				if (value) {
					*value = '\0';
					strlwr(line);								// We'll store all header names in lower case
					value++;
					while (isspace(*value)) value++;
					ssmap_Add(headerMap, line, value);
				} else {							// Maybe a continuation line?  We'll just drop it.
				}
				line=nextLine;
			}
		}

		if (!err) {
			const char *responseError = CheckWampHttpResponse(headerMap, outwardInfo->nonce);
			if (responseError) {
				err = 4;
				mesg = hprintf(NULL, "Invalid HTTP response: %s", responseError);
			} else {
				WS *ws = ws_NewOnChannel(chan);
				wamp = wamp_NewOnWebsocket(ws, 1);
				wamp_SetRealm(wamp, "spider");
				chan_RegisterError(chan, wamp_OnClientChannelError);			// Kill the channel if it goes dead
			}
		}
	}

	if (err) {
		chan_Delete(chan);
	}

	if (cb_onDone)
		(cb_onDone)(wamp, mesg);

	return err;
}

STATIC void SplitEndpoint(const char *szEndpoint, const char **pszProtocol, const char **pszAddress, int *pnPort, const char **pszURI, int bAllowDefault)
// Splits a protocol://address:port/URI strings into bits
// Give NULL pointers for anything you don't want.
// If bAllowDefault suitable defaults are given for any missing bits apart from the actual address.
// Protocol="https", URI="/", Port=443.
{
	char *copy=strdup(szEndpoint);
	char *chp;
	const char *szProtocol = NULL;
	const char *szAddress = NULL;
	const char *szURI = NULL;
	int nPort = 0;
	char *szColon, *szSlash;

	szEndpoint = copy;
	chp = strstr(szEndpoint, "://");							// First take the protocol off the front
	if (chp) {
		*chp='\0';
		szProtocol=strdup(szEndpoint);
		szEndpoint=chp+3;
	}

	szColon=strchr(szEndpoint, ':');							// Look for a port number
	szSlash=strchr(szEndpoint, '/');							// Look for a URI
	if (szColon && (!szSlash || szColon < szSlash)) {			// fred.com:123 or fred.com:123/uri
		*szColon='\0';
		szAddress=strdup(szEndpoint);
		nPort=atoi(szColon+1);
		if (szSlash) {											// fred.com:123/uri
			szURI=strdup(szSlash);
		} else {												// fred.com:123
			szURI=strdup("/");
		}
	} else {													// fred.com/uri or fred.com
		if (szSlash) {											// fred.com/uri
			szURI=strdup(szSlash);
			*szSlash='\0';
		} else {												// fred.com
			szURI=strdup("/");
		}
		szAddress=strdup(szEndpoint);
	}

	// Here, szProtocol or nPort may not be set so allow them to default each other or assume https/443
	if (bAllowDefault) {
		if (!szProtocol || !nPort) {
			if (!szProtocol) {										// No protocol so default it from port
				if (nPort == 443 || !nPort) {
					szProtocol=strdup("https");						// 443 implies https
				} else {
					szProtocol=strdup("http");
				}
			}
			if (!nPort) {											// Let the port set from the protocol
				if (!strcasecmp(szProtocol, "http")) {
					nPort=80;
				} else {
					nPort=443;
				}
			}
		}
	}

	szDelete(copy);

	if (pszProtocol) *pszProtocol=szProtocol; else szDelete(szProtocol);
	if (pszAddress) *pszAddress=szAddress; else szDelete(szAddress);
	if (pszURI) *pszURI=szURI; else szDelete(szURI);
	if (pnPort) *pnPort=nPort;
}

STATIC int wamp_SendHeader(CHAN *chan, SSMAP *header)
// Sends the header given to the channel.
// Adds in the usual spider gumpf
{
	time_t now = time(NULL);
	struct tm* tm = localtime(&now);
	char timebuf[50];
	strftime(timebuf, sizeof(timebuf), "%a, %d %b %y %T %z", tm);
//	ssmap_Add(header, "Date", timebuf);

	int day, mon, year, hour, min, sec;
	const char szMon[]="JanFebMarAprMayJunJulAugSepOctNovDec";
	for (mon=0;mon<12;mon++) if (!memcmp(szMon+mon*3, __DATE__, 3)) break;
	mon++;
	day=atoi(__DATE__+4);
	year=atoi(__DATE__+7);
	hour=atoi(__TIME__);
	min=atoi(__TIME__+3);
	sec=atoi(__TIME__+6);
	const char *tmp = hprintf(NULL, "%04d%02d%02d%02d%02d%02d", year, mon, day, hour, min, sec);
//	ssmap_Add(header, "X-WAMP-LIB-COMPILED", tmp);
	szDelete(tmp);

	ssmap_Reset(header);
	const char *name;
	const char *value;
	while (ssmap_GetNextEntry(header, &name, &value)) {
		chan_WriteHeap(chan, -1, hprintf(NULL, "%s: %s\r\n", name, value));
	}

	return 0;
}

API int wamp_Connect(CHANPOOL *pool, const char *url, INTWAMPFN cb_OnDone)
// Makes a connection to another WAMP process on a given URL and drops it into the pool given.
// ... Currently this will always be WebSocket based, but should probably allow for a RawSocket connection as it's simpler
// 1. Decode the address and complain if it's duff
// 2. Make the physical connection (note that this may be TLS)
// 3. Create a channel on this connection
// 4. Put the HTTP header into the outgoing queue for the channel
// 5. Arrange that the channel is handled by our 'connection' responder
// 6. Drop back to the rest can be done asynchronously
// ...  Time passes and we are in the loop, we'll receive some input on this channel (or it might go away):
// 7. Check the response header is good
// 8. Link the 'WAMP' to the web socket and drop back
// 9. Send an initial HELLO packet.
// Returns	1	Failed to figure an address
// ALWAYS calls cb_OnDone in one way or another
{
	const char *protocol = NULL;
	const char *address = NULL;
	int port = 0;
	const char *uri = NULL;
	static const char *mesg = NULL;

	szDelete(mesg);
	mesg = NULL;

// 1. Decode the address and complain if it's duff
	SplitEndpoint(url, &protocol, &address, &port, &uri, 0);
	if (!protocol) protocol = strdup("http");
	if (!port) port = 4512;									// TODO: Really?

	if (!address) {											// Couldn't understand it
//Log("WA: Couldn't understand address to which to connect WAMP");
		if (cb_OnDone) {
			mesg = hprintf(NULL, "Cannot understand address '%s'", url);
			(cb_OnDone)(NULL, mesg);
		}
		return 1;
	}

// 2. Make the physical connection (note that this may be TLS)
	int nSock = tcp_Connect(address, port);
	if (!nSock) {
//Log("WA: Could not create a connection to %s:%d\n", address, port);
		if (cb_OnDone) {
			mesg = hprintf(NULL, "Cannot connect to host on %s:%d", address, port);
			(cb_OnDone)(NULL, mesg);
		}
		return 2;
	}

	BIO *bio=BIO_new_socket(nSock, BIO_NOCLOSE);
// TODO: If this is TLS, we'll need to do a little more work in here

// 3. Create a channel on this connection
	CHAN *chan = chan_NewBio(pool, bio, CHAN_IN | CHAN_OUT);

// 4. Put the HTTP header into the outgoing queue for the channel
	static char inited = 0;
	if (!inited++)
		srand48(time(NULL));

	char buf[16];
	((long*)buf)[0] = lrand48();
	((long*)buf)[1] = lrand48();
	((long*)buf)[2] = lrand48();
	((long*)buf)[3] = lrand48();
	const char *nonce = mime_Base64Enc(16, buf, 0, NULL);

	SSMAP *header = ssmap_New();
	ssmap_Add(header, "Host",address);
	ssmap_Add(header, "Upgrade","websocket");
	ssmap_Add(header, "Connection","Upgrade");
	ssmap_Add(header, "Origin","localhost");				// TODO: May not be the case!
	ssmap_Add(header, "Sec-WebSocket-Protocol", "wamp.2.json");
	ssmap_Add(header, "Sec-WebSocket-Version", "13");
	ssmap_Add(header, "Sec-WebSocket-Key", nonce);

	outward_connection_t *outwardInfo = NEW(outward_connection_t, 1);
	outwardInfo->nonce = nonce;
	outwardInfo->onDone = cb_OnDone;

	chan_WriteHeap(chan, -1, hprintf(NULL, "GET %s HTTP/1.1\r\n", uri));

	wamp_SendHeader(chan, header);

	chan_Write(chan, -1, "\r\n");				// Send the blank body

// 5. Arrange that the channel is handled by our 'connection' responder
	chan_OnDelete(chan, wamp_OnChannelDeleteOutward);
	chan_SetInfo(chan, outwardInfo);
	chan_RegisterReceiver(chan, wamp_OnHttpResponse, 1);

// 6. Drop back to the rest can be done asynchronously
	return 0;
}

typedef struct inward_connection_t {
	int stage;
	const char *verb;
	const char *uri;
	const char *version;
	SSMAP *headers;
	INTWAMPFN onDone;
} inward_connection_t;

STATIC void wamp_OnChannelDeleteInward(CHAN *chan)
{
Log("WA: Wamp inward channel %s is being deleted", chan_Name(chan));

	inward_connection_t *in = (inward_connection_t*)chan_Info(chan);

	if (in) {							// Must be non-NULL but being safe is cheap
//Log("WA: *** Inward = %p", in);
//Log("WA: Verb = '%s'", in->verb);
//Log("WA: URi = '%s'", in->uri);
//Log("WA: Version = '%s'", in->version);
		ssmap_Delete(in->headers);
		szDelete(in->verb);
		szDelete(in->uri);
		szDelete(in->version);
		free((char*)in);

		chan_SetInfo(chan, NULL);
	}
}

static int wamp_SendHttp(CHAN *chan, int code, SSMAP *header, int contentLength, const char *content)
// contentLength = -1, content != NULL		Send content, which is a string
// contentLength = n, content != NULL		Send contentLength bytes at content
// contentLength = -1. content = NULL		Send header with no content length
// contentLength = 0..., content = NULL		Send header with content length, but no content
{
//Log("WA: wamp_SendHttp(%s, %d, %p, %d, %p)", chan_Name(chan), code, header, contentLength, content);
	if (content && contentLength == -1)
		contentLength = strlen(content);

	int deleteHeader = 0;					// Set to 1 if we create a header ourselves

	if (!header) {
		header = ssmap_New();
		deleteHeader = 1;
	}

	if (contentLength != -1) {
		char buf[20];

		snprintf(buf, sizeof(buf), "%d", contentLength);
		ssmap_Add(header, "Content-Length", buf);
	}

	char hostname[50];
	gethostname(hostname, sizeof(hostname));
//	ssmap_Add(header, "Host", hostname);

	chan_Write(chan, -1, wamp_HttpHeader(code));			// Send the "HTTP/1.1 code text" line

	wamp_SendHeader(chan, header);				// Send the header

	chan_Write(chan, -1, "\r\n");					// Separate the header from the body

	if (contentLength && content)					// Send the body if there is any
		chan_Write(chan, contentLength, content);

	if (deleteHeader)
		ssmap_Delete(header);

	return 0;
}

static int wamp_FailWebsocket(CHAN *chan, const char *reason)
{
	inward_connection_t *inwardInfo = chan_Info(chan);
	INTWAMPFN cb_onDone = inwardInfo->onDone;
Log("WA: Failing websocket connection - %s", reason);
	wamp_SendHttp(chan, 400, NULL, -1, reason);
	chan_CloseOnEmpty(chan);

	if (cb_onDone)
		(cb_onDone)(NULL, reason);
}

static int wamp_HaveWebsocket(CHAN *chan)
// Called when we've got in a header that declares us as a websocket
// At this point, we either succeed or fail!
{
	inward_connection_t *inwardInfo = chan_Info(chan);
	SSMAP *headers = inwardInfo->headers;
	INTWAMPFN cb_onDone = inwardInfo->onDone;

	SSMAP *responseMap = ssmap_New();

	const char *szSubProtocol = ssmap_GetValue(headers, "sec-websocket-protocol");
	if (!szSubProtocol) return wamp_FailWebsocket(chan, "sec-Websocket-Protocol missing");
	if (stricmp(szSubProtocol, "wamp.2.json")) return wamp_FailWebsocket(chan, "sec-Websocket-Protocol must be wamp.2.json");

	const char *szNonce = ssmap_GetValue(headers,"sec-websocket-key");
	if (!szNonce) return wamp_FailWebsocket(chan, "sec-Websocket-Key missing");
	if (strlen(szNonce) != 24) return wamp_FailWebsocket(chan, "sec-Websocket-Key must be 24 characters");

	const char *szKey = hprintf(NULL, "%s%s", szNonce, "258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
	unsigned char buf[20];
	SHA1((const unsigned char *)szKey, strlen(szKey), buf);
	const char *szAccept = mime_Base64Enc(20, (const char *)buf, 0, NULL);
//Log("WA: IN NONCE:  '%s'", szNonce);
//Log("WA: IN ACCEPT: '%s'", szAccept);

	ssmap_Add(responseMap, "Server", "MTWAMP/1.0");
	ssmap_Add(responseMap, "Upgrade","websocket");
	ssmap_Add(responseMap, "Connection","Upgrade");
	ssmap_Add(responseMap, "Sec-WebSocket-Accept",szAccept);
	ssmap_Add(responseMap, "Sec-WebSocket-Protocol",szSubProtocol);

	wamp_SendHttp(chan, 101, responseMap, -1, NULL);

	WS *ws = ws_NewOnChannel(chan);									// Create a new websocket connected to the channel
	WAMP *wamp = wamp_NewOnWebsocket(ws, 0);						// Add a wamp client with the websocket
	chan_RegisterError(chan, wamp_OnClientChannelError);

	if (cb_onDone)
		(cb_onDone)(wamp, NULL);
}

static int wamp_OnPreamble(CHAN *chan)
// Pubsub connection is in progress - we accept the HTTP header here then connect to a web socket if it goes well.
{
	HLIST *h = chan_IncomingQueue(chan);
	inward_connection_t *inwardInfo = chan_Info(chan);

//Log("WA: Stage %d in preamble", inwardInfo->stage);
	switch (inwardInfo->stage) {
	case 1:															// Stage 0, initial connection
		{
			const int maxpeek = 10240;
			int got;
			const char *header = hlist_Peek(h, maxpeek, &got);

			const char *lf = memchr(header, '\n', got);					// We're going to need a linefeed
			if (lf && got >= 11) {										// Have at least one line ("V / HTTP/2\n" is shortest possible)
				got = lf-header+1;										// The number of bytes we want
				char *szLine = (char *)malloc(got);						// Take the line from the input
				hlist_GetDataToBuffer(h, got, szLine);

				szLine[--got]='\0';										// Replace the \n with '\0'
				if (szLine[got-1] == '\r')								// Should be a trailing \r
					szLine[--got]='\0';									// so trim it off

				const char *szVerb = NULL;
				const char *szURI = NULL;
				const char *szVersion = NULL;
				char *szSpace;
				char *chp;

				while (isspace(*szLine)) szLine++;						// Allow blanks before the VERB

				szSpace = strchr(szLine, ' ');							// Following verb

				if (szSpace) {
					char *chp;
					szVerb=strnappend(NULL, szLine, szSpace-szLine);	// Get copy of first word as szVerb

					while (*szSpace == ' ') szSpace++;					// Skip to second word
					chp=szSpace;
					szSpace=strchr(chp, ' ');							// Look for space after the URI
					if (szSpace) {
						szURI=strnappend(NULL, chp, szSpace-chp);
					}

					while (*szSpace == ' ') szSpace++;					// Skip to next word
					szVersion=strdup(szSpace);							// Take last one (HTTP/1.x)
				}

			// szVerb, szURI and szVersion should now all be kosher and on the heap
				if (szVerb && szURI && szVersion) {
					inwardInfo->stage = 2;
					inwardInfo->verb = szVerb;
					inwardInfo->uri = szURI;
					inwardInfo->version = szVersion;
				}
			}
		}
		break;
	case 2:																// Got VERB line, need header...
		{
			const int maxpeek = 10240;
			int got;
			const char *header = hlist_Peek(h, maxpeek, &got);

			const char *lf = memchr(header, '\n', got);					// We're going to need a linefeed
			if (lf) {													// Have at least one line
				got = lf-header+1;										// The number of bytes we want
				const char *colon = memchr(header, ':', got);			// There must be a colon

				char *szLine = (char *)malloc(got);						// Take the line from the input
				hlist_GetDataToBuffer(h, got, szLine);

				szLine[--got]='\0';										// Replace the \n with '\0'
				if (szLine[got-1] == '\r')								// Should be a trailing \r
					szLine[--got]='\0';									// so trim it off

				if (colon) {
					char *value = strchr(szLine, ':');					// There must be a colon now as else we wouldn't be here
					*value = '\0';
					value++;
					while (isspace(*value)) value++;

					strlwr(szLine);
					ssmap_Add(inwardInfo->headers, szLine, value);
				} else {												// No colon, maybe it's a blank line
					if (!got) {
						int isWebSocket = 0;

						const char *szConnection = ssmap_GetValue(inwardInfo->headers, "connection");
						const char *szUpgrade;
						if (szConnection && !stricmp(szConnection, "upgrade")) {
							szUpgrade = ssmap_GetValue(inwardInfo->headers, "upgrade");

							if (szUpgrade && !stricmp(szUpgrade, "websocket")) {
								isWebSocket = 1;
							}
						}

						if (isWebSocket) {
							wamp_HaveWebsocket(chan);
						} else {
							wamp_FailWebsocket(chan, "I can only accept websocket connections, ever so sorry (A)");
						}
					} else {
						wamp_FailWebsocket(chan, "Invalid header");
					}
				}

				szDelete(szLine);
			}
		}
		break;
	case 3:																// Got the header and such, perhaps we'll be wamping!
		{
			int isWebSocket = 0;

			const char *szConnection = ssmap_GetValue(inwardInfo->headers, "connection");
			const char *szUpgrade;
			if (szConnection && !stricmp(szConnection, "upgrade")) {
				szUpgrade = ssmap_GetValue(inwardInfo->headers, "upgrade");

				if (szUpgrade && !stricmp(szUpgrade, "websocket")) {
					isWebSocket = 1;
				}
			}

			if (isWebSocket) {
				wamp_HaveWebsocket(chan);
			} else {
				wamp_FailWebsocket(chan, "I can only accept websocket connections, ever so sorry (B)");
			}
		}
		break;
	default:
		break;
	}
}

API void wamp_AcceptBio(CHANPOOL *pool, BIO *bio, INTWAMPFN cb_onDone)
// Accept an incoming connection, attaching it to the pool if we're successful.
// cb_onDone is ALWAYS called when completed, either with the WAMP* or NULL on failure
{
	CHAN *chan = chan_NewBio(pool, bio, CHAN_IN | CHAN_OUT);
	inward_connection_t *inwardInfo = NEW(inward_connection_t, 1);
	inwardInfo->stage = 1;
	inwardInfo->verb = NULL;
	inwardInfo->uri = NULL;
	inwardInfo->version = NULL;
	inwardInfo->headers = ssmap_New();
	inwardInfo->onDone = cb_onDone;
	chan_OnDelete(chan, wamp_OnChannelDeleteInward);
	chan_SetInfo(chan, inwardInfo);
//Log("WA: *** Setting channel info for %s to %p", chan_Name(chan), inwardInfo);

	chan_RegisterReceiver(chan, wamp_OnPreamble, 1);
}
