#if 0
./makeh $0
exit 0
#endif

#include <arpa/inet.h>
#include <ctype.h>
#include <netdb.h>
#include <openssl/err.h>
#include <openssl/sha.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <setjmp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>
#include <unistd.h>

#include "hbuf.h"
#include "heapstrings.h"
#include "mime.h"
#include "mtmacro.h"
#include "mtstrings.h"

#include "mtwamputil.h"

#define API
#define STATIC static

#define WSS_ENABLED		0

#if 0
// START HEADER

#include "mtchannel.h"
#include "mtwebsocket.h"
#include "mtwamp.h"

typedef int (*WAMPCB_ConnectIn)(WAMP *wamp, const char *mesg);
typedef int (*WAMPCB_ConnectOut)(WAMP *wamp, const char *mesg, void *data);

// END HEADER
#endif

typedef struct outward_connection_t {
	const char *		nonce;
	WAMPCB_ConnectOut	onDone;
	const char *		peerName;					// The URL of the peer we're connecting to
	void *				data;						// Pointer carried through to 'onDone'
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

	if (!nlen) return haystack;

	needle_first = *(unsigned char *)needle;

	while (plen >= nlen && (p = (const char *)memchr(p, needle_first, plen - nlen + 1))) {
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
		case 418: s="I'm a teapot"; break;
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

static const char *wamp_HttpHeader(int nCode)
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
		szDelete(out->peerName);
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
	if (!upgrade || strcasecmp(upgrade, "WebSocket")) return "Upgrade header must be 'WebSocket'";
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

STATIC void wamp_OnClientChannelDelete(WS *ws)
//HERE - need to catch the two places this is referenced, adding Info() to the channel then using that here to delete the WAMP, which will in turn delete the channel and deal with the other effects of the demise of a WAMP.
{
	if (ws) {
		WAMP *wamp = (WAMP*)ws_Owner(ws);

		wamp_Delete(wamp);
	}
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
	outward_connection_t *outwardInfo = (outward_connection_t *)chan_Info(chan);
	WAMPCB_ConnectOut cb_onDone = outwardInfo->onDone;
	void *data = outwardInfo->data;
	int err = 0;											// Optimistically assume we won't get an error
	static const char *mesg = NULL;							// An error message to be passed to cb_onDone()
	WAMP *wamp = NULL;										// cb_OnDone will be called with this

	szDelete(mesg);
	mesg = NULL;

	const int maxpeek = 10240;								// 10k is very ample for a WAMP connection response message
	int got;
	const char *peek = chan_Peek(chan, maxpeek, &got);

	const char *crlfcrlf = memmem(peek, got, "\r\n\r\n", 4);	// We're going to need a linefeed
	if (!crlfcrlf) return 1;								// Have not got a full header yet so go back and wait

	got = crlfcrlf-peek+4;									// The number of bytes we want
	char *header = (char *)malloc(got);						// Take the line from the input
	chan_GetData(chan, got, header);

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
				ws_SetOwner(ws, wamp);
				ws_OnDelete(ws, wamp_OnClientChannelDelete);			// Kill the channel if it goes dead

				wamp_SetPeer(wamp, outwardInfo->peerName);
			}
		}
	}

	if (err) {
		chan_Delete(chan);
	}

	if (cb_onDone)
		(cb_onDone)(wamp, mesg, data);

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
	chp = (char *)strstr(szEndpoint, "://");							// First take the protocol off the front
	if (chp) {
		*chp='\0';
		szProtocol=strdup(szEndpoint);
		szEndpoint=chp+3;
	}

	szColon=(char *)strchr(szEndpoint, ':');							// Look for a port number
	szSlash=(char *)strchr(szEndpoint, '/');							// Look for a URI
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

STATIC const char *wamp_Header(SSMAP *header)
// Returns the header in a string
// Adds in the usual spider gumpf
{
	const char *result = NULL;

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
//Log("HEADOUT: %s: %s", name, value);
		result = hprintf(result, "%s: %s\r\n", name, value);
	}

	return result;
}

#if WSS_ENABLED

static int verify_callback_rcv(int preverify_ok, X509_STORE_CTX *ctx)
{
	return 1;
}

static DH *get_dh1024()
{
	static unsigned char dh1024_p[]={
		0x98,0x28,0xDC,0x86,0x4B,0x8D,0xFD,0xF1,0x27,0x33,0xC9,0x6C,
		0x88,0x41,0x7D,0xBA,0xD0,0xDE,0xD5,0x42,0x14,0x51,0xF6,0x72,
		0x06,0x4D,0x08,0xEB,0x46,0x7A,0xA5,0x5C,0xB8,0x65,0xC9,0x20,
		0xB3,0x7E,0xE5,0x8E,0xC2,0xAC,0x33,0xA8,0xC6,0x96,0xF1,0x98,
		0xE8,0x7F,0x75,0x7D,0xDA,0xD7,0x93,0x5A,0x6F,0x2F,0xE6,0xE0,
		0xB3,0xB6,0x2B,0x31,0xCF,0xB1,0xF2,0xC2,0x60,0x0C,0x18,0x33,
		0x06,0xF6,0xE3,0xBB,0x6D,0xF1,0x80,0xA0,0x1C,0x53,0xBE,0x9F,
		0x74,0x6E,0xA8,0xBE,0x36,0x03,0x9B,0xF4,0xD0,0x3C,0x87,0xE0,
		0x6E,0xA2,0x13,0x87,0x2A,0x85,0x27,0x87,0x4F,0xAA,0x2B,0xAA,
		0x34,0x2E,0x97,0x36,0xC4,0xA4,0x32,0x8D,0x0B,0xC6,0x5E,0x68,
		0x4F,0x79,0xC7,0x79,0x85,0x9E,0x0E,0x53,
	};
	static unsigned char dh1024_g[]={
		0x02,
	};
	DH *dh;

	if ((dh=DH_new()) == NULL) return(NULL);
	dh->p=BN_bin2bn(dh1024_p,sizeof(dh1024_p),NULL);
	dh->g=BN_bin2bn(dh1024_g,sizeof(dh1024_g),NULL);
	if ((dh->p == NULL) || (dh->g == NULL))
	{ DH_free(dh); return(NULL); }
	return(dh);
}

static void load_dh_params(SSL_CTX *ctx)
{
	DH *ret=get_dh1024();

	if(SSL_CTX_set_tmp_dh(ctx,ret)<0) {
		Log("WA: FATAL, weird - Couldn't set DH parameters");
	}
}

static char *_szPassword = NULL;
static char s_server_session_id_context[] = "context1";

static int cb_Password(char *buf,int num, int rwflag,void *userdata)
	// Callback function for returning the password to get into the DES3 encrypted private key file
{
	if(num<(int)strlen(_szPassword)+1) return(0);       // Too short a buffer

	strcpy(buf,_szPassword);                        // Copy over plain text buffer???

	return(strlen(_szPassword));
}

static char *_szCtxError = NULL;

static const char *ctx_Error()
{
	return _szCtxError;
}

static void ctx_Delete(SSL_CTX *ctx)
// Delete an SSL context
{
	if (ctx) SSL_CTX_free(ctx);
}

static SSL_CTX *ctx_New(const char *szConfigDir, const char *szPassword)
// Create a new SSL context and return it
//   mt.ca, mt.cert and mt.key are used with the default pw
//   These are the certificate authority chain, the local certificate and the key respectively.
// On error, returns NULL - call ctx_Error() to pick up the error message
// NB. We keep szCertCA, szCertLocal and szKeyLocal around in case SSL_CTX_load_verify_locations() needs
//		them after the function exits.
{
	const SSL_METHOD *meth = NULL;
	SSL_CTX *ctx = NULL;

	const char *szCertCA=hprintf(NULL, "%s/spiderca.cert.pem", szConfigDir);
	const char *szCertLocal=hprintf(NULL, "%s/spider.cert.pem", szConfigDir);
	const char *szKeyLocal=hprintf(NULL, "%s/spiderca.key.pem", szConfigDir);
	_szPassword=strdup(szPassword);

	szDelete(_szCtxError);
	_szCtxError = NULL;

	static int initialised = 0;
	if (!initialised) {				// Need to initialise
		SSL_library_init();
		SSL_load_error_strings();
	}

// No longer do this as giving a handler for SIGPIPE stops write() from returning an error on writing to
// a broken pipe and we'd rather handle it at the 'write()' stage as it's more controled.
//	signal(SIGPIPE,sigpipe_handle);					// In case of premature connection closure

	meth=SSLv23_method();							// Create our context
	ctx=SSL_CTX_new(meth);
//Log("%p=SSL_CTX_new(%p)", ctx, meth);
	if (!ctx) {
		_szCtxError = hprintf(NULL, "Failed to create an SSL/TLS context");
	}

	if (!_szCtxError) {								// Get our certificates from the key file
//Log("Getting certificate chain file");
		if (!(SSL_CTX_use_certificate_chain_file(ctx, szCertLocal))) {
			_szCtxError = hprintf(NULL, "Can't read local certificate file '%s'", szCertLocal);
		}
	}

	if (!_szCtxError) {								// Set the password
//Log("Setting the password");
		SSL_CTX_set_default_passwd_cb(ctx, cb_Password);

		if(!(SSL_CTX_use_PrivateKey_file(ctx, szKeyLocal, SSL_FILETYPE_PEM))) {
			_szCtxError = hprintf(NULL, "Can't read key file '%s'", szKeyLocal);
		}
	}

	if (!_szCtxError) {								// Get the list of trusted certificates
//Log("Getting the list of trusted certificates");
		if(!(SSL_CTX_load_verify_locations(ctx, szCertCA,0))) {
			_szCtxError = hprintf(NULL, "Can't read CA list file '%s'", szCertCA);
		}
	}

	if (_szCtxError)
		Log("ctx_New(): Error = '%s'", _szCtxError ? _szCtxError : "None at all");

	szDelete(szCertCA);
	szDelete(szCertLocal);
	szDelete(szKeyLocal);

	if (_szCtxError) {						// We came unstuck somewhere
		return NULL;
	} else {
		load_dh_params(ctx);				// Exits on failure

		return ctx;
	}
}

#define PASSWORD "mountain"

static int LogSslError(SSL *ssl, int ret)
	// Logs the error that occurred.
	// Returns  0   It was some miscellaneous error
	//          1   Dropped line
	//          2   Broken pipe (roughly equivalent to dropping the line)
{
	int nErr2 = SSL_get_error(ssl, ret);
	int nErr3;
	const char *szErr;
	int nResult;

	switch (nErr2) {
		case SSL_ERROR_NONE:                szErr="NONE";   break;
		case SSL_ERROR_SSL:                 szErr="SSL"; break;
		case SSL_ERROR_WANT_READ:           szErr="WANT_READ"; break;
		case SSL_ERROR_WANT_WRITE:          szErr="WANT_WRITE"; break;
		case SSL_ERROR_WANT_X509_LOOKUP:    szErr="WANT_X509_LOOKUP"; break;
		case SSL_ERROR_SYSCALL:             szErr="SYSCALL"; break;
		case SSL_ERROR_ZERO_RETURN:         szErr="ZERO_RETURN"; break;
		case SSL_ERROR_WANT_CONNECT:        szErr="WANT_CONNECT"; break;
		case SSL_ERROR_WANT_ACCEPT:         szErr="WANT_ACCEPT"; break;
		default:    szErr="Dunno"; break;
	}

	if (nErr2 == SSL_ERROR_SYSCALL && !ret) {
		nResult=1;                              // The far end dropped the line neatly
	} else if (nErr2 == SSL_ERROR_SYSCALL && ret == -1 && errno == 32) {
		nResult=2;                              // Broken pipe - the far end broke off mid-protocol?
	} else {
		nResult=0;
		Log("SSL Error ret = %d -> %d \"SSL_ERROR_%s\" (errno = %d)", ret, nErr2, szErr, errno);
	}

	while ((nErr3 = ERR_get_error())) {
		Log("SSL: %s", ERR_error_string(nErr3, NULL));
	}

	return nResult;
}

STATIC BIO *wamp_NegotiateTls(int fd)
{
	SSL_CTX *ctx;
	SSL *ssl;
	BIO *sbio;
	int r;
	BIO *io,*ssl_bio;
	X509 *peer;
	const char *szCheck;
	int nMode = 0;								// Caller certificate verification mode

Log("WA: At line %d", __LINE__);
	/* Build our SSL context*/
	ctx=ctx_New("/usr/mt/spider/etc", PASSWORD);
	if (!ctx) {
		Log(ctx_Error());
		return NULL;
	}
	SSL_CTX_set_session_id_context(ctx, (const unsigned char *)s_server_session_id_context, sizeof(s_server_session_id_context));
Log("WA: At line %d", __LINE__);
	/* Build our SSL context*/

	// Arrange that we ask the remote end for their certificate
//o	szCheck=config_GetSetString("verify-tls", "Ask", "Ask, Must, Once or None");
	szCheck = "Ask";
Log("WA: At line %d", __LINE__);
	/* Build our SSL context*/

	switch (toupper(*szCheck)) {
	case 'V': case 'A':							// Verify or Ask
		nMode = SSL_VERIFY_PEER;
		break;
	case 'M':									// Must
		nMode = SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT;
		break;
	case 'O':									// Once
		nMode = SSL_VERIFY_PEER | SSL_VERIFY_CLIENT_ONCE;
		break;
	case 'N':									// None or No
		nMode = SSL_VERIFY_NONE;
		break;
	default:
		Log("WARNING: VERIFY-TLS setting (%s) is not recognised", szCheck);
		nMode = SSL_VERIFY_NONE;
	}

	SSL_CTX_set_verify(ctx,nMode,verify_callback_rcv);

Log("WA: At line %d", __LINE__);
	/* Build our SSL context*/
	ssl=SSL_new(ctx);
Log("WA: At line %d", __LINE__);
	/* Build our SSL context*/

	SSL_set_verify(ssl, nMode, verify_callback_rcv);
Log("WA: At line %d", __LINE__);
	/* Build our SSL context*/

	sbio=BIO_new_socket(fd, BIO_NOCLOSE);
Log("WA: At line %d", __LINE__);
	/* Build our SSL context*/
	SSL_set_bio(ssl, sbio, sbio);
Log("WA: At line %d", __LINE__);
	/* Build our SSL context*/

	if (((r=SSL_accept(ssl)) <= 0)) {
		int nOk = LogSslError(ssl,r);
		if (!nOk) {
			Log("SSL accept error (r=%d, errno=%d)", r, errno);
			return NULL;
		} else {
			ctx_Delete(ctx);				// Tidy up
			if (nOk == 1) {
				Log("Calling SSL connection immediately hung up");
			} else {
				Log("Calling SSL connection broke the connection");
			}
			exit(0);						// Only a child and we're done
		}
	}

	io=BIO_new(BIO_f_buffer());
	ssl_bio=BIO_new(BIO_f_ssl());
	BIO_set_ssl(ssl_bio,ssl,BIO_CLOSE);
	BIO_push(io,ssl_bio);

Log("WA: At line %d", __LINE__);
	/* Build our SSL context*/
	SSL_get_verify_result(ssl);
	peer=SSL_get_peer_certificate(ssl);

Log("WA: At line %d", __LINE__);
	/* Build our SSL context*/
// Here is where we need to check the name returned below against the expected CN of the caller.
	if (peer) {
		char buf[256];

		X509_NAME_get_text_by_NID(X509_get_subject_name(peer), NID_commonName, buf, sizeof(buf));
		Log("Peer identified as '%s'", buf);

		X509_NAME_oneline(X509_get_subject_name(peer), buf, 256);
		Log("Issuer of certificate was '%s'", buf);

		X509_free(peer);
	} else {
		if (nMode & SSL_VERIFY_PEER) Log("Client has not provided a certificate");
	}

Log("WA: At line %d", __LINE__);
	/* Build our SSL context*/
	return io;
}
#endif

API int wamp_Connect(CHANPOOL *pool, const char *url, WAMPCB_ConnectOut cb_OnDone, void *data /*=NULL*/)
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
//			2	Unrecognised protocol
//			3	Failed to connect
//			4	SSL connection attempted when SSL not compiled in
//			5	SSL protocol failed
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
			(cb_OnDone)(NULL, mesg, data);
		}
		return 1;
	}

	const char *host = strdup(address);
	if (port) host = hprintf(host, ":%d", port);

	if (strcasecmp(protocol, "http")
			&& strcasecmp(protocol, "https")
			&& strcasecmp(protocol, "ws")
			&& strcasecmp(protocol, "wss")) {
		if (cb_OnDone) {
			mesg = hprintf(NULL, "Unrecognised protocol '%s' - must be http, https, ws or wss", protocol);
			(cb_OnDone)(NULL, mesg, data);
		}
		return 2;
	}

// 2. Make the physical connection (note that this may be TLS)
	int nSock = tcp_Connect(address, port);
	if (!nSock) {
//Log("WA: Could not create a connection to %s:%d\n", address, port);
		if (cb_OnDone) {
			mesg = hprintf(NULL, "Cannot connect to host on %s:%d", address, port);
			(cb_OnDone)(NULL, mesg, data);
		}
		return 3;
	}

	BIO *bio = NULL;
	if (!strcasecmp(protocol, "https") || !strcasecmp(protocol, "wss")) {
#if WSS_ENABLED
		bio = wamp_NegotiateTls(nSock);
		if (!bio) {
			Log("Failed SSL protocol");
			return 5;
		}
#else
		Log("WA: Attempt to make WSS: connection when SSL not compiled in");
		return 4;
#endif

// TODO: If this is TLS, we'll need to do a little more work in here
	} else {
		bio=BIO_new_socket(nSock, BIO_NOCLOSE);
	}

// 3. Create a channel on this connection
	CHAN *chan = chan_NewBio(pool, bio, CHAN_IN | CHAN_OUT);

// 4. Put the HTTP header into the outgoing queue for the channel
	static char inited = 0;
	if (!inited++)
		srand48(time(NULL));

	char buf[sizeof(long)*4];		// SCO sizeof(long)=4, linux sizeof(long) may be 8
	((long*)buf)[0] = lrand48();
	((long*)buf)[1] = lrand48();
	((long*)buf)[2] = lrand48();
	((long*)buf)[3] = lrand48();
	const char *nonce = mime_Base64Enc(16, buf, 0, NULL);		// Even in 64-bit environment, we only want 16 bytes worth

	SSMAP *header = ssmap_New();
	ssmap_Add(header, "Host",host);
	ssmap_Add(header, "Upgrade","WebSocket");
	ssmap_Add(header, "Connection","Upgrade");
	ssmap_Add(header, "Origin","http://localhost");				// TODO: May not be the case!
	ssmap_Add(header, "Sec-WebSocket-Protocol", "wamp.2.json");
	ssmap_Add(header, "Sec-WebSocket-Version", "13");
	ssmap_Add(header, "Sec-WebSocket-Key", nonce);

	outward_connection_t *outwardInfo = NEW(outward_connection_t, 1);
	outwardInfo->nonce = nonce;
	outwardInfo->peerName = strdup(url);
	outwardInfo->onDone = cb_OnDone;
	outwardInfo->data = data;

	chan_WriteHeap(chan, -1, hprintf(NULL, "GET %s HTTP/1.1\r\n", uri));

	const char *headerText = wamp_Header(header);
	chan_Write(chan, -1, headerText);			// Send the headers
	szDelete(headerText);

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
	WAMPCB_ConnectIn onDone;
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

	const char *topLine = wamp_HttpHeader(code);		// Get the "HTTP/1.1 code text" line

	const char *headers = wamp_Header(header);			// Send the header

	const char *headerPart = hprintf(NULL, "%s%s\r\n", topLine, headers);
	szDelete(topLine);
	szDelete(headers);

	chan_Write(chan, -1, headerPart);					// Send the whole lot together

	if (contentLength && content)					// Send the body if there is any
		chan_Write(chan, contentLength, content);

	if (deleteHeader)
		ssmap_Delete(header);

	return 0;
}

static int wamp_FailWebsocket(CHAN *chan, const char *reason)
{
	inward_connection_t *inwardInfo = (inward_connection_t *)chan_Info(chan);
	WAMPCB_ConnectIn cb_onDone = inwardInfo->onDone;
Log("WA: Failing websocket connection - %s", reason);
	wamp_SendHttp(chan, 400, NULL, -1, reason);
	chan_CloseOnEmpty(chan);

	if (cb_onDone)
		(cb_onDone)(NULL, reason);

	return 0;
}

static int wamp_HaveWebsocket(CHAN *chan)
// Called when we've got in a header that declares us as a websocket
// At this point, we either succeed or fail!
{
	inward_connection_t *inwardInfo = (inward_connection_t *)chan_Info(chan);
	SSMAP *headers = inwardInfo->headers;
	WAMPCB_ConnectIn cb_onDone = inwardInfo->onDone;

	SSMAP *responseMap = ssmap_New();

	const char *szSubProtocol = ssmap_GetValue(headers, "sec-websocket-protocol");
	if (!szSubProtocol)
		szSubProtocol = "";

	if (*szSubProtocol && strnicmp(szSubProtocol, "wamp.2.json", 4))		// Something, but not what we're looking for
		return wamp_FailWebsocket(chan, "Only accepted sec-Websocket-Protocol is 'wamp.2.json'");

	const char *szNonce = ssmap_GetValue(headers,"sec-websocket-key");
	if (!szNonce) return wamp_FailWebsocket(chan, "sec-Websocket-Key missing");
	if (strlen(szNonce) != 24) return wamp_FailWebsocket(chan, "sec-Websocket-Key must be 24 characters");

	const char *szKey = hprintf(NULL, "%s%s", szNonce, "258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
	unsigned char buf[20];
	SHA1((const unsigned char *)szKey, strlen(szKey), buf);
	const char *szAccept = mime_Base64Enc(20, (const char *)buf, 0, NULL);

	ssmap_Add(responseMap, "Server", "MTWAMP/1.0");
	ssmap_Add(responseMap, "Upgrade","WebSocket");
	ssmap_Add(responseMap, "Connection","Upgrade");
	ssmap_Add(responseMap, "Sec-WebSocket-Accept",szAccept);
	if (*szSubProtocol)
		ssmap_Add(responseMap, "Sec-WebSocket-Protocol", "wamp.2.json");

	wamp_SendHttp(chan, 101, responseMap, -1, NULL);

	WS *ws = ws_NewOnChannel(chan);										// Create a new websocket connected to the channel
	if (!strnicmp(szSubProtocol, "wamp.2.json", 4)) {
		WAMP *wamp = wamp_NewOnWebsocket(ws, 0);						// Add a wamp client with the websocket
		ws_SetOwner(ws, wamp);
		ws_OnDelete(ws, wamp_OnClientChannelDelete);

		if (cb_onDone)
			(cb_onDone)(wamp, NULL);
	}

	return 0;
}

static int wamp_OnPreamble(CHAN *chan)
// Pubsub connection is in progress - we accept the HTTP header here then connect to a web socket if it goes well.
{
	inward_connection_t *inwardInfo = (inward_connection_t *)chan_Info(chan);
	if (!inwardInfo) {
		Log("wamp_OnPreamble(%s) - inwardInfo is NULL!", chan_Name(chan));
		return 1;
	}

//Log("WA: Preamble being processed - inwardInfo = %p", inwardInfo);
//Log("WA: Stage %d in preamble", inwardInfo->stage);
	switch (inwardInfo->stage) {
	case 1:															// Stage 1, initial connection - look for the VERB line
		{
			const int maxpeek = 10240;
			int got;
			const char *header = chan_Peek(chan, maxpeek, &got);

			const char *lf = (const char *)memchr(header, '\n', got);	// We're going to need a linefeed
			if (lf && got >= 11) {										// Have at least one line ("V / HTTP/2\n" is shortest possible)
				got = lf-header+1;										// The number of bytes we want
				char *szLine = (char *)malloc(got);						// Take the line from the input
				chan_GetData(chan, got, szLine);

				szLine[--got]='\0';										// Replace the \n with '\0'
				if (szLine[got-1] == '\r')								// Should be a trailing \r
					szLine[--got]='\0';									// so trim it off

				const char *szVerb = NULL;
				const char *szURI = NULL;
				const char *szVersion = NULL;
				char *szSpace;

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
				if (szVerb && szURI && szVersion) {						// Prepare ourselves for the next stage
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
			const char *header = chan_Peek(chan, maxpeek, &got);

			const char *lf = (const char *)memchr(header, '\n', got);	// We're going to need a linefeed
			if (lf) {													// Have at least one line
				got = lf-header+1;										// The number of bytes we want
				const char *colon = (const char *)memchr(header, ':', got);	// There must be a colon

				char *szLine = (char *)malloc(got);						// Take the line from the input
				chan_GetData(chan, got, szLine);

//Log("WA: HEADER (%s): %s", chan_Name(chan), Printable(got, szLine));
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
						if (szConnection && stristr(szConnection, "upgrade")) {
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

	return 0;
}

API void wamp_AcceptBio(CHANPOOL *pool, BIO *bio, WAMPCB_ConnectIn cb_onDone, SSMAP *headers/*=NULL*/)
// Accept an incoming connection, attaching it to the pool if we're successful.
// cb_onDone is ALWAYS called when completed, either with the WAMP* or NULL on failure.
// If headers is non-NULL then we assume we've already read the headers and they're in that map.
{
	CHAN *chan = chan_NewBio(pool, bio, CHAN_IN | CHAN_OUT);
	inward_connection_t *inwardInfo = NEW(inward_connection_t, 1);
	inwardInfo->verb = NULL;
	inwardInfo->uri = NULL;
	inwardInfo->version = NULL;
	if (headers) {									// We have already parsed the header
		inwardInfo->stage = 3;
		inwardInfo->headers = headers;
	} else {
		inwardInfo->stage = 1;
		inwardInfo->headers = ssmap_New();
	}
	inwardInfo->onDone = cb_onDone;
	chan_OnDelete(chan, wamp_OnChannelDeleteInward);
	chan_SetInfo(chan, inwardInfo);
Log("WA: *** Setting channel info for %s to %p", chan_Name(chan), inwardInfo);

	chan_RegisterReceiver(chan, wamp_OnPreamble, 1);
}
