// Spider - The thing that marshalls requests to RPC code
//

// 23-01-11 RJ 1.00 Taken from version 1.55 of the MMTS source code
// 08-04-11 RJ 1.01 Reads environment variables for RPCs from etc/environment.ini
// 17-06-11 RJ 1.02 Made XML in stdin be rendered properly to the API
// 10-07-11 RJ 1.03 Added POST /mtrpc functionality to be a slicker interface
// 21-07-11 RJ 1.04 Had better attempt at having input render properly
// 26-07-11 RJ 1.05 Correctly renders <input>, handles returning stderr properly and had better message lists
// 09-08-11 RJ 1.06 Better handling of HTML lists of messages and re-compiled with rogxml fix (for ]]> matching)
// 28-09-11 RJ 1.07 Sets unask(2) and uid/gid to match pm1
// 12-01-12 SC 1.08 API stderr logged even in event of unexpected termination
// 16-03-12 RJ 1.09 21845 Set RPC_CLIENTIP from actual IP rather than trust the client to tell us
// 24-04-12 RJ 1.10 Added SPIDER_VERSION, SPIDER_BASEDIR, SPIDER_SERVER variables when calling RPCs
// 12-07-12 RJ 1.11 Recompiled to incorporate the UTF-8 XML amendments in the rogxml library
// 29-08-12 RJ 1.20 Compiled under linux, removing dependencies on spider-contract.c and spider-message.c
// 24-12-12 RJ 1.21 Nicer colour scheme when listing stuff
// 19-02-13 RJ 1.22 Tidy up all but the most recent 5000 ($maxmsglog) dirs from msglog each night
// 21-02-13 RJ 1.23 Rationalised the spider.conf reading/writing
// 22-02-13 RJ 1.24 Fixed that rcv.xml wasn't getting saved in debug mode
// 10-03-13 RJ 1.25 Added spider.enumerateAPIs
// 01-04-13 RJ 1.25x Experimental - handling session IDs (bHandlingSessions)
// 17-05-13 RJ 1.26 26744 Return more useful error on broken pipe when feeding an API
// 29-08-13 RJ 1.27 Changed the log time to use msecs to give finer timings
// 24-11-13 RJ 1.28 Changed handling of return codes 2-9 from RPCs
// 26-11-13 RJ 1.29 Stopped spurious errors generated when API returns 0...
// 31-12-13 RJ 1.30 Initial add of version numbers to all RPCs
// 08-02-14 RJ 1.31 Added management information log (logs/spider.mi)
// 19-02-14 RJ 1.32 Fixed up ADL handling so it now seems to make sense & removed some commented out code
// 19-02-14 RJ 1.33 Added ability to re-run a call
// 23-02-14 RJ 1.34 Added authorisation using auth.sl3
// 29-04-14 RJ 1.35 Backs self up to binary.version and puts symbolic link into /usr/bin
// 10-05-14 RJ 2.00 30341 Including home-grown certificates for authentication
// 08-06-14 RJ 2.01 Made msglog heirarchical like MMTS
// 16-06-14 RJ 2.02 Added OS constant and display it variously
// 20-07-14 RJ 2.03 Updated & debugged some to make Linux version execute correctly
// 20-07-14 RJ 2.04 30761 Enabled environments
// 21-07-14 RJ 2.05 Added ability to send spider request from command line
// 31-07-14 RJ 2.06 30939 Added sensible contract handling and tidied logging a little
// 03-08-14 RJ 2.07 30939 Fixing usage recording
// 10-08-14 RJ 2.08 30939 Improved contract handling
// 10-08-14 RJ 2.09 30988 Roll over log and mi files if they get big (> 1GB)
// 10-08-14 RJ 2.10 30988 Updated command line request while there (forgot to checkin beforehand)

#define VERSION				"2.10"

// TODO: Environment switching according to subsidiary software version and other parameters
// TODO: Fix isRevoked once environment switching is done - also rationalise the rest of the certificate checking
// TODO: Consider actual certificate files rather than individual file settings
// TODO: Allow: spider some.api params

// TODO: Look at rog_MkDir as it seems flawed (there are comments there)

// xTODO: Look for session.create and snaffle the session ID for the spider.im file
// xTODO: Make msglog hierarchical like mmts
// xTODO: Clever stuff with certificates
// xTODO: Link spider to /usr/bin/spider
// xTODO: Copy binary to /usr/spider/bin/spider.134 etc.

// TODO?: (maybe not for IM) In all responses need to include: spider version, API version, message tracking id
// xTODO: Message payloads need to be available for a year (zipped?), and immediately available for a month
// TODO?: Need to keep retention period information on log files - this can live in sqlite
// xTODO: Provide the version of the API in the response.  Might be file date/what/ADL
// TODO: keep a sqlite table of information for each api.  Check this on each API invocation and update if
// the API has been updated.  Keep at least: lastupdatetime,modify time of API exe, ADL information
// xTODO: Include an attribute in all replies that provides the version of the API (either date of modify
// or version taken from ADL).
//
// xTODO: Plan for archiving messages:
// Store messages in a directory structure similar to that used by MMTS year/month/day
// Process each night to zip those that are not in the current or previous month - zip up a day at a time
//    Actually, do those whose modify time is older than 6 weeks so that unzipped folders are effectively cached
// When looking back at old messages, unzip any day that is referenced so it's like it was
// This will get tidied up again overnight.

// TODO: Present session IDs to callers as GUIDs (translate user.login response, translate back on the way in)
// TODO: Safeguards against bad callers:
// xTODO:	Have a per-API limit of maximum calls within a certain time
// TODO:	Have a limit to how long an API should be allowed to run before being killed
// xTODO:	Authentication for 3rd party callers
// xTODO:	Perhaps don't allow 3rdparty calls except on HTTPS ports
// xTODO: Provide _function="spider.enumerateAPIs" that lists all APIs and do something to return per-API ADL info
// TODO: When catching SIGQUIT, set a flag and exit cleanly (and set an alarm for 4 seconds and force quit)
// TODO: Tidy up interaction with browser that shows the browser session in the display
// xTODO: Tidy up the list of messages further
// TODO: [ See GetAdl() ] Add -ADL checking (look for 'mtADLParamList' in the binary first, then call with -ADL)
// TODO: Manage persistent dir for each API
// [ADL_SUPPORTED] [INSTALL_SUPPORTED]
// xTODO: When an API terminates with a s signal, report the signal name

#ifdef __SCO_VERSION__
	#define OS	"SCO"
#else
	#define OS	"LINUX"
#endif

#include <arpa/inet.h>
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <poll.h>
#include <pwd.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <setjmp.h>
#include <sys/fcntl.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/time.h>
#include <sys/times.h>
#include <sys/types.h>
#include <sys/resource.h>
#include <sys/select.h>
#include <sys/shm.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <utime.h>
#include <zlib.h>

#include "csv.h"
#include "guid.h"
#include "hbuf.h"
#include "heapstrings.h"
#include "mime.h"
#include "mtmacro.h"
#include "rogxml.h"
#include "smap.h"
#include "mtsl3.h"
#include "xstring.h"

#include "sqlite3.h"

#ifndef SHUT_RD
	#define SHUT_RD  0			// These should be in sys/socket.h but aren't...???
	#define SHUT_WR  1
	#define SHUT_RDWR 2
#endif

#define PORT_APP	4509					// 'MMTS hello' style port number (default for 'resend')
#define PORT_HTTPS	4510					// Default port for HTTPS connections
#define PORT_HTTP	4511					// Default port for HTTP connections

#define LOGFILE_LIMIT	1000000000			// Maximum size of corresponding log files
#define MIFILE_LIMIT	1000000000

static int _bPackXML = 0;					// 1 to pack XML sent to application

#define	XML_MIME_TYPE		"text/xml"		// Something may prefer 'text/xml'

#ifndef max
#define max(a,b) ((a)>(b)?(a):(b))
#endif

static int _nAlarmNumber = 0;						// Alarm number - 0 disables returning long jump when timer triggers
static jmp_buf jmpbuf_alarm;						// Used when alarm goes off for timeouts

static time_t _tChildStart=0;						// Child start time for summary

static const char *szDefaultBaseDir = "/usr/mt/spider";	// Unless told otherwise
static const char *szEtcDirLeaf = "etc";			// Where miscellaneous stuff goes
static const char *szLogDirLeaf = "logs";			// Where logs go
static const char *szOutDirLeaf = "out";			// Outgoing messages
static const char *szEnvDirLeaf = "env";			// Environment directories
static const char *szRpcDirLeaf = "rpc";			// Where RPC binaries live
static const char *szTmpDirLeaf = "tmp";			// Where temporary files
static const char *szContractDirLeaf = "contracts";	// Where contracts live
static const char *szRpcDataDirLeaf = "rpcdata";	// Where RPC binaries live
static const char *szMsgLogDirLeaf = "msglog";		// Logged messages

static const char *szEnvironment = NULL;			// Set to a string if the envionment is non-default

static const char *szMyName = "spider";
static char szHostname[50];

static const char *g_szRpcError = NULL;				// Error returned from RPC call

static const char *g_cbacId = NULL;					// Globally accessible copy of cbacId of current caller

static const char *g_Organisation = NULL;			// The organisation that is calling us
static const char *g_Product = NULL;				// The product that is calling us
static const char *g_OrganisationProduct = NULL;	// organisation:product

static int bHandlingSessions = 0;					// We handle session IDs

const char *MonthNames[] = {"?","January","February","March","April","May","June","July","August","September","October","November","December"};

// Bit messy at the moment but we have two protocols, one for external and one for internal (applications)
// It used to be that the external one would always be TLS encrypted, but this is no longer the case...
#define SRC_IP 1							// Internal - 'Plain' application connection
#define SRC_TLS 2							// External - TLS connection
#define SRC_PLAIN 3							// External - 'Plain' connection
#define SRC_DROPPED 1000					// Must be higher than any count of network ports

typedef struct NetworkPort_t {
	int nPort;								// Port on which we're connecting
	int bEnabled;							// Only use if enabled
	int nProtocol;							// Protocol (TCP, TLS)
	time_t tRetry;							// When to re-try if not initially working
	int nRetries;							// Number of retries left
	int nSock;								// Socket being used
	int nCount;								// Total number of connections
} NetworkPort_t;

NetworkPort_t *NetworkPort = NULL;
static int nNetworkPorts=0;

static const char *_szSenderIp = NULL;			// IP of the remote incoming connection
static int _nSenderPort = 0;					// Port from which incoming message came

static const char *_szIncomingIp = NULL;		// The IP address on which we accepted the connection
static const char *_szIncomingHost = NULL;		// The host named in incoming HTTP header
static int _nIncomingPort = 0;					// Port on which we accepted the message
static int _nIncomingProtocol = 0;				// Protocol used to connect

static int _nTotalConnections = 0;				// Total connections accepted
static int _nDroppedCount = 0;					// Number of dropped files picked up

static char bIsDaemon = 1;						// 1 if Daemon, 0 otherwise

static const char *szBaseDir = NULL;
static const char *szEtcDir = NULL;				// Miscellaneous settings
static const char *szLogDir = NULL;				// Where logs go
static const char *szOutDir = NULL;				// Outgoing messages
static const char *szEnvDir = NULL;				// Environment directories
static const char *szRpcDir = NULL;				// Where RPC binaries live
static const char *szTmpDir = NULL;				// Where temporary stuff lives
static const char *szContractDir = NULL;		// Where contract files live
static const char *szRpcDataDir = NULL;			// Where RPC data lives
static const char *szMsgLogDir = NULL;			// Logged messages

static const char *szLogFile = NULL;
static const char *szMiFile = NULL;

static const char *szConfigFile = NULL;

static const char *szPidFile = NULL;			// Location of pid file

static char bVerbose = 0;						// Quiet unless told otherwise (used with -s and -S)

#define PASSWORD "mountain"						// The password to access our key

static BIO *bio_err = NULL;

static time_t _nStartTime;				// Time server started

#define CLIENT_AUTH_NONE		0
#define CLIENT_AUTH_REQUEST		1
#define CLIENT_AUTH_NEED		2

#include <compile>
static const char _id[] = "@(#)SPIDER version " VERSION " (" OS ") compiled " __DATE__ " at " __TIME__;

static char s_server_session_id_context[] = "context1"; 
//static int s_server_auth_session_id_context = 2;

static const char *_szPassword = NULL;
static const char *argv0 = NULL;			// The location of our executable

static const char *GetEtcDir()					{ return szEtcDir; }

static void Log(const char *szFmt, ...);
static void Exit(int nCode);

// Management information to be written to spider.mi
int mi_inhibit = 0;						// Set by internal processes to inhibit logging of MI on exit
struct timeval mi_tv;					// Clock ticks since start of ServeRequest()
int mi_msecs = 0;						// Number of total elapsed milliseconds
struct tms mi_tms;						// Clock details
unsigned long mi_BytesIn = 0;			// Received from caller
unsigned long mi_BytesOut = 0;			// Written to caller
const char *mi_caller = NULL;			// name of caller
const char *mi_function = NULL;			// Name of function
int mi_status = -1;						// Returned status
const char *mi_id = NULL;				// Message ID
const char *mi_session = NULL;			// Session involved
const char *mi_protocol = NULL;			// Protocol used to connect

int g_sock = -1;                            // Global things relating to the current SSL connection
SSL *g_ssl = NULL;
SSL_CTX *g_ctx = NULL;
const char *g_ssl_cn = NULL;                // The Cn of the SSL peer
#define SSL_MAXDEPTH  10
const char *g_ssl_subject[SSL_MAXDEPTH];

S3 *dbAccess = NULL;

typedef struct limits_t {				// Structure that holds limits for individual caller's APIs
	char	limited;					// 1 if there are limits, 0 otherwise (quicker than checking the 8 individual limits)
	int		qhc;
	int		qht;
	int		qdc;
	int		qdt;
	int		bhc;
	int		bht;
	int		bdc;
	int		bdt;
} limits_t;

typedef struct contract_t {
	const char *id;
	time_t mtime;						// Modify date of the file
	limits_t *limits;					// Limits imposed on the API
	const char *szFilename;				// Full name of the contract file
	const char *szOrganisationProduct;	// The organisation:product
	SSET *versions;						// The set of versions to which this contract relates
	SPMAP *apis;						// The APIs that are covered by this contract
	const char *szValidFrom;			// Valid from/to dates (YYYY-MM-DD)
	const char *szValidTo;
} contract_t;

SIMAP *contracts = NULL;				// Points to 'contract_t's, indexed on organisation:product

void ReallyExit(int nCode);

/////////////////////
//////////////////////
//////////////////////////   Here temporarily until rcache is linked in
///////////////////////
///////////////////////

const char *rcache_Encode(const char *szPlain)
// Encodes as follows:
// \ -> \\, BS -> \b, hichars (and = and ") -> \xhh etc.
// hex 00-31 -> \x00..\x1f
// Returns a string on the heap
// Creates a buffer on the heap, extending it by 'delta' byte increments
{
	if (!szPlain) return NULL;							// Someone's trying to trick us

	char *szResult = strdup("");
	int len = 0;										// Total length of szResult
	const int delta=20;									// How many bytes of this extent used in buffer
	int sublen = delta;									// Size of this buffer segment (init to force alloc 1st time)
	char c;

	while ((c=*szPlain++)) {
		const char *str="";
		char buf[5];
		switch (c) {
		case '\a':	str="\\a";	break;
		case '\b':	str="\\b";	break;
		case '\f':	str="\\f";	break;
		case '\n':	str="\\n";	break;
		case '\r':	str="\\r";	break;
		case '\t':	str="\\t";	break;
		case '\v':	str="\\v";	break;
		case '\\':	str="\\\\";	break;
		default:
			if (c < ' ' || c > '~' || c == '=' || c == '"') {	// Need to escape = as it's our separator, " as we may quote
				snprintf(buf, sizeof(buf), "\\x%02x", c & 0xff);
				str=buf;
			}
		}
		if (*str) {
			int slen=strlen(str);
			if (sublen+slen >= delta) {				// We're overflowing
				sublen=0;
				szResult = realloc(szResult, len+delta);
			}
			memcpy(szResult+len, str, slen);
			len+=slen;
			sublen+=slen;
		} else {
			if (sublen == delta) {
				sublen=0;
				szResult = realloc(szResult, len+delta);
			}
			sublen++;
			szResult[len++]=c;
		}
	}
	if (sublen == delta) szResult = realloc(szResult, len+1);
	szResult[len]='\0';

	return szResult;
}

const char *rcache_Decode(char *szCoded)
// Decodes a string, replacing \x with whatever is necessary in situ.
// Always returns the same string passed.
{
	if (!szCoded || !strchr(szCoded, '\\')) return szCoded;		// Null or no escaped chars

	char *chp=szCoded;
	char *src=szCoded;
	char *dest=szCoded;

	while ((chp=strchr(src, '\\'))) {
		int done=0;

		if (dest != szCoded && chp != src) memmove(dest, szCoded, chp-src);
		dest+=chp-src;
		src=chp+2;							// Assume we're skipping the \ and one char
		switch (chp[1]) {
		case 'a': *dest++='\a'; break;
		case 'b': *dest++='\b'; break;
		case 'f': *dest++='\f'; break;
		case 'n': *dest++='\n'; break;
		case 'r': *dest++='\r'; break;
		case 't': *dest++='\t'; break;
		case 'v': *dest++='\v'; break;
		case 'x': {
					done=1;					// Remains set if we don't have two chars after the \x
					int a=chp[2];
					if (a) {
						src++;
						int b=chp[3];
						if (b) {
							src++;
							if (a>='a') a-=('a'-'A');
							if (b>='a') b-=('a'-'A');
							a-='0'; if (a > 9) a-='A'-'9'-1;
							b-='0'; if (b > 9) b-='A'-'9'-1;
							*dest++=(a<<4) | b;
							done=0;
						}
					}
				}
				break;
		case '\0': *dest++='\\';				// There's a \ at the end of the string
				   src--;
				done=1;
				break;
		default: *dest++=chp[1]; break;
		}
		if (done) break;						// Drop out as escaping is bad
	}

	if (src != dest) memmove(dest, src, strlen(src)+1);

	return szCoded;
}

///////////////////////////////
/////////////////////////////// end of copied rcache functions
///////////////////////////////

const char *FullSignalName(int sig)
{
	const char *szName = NULL;

	switch (sig) {
	case SIGHUP:		szName="hangup"; break;
	case SIGINT:		szName="interrupt (rubout)"; break;
	case SIGQUIT:		szName="quit (ASCII FS)"; break;
	case SIGILL:		szName="illegal instruction (not reset when caught)"; break;
	case SIGTRAP:		szName="trace trap (not reset when caught)"; break;
//	case SIGIOT:		szName="IOT instruction"; break;
	case SIGABRT:		szName="used by abort, replace SIGIOT in the future"; break;
	case SIGFPE:		szName="floating point exception"; break;
	case SIGKILL:		szName="kill (cannot be caught or ignored)"; break;
	case SIGBUS:		szName="bus error"; break;
	case SIGSEGV:		szName="segmentation violation"; break;
	case SIGSYS:		szName="bad argument to system call"; break;
	case SIGPIPE:		szName="write on a pipe with no one to read it"; break;
	case SIGALRM:		szName="alarm clock"; break;
	case SIGTERM:		szName="software termination signal from kill"; break;
	case SIGUSR1:		szName="user defined signal 1"; break;
	case SIGUSR2:		szName="user defined signal 2"; break;
	case SIGCLD:		szName="child status change"; break;
//	case SIGCHLD:		szName="child status change alias (POSIX)"; break;
	case SIGPWR:		szName="power-fail restart"; break;
	case SIGWINCH:		szName="window size change"; break;
	case SIGURG:		szName="urgent socket condition"; break;
	case SIGPOLL:		szName="pollable event occured"; break;
//	case SIGIO:			szName="socket I/O possible (SIGPOLL alias)"; break;
	case SIGSTOP:		szName="stop (cannot be caught or ignored)"; break;
	case SIGTSTP:		szName="user stop requested from tty"; break;
	case SIGCONT:		szName="stopped process has been continued"; break;
	case SIGTTIN:		szName="background tty read attempted"; break;
	case SIGTTOU:		szName="background tty write attempted"; break;
	case SIGVTALRM:		szName="virtual timer expired"; break;
	case SIGPROF:		szName="profiling timer expired"; break;
	case SIGXCPU:		szName="exceeded cpu limit"; break;
	case SIGXFSZ:		szName="exceeded file size limit"; break;
#ifdef __SCO_VERSION__
	case SIGEMT:		szName="EMT instruction"; break;
	case SIGWAITING:	szName="all LWPs blocked interruptibly notification"; break;
	case SIGLWP:		szName="signal reserved for thread library implementation"; break;
	case SIGAIO:		szName="Asynchronous I/O signal"; break;
	case SIGMIGRATE:	szName="SSI - migrate process"; break;
	case SIGCLUSTER:	szName="SSI - cluster reconfig"; break;
#endif
	default: 			szName="Unknown"; break;
	}

	return szName;
}

const char *memmem(const char *haystack, size_t hlen, const char *needle, size_t nlen)
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

int GrepFile(const char *szDir, const char *szFile, const char *szSearch)
// If the entire filename is in szFile, pass szDir as NULL
{
	if (!szSearch || !*szSearch) return 1;

	int bFound=0;
	const char *szFilename = hprintf(NULL, "%s%s%s", szDir, szDir?"/":"", szFile);
	FILE *fp=fopen(szFilename, "r");

	if (fp) {
		char buf[2048];								// We'll read this much at a time
		int len=strlen(szSearch);
		int offset=0;
		int got;

		while ((got=fread(buf+offset, 1, sizeof(buf)-offset-1, fp))) {
			if (memmem(buf, got, szSearch, len)) {
				bFound=1;
				break;
			}
			if (got > len) {						// In case string overlaps buffer end, copy end of buffer to start
				memcpy(buf, buf+got-len, len);
				offset=len;
			}
		}
		fclose(fp);
	}

	return bFound;
}

static char *SkipSpaces(const char *t)
{
	while (isspace(*t)) t++;

	return (char*)t;
}

static void Fatal(const char *szFmt, ...)
// Plain errors
{
	static int inFatal = 0;

	if (inFatal) ReallyExit(0);							// Recursive call to Fatal is a reason to simply leave.
	inFatal=1;

	va_list ap;
	char buf[1000];

	va_start(ap, szFmt);
	vsnprintf(buf, sizeof(buf), szFmt, ap);
	va_end(ap);

	fprintf(stderr,"%s: %s\n",szMyName, buf);

	Log("Error: %s", buf);

	Exit(99);
	inFatal=0;										// Shouldn't happen
}

const char *NiceTime(time_t t)
{
	static int resno=0; resno=!resno;                   // Alternating 0/1
	static const char *results[2] = {NULL,NULL};
	const char *result = results[resno];

	if (!t) t = time(NULL);
	struct tm *tm = localtime(&t);

	szDelete(result);

	result = hprintf(NULL, "%02d-%02d-%04d %02d:%02d:%02d",
			tm->tm_mday, tm->tm_mon+1, tm->tm_year + 1900,
			tm->tm_hour, tm->tm_min, tm->tm_sec);

	return result;
}

static const char *ProtocolName(nProtocol)
{
	switch (nProtocol) {
	case SRC_IP: return "Application";
	case SRC_TLS: return "HTTPS";
	case SRC_PLAIN: return "HTTP";
	case SRC_DROPPED: return "DROPPED";
	}

	return "UNKNOWN PROTOCOL";
}

static void AddPort(int nProtocol, int nPort)
// Adds a port for the protocol.
// Either succeeds or drops out with a fatal error.
{
	if (!nPort) Fatal("Invalid/zero port number for protocol %s", ProtocolName(nProtocol));
	int i;

	// Make sure we're not going to generate a table of duplicates
	for (i=0;i<nNetworkPorts;i++) {
		if (nPort == NetworkPort[i].nPort) {
			if (nProtocol == NetworkPort[i].nProtocol)		// Duplicate so silently ignore it
				return;
			Fatal("Port %d cannot be used for both %s and %s",
					nPort, ProtocolName(NetworkPort[i].nProtocol), ProtocolName(nProtocol));
		}
	}

	if (NetworkPort) {
		RENEW(NetworkPort, NetworkPort_t, nNetworkPorts+1);
	} else {
		NetworkPort = NEW(NetworkPort_t, 1);
	}
	NetworkPort[nNetworkPorts].nPort = nPort;
	NetworkPort[nNetworkPorts].bEnabled = 1;
	NetworkPort[nNetworkPorts].nProtocol = nProtocol;
	NetworkPort[nNetworkPorts].tRetry = 0;
	NetworkPort[nNetworkPorts].nRetries = 0;
	NetworkPort[nNetworkPorts].nSock = 0;
	NetworkPort[nNetworkPorts].nCount = 0;
	nNetworkPorts++;
}

static void AddPorts(int nProtocol, const char *szPorts)
{
	if (szPorts) {
		char *tmp = strdup(szPorts);
		char *chp;

		for (chp=strtok(tmp, ",");chp;chp=strtok(NULL, ",")) {
			chp=SkipSpaces(chp);
			int nPort = atoi(chp);
			AddPort(nProtocol, nPort);
		}
		szDelete(tmp);
	}
}

int CopyFile(const char *szSrc, const char *szDest)
// Copies szSrc to szDest
// Returns		-1		Could not open szDest
//				-2		Could not open szSrc
//				 0		Ok
{
	FILE *fpDest, *fpSrc;
	char buf[10240];
	size_t got;

	fpDest=fopen(szDest, "w");					// Create a new file
	if (!fpDest) return -1;						// Failed - can't do it then
	fpSrc=fopen(szSrc, "r");					// Open up the original
	if (!fpSrc) {fclose(fpDest); return -2;}	// Someone is giving us a duff filename?
	while ((got=fread(buf, 1, sizeof(buf), fpSrc))) {
		fwrite(buf, 1, got, fpDest);			// Copy it over
	}
	fclose(fpSrc);								// Close them up
	fclose(fpDest);

	return 0;
}

static const char *QualifyInDir(const char *szDir, const char *szName)
// Qualifies the filename relative to the directory passed (or szBaseDir if NULL).
// If passed 'NULL', returns szDir (or szBaseDir);
// Result is always on the heap
{
	if (!szDir) szDir=szBaseDir;

	if (szName && *szName) {
		szName = SkipSpaces(szName);

		if (*szName == '/') {
			szName = strdup(szName);
		} else {
			szName = hprintf(NULL, "%s/%s", szDir, szName);
		}
	} else {
		szName=strdup(szDir);
	}

	return szName;
}

static const char *QualifyFile(const char *szName)
// Returns a string on the heap that is the name passed if it is an absolute filename or the
// same string relative to the base directory if it is relative.
{
	return QualifyInDir(NULL, szName);
}

static time_t DecodeTimeStamp(const char *szDt)
// Turns a YYYY-MM-DDTHH:MM:SS thing into a unix time
// As a special case, pass NULL to return the current time
{
	struct tm tm;

	if (szDt) {
		int nLen = strlen(szDt);


		memset(&tm, 0, sizeof(tm));
		tm.tm_isdst=-1;				// Trust this not to be timezoned
		if (nLen == 10) {			// YYYY-MM-DD	(assume 00:00:00)
			tm.tm_year=atoi(szDt)-1900;
			tm.tm_mon=atoi(szDt+5)-1;
			tm.tm_mday=atoi(szDt+8);
		} else if (nLen == 19) {	// YYYY-MM-DDTHH:MM:SS
			tm.tm_year=atoi(szDt)-1900;
			tm.tm_mon=atoi(szDt+5)-1;
			tm.tm_mday=atoi(szDt+8);
			tm.tm_hour=atoi(szDt+11);
			tm.tm_min=atoi(szDt+14);
			tm.tm_sec=atoi(szDt+17);
		} else {
//Log("Len = %d, dt='%s'", nLen, szDt);
			return 0;
		}
	} else {
		return time(NULL);
	}
//Log("y=%d,m=%d,d=%d,h=%d,m=%d,s=%d",tm.tm_year,tm.tm_mon,tm.tm_mday,tm.tm_hour,tm.tm_min,tm.tm_sec);

	return mktime(&tm);
}

static const char *TimeStamp(time_t t)
// Returns a YYYY-MM-DDTHH:MM:SS style timestamp for the time given.  '0' means 'now'.
{
	static char buf[20];
	struct tm *tm;

	if (!t) time(&t);
	tm=gmtime(&t);

	snprintf(buf, sizeof(buf), "%04d-%02d-%02dT%02d:%02d:%02d",
			tm->tm_year+1900, tm->tm_mon+1, tm->tm_mday,
			tm->tm_hour, tm->tm_min, tm->tm_sec);

	return buf;
}

static const char *HL7TimeStamp(time_t t)
// Returns a YYYYMMDDHHMMSS style timestamp for the time given.  '0' means 'now'.
{
	static char buf[20];
	struct tm *tm;

	if (!t) time(&t);
	tm=gmtime(&t);

	snprintf(buf, sizeof(buf), "%04d%02d%02d%02d%02d%02d",
			tm->tm_year+1900, tm->tm_mon+1, tm->tm_mday,
			tm->tm_hour, tm->tm_min, tm->tm_sec);

	return buf;
}

static const char *SqlTimeStamp(time_t t, int dst)
// Returns a YYYY-MM-DD HH:MM:SS style timestamp for the time given.  '0' means 'now'.
{
	static char buf[20];
	struct tm *tm;

	if (!t) time(&t);
	tm=dst ? localtime(&t) : gmtime(&t);

	snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d",
			tm->tm_year+1900, tm->tm_mon+1, tm->tm_mday,
			tm->tm_hour, tm->tm_min, tm->tm_sec);

	return buf;
}

static int rog_MkDir(const char *fmt, ...)
// Ensures that the given path exists
// Returns  1   The path now exists
//          0   Something went really wrong and I couldn't create it
// TODO: Needs looking at...
//		Does it infinitely recurse if it fails if for example a file exists that matches the dir name?
//		It does a 'chmod' if it fails, not if it succeeds
//		The check for 'root directory' seems flawed
{
	char path[256+1];
	char *chp;

	va_list ap;
	va_start(ap, fmt);
	vsnprintf(path, sizeof(path), fmt, ap);
	va_end(ap);

	if (*path != '/') {
		const char *szPath=QualifyFile(path);
		strncpy(path, szPath, sizeof(path));
		szDelete(szPath);
	}

	if (mkdir(path, 0777)) {            // Error
		switch (errno) {
		case EEXIST:                    // it already exists - great!
			break;
		case ENOENT:                    // An ancestor doesn't exist
			chp=strrchr(path, '/');

			if (chp) {                  // Good we are not base dir!
				*chp='\0';              // Terminate are parent dir
				if (!rog_MkDir("%s", path)) return 0;
				*chp='/';
				if (!rog_MkDir("%s", path)) return 0;
			}
			break;
		default:
			chmod(path, 0777);
			return 0;
		}
	}

	return 1;
}

static const char *SignalName(int n)
{
	static char buf[20];
	const char *names[]={"0",
		"HUP","INT","QUIT","ILL","TRAP","ABRT","EMT","FPE",
		"KILL","BUS","SEGV","SYS","PIPE","SUGALRM","TERM","USR1",
		"USR2","CHLD","PWR","WINCH","URG","POLL","STOP","TSTP"};

	if (n >= 0 && n<sizeof(names)/sizeof(*names))
		snprintf(buf, sizeof(buf), "SIG%s (%d)", names[n], n);
	else
		snprintf(buf, sizeof(buf), "UNKNOWN (%d)", n);

	return buf;
}

static char *ReadLine(FILE *fd)
{
	static char buf[256];

	if (fgets(buf, sizeof(buf), fd)) {
		char *chp=strchr(buf, '\n');
		if (chp) *chp='\0';
		return buf;
	} else {
		return NULL;
	}
}

void s3ErrorHandler(struct S3 *s, int nErr, const char *szErr, const char *szQuery)
{
	Log("Executed: %s", szQuery);
	Fatal("SQL Error %d: %s", nErr, szErr);
}

S3 *s3MainDb = NULL;

void s3_CreateTable(S3 *db, const char *table, const char *defn)
{
	int err = 5;
	int count = 0;

	while (err == 5 && count++ < 50) {
		err = s3_Dof(db, "CREATE TABLE IF NOT EXISTS %S (%S)", table, defn);
		if (err == 5) usleep(20);
	}

	if (err) {
		Fatal("Error %d (%s) creating table %s", s3_ErrorNo(db), s3_ErrorStr(db), table);
	}
}

void s3Main_CreateTable(const char *table, const char *defn)
{
	s3_CreateTable(s3MainDb, table, defn);
}

S3 *s3Main()
{
	if (!s3MainDb) {
		s3MainDb = s3_Open("spider.sl3");

		s3Main_CreateTable("dubcheck",
				" uuid CHAR(36), "
				" dt DATETIME DEFAULT CURRENT_TIMESTAMP, "
				" PRIMARY KEY (uuid) "
				);

		s3Main_CreateTable("sessions",
				" id integer, "
				" uuid CHAR(36), "
				" dt DATETIME DEFAULT CURRENT_TIMESTAMP, "
				" PRIMARY KEY (id), "
				" UNIQUE (uuid) "
				);

		s3Main_CreateTable("calls",
				" id integer, "
				" dt DATETIME DEFAULT CURRENT_TIMESTAMP, "
				" logid CHAR(20), "
				" logdir text, "
				" UNIQUE (logid) "
				);
//		fprintf(fp, " %s", PeerIp());															// IP
//		fprintf(fp, " %s", mi_id ? mi_id : "(NONE)");											// ID (dir)
//		fprintf(fp, " %0.2f", (double)(mi_tms.tms_utime+mi_tms.tms_stime)/CLOCKS_PER_SEC);		// Spider time
//		fprintf(fp, " %0.2f", (double)(mi_tms.tms_cutime+mi_tms.tms_cstime)/CLOCKS_PER_SEC);	// API time
//		fprintf(fp, " %0.3f", (double)mi_msecs/1000.0);											// Elapsed
//		fprintf(fp, " %s", (mi_session && *mi_session) ? mi_session : "(NONE)");				// Session
//		fprintf(fp, " %d", mi_status);															// Returned status
//		fprintf(fp, " %s", mi_function ? mi_function : "(NONE)");								// Function / URI

		s3Main_CreateTable("rpc",
				" id integer, "
				" name text, "
				" version text, "
				" updated DATETIME DEFAULT CURRENT_TIMESTAMP, "
				" modified DATETIME, "
				" adl text, "
				" PRIMARY KEY (id), "
				" UNIQUE (name) "
				);
	}

	return s3MainDb;
}

void s3MainDelete()
{
	s3_Destroy(&s3MainDb);
}

S3 *_s3_Usage = NULL;

const char *usage_DatabaseFile()
{
	static const char *file = NULL;

	if (!file) {
		file = hprintf(NULL, "%s/%s", szEtcDir, "usage.sl3");
	}

	return file;
}

S3 *usage_Database()
{
	if (!_s3_Usage) {
		_s3_Usage = s3_Open(usage_DatabaseFile());

		s3_CreateTable(_s3_Usage, "usage",
				" id INTEGER PRIMARY KEY, "
				" period TEXT, "
				" calls INTEGER, "
				" duration INTEGER, "
				" bytesin INTEGER, "
				" bytesout INTEGER, "
				" api TEXT, "
				" caller TEXT, "
				" UNIQUE (period,api,caller), "
				" UNIQUE (period,caller,id) "
				);
	}

	return _s3_Usage;
}

void usage_Close()
{
	s3_Destroy(&_s3_Usage);
}

void UsageDaily()
// Roll over usage stats
{
}

const char *usage_PeriodForHour(int hour)
// Returns the busy 'period' string for the given hour today.
// This is arranged so that two simultaneous calls won't over-write each other.
{
	static const char *buf[]={NULL,NULL};
	static int bufno=0;

	time_t now = time(NULL);
	struct tm *tm=localtime(&now);

	bufno=!bufno;
	szDelete(buf[bufno]);

	buf[bufno] = hprintf(NULL, "%04d-%02d-%02d %02d", tm->tm_year+1900, tm->tm_mon+1, tm->tm_mday, hour);

	return buf[bufno];
}

const char *usage_PeriodBusyStart()
// Return today's busy start time
{
	return usage_PeriodForHour(8);
}

const char *usage_PeriodBusyEnd()
// Return today's busy end time
{
	return usage_PeriodForHour(18);
}

const char *usage_PeriodNow()
{
	time_t now = time(NULL);
	struct tm *tm=localtime(&now);

	return usage_PeriodForHour(tm->tm_hour);
}

int usage_Used(const char *szOrganisationProduct, const char *szApi, int bBusy, int bDaily, int bTime)
// Returns the relevant usage against one of the qhc,qht,qdc,qdt,bhc,bht,bdc,bdt limits
// All usage is against 'today'
// bBusy is used if bDaily is set and sums the relevant part of the current day
// bDaily sums the usage across the relevant busy/quiet portiong of the day
// bTime is true to return the time (in ms) used or false to return the calls
{
	const char *field = bTime ? "duration" : "calls";			// Which field we're returning
	const char *periodClause = NULL;
	const char *resultField = NULL;
	const char *nowHour = strdup(usage_PeriodNow());

	int isBusy = strcmp(nowHour, usage_PeriodBusyStart()) >= 0 && strcmp(nowHour, usage_PeriodBusyEnd()) < 0;

	if (bDaily) {
		if (bBusy) periodClause = s3_Subst("period >= %s AND period < %s", usage_PeriodBusyStart(), usage_PeriodBusyEnd());
		else periodClause = s3_Subst("period > %s OR period >= %s", usage_PeriodBusyStart(), usage_PeriodBusyEnd());

		resultField = hprintf(NULL, "SUM(%s)", field);
	} else {
		periodClause = s3_Subst("period = %s", usage_PeriodNow());
		resultField = strdup(field);
	}

	const char *query = hprintf(NULL, "SELECT %s AS total FROM usage WHERE %s AND api='%s' AND caller = '%s'", resultField, periodClause, szApi, szOrganisationProduct);

	S3 *s3 = usage_Database();
	S3LONG(s3, total);
	s3_MustDo(s3, query);
//Log("[%d] > %s", total, query);

	szDelete(nowHour);
	szDelete(resultField);
	szDelete(periodClause);

	return total;
}

void usage_Rollover()
// Roll hourly usage over to daily etc.
{
	S3 *s3 = usage_Database();
	const char *where = hprintf(NULL, "LENGTH(period)=13 and PERIOD NOT LIKE '%.10s%%'", usage_PeriodNow());

	s3_MustDo(s3, "BEGIN TRANSACTION");							// So nothing can go wrong...

	// Ensure that we're not going to insert into records that already exist
	SSET *days = sset_New();
	const char *query = hprintf(NULL, "SELECT DISTINCT SUBSTR(period,1,10) FROM usage WHERE %s", where);
	s3it *it = s3_MustQuery(s3, query);
	szDelete(query);
	const char **row;
	for (row=s3it_Next(it);row;row=s3it_Next(it)) {
		sset_Add(days, row[0]);
	}
	s3it_Delete(it);

	// For any that do exist, rename their period to be 'DATE xx' so that they'll be collected up in the main INSERT
	const char *day;
	while (sset_GetNextEntry(days, &day)) {
		char dayTemp[20];

		snprintf(dayTemp, sizeof(dayTemp), "%s xx", day);
		s3_Dof(s3, "UPDATE usage SET period=%s WHERE period=%s", dayTemp, day);
	}

	query = hprintf(NULL, "INSERT INTO usage "
					" (period,calls,duration,bytesin,bytesout,api,caller) "
					" SELECT SUBSTR(period,1,10) date, SUM(calls),SUM(duration),SUM(bytesin),SUM(bytesout),api,caller FROM usage "
					" WHERE %s "
					" GROUP BY date,api,caller",
					where);
	s3_MustDo(s3, query);
	szDelete(query);

	query = hprintf(NULL, "DELETE FROM usage WHERE %s", where);
	s3_MustDo(s3, query);
	szDelete(query);

	s3_MustDo(s3, "COMMIT");

	szDelete(where);
}

time_t FileTime(const char *szFile)
// Returns the time of the last update to a file or 0 if the file doesn't exist.
{
// TODO: Make this compatible with lare files.  Currently just using stat64 breaks linux.  Simple to fix!
	struct stat st;
	time_t mtime=0;

	if (!stat(szFile, &st)) mtime=st.st_mtime;

	return mtime;
}

void CreateAccessDb(const char *szName)
{
	S3 *db = s3_Open(szName);

	if (db) {
		s3_CreateTable(db, "productId",
				" id CHAR(36), "
				" organisation TEXT, "
				" product TEXT, "
				" PRIMARY KEY (id), "
				" UNIQUE (organisation,product) "
				);

		s3_CreateTable(db, "productApis",
				" id CHAR(36), "
				" api TEXT, "
				" PRIMARY KEY (id,api) "
				);

		s3_CreateTable(db, "productVersion",
				" id CHAR(26), "
				" version text, "
				" PRIMARY KEY (id) "
				);
		s3_Close(db);
	}
}

int UpdateTableFromFile(S3 *db, const char *table, const char *szFile)
// Returns the number of updates done
{
	CSVX *csv = csvx_NewFile(szFile);
	int count=0;

	if (!csv) return count;							// No file...

	int values;
	while ((values=csvx_GetRecord(csv))) {
		if (values == csvx_GetNameCount(csv)) {
			const char *szFields = NULL;
			const char *szValues = NULL;
			int i;

			for (i=1;i<=values;i++) {
				const char *field = csvx_GetNameByNumber(csv, i);
				const char *value = csvx_GetDataByNumber(csv, i);

				const char *escapedValue = s3_Subst("%s",value);
				szFields = hprintf(szFields, "%s%s",i==1 ? "" : ",", field);
				szValues = hprintf(szValues, "%s%s",i==1 ? "" : ",", escapedValue);
				szDelete(escapedValue);
			}

			const char *szStatement = hprintf(NULL, "INSERT OR REPLACE INTO %s (%s) VALUES (%s)", table, szFields, szValues);
			szDelete(szFields);
			szDelete(szValues);
			count++;

			s3_Do(db, szStatement);
			szDelete(szStatement);
		} else {
			Log("Bad field count (%d) on line %d of %s", csvx_GetDataCount(csv), csvx_GetLineNumber(csv), szFile);
		}
	}

	csvx_Delete(csv);

	return count;
}

static int config_FileSetRaw(const char *szFilename, const char *szName, const char *szNewEntry)
// Sets a string in the config file or deletes it if szNewEntry is NULL
// This is done with as little 'messing up' of the original file as possible.
// Method is to copy the file, with amendment, to a temporary file then copy that back over the original.
// Returns	1	All went ok.
//			0	Failed - variable not set
{
	const char *szTmp;
	FILE *fpt;
	FILE *fp;
	const char *szLine;
	int nLen=strlen(szName);

	fp=fopen(szFilename, "r");					// Can't open the file we're meant to update so give up
	if (!fp) return 0;

	szTmp=hprintf(NULL, "/tmp/spider.tmp.%d", getpid());
	fpt=fopen(szTmp, "w");
	if (!fpt) {									// Couldn't open temp file - something is amiss...
		szDelete(szTmp);
		fclose(fp);
		return 0;
	}

	while ((szLine=ReadLine(fp))) {
		const char *szEquals, *chp;

		szEquals=strchr(szLine, '=');

		if (!*szLine || *szLine == '#' || !szEquals) {
			fputs(szLine, fpt);
			fputc('\n', fpt);
			continue;			// Blank or comment
		}
		chp=szEquals-1;
		while (chp>szLine && isspace(*chp)) chp--;
		if (chp-szLine+1 == nLen && !strnicmp(szName, szLine, chp-szLine+1)) {		// Already in the file
			if (szNewEntry) {
				fputs(szNewEntry, fpt);
				fputc('\n', fpt);
			}
			szName=NULL;
		} else {
			fputs(szLine, fpt);
			fputc('\n', fpt);
		}
	}
	if (szName && szNewEntry) {
		fputs(szNewEntry, fpt);
		fputc('\n', fpt);
	}
	fclose(fpt);
	fclose(fp);

	// Ok, we have a temp file that's been updated so copy it back

	fpt=fopen(szTmp, "r");
	szDelete(szTmp);
	if (!fpt) return 0;

	fp=fopen(szFilename, "w");
	if (!fp) {
		fclose(fpt);
		return 0;
	}

	while ((szLine=ReadLine(fpt))) {
		fputs(szLine, fp);
		fputc('\n', fp);
	}
	fclose(fpt);
	fclose(fp);
	unlink(szTmp);

	return 1;
}

static int config_SetString(const char *szName, const char *szValue, int bQuote, const char *szComment)
// Writes a string with optional comment to the config file, quoting it if bQuote is set
// A string can be deleted by setting szValue to NULL
{
	if (!szConfigFile) return 0;
	const char *szNewEntry=NULL;

	if (szValue) {
		if (bQuote) {
			const char *szEncodedValue = rcache_Encode(szValue);
			szNewEntry=hprintf(NULL, "%s=\"%s\"", szName, szEncodedValue);
			szDelete(szEncodedValue);
		} else {
			szNewEntry=hprintf(NULL, "%s=%s", szName, szValue);
		}
		if (szComment && *szComment) {
			int n = strlen(szNewEntry);
			int c = strlen(szComment);
			int indent;

			if (n + c > 76 || n > 30) indent=1;			// Long combination so just put a space before the '#'
			else if (c < 48) indent=30-n;				// Line comments up at 30 chars if possible
			else indent=76-n-c;							// Otherwise make comment end at pos. 78

			szNewEntry = hprintf(szNewEntry, "%*s# %s", indent, "", szComment);
		}
	}

	int result = config_FileSetRaw(szConfigFile, szName, szNewEntry);

	szDelete(szNewEntry);

	return result;
}

static const char *config_FileGetString(const char *szFilename, const char *szName)
// Gets a string from the given file.
// returns a string on the heap if successful or NULL if there is
// no string or no config file...
{
	FILE *fp;
	const char *szLine;										// Bit of a cheat as we'll point char* into this
	int nLen=strlen(szName);
	char *szValue = NULL;

	fp=fopen(szFilename, "r");
	if (!fp) return NULL;

	while ((szLine=ReadLine(fp))) {
		char *szEquals, *chp;

		if (!*szLine || *szLine == '#') continue;			// Blank or comment
		szEquals=strchr(szLine, '=');
		if (!szEquals) continue;							// No equals on line
		chp=szEquals-1;
		while (chp>szLine && isspace(*chp)) chp--;
		if (chp-szLine+1 == nLen && !strnicmp(szName, szLine, chp-szLine+1)) {
			szValue = szEquals+1;							// Next char after the =
			break;
		}
	}
	fclose(fp);

	if (szValue) {
		while (isspace(*szValue)) szValue++;

//Log("'%s:%s' = '%s'",szFilename,szName,szValue);
		char *szCloseQuote = NULL;
		if (*szValue == '"') {							// Quoted string
			szCloseQuote = strchr(szValue+1, '"');
		}

		if (szCloseQuote) {								// Fully quoted
			*szCloseQuote='\0';
			const char *decoded=strdup(rcache_Decode(szValue+1));
			return decoded;
		} else {										// Not quoted
			char *szHash = strchr(szValue, '#');

			if (szHash) {
				while (szHash>szValue && isspace(szHash[-1])) szHash--;
				*szHash='\0';
			}
			return strdup(szValue);
		}
	}

//Log("%s:%s not found",szFilename,szName);

	return NULL;
}

static const char *config_GetString(const char *szName)
// Gets a string from the config file.
// returns a string on the heap if successful or NULL if there is
// no string or no config file...
{
	if (!szConfigFile) return NULL;

	return config_FileGetString(szConfigFile, szName);
}

static const char *config_GetSetString(const char *szName, const char *szDef, const char *szComment)
// Gets a string from the config file.
// returns a string on the heap if successful or szDef if there is
// no string
// If there is no string found then 'szDef' is written back to the file for next time
{
	if (!szConfigFile) return NULL;

	const char *szResult = config_FileGetString(szConfigFile, szName);
	if (!szResult) {
		if (szDef) {
			szResult = strdup(szDef);
			config_SetString(szName, szDef, 1, szComment);
		}
	}

	return szResult;
}

static int config_GetSetInt(const char *szName, int nDef, const char *szComment)
// Returns an integer setting from the config file or 'nDef' if it isn't in there
// If it doesn't already exist in the file, then set it
{
	const char *szStr = config_GetString(szName);

	if (szStr) {
		nDef=atoi(szStr);
		szDelete(szStr);
	} else {
		char buf[20];

		snprintf(buf, sizeof(buf), "%d", nDef);
		config_SetString(szName, buf, 0, szComment);
	}

	return nDef;
}

static int config_GetBool(const char *szName, int nDef, const char *szComment)
// Returns 1 or 0 depending on the state of the variable in the config file
// or returns nDef if the variable isn't mentioned.
// if nDef is 0 or 1 then the file will be updated with it if it doesn't currently contain a value.
// Truth is defined as the value starting with 'Y', 'T' or non-zero
{
	const char *szStr = config_GetString(szName);
	int result = nDef;

	if (szStr) {
		int c = toupper(*szStr);

		result = (c == 'Y' || c == 'T' || atoi(szStr));

		szDelete(szStr);
	} else {
		if (nDef == 1) config_SetString(szName, "Yes", 0, szComment);
		else if (nDef == 0) config_SetString(szName, "No", 0, szComment);
	}

	return result;
}

static const char *config_GetFile(const char *szName, const char *szDef, const char *szComment)
// Gets a fully qualified filename which is 'szName' from the config file if it
// is in the config file or 'szDef' if it isn't found.
{
	const char *szPath = NULL;
	const char *szFile = config_GetSetString(szName, szDef, szComment);

	if (szFile) szPath=QualifyFile(szFile);

	szDelete(szFile);

	return szPath;
}

#if 0	// Not currently used
static const char *config_GetDir(const char *szName, const char *szDef, const char *szComment)
// Reads a directory name from the config file and returns it, making sure it exists.
// If the string is not in the config file then 'szDef' is used instead
// Returns a heap-based string
{
	const char *szPath = config_GetFile(szName, szDef, szComment);

	rog_MkDir("%s", szPath);

	return szPath;
}
#endif

int isPromiscuous()
// Returns whether or not we're in promiscuous mode (i.e. will do things for anyone!)
{
	static int state = -1;

	if (state == -1) state = config_GetBool("promiscuous",0,"Set to YES to allow us to talk to strangers");

	return state;
}

int RefreshAccessDatabase()
// NB. This function should only be called when there are no child processes.
// Initialises or updates the access database used to authenticate TLS callers.
// Checks time of source db and update files against last check (initially 1/1/1970)
// Return silently if last check is since any changes
// Otherwise, copies the initial database into the working locations
// Applies patch files
// Updates 'last check' time.
// Returns	0	Nothing happened (nothing changed)
//			1	Something (may have) changed
{
	static time_t lastCheck = 0;
	const char *szSrcDir = "/usr/mt/spider/etc/static";
	const char *szDestDir = "/usr/mt/spider/tmp";
	const char *szUpdateDir = "/usr/mt/spider/etc/updates";
	const char *database = "access";

	const char *szSrcDb = hprintf(NULL, "%s/%s.sl3", szSrcDir, database);
	const char *szDestDb = hprintf(NULL, "%s/%s.sl3", szDestDir, database);
	time_t mtime = FileTime(szSrcDb);

	if (mtime && mtime <= lastCheck)		// Drop out if original db exists and hasn't changed
		return 0;

	if (dbAccess) s3_Close(dbAccess);

	Log("mtime = %d", mtime);
	if (!mtime) {
		unlink(szDestDb);
		CreateAccessDb(szDestDb);			// Create empty access db (mainly useful for us creating from update files)
	} else {
		CopyFile(szSrcDb, szDestDb);		// Copy template access db
	}

	dbAccess = s3_OpenExisting(szDestDb);

	const char **tables = s3_Tables(dbAccess);
	if (tables) {
		const char **t;
		for (t=tables;*t;t++) {
			const char *table = *t;

			char *csvFile = hprintf(NULL, "%s/%s.%s.csv", szUpdateDir, database, table);

			if (!access(csvFile, 4)) {
				int count=UpdateTableFromFile(dbAccess, table, csvFile);
				if (count) Log("Local updates to %s.%s table = %d", database, table, count);
			} else {
				FILE *fp = fopen(csvFile, "w");
				const char **fields = s3_Columns(dbAccess, *t);

				fprintf(fp, "# %s.%s.csv created by %s at %s\n", database, table, szMyName, NiceTime(0));
				fprintf(fp, "# Add rows to make additions or changes to %s\n", table);
				fprintf(fp, "#\n");
				const char **f;
				for (f=fields;*f;f++) {
					const char *field = *f;

					fprintf(fp, "%s\"%s\"", f==fields?"":",", field);
				}
				fprintf(fp, "\n");
				fclose(fp);
				Log("Created empty update file %s", csvFile);
			}
		}
	}

	lastCheck = time(NULL);

	return 1;
}

void SetOrganisationProduct(const char *szOrganisation, const char *szProduct)
{
	szDelete(g_Organisation);
	szDelete(g_Product);
	szDelete(g_OrganisationProduct);

	if (szOrganisation && szProduct) {
		g_Organisation = strdup(szOrganisation);
		g_Product = strdup(szProduct);
		g_OrganisationProduct = hprintf(NULL, "%s:%s", szOrganisation, szProduct);

	}
}

contract_t *contract_Find(const char *szOrganisationProduct)
{
	contract_t *c = NULL;

	if (contracts)
		c = (contract_t*)simap_GetValue(contracts, szOrganisationProduct, 0);

	return c;
}

const char *cbac_GetId(const char *szOrganisationProduct)
{
	contract_t *c = contract_Find(szOrganisationProduct);

	return c ? c->id : NULL;
}

int cbac_AllowedVersion(const char *szOrganisationProduct, const char *szVersion)
{
	if (isPromiscuous()) return 1;

	contract_t *c = contract_Find(szOrganisationProduct);

	return c && (!c->versions || sset_IsMember(c->versions, szVersion));
}

const char *cbac_CanRunApi(const char *szOrganisationProduct, const char *szApi, int bCheckLimits)
// Checks if an API can be run at this time by this caller
// Returns NULL if ok
// Returns an error message otherwise
{
	static const char *result = NULL;
	szDelete(result);

Log("cpac_CanRunApi(%s, %s)", szOrganisationProduct, szApi);
	if (isPromiscuous()) return NULL;

	int allowed = 0;

	contract_t *c = contract_Find(szOrganisationProduct);

	if (c) {
		limits_t *limit;
		if (!strcasecmp(szApi, "any")) {
			limit = c->limits;
		} else {
			limit = spmap_GetValue(c->apis, szApi);

			if (!limit) {
				char *szApiCopy = strdup(szApi);

				while (!limit && *szApiCopy != '*') {
					char *dot = strrchr(szApiCopy, '.');
					char *lastElement = dot ? dot+1 : szApiCopy;
					if (*lastElement) {							// There must be a character (szApi isn't blank or ending with .)
						strcpy(lastElement, "*");
						limit = spmap_GetValue(c->apis, szApiCopy);	// Look for wildcard version
						if (dot) *dot='\0';
					} else {
						break;									// Api component was blank
					}
				}
			}
		}

		if (limit && bCheckLimits) {
//Log("%s[%s] limits = %d (%d,%d,%d,%d) (%d,%d,%d,%d)", szOrganisationProduct, szApi, limit->limited, limit->qhc,limit->qht,limit->qdc,limit->qdt, limit->bhc,limit->bht,limit->bdc,limit->bdt);
			if (limit->limited) {
				const char *nowHour = strdup(usage_PeriodNow());
				const char *bad = NULL;

				int isBusy = strcmp(nowHour, usage_PeriodBusyStart()) >= 0 && strcmp(nowHour, usage_PeriodBusyEnd()) < 0;

				if (!isBusy) {				// We're in the quiet period
					if (!bad && limit->qhc > -1) {			// Limited by quiet hourly calls
						int calls = usage_Used(szOrganisationProduct, szApi, 0, 0, 0);
						if (calls >= limit->qhc) bad = hprintf(NULL, "Quiet hourly call limit of %d exceeded", limit->qhc);
					}
					if (!bad && limit->qht > -1) {			// Limited by quiet hourly time
						int ms = usage_Used(szOrganisationProduct, szApi, 0, 0, 1);
						if (ms >= limit->qht) bad = hprintf(NULL, "Quiet hourly time limit of %dms exceeded", limit->qht);
					}
					if (!bad && limit->qdc > -1) {			// Limited by quiet daily calls
						int calls = usage_Used(szOrganisationProduct, szApi, 0, 1, 0);
						if (calls >= limit->qdc) bad = hprintf(NULL, "Quiet daily call limit of %d exceeded", limit->qdc);
					}
					if (!bad && limit->qdt > -1) {			// Limited by quiet daily time
						int ms = usage_Used(szOrganisationProduct, szApi, 0, 1, 1);
						if (ms >= limit->qdt) bad = hprintf(NULL, "Quiet daily time limit of %dms exceeded", limit->qdt);
					}
				} else {
					if (!bad && limit->bhc > -1) {			// Limited by busy hourly calls
						int calls = usage_Used(szOrganisationProduct, szApi, 1, 0, 0);
						if (calls >= limit->bhc) bad = hprintf(NULL, "Busy hourly call limit of %d exceeded", limit->bhc);
					}
					if (!bad && limit->bht > -1) {			// Limited by busy hourly time
						int ms = usage_Used(szOrganisationProduct, szApi, 1, 0, 1);
						if (ms >= limit->bht) bad = hprintf(NULL, "Busy hourly time limit of %dms exceeded", limit->bht);
					}
					if (!bad && limit->bdc > -1) {			// Limited by busy daily calls
						int calls = usage_Used(szOrganisationProduct, szApi, 1, 1, 0);
						if (calls >= limit->bdc) bad = hprintf(NULL, "Busy daily call limit of %d exceeded", limit->bdc);
					}
					if (!bad && limit->bdt > -1) {			// Limited by busy daily time
						int ms = usage_Used(szOrganisationProduct, szApi, 1, 1, 1);
						if (ms >= limit->bdt) bad = hprintf(NULL, "Busy daily time limit of %dms exceeded", limit->bdt);
					}
				}
				if (bad) {
					Log("%s calling %s: %s", szOrganisationProduct, szApi, bad);
					result = hprintf(bad, " for %s API", szApi);		// Note this message works OK when szApi = "any"
				} else {
					Log("%s is within limits for %s", szOrganisationProduct, szApi);
				}
			} else {						// Is mentioned and hasn't got any limits attached
				Log("%s allowed for %s without limit", szApi, szOrganisationProduct);
			}
		} else {							// Api not mentioned in the list so not allowed
			Log("API '%s' not enabled for %s", szApi, szOrganisationProduct);
			result = hprintf(NULL, "%s is not in %s's contract", szApi, szOrganisationProduct);
		}
	} else {
		Log("There is no contract for %s", szOrganisationProduct);
	}

	return result;
}

static void DeleteArgv(const char **argv)
// Deletes a vector of strings that looks like 'argv', in which every element is a pointer to a string
// on the heap until we hit a NULL pointer, which indicates the end of the vector.
{
	if (argv) {
		const char **original = argv;		// So we can free the actual vector when we've done

		while (*argv) {
			free((char*)(*argv));
			argv++;
		}

		free(original);
	}
}

const char **GetDirEntries(const char **list, const char *prefix, const char *szDir, const char *szExt)
{
	int nlist=0;					// Entries in list not including trailing NULL
	const char **l = list;			// Temporary roamer

	while (*l) {					// Count list entries
		l++;
		nlist++;
	}

	DIR *dir=opendir(szDir);
	if (dir) {
		struct dirent *d;

		while ((d=readdir(dir))) {
			const char *szFilename = hprintf(NULL, "%s/%s", szDir, d->d_name);
			const char *szPrefix = hprintf(NULL, "%s%s%s", prefix, *prefix?"/":"", d->d_name);
			if (*d->d_name == '.') continue;

			if (IsDir(szFilename)) {
				list=GetDirEntries(list, szPrefix, szFilename, szExt);
				l=list;				// Need to re-count the list
				nlist=0;
				while (*l) { l++; nlist++; }
				szDelete(szPrefix);
			} else {
				if (!szExt || (strlen(szPrefix) >= strlen(szExt) && !strcmp(szPrefix+strlen(szPrefix)-strlen(szExt), szExt))) {
					RENEW(list, const char*, nlist+2);
					list[nlist]=szPrefix;
					list[nlist+1]=NULL;
					nlist++;
				}
			}
			szDelete(szFilename);
		}
		closedir(dir);
	}

	return list;
}

limits_t *limits_Delete(limits_t *al)
{
	if (al) {
		free((char*)al);
	}
}

limits_t *limits_New()
{
	limits_t *al = NEW(limits_t, 1);

	al->limited=0;
	al->qhc=al->qht=al->qdc=al->qdt=al->bhc=al->bht=al->bdc=al->bdt=-1;

	return al;
}

void limits_Dump(limits_t *limit)
{
	if (limit) {
		if (limit->limited) {
			const char *line = hprintf(NULL, "Limited: ");
			if (limit->qhc > -1) line=hprintf(line, " qhc=%d", limit->qhc);
			if (limit->qht > -1) line=hprintf(line, " qht=%d", limit->qht);
			if (limit->qdc > -1) line=hprintf(line, " qdc=%d", limit->qdc);
			if (limit->qdt > -1) line=hprintf(line, " qdt=%d", limit->qdt);
			if (limit->bhc > -1) line=hprintf(line, " bhc=%d", limit->bhc);
			if (limit->bht > -1) line=hprintf(line, " bht=%d", limit->bht);
			if (limit->bdc > -1) line=hprintf(line, " bdc=%d", limit->bdc);
			if (limit->bdt > -1) line=hprintf(line, " bdt=%d", limit->bdt);
			Log("%s", line);
			szDelete(line);
		} else {
			Log("Not limited");
		}
	} else {
		Log("No limit");
	}
}

const char *limits_Parse(limits_t *limits, const char *text)
// parses the text passed into the limits_t structure, setting any previously unset limits
// Complains if they're already set
// Returns NULL	all ok
//			"error message" if there's a problem
{
	const char *szError = NULL;

	if (!text) szError = strdup("No limit text?");

	if (!szError) {
		char *parse = strdup(text);
		char *limit;

		for (limit=strtok(parse, ",");limit;limit=strtok(NULL, ",")) {
			limit=SkipSpaces(limit);
			char *eq = strchr(limit, '=');
			if (!eq) {
				szError = strdup("No = in limit");
			} else {
				*eq='\0';
				rtrim(limit);
				eq=SkipSpaces(eq+1);
				int value=atoi(eq);
					   if (!strcasecmp(limit, "qhc"))	{ if (limits->qhc > -1) szError=strdup("qhc set twice"); limits->qhc=value;
				} else if (!strcasecmp(limit, "qht"))	{ if (limits->qht > -1) szError=strdup("qht set twice"); limits->qht=value;
				} else if (!strcasecmp(limit, "qdc"))	{ if (limits->qdc > -1) szError=strdup("qdc set twice"); limits->qdc=value;
				} else if (!strcasecmp(limit, "qdt"))	{ if (limits->qdt > -1) szError=strdup("qdt set twice"); limits->qdt=value;
				} else if (!strcasecmp(limit, "bhc"))	{ if (limits->bhc > -1) szError=strdup("bhc set twice"); limits->bhc=value;
				} else if (!strcasecmp(limit, "bht"))	{ if (limits->bht > -1) szError=strdup("bht set twice"); limits->bht=value;
				} else if (!strcasecmp(limit, "bdc"))	{ if (limits->bdc > -1) szError=strdup("bdc set twice"); limits->bdc=value;
				} else if (!strcasecmp(limit, "bdt"))	{ if (limits->bdt > -1) szError=strdup("bdt set twice"); limits->bdt=value;
				} else {
					szError = hprintf(NULL, "limit name '%s' not recognised", limit);
				}
			}
			if (szError) break;
			limits->limited=1;
		}

		szDelete(parse);
	}

	return szError;
}

void contract_Delete(contract_t *c)
{
	if (c) {
		szDelete(c->id);
		szDelete(c->szFilename);
		szDelete(c->szOrganisationProduct);
		limits_Delete(c->limits);
		sset_Delete(c->versions);

		limits_t *al;
		spmap_Reset(c->apis);
		while (spmap_GetNextEntry(c->apis, NULL, (void*)&al)) {
			limits_Delete(al);
		}
		spmap_Delete(c->apis);
		szDelete(c->szValidFrom);
		szDelete(c->szValidTo);

		free((char*)c);
	}
}

contract_t *contract_New()
{
	contract_t *c = NEW(contract_t, 1);

	c->id=NULL;
	c->szFilename=NULL;
	c->szOrganisationProduct=NULL;
	c->limits=NULL;
	c->apis=spmap_New();
	c->versions=NULL;
	c->szValidFrom=NULL;
	c->szValidTo=NULL;

	return c;
}

void contract_Dump(const char *name, contract_t *c)
{
	if (c) {
		Log("Got something for %s", name);
		Log("  That's (%s) %s in file %s (valid %s - %s)",
			c->id, c->szOrganisationProduct, c->szFilename, c->szValidFrom, c->szValidTo);
		if (c->versions) {
			sset_Reset(c->versions);
			const char *version;
			while (sset_GetNextEntry(c->versions, &version)) {
				Log("  Version %s", version);
			}
		} else {
			Log("  All versions");
		}

		if (c->limits) limits_Dump(c->limits);

		const char *api;
		if (0) while (spmap_GetNextEntry(c->apis, &api, NULL)) {
			Log("    API = '%s'", api);
		}
	} else {
		Log("NULL contract pointer");
	}
}

void contract_DumpAll()
{
	if (contracts) {
		const char *name = NULL;
		contract_t *c;
		simap_Reset(contracts);
		while (simap_GetNextEntry(contracts, &name, (int*)&c)) {
			contract_Dump(name, c);
		}
	} else {
		Log("No contracts loaded");
	}
}

void contract_Remove(const char *szOrganisationProduct)
// Removes a contract from the list
{
	contract_t *c = contract_Find(szOrganisationProduct);

	if (c) {
		contract_Delete(c);
		simap_DeleteKey(contracts, szOrganisationProduct);
	}
}

void contract_RemoveAll()
{
	if (contracts) {
		simap_Reset(contracts);

		contract_t *c;
		while (simap_GetNextEntry(contracts, NULL, (int*)&c)) {
			contract_Delete(c);
		}
		simap_Delete(contracts);
		contracts=NULL;
	}
}

// Used for warnings
#define CONTRACT_INCLUDE	5					// Maximum include level
const char *_contract_Filename[CONTRACT_INCLUDE];
int _contract_Lineno[CONTRACT_INCLUDE];
FILE *_contract_fp[CONTRACT_INCLUDE];
int _contract_stackpos = 0;
int _contract_Errors = 0;

const char **contract_Files()
// Returns an argv-style list of filenames on the heap
// Always returns something even if no files
{
	const char **files=NEW(const char*,1);
	files[0]=NULL;

	files = GetDirEntries(files, "", szContractDir, ".contract");

	return files;
}

int contract_Warn(const char *fmt, ...)
{
	char buf[500];

	va_list ap;
	va_start(ap, fmt);
	vsnprintf(buf, sizeof(buf), fmt, ap);
	va_end(ap);

	Log("Contract error %s in %s, line %d", buf, _contract_Filename[_contract_stackpos], _contract_Lineno[_contract_stackpos]);

	_contract_Errors++;
}

char *contract_Line()
// Return the next line from the current contract, handling include files
{
	char *line=ReadLine(_contract_fp[_contract_stackpos]);
	_contract_Lineno[_contract_stackpos]++;
	if (line) {
		if (!strncasecmp(line, "include", 7)) {
			const char *includeFile = QualifyInDir(szContractDir, SkipSpaces(line+7));

			FILE *fp=fopen(includeFile, "r");
			if (!fp) {
				contract_Warn("Could not open contract include file %s", includeFile);
				szDelete(includeFile);
				return contract_Line();
			}

			if (_contract_stackpos >= CONTRACT_INCLUDE -1) {
				contract_Warn("Too many include files when including %s", includeFile);
			} else {
				_contract_stackpos++;
				_contract_Lineno[_contract_stackpos]=0;
				_contract_fp[_contract_stackpos]=fp;
			}
			szDelete(includeFile);
			return contract_Line();
		}
		return line;
	} else {
		fclose(_contract_fp[_contract_stackpos]);
		if (!_contract_stackpos) return NULL;
		_contract_stackpos--;
		return contract_Line();
	}
}

contract_t *contract_Read(const char *szFilename)
// Read in a contract for an organisation/product and link it into the main set
{
	FILE *fp = fopen(szFilename, "r");
	if (!fp) Fatal("Cannot open contract file %s", szFilename);

	_contract_stackpos=0;
	_contract_fp[0]=fp;
	_contract_Filename[0] = szFilename;
	_contract_Lineno[0] = 0;

	_contract_Errors = 0;

	if (!fp) {
		contract_Warn("Cannot open file");
		return NULL;
	}

	const char *organisation = NULL;
	const char *product = NULL;

	contract_t *c = contract_New();

	const char *section=NULL;
	char *line;
	while (line=contract_Line()) {

		if (*line == '#' || !*line) continue;

		if (!isspace(*line)) {
			char *rest = strchr(line, ':');
			if (rest) {
				*rest='\0';
				rest++;
			}

			szDelete(section);
			section=strdup(line);

			if (!rest) continue;					// Line didn't end with a ':'
			line=rest;								// The bit after the colon
		}

		line=SkipSpaces(line);

//Log("Line (%s): '%s'", section, line);
		if (!section) contract_Warn("Missing section header on contract %s", szFilename);

		if (!strcasecmp(section, "id")) {
			if (c->id) contract_Warn("Multiple IDs are not allowed");
			c->id=strdup(line);
		} else if (!strcasecmp(section, "organisation")) {
			if (organisation) contract_Warn("Multiple organisations are not allowed");
			organisation=strdup(line);
		} else if (!strcasecmp(section, "product")) {
			if (product) contract_Warn("Multiple products are not allowed");
			product=strdup(line);
		} else if (!strcasecmp(section, "valid from")) {
			if (c->szValidFrom) contract_Warn("Multiple 'valid from' not allowed");
			c->szValidFrom=strdup(line);
		} else if (!strcasecmp(section, "valid to")) {
			if (c->szValidTo) contract_Warn("Multiple 'valid to' not allowed");
			if (strlen(line) == 10) {
				c->szValidTo = hprintf(NULL, "%s 23:59:59", line);		// If we say '2014-08-10' we mean '2014-08-11 23:59:59'
			} else {
				c->szValidTo = strdup(line);
			}
		} else if (!strcasecmp(section, "version") || !strcasecmp(section, "versions")) {
			if (!c->versions) c->versions = sset_New();
			sset_Add(c->versions, line);
		} else if (!strcasecmp(section, "limit") || !strcasecmp(section, "limits")) {
			if (!c->limits) c->limits = limits_New();
			const char *err = limits_Parse(c->limits, line);
			if (err) contract_Warn("%s", err);
		} else if (!strcasecmp(section, "apis")) {
			limits_t *al = limits_New();

			char *limits=strchr(line, ' ');
			if (limits) {
				*limits = '\0';
				limits = SkipSpaces(limits+1);
				if (*limits != '(') {
					contract_Warn("Unexpected character '%c'", *limits);
				} else {
					limits=SkipSpaces(limits+1);
					char *ket = strchr(limits, ')');
					if (!ket) {
						contract_Warn("No closing bracket after API limits");
					} else {
						limits_Parse(al, limits);
					}
				}
			}
//if (al->limited) Log("%s:%s[%s] limits = (%d,%d,%d,%d) (%d,%d,%d,%d)", organisation, product, line, al->qhc,al->qht,al->qdc,al->qdt, al->bhc,al->bht,al->bdc,al->bdt);
			if (spmap_GetValue(c->apis, line)) {
				contract_Warn("API %s listed more than once", line);
			}
			spmap_Add(c->apis, line, al);
		} else contract_Warn("Unrecognised section '%s'", section);
	}
	fclose(fp);

	if (!c->id) contract_Warn("No ID");
	if (!c->versions) Log("No version in contract %s - all assumed", szFilename);
	if (!organisation) contract_Warn("No organisation");
	if (!product) contract_Warn("No product");

	c->szFilename=strdup(szFilename);
	c->szOrganisationProduct = hprintf(NULL, "%s:%s", organisation, product);

	if (_contract_Errors) {
		Log("Total of %d error%s in %s - contract ignored", _contract_Errors, _contract_Errors==1?"":"s", szFilename);
		contract_Delete(c);
		return NULL;
	}

	if (!contracts) contracts=simap_New();
	contract_t *existing = (contract_t*)simap_GetValue(contracts, c->szOrganisationProduct, 0);
	if (existing) {
		if (strcmp(szFilename, existing->szFilename)) {			// It occurs in two files...
			Log("*** Contract %s in %s is replacing that already defined in %s", c->szOrganisationProduct, szFilename, existing->szFilename);
		} else {												// It's being updated
			Log("Contract %s is being re-read from %s", c->szOrganisationProduct, szFilename);
		}
		contract_Remove(c->szOrganisationProduct);				// Remove the existing one
	}

	struct stat st;
	stat(szFilename, &st);										// If this returns an error, there is a serious problem!
	c->mtime=st.st_mtime;
	simap_Add(contracts, c->szOrganisationProduct, (int)c);		// This will replace the existing if it's there

	return c;
}

time_t _contract_DirTime = 0;				// Date time of /usr/mt/spider/etc/contracts

void contract_ReadAll(int bForce)
{
	struct stat st;
	char bUpdated = 0;

	if (stat(szContractDir, &st)) {
		Log("No contract directory - %s", szContractDir);
		return;
	}

	if (!(st.st_mode & S_IFDIR)) {
		Log("Contract directory (%s) is not a directory!", szContractDir);
		return;
	}

	if (st.st_mtime != _contract_DirTime) {
		bForce=1;	// Contract directory changed to re-read everything anyway
		Log("Contract directory touched - re-reading all contracts");
	}

	if (bForce) {									// Drop all contracts and re-read
		const char **files = contract_Files();
		const char **file=files;

		contract_RemoveAll();
		while (*file) {
			contract_t *c;

			const char *szFilename = hprintf(NULL, "%s/%s", szContractDir, *file);
			c = contract_Read(szFilename);
			szDelete(szFilename);
			file++;
		}
		DeleteArgv(files);
		bUpdated=1;
	} else {														// Nothing added/deleted so just check all up to date
		if (contracts) {
			contract_t *c;
			const char *szOrganisationProduct;
			const char *szFilename;
			simap_Reset(contracts);
			SSET *reread = sset_New();
			SSET *delete = sset_New();

			while (simap_GetNextEntry(contracts, &szOrganisationProduct, (int *)&c)) {
				struct stat st;

				if (!stat(c->szFilename, &st)) {									// Time differs so must be updated

					if (st.st_mtime != c->mtime) {
						sset_Add(reread, c->szFilename);
						bUpdated=1;
					}
				} else {															// Couldn't stat - it's gone
					sset_Add(delete, szOrganisationProduct);
					bUpdated=1;
				}
			}

			while (sset_GetNextEntry(reread, &szFilename)) {
				Log("Contract %s changed - re-reading", szFilename);
				contract_Read(szFilename);
			}
			sset_Delete(reread);

			while (sset_GetNextEntry(delete, &szOrganisationProduct)) {
				Log("Contract file for %s has gone - forgetting it", szOrganisationProduct);
				simap_DeleteKey(contracts, szOrganisationProduct);
			}
			sset_Delete(delete);
		}
	}

	_contract_DirTime = st.st_mtime;

	if (bUpdated) {				// Things have changed so make it plain what we now cater for
		const char *szOrganisationProduct;

		if (contracts) {
			simap_Reset(contracts);
			while (simap_GetNextEntry(contracts, &szOrganisationProduct, NULL)) {
				Log("Have contract covering %s", szOrganisationProduct);
			}
		} else {
			Log("No contracts found in %s", szContractDir);
		}
	}
}

static int cmp_str(const void *a, const void *b)
{
	return strcmp(*(char**)a, *(char**)b);
}

int IsDir(const char *szDir)
{
	struct stat st;
	int bResult=0;

	if (!stat(szDir, &st)) {
		if (st.st_mode & S_IFDIR) {
			bResult=1;
		}
	}

	return bResult;
}

const char *adl_Version(rogxml *rxAdl)
// Gets the version from the ADL passed or returns NULL if that proves unfruitful
// Result is on the heap.
{
	const char *result = NULL;

	rogxml *rxVersion = rogxml_FindChild(rxAdl, "Version");

	if (rxVersion) result = rogxml_GetValue(rxVersion);
	if (result) result=strdup(result);						// NB. Move to the heap
	if (!result) {
		rogxml *rxBuild = rogxml_FindByPath(rxAdl, "/*/build");
		if (rxBuild) {
//			const char *szNo = rogxml_GetAttr(rxBuild, "no");
			const char *szDate = rogxml_GetAttr(rxBuild, "date");		// Only using these two at present
			const char *szTime = rogxml_GetAttr(rxBuild, "time");
//			const char *szTag = rogxml_GetAttr(rxBuild, "tag");
//			const char *szBranch = rogxml_GetAttr(rxBuild, "branch");
			const char *szVersion = rogxml_GetAttr(rxBuild, "version");

			if (szVersion && *szVersion) {
				result = strdup(szVersion);
			} else if (szDate && szTime && strlen(szDate) == 10 && strlen(szTime) == 8) {
				result=hprintf(NULL, "0.00%2d%.2s%.2s%.2s%.2s%.2s",
					atoi(szDate+2)+50, szDate+5, szDate+8,				// Year is 64 for 2014
					szTime, szTime+3, szTime+6);
			}
		}
	}

	return result;
}

rogxml *binary_Adl(const char *binary)
// Returns the ADL from a binary
// If there isn't any, returns NULL
{
	rogxml *rxAdl = NULL;

	if (GrepFile(NULL, binary, "[ADL_SUPPORTED]")) {
		const char *cmd = hprintf(NULL, "%s -ADL", binary);

		FILE *fp = popen(cmd, "r");
// NB. If this bit mysteriously fails it's because the shell is incorrectly outputting some thing to stdout
// before running the binary.  If this is a problem in real-life then an alternative to popen() can be created
// but really the shell shouldn't do this and it's probably down to a badly written shell startup script
		if (fp) {
			rxAdl = rogxml_ReadFp(fp);
			if (!rxAdl) rxAdl=rogxml_NewError(102, "Invalid XML returned from function");
			fclose(fp);
		} else {
			rxAdl = rogxml_NewError(101, "Could not open binary");
		}
		szDelete(cmd);
	} else {
		rxAdl = rogxml_NewError(100, "ADL not supported");
	}

	return rxAdl;
}

const char *binary_Version(const char *szBinary)
{
	struct stat st;
	static const char *result = NULL;

	szDelete(result);
	result=NULL;

	int err = stat(szBinary, &st);
	if (err) {
		result=strdup("0.00");
	} else {
		rogxml *rxAdl = binary_Adl(szBinary);
		if (rxAdl) {
			result = adl_Version(rxAdl);
			rogxml_Delete(rxAdl);
		}
	}

	if (!result) {												// Real thing not there so use mod. date of binary 
		struct tm *tm=gmtime(&st.st_mtime);

		result = hprintf(NULL, "0.00%02d%02d%02d%02d%02d%02d",
			tm->tm_year%100, tm->tm_mon+1, tm->tm_mday,						// Year is 14 for 2014
			tm->tm_hour, tm->tm_min, tm->tm_sec);
	}


	return result;
}

const char *api_Binary(const char *szApi)
// Finds the binary corresponding to an API name
// Returns effectively a static string
{
	static const char *szBinary = NULL;

	szDelete(szBinary);
	szBinary=strdup(szRpcDir);
	const char *chp=szApi;

	for (;;) {
		const char *next;
		struct stat st;

		if (!*chp || *chp=='.') return NULL;						// Blank component in API name
		next = strchr(chp, '.');
		if (!next) next=chp+strlen(chp);							// Last component
		szBinary=hprintf(szBinary, "/%.*s", next-chp, chp);			// Add it to our path
		int err = stat(szBinary, &st);

		if (err && errno == ENOENT)
			return NULL;											// Component not found

		if (st.st_mode & S_IFREG) break;							// We're there, worry about executability later

		if (!(st.st_mode & S_IFDIR)) return NULL;					// Component leads to non-dir, non-file

		if (!*next) return NULL;									// Incomplete - final component is a dir

		chp=next+1;												// Move on to next component
	}

	return szBinary;
}

int api_Refresh(const char *szApi, const char *szBinary, const char **pVersion, rogxml **pAdl)
// Updates information held on the API and returns the ADL
// Returns	0	Yes, all went ok
//			1	We don't recognise the api
{
	struct stat st;

	if (!szBinary) szBinary = api_Binary(szApi);

	int err = stat(szBinary, &st);
	if (err) return 1;

	time_t mtime = st.st_mtime;
	const char *szStamp = strdup(SqlTimeStamp(mtime, 0));

	S3 *s = s3Main();
	s3it *q = s3_Queryf(s, "SELECT modified,version,adl FROM rpc WHERE name = %s", szApi);
	err = s3_ErrorNo(s);
	if (err) {
		const char *errstr = s3_ErrorStr(s);
		Log("SQL Error %d: %s", err, errstr);
		Log("QRY: %s", s3_LastQuery(s));
	}

	if (q) {
		const char **row = s3it_Next(q);
		int inDate = 0;

		if (row) {
			if (!strcmp(row[0], szStamp)) {
				inDate = 1;
				if (pVersion) *pVersion = strdup(row[1]);
				if (pAdl) *pAdl = rogxml_FromText(row[2]);
			}
		}

		if (!inDate) {
			rogxml *rxAdl = binary_Adl(szBinary);
			const char *szAdl = rogxml_ToText(rxAdl);
			const char *version = adl_Version(rxAdl);

			const char *szNow = SqlTimeStamp(0, 0);
			if (!version) version = strdup(binary_Version(szBinary));

			s3_Dof(s, "REPLACE INTO rpc (name,updated,modified,version,adl) VALUES (%s,%s,%s,%s,%s)",
					szApi, szNow, szStamp, version, szAdl);

			int err = s3_ErrorNo(s);
			if (err) {
				const char *errstr = s3_ErrorStr(s);
				Log("SQL Error %d: %s", err, errstr);
				Log("QRY: %s", s3_LastQuery(s));
			}
			szDelete(szAdl);

			if (pVersion) *pVersion = version; else szDelete(version);
			if (pAdl) *pAdl = rxAdl; else rogxml_Delete(rxAdl);
		}
	}
	szDelete(szStamp);
	s3it_Delete(q);

	return 0;
}

rogxml *api_Adl(const char *api)
{
	rogxml *rxAdl = NULL;
	api_Refresh(api, NULL, NULL, &rxAdl);

	return rxAdl;
}

const char *api_Version(const char *api)
{
	const char *szVersion = NULL;

	api_Refresh(api, NULL, &szVersion, NULL);

	return szVersion;
}

int isRecognisedSpiderFunction(const char *szApi)
// Returns 1 if this is a function that is handled internally
{
	if (!szApi || strlen(szApi) < 8) return 0;

	return !strncasecmp(szApi, "spider.", 7);
}

const char *BinaryFromApi(const char *szApi)
// Finds the binary corresponding to an API name
// For internal functions, this will be "spider"
// For APis that don't exist, it'll return NULL
// Returns effectively a static string
{
	static const char *szBinary = NULL;

	if (isRecognisedSpiderFunction(szApi)) {
		return "spider";
	}

	szDelete(szBinary);
	szBinary=strdup(szRpcDir);
	const char *chp=szApi;

	for (;;) {
		const char *next;
		struct stat st;

		if (!*chp || *chp=='.') return NULL;						// Blank component in API name
		next = strchr(chp, '.');
		if (!next) next=chp+strlen(chp);							// Last component
		szBinary=hprintf(szBinary, "/%.*s", next-chp, chp);			// Add it to our path
		int err = stat(szBinary, &st);

		if (err && errno == ENOENT)
			return NULL;											// Component not found

		if (st.st_mode & S_IFREG) break;							// We're there, worry about executability later

		if (!(st.st_mode & S_IFDIR)) return NULL;					// Component leads to non-dir, non-file

		if (!*next) return NULL;									// Incomplete - final component is a dir

		chp=next+1;													// Move on to next component
	}

	return szBinary;
}

const char **GetAllApis()
{
	const char **list=NEW(const char*,1);
	list[0]=NULL;

	list = GetDirEntries(list, "", szRpcDir, NULL);

	const char **l = list;			// Temporary roamer
	int nlist = 0;

	while (*l) {					// Count list entries
		l++;
		nlist++;
	}

	qsort(list, nlist, sizeof(*list), cmp_str);

	return list;
}

const char *NewSessionId(const char *szRpcSession)
// Creates an obfuscated session from the RPC session ID and returns it
// The result should be treated as a static string (and may be simply szRpcSession)
{
	static const char *szResult = NULL;
	szDelete(szResult);

	if (bHandlingSessions) {
		S3 *s = s3Main();

		szResult = guid_ToText(NULL);
		int err = s3_Dof(s, "INSERT INTO sessions (id,uuid) VALUES (%s,%s)", szRpcSession, szResult);
		if (!err) {
			Log("SESSION NEW %s -> %s", szRpcSession, szResult);
		} else {
			const char *szErr = s3_ErrorStr(s);

			Log("Error %d - %s", err, szErr);
		}
		return szResult;
	}

	return szRpcSession;
}

const char *ApiSessionFromRpc(const char *szRpcSession)
// Given a numeric session id from an RPC, returns the GUID form for the API
{
	static const char *szResult = NULL;
	szDelete(szResult);

	if (bHandlingSessions) {
		S3 *s = s3Main();

		s3it *q = s3_Queryf(s, "SELECT uuid FROM sessions WHERE id='%s'", szRpcSession);
		if (q) {
			const char **row = s3it_Next(q);
			if (row) {
				szResult = strdup(row[0]);
				Log("SESSION %s -> %s", szRpcSession, szResult);
			}
			s3it_Destroy(&q);
		}
		return szResult;
	}

	return szRpcSession;
}

const char *RpcSessionFromApi(const char *szApiSession)
// Given a UUID session id, returns the corresponding numeric one
// Returns NULL if it isn't known
{
	static const char *szResult = NULL;
	szDelete(szResult);

	if (bHandlingSessions) {
		S3 *s = s3Main();

		s3it *q = s3_Queryf(s, "SELECT id FROM sessions WHERE uuid='%s'", szApiSession);
		if (q) {
			const char **row = s3it_Next(q);
			if (row) {
				szResult = strdup(row[0]);
				Log("SESSION %s -> %s", szApiSession, szResult);
			}
			s3it_Destroy(&q);
		}
		return szResult;
	}

	return szApiSession;
}

// The packing functions work as follows:
// Call pack_InitPack() before you start introducing data to pack
// Call pack_Pack() to add data to be packed
// Call pack_GetPacked() when you've finished adding data and want the returned result
// The calls for unpacking are identical except for the 'Un' bit!
// Only the pack_Unpack() call returns an error code.

HBUF *hPackBuf = NULL;
HBUF *hUnpackBuf = NULL;
z_stream packstrm;
z_stream unpackstrm;

static void pack_InitPack()
{
//Log("pack_InitPack()");
	hPackBuf = hbuf_New();
	packstrm.zalloc=Z_NULL;
	packstrm.zfree=Z_NULL;
	packstrm.opaque=Z_NULL;
	deflateInit(&packstrm, Z_BEST_COMPRESSION);				// << Level?
}

static int pack_Pack(const char *data, int nLen)
// NB. If this is called with nLen==-1, it indicates the end of the data.
// The two blocks within here are virtually the same but I read the documentation on a hard day
// It might be worth comressing them back to shorter code again some day, but beware the different
// return conditions on 'deflate()'
{
	char buf[1024];

//Log("pack_Pack(%x, %d)", data, nLen);
	if (nLen == -1) {				// We're finishing off the compression
		packstrm.avail_in = 0;
		packstrm.next_in = (unsigned char *)data;

		do {
			int have;
			int ret;

			packstrm.avail_out = sizeof(buf);
			packstrm.next_out = (unsigned char *)buf;

			ret=deflate(&packstrm, Z_FINISH);

			have = sizeof(buf)-packstrm.avail_out;		// We have to make more room and continue (should never happen!)
//Log("End Buf at %x: Adding %d", hPackBuf, have);
			hbuf_AddBuffer(hPackBuf, have, buf);
			if (ret == Z_STREAM_END) break;				// We're done
		} while (packstrm.avail_out == 0);
	} else {
		packstrm.avail_in = nLen;
		packstrm.next_in = (unsigned char *)data;

		do {
			int have;

			packstrm.avail_out = sizeof(buf);
			packstrm.next_out = (unsigned char *)buf;

			deflate(&packstrm, Z_NO_FLUSH);
			have = sizeof(buf)-packstrm.avail_out;
			hbuf_AddBuffer(hPackBuf, have, buf);
		} while (packstrm.avail_out == 0);
	}

	return 0;
}

static const char *pack_GetPacked(int *pnLen)
{
	const char *pResult;
//Log("pack_GetPacked(%x)", pnLen);

	pack_Pack("", -1);
	deflateEnd(&packstrm);

	if (pnLen) *pnLen = hbuf_GetLength(hPackBuf);
	pResult = hbuf_ReleaseBuffer(hPackBuf);

	hbuf_Delete(hPackBuf);
	hPackBuf=NULL;

	return pResult;
}

static void pack_InitUnpack()
{
	hUnpackBuf = hbuf_New();
	unpackstrm.zalloc = Z_NULL;
	unpackstrm.zfree = Z_NULL;
	unpackstrm.opaque = Z_NULL;
	unpackstrm.avail_in = 0;
	unpackstrm.next_in = Z_NULL;
	inflateInit(&unpackstrm);
}

static int pack_Unpack(const char *data, int nLen)
// Adds some compressed data into the buffer
// Returns	0	Added and not at the end
//			1	Added and we're at the end of the data
//			2	Need a dictionary so cannot process
//			3	Some other error in processing the compressed stream
{
	char buf[1024];
	int ret;

	unpackstrm.avail_in = nLen;
	unpackstrm.next_in = (unsigned char*)data;

	do {
		int have;

		unpackstrm.avail_out = sizeof(buf);
		unpackstrm.next_out = (unsigned char *)buf;
		ret = inflate(&unpackstrm, Z_NO_FLUSH);
		if (ret == Z_NEED_DICT) return 2;
		if (ret != Z_OK) return 3;
		have = sizeof(buf)-unpackstrm.avail_out;
		hbuf_AddBuffer(hUnpackBuf, have, buf);
	} while (unpackstrm.avail_out == 0);

	return ret == Z_STREAM_END ? 1 : 0;
}

static const char *pack_GetUnpacked(int *pnLen)
{
	const char *pResult;

	inflateEnd(&unpackstrm);

	hbuf_AddChar(hUnpackBuf, '\0');						// Ensure it's terminated in case it's a string
	if (pnLen) *pnLen = hbuf_GetLength(hUnpackBuf)-1;		// Adjust for the '\0'
	pResult = hbuf_ReleaseBuffer(hUnpackBuf);

	hbuf_Delete(hUnpackBuf);
	hUnpackBuf = NULL;

	return pResult;
}

int PeerPort(void) { return _nSenderPort; }

static const char *PeerIp()
{
	static char buf[20];

	if (PeerPort()) {
		snprintf(buf, sizeof(buf), "%s", _szSenderIp);
		return buf;
	} else {
		return "-";
	}
}

#if PID_MAX < 238328
static const char *pidstr()
// Converts the process id into a 3 character string
// A bit of maths 10^(LOG(65536)/3) shows we only need base 41 to represent 64K as three chars so 62 is ample
// and in fact will work up to 238,327 (17.8 bits).
{
	static char buf[4] = "";

	if (!*buf) {				// Haven't passed this way before
		unsigned long nPid=(unsigned long)getpid();
		static const char chars[]="0123456789"
								 "abcdefghijklmnopqrstuvwxyz"
								 "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
		#define PIDSTRBASE 	(sizeof(chars)-1)

		int c = nPid % PIDSTRBASE;
		int b = (nPid / PIDSTRBASE) % PIDSTRBASE;
		int a = (nPid / PIDSTRBASE / PIDSTRBASE) % PIDSTRBASE;

		buf[0]=chars[a];
		buf[1]=chars[b];
		buf[2]=chars[c];
		buf[3]='\0';
	}

	return buf;
}
#else
#error "Change pidstr() if PID_MAX can be more than about 17 bits"
#endif

static const char *rogxml_ToNiceText(rogxml *rx)
// Returns the string representation of 'rx', ensuring indentation etc. is nice
{
	rogxml_SetIndentString("  ");
	rogxml_SetLinefeedString("\n");

	return rogxml_ToText(rx);
}

static char _bNoteInhibit = 0;

// msc = MMTS SPIDER COMMON
// The idea is to move these to a common library at some point

int msc_isDir(const char *szDir)
{
	struct stat st;
	int bResult=0;

	if (!stat(szDir, &st)) {
		if (st.st_mode & S_IFDIR) {
			bResult=1;
		}
	}

	return bResult;
}

int msc_DirCount(const char *szDir)
// Returns the number of files/directories in the given directory excluding anything starting with '.'
{
	int count=0;

	DIR *dir=opendir(szDir);
	struct dirent *d;

	if (dir) {
		while ((d=readdir(dir))) {
			if (*d->d_name == '.') continue;		// Skip '.' files
			count++;
		}
		closedir(dir);
	}

	return count;
}

const char *th(int day)
{
	if (day > 15) day %= 10;
	if (day > 3) day = 0;
	return "th\0st\0nd\0rd"+(day*3);
}

int msc_MonthLen(int month, int year)
{
	static int lens[]={0,31,28,31,30,31,30,31,31,30,31,30,31};

	month = (month+11)%12+1;
	return (month == 2 && !(year & 3)) ? 29 : lens[month];
}

void msc_SubtractDays(int period, int *day, int *month, int *year)
// Subtracts the days from the day, month and year pointed to
{
	while (period>0) {
//Log("Period = %d, date = %d/%d/%d", period, *day, *month, *year);
		if (period < *day) {
			*day-=period;
			break;
		}

		period-=*day;
		(*month)--;
		if (*month < 1) {
			*month=12;
			(*year)--;
		}
		*day=msc_MonthLen(*month, *year);
	}
}

const char *msc_MessageId()
// Returns a message ID that is always 11 characters long YYMMDDxxxxx
// This is used as a note directory /usr/mt/mmts/msglog/YY/MM/DD/xxxxx
{
	static char id[20];
	static int index = 0;
	static int initialised = 0;		// When 0, we look in the directory and get the next number

	if (bIsDaemon) {				// Daemon so we're generating a new ID
		time_t now=time(NULL);
		struct tm *tm = gmtime(&now);
		sprintf(id, "%02d%02d%02d-----", tm->tm_year % 100, tm->tm_mon+1, tm->tm_mday);

		const char *szDir = hprintf(NULL, "%s/%.2s/%.2s/%.2s", szMsgLogDir, id, id+2, id+4);
		if (!msc_isDir(szDir)) {										// No dir, must be a new day
			index=0;
		} else {
			if (!initialised) {										// Need to set initial index
				index=0;											// We'll use this unless there's a directory
				DIR *dir=opendir(szDir);

				if (dir) {											// Today's dir already exists - find last entry
					int bFound = 0;
					char szLast[6];
					struct dirent *d;

					strcpy(szLast, "00000");						// Always lower than 'aaaaa'
					while ((d=readdir(dir))) {
						const char *szName = d->d_name;
						if (strlen(szName) == 5) {					// Only interested in 5 char entries
							if (strcmp(szName, szLast) > 0) {
								strcpy(szLast, szName);
								bFound=1;
							}
						}
					}
					closedir(dir);
					szDelete(szDir);

					if (bFound) {									// Found some existing entries
						const char *chp=szLast;
						char c;

						while ((c=*chp++)) {
							index=index*26+(c-'a');
						}
						index++;									// We need to be one beyond them
					}
				}
				initialised=1;										// So we don't suffer this section again
			}
		}

		int tmp=index;
		char *chp=id+strlen(id)-1;		// Point at the final '-'
		while (*chp == '-') {
			*chp='a'+tmp%26;
			tmp/=26;
			chp--;
		}

		index++;												// Index for next time
	}

	return id;
}

static const char *szGlobalNoteDir = NULL;
static const char *szGlobalNoteDirTail = NULL;

const char *msc_NoteDir()
{
	static char bInHere = 0;				// Flag to stop re-entrancy problems

	if (!bInHere) {
		bInHere=1;

		if (!szGlobalNoteDir && !_bNoteInhibit) {
			const char *mid = msc_MessageId();				// Always an 11 char string - YYMMDDxxxxx
			szGlobalNoteDir=hprintf(NULL, "%s/%.2s/%.2s/%.2s/%s", szMsgLogDir, mid, mid+2, mid+4, mid+6);
			szGlobalNoteDirTail=hprintf(NULL, "%.2s/%.2s/%.2s/%s", mid, mid+2, mid+4, mid+6);
			rog_MkDir(szGlobalNoteDir);
			Log("MAIN: Message log directory is %s", szGlobalNoteDir);
		}

		bInHere=0;
	}

	return szGlobalNoteDir;
}

static const char *msc_NoteDirTail()
// Returns the bit like 14/08/01/aaabc
{
	if (!szGlobalNoteDirTail) msc_NoteDir();

	return szGlobalNoteDirTail;
}

static const char *NoteDirSuffix()
{
	static char buf[30];
	static char bBeenHere=0;

	if (!bBeenHere) {
		snprintf(buf, sizeof(buf), "%s-%s", HL7TimeStamp(0), pidstr());
		mi_id=strdup(buf);
		bBeenHere=1;
	}

	return buf;
}

static void NoteInhibit(int bOpt)
// Call with non-0 to stop (and delete) notes
{
	if (bOpt == -1) {							// We're setting from the config file
		bOpt = config_GetBool("debug", -1, "Set to YES/NO to force/inhibit logging messages in msglog");
		if (bOpt == -1) return;					// No default in config so we'll let it ride...
		bOpt = !bOpt;							// Debug means we don't want to log...
	}

	_bNoteInhibit = bOpt;
	if (bOpt && szGlobalNoteDir) {				// Need to delete any that have been created already
		const char *szCommand = hprintf(NULL, "rm -rf %s", szGlobalNoteDir);
		system(szCommand);
		szDelete(szCommand);
		szGlobalNoteDir=NULL;
	}
}

static int ssmap_PutFile(SSMAP *map, const char *szFilename)
{
	FILE *fp;

	fp=fopen(szFilename, "w");
	if (map && fp) {
		const char *szName;
		const char *szValue;

		ssmap_Sort(map, strcmp);
		ssmap_Reset(map);

		while (ssmap_GetNextEntry(map, &szName, &szValue)) {
			fprintf(fp, "%s=%s\n", szName, szValue);
		}
		fclose(fp);
	}

	return !!fp;
}

static SSMAP *ssmap_GetFile(const char *szFilename)
// Reads a map from a file.  Note that the map may be empty if the file doesn't exist.
{
	FILE *fp;
	SSMAP *sm=ssmap_New();

	fp=fopen(szFilename, "r");
	if (fp) {
		for (;;) {
			const char *szLine = ReadLine(fp);
			const char *szValue;

			if (!szLine) break;

			szLine=SkipSpaces(szLine);
			if (*szLine == '#' || *szLine == '\0') continue;		// Skips blanks and hashed out lines
			szValue=strchr(szLine, '=');							// Look for the '=' sign
			if (szValue) {
				const char *chp=szValue-1;							// Last char of the key part
				const char *szKey;

				szValue=SkipSpaces(szValue+1);						// Skip spaces in the value
				while (chp >= szLine && isspace(*chp))				// Work back to the end of the key
					chp--;
				szKey=strnappend(NULL, szLine, chp-szLine+1);		// Take a copy of the key only
				ssmap_Add(sm, szKey, szValue);						// Add into the map
				szDelete(szKey);									// Drop the key copy
			}
		}
		fclose(fp);
	}

	return sm;
}

SSMAP *_info_map = NULL;
SSMAP *_file_map = NULL;

static void InfoStr(const char *szName, const char *szValue)
{
	if (!_info_map) {
		_info_map = ssmap_New();
		time_t now=time(NULL);							// Add the datetime entry
		struct tm *tm = gmtime(&now);
		struct timeval tp;
		int csecs;
		char buf[100];

		gettimeofday(&tp, NULL);
		csecs=tp.tv_usec / 10000;

		snprintf(buf, sizeof(buf), "%.3s %02d-%02d-%04d %02d:%02d:%02d.%02d",
			"SunMonTueWedThuFriSat"+tm->tm_wday*3,
			tm->tm_mday, tm->tm_mon+1, tm->tm_year + 1900,
			tm->tm_hour, tm->tm_min, tm->tm_sec,
			csecs);
		ssmap_Add(_info_map, "TIME-START", buf);
	}

	ssmap_Add(_info_map, szName, szValue ? szValue : "");
}

static void InfoInt(const char *szName, int nValue)
{
	char buf[20];

	snprintf(buf, sizeof(buf), "%d", nValue);
	InfoStr(szName, buf);
}

static void FileDescription(const char *szFilename, const char *szDescription)
{
	if (!_file_map) _file_map = ssmap_New();

	const char *chp = strrchr(szFilename, '/');
	if (chp) szFilename = chp+1;
	ssmap_Add(_file_map, szFilename, szDescription);
}

static void WriteMaps()
{
	if (_info_map) {
		const char *szFilename = hprintf(NULL, "%s/info.map", msc_NoteDir());
		ssmap_PutFile(_info_map, szFilename);
		szDelete(szFilename);
	}

	if (_file_map) {
		const char *szFilename = hprintf(NULL, "%s/file.map", msc_NoteDir());
		ssmap_PutFile(_file_map, szFilename);
		szDelete(szFilename);
	}
}

static void Note(const char *szFmt, ...)
{
	static int bNotedDate = 0;

	if (!_bNoteInhibit) {
		va_list ap;
		char buf[256];
		FILE *fp;
		const char *szFilename;

		if (bIsDaemon) return;									// Master daemon process doesn't make notes

		szFilename = hprintf(NULL, "%s/info", msc_NoteDir());

		va_start(ap, szFmt);
		vsnprintf(buf, sizeof(buf), szFmt, ap);
		va_end(ap);

		fp=fopen(szFilename, "a");
		if (fp) {
			if (!bNotedDate) {

				time_t now=time(NULL);							// Add the datetime entry
				struct tm *tm = gmtime(&now);
				struct timeval tp;
				int csecs;

				gettimeofday(&tp, NULL);
				csecs=tp.tv_usec / 10000;

				fprintf(fp, "D|%02d-%02d-%04d %02d:%02d:%02d.%02d %.3s\n",
					tm->tm_mday, tm->tm_mon+1, tm->tm_year + 1900,
					tm->tm_hour, tm->tm_min, tm->tm_sec,
					csecs,
					"SunMonTueWedThuFriSat"+tm->tm_wday*3);
				bNotedDate = 1;
			}
			fprintf(fp, "%s\n", buf);
			fclose(fp);
		}

		szDelete(szFilename);
	}
}

static void NoteMessage(const char *szMessage, int nLen, const char *szTag, const char *szExt, const char *szDescr)
{
	if (!_bNoteInhibit) {
		FILE *fp;

		if (bIsDaemon) return;

		const char *szFile = hprintf(NULL, "%s.%s", szTag, szExt);
		const char *szFilename=hprintf(NULL, "%s/%s", msc_NoteDir(), szFile);

		if (nLen == -1) nLen=strlen(szMessage);
		fp=fopen(szFilename, "w");
		if (fp) {
			fwrite(szMessage, nLen, 1, fp);
			fclose(fp);
		}

		FileDescription(szFilename, szDescr);
		Note("M|%s.%s|%s|%d", szTag, szExt, szDescr, nLen);

		szDelete(szFile);
		szDelete(szFilename);
	}
}

static void NoteRpcError()
// I could have used NoteMessage() to do this then I was going to add a header to the file but then changed
// my mind as everything should already be in the note directory.
// NoteRpcMessage(g_szRpcError, -1, "stderr", "txt", "Stdout returned from RPC handler");
{
	if (!_bNoteInhibit && g_szRpcError) {
		const char *szFilename;
		FILE *fp;

		szFilename=hprintf(NULL, "%s/stderr.txt", msc_NoteDir());
		fp=fopen(szFilename, "w");
		if (fp) {
			int len = strlen(g_szRpcError);
			fwrite(g_szRpcError, len, 1, fp);
			fclose(fp);

			FileDescription("stderr.txt", "API Debug (stderr)");
			Note("M|stderr.txt|Error returned from RPC handler|%d", len);
			Note("e|%d byte%s", len, len==1?"":"s");
		}
		szDelete(szFilename);
		char *chp=strchr(g_szRpcError, '\n');
		if (chp) *chp='\0';
		InfoStr("API-ERROR",g_szRpcError);
		Note("E|%s", g_szRpcError);
		if (chp) *chp='\n';
	}
}

static void NoteMessageXML(rogxml *rx, const char *szTag, const char *szDescr, char cSize)
// Logs the message away
// Also makes a textual note of the size if cSize is non-zero (this is currently just 'R' or nothing)
{
//Log("NoteMessageXML(%x, \"%s\", \"%s\")", rx, szTag, szDescr);
	if (!_bNoteInhibit) {
		int nErr;
		const char *szXML;

		if (!rx) {									// No XML in the message
			szXML=hprintf(NULL, "<XML>There is no XML in the message</XML>");
		} else if ((nErr=rogxml_ErrorNo(rx))) {		// XML there but erroneous somehow
			szXML=hprintf(NULL, "<XML-ERROR err=\"%d\">%s</XML-ERROR>", nErr, rogxml_ErrorText(rx));
		} else {
			szXML=rogxml_ToNiceText(rx);
		}
		NoteMessage(szXML, -1, szTag, "xml", szDescr);
		if (cSize) {
			Note("%c|%d bytes", cSize, strlen(szXML));
		}
		szDelete(szXML);
	}
}

static void NoteLog(const char *szText)
{
	const char *szFilename;
	const char *szDir;
	FILE *fp;

	if (bIsDaemon) return;

	szDir = msc_NoteDir();
	if (!szDir) return;

	szFilename = hprintf(NULL, "%s/log", szDir);
	fp=fopen(szFilename, "a");
	if (fp) {
		static long start_sec = 0;
		static long start_usec = 0;
		struct timeval tv;

		gettimeofday(&tv, NULL);
		if (!start_sec) {
			start_sec=tv.tv_sec;
			start_usec=tv.tv_usec;
		}
		tv.tv_sec-=start_sec;
		tv.tv_usec-=start_usec;									// Might make usec negative, but that's ok

		long csecs = tv.tv_sec * 100 + tv.tv_usec / 10000;		// This works even if usec went negative

		fprintf(fp, "%02ld:%02ld.%02ld ", csecs / 6000, (csecs / 100) % 60, csecs % 100);
		fprintf(fp, "%s\n", szText);
		fclose(fp);
	}

	szDelete(szFilename);
}

void usage_Update(int duration, int bytesin, int bytesout, const char *period, const char *api, const char *caller)
{
	int err;

	S3 *s3 = usage_Database();

	err = s3_Dof(s3, "INSERT INTO usage (calls,duration,bytesin,bytesout, period,api,caller) VALUES (1,%d,%d,%d,%s,%s,%s)",
				duration, bytesin, bytesout, period,api,caller);
	if (err == 19) {
		s3_MustDof(s3, "UPDATE usage SET calls=calls+1, "
				" duration=duration+%d, "
				" bytesin=bytesin+%d, "
				" bytesout=bytesout+%d "
				" WHERE period=%s AND api=%s AND caller=%s",
				duration, bytesin, bytesout, period,api,caller);
	} else if (err) {
		s3ErrorHandler(s3, err, s3_ErrorStr(s3), s3_LastQuery(s3));
	}
//Log("%s", s3_LastQuery(s3));

	return;
}

static void LogMi()
// Writes the management information to a file
{
//Log("LogMi() - inhibit=%d, daemon=%d, logdir='%s'", mi_inhibit, bIsDaemon, szLogDir);
	if (mi_inhibit) return;
	if (bIsDaemon) return;

	time_t now=time(NULL);
	struct tm *tm = gmtime(&now);

	if (!mi_caller) mi_caller=strdup("(UNKNOWN)");
	if (!mi_function) mi_function=strdup("(NONE)");

	FILE *fp = fopen(szMiFile, "a");
	if (fp) {
		fprintf(fp, "%02d-%02d-%02d %02d:%02d:%02d",
				tm->tm_mday, tm->tm_mon+1, tm->tm_year % 100,
				tm->tm_hour, tm->tm_min, tm->tm_sec);

		fprintf(fp, " %s", PeerIp());															// IP
		fprintf(fp, " %s", mi_protocol ? mi_protocol : "(UNKNOWN)");							// Connection protocol
		fprintf(fp, " %s", mi_id ? mi_id : "(NONE)");											// ID (dir)
		fprintf(fp, " %0.2f", (double)(mi_tms.tms_utime+mi_tms.tms_stime)/CLOCKS_PER_SEC);		// Spider time
		fprintf(fp, " %0.2f", (double)(mi_tms.tms_cutime+mi_tms.tms_cstime)/CLOCKS_PER_SEC);	// API time
		fprintf(fp, " %0.3f", (double)mi_msecs/1000.0);											// Elapsed
		fprintf(fp, " %s", (mi_session && *mi_session) ? mi_session : "(NONE)");				// Session
		fprintf(fp, " %s", mi_caller);															// Caller ID
		fprintf(fp, " %d", mi_status);															// Returned status
		fprintf(fp, " %s", mi_function);														// Function / URI
		fprintf(fp, "\n");
		fclose(fp);
	} else {
		Log("Could not write MI to %s", szMiFile);
	}

	InfoStr("MI-SESSION",mi_session);
	InfoStr("MI-CALLER-ID",mi_caller);
	InfoInt("MI-STATUS",mi_status);

	const char *period = usage_PeriodNow();

	usage_Update(mi_msecs, mi_BytesIn,mi_BytesOut, period, mi_function, g_OrganisationProduct);
	usage_Update(mi_msecs, mi_BytesIn,mi_BytesOut, period, "any", g_OrganisationProduct);
	usage_Update(mi_msecs, mi_BytesIn,mi_BytesOut, period, mi_function, "any");

	usage_Close();
}

static void file_Rollover(const char *szFilename, unsigned int limit)
// Checks the size of the log file and retires it if it's getting too big
// NB. This is only called by the daemon and might exit if the log file can't be rolled over
{
	struct stat st;

	if (!szFilename || !limit) return;

	if (!stat(szFilename, &st) && st.st_size > limit) {		// Log file exists and is big
		int suffix=1;
		char *rollover=malloc(strlen(szFilename)+4+1);

		while (suffix <= 999) {
			sprintf(rollover, "%s.%03d", szFilename, suffix);
			if (access(rollover, 0))
				break;
			suffix++;
		}

		if (suffix > 999) {
			Fatal("I cannot roll over the file.  %s.001 to %s.999 exist???", szFilename, szFilename);
		}
		Log("Rolling %s over to %s", szFilename, rollover);
		if (RogRename(szFilename, rollover)) {
			Fatal("I can't roll over the file to %s", rollover);
		}
		if (!strcmp(szLogFile, szFilename)) {
			Log("Spider " VERSION " for " OS " (Made " __TIME__ " on " __DATE__ ", using %s)", SSLeay_version(SSLEAY_VERSION));
			Log("Previous log file archived as %s", rollover);
		}
		szDelete(rollover);
	}
}

static void Log(const char *szFmt, ...)
// Sends a log entry to both main and message logs
// If 'szFmt' starts "MAIN: " then only sends to the main log
// If 'szFmt' starts "MESSAGE: " then only sends to the message log
{
	if (!szLogDir) return;								// Too early to log anything

	va_list ap;
	char buf[1000];
	char bForMessage = 1;
	char bForMain = 1;
	const char *szMessage = szFmt;

	if (!strnicmp(szMessage, "MESSAGE: ", 9)) {			// Only for message log
		szMessage += 9;
		bForMain = 0;
	}

	if (!strnicmp(szMessage, "MAIN: ", 6)) {			// Only for main log
		szMessage += 6;
		bForMessage = 0;
	}

	if (bForMain && szLogDir) {							// Wants to be logged into main log
		FILE *fp=fopen(szLogFile, "a");
//{FILE *fp=fopen("/tmp/fred","w");fprintf(fp, "Log file is '%s'\n", szLogFile);fclose(fp);}
		if (fp) {
			time_t now=time(NULL);
			struct tm *tm = gmtime(&now);
			struct timeval tp;
			int msecs;

			gettimeofday(&tp, NULL);
			msecs=tp.tv_usec / 1000;

			fprintf(fp, "%02d-%02d-%02d %02d:%02d:%02d.%03d ",
					tm->tm_mday, tm->tm_mon+1, tm->tm_year % 100,
					tm->tm_hour, tm->tm_min, tm->tm_sec,
					msecs);
			fprintf(fp, "%-15s ", PeerIp());
			fprintf(fp, "%5d ", getpid());
			if (!bIsDaemon) fprintf(fp, " ");
			va_start(ap, szFmt);
			vfprintf(fp, szMessage, ap);
			va_end(ap);
			fprintf(fp, "\n");

			fclose(fp);
		}
	}

	if (bForMessage) {												// Wants to go into message log
		va_start(ap, szFmt);
		vsnprintf(buf, sizeof(buf), szMessage, ap);
		va_end(ap);
		NoteLog(buf);
	}

}

static int ProcessAlive(int nPid)
// Returns 1 if the process is still alive, otherwise 0
{
	int nErr=kill(nPid, 0);
	return (!nErr || errno == EPERM);		// No error or EPERM so the process exists
}

static char *roginet_ntoa(struct in_addr in)
{
	static char buf[16];
	unsigned char a = in.s_addr & 0xff;
	unsigned char b = (in.s_addr >> 8) & 0xff;
	unsigned char c = (in.s_addr >> 16) & 0xff;
	unsigned char d = (in.s_addr >> 24) & 0xff;

	snprintf(buf, sizeof(buf), "%u.%u.%u.%u", a, b, c, d);

	return buf;
}

typedef struct childinfo_t {
	int			pid;			// Process ID of the child
	time_t		tStarted;		// When it started
	const char *szIp;			// IP that triggered the child
	const char *szId;			// Message ID the child is dealing with
} childinfo_t;

static int nPids=0;
static childinfo_t *aPid = NULL;

static void Idle();

static void ReallyExit(int nCode)
// Does only safe things before leaving
// Only atomic things that cannot possibly call us back should be in here.
{
	s3MainDelete();
	usage_Close();

	exit(nCode);
}

static void Exit(int nCode)
	// Quits the daemon nice and tidily
{
	LogMi();
	WriteMaps();

	fflush(stdout);
	fflush(stderr);

	if (bIsDaemon) {
		if (szPidFile) unlink(szPidFile);       // Stops 'unexpected termination' message from status call

		if (nPids) {
			int i;

			Log("Daemon exiting: %d child%s to tidy up", nPids, nPids==1?"":"ren");
			for (i=0;i<nPids;i++) {					// Ask all the children to commit suicide
				Log("Sending kill %s to %d", SignalName(3), aPid[i]);
				kill(aPid[i].pid, 3);				// Send kill gently signal
			}
			sleep(2);								// Give them chance to cross into Hades
			Idle();									// Note their deaths...

			for (i=0;i<nPids;i++) {					// Slaughter any that are still alive
				Log("Sending kill %s to %d", SignalName(9), aPid[i]);
				kill(aPid[i].pid, 9);				// Send kill signal
			}
			sleep(1);								// Give them chance to cross into Hades
			Idle();									// Note their deaths...
		} else {
			Log("Daemon exiting: No active children");
		}
	}

	ReallyExit(nCode);
}

static const char *UriDecode(char *str)
// URI decodes the string in situ - always returns 'str'
{
	if (strchr(str, '+') || strchr(str, '%')) {	// Only do anything if we need to
		const char *in=str;
		char *out=str;
		char c;

		while ((c=*in)) {
			if (c == '+') {
				c=' ';
			} else if (c == '%') {
				int a=in[1];					// a will be 0 if '%' was last char in string
				int b=a?in[2]:0;				// b will be 0 if '%' was second last char in string or last char

				if (b) {
					a=(a>='0' && a<='9') ? a-'0' : toupper(a)-'A'+10;		// Dirty but quick hex to binary
					b=(b>='0' && b<='9') ? b-'0' : toupper(b)-'A'+10;
					c=a*16+b;
					in+=2;
				}
			}
			in++;
			*out++=c;
		}
		*out='\0';
	}

	return str;
}

int _nError=0;
const char *_szError = NULL;

static int SetError(int nErr, const char *szFmt, ...)
{
	_nError = nErr;
	szDelete(_szError);

	if (szFmt) {
		va_list ap;
		char buf[256];

		va_start(ap, szFmt);
		vsnprintf(buf, sizeof(buf), szFmt, ap);
		va_end(ap);

		_szError = strdup(buf);
	} else {
		_szError = NULL;
	}

	return nErr;
}

int GetErrorNo()			{ return _nError; }
const char *GetErrorStr()	{ return _szError; }

static int child_Find(int pid)
// Returns the index of the child in question or -1 if not found
{
	int i;

	for (i=0;i<nPids;i++) {
		if (aPid[i].pid == pid) {
			return i;
		}
	}

	return -1;
}

static int child_FindAdd(int pid)
// Finds the index of the child, adding a new one if not found
// I.e. always returns a slot
{
	int i = child_Find(pid);

	if (i >= 0) return i;

	RENEW(aPid, childinfo_t, nPids+1);
	aPid[nPids].pid=pid;
	aPid[nPids].szIp=NULL;
	aPid[nPids].szId=NULL;

	return nPids++;
}

static int child_Add(int pid, const char *szIp, const char *szId)
// Adds to the list of child processes so we can report on them and tidy them up when we die
// If a directly incoming message is being handled then szIp and szId will be set
// Otherwise we're dealing with a retry so szIp will be blank
{
	int i = child_FindAdd(pid);

	aPid[i].tStarted=time(NULL);
	aPid[i].szIp=szIp ? strdup(szIp) : NULL;
	aPid[i].szId=szId ? strdup(szId) : NULL;

	return nPids;
}

static int child_Forget(int pid)
// Take the child out of the array but don't bother to deallocate it
// Returns the number of children now active
{
	int i = child_Find(pid);

	if (i >= 0) {
		if (i<nPids-1) {
			childinfo_t *ci=aPid+i;
			szDelete(ci->szIp);
			szDelete(ci->szId);

			memmove(aPid+i, aPid+i+1, (nPids-i-1)*sizeof(childinfo_t));
		}
		nPids--;
	}

	return nPids;
}

static int allow_n = 0;
static char **allow_ip = NULL;
static char **allow_descr = NULL;

static void allow_Init()
{
	const char *szFilename = hprintf(NULL, "%s/%s", GetEtcDir(), "allow");
	FILE *fp = fopen(szFilename, "r");

	if (fp) {
		const char *szLine;

		while ((szLine = ReadLine(fp))) {
			char *szIp, *szDescr;

			while (isspace(*szLine)) szLine++;
			if (!*szLine || *szLine == '#') continue;

			szIp = strtok((char*)szLine, ":");
			szDescr = strtok(NULL, ":");
			if (!szDescr) szDescr="";
			if (allow_n++) {
				RENEW(allow_ip, char*, allow_n);
				RENEW(allow_descr, char*, allow_n);
			} else {
				allow_ip=NEW(char*, 1);
				allow_descr=NEW(char*, 1);
			}
			allow_ip[allow_n-1]=strdup(szIp);
			allow_descr[allow_n-1]=strdup(szDescr);
		}
		fclose(fp);
	}

	szDelete(szFilename);
}

static const char *allow_Allowed(const char *szIp)
// Checks if a specific IP address is allowed to access SPIDER in a 'local' manner...
// Over the application port in any manner
// Over the HTTPS port to examine status
{
	int i;

	if (!allow_n) return szIp;							// Nothing allowed -> everything allowed

	for (i=0;i<allow_n;i++) {
		if (!strcmp(szIp, allow_ip[i]))
			return allow_descr[i];
	}

	return NULL;
}

static int MyBIO_flush(BIO *io)
{
	FILE *fp=fopen("/tmp/out.dat","a");
	if (fp) {
		fputs("\n----------------------------------------------------------- FLUSH\n", fp);
		fclose(fp);
	}

	return BIO_flush(io);
}

static int MyBIO_write(BIO *io, const char *szData, int nLen)
{
	int nWritten= BIO_write(io, szData, nLen);		// Entire message
	mi_BytesOut += nWritten;

	// Everything from here on is logging...

	if (!_bNoteInhibit) {
		static BIO *lastio = NULL;
		const char *szNoteDir = msc_NoteDir();
		FILE *fp=fopen("/tmp/out.dat","a");
		FILE *fpnote=NULL;
		static int nIndex = 1;
		const char *szNoteFile = NULL;
		const char *szBase = NULL;

		if (szNoteDir) {						// We're keeping notes
			if (lastio != io) nIndex++;
			szBase=hprintf(NULL, "rawout-%d", nIndex);
			szNoteFile = hprintf(NULL, "%s/%s.txt", szNoteDir, szBase);
			fpnote=fopen(szNoteFile, "a");
			szDelete(szNoteFile);
		}

		if (lastio != io) {							// We're sending to someone new
			if (fpnote) {
				const char *szFilename = hprintf(NULL, "%s.txt", szBase);
				FileDescription(szFilename, "Data sent over wire");
				szDelete(szFilename);

				Note("M|%s.%s|%s|%d", szBase, "txt", "Data sent over wire", -1);
			}
			if (fp) fputs("\n----------------------------------------------------------- NEW PORT\n", fp);
			lastio=io;
		}
		if (fp && nLen != nWritten) {
			fputs("\n", fp);
			fputs("===========================================\n", fp);
			fprintf(fp, "ERROR - Attempted to write %d, actually wrote %d in next section\n", nLen, nWritten);
			fputs("===========================================\n", fp);
		}
		if (fp) {
			fwrite(szData,nLen,1,fp);
			fclose(fp);
		}
		if (szNoteDir) {
			szDelete(szBase);
			if (fpnote) {
				fwrite(szData,nLen,1,fpnote);
				fclose(fpnote);
			}
		}
	}

	return nWritten;
}

static int MyBIO_puts(BIO *io, const char *szText)
{
//	return BIO_puts(io, szText);
	return MyBIO_write(io, szText, strlen(szText));
}

static void FatalSSL(const char *szFmt, ...)
// SSL errors so we can return more information
{
	va_list ap;
	char buf[1000];

	va_start(ap, szFmt);
	vsnprintf(buf, sizeof(buf), szFmt, ap);
	va_end(ap);

	Log("Exit: SSL Error: %s", buf);

	BIO_printf(bio_err, "%s\n", buf);
	ERR_print_errors(bio_err);

	Exit(98);
}

static int GetRecordedDaemonPid()
// Returns 0 if it isn't recorded - will return non-0 if the .pid file exists even if the process is dead
{
	FILE *fp=fopen(szPidFile, "r");
	char buf[30]="0";					// Used both for existing process's pid and our own
	int nPid = 0;

	if (fp) {
		fgets(buf, sizeof(buf), fp);
		fclose(fp);

		nPid=atoi(buf);
	} else {
//		Log("I can't open '%s' - errno = %d", szPidFile, errno);
	}

	return nPid;
}

static int GetDaemonPid()
// Returns	0		There is no daemon running
//			1...	The PID of the daemon
{
	int nPid=GetRecordedDaemonPid();

	if (nPid && ProcessAlive(nPid)) return nPid;

	// There is now no existing instance as there is no .pid file or we didn't see it on the kill

	return 0;
}

static void CheckWeAreDaemon()
// Checks that we are the daemon - if not then quits...
{
	int nPid=GetDaemonPid();

	if (nPid == getpid()) return;			// That's what we're looking for...

	if (nPid) {								// There's a daemon, but it's not us...
		Log("Exit: Panic: Pid file says my process ID is %d", nPid);
	} else {
		Log("Exit: Panic: Can't read my pid from %s", szPidFile);
	}

	ReallyExit(0);		// Don't use the usual exit as it tidies up the .pid file that might be in use...!
}

static int StopAnyPrevious()
// Stops any previous incarnations and puts us in as leader
// If this doesn't succeed then we print an error message and exit.
// Returns the process id of the previous process or 0 if there was none.
{
	int nPid = GetDaemonPid();

	if (nPid) {
		int nSignal;
		int i;

		// Here we send a SIGQUIT and wait 5 secs for the process to go.
		// If it is still there after this time, we send a SIQKILL and wait again
		for (nSignal=SIGQUIT;nSignal<=SIGKILL;nSignal+=(SIGKILL-SIGQUIT)) {		// Try both signals
			if (kill(nPid, SIGQUIT)) {		// Error in kill
				if (errno == ESRCH) break;	// No such process
				if (errno == EPERM) {
					fprintf(stderr, "%s: I don't have permission to kill existing instance (pid=%d)\n", szMyName, nPid);
					Exit(4);
				}
			}
			for (i=0;i<5;i++) {			// Check 5 times for death of the other process
				if (!ProcessAlive(nPid)) break;			// It's gone
				sleep(1);
			}
		}
		// Either the process didn't exist before or we've killed it
		// If it is still alive then it must be caught in the kernel and we're stuffed
		if (nSignal != SIGQUIT && nSignal != SIGKILL) {
			fprintf(stderr, "%s: Existing instance (pid=%d) would not die\n", szMyName, nPid);
			Exit(5);
		}
	}
	// There is now no existing instance

	return nPid;
}

static void HandleSignal(int n)
{
	signal(n, HandleSignal);

	switch (n) {
	case SIGHUP:										// 1 Exit Hangup [see termio(7)]
		break;
	case SIGINT:										// 2 Exit Interrupt [see termio(7)]
		break;
	case SIGQUIT:										// 3 Core Quit [see termio(7)]
	case SIGTERM:										// 15 Exit Terminated
		Log("Exit: Signal %s", SignalName(n));
		Exit(0);
		break;
	case SIGILL:										// 4 Core Illegal Instruction
	case SIGTRAP:										// 5 Core Trace/Breakpoint Trap
	case SIGABRT:										// 6 Core Abort
#ifdef __SCO_VERSION__
	case SIGEMT:										// 7 Core Emulation Trap
#endif
	case SIGFPE:										// 8 Core Arithmetic Exception
	case SIGBUS:										// 10 Core Bus Error
	case SIGSEGV:										// 11 Core Segmentation Fault
	case SIGSYS:										// 12 Core Bad System Call
		signal(n, SIG_DFL);								// Let us drop out next time
		Log("Killed - %s", SignalName(n));				// This order in case it was 'Log' that killed us!
		kill(getpid(), n);								// Kill ourselves
		break;
	case SIGKILL:										// 9 Exit Killed - never seen, just kills us
		break;
	case SIGPIPE:										// 13 Exit Broken Pipe
		break;
	case SIGALRM:										// 14 Exit Alarm Clock
		alarm(0);										// Stop anything further
		if (_nAlarmNumber) {
			int nAlarmNumber=_nAlarmNumber;
			_nAlarmNumber=0;
			longjmp(jmpbuf_alarm, nAlarmNumber);
		}
		return;											// False alarm...
		break;
	case SIGUSR1:										// 16 Exit User Signal 1
//		WriteStatus();
		return;
		break;
	case SIGUSR2:										// 17 Exit User Signal 2
		break;
	default:
		break;
	}

	signal(n, SIG_DFL);									// Let us drop out next time
	Log("Unexpected signal %s - quitting", SignalName(n));		// This order in case it was 'Log' that killed us!
	kill(getpid(), n);									// Kill ourselves
	ReallyExit(0);										// Shouldn't ever get here
}

static void SetIncomingHost(MIME *mime)
{
	const char *szHost = mime_GetHeader(mime, "host");
	const char *chp;

	szDelete(_szIncomingHost);

	_szIncomingHost = (szHost && (chp = strchr(szHost,':')))
		? strnappend(NULL, szHost, chp-szHost)
		: strdup(_szIncomingIp);
}

static int RogRename(const char *szSrc, const char *szDest)
// Returns	0		All is good
//			1...	Errno
{
	int err;

	err=rename(szSrc, szDest);
	if (err && errno == EXDEV) {					// Different file system, we can deal with that...
		FILE *fpDest, *fpSrc;
		char buf[10240];
		size_t got;

		fpDest=fopen(szDest, "w");					// Create a new file
		if (!fpDest) return -1;						// Failed - can't do it then
		fpSrc=fopen(szSrc, "r");					// Open up the original
		if (!fpSrc) {fclose(fpDest); return -1;}	// Someone is giving us a duff filename?
		while ((got=fread(buf, 1, sizeof(buf), fpSrc))) {
			fwrite(buf, 1, got, fpDest);			// Copy it over
		}
		fclose(fpSrc);								// Close them up
		fclose(fpDest);
		if (unlink(szSrc)) {						// Damn - copied it but can't delete the original!
			unlink(szDest);							// If we can't tidy the dest, we're stuffed...
			return -1;
		}
		err=0;										// We did it.
	}

	return err ? errno : 0;
}

static char szReadBuf[1024];
static char *szReadBufp=NULL;			// Pointer to next char in buffer
static char *szReadBufEnd;				// How many characters are actually in it
int nReadStream;						// The stream to read
FILE *fpDebugReadStream = NULL;			// Debug output if wanted

// NB. To capture input from the internal connection, create a file called /tmp/spider.in.debug
//     This will get the next chunk of characters and then get renamed to /tmp/spider.in

static void CloseReadStreamDebug()
{
	if (fpDebugReadStream) {
		fclose(fpDebugReadStream);
		fpDebugReadStream=NULL;
	}
}

static void InitReadStream(int fd)
{
	szReadBufp=szReadBufEnd=szReadBuf;
	nReadStream = fd;
	CloseReadStreamDebug();
	if (!access("/tmp/spider.in.debug", 0)) {
		fpDebugReadStream = fopen("/tmp/spider.in.debug", "w");
		if (fpDebugReadStream) RogRename("/tmp/spider.in.debug", "/tmp/spider.in");
	}
}

// TODO:	ReadStream returns one char, I could really do with something that returns a whole buffer full
//			as it would make the compressed file reading much more efficient.
static int ReadStream()
// This function is called from the rogxml library to read XML from the input stream
{
	int nGot;

	if (szReadBufp != szReadBufEnd) {
		unsigned char c=*(unsigned char *)(szReadBufp++);

		if (fpDebugReadStream) {
			putc(c, fpDebugReadStream);
		}

		return c;
	}

	nGot = recv(nReadStream, szReadBuf, sizeof(szReadBuf), 0);
	if (nGot > 0) {					// Easy case, got some data...
		szReadBufEnd=szReadBuf+nGot;
		szReadBufp=szReadBuf;
		alarm(10);						// Give us another 10 secs to deal with it
		return ReadStream();			// Was: return *(unsigned char *)(szReadBufp++);
	}

	CloseReadStreamDebug();

	return -1;							// Might just indicate end of file...
}

static rogxml *ErrorMessage(int nErr, int nSeverity, const char *szFmt, ...)
{
	va_list ap;
	char szErr[256];
	rogxml *rx;

	va_start(ap, szFmt);
	vsnprintf(szErr, sizeof(szErr), szFmt, ap);
	va_end(ap);

	rx=rogxml_NewElement(NULL, "SPIDER-Ack");
	rogxml_AddText(rx, szErr);
	rogxml_SetAttr(rx, "type", "error");
	rogxml_SetAttrInt(rx, "code", nErr);
	rogxml_SetAttr(rx, "severity", nSeverity ? "fatal" : "warning");

	return rx;
}

static int SendError(int fd, int nErr, int nSeverity, const char *szFmt, ...);

static int SendXML(int fd, rogxml *rx, const char *szDescription)
// Send an XML message to the caller.
// If _bPackXML is set then send it compressed unless it's an error, which are always sent in the clear
// Returns	0		Writing seemed to go ok (does not gaurantee that it's received ok)
//			1...	Error on writing (usually EPIPE (32))
{
	const char *szXML;
	int nErr = rogxml_ErrorNo(rx);
	int nSendErr = 0;
	static int nNumberSent;

	nNumberSent++;

	if (nErr) {								// An error so we need to send a nack
		nSendErr = SendError(fd, nErr, 1, "%s", rogxml_ErrorText(rx));
	} else {
		if (_bPackXML) {					// Send it as small as possible
			rogxml *rxPacked;
			const char *szPacked = NULL;
			int nPackedLen = 0;
			int nLen;
			int nWritten;

			rogxml_SetIndentString("");
			rogxml_SetLinefeedString("");

			Log("Sending SPIDER-Packed");
			rxPacked = rogxml_NewElement(NULL, "SPIDER-Packed");
			szXML = rogxml_ToText(rxPacked);
			nLen = strlen(szXML);
			nWritten = write(fd, szXML, nLen);
			szDelete(szXML);

			if (nWritten == nLen) {			// Only write the main message if we succeeded in writing the lead element
				write(fd, "\n", 1);			// We need a linefeed after the initial element
				szXML = rogxml_ToText(rx);
				pack_InitPack();
				pack_Pack(szXML, strlen(szXML));
				szPacked = pack_GetPacked(&nPackedLen);

				nWritten = write(fd, szPacked, nPackedLen);
//Log("Original was %d, compressed is %d, have written %d", strlen(szXML), nPackedLen, nWritten);

				szDelete(szPacked);
				szDelete(szXML);
				if (nWritten != nPackedLen) nSendErr = errno ? errno : -1;
			} else {
				nSendErr = errno ? errno : -1;
			}
		} else {							// Ordinary unpacked message
			int nLen = 0;
			int nWritten;
			szXML = rogxml_ToNiceText(rx);

			nLen = strlen(szXML);

			nWritten = write(fd, szXML, nLen);
			if (nWritten != nLen) {
				nSendErr = errno ? errno : -1;
				Log("Tried to write %d, wrote %d (errno=%d)", nLen, nWritten, errno);
			}

			szDelete(szXML);
		}
	}

	const char *szNoteName = hprintf(NULL, "toapp%d", nNumberSent);
	if (nSendErr) {
		const char *szNote = hprintf(NULL, "%s sent to app (failed)", szDescription);
		NoteMessageXML(rx, szNoteName, szNote, 0);
		Log("Error %d sending to application", nSendErr);
		szDelete(szNote);
	} else {
		const char *szNote = hprintf(NULL, "%s sent to app", szDescription);
		NoteMessageXML(rx, szNoteName, szNote, 0);
		szDelete(szNote);
	}
	szDelete(szNoteName);

	return nSendErr;
}

static int SendError(int fd, int nErr, int nSeverity, const char *szFmt, ...)
// Sends an error code in place of the 'happy' SPIDER-Ack
// nErr is the error code, nSeverity is 0=warning, 9=fatal
// Returns	0	Seemed to send ok (may not have been received ok)
//			1	Error (see errno for details)

{
	va_list ap;
	char szErr[256];
	rogxml *rxErr;
	int nSendErr = 0;

	va_start(ap, szFmt);
	vsnprintf(szErr, sizeof(szErr), szFmt, ap);
	va_end(ap);

	rxErr=ErrorMessage(nErr, nSeverity, "%s", szErr);

	nSendErr = SendXML(fd, rxErr, "error");

	if (nSendErr) {
		Log("Error sent %d: %s (but error %d on sending)", nErr, szErr, nSendErr);
	} else {
		Log("Error sent %d: %s", nErr, szErr);
	}
//	ipc_Sendf(getppid(), ipc_log, "Error %d: %s", nErr, szErr);

	rogxml_Delete(rxErr);

	return nSendErr;
}


static rogxml *ReceiveXML(int fd, int nTimeout)
// Waits up to nTimeout seconds for an XML message from the application
// Returns the XML received or an error if a timeout occurred
{
	rogxml *rx;

	if (nTimeout) {							// Arrange that we get a wakeup call if necessary
		_nAlarmNumber=2;
		if (setjmp(jmpbuf_alarm)) {
			return rogxml_NewError(12, "Timeout waiting for message");
		} else {
			alarm(nTimeout);
		}
	}

	InitReadStream(fd);
	rx=rogxml_ReadFn(ReadStream);
	while (rogxml_IsInstruction(rx)) {		// Not very pretty but deals with <?xml> things...
		rogxml_Delete(rx);
		rx=rogxml_ReadFn(ReadStream);
	}

	if (!strcmp(rogxml_GetLocalName(rx), "SPIDER-Packed")) {			// Special case of packed input
		int c;
		const char *unpacked;
		char buf[1024];
		int got;
		int len;
		int nPacking;					// Packing status - 0=still going, 1=done, >1=error
		int nCount=0;

		_bPackXML = 1;					// Send packed stuff back by default

		Log("Compressed message received");
		for (;;) {						// Skip up to a linefeed
			c=ReadStream();
			if (c == -1 || c == 10) break;
		}
		if (c == -1) return NULL;		// There wasn't anything else...
		rogxml_Delete(rx);
		pack_InitUnpack();
		for (;;) {						// Need to use ReadStream as it probably has stuff buffered up...
			nCount++;
			c=ReadStream();
			if (c == -1) break;
			*buf=c;
			got=1;
			nPacking = pack_Unpack(buf, got);
			if (nPacking) break;
		}
		unpacked=pack_GetUnpacked(&len);
		Log("Uncompressed %d to %d", nCount, len);		// Gloat
		rx=rogxml_FromText(unpacked);
		szDelete(unpacked);
	}

	alarm(0);
	_nAlarmNumber=0;

	return rx;
}

static rogxml *AckMessage(const char *szMsgType, const char *szMsgId, const char *szMsgRcv, const char *szMsgSnd)
{
	rogxml *rx;

	rx=rogxml_NewElement(NULL, "SPIDER-Ack");
	rogxml_SetAttr(rx, "type", "ack");
	rogxml_SetAttrInt(rx, "code", 0);

	if (szMsgType) rogxml_AddTextChild(rx, "MsgType", szMsgType);
	if (szMsgId) rogxml_AddTextChild(rx, "MsgId", szMsgId);
	if (szMsgRcv) rogxml_AddTextChild(rx, "MsgRcv", szMsgRcv);
	if (szMsgSnd) rogxml_AddTextChild(rx, "MsgSnd", szMsgSnd);

	return rx;
}

SSMAP *ssEnv = NULL;
static void EnvSet(const char *szName, const char *szValue)
// Set or unset an environment variable
// Call with NULL szValue to unset
// Don't pass a szName with an '=' in it. Really.
{
	if (szValue) {
		if (!ssEnv) {
			ssEnv=ssmap_New();
			ssmap_CopyKeys(ssEnv, 1);				// Make sure keys look after themselves
		}

		const char *szVariable = hprintf(NULL, "%s=%s", szName, szValue);		// To be passed to putenv
		ssmap_Add(ssEnv, szName, szVariable);
		szDelete(szVariable);							// We need to use the string that we later use to delete
		szVariable=ssmap_GetValue(ssEnv, szName);		// So read it back from the map
		if (szVariable) {								// It'd be strange if we didn't find it...
			putenv((char*)szVariable);
//			Log("putenv(%s) [%d]", szVariable, szVariable);
		}
	} else {
		if (ssEnv) {
			const char *szVariable = ssmap_GetValue(ssEnv, szName);
			if (szVariable) {
				char *chp=strrchr((char*)szVariable, '=');
				if (chp) {								// This really has to find one
					*chp='\0';							// Blatt it so we have "NAME="
					putenv((char*)szVariable);
				}
				ssmap_DeleteKey(ssEnv, szName);
			}
		}
	}
}

static void ReadEnvironmentFile(const char *szFile)
// The file in this case is a load of NAME=VALUE pairs (with blanks and '#' comments ignored)
// Note that putenv() has touchy semantics whereby the string passed must remain valid so this function
// is careful to tidy up previous strings using the (undocumented) feature whereby "VAR=" can be passed
// to putenv() to remove a variable completely.
{
	static time_t tLastUpdated=0;						// Last recorded file update time
	struct stat st;
	static SSMAP *envmap = NULL;						// Map of environment variables

	int err = stat(szFile, &st);
	if (!err && st.st_mtime > tLastUpdated) {			// Exists and updated since previous check
		tLastUpdated=st.st_mtime;

		if (envmap) {									// Tidy up the old environment
			ssmap_Reset(envmap);
			const char *szValue;

			while (ssmap_GetNextEntry(envmap, NULL, &szValue)) {
				char *pEquals=strchr(szValue, '=');

				if (pEquals) {
					((char *)pEquals)[1]='\0';			// Turn "NAME=VALUE" into "NAME="
					putenv((char*)szValue);				// Deletes the env variable
				}
			}
			ssmap_Delete(envmap);
		}
		envmap = ssmap_GetFile(szFile);
		if (envmap) {
			ssmap_Reset(envmap);
			const char *szName;
			const char *szValue;
			int count=0;

			while (ssmap_GetNextEntry(envmap, &szName, &szValue)) {
				const char *szNewValue = hprintf(NULL, "%s=%s", szName, szValue);	// Make value "NAME=VALUE"
				ssmap_Add(envmap, szName, szNewValue);								// Set that back into the map
				szDelete(szNewValue);												// Delete the temp copy

				szValue=ssmap_GetValue(envmap, szName);								// Get the permanently alloced one
				putenv((char*)szValue);												// Use it to set the env
				count++;
			}

			Log("Read %d environment variable%s from %s", count, count==1?"":"s", szFile);
		}
	}
}

static int BIO_putf(BIO *b, const char *szFmt, ...)
// Like BIO_puts but accepts a formatted string.
// Truncates over 1000 chars
{
	va_list ap;
	char buf[1000];

	va_start(ap, szFmt);
	vsnprintf(buf, sizeof(buf), szFmt, ap);
	va_end(ap);

	return MyBIO_puts(b, buf);
}

// Little functions to handle printing to our peer...
static BIO *g_io = NULL;
static void ioset(BIO *io)
{
	g_io = io;
}

static int ioprintf(const char *szFmt, ...)
// Like printf(), but sends it to our 'io' mate
{
	if (g_io) {
		va_list ap;
		char buf[1000];

		va_start(ap, szFmt);
		vsnprintf(buf, sizeof(buf), szFmt, ap);
		va_end(ap);

		return MyBIO_puts(g_io, buf);
	}
	return 0;
}

static int cb_Password(char *buf,int num, int rwflag,void *userdata)
// Callback function for returning the password to get into the DES3 encrypted private key file
{
	if(num<strlen(_szPassword)+1) return(0);		// Too short a buffer

	strcpy(buf,_szPassword);						// Copy over plain text buffer???

	return(strlen(_szPassword));
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

	if(SSL_CTX_set_tmp_dh(ctx,ret)<0)
		FatalSSL("Couldn't set DH parameters");
}

static char *_szCtxError = NULL;

static const char *ctx_Error()
{
	return _szCtxError;
}

static const char *GetCertificateDir()
// Depending on the environment, returns the directory with the certifcates etc. (mt.cert, mt.ca, mt.key)
// Returns a static string so no need to free it.
{
	return GetEtcDir();						// The default
}

SSL_CTX *ctx_New(const char *szConfigDir, const char *szPrefix, const char *szPassword)
// Create a new SSL context and return it
// If the directory does contain certs.conf then 'szPrefix' is used to extract the following from the file:
//   prefix-ca, prefix-cert, prefix-key and prefix-pw
//   If they are not found, then ca, cert, key and pw are used
//   If they are not found (or the file doesn't exist) then mt.ca, mt.cert and mt.key are used with the default pw
//   These are the certificate authority chain, the local certificate and the key respectively.
// On error, returns NULL - call ctx_Error() to pick up the error message
// NB. We keep szCertCA, szCertLocal and szKeyLocal around in case SSL_CTX_load_verify_locations() needs
//		them after the function exits.
{
	const SSL_METHOD *meth = NULL;
	SSL_CTX *ctx = NULL;

//Log("Building TLS/SSL context using authentication in %s for %s", szConfigDir, szPrefix);

	const char *szCertCA="/usr/mt/spider/etc/spiderca.cert.pem";
	const char *szCertLocal="/usr/mt/spider/etc/spider.cert.pem";
	const char *szKeyLocal="/usr/mt/spider/etc/spiderca.key.pem";
	_szPassword=strdup(szPassword);
//Log("ca=%s, cert=%s, key=%s, pw=%s", szCertCA, szCertLocal, szKeyLocal, _szPassword);

	szDelete(_szCtxError);
	_szCtxError = NULL;

	if (!bio_err) {				// Need to initialise
		SSL_library_init();
		SSL_load_error_strings();

		bio_err=BIO_new_fp(stderr,BIO_NOCLOSE);		// For writing errors
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

	if (_szCtxError) {						// We came unstuck somewhere
		return NULL;
	} else {
		load_dh_params(ctx);				// Exits on failure

		return ctx;
	}
}

#if 0
SSL_CTX *x2ctx_New(const char *szConfigDir, const char *szPrefix, const char *szPassword)
// Create a new SSL context and return it
// If the directory does contain certs.conf then 'szPrefix' is used to extract the following from the file:
//   prefix-ca, prefix-cert, prefix-key and prefix-pw
//   If they are not found, then ca, cert, key and pw are used
//   If they are not found (or the file doesn't exist) then mt.ca, mt.cert and mt.key are used with the default pw
//   These are the certificate authority chain, the local certificate and the key respectively.
// On error, returns NULL - call ctx_Error() to pick up the error message
// NB. We keep szCertCA, szCertLocal and szKeyLocal around in case SSL_CTX_load_verify_locations() needs
//		them after the function exits.
{
	SSL_METHOD *meth = NULL;
	SSL_CTX *ctx = NULL;

	static const char *szCertCA = NULL;
	static const char *szCertLocal = NULL;
	static const char *szKeyLocal = NULL;

	szDelete(szCertCA);						// Tidy up the memory if we've been this way before
	szDelete(szCertLocal);
	szDelete(szKeyLocal);

Log("Building TLS/SSL context using authentication in %s for %s", szConfigDir, szPrefix);

	const char *szConfig = hprintf(NULL, "%s/certs.conf", szConfigDir);
	SSMAP *ssCerts = ssmap_GetFile(szConfig);
	szDelete(szConfig);

	int nBuf = strlen(szPrefix)+100;							// Prefix plus -cert and some spare
	char *buf = (char*)malloc(nBuf+1);

	snprintf(buf, nBuf, "%s-ca", szPrefix);
	const char *szCaFile = ssmap_GetValue(ssCerts, buf);
	if (!szCaFile) szCaFile=ssmap_GetValue(ssCerts, "ca");
	if (!szCaFile) szCaFile="mt.ca";

	snprintf(buf, nBuf, "%s-cert", szPrefix);
	const char *szCertFile = ssmap_GetValue(ssCerts, buf);
	if (!szCertFile) szCertFile=ssmap_GetValue(ssCerts, "cert");
	if (!szCertFile) szCertFile="mt.cert";

	snprintf(buf, nBuf, "%s-key", szPrefix);
	const char *szKeyFile = ssmap_GetValue(ssCerts, buf);
	if (!szKeyFile) szKeyFile=ssmap_GetValue(ssCerts, "key");
	if (!szKeyFile) szKeyFile="mt.key";

	snprintf(buf, nBuf, "%s-pw", szPrefix);
	const char *szPw = ssmap_GetValue(ssCerts, buf);
	if (!szPw) szPw=ssmap_GetValue(ssCerts, "pw");
	if (!szPw) szPw=szPassword;

Log("ca=%s, cert=%s, key=%s, pw=%s", szCaFile, szCertFile, szKeyFile, "xyzzy");
	szDelete(_szPassword);
	_szPassword=strdup(szPw);

	szCertCA=hprintf(NULL, "%s/%s", szConfigDir, szCaFile);
	szCertLocal=hprintf(NULL, "%s/%s", szConfigDir, szCertFile);
	szKeyLocal=hprintf(NULL, "%s/%s", szConfigDir, szKeyFile);

	szDelete(buf);
	ssmap_Delete(ssCerts);
Log("ca=%s, cert=%s, key=%s, pw=%s", szCertCA, szCertLocal, szKeyLocal, _szPassword);

	szDelete(_szCtxError);
	_szCtxError = NULL;

	if (!bio_err) {				// Need to initialise
		SSL_library_init();
		SSL_load_error_strings();

		bio_err=BIO_new_fp(stderr,BIO_NOCLOSE);		// For writing errors
	}

// No longer do this as giving a handler for SIGPIPE stops write() from returning an error on writing to
// a broken pipe and we'd rather handle it at the 'write()' stage as it's more controled.
//	signal(SIGPIPE,sigpipe_handle);					// In case of premature connection closure

	meth=SSLv23_method();							// Create our context
	ctx=SSL_CTX_new(meth);
Log("%p=SSL_CTX_new(%p)", ctx, meth);
	if (!ctx) {
		_szCtxError = hprintf(NULL, "Failed to create an SSL/TLS context");
	}

	if (!_szCtxError) {								// Get our certificates from the key file
Log("Getting certificate chain file");
		if (!(SSL_CTX_use_certificate_chain_file(ctx, szCertLocal))) {
			_szCtxError = hprintf(NULL, "Can't read local certificate file '%s'", szCertLocal);
		}
	}

	if (!_szCtxError) {								// Set the password
Log("Setting the password");
		SSL_CTX_set_default_passwd_cb(ctx, cb_Password);

		if(!(SSL_CTX_use_PrivateKey_file(ctx, szKeyLocal, SSL_FILETYPE_PEM))) {
			_szCtxError = hprintf(NULL, "Can't read key file '%s'", szKeyLocal);
		}
	}

	if (!_szCtxError) {								// Get the list of trusted certificates
Log("Getting the list of trusted certificates");
		if(!(SSL_CTX_load_verify_locations(ctx, szCertCA,0))) {
			_szCtxError = hprintf(NULL, "Can't read CA list file '%s'", szCertCA);
		}
	}

	if (_szCtxError)
		Log("ctx_New(): Error = '%s'", _szCtxError ? _szCtxError : "None at all");

	if (_szCtxError) {						// We came unstuck somewhere
		return NULL;
	} else {
		load_dh_params(ctx);				// Exits on failure

		return ctx;
	}
}
#endif

static void ctx_Delete(SSL_CTX *ctx)
// Delete an SSL context
{
	if (ctx) SSL_CTX_free(ctx);
}

typedef struct note_t {
	int nEntries;
	char **pszEntry;				// Most entries
	int nMessages;
	char **pszMessage;			// 'M' message entries
} note_t;

static void note_Delete(note_t *n)
{
	if (n) {
		int i;

		for (i=0;i<n->nEntries;i++) szDelete(n->pszEntry[i]);
		for (i=0;i<n->nMessages;i++) szDelete(n->pszMessage[i]);
		free((char*)n->pszEntry);
		free((char*)n->pszMessage);
		free((char*)n);
	}
}

static note_t *note_LoadMessage(const char *szDir)
// Given a directory (relative to msgdir), loads the 'info' file
{
	note_t *n;
	const char *szFilename = hprintf(NULL, "%s/%s/info", szMsgLogDir, szDir);
	FILE *fp;

	fp=fopen(szFilename, "r");
	szDelete(szFilename);
	if (!fp) return NULL;

	n=NEW(note_t, 1);
	n->nEntries=0;
	n->pszEntry=NEW(char*,1);
	n->pszEntry[0]=NULL;
	n->nMessages=0;
	n->pszMessage=NEW(char*,1);
	n->pszMessage[0]=NULL;

	for (;;) {
		const char *szLine = ReadLine(fp);
		int *pn;
		char ***pps;

		if (!szLine) break;

		if (*szLine == 'M') {
			pn=&n->nMessages;
			pps=&n->pszMessage;
		} else {
			pn=&n->nEntries;
			pps=&n->pszEntry;
		}
		RENEW(*pps, char*, *pn+2);
		(*pps)[*pn]=strdup(szLine);
		(*pps)[++(*pn)]=NULL;
	}
	fclose(fp);

	return n;
}

int note_GetEntryCount(note_t *n)		{return n?n->nEntries:0;}
int note_GetMessageCount(note_t *n)		{return n?n->nMessages:0;}

static const char *note_GetEntry(note_t *n, int i)
{
	if (!n) return NULL;
	return (i<1 || i>n->nEntries) ? NULL : n->pszEntry[i-1];
}

static const char *note_FindEntry(note_t *n, char c)
{
	int nNotes=note_GetEntryCount(n);
	int i;

	for (i=1;i<=nNotes;i++) {
		const char *szEntry = note_GetEntry(n, i);
		if (*szEntry == c) return szEntry+2;
	}

	return NULL;
}

static const char *Link(const char *fmt, ...)
// Turns a link into a qualified one, taking into consideration that we might be within an environment
// Handles 10 simultaneous calls without over-writing previous results.
// DO NOT free the result from this call.
{
	char buf[500];
	static const char **linkcache=NULL;
	static int linkno=0;
	const int maxlinkno=10;
	const char *link = NULL;
	const char *ref;

	if (!linkcache) {
		int i;

		linkcache=NEW(const char *, maxlinkno);
		for (i=0;i<maxlinkno;i++) linkcache[i]=NULL;
	}

	va_list ap;
	va_start(ap, fmt);
	vsnprintf(buf, sizeof(buf), fmt, ap);
	va_end(ap);
	ref=buf;

	if (*ref == '/') ref++;
	if (szEnvironment) {
		link = hprintf(NULL, "\"/%s/%s\"", szEnvironment, ref);
	} else {
		link = hprintf(NULL, "\"/%s\"", ref);
	}

	linkno++;
	if (linkno == maxlinkno) linkno=0;
	szDelete(linkcache[linkno]);
	linkcache[linkno]=link;

	return link;
}

const char *NoteFileAnchor(const char *file, const char *title)
{
	static const char *result = NULL;

	szDelete(result);

	result = hprintf(NULL, "<a href=%s>%s</a>", Link("/getfile/%s/%s", msc_NoteDirTail(), file), title);

	return result;
}

const char *msc_UriEncode(const char *szText)
// URI encodes the given text, returning a string on the heap.
// Turns out we do this:
// Keep a-z, A-Z, 0-9, ., -, * and _
// Change spaces to '+'
// Change everything else to %xx where 'xx' is the hex of the character
{
	char szSrc[2];
	char szDest[4];
	int c;

	szText = strdup(szText);							// We need to work with a heap-based copy
	szSrc[1]='\0';
	szText = strsubst(szText, "%", "%25");				// Must do this first, you'll need to think about it
	for (c='!';c<='~';c++) {
		if (isalnum(c) || c=='.' || c=='-' || c=='*' || c=='_' || c=='%') continue;	// We leave these alone

		if (!strchr(szText, c)) continue;				// Quickest to check first - skip if the char doesn't appear
		*szSrc=c;										// szSrc is now a string consisting of the single character

		snprintf(szDest, sizeof(szDest), "%%%02x", c);	// szDest is now "%xx" with the correct replacement
		szText = strsubst(szText, szSrc, szDest);		// Replace all occurrences
	}

	szText = strsubst(szText, " ", "+");				// Now do the slightly weird special case

	return szText;
}

const char *msc_DayZipFile(int day, int month, int year)
{
	static const char *result = NULL;
	szDelete(result);

	result = hprintf(NULL, "%s/%02d/%02d/mmts-%02d%02d%02d.zip",
					szMsgLogDir, year, month, year, month, day);
	return result;
}

int msc_HasLogError(const char *szFile)
// Returns 1 if the file has the word 'error' in it, 0 otherwise
{
	int bResult=0;
	FILE *fp=fopen(szFile, "r");

	if (fp) {
		const char *szLine;

		while ((szLine=ReadLine(fp))) {
			if (stristr(szLine, "error")) {
				bResult=1;
				break;
			}
		}
		fclose(fp);
	}

	return bResult;
}

const char *msc_StatusLink(int d, int m, int y, int bDeep, const char *szFilter)
{
	static const char *result=NULL;
	const char *suffix;

	szDelete(result);

	if (szFilter && *szFilter) {
		const char *szEncodedFilter = msc_UriEncode(szFilter);

		suffix=hprintf(NULL,"?%sf=%s", bDeep?"deep=on&":"", szEncodedFilter);
		szDelete(szEncodedFilter);
	} else {
		suffix=strdup("");
	}

	if (d)		result=hprintf(NULL, "/status/20%02d/%02d/%02d%s", y, m, d, suffix);
	else if (m) result=hprintf(NULL, "/status/20%02d/%02d%s", y, m, suffix);
	else		result=hprintf(NULL, "/status/20%02d%s", y, suffix);

	szDelete(suffix);

	return Link(result);
}

const char *msc_FilterDayLink(int day, int month, int year, const char *szText)
// Return an effectively static string that will link to a search screen with items filtered by the string
{
	static const char *szResult = NULL;
	szDelete(szResult);

	if (year < 99) year+=2000;
	if (szText && *szText) {
		const char *szEncodedFilter = msc_UriEncode(szText);
		szResult=hprintf(NULL, "<a href=%s>%s</a>",
				Link("/status/%04d/%02d/%02d?f=%s", year, month, day, szEncodedFilter, szText),
				szText);
		szDelete(szEncodedFilter);
	} else {
		szResult=hprintf(NULL, "<a href=%s>all</a>", Link("/status/%04d/%02d/%02d", year, month, day));
	}

	return szResult;
}

void msc_DisplayCalendar(BIO *io, int d, int m, int y, int bDeep, const char *szFilter)
// Displays a calendar and whatnot to enable speedy selection of messages
{
//	const char szMon[]="JanFebMarAprMayJunJulAugSepOctNovDec";
	time_t now=time(NULL);
	struct tm *tm=gmtime(&now);
	int thisYear = tm->tm_year%100;
	int thisMonth = tm->tm_mon+1;
	int thisDay = tm->tm_mday;

	if (!y) {
		y = thisYear;
		m = thisMonth;
		d = thisDay;
	} else if (!m) {
		if (y % 100 == thisYear) {
			m = thisMonth;
			d = thisDay;
		} else {
			m = 12;
			d = 31;
		}
	} else if (!d) {
		if (y % 100 == thisYear && m == thisMonth) {
			d = thisDay;
		} else {
			d = 1;
		}
	}

//	const char *szMonthDir = hprintf(NULL, "%s/%02d/%02d", szMsgLogDir, y, m);
//	const char *szNextMonthDir = hprintf(NULL, "%s/%02d/%02d", szMsgLogDir, y+(m==12), (m==12)?1:(m+1));
//	const char *szPrevMonthDir = hprintf(NULL, "%s/%02d/%02d", szMsgLogDir, y-(m==1), (m==1)?12:(m-1));
	int m1, y1;
	if (y*100+m < thisYear*100+thisMonth) {
		m1=(m+10)%12+1;
		y1=y-(m<2);
	} else {
		m1=(m+9)%12+1;
		y1=y-(m<3);
	}
	if (y1 < 0) y1=0;									// Don't go before 2000
	int year1=thisYear-2;
	int yearn=thisYear;

	BIO_putf(io, "<style>\n");
	BIO_putf(io, "td.month {font-family:verdana; font-size:10pt; text-align:center}");
	BIO_putf(io, "td.day {text-align:center; width:30px; text-wrap:none}");
	BIO_putf(io, "td.nonday {text-align:center; width:30px}");
	BIO_putf(io, "</style>\n");

	BIO_putf(io, "<table border>\n");
	BIO_putf(io, "<tr>");
	int year;
	for (year=year1;year<=yearn;year++) {
		BIO_putf(io, "<td colspan=\"2\" style=\"width:30px;text-align:center\"><a style=\"text-decoration:none; font-weight:bold;font-size:8pt;color:black\" href=%s>", msc_StatusLink(0, 0, year, bDeep, szFilter));
		BIO_putf(io, "20%02d", year);
		BIO_putf(io, "</a></td>");
	}
	BIO_putf(io, "<td class=\"month\" colspan=\"7\">%s 20%02d</td>", MonthNames[m1], y1);
	BIO_putf(io, "<td class=\"month\" colspan=\"7\">%s 20%02d</td>", MonthNames[m1%12+1], y1+(m1==12));
	BIO_putf(io, "<td class=\"month\" colspan=\"7\">%s 20%02d</td>", MonthNames[(m1+1)%12+1], y1+(m1>=11));
	BIO_putf(io, "</tr>");

	int r;
	for (r=0;r<6;r++) {
		BIO_putf(io, "<tr>");

		int year;
		for (year=year1;year<=yearn;year++) {
			const char *background = (year-year1)&1?"#999999":"#777777";
			if (year == year1) {
				int myYear = year-r-1;
				BIO_putf(io, "<td colspan=\"2\" style=\"text-align:center;background-color:%s\"><a style=\"text-decoration:none; font-weight:bold;font-size:8pt;color:%s\" href=%s>20%02d</a></td>",
						background,
						"white", 
						msc_StatusLink(0, 0, myYear, bDeep, szFilter),
						myYear);
			} else {
				int month;
				for (month=r*2+1;month<=r*2+2;month++) {
					BIO_putf(io, "<td style=\"text-align:center;background-color:%s\"><a style=\"text-decoration:none; font-weight:bold;font-size:8pt;color:%s\" href=%s>%.3s</a></td>",
						background,
						(month == thisMonth && year == thisYear) ? "yellow" : "white", 
						msc_StatusLink(0, month, year, bDeep, szFilter),
						MonthNames[month]);
				}
			}
		}

		int mi;
		for (mi=0;mi<3;mi++) {
			int month=(m1+mi+11)%12+1;
			int year=(m1+mi>12)?y1+1:y1;
			int dow=(year+year/4+"0144025036146"[month]-((month<3) && !(year%4)))%7;
			int len="0303232332323"[month]-'0'+28+(month==2 && !(year % 4));
			int day=(dow?(2-dow):-5)+r*7;

			int c;
			for (c=0;c<=6;c++) {
			//	const char *background=(r&1) ? "#7799bb" : "#99ccdd";
				const char *background=(r&1) ? "#5b8eb2" : "#1d8eb2";
				const char *colour="white";
				const char *borderl="none";
				const char *borderr="none";
				const char *bordert="none";
				const char *borderb="none";

				if (c >= 5) {
					background="#999999";
					colour="#dddddd";
				}
				if (year == y && month == m && day == d) {
					borderl="3px solid #F00";
					borderr="3px solid #F00";
					bordert="3px solid #F00";
					borderb="3px solid #F00";
				}

				const char *szStyle=hprintf(NULL,
						"background:%s;"
						"border-left:%s;"
						"border-right:%s;"
						"border-top:%s;"
						"border-bottom:%s",
						background, borderl, borderr, bordert, borderb);

				if (day >= 1 && day <= len) {
					const char *szDay = hprintf(NULL, "%s/%02d/%02d/%02d", szMsgLogDir, year, month, day);
					int count=msc_DirCount(szDay);
					char szCount[10];

					if (count) {
						snprintf(szCount, sizeof(szCount), "%d", count);
					} else {
						if (!access(msc_DayZipFile(day, month, year), 4)) {
							strcpy(szCount, "(arc)");
						} else {
							strcpy(szCount, "&nbsp;");
						}
					}

					szDelete(szDay);
					BIO_putf(io, "<td class=\"day\" style=\"%s\">"
							"<a style=\"text-decoration:none; font-weight:bold;font-size:10pt;color:%s\" href=%s>"
							"%d"
							"<br/>"
							"<div style=\"font-size:8pt;color:black\">"
							"%s"
							"</div>"
							"</a>"
							"</td>",
							szStyle,
							colour,
							msc_StatusLink(day, month, year, bDeep, szFilter),
							day, szCount);
				} else {
					BIO_putf(io, "<td class=\"nonday\" style=\"%s\">"
							"&nbsp;<br/>&nbsp;"
							"</td>",
							szStyle);
				}
				szDelete(szStyle);
				day++;
			}

//			BIO_printf(io, "<td style=\"border-left:solid 1px black;border-right:solid 1px black;border-top:none;border-bottom:none\">&nbsp</td>");
		}

		BIO_putf(io, "</tr>");
	}

	BIO_putf(io, "</table>\n");
}

void msc_DisplayMsgLogLine(BIO *io, const char *dir, int bDeep, const char *szFilter, int day, int month, int year)
{
	const char *szDescription = "none";			// These defaults are for the case where there is no notes dir
	const char *szTmp = NULL;
	char *szIP = strdup("-");
	const char *szZInfo = NULL;
	const char *szStderr = NULL;
	const char *szReturn = NULL;
	const char *szErrSize = NULL;
	const char *szCode = NULL;
	const char *szDateTime = NULL;
	int nCode = -2;
	int bInclude;
	int nRow=0;

	note_t *note=note_LoadMessage(dir);
	if (note) {
		szDescription = note_FindEntry(note, 'H');
		if (!szDescription) szDescription="Unknown";
		szZInfo = note_FindEntry(note, 'Z');	// Extra descriptive text to take place of message type
		szStderr = note_FindEntry(note, 'E');	// First line of stderr
		szErrSize = note_FindEntry(note, 'e');
		szReturn = note_FindEntry(note, 'R');
		szTmp = note_FindEntry(note, 'S');
		szCode = note_FindEntry(note, 'C');		// Error code returned from RPC binary
		szDateTime = note_FindEntry(note, 'D');		// Error code returned from RPC binary
		nCode = szCode ? atoi(szCode) : -1;		// -1 should be for things that errored out completely or old logs
	}

	if (szTmp) {
		char *chp;
		szIP=strdup(szTmp);
		for (chp=szIP;*chp;chp++) if (*chp == '|') *chp='\0';		// Don't care about peer port number
	}
	if (!szErrSize) szErrSize = "-";

		if (!szReturn) szReturn="-";

	bInclude=1;
	if (szFilter && *szFilter) {
		bInclude=0;
		if (stristr(szIP, szFilter)) bInclude=1;
		if (stristr(dir, szFilter)) bInclude=1;
		if (stristr(szReturn, szFilter)) bInclude=1;
	}

	if (bInclude) {
		const char *cola, *colb;

		nRow++;
		BIO_putf(io, "<tr ");
		if (nCode == -2) {					// No 'info' file
			cola="#ff77ff";
			colb="#ff55ff";
		} else if (nCode == -1) {			// Info file but with no 'C' entry
			cola="#ffffff";
			colb="#dddddd";
		} else if (nCode == 0) {			// No error - green
			cola="#00ff99";
			colb="#00bb77";
		} else if (nCode < 10) {			// System error - pinky
			cola="#ffbbbb";
			colb="#ff9999";
		} else {
			cola="#bbbbff";					// RPC error - bluey
			colb="#9999ff";
		}
		BIO_putf(io, "bgcolor=\"%s\">", nRow & 1 ? cola : colb);
		BIO_putf(io, "<td nowrap=1>%.2s-%.2s-%.4s", szDateTime+0, szDateTime+3, szDateTime+6);
		BIO_putf(io, "<td>%.2s:%.2s:%.2s", szDateTime+11, szDateTime+14, szDateTime+17);
		BIO_putf(io, "<td nowrap=1><a href=%s>%s</a>", Link("/getinteraction/%s", dir), dir);
		const char *rcv = hprintf(NULL, "msglog/%s/rcv.xml", dir);
		if (!access(rcv, 4)) {
			BIO_putf(io, "<td><a href=%s>Rerun</a></td>", Link("/rerunform/%s", dir));
		} else {
			MyBIO_puts(io, "<td></td>");
		}
		szDelete(rcv);

		// Unfortunately, I chose two different names for the input files so we need to identify which one we mean
		const char *szInputFile=hprintf(NULL, "msglog/%s/rpc-in.xml", dir);
		if (!access(szInputFile, 0)) {		// It's called rpc-in.xml
			BIO_putf(io, "<td nowrap=1><a href=%s>%s</a></td>", Link("/getfile/%s/rpc-in.xml", dir), szZInfo, szZInfo);
		} else {							// It's called rcv.xml
			BIO_putf(io, "<td nowrap=1><a href=%s>%s</a></td>", Link("/getfile/%s/rcv.xml", dir), szZInfo, szZInfo);
		}
		szDelete(szInputFile);

		// Returned
		if (nCode >= 0) {
			BIO_putf(io, "<td nowrap=1>%d (<a href=%s>%s</a>)</td>", nCode, Link("/getfile/%s/rpc-out.xml", dir), szReturn);
		} else {
			BIO_putf(io, "<td nowrap=1><a href=%s>%s</a></td>", Link("/getfile/%s/rpc-out.xml", dir), szReturn);
		}

		// Stderr

		const char *szStderrFile=hprintf(NULL, "msglog/%s/stderr.txt", dir);
		if (!access(szStderrFile, 0)) {		// We have stderr text
			BIO_putf(io, "<td nowrap=1><a href=%s title=\"%s\">%s</a></td>", Link("/getfile/%s/stderr.txt", dir), szStderr, szErrSize);
		} else {
			BIO_putf(io, "<td>");
		}
		szDelete(szStderrFile);
		BIO_putf(io, "<td><a href=%s>%s</a></td>", Link("/status?f=%s", szIP), szIP);
		BIO_putf(io, "<td>%s", szDescription);
	}

	szDelete(szIP);
}

int msc_strrcmp(const char *a, const char *b)
{
	return strcmp(b, a);
}

void mwc_DisplayMessages(BIO *io, int bDeep, const char *szFilter, int fromDay, int fromMonth, int fromYear, int toDay, int toMonth, int toYear)
{
	fromYear %= 100;
	toYear %= 100;
	int day, month, year;

	day=toDay;
	month=toMonth;
	year=toYear;

	while (year*10000+month*100+day >= fromYear*10000+fromMonth*100+fromDay) {
		char dir[100];

		snprintf(dir, sizeof(dir), "%s/%02d/%02d/%02d", szMsgLogDir, year, month, day);

		if (!msc_isDir(dir)) {
			const char *szZip = msc_DayZipFile(day, month, year);

			if (!access(szZip, 4)) {
				Log("Unzipping messages for %d/%02d/%02d", day, month, year);
				BIO_putf(io, "<br><font color=red>Please wait while messages for %d-%.3s-20%02d are retrieved from archive...</font><br>", day, MonthNames[month], year);
				const char *szCommand = hprintf(NULL, "unzip -qqoXK %s '%s/%02d/%02d/%02d/*'",
						szZip, szMsgLogDirLeaf, year, month, day);
				system(szCommand);
				szDelete(szCommand);
			}
		}

		if (msc_isDir(dir)) {
			DIR *fdir = opendir(dir);
			struct dirent *d;
			SSET *names = sset_New();

			while ((d = readdir(fdir))) {
				const char *name = d->d_name;
				char msgdir[100];

				snprintf(msgdir, sizeof(msgdir), "%s/%s", dir, name);

				if (*name != '.' && msc_isDir(msgdir)) {
					sset_Add(names, name);
				}
			}
			closedir(fdir);

			int count = sset_Count(names);
			if (count) {
				sset_Sort(names, msc_strrcmp);
				BIO_putf(io, "<tr><td colspan=14><b><font size=+1 face=verdana>%d message%s on %d%s %s, 20%02d",
					count, count==1?"":"s",
					day, th(day), MonthNames[month], year);
				if (szFilter) {
					BIO_putf(io, " - displaying those containing '%s'", szFilter);
				}

				const char *name;
				while (sset_GetNextEntry(names, &name)) {
					char reldir[100];

					snprintf(reldir, sizeof(reldir), "%02d/%02d/%02d/%s", year, month, day, name);
					msc_DisplayMsgLogLine(io, reldir, bDeep, szFilter, day, month, year);
				}
			}

			sset_Delete(names);
		}

		day--;
		if (day < 1) {
			day=31;
			month--;
			if (month < 1) {
				month=12;
				year--;
			}
		}
	}
}

void TableMessageLog(BIO *io, int bDeep, const char *szFilter, int fromDay, int fromMonth, int fromYear, int toDay, int toMonth, int toYear)
{
	if (!toYear && !toMonth && !toDay) {
		time_t now=time(NULL);
		struct tm *tm=gmtime(&now);
		toYear=tm->tm_year % 100;
		toMonth=tm->tm_mon+1;
		toDay=tm->tm_mday;
	}

	if (fromDay == 0) fromDay=1;
	if (fromMonth == 0) fromMonth=1;
	if (toDay == 0) toDay=31;
	if (toMonth == 0) toMonth=12;

	msc_DisplayCalendar(io, fromDay, fromMonth, fromYear, bDeep, szFilter);

	if (fromDay == toDay && fromMonth == toMonth && fromYear == toYear) {
		BIO_putf(io, "Displaying messages processed on %2d/%02d/20%02d", fromDay, fromMonth, fromYear);
	} else {
		BIO_putf(io, "Displaying messages processed between %2d/%02d/20%02d and %2d/%02d/20%02d<hr>", fromDay, fromMonth, fromYear, toDay, toMonth, toYear);
	}

	MyBIO_puts(io, "<style>"
						"* {font-family: calibri, verdana, arial} "
						"td {font-size:9pt} "
						"td.TD_MSGID {font-family: courier; }"
						"td.TD_Gen {background-color: #ffddff; text-align: center;}"
						"td.TD_Unk {background-color: #ffffff; text-align: center;}"
						"td.TD_eTP {background-color: #ffffdd; text-align: center;}"
						"td.TD_PDS {background-color: #ffdddd; text-align: center;}"
						"td.TD_CaB {background-color: #ddffdd; text-align: center;}"
						"td.TD_RPC {background-color: #ddffff; text-align: center;}"
					"</style>\n");
	MyBIO_puts(io, "<table cellpadding=\"3\" cellspacing=\"0\" border=\"0\" bgcolor=#ddddff width=100%>\n");

	MyBIO_puts(io, "<tr bgcolor=\"#aaaaee\">");
	MyBIO_puts(io, "<th>Date</th>");
	MyBIO_puts(io, "<th>UTC</th>");
	MyBIO_puts(io, "<th>ID</th>");
//	MyBIO_puts(io, "<th>Env</th>");
	MyBIO_puts(io, "<th>Rerun</th>");
	MyBIO_puts(io, "<th>Function</th>");
	MyBIO_puts(io, "<th>Returned</th>");
	MyBIO_puts(io, "<th>Stderr</th>");
	MyBIO_puts(io, "<th>IP</th>");
	MyBIO_puts(io, "<th>Source</th>");
	MyBIO_puts(io, "</tr>\n");

	mwc_DisplayMessages(io, bDeep, szFilter, fromDay, fromMonth, fromYear, toDay, toMonth, toYear);

	BIO_putf(io, "</table>");
}

static int MyBIO_getc(BIO *io)
// Gets a single character (0..255) or EOF
{
	char c;
	int nGot;
	int nResult;

	static BIO *lastio = NULL;
	static int nIndex = 1;
	const char *szNoteDir;
	FILE *fp;
	FILE *fpnote;
	const char *szNoteFile;
	const char *szBase;

//	Log("In getc");
	nGot = BIO_read(io, &c, 1);
	mi_BytesIn++;
	nResult = (nGot == 1) ? (unsigned char)c : EOF;
//Log("Done getc read nGot=%d, nResult = %d, c='%c', errno=%d, flags=%x", nGot, nResult, (c>=20&&c<=126)?c:'-', errno, io->flags);

	// Everything from here on is logging...!

	if (!_bNoteInhibit) {
		szNoteDir = msc_NoteDir();
		fp=fopen("/tmp/in.dat","a");
		fpnote = NULL;
		szNoteFile = NULL;
		szBase = NULL;

		if (szNoteDir) {						// We're keeping notes
			szBase=hprintf(NULL, "rawin-%d", nIndex);
			szNoteFile = hprintf(NULL, "%s/%s.txt", szNoteDir, szBase);
			fpnote=fopen(szNoteFile, "a");
			szDelete(szNoteFile);
		}
		if (lastio != io) {						// Changed to a new thrower
			if (fp) fprintf(fp, "\n----------------------------------------------------------- NEW PORT (%p != %p)\n", lastio, io);
			if (fpnote) {
				const char *szFilename = hprintf(NULL, "%s.txt", szBase);
				FileDescription(szFilename, "Data received over wire");
				szDelete(szFilename);

				Note("M|%s.%s|%s|%d", szBase, "txt", "Data received over wire", -1);
			}
			lastio=io;
		}
		if (nResult != EOF) {					// We read some stuff
			if (fp) fputc(c, fp);
			if (fpnote) fputc(c, fpnote);
		} else {								// End of file
			if (fp) fputs("\n----------------------------------------------------------- EOF\n", fp);
			nIndex++;
			if (nIndex > 9) {
				Log("Runaway attachment problem - quitting");
				Exit(0);
			}
		}
		if (fp) fclose(fp);
		if (szNoteDir) {
			szDelete(szBase);
			if (fpnote) fclose(fpnote);
		}
	}

	return nResult;
}

static int MyBIO_gets(BIO *io, char *buf, int nLen)
{
	int nGot = 0;

	while (nLen > 1) {
		int c=MyBIO_getc(io);
		if (c=='\n' || c==EOF) break;
		*buf++=c;
		nLen--;
		nGot++;
	}
	*buf='\0';
	return nGot;
}

BIO *mimeio = NULL;

static int getiochar()
{
	int ch = MyBIO_getc(mimeio);
	return ch;
//	return MyBIO_getc(mimeio);
}

static MIME *GetIOMIME(BIO *io)
{
	mimeio=io;

	return mime_ReadFn(getiochar);
}

static MIME *GetIOMIMEHeader(BIO *io)
{
	mimeio=io;
	MIME *mime = mime_ReadHeaderFn(getiochar);
//Log("%p = mime_ReadHeaderFn()", mime);
	return mime;
}

static const char *kmg(size_t size)
{
	static char buf[10];
	double dbl=(double)size;
	char unit='B';

	if (dbl > 1023) { dbl /= 1024; unit='K'; }
	if (dbl > 1023) { dbl /= 1024; unit='M'; }
	if (dbl > 1023) { dbl /= 1024; unit='G'; }

	if (dbl < 10.0 && unit != 'B') {
		snprintf(buf, sizeof(buf), "%.1f%c", dbl, unit);
	} else {
		snprintf(buf, sizeof(buf), "%.0f%c", dbl, unit);
	}

	return buf;
}

static void TableLine(BIO *io, const char *title, const char *value, int bBold)
{
	if (!value) value="Unknown";

	BIO_putf(io, "<tr><td bgcolor=#ffaa88>%s</td><td bgcolor=#ffffaa>", title);
	if (bBold) BIO_putf(io, "<b>");
	BIO_putf(io, "%s", value);
	if (bBold) BIO_putf(io, "</b>");
	BIO_putf(io, "</td></tr>\r\n", value);
}

static void TableNotes(BIO *io, note_t *note, const char *szDir)
{
	const char *szInfoFile = hprintf(NULL, "%s/%s/info.map", szMsgLogDir, szDir);
	SSMAP *info = ssmap_GetFile(szInfoFile);

	if (info) {
		BIO_putf(io, "<table bgcolor=#000077 bordercolor=blue border=1 cellspacing=1 width=100%>\r\n");
		BIO_putf(io, "<tr bgcolor=#aa88ff><th colspan=2><font size=5>Details</font>");
		TableLine(io, "Function", ssmap_GetValue(info, "API-FUNCTION"), 1);
		TableLine(io, "Date-Time", ssmap_GetValue(info, "TIME-START"), 0);
		TableLine(io, "Directory", szDir, 0);
		TableLine(io, "Session", ssmap_GetValue(info, "MI-SESSION"), 0);
		TableLine(io, "Caller", ssmap_GetValue(info, "MI-CALLER-ID"), 0);
		TableLine(io, "Status", ssmap_GetValue(info, "MI-STATUS"), 0);
		TableLine(io, "Calling method", ssmap_GetValue(info, "CLIENT-TYPE"), 0);
		BIO_putf(io, "</table>\r\n");
	} else {
		BIO_putf(io, "<table bgcolor=#000077 bordercolor=blue border=1 cellspacing=1 width=100%>\r\n");
		BIO_putf(io, "<tr bgcolor=#aa88ff><th colspan=2><font size=5>No details available</font>");
		BIO_putf(io, "</table>\r\n");
	}

	ssmap_Delete(info);
	szDelete(szInfoFile);
}

static void TableFiles(BIO *io, const char *szDir)
{
	const char *szMapFile = hprintf(NULL, "%s/%s/file.map", szMsgLogDir, szDir);
	SSMAP *map = ssmap_GetFile(szMapFile);

	if (map) {
		BIO_putf(io, "<table bgcolor=#000077 bordercolor=blue border=1 cellspacing=1 width=100%>\r\n");
		BIO_putf(io, "<tr bgcolor=#aa88ff><th colspan=3><font size=5>Files</font></th></tr>");
		BIO_putf(io, "<tr bgcolor=#bb99ff><th>Type<th>Content<th>Size");
		ssmap_Sort(map, strcmp);
		const char *szFile;
		const char *szDescription;

		while (ssmap_GetNextEntry(map, &szFile, &szDescription)) {
			struct stat st;
			const char *szFilename = hprintf(NULL, "%s/%s/%s", szMsgLogDir, szDir, szFile);
			const char *szExt = strchr(szFile, '.');
			szExt = szExt ? szExt+1 : "";

			int err = stat(szFilename, &st);
			const char *size="?";
			if (!err) {
				size = kmg(st.st_size);
			}

			BIO_putf(io, "<tr bgcolor=#ffccaa>");
			BIO_putf(io, "<td>%s</td>", szExt);
			BIO_putf(io, "<td><a href=%s>%s</a></td>", Link("/getfile/%s/%s", szDir, szFile), szDescription);
			BIO_putf(io, "<td align=\"right\">%s</td>", size);
			BIO_putf(io, "</tr>");

			szDelete(szFilename);
		}
		BIO_putf(io, "</table>\r\n");
	} else {
		BIO_putf(io, "<table bgcolor=#000077 bordercolor=blue border=1 cellspacing=1 width=100%>\r\n");
		BIO_putf(io, "<tr bgcolor=#aa88ff><th colspan=2><font size=5>No files stored</font>");
		BIO_putf(io, "</table>\r\n");
	}

	ssmap_Delete(map);
	szDelete(szMapFile);
}

static void TableNoteLogs(BIO *io, note_t *note, const char *szDir)
{
	const char *szLog = hprintf(NULL, "%s/%s/log", szMsgLogDir, szDir);
	FILE *fp=fopen(szLog, "r");

	if (fp) {
		const char *szLine;

		BIO_putf(io, "<table border=1 cellspacing=0 width=100%>\r\n");
		BIO_putf(io, "<tr bgcolor=#44aaff><th>Time<th>Event\r\n");
		while ((szLine=ReadLine(fp))) {
			int bError = !!stristr(szLine, "error");
			char *szLineCopy = strdup(szLine);
			char *szTime=strtok(szLineCopy, " ");
			char *szEvent=strtok(NULL, "");
			BIO_putf(io, "<tr><td>%s</td>", szTime);
			BIO_putf(io, "<td>");
			if (bError) BIO_putf(io, "<font color=red>");
			BIO_putf(io, "%s", szEvent);
			if (bError) BIO_putf(io, "</font>");
			BIO_putf(io, "</td>");
			szDelete(szLineCopy);
		}
		BIO_putf(io, "</table>\r\n");
		fclose(fp);
	} else {
		BIO_putf(io, "<table><tr><td bgcolor=red>Cannot open %s</table>", szLog);
	}

	szDelete(szLog);
}

static void SendHtmlHeader(BIO *io)
{
// $mtorange="#f57023"; $mtblue="#2b509a";
	time_t now = time(NULL);
	struct tm *tm=gmtime(&now);

	BIO_putf(io, "<META HTTP-EQUIV='Pragma' CONTENT='no-cache'>");
	BIO_putf(io, "<hr>");
	BIO_putf(io, "<table border=0 width=100%% bgcolor=#f57023>");
	BIO_putf(io, "<tr>");
	BIO_putf(io, "<td rowspan=2 colspan=2 align=center><font size=6 color=white>SPIDER v" VERSION " - RPC Server (" OS ")</font>");
	if (szEnvironment) {
		BIO_putf(io, "<br><font size=3>(environment = %s)</font>", szEnvironment);
	}
	BIO_putf(io, "<td bgcolor=#2b509a><font size=3 color=white> &nbsp; %s</font>", szHostname);
	BIO_putf(io, "<tr><td bgcolor=#2b509a><font size=3 color=white> &nbsp; %s</font>", _szIncomingIp);
	BIO_putf(io, "<tr bgcolor=#b8541b><td>");
	BIO_putf(io, "| <a href=%s><font color=white>Home</font></a>", Link("/"));
	BIO_putf(io, "| <a href=%s><font color=white>Messages</font></a> ", Link("/status"));
	BIO_putf(io, "| <a href=\"https://%s\"><font color=white>MMTS</font></a> ", szHostname);
	BIO_putf(io, "| <a href=%s><font color=white>Test</font></a>", Link("/mtrpc"));
	BIO_putf(io, "| <a href=%s><font color=white>APIs</font></a>", Link("/apis"));
	BIO_putf(io, "|");
	BIO_putf(io, "<td><font color=white> &nbsp; UTC is %d-%02d-%04d %02d:%02d:%02d</font>",
		tm->tm_mday, tm->tm_mon+1, tm->tm_year+1900,
		tm->tm_hour, tm->tm_min, tm->tm_sec);
	BIO_putf(io, "</table>");
	BIO_putf(io, "<hr>");
}

const char *HttpResponseText(int code)
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
		case 500: s="Internal Server Error"; break;
		case 501: s="Not Implemented"; break;
		case 502: s="Bad gateway"; break;
		case 503: s="Service Unavailable"; break;
		case 504: s="Gateway Timeout"; break;
		case 505: s="HTTP Version Not Supported"; break;
		default:  s="UNKNOWN CODE"; break;
	}

	return s;
}

static const char *HttpHeader(nCode)
{
	const char *result = NULL;

	szDelete(result);

	mi_status = nCode;
	result = hprintf(NULL, "HTTP/1.1 %d %s\r\n", nCode, HttpResponseText(nCode));

	return result;
}

static void SendHttpHeader(BIO *io, int nCode, const char *szContentType, int nContentLength)
{
	int day, mon, year, hour, min, sec;
	const char szMon[]="JanFebMarAprMayJunJulAugSepOctNovDec";
	time_t now = time(NULL);
	struct tm* tm = localtime(&now);
	char timebuf[50];

	strftime(timebuf, sizeof(timebuf), "%a, %d %b %y %T %z", tm);

	for (mon=0;mon<12;mon++) if (!memcmp(szMon+mon*3, __DATE__, 3)) break;
	mon++;
	day=atoi(__DATE__+4);
	year=atoi(__DATE__+7);
	hour=atoi(__TIME__);
	min=atoi(__TIME__+3);
	sec=atoi(__TIME__+6);

	if (!szContentType) szContentType="text/html";

	BIO_putf(io, HttpHeader(nCode));
	MyBIO_puts(io,"Server: Microtest Spider " VERSION "\r\n");
	BIO_putf(io, "Date: %s\r\n", timebuf);
	BIO_putf(io, "Host: %s:%d\r\n", szHostname, _nIncomingPort);
	MyBIO_puts(io,"X-SPIDER-VERSION: " VERSION "\r\n");
	BIO_putf(io, "X-SPIDER-COMPILED: %04d%02d%02d%02d%02d%02d\r\n", year, mon, day, hour, min, sec);
	BIO_putf(io, "Content-type: %s\r\n", szContentType);
	if (nContentLength >= 0) BIO_putf(io, "Content-length: %d\r\n", nContentLength);
	MyBIO_puts(io, "\r\n");
}

static int SendHttpContent(BIO *io, int nCode, const char *szContentType, const char *szContent)
{
	SendHttpHeader(io, nCode, szContentType, strlen(szContent));
	BIO_puts(io, szContent);
	(void)BIO_flush(io);

	return 0;
}

static void FatalHttp(BIO *io, int nCode, const char *szFmt, ...)
// Fatal error, but tell the thing hanging off the io channel
{
	va_list ap;
	char buf[1000];

	va_start(ap, szFmt);
	vsnprintf(buf, sizeof(buf), szFmt, ap);
	va_end(ap);

	SendHttpContent(io, nCode, "text/plain", buf);

	Fatal("%s", buf);

	Exit(99);
}

static int ParseURI(const char *szURI, const char ***ppszName, const char ***ppszValue, char bEncoded)
// Parses a URI of the form a=b+3&c=d%3b-%3e to produce the following:
// *ppszName = [ "a", "c", NULL ]
// *ppszValue = [ "b 3", "d<->", NULL ] or
// *ppszValue = [ "b+3", "d3&c=d%3b-%3e", NULL ] if !bEncoded (i.e. bEncoded indicates values are encoded
// I.e. Name and Value are two arrays of names and values ending in NULL pointers (like argv)
// Passing in NULL for szURI, ppszName or ppszValue is safe if the values aren't wanted.
// Returns the number of parameters found
{
	int nParams=0;
	const char **pszName = NEW(const char*, 1);			// Two vectors, just large enough to hold the terminating
	const char **pszValue = NEW(const char*, 1);		// NULL pointers that are added at the end

	if (szURI) {
		char *pA;										// Ampersands
		char *pEq;										// Equalss
		char *szParams;									// Copy of the parameters

		szParams=strdup(szURI);							// Buffer we can muck up
		char *szTmpParams = szParams;					// Copy the pointer so we can free it

		for (;;) {
			pA = strchr(szParams, '&');					// Find separator
			if (pA) *pA='\0';

			pEq=strchr(szParams, '=');
			if (pEq) {
				*pEq='\0';
				RENEW(pszName, const char *, nParams+2);
				RENEW(pszValue, const char *, nParams+2);
				pszName[nParams]=strdup(szParams);
				if (bEncoded) UriDecode(pEq+1);
				pszValue[nParams]=strdup(pEq+1);
				nParams++;
			}
			if (!pA) break;
			szParams=pA+1;
		}
		szDelete(szTmpParams);
	}

	pszName[nParams]=NULL;
	pszValue[nParams]=NULL;

	if (ppszName) *ppszName=pszName; else DeleteArgv(pszName);		// Assign back names and values if passed pointers
	if (ppszValue) *ppszValue=pszValue; else DeleteArgv(pszValue);	// weren't NULL, otherwise free them up

	return nParams;
}

static int ParseAddress(const char *szURI, const char **pszAddr, const char ***ppszName, const char ***ppszValue)
// Parses a URI of the form /whatever?a=b+3&c=d%3b-%3e to produce the following:
// *pszAddr = "/whatever"
// *ppszName = [ "a", "c", NULL ]
// *ppszValue = [ "b 3", "d<->", NULL ]
// I.e. Name and Value are two arrays of names and values ending in NULL pointers (like argv)
// Addr is the tidied up URI (it is safe to call with (szUri, &szUri, ...))
// Passing in NULL for pszAddr, ppszName or ppszValue is safe if the values aren't wanted.
{
	int nParams=0;
	const char *chp = NULL;
	const char *szAddr;

	if (szURI) chp=strchr(szURI, '?');						// Look for parameters...
	if (chp) {												// /uri?a=b&c=d
		szAddr=strnappend(NULL, szURI, chp-szURI);			// szAddr is the portion before the '?'
		nParams = ParseURI(chp+1, ppszName, ppszValue, 1);
	} else {
		szAddr=szURI ? strdup(szURI) : NULL;
		nParams = ParseURI(NULL, ppszName, ppszValue, 1);	// Basically just sets Name and Value to empty arrays
	}

	if (pszAddr) *pszAddr=szAddr; else szDelete(szAddr);

	return nParams;
}

static const char *HtmlEscape(const char *szText)
// szText MUST be on the heap.
// The return value is a separate copy of the text passed, with '<' and '>' expanded.
// The parameter passed IS NOT deleted by this function.
{
	const char *szEscaped = strdup(szText);

	szEscaped=strsubst(szEscaped, "<", "&lt;");
	szEscaped=strsubst(szEscaped, ">", "&gt;");

	return szEscaped;
}

static void SendApiList(BIO *io)
{
	const char **list = GetAllApis();
	const char **l=list;

	BIO_putf(io, "<style>"
						"* {font-family: calibri, verdana, arial} "
						"td {font-size:11pt} "
						"td.TD_MSGID {font-family: courier; }"
						"td.api {background-color: #bbbbcc; text-align: left;}"
						"td.version {background-color: #aaaadd; text-align: left;}"
						"td.descr {background-color: #aaaadd; text-align: left;}"
					"</style>\n");
	BIO_putf(io, "<table border=\"0\">\n");
	BIO_putf(io, "<tr>");
	BIO_putf(io, "<th>API</th>");
	BIO_putf(io, "<th>Version</th>");
	BIO_putf(io, "<th>Description</th>");
	BIO_putf(io, "</tr>\n");
	while (*l) {

		BIO_putf(io, "<tr>");
		rogxml *adl = api_Adl(*l);
		const char *szDescription = rogxml_GetValue(rogxml_FindByPath(adl, "/*/Description"));
		const char *szVersion = api_Version(*l);
		BIO_putf(io, "<td class=\"api\"><a href=%s>%s</a></td>", Link("/mtrpc?api=%s", *l), *l);
		BIO_putf(io, "<td class=\"version\">%s</td>", szVersion ? szVersion : "-");
		BIO_putf(io, "<td class=\"descr\">%s</td>", szDescription ? szDescription : "");
		l++;
		BIO_putf(io, "</tr>\n");
		rogxml_Delete(adl);
	}
}

static void RpcEntryForm(BIO *io, char bDebug, const char *szSession, const char *szXml, const char *szApi)
{
	const char *szDescription = NULL;
	const char *szInfo = NULL;

	if (!szXml || !*szXml) {
		if (szApi) {
			rogxml *rx = api_Adl(szApi);
			rogxml *rxXml = rogxml_NewElement(NULL, "SPIDER-RPC");
			rogxml_AddAttr(rxXml, "_function", szApi);
			if (szSession) rogxml_AddAttr(rxXml, "RPC_SESSIONID",szSession);

			szDescription = rogxml_GetValue(rogxml_FindByPath(rx, "/*/Description"));
			const char *szVersion = rogxml_GetValue(rogxml_FindByPath(rx, "/*/Version"));
			rogxml *rxCommandLine = rogxml_FindByPath(rx, "/*/CommandLine");

			szInfo = hprintf(szInfo, "<font size=\"4\" face=\"verdana\" color=\"#0088dd\">API: <b>%s</b> (version %s)</font><br/>"
					"%s<br/>", szApi, szVersion, szDescription);
			szInfo = hprintf(szInfo, "<table border>\n");
			szInfo = hprintf(szInfo, "<tr><th>Parameter</th><th>Type</th><th>Description</th></tr>\n");
			rogxml *rxParam;
			for (rxParam=rogxml_FindChild(rxCommandLine,"Param");rxParam;rxParam=rogxml_FindSibling(rxParam,"Param")) {
				const char *szName = rogxml_GetAttr(rxParam, "name");
				const char *szType = rogxml_GetAttr(rxParam, "type");
				const char *szParamDescr = rogxml_GetValue(rogxml_FindChild(rxParam, "Description"));
				const char *szValue = "?";
				if (!strnicmp(szType, "Int", 3)) szValue="123";
				else if (!stricmp(szType, "String")) szValue="str";

				rogxml_AddAttr(rxXml, szName, szValue);

				szInfo = hprintf(szInfo, "<tr><td>%s</td><td>%s</td><td>%s</td></tr>\n", szName, szType, szParamDescr);
			}
			szInfo = hprintf(szInfo, "</table>\n");
			szXml = rogxml_ToText(rxXml);
		} else {
			szXml=strdup("<SPIDER-RPC _function=\"\"/>");
		}
	} else {
		szXml = strdup(szXml);
	}

	const char *szEscaped;
#if 0
	szEscaped = HtmlEscape(szXml);
	szEscaped = strsubst(szEscaped, " _function", "\n  _function");
	szEscaped = strsubst(szEscaped, "\" ", "\"\n  ");
	szEscaped = strsubst(szEscaped, "/&gt;", "\n/&gt;");
#else
	rogxml *rx = rogxml_FromText(szXml);
	if (rx) {
		const char *szTmp = rogxml_ToText(rx);
		szEscaped = HtmlEscape(szTmp);
		szEscaped = strsubst(szEscaped, " _function", "\n  _function");
		szEscaped = strsubst(szEscaped, "\" ", "\"\n  ");
		szEscaped = strsubst(szEscaped, "/&gt;", "\n/&gt;");
		szDelete(szTmp);
		rogxml_Delete(rx);
	} else {
		szEscaped = HtmlEscape(szXml);
	}
#endif

	BIO_putf(io, "<table><tr>\n");
	BIO_putf(io, "<form method=POST action=\"/mtrpc\">\n");
	BIO_putf(io, "<input type=hidden name=browser value=1>\n");
	BIO_putf(io, "<tr>");
	BIO_putf(io, "<td valign=top><input type=submit value=Execute></td>\n");
	BIO_putf(io, "<td rowspan=2><textarea name=xml cols=80 rows=20>%s</textarea></td>", szEscaped);
	BIO_putf(io, "</tr>");
	BIO_putf(io, "<tr>");
	BIO_putf(io, "<td valign=top>Debug <input type=checkbox ");
	if (bDebug) BIO_putf(io, "checked ");
	BIO_putf(io, "name=debug></td>\n");
	BIO_putf(io, "</tr>");
	BIO_putf(io, "</form>\n");
	BIO_putf(io, "</table>");

	if (szInfo) {
		BIO_putf(io, szInfo);
	}

	szDelete(szEscaped);
}

static void ShowPorts(BIO *io, int nProtocol)
{
	int showing=0;
	int i;

	for (i=0;i<nNetworkPorts;i++) {
		if (NetworkPort[i].nProtocol == nProtocol) {
			BIO_puts(io, "<tr>");
			if (!showing) {
				BIO_putf(io, "<td>%s ports</td><td bgcolor=#eeeeff>", ProtocolName(nProtocol));
				showing=1;
			} else {
				BIO_putf(io, "<td></td><td bgcolor=#eeeeff>");
			}
			if (nProtocol == SRC_TLS || nProtocol == SRC_PLAIN) {
				const char *szHref = hprintf(NULL, "%s://%s:%d", nProtocol == SRC_TLS ? "https": "http", _szIncomingHost, NetworkPort[i].nPort);
				BIO_putf(io, "<a href=\"%s\">%d</a> (%d)", szHref, NetworkPort[i].nPort, NetworkPort[i].nCount);
				szDelete(szHref);
			} else {
				BIO_putf(io, "%d (%d)", NetworkPort[i].nPort, NetworkPort[i].nCount);
			}
			BIO_puts(io, "</td></tr>");
		}
	}
}

void SetBaseDir(const char *szNewBaseDir)
// Given that the current directory is the base directory, get configuration.
// Fatal error if we cannot chdir to the directory
{
	szDelete(szBaseDir);
	szBaseDir = strdup(szNewBaseDir);

	if (chdir(szBaseDir)) Fatal("Cannot chdir to base directory (%s)", szBaseDir);

	szDelete(szEtcDir); szEtcDir=hprintf(NULL, "%s/%s", szBaseDir, szEtcDirLeaf); rog_MkDir(szEtcDir);
	szDelete(szLogDir); szLogDir=hprintf(NULL, "%s/%s", szBaseDir, szLogDirLeaf); rog_MkDir(szLogDir);
	szDelete(szOutDir); szOutDir=hprintf(NULL, "%s/%s", szBaseDir, szOutDirLeaf); rog_MkDir(szOutDir);
	szDelete(szRpcDir); szRpcDir=hprintf(NULL, "%s/%s", szBaseDir, szRpcDirLeaf); rog_MkDir(szRpcDir);
	szDelete(szTmpDir); szTmpDir=hprintf(NULL, "%s/%s", szBaseDir, szTmpDirLeaf); rog_MkDir(szTmpDir);
	szDelete(szContractDir); szContractDir=hprintf(NULL, "%s/%s", szEtcDir, szContractDirLeaf); rog_MkDir(szContractDir);
	szDelete(szRpcDataDir); szRpcDataDir=hprintf(NULL, "%s/%s", szBaseDir, szRpcDataDirLeaf); rog_MkDir(szRpcDataDir);
	szDelete(szMsgLogDir); szMsgLogDir=hprintf(NULL, "%s/%s", szBaseDir, szMsgLogDirLeaf); rog_MkDir(szMsgLogDir);

	szDelete(szLogFile); szLogFile=hprintf(NULL, "%s/%s.log", szLogDir, szMyName); chmod(szLogFile, 0666);
	szDelete(szMiFile); szMiFile=hprintf(NULL, "%s/%s.mi", szLogDir, szMyName); chmod(szMiFile, 0666);

	if (bIsDaemon) {								// Don't let environments have environments...
		szDelete(szEnvDir); szEnvDir=hprintf(NULL, "%s/%s", szBaseDir, szEnvDirLeaf); rog_MkDir(szEnvDir);
	}

	szConfigFile = hprintf(NULL, "%s/%s.conf", szEtcDir, szMyName);
	FILE *fp = fopen(szConfigFile, "a");		// Ensure config file exists
	if (fp) fclose(fp);
}

static int SetEnvironment(const char *szEnv)
// Tries to set the environment to that given
// Returns	1	Ok, done it
//			0	No such environment
{
	int result = 0;

	if (szEnv && *szEnv) {
		const char *szNewBaseDir = hprintf(NULL, "%s/%s", szEnvDir, szEnv);
		if (IsDir(szNewBaseDir)) {
			SetBaseDir(szNewBaseDir);

			szDelete(szEnvironment);
			szEnvironment = strdup(szEnv);

			result = 1;
		}
		szDelete(szNewBaseDir);
	}

	return result;
}

static int DealWithGET(BIO *io, const char *szURI)
{
	MIME *mime=GetIOMIMEHeader(io);
	const char *szAddr = NULL;						// Address part of szURI (always starts with '/')
	const char **szNames = NULL;					// Vector of names of parameters
	const char **szValues = NULL;					// Vector of values of parameters
	int nParams=0;									// Number of parameters

	NoteInhibit(1);

	if (szURI && *szURI == '/') {
		const char *chp = strchr(szURI+1, '/');		// Check if we're referencing an environment
		const char *szEnv = chp ? strnappend(NULL, szURI+1, chp-szURI-1) : strdup(szURI+1);
		if (SetEnvironment(szEnv)) {
			szURI += strlen(szEnv)+1;
		}
		szDelete(szEnv);
	}

	const char *szUserAgent = mime_GetHeader(mime, "User-Agent");
	if (szUserAgent) {
		char *copy = strdup(szUserAgent);
		char *chp;

		if ((chp=stristr(copy, "MSIE"))) {			// Note there is some importance in the order of these tests
			mi_caller=hprintf(NULL, "IE:%s", strtok(chp+5, "; "));
		} else if ((chp=stristr(copy, "Firefox/"))) {
			mi_caller=hprintf(NULL, "Firefox:%s", strtok(chp+8, "; "));
		} else if ((chp=stristr(copy, "Chrome/"))) {
			mi_caller=hprintf(NULL, "Chrome:%s", strtok(chp+7, "; "));
		} else if ((chp=stristr(copy, "Safari/"))) {
			mi_caller=hprintf(NULL, "Safari:%s", strtok(chp+7, "; "));
		}
		szDelete(copy);
	}

	mi_function=strdup(szURI);
	if (mi_caller) {
		Log("HTTP GET %s (%s)", szURI, mi_caller);
	} else if (g_OrganisationProduct) {
		Log("HTTP GET %s [%s:%s]", szURI, g_OrganisationProduct);
	} else {
		Log("HTTP GET %s", szURI);
	}
	nParams = ParseAddress(szURI, &szAddr, &szNames, &szValues);
	SetIncomingHost(mime);

	if (!strcasecmp(szAddr, "/")) {
		const char *szMime;
//		X509 *certPeer = SSL_get_peer_certificate(ssl);
		int nSecs=time(NULL)-_nStartTime;
		int nHours=nSecs/3600;
		int nMins=nSecs/60%60;

		nSecs %= 60;

		SendHttpHeader(io, 200, NULL, -1);
		SendHtmlHeader(io);

		MyBIO_puts(io, "<table border=0 bgcolor=#ddddff>");
		BIO_putf(io, "<tr><td width=150>Uptime<td bgcolor=#eeeeff>%d:%02d:%02d", nHours, nMins, nSecs);
		BIO_putf(io, "<tr><td width=150>Host<td bgcolor=#eeeeff>%s", _szIncomingHost);
		BIO_putf(io, "<tr><td width=150>Server<td bgcolor=#eeeeff>%s on %s", szHostname, _szIncomingIp);
		BIO_putf(io, "<tr><td>Client (you)<td bgcolor=#eeeeff>%s:%d", PeerIp(), PeerPort());
		BIO_putf(io, "<tr><td>Server parent process<td bgcolor=#eeeeff>%d", getppid());
		BIO_putf(io, "<tr><td>Server child process<td bgcolor=#eeeeff>%d", getpid());
		BIO_putf(io, "<tr><td>Compiled<td bgcolor=#eeeeff>%s at %s", __DATE__, __TIME__);
		BIO_putf(io, "<tr><td>Files from drop directory<td bgcolor=#eeeeff>%d", _nDroppedCount);
		BIO_putf(io, "<tr><td>Total connections processed<td bgcolor=#eeeeff>%d (<a href=/status>click for details</a>)", _nTotalConnections);
		BIO_putf(io, "<tr><td>RPC Directory<td bgcolor=#eeeeff>%s", szRpcDir);
		ShowPorts(io, SRC_IP);
		ShowPorts(io, SRC_PLAIN);
		ShowPorts(io, SRC_TLS);
		MyBIO_puts(io, "</table>");

		MyBIO_puts(io, "<hr>");
		MyBIO_puts(io, "Your request was:<xmp>\r\n");
		szMime=mime_RenderHeap(mime);
		MyBIO_puts(io, szMime);
		MyBIO_puts(io, "</xmp>\r\n");
		szDelete(szMime);
	} else if (!strncasecmp(szAddr, "/rerunform", 10)) {
		const char *szFilename=hprintf(NULL, "%s%s", szMsgLogDir, szAddr+10);
		if (strstr(szFilename, "/../")) {
			SendHttpHeader(io, 403, NULL, -1);
			BIO_putf(io, "You can't traverse the tree - sorry");
			return 0;
		}

		if (IsDir(szFilename)) szFilename = hprintf(szFilename, "/rcv.xml");
		char buf[32768];
		FILE *fp=fopen(szFilename, "r");
		int got=0;
		if (fp) got = fread(buf, 1, sizeof(buf), fp);
		if (fp) fclose(fp);

		if (!got) {
			SendHttpHeader(io, 404, NULL, -1);
			BIO_putf(io, "I can't read anything to resend from the file (%s)", szFilename);
			return 0;
		}

		SendHttpHeader(io, 200, NULL, -1);
		SendHtmlHeader(io);
		RpcEntryForm(io, 0, "", buf, "");
	} else if (!strcasecmp(szAddr, "/favicon.ico")) {
		const char *szIconFile = hprintf(NULL, "%s/favicon.ico", szEtcDir);
		FILE *fp=fopen(szIconFile, "r");

		if (fp) {
			char buf[1024];
			int got;

			fseek(fp, 0, 2);
			SendHttpHeader(io, 200, "image/x-icon", ftell(fp));
			fseek(fp, 0, 0);
			while ((got=fread(buf, 1, sizeof(buf), fp)))
				MyBIO_write(io, buf, got);
			fclose(fp);
			Log("FAVICON %s", szIconFile);
		}
		szDelete(szIconFile);
	} else if (!strncasecmp(szAddr, "/mtrpc", 6)) {
		int i;
		const char *szApi = NULL;

		SendHttpHeader(io, 200, NULL, -1);
		SendHtmlHeader(io);
		for (i=0;i<nParams;i++) {
			if (!stricmp(szNames[i], "api")) szApi=szValues[i];
		}

		if (szApi) {
			const char *szXml = hprintf(NULL, "<SPIDER-RPC _function=\"%s\"/>", szApi);

			RpcEntryForm(io, 0, "", "", szApi);

			szDelete(szXml);
		} else {
			RpcEntryForm(io, 0, "", "", "");
		}
	} else if (!strncasecmp(szAddr, "/apis", 6)) {
		SendHttpHeader(io, 200, NULL, -1);
		SendHtmlHeader(io);
		SendApiList(io);
	} else if (!strcasecmp(szAddr, "/status") || !strncasecmp(szAddr, "/status/", 8)) {
		const char *szFilter=NULL;
		int bDeep = 0;
		int i;
		int y=0, m=0, d=0;

		if (szAddr[7]) {											// We have date components
			char *szCopyAddr = strdup(szAddr);
			const char *sy=NULL, *sm=NULL, *sd=NULL;
			sy = strtok(szCopyAddr+8, "/");
			if (sy) {
				sm = strtok(NULL, "/");
				if (sm) {
					sd = strtok(NULL, "/");
				}
			}
			y = sy ? atoi(sy) % 100 : 0;
			m = sm ? atoi(sm) : 0;
			d = sd ? atoi(sd) : 0;
			szDelete(szCopyAddr);
		} else {
			time_t now=time(NULL);
			struct tm *tm = gmtime(&now);
			d=tm->tm_mday;
			m=tm->tm_mon+1;
			y=tm->tm_year+1900;
		}
		y%=100;

		SendHttpHeader(io, 200, NULL, -1);
		SendHtmlHeader(io);
		int nPeriod=1;

		for (i=0;i<nParams;i++) {
			if (!stricmp(szNames[i], "f")) szFilter=szValues[i];
			if (!stricmp(szNames[i], "deep")) bDeep=1;
			if (!stricmp(szNames[i], "period")) nPeriod=atoi(szValues[i]);
//BIO_putf(io, "('%s'='%s') '%s'<br>", szNames[i], szValues[i], szFilter);
		}

		const char *sel1=(nPeriod==1)?"selected":"";
		const char *sel7=(nPeriod==7)?"selected":"";
		const char *sel31=(nPeriod==31)?"selected":"";
		const char *sel91=(nPeriod==91)?"selected":"";


		if (szFilter && !*szFilter) szFilter=NULL;

//BIO_putf(io, "Params='%s', Addr='%s', szParam='%s', szValue='%s'<br>", szParams, szAddr, szParam, szValue);
		// Yes, it's a table but it's the only way I know of stopping a form from putting in some vertical space
		BIO_putf(io, "<table border=0>");
		BIO_putf(io, "<tr>");
		BIO_putf(io, "<form>");
		BIO_putf(io, "<td>");
		BIO_putf(io, "Displaying %slogs for ", szFilter?"":"all ");
		if (d) {		BIO_putf(io, "%d%s %s, %04d", d, th(d), MonthNames[m], 2000+y);
		} else if (m) {	BIO_putf(io, "%s %04d", MonthNames[m], 2000+y);
		} else {		BIO_putf(io, "all of %04d", 2000+y);
		}
		if (szFilter) {
			BIO_putf(io, ", filtered on <input name=f size=20 value=\"%s\">", szFilter);
		} else {
			BIO_putf(io, ".  Search for: <input name=f size=20 value=\"\">");
		}
		BIO_putf(io, " (look deeply <input type=checkbox name=deep %s>) ", bDeep?"checked":"");
		BIO_putf(io, " over ");
		BIO_putf(io, "<select name=\"period\">\n");
		if (d) {
			BIO_putf(io, "  <option value=\"1\" %s>%02d-%.3s-20%02d</option>\n", sel1, d, MonthNames[m], y%100);
		} else {
			BIO_putf(io, "  <option value=\"1\" %s>%.3s-20%02d</option>\n", sel1, MonthNames[m], y%100);
		}
		BIO_putf(io, "  <option value=\"7\" %s>Last week</option>\n", sel7);
		BIO_putf(io, "  <option value=\"31\" %s>Last month</option>\n", sel31);
		BIO_putf(io, "  <option value=\"91\" %s>Last 3 months</option>\n", sel91);
		BIO_putf(io, "</select>\n");
		BIO_putf(io, "<input type=submit value=\"Search\">");
		BIO_putf(io, "</td>");
		BIO_putf(io, "</form>");
		BIO_putf(io, "</table>");

		int fromDay=d;
		int fromMonth=m;
		int fromYear=y;
		msc_SubtractDays(nPeriod-1, &fromDay,&fromMonth,&fromYear);
		TableMessageLog(io, bDeep, szFilter, fromDay, fromMonth, fromYear, d, m, y);

		MyBIO_puts(io, "<table width=100% bgcolour=#eeddee>");
		MyBIO_puts(io, "<tr><td valign=top>");

		MyBIO_puts(io, "<td valign=top>");
		MyBIO_puts(io, "</table>");
#if 0
	} else if (!strcasecmp(szAddr, "/xstatus")) {
		int nSecs=time(NULL)-_nStartTime;
		int nMessages=100;
		const char *szFilter="";
		int i;

		nSecs%=60;

		SendHttpHeader(io, 200, NULL, -1);
		SendHtmlHeader(io);

		for (i=0;i<nParams;i++) {
			if (!stricmp(szNames[i], "n")) nMessages=atoi(szValues[i]);
			if (!stricmp(szNames[i], "f")) szFilter=szValues[i];
		}

		BIO_putf(io, "<table><tr><form><td>Display <input name=n value=%d size=4> messages, filtered on <input name=f size=20 value=\"%s\"> <input type=submit value=Display></form></table>", nMessages, szFilter);

		MyBIO_puts(io, "<table width=100% bgcolour=#eeddee>");
		MyBIO_puts(io, "<tr><td valign=top>");
		TableMessageLog(io, nMessages, szFilter);

		MyBIO_puts(io, "<td valign=top>");
//		TableActiveChildren(io);
		MyBIO_puts(io, "</table>");

#endif
	} else if (!strncasecmp(szAddr, "/getinteraction/", 16)) {
		const char *szDir=szAddr+16;
		note_t *note=note_LoadMessage(szDir);
		const char *szDatetime = note_FindEntry(note, 'D');

		SendHttpHeader(io, 200, NULL, -1);
		SendHtmlHeader(io);

		if (!szDatetime) szDatetime = "some time in the past";

		BIO_putf(io, "<table width=100%% bgcolor=#0077ff>\r\n");
		BIO_putf(io, "<tr><td align=center><font size=5 color=white>");

		BIO_putf(io, "Call on ");
		BIO_putf(io, "%s", note_FindEntry(note, 'H'));
		BIO_putf(io, " at %s", szDatetime);
		BIO_putf(io, "</font>\r\n");
		BIO_putf(io, "</table>\r\n");
		BIO_putf(io, "<hr>\r\n");

		BIO_putf(io, "<table width=100%>");

		BIO_putf(io, "<tr><td valign=top>");
		TableNotes(io, note, szDir);
		BIO_putf(io, "<hr>");
		TableFiles(io, szDir);

		BIO_putf(io, "<td valign=top>");
		TableNoteLogs(io, note, szDir);

		BIO_putf(io, "</table>");


		note_Delete(note);
	} else if (!strncasecmp(szAddr, "/getfile/", 9)) {
		const char *szFilename=hprintf(NULL, "%s/%s", szMsgLogDir, szAddr+9);
		if (strstr(szFilename, "/../")) {
			SendHttpHeader(io, 403, NULL, -1);
			BIO_putf(io, "You can't traverse the tree - sorry");
			return 0;
		}
		FILE *fp=fopen(szFilename, "r");
		const char *szExt = strrchr(szAddr, '.');
		const char *szContentType;

		if (szExt) szExt++;
		else szExt="text";

		if (!fp) {
			SendHttpHeader(io, 404, NULL, -1);
			BIO_putf(io, "I'm sorry - I can't open %s", szFilename);
		} else {
			char buf[1024];
			int got;
			szContentType = hprintf(NULL, "text/plain");
			SendHttpHeader(io, 200, szContentType, -1);
			szDelete(szContentType);

			while ((got=fread(buf, 1, sizeof(buf), fp)))
				MyBIO_write(io, buf, got);
			fclose(fp);
		}
	} else {
		char *szRequest=strdup(szAddr+1);						// command/path1/path2?param1=a&param2=b
		char *szParams=strchr(szRequest, '?');					// ?param1=a&param2=b
		char *szCommand=NULL;									// Command used to execute the script
		char *szScript=NULL;									// Name of script file
		char *szCommandLine=NULL;								// Full command line passed to shell
		char buf[1024];
		FILE *fp = NULL;
		int got;
		char *szPathInfo;
		char *chp;

// Debugging - let's see anything that's output
//SendHttpHeader(io, 200, "text/plain", -1);
//SendHtmlHeader(io);

		if (szParams) {
			char *chp=szParams+1;								// param1=a&param2=b

			while (*chp) {
				if (*chp == '&') *chp=' ';
				chp++;
			}
			*szParams++='\0';									// param1=a param2=b
		} else {
			szParams="";
		}

		chp=strchr(szRequest, '/');								// /path1/path2
		if (chp) {
			szPathInfo=hprintf(NULL, "PATH_INFO=%s", chp);
			*chp='\0';
		} else {
			szPathInfo=strdup("PATH_INFO=");
		}
		putenv(szPathInfo);
		chdir(szBaseDir);

		// We'll try php, perl and bourne shell in that order
		if (!szCommand) {
			szDelete(szScript);
			szScript=hprintf(NULL, "%s/htdocs/%s.php", szBaseDir, szRequest);		// Try it as php
			if (!access(szScript, 1)) {						// Can execute it as php
				szCommand="php";
			}
		}

		if (!szCommand) {
			szDelete(szScript);
			szScript=hprintf(NULL, "%s/htdocs/%s.pl", szBaseDir, szRequest);		// Try it as perl
			if (!access(szScript, 1)) {					// Can execute it as perl
				szCommand="perl";
			}
		}

		if (!szCommand) {
			szDelete(szScript);
			szScript=hprintf(NULL, "%s/htdocs/%s.sh", szBaseDir, szRequest);		// Try it as bourne shell
			if (!access(szScript, 1)) {					// Can execute it as bourne shell
				szCommand="sh";
			}
		}

		if (szCommand) {
			szCommandLine = hprintf(NULL, "%s %s %s", szCommand, szScript, szParams);
			fp=popen(szCommandLine, "r");
		}

		if (fp) {
			MyBIO_puts(io, HttpHeader(200));
//BIO_putf(io, "Content-type: text/html\r\n\r\nCommand line = '%s'<hr>", szCommandLine);
			while ((got=fread(buf, 1, sizeof(buf), fp))) {
				MyBIO_write(io, buf, got);
			}
			fclose(fp);
		} else {
			MyBIO_puts(io, HttpHeader(403));
			MyBIO_puts(io, "Server: Microtest Spider " VERSION "\r\n");
			MyBIO_puts(io, "\r\n");
			MyBIO_puts(io, "You are not allowed to do whatever it was you just tried to do.");
		}
		putenv("PATH_INFO=");
		szDelete(szCommandLine);
	}

	mime_Delete(mime);

	return 0;
}

static int DealWithPUT(BIO *io, const char *szURI)
{
	Log("HTTP PUT %s", szURI);
	NoteInhibit(1);

	return 0;
}

static void SetIds(const char *szUser)
// Sets the effective user and group IDs to those of the given user
{
	struct passwd *pw = getpwnam(szUser);

	if (pw) {
		int nErr;

		nErr = setgid(pw->pw_gid);
		if (nErr) Log("Error %d setting group id of %d (to match %s)", nErr, pw->pw_gid, szUser);
		nErr = setuid(pw->pw_uid);
		if (nErr) Log("Error %d setting user id of %d (to match %s)", nErr, pw->pw_uid, szUser);
	} else {
		Log("Failed to set uid/gid to those of '%s'", szUser);
	}
}

static rogxml *NewResult()
{
	rogxml *rx = rogxml_NewElement(NULL, "SPIDER-RPC-Result");

	return rx;
}

static rogxml *SPIDERSystemError(int nErr, const char *szErr)
// Create an RPC system error lump with the given error number and text
{
	mi_status=nErr;
	rogxml *rxResult = NewResult();
	rogxml_SetAttr(rxResult, "status", "1");					// System error
	rogxml_SetAttrInt(rxResult, "error", nErr);					// Error number
	rogxml *rxError = rogxml_AddTextChild(rxResult, "error", szErr);
	rogxml_SetAttr(rxError, "source", "SPIDER");

	return rxResult;
}

static rogxml *SPIDERRpcName(rogxml *rxRpc)
// Old style RPC handling with the name attribute
{
	rogxml *rx = NULL;
	char   *pgname = NULL;
	char   *basepgname;
	const char *param[128]; /* max 128 parameters */
	const char *stdin_buf = NULL;
	HBUF	*BufStdout = NULL;				// Incoming buffers for stdout and stderr
	HBUF	*BufStderr = NULL;
	rogxml *rxResultSet = NewResult();
	int i, retval;
	pid_t pid;
	int  fd1[2]; /* stdin */
	int  fd2[2]; /* stdout */
	int  fd3[2]; /* stderr */

	/* initialise the array of parameters */
	memset(param,0,sizeof(param));

	/* Retrieve Name attribute */
	basepgname = strdup(rogxml_GetAttr(rxRpc, "name"));
	if( basepgname == NULL ) {
		/* Error getting "name" attribute */
		return ErrorMessage(501, 1, "No procedure name");
	}
	/* Parse the name space to find the location of the application */
	for( i = 0; basepgname[i]; ++i ) {
		if( basepgname[i] == '.' )
			basepgname[i] = '/';
	}

	pgname = hprintf(NULL, "%s/%s", szRpcDir, basepgname );
	param[0] = strdup( pgname );

	if( access( pgname, R_OK|X_OK ) != 0 )
	{	/* error checking file */
		rogxml* err = ErrorMessage(502, 1, "Procedure '%s' not known", pgname);
		// cleanup
		free(pgname);
		free(basepgname);
		for(i=0; param[i] != NULL; ++i ) {
			free( (void*)param[i] );
		}
		return err;
	}

	/* Build parameter list */
	for( rx = rogxml_FindFirstChild(rxRpc), i=1; rx != NULL; rx = rogxml_FindNextSibling(rx) ) {
		const char *name = rogxml_GetLocalName(rx);
		if( strcmp( name, "p" ) == 0 ) {
			param[i++] = rogxml_GetValue(rx);
		}
		else if( strcmp( name, "stdin" ) == 0 ) {
			/* extract stdin data */
			stdin_buf = rogxml_ToText(rx);			// v1.02
		}
		else {
			/* unknown element - ignore */
		}
	}

	/* Create Pipe */
	if( pipe(fd1) == -1 || pipe(fd2) == -1 || pipe(fd3) == -1 ) {
		char* err = strerror(errno);
		Log("Error creating pipe: %s", err);
		return ErrorMessage(503, 1, err);
	}

	if( (pid = fork()) == 0 ) {
		/* Child Process */
		/* Close unused pipe-ends */
		close( fd1[1] );
		close( fd2[0] );
		close( fd3[0] );

		/* Map STDIN and STDOUT to the pipe ends and execute */
		if( fd1[0] != STDIN_FILENO ) {
			if( dup2( fd1[0], STDIN_FILENO) == -1 ) {
				char* err = strerror(errno);
				Log("Error copying pipe: %s", err);
				return ErrorMessage(504, 1, err);
			}
			close(fd1[0]);
		}
		if( fd2[1] != STDOUT_FILENO ) {
			if( dup2( fd2[1], STDOUT_FILENO) == -1 ) {
				char* err = strerror(errno);
				Log("Error copying pipe: %s", err);
				return ErrorMessage(504, 1, err);
			}
			close(fd2[1]);
		}
		if( fd3[1] != STDERR_FILENO ) {
			if( dup2( fd3[1], STDERR_FILENO) == -1 ) {
				char* err = strerror(errno);
				Log("Error copying pipe: %s", err);
				return ErrorMessage(504, 1, err);
			}
			close(fd3[1]);
		}

		// Arrange that all attributes to the RPC element appear as environment variables in the form
		// RPCNAME=value (name is always capitalised)
		rogxml *rxAttr;
		for (rxAttr=rogxml_FindFirstAttribute(rxRpc);rxAttr;rxAttr=rogxml_FindNextSibling(rxAttr)) {
			char *szName = strdup(rogxml_GetLocalName(rxAttr));
			strupr(szName);
			const char *szTmp = hprintf(NULL, "RPC%s=%s", szName, rogxml_GetValue(rxAttr));	// Acceptable leak
			putenv((char*)szTmp);
			szDelete(szName);
		}

		Log("Executing RPC command: %s", pgname );
		if( execv( pgname, (char * const *)param ) == -1 ) {
			char* err = strerror(errno);
			Log("Error running process: %s", err);
			return ErrorMessage(505, 1, err);
		}
	}
	else {
		/* Parent Process */
		ssize_t r = 0, w = 0;
		int status = 0;
		struct pollfd fds[2];

		/* close unused pipe-ends */
		close( fd1[0] );
		close( fd2[1] );
		close( fd3[1] );

		fds[0].fd = fd2[0];
		fds[1].fd = fd3[0];
		/* PORTABILITY ISSUE */
		/* Checking for EOF on a pipe is done with the POLLHUP flag on UnixWare
		   and Linux, this is not always true on other systems */
		fds[0].events = POLLIN | POLLHUP;
		fds[1].events = POLLIN | POLLHUP;

		/* Write input to child stdin */
		if( stdin_buf != NULL ) {
			do {
				retval = write( fd1[1], stdin_buf + w, strlen(stdin_buf) - w );
				if( retval == -1 ) {
					Log("Error writing to RPC stdin: %s", strerror(errno));
					break;
				}
				w += retval;
			} while( retval > 0 && w != strlen(stdin_buf) );
		}
		close( fd1[1] );

		BufStdout= hbuf_New();
		BufStderr= hbuf_New();

		/* read all out put until pipe closes */
		while( (retval = poll( fds, 2, 1000 )) >= 0 ) {				// The 1000 is 1000ms
			char buf[1024];

			if( fds[0].revents & POLLIN ) {							/* Read RPC stdout */
				r = read( fds[0].fd, buf, sizeof(buf));				// Read what we can
				hbuf_AddBuffer(BufStdout, r, buf);					// Add it to the buffer
				if( fds[0].revents & POLLHUP ) {					// Other program terminated
					while (r) {										// Suck the buffer clean
						r = read( fds[0].fd, buf, sizeof(buf));
						hbuf_AddBuffer(BufStdout, r, buf);
					}
				}
			}

			if( fds[1].revents & POLLIN ) {							/* Read RPC stderr */
				r = read( fds[1].fd, buf, sizeof(buf));				// Read what we can
				hbuf_AddBuffer(BufStderr, r, buf);					// Add it to the buffer
				if( fds[1].revents & POLLHUP ) {					// Other program terminated
					while (r) {										// Suck the buffer clean
						r = read( fds[1].fd, buf, sizeof(buf));
						hbuf_AddBuffer(BufStderr, r, buf);
					}
				}
			}

			if( fds[0].revents & POLLHUP ) {						/* Close RPC stdout */
				fds[0].fd = -1;
				break;												/* if stdout closes, we're done */
			}

			if( fds[1].revents & POLLHUP ) {						/* Close RPC stderr */
				fds[1].fd = -1;
			}
		}
		if( retval == 0 ) {
			Log("Warning: poll() timed out.");
		}
		if( retval == -1 ) {
			Log("Error reading from RPC: %s", strerror(errno));
		}

		/* All pipes are closed, wait to prevent zombie process */
		/* Wait for child process to end */
		if( waitpid( pid, &status, 0 ) == -1 ) {
			/* wait error */
			Log("Error waiting for RPC to finish: %s", strerror(errno));
		}

		if( WIFEXITED(status) ) {
			/* child process exited */
			int retval = WEXITSTATUS(status);
			rogxml_SetAttrInt(rxResultSet,"result",retval);
			Log("RPC call exited normally (exit code = %d)", retval);
		}
		else if( WIFSIGNALED(status) ) {
			/* child process killed by signal */
			rogxml_SetAttrInt(rxResultSet,"termsig", WTERMSIG(status));
			Log("RPC killed with kill -%d", WTERMSIG(status));
		}
		else {
			/* unknown exit reason */
			Log("RPC exited with unknown status");
		}

	}
	Log("Tidying up RPC");

	/* package stdout */
	int nStdoutLen = hbuf_GetLength(BufStdout);
	if (nStdoutLen) {
		hbuf_AddChar(BufStdout, 0);									// Terminate the buffer
		const char * buf = hbuf_ReleaseBuffer(BufStdout);			// Get the buffer

		rogxml *rx=rogxml_FromText(buf);							// Try and interpret it as XML
		if (!rogxml_ErrorNo(rx)) {									// Yay, XML it is then
			rogxml *rxStdout = rogxml_AddChild( rxResultSet, "stdout");
			rogxml_LinkChild(rxStdout, rx);
			Log("Added XML stdout (size=%d)", nStdoutLen);
		} else {													// Just text so add it as CDATA
			rogxml_AddCDataChild( rxResultSet, "stdout", buf );
			Log("Added CDATA stdout (size=%d)", nStdoutLen);
			rogxml_Delete(rx);
		}
	}
	hbuf_Delete(BufStdout);

	/* package stderr */
	int nStderrLen = hbuf_GetLength(BufStderr);						// See if we had any stderr
	if (nStderrLen) {
		hbuf_AddChar(BufStderr, 0);									// Terminate the buffer
		const char * buf = hbuf_ReleaseBuffer(BufStderr);			// Get the buffer
		rogxml_AddCDataChild( rxResultSet, "stderr", buf);	// Always treat stderr as CDATA
		Log("Added CDATA stderr (size=%d)", nStderrLen);
	}
	hbuf_Delete(BufStderr);

	/* cleanup */
	free(pgname);
	free(basepgname);
	for(i=0; param[i] != NULL; ++i ) {
		free( (void*)param[i] );
	}

	return rxResultSet;
}

static rogxml *SpiderFunction(rogxml *rxRpc, const char *szFunction)
// Execute an internal spider function (szFunction is the part after "spider."
{
	rogxml *rxResultSet = NewResult();
	rogxml_SetAttr(rxResultSet, "version", VERSION);

	if (!stricmp(szFunction, "enumerateAPIs")) {
		const char **list = GetAllApis();
		const char **l = list;
		int napi=0;

		rogxml *rxResult = rogxml_NewElement(NULL, "result");		// Element to hang results from

		while (*l) {
			const char *error = cbac_CanRunApi(g_OrganisationProduct, *l, 0);
			if (!error) {
				rogxml *rx=rogxml_AddTextChild(rxResult, "api",*l);
//				rogxml_SetAttr(rx, "binary",api_Binary(*l));
				napi++;
			}
			l++;
		}
		rogxml_SetAttrInt(rxResult, "count", napi);
		rogxml_LinkChild(rxResultSet, rxResult);
	} else if (!stricmp(szFunction, "adl")) {
		const char *szFunction = rogxml_GetAttr(rxRpc, "function");
		rogxml *rx = api_Adl(szFunction);
		if (rx) {
			rogxml *rxResult = rogxml_NewElement(NULL, "result");		// Element to hang results from
			rogxml_LinkChild(rxResult, rx);
			rogxml_LinkChild(rxResultSet, rxResult);
		} else {
			rxResultSet = rogxml_NewError(402, "Could not retrieve ADL from %s", szFunction);
		}
	} else {
		rxResultSet = rogxml_NewError(401, "Function spider.%s not recognised", szFunction);
	}

	return rxResultSet;
}

static rogxml *CheckAuthorisation(const char *auth)
// String is supplier:product:version:key
// Returns	NULL	Everything is ok - proceed
//			rogxml*	An error created using 'ErrorMessage()'
{
	rogxml *result = NULL;
	const char *szErr = NULL;

	if (!auth) auth="unknown:unknown:unknown:xyzzy";
	char *copyAuth = strdup(auth);

	const char *szSupplier = strtok(copyAuth, ":");
	const char *szApp = strtok(NULL, ":");
	const char *szVersion = strtok(NULL, ":");
	const char *szKey = strtok(NULL, ":");

	if (!szKey) szErr = hprintf(NULL, "Incomplete authorisation '%s'", auth);

	mi_caller = hprintf(NULL, "%s:%s:%s", szSupplier,szApp,szVersion);
	if (!szErr) {
		S3 *s3 = s3_OpenExisting("/usr/mt/spider/auth.sl3");

		if (s3) {
			S3LONG(s3, enabled);
			S3LONG(s3, count);

			enabled=-1;
			s3_MustQueryf(s3, "SELECT enabled "
				" FROM authorisation "
				" WHERE supplier=%s "
				"   AND (app=%s OR app='*') "
				"   AND (version=%s OR version='*') "
				"   AND (key=%s OR key='*')",
				szSupplier, szApp, szVersion, szKey);

			switch (enabled) {
			case 0:		// Explicitly disabled
				szErr=hprintf(NULL, "Application %s/%s, version %s disabled", szSupplier, szApp, szVersion);
				break;
			case 1:		// Explicitly enabled
				break;
			case -1:	// Not found
				s3_MustQueryf(s3, "SELECT count(*) as count "
					" FROM authorisation "
					" WHERE supplier=%s "
					"   AND (app=%s OR app='*') "
					"   AND (version=%s OR version='*') ",
					szSupplier, szApp, szVersion);
				if (count) {
					szErr = hprintf(NULL, "Key incorrect for version %s of application %s:%s", szVersion, szSupplier, szApp);
				} else {
					s3_MustQueryf(s3, "SELECT COUNT(*) as count "
						" FROM authorisation "
						" WHERE supplier=%s AND app=%s",
						szSupplier, szApp);
					if (count) {
						szErr = hprintf(NULL, "Version %s of application %s:%s not recognised", szVersion, szSupplier, szApp);
					} else {
						s3_MustQueryf(s3, "SELECT COUNT(*) as count "
							" FROM authorisation "
							" WHERE supplier=%s",
							szSupplier);
						if (count) {
							szErr = hprintf(NULL, "Application %s:%s not recognised", szSupplier, szApp);
						} else {
							szErr = hprintf(NULL, "Supplier %s not recognised", szSupplier);
						}
					}
				}
				break;
			}
			s3_Close(s3);
		} else {
			szErr = strdup("Cannot open authorisation database");
		}
	}

	Log("Authcheck '%s' is %s", auth, szErr ? szErr : "OK");

	if (szErr) {
		Log("Authorisation error: %s", szErr);
		result = ErrorMessage(600, 1, szErr);
		szDelete(szErr);
	}

	szDelete(copyAuth);

	return result;
}

static rogxml *SPIDERRPCFunction(rogxml *rxRpc)
// Handles the NEW style RPC with a _function attribute.
// Note there is a subtle side-effect in here...  If g_szRpcError is non-NULL on exit, the content is written to
// the log AFTER the result is returned.  This should be the stderr of the executed binary.
{
	rogxml *rx = NULL;
	const char *szFunction;
	const char *szBinary;
	const char *stdin_buf = NULL;
	HBUF	*BufStdout = NULL;				// Incoming buffers for stdout and stderr
	HBUF	*BufStderr = NULL;
	int retval;
	pid_t pid;
	int fdin[2];							// Pipe for stdin
	int fdout[2];							// Pipe for stdout
	int fderr[2];							// Pipe for stderr
	const char **args;						// Program arguments passed through
	const char *szError=NULL;				// Internally generated prefix to stderr if we can't read stdout
	int nErrno = 0;							// Error number tacked onto the result

	const char *szAuth = rogxml_GetAttr(rxRpc, "_auth");	// supplier:product:version:key
	if (config_GetBool("authorise", 0, "Set to YES to authorise using auth.sl3")) {
		rx = CheckAuthorisation(szAuth);
		if (rx) return rx;
	} else if (szAuth) {					// Call authorisation anyway to pull information for MI
		rx = CheckAuthorisation(szAuth);
		rogxml_Delete(rx);					// Ignore any returned error
	}

	szFunction = strdup(rogxml_GetAttr(rxRpc, "_function"));
	if (!szFunction || !*szFunction) {		// We should only get here if it's defined but it may be blank...
		return ErrorMessage(501, 1, "_function attribute is blank - I don't know what to do!");
	}

	rogxml *rxAdl = NULL;
	const char *szVersion = NULL;
	api_Refresh(szFunction, NULL, &szVersion, &rxAdl);

	InfoStr("API-FUNCTION",szFunction);
	Note("F|%s", szFunction);

	const char *szSession = rogxml_GetAttr(rxRpc, "RPC_SESSIONID");
	if (szSession) {
		mi_session = strdup(szSession);
		const char *szRpcSession = RpcSessionFromApi(szSession);
		if (szRpcSession) {
			rogxml_SetAttr(rxRpc, "RPC_SESSION", szRpcSession);
		}
	}

	if (!strncasecmp(szFunction,"spider.",7)) {
		return SpiderFunction(rxRpc, szFunction+7);
	}

	// Take the function component by component and look for a binary
	szBinary=strdup(szRpcDir);
	const char *chp=szFunction;

	for (;;) {
		const char *next;
		struct stat st;

		if (!*chp || *chp=='.') return rogxml_NewError(502, "Blank component in function (%s)", szFunction);
		next = strchr(chp, '.');
		if (!next) next=chp+strlen(chp);						// Last component
		szBinary=hprintf(szBinary, "/%.*s", next-chp, chp);			// Add it to our path
		int err = stat(szBinary, &st);

		if (err && errno == ENOENT)
			return rogxml_NewError(503, "Function component not found at '%.*s' (%s)",
					next-szFunction, szFunction, szBinary);

		if (st.st_mode & S_IFREG) break;							// We're there, worry about executability later

		if (!(st.st_mode & S_IFDIR))
			return rogxml_NewError(504, "Strange file entry found at '%s'", szBinary);

		if (!*next)
			return rogxml_NewError(505, "Incomplete function '%s'", szFunction);	// Final component is dir

		chp=next+1;												// Move on to next component
	}

	InfoStr("API-BINARY", szBinary);
	Note("B|%s", szBinary);
	if (access(szBinary, R_OK|X_OK))
		return rogxml_NewError(506, "Function '%s' not executable (at %s)", szFunction, szBinary);

	// Build the argument list, which looks as if the rpc is called as:
	// /usr/mt/spider/rpc/fred _function=fred attr1=whatever attr2=whatever
	// Where 'attr1' etc. are all attributes passed in the root element

	int nArgs=0;												// Count of arguments
	rogxml *rxAttr;
	for (rxAttr=rogxml_FindFirstAttribute(rxRpc);rxAttr;rxAttr=rogxml_FindNextSibling(rxAttr)) {
		nArgs++;
	}

	args = NEW(const char *, nArgs+3);				// +2 is program, possibly RPC_CLIENTIP and NULL
	args[0]=strdup(szBinary);
	int nArg=1;

	for (rxAttr=rogxml_FindFirstAttribute(rxRpc);rxAttr;rxAttr=rogxml_FindNextSibling(rxAttr)) {
		const char *szName = rogxml_GetLocalName(rxAttr);
		if (strcmp(szName, "RPC_CLIENTIP")) {
			const char *szArg = hprintf(NULL, "%s=%s", szName, rogxml_GetValue(rxAttr));
			Note("A|%s", szArg);
			args[nArg++]=szArg;
		}
	}
	args[nArg++]=hprintf(NULL, "RPC_CLIENTIP=%s", _szSenderIp);
	args[nArg]=NULL;

	// Look for contained elements that we currently support (just "input")
	for(rx = rogxml_FindFirstChild(rxRpc); rx != NULL; rx = rogxml_FindNextSibling(rx) ) {
		const char *name = rogxml_GetLocalName(rx);
		if (!strcmp( name, "input" )) {
			if (stdin_buf) return rogxml_NewError(507, "Multiple input elements are not allowed");
			rogxml *rxWalk = rogxml_FindFirstChild(rx);		// Want to walk through all children, rendering them (1.04)

			while (rxWalk) {
				if (rogxml_IsElement(rxWalk)) {
					stdin_buf = strappend(stdin_buf, rogxml_ToText(rxWalk));	// element so render it
				} else {
					stdin_buf = strappend(stdin_buf, rogxml_GetValue(rxWalk));	// Text child so need it verbatim
				}

				rxWalk = rogxml_FindNextSibling(rxWalk);
			}
//			stdin_buf = rogxml_GetValue(rx);		// Change to rogxml_ToText(rx) for v1.02
//			stdin_buf = rogxml_ToText(rx);		// Change to rogxml_ToText(rx) for v1.02
		}
	}

	if (stdin_buf) {
		int nLen = strlen(stdin_buf);
		NoteMessage(stdin_buf, nLen, "stdin", "txt", "stdin to API");

		Log("API input (%s): %d byte%s", NoteFileAnchor("stdin.txt", "stdin"), nLen, nLen==1?"":"s");
	} else {
		Log("API input: None");
	}

	// Create three pipes for stdin, stdout and stderr
	if( pipe(fdin) == -1 || pipe(fdout) == -1 || pipe(fderr) == -1 ) {
		const char* err = strerror(errno);
		return rogxml_NewError(508, "Error %d creating pipe: %s", errno, err);
	}
	const char *szPort = hprintf(NULL, "%d", _nIncomingPort);
	const char *szProtocol = hprintf(NULL, "%s", ProtocolName(_nIncomingProtocol));

	EnvSet("SPIDER_VERSION", VERSION);
	EnvSet("SPIDER_OS", OS);
	EnvSet("SPIDER_BASEDIR", szBaseDir);
	EnvSet("SPIDER_RPCDIR", szRpcDir);
	EnvSet("SPIDER_RPCDATADIR", szRpcDataDir);
	EnvSet("SPIDER_SERVER", argv0);
	EnvSet("SPIDER_HOST", _szIncomingHost);
	EnvSet("SPIDER_IP", _szIncomingIp);
	EnvSet("SPIDER_PORT", szPort);
	EnvSet("SPIDER_PROTOCOL", szProtocol);

	if (g_Organisation) EnvSet("RPC_SUPPLIER", g_Organisation);				// Should be ascertained from the TLS certificate
	if (g_Product) EnvSet("RPC_PRODUCT", g_Product);
	// RPC_PRODUCT_VERSION is set elsewhere
	EnvSet("RPC_CBACID", g_cbacId);

	szDelete(szPort);
	szDelete(szProtocol);

	// MAIN API FORK

	if (!(pid = fork())) {								// Child process
		close(fdin[1]);									// Close un-used pipe ends
		close(fdout[0]);
		close(fderr[0]);

		// Map STDIN, STDOUT and STERRR to the pipe ends
		if (dup2(fdin[0], STDIN_FILENO) == -1) return rogxml_NewError(509, "Error %d piping stdin", errno);
		if (dup2(fdout[1], STDOUT_FILENO) == -1) return rogxml_NewError(509, "Error %d piping stdout", errno);
		if (dup2(fderr[1], STDERR_FILENO) == -1) return rogxml_NewError(509, "Error %d piping stderr", errno);

		close(fdin[0]);									// Close duplicated pipe ends
		close(fdout[1]);
		close(fderr[1]);

		umask(02);										// Set a sensitive and caring environment for
		SetIds("pm1");									// the API, which acts as user pm1

		Log("Executing: %s", szBinary);
		if (execv(szBinary, (char * const *)args) == -1) {
			char* err = strerror(errno);
			return rogxml_NewError(510, "Error %d (%s) running RPC process: %s", errno, err, szBinary);
		}
	}

	ssize_t r = 0, w = 0;
	int status = 0;
	struct pollfd fds[2];

	close(fdin[0]);										// Close unused pipe-ends
	close(fdout[1]);
	close(fderr[1]);

	fds[0].fd = fdout[0];
	fds[1].fd = fderr[0];
	/* PORTABILITY ISSUE */
	/* Checking for EOF on a pipe is done with the POLLHUP flag on UnixWare
	   and Linux, this is not always true on other systems */
	fds[0].events = POLLIN | POLLHUP;
	fds[1].events = POLLIN | POLLHUP;

	/* Write input to child stdin */
	int bBrokenPipe = 0;					// Set if the API goes away while we're talking to it (very rude)
	if (stdin_buf) {
		int written;

		do {
			written = write( fdin[1], stdin_buf + w, strlen(stdin_buf) - w );
			if (written == -1) {
				if (errno == 32) {
					Log("Broken pipe error (32) returned while writing data to API");
					bBrokenPipe = 1;
					break;
				} else {
					return rogxml_NewError(511, "Error %d (%s) writing to RPC stdin", errno, strerror(errno));
				}
			}
			w += written;
		} while( written > 0 && w != strlen(stdin_buf) );
	}
	close( fdin[1] );

	BufStdout= hbuf_New();										// Buffers to hold stdout and stderr
	BufStderr= hbuf_New();

	/* read all out put until pipe closes */
	int pollval;
	while ((pollval = poll( fds, 2, 1000 )) >= 0) {				// The 1000 is 1000ms
		char buf[1024];

		if( fds[0].revents & POLLIN ) {							/* Read RPC stdout */
			r = read( fds[0].fd, buf, sizeof(buf));				// Read what we can
			hbuf_AddBuffer(BufStdout, r, buf);					// Add it to the buffer
			if( fds[0].revents & POLLHUP ) {					// Other program terminated
				while (r) {										// Suck the buffer clean
					r = read( fds[0].fd, buf, sizeof(buf));
					hbuf_AddBuffer(BufStdout, r, buf);
				}
			}
		}

		if( fds[1].revents & POLLIN ) {							/* Read RPC stderr */
			r = read( fds[1].fd, buf, sizeof(buf));				// Read what we can
			hbuf_AddBuffer(BufStderr, r, buf);					// Add it to the buffer
			if( fds[1].revents & POLLHUP ) {					// Other program terminated
				while (r) {										// Suck the buffer clean
					r = read( fds[1].fd, buf, sizeof(buf));
					hbuf_AddBuffer(BufStderr, r, buf);
				}
			}
		}

		if( fds[0].revents & POLLHUP ) {						/* Close RPC stdout */
			fds[0].fd = -1;
			break;												/* if stdout closes, we're done */
		}

		if( fds[1].revents & POLLHUP ) {						/* Close RPC stderr */
			fds[1].fd = -1;
		}
	}

	if (pollval == -1) return rogxml_NewError(512, "Error %d (%s) polling RPC", errno, strerror(errno));

	if (waitpid( pid, &status, 0 ) == -1 ) {					// Await termination status of child
		return rogxml_NewError(513, "Error %d (%s) waiting for RPC (%d) to finish", errno, strerror(errno), pid);
	}

	/* package stderr */
	int nStderrLen = hbuf_GetLength(BufStderr);						// See if we had any stderr
	if (nStderrLen) {
		hbuf_AddChar(BufStderr, 0);									// Terminate the buffer
		const char * buf = hbuf_ReleaseBuffer(BufStderr);			// Get the buffer

//Log("Err len = %d: %s", nStderrLen, buf);
		NoteMessage(buf, -1, "stderr", "txt", "stderr from API");
		Note("M|stderr.txt|Error output from RPC|%d", nStderrLen);
		if (g_szRpcError) {											// Prepend any existing error message
			g_szRpcError = strappend(g_szRpcError, buf);
			szDelete(buf);
		} else {
			g_szRpcError = buf;
		}
		Log("API Debug (%s): %d byte%s", NoteFileAnchor("stderr.txt", "stderr"), nStderrLen, nStderrLen==1?"":"s");
	} else {
		Log("API Debug (stderr): none");
	}
	hbuf_Delete(BufStderr);

	if (!WIFEXITED(status)) {									// Non-standard exit from RPC binary
		if (WIFSIGNALED(status)) {								// Someone killed the child (may be it errored)
			return rogxml_NewError(514, "RPC %s terminated with SIGNAL %d (%s)",
					szBinary, WTERMSIG(status), FullSignalName(WTERMSIG(status)));
		} else {
			return rogxml_NewError(515, "RPC %s exited oddly", szBinary);
		}
	}

	retval = WEXITSTATUS(status);
	Log("RPC call exited with status = %d", retval);
// Original functionality misunderstood by JW so changed...
//	if (retval > 1 && retval < 10)
//		return rogxml_NewError(516, "Invalid return code (%d) from RPC '%s' (must not be 2..9)", retval, szFunction);

	if (retval && retval < 10) {											// Indicates error returned from binary
		nErrno=500+retval;
		szError=hprintf(NULL, "System error returned by RPC process '%s'", szFunction);
	}

	rogxml *rxResultSet = NewResult();
	rogxml_SetAttr(rxResultSet, "version", szVersion);
//Log("We have version '%s'", szVersion);

	// Stdout might contain a number of results and/or a number of errors, or something non-XML
	// This is where we interpret that stuff.
	int nStdoutLen = hbuf_GetLength(BufStdout);
//Log("We have stdout of length %d", nStdoutLen);
	if (nStdoutLen) {
		Log("API Output (%s): %d byte%s", NoteFileAnchor("stdout.txt", "stdout"), nStdoutLen, nStdoutLen==1?"":"s");
	} else {
		Log("API Output (stdout): none");
	}
	if (nStdoutLen) {
		hbuf_AddChar(BufStdout, 0);									// Terminate the buffer
		const char * buf = hbuf_ReleaseBuffer(BufStdout);			// Get the buffer
		const char *next=SkipSpaces(buf);							// To allow reading of multiple elements
		rogxml *rxErrors = rogxml_NewElement(NULL, "errors");		// Element to hang errors from
		rogxml *rxResult = rogxml_NewElement(NULL, "result");		// Element to hang results from

		NoteMessage(buf, -1, "stdout", "txt", "API Output (stdout)");

		while (*next) {
			rogxml *rx=rogxml_FromText(next);						// Try and interpret it as XML

			if (rx) {
				if (rogxml_ErrorNo(rx)) {								// Some duff XML from the binary
					Log("Bad XML from RPC: %s", rogxml_ErrorText(rx));
					g_szRpcError=hprintf(NULL, "Bad XML in stdout: %s\n---- Stdout from here\n%s\n---- Stderr from here\n", rogxml_ErrorText(rx), buf);
					retval=1;
					nErrno=517;

					rogxml *rxErr=rogxml_NewElement(NULL, "error");		// Prepend to any other errors
					rogxml_SetAttr(rxErr, "source", "SPIDER");
					rogxml_AddTextf(rxErr, "Invalid XML returned from RPC '%s' (%s)", szFunction, rogxml_ErrorText(rx));
					rogxml_LinkChild(rxErrors, rxErr);
					break;
				}

				const char *szName = rogxml_GetLocalName(rx);
				rogxml_SetAttr(rx, "api-version", szVersion);

				if (!strcmp(szName, "error")) {						// Child is returning an error
					if (!retval) {									// Returning error elements on no error...
						retval=1;
						nErrno=518;
						rogxml *rx=rogxml_NewElement(NULL, "error");		// Prepend to any other errors
						rogxml_SetAttr(rx, "source", "SPIDER");
						rogxml_AddTextf(rx, "Undetected errors within RPC '%s'", szFunction);
						rogxml_LinkChild(rxErrors, rx);
					}
					rogxml_LinkChild(rxErrors, rx);
					rx=NULL;
					if (nErrno == 500) {							// Forget our own default error
						szDelete(szError);
						szError=NULL;
					}
				} else {
					if (bHandlingSessions) {
						if (!strcmp(szFunction, "user.login")) {
							const char *szApiSession = rogxml_GetAttr(rx, "sessionId");
							if (szApiSession) {
								const char *szReplacement = NewSessionId(szApiSession);
								rogxml_SetAttr(rx, "sessionId", szReplacement);
							}
						}
					} else {
						if (!strcmp(szFunction, "user.login")) {
							const char *szApiSession = rogxml_GetAttr(rx, "sessionId");
							if (szApiSession) {
								mi_session = strdup(szApiSession);
							}
						} else if (!strcmp(szFunction, "session.create")) {
							const char *szApiSession = rogxml_GetAttr(rx, "sessionId");
							if (szApiSession) {
								mi_session = strdup(szApiSession);
							}
						}
					}
					rogxml_LinkChild(rxResult, rx);					// Link it in
				}
			}

			next = SkipSpaces(rogxml_GetNextText());
		}

		if (rogxml_FindFirstChild(rxResult)) {							// Add results if we have any
			rogxml_LinkChild(rxResultSet, rxResult);
		} else {
			rogxml_Delete(rxResult);									// Otherwise drop the element
		}

		for (;;) {														// Add any error elements
			rogxml *rx = rogxml_FindFirstChild(rxErrors);

			if (rx) {
				rogxml_LinkChild(rxResultSet, rx);
			} else {
				break;
			}
		}
		rogxml_Delete(rxErrors);

		szDelete(buf);
	}
	hbuf_Delete(BufStdout);

	if (bBrokenPipe) {
		if (szError) {
			szError = hprintf(szError, " and the API broke the pipe (died?) while spider was sending it data");
		} else {
			szError = strdup("The API broke the pipe (died?) while spider was sending it data");
		}
	}

	if (szError) {											// We have our own error we'd like to add
		rogxml *rx=rogxml_NewElement(NULL, "error");		// Prepend to any other errors
		rogxml_SetAttr(rx, "source", "SPIDER");
		rogxml_AddText(rx, szError);
		rogxml_LinkChild(rxResultSet, rx);
	}

	InfoInt("API-RETVAL",retval);
	Note("C|%d", retval);
	mi_status=retval;
	if (retval == 0 || retval >= 10) {
		rogxml_SetAttrInt(rxResultSet, "status", retval ? 2 : 0);		// Always return 2 for a business error
		if (retval >= 10) rogxml_SetAttrInt(rxResultSet, "error", retval);	// Return the actual exit code here
	} else {
		rogxml_SetAttrInt(rxResultSet, "status", retval);
		if (nErrno) rogxml_SetAttrInt(rxResultSet, "error", nErrno);
	}

	return rxResultSet;
}

static rogxml *SPIDERRpc(rogxml *rxRpc)
// Dual purpose rpc handling.  If there is a '_function' attribute then we're using the new style
// argument passing and return mechanism, if instead there's a 'name' attribute then we're using
// the old one, if there neither then it's an error.
{
	const char *function = rogxml_GetAttr(rxRpc, "_function");
	rogxml *rxRpcResult = NULL;

	if (function) {									// New style call with _function
		mi_function = strdup(function);
		Log("SPIDER-RPC Function=%s", function);
		rxRpcResult=SPIDERRPCFunction(rxRpc);
		NoteMessageXML(rxRpcResult, "rpc-out", "Caller result (XML)", 'R');

		int nErr = rogxml_ErrorNo(rxRpcResult);

		InfoStr("API-FUNCTION",function);
		Note("Z|%s", function);
		if (nErr) {						// We've returned an error, which will be a system error
			const char *szErr = rogxml_ErrorText(rxRpcResult);

			Log("RPC Error %d: %s", nErr, szErr);
			rogxml *rxResult=SPIDERSystemError(nErr, szErr);
			const char *szVersion = rogxml_GetAttr(rxRpcResult, "version");
			if (szVersion) rogxml_SetAttr(rxResult, "version", szVersion);
			rogxml_Delete(rxRpcResult);
			rxRpcResult=rxResult;
		}

		if (g_szRpcError) {
			rogxml_SetAttr(rxRpcResult, "errorlog", NoteDirSuffix());
		}
	} else {
		Log("Error: Legacy call with 'name' attribute - don't use this");
		const char *name = rogxml_GetAttr(rxRpc, "name");
		if (name) {
			mi_function = strdup(name);
			Log("SPIDER-RPC Name=%s", name);
			rxRpcResult = SPIDERRpcName(rxRpc);
			NoteMessageXML(rxRpcResult, "rpc-out", "Entire RPC response", 'R');
		} else {
			mi_function = strdup("NOT-GIVEN");
			rxRpcResult = rogxml_NewError(101, "RPC call must include either _function or name attribute");
		}
	}

	if (!_bNoteInhibit) {
		NoteRpcError();					// Stderr from the RPC is held in a global var, logged away in this function
	}

	return rxRpcResult;
}


static int DealWithPOST(BIO *io, const char *szURI)
// Post has come in over SSL/io addressed to szURI
{
	MIME *mime;

	const char *szAddr = NULL;						// Address part of szURI (always starts with '/')
	const char **szNames = NULL;					// Vector of names of parameters
	const char **szValues = NULL;					// Vector of values of parameters

	Log("POST %s", szURI);

//	bAuth=AuthenticatePeer(ssl, NULL, 1);
//	if (!bAuth) Fatal("POSTs only accepted from authenticated peers");
//	if (!bAuth) Log("Peer authentication problem ignored");

	mime_ParseHeaderValues(0);				// Don't want to parse header values
	mime=GetIOMIME(io);
	if (!mime) Fatal("Malformed MIME received");
	SetIncomingHost(mime);

	ParseAddress(szURI, &szAddr, &szNames, &szValues);

	szDelete(mi_caller);
	mi_caller=hprintf(NULL, "%s:%s", g_Organisation, g_Product);
//Log("ParseAddress(%s,%s,%s,%s)", szURI, szAddr, "...", "...");
	if (!strcasecmp(szAddr, "/rpc") || !strncasecmp(szAddr, "/rpc/", 5)) {
		if (!strcasecmp(szAddr, "/rpc")) Fatal("No API name specified in /rpc call");
		const char *szBody = mime_GetBodyText(mime);
		const char *szApi = szAddr+5;

		const char *szSessionId = mime_GetHeader(mime, "X-MTRPC-SESSION-ID");
		const char *szSessionToken = mime_GetHeader(mime, "X-MTRPC-SESSION-TOKEN");
		const char *szProductVersion = mime_GetHeader(mime, "X-MTRPC-PRODUCT-VERSION");

		const char *szNow = SqlTimeStamp(0, 1);

		if (szProductVersion) {
			szDelete(mi_caller);
			mi_caller=hprintf(NULL, "%s:%s:%s", g_Organisation, g_Product, szProductVersion);
		}

		if (szSessionId) {
			szDelete(mi_session);
			mi_session = strdup(szSessionId);
		}

		if (!g_Organisation || !g_Product) {
			FatalHttp(io, 400, "Organisation and Product must be specified in rpc call");
		}
		g_cbacId = cbac_GetId(g_OrganisationProduct);

		if (!g_cbacId && !isPromiscuous()) FatalHttp(io, 403, "Access denied to %s (no contract)", g_OrganisationProduct);

		if (!szProductVersion) FatalHttp(io, 400, "RPC call must have product version in header X-MTRPC-PRODUCT-VERSION");
		int versionAllowed = cbac_AllowedVersion(g_OrganisationProduct, szProductVersion);

		contract_t *c = contract_Find(g_OrganisationProduct);
		if (!c) FatalHttp(io, 403, "Access denied - no contract for %s", g_OrganisationProduct);

		if (c->szValidTo && strcmp(szNow, c->szValidTo) > 0) {
			FatalHttp(io, 403, "Access denied - contract for %s expired at %s", g_OrganisationProduct, c->szValidTo);
		}
		if (c->szValidFrom && strcmp(szNow, c->szValidFrom) < 0) {
			FatalHttp(io, 403, "Access denied - contract for %s not valid until %s", g_OrganisationProduct, c->szValidFrom);
		}
		if (!versionAllowed) FatalHttp(io, 403, "Access denied to %s (version %s not in contract)", g_OrganisationProduct, szProductVersion);

		if (c->limits) {				// Contract level limits
			const char *error = cbac_CanRunApi(g_OrganisationProduct, "any", 1);
			if (error) FatalHttp(io, 403, "Call failed: %s", error);
		}
		const char *error = cbac_CanRunApi(g_OrganisationProduct, szApi, 1);		// Check other restrictions on API
		if (error) FatalHttp(io, 403, "Call failed: %s", error);

		const char *szContentType=mime_GetContentType(mime);
		if (!szContentType) szContentType="text/xml";
		if (strcmp(szContentType, "text/xml")) FatalHttp(io, 400, "RPC call Content-Type must be text/xml (not %s)", szContentType);

		Log("Security cleared for %s's %s v%s", g_Organisation, g_Product, szProductVersion);
		if (szBody) {
			rogxml *rxRpcResult = NULL;

			rogxml *rxRpc;
			if (*szBody) {
				rxRpc = rogxml_FromText(szBody);
			} else {
				rxRpc = rogxml_FromText("<spider/>");
			}

			if (rogxml_ErrorNo(rxRpc)) {
				FatalHttp(io, 501, "Error %d - %s", rogxml_ErrorNo(rxRpc), rogxml_ErrorText(rxRpc));
			} else {
				mi_function = strdup(szApi);
				InfoStr("API-FUNCTION", szApi);
				Note("Z|%s", szApi);

				rogxml_SetAttr(rxRpc, "_function", szApi);
				const char **pname = szNames;
				const char **pvalue = szValues;
				while (*pname) {
					rogxml_SetAttr(rxRpc, *pname, *pvalue);
					pname++;
					pvalue++;
				}

				NoteMessageXML(rxRpc, "rcv", "Caller request (XML)", 0);

				EnvSet("RPC_SESSION_ID", szSessionId);
				EnvSet("RPC_SESSION_TOKEN", szSessionToken);
				EnvSet("RPC_PRODUCT_VERSION", szProductVersion);

				rxRpcResult=SPIDERRPCFunction(rxRpc);
				NoteMessageXML(rxRpcResult, "rpc-out", "RPC response", 'R');

				int nErr = rogxml_ErrorNo(rxRpcResult);
				if (nErr) {						// We've returned an error, which will be a system error
					const char *szErr = rogxml_ErrorText(rxRpcResult);

					Log("RPC Error %d: %s", nErr, szErr);
					rogxml *rxResult=SPIDERSystemError(nErr, szErr);

					// Need to put the API version number into the error
					const char *szVersion = rogxml_GetAttr(rxRpcResult, "version");
					if (szVersion) rogxml_SetAttr(rxResult, "version", szVersion);

					rogxml_Delete(rxRpcResult);
					rxRpcResult=rxResult;
				}

				if (g_szRpcError) {
					rogxml_SetAttr(rxRpcResult, "errorlog", NoteDirSuffix());
				}
			}
			if (rxRpcResult) {
				const char *szResult = rogxml_ToText(rxRpcResult);

				if (szResult) {
					SendHttpHeader(io, 200, "text/xml", strlen(szResult));

					MyBIO_puts(io, szResult);

					Log("Returned result of %d bytes", strlen(szResult));
					szDelete(szResult);
				} else {
					Log("Could not render returned result");
				}
			}
		}
	} else if (!strcasecmp(szAddr, "/mtrpc")) {
		const char *szBody = mime_GetBodyText(mime);
		const char **name;
		const char **value;
		int i;
		int bBrowser=0;						// 1 if we're coming in via a browser and hence want to present a form etc.
		char bDebug=0;						// 1 if we want to log progress
		const char *szXML = NULL;
		const char *szContentType=mime_GetContentType(mime);
		char bEncoded = !!stristr(szContentType, "urlencoded");	// All browsers use "application/x-www-form-urlencoded"
		int parts=0;

		if (bEncoded) {
			parts = ParseURI(szBody, &name, &value, 1);
		} else {
			szXML = szBody;
		}

		const char *szDebug = mime_GetHeader(mime, "X-MTRPC-DEBUG");
		if (szDebug) bDebug=atoi(szDebug);

		const char *szAccept = mime_GetHeader(mime, "Accept");		// Return an HTTP header if we receive an 'Accept'

		for (i=0;i<parts;i++) {
			if (!stricmp(name[i], "debug")) {
				bDebug=1;
			} else if (!stricmp(name[i], "XML")) {
				szXML=value[i];
			} else if (!stricmp(name[i], "browser")) {
				bBrowser=atoi(value[i]);
			}
		}

bDebug=1;
		NoteInhibit(!bDebug);										// Only record notes if we're debugging

		if (bBrowser) {
			SendHttpHeader(io, 200, "text/html", -1);
			SendHtmlHeader(io);
			RpcEntryForm(io, bDebug, "", szXML, "");
			if (bDebug) {
				ioprintf("Message log (%s) is <a href=%s>here</a>",
						NoteDirSuffix(), Link("/getinteraction/%s", NoteDirSuffix()));
			}
			BIO_putf(io, "<hr>");
		}

		if (szXML) {
			rogxml *rxRpc = rogxml_FromText(szXML);
			rogxml *rxResult = NULL;

			NoteMessageXML(rxRpc, "rcv", "Caller request (XML)", 0);
			if (rogxml_ErrorNo(rxRpc)) {
				rxResult=SPIDERSystemError(rogxml_ErrorNo(rxRpc), rogxml_ErrorText(rxRpc));
			} else {
				rxResult=SPIDERRpc(rxRpc);			// The actual RPC call is made here
			}
			if (rxResult) {
				const char *szResult = NULL;

				if (bBrowser) {
					rogxml *rx=rogxml_FindFirstChild(rogxml_FindFirstChild(rxResult));

					if (!stricmp(rogxml_GetLocalName(rx), "html")) {
						szResult = rogxml_ToText(rx);
					} else {
						const char *szText = rogxml_ToText(rxResult);

						szResult = hprintf(NULL, "<xmp>%s</xmp>", szText);
						szDelete(szText);
					}
				} else {
					szResult = rogxml_ToText(rxResult);
				}

				// Already have a header if we're a browser request
				if (szAccept && !bBrowser) SendHttpHeader(io, 200, "text/xml", strlen(szResult));

				MyBIO_puts(io, szResult);
			} else {
				if (bBrowser) {
					ioprintf("Strangely, there is no result at all...");
				}
			}
		}
	} else {
		Fatal("Received posted MIME message to '%s' and don't know what to do with it...", szURI);
	}

	return 0;
}

static int ReceiveHTTP(BIO *io, const char *szMethod, const char *szURI, const char *szVersion)
// Dispatcher for all incoming HTTPS messaging
{
	int n;

//	InfoStr("CLIENT-IP",_szSenderIp);
//	InfoInt("CLIENT-PORT",_nIncomingPort);

	Note("S|%s|%d", _szSenderIp, _nSenderPort);
	Note("H|REQUEST (%d)", _nIncomingPort);

	if (!stricmp(szMethod, "GET")) {		n=DealWithGET(io, szURI); }
	else if (!stricmp(szMethod, "PUT")) {	n=DealWithPUT(io, szURI); }
	else if (!stricmp(szMethod, "POST")) {	n=DealWithPOST(io, szURI); }
	else {
		Log("Unrecognised HTTP method: '%s'", szMethod);
		NoteInhibit(1);
		n=0;
	}

	return n;
}

void timer_Start()
{
	gettimeofday(&mi_tv, NULL);
}

void timer_Stop()
{
	struct tms tms;
	struct timeval tv;

	gettimeofday(&tv, NULL);
	int secs = tv.tv_sec - mi_tv.tv_sec;
	int msecs = (tv.tv_usec - mi_tv.tv_usec)/1000;
	mi_msecs = secs*1000+msecs;

	mi_tms.tms_utime = tms.tms_utime - mi_tms.tms_utime;
	mi_tms.tms_stime = tms.tms_stime - mi_tms.tms_stime;
	mi_tms.tms_cutime = tms.tms_cutime - mi_tms.tms_cutime;
	mi_tms.tms_cstime = tms.tms_cstime - mi_tms.tms_cstime;
}

static void ServeRequest(BIO *io)
// Talk to the Client that's calling us
{
	char buf[1024];
	int nGot;					// Bytes got from a read

	timer_Start();

	Log("Serving request from %s", g_OrganisationProduct);
	nGot=MyBIO_gets(io, buf, sizeof(buf)-1);
//Log("Got initial %d bytes from caller", nGot);
	// The spec is very specific here but we'll be lax and allow extra spaces.
	// The spec (RFC 2616) recommends allowing a blank line at the top of the request
	// We don't allow that.
	if (nGot) {
		const char *szMethod = NULL;
		const char *szURI = NULL;
		const char *szVersion = NULL;
		char *szLine=buf;
		char *szSpace;
		char *chp;

		while (isspace(*szLine)) szLine++;
		chp=strchr(szLine, '\n');
		if (chp) *chp='\0';

		szSpace = strchr(szLine, ' ');			// Following method

		if (szSpace) {
			char *chp;
			szMethod=strnappend(NULL, szLine, szSpace-szLine);
			while (*szSpace == ' ') szSpace++;
			chp=szSpace;
			szSpace=strchr(chp, ' ');
			if (szSpace) {
				szURI=strnappend(NULL, chp, szSpace-chp);
			}
			while (*szSpace == ' ') szSpace++;
			szVersion=strdup(szSpace);
		}
		// szMethod, szURI and szVersion should now all be kosher
		if (szMethod && szURI && szVersion) {
			ReceiveHTTP(io, szMethod, szURI, szVersion);
			szDelete(szMethod);
			szDelete(szURI);
			szDelete(szVersion);
		}
	} else {
		Log("Incoming connection didn't say anything");
		NoteInhibit(1);
	}

	MyBIO_flush(io);

	timer_Stop();

//	LogMi();			// Done at Exit() now

	return;
}

static int tcp_ListenOn(int nPort)
// Creates an incoming port and listens on it.
// Listen on a TCP port and return a socket
// Returns	0...	Socket number
//			-1		Failed to make the socket (most odd)
//			-2		Couldn't bind (usually means something already has it)
//			-3		Listen failed (also most odd)
{
	int nSock;
	struct sockaddr_in sin;
	int val=1;

	if ((nSock=socket(AF_INET,SOCK_STREAM,0))<0) {
		SetError(1001, "Couldn't make socket for port %d (errno = %d)", nPort, errno);
		return -1;
	}

	memset(&sin,0,sizeof(sin));
	sin.sin_addr.s_addr=INADDR_ANY;
	sin.sin_family=AF_INET;
	sin.sin_port=htons(nPort);
	setsockopt(nSock,SOL_SOCKET,SO_REUSEADDR, &val,sizeof(val));

	if (bind(nSock, (struct sockaddr *)&sin, sizeof(sin))>=0) {
		if (listen(nSock, 5)) {
			SetError(1002, "Couldn't listen on socket for port %d (errno = %d)", nPort, errno);
			close(nSock);
			return -3;
		}
		return nSock;
	}

	return -2;
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

	memset(&addr,0,sizeof(addr));
	addr.sin_family=AF_INET;
	addr.sin_port=htons(nPort);

	// This call takes 75 seconds if there is no DNS...
	nStarted=time(NULL);
	hp=gethostbyname(szHost);
	nFinished=time(NULL);
	if (nFinished - nStarted > 5) {
		Log("Lookup of '%s' was slow (%d seconds) - DNS Problem?", szHost, nFinished-nStarted);
	}

	if (hp) {
		addr.sin_addr=*(struct in_addr*)hp->h_addr_list[0];
	} else {
		unsigned int ad=inet_addr(szHost);
		addr.sin_addr=*(struct in_addr*)&ad;
	}

	if((nSock=socket(AF_INET,SOCK_STREAM, IPPROTO_TCP))<0) {
		Log("Error: Couldn't create socket to connect to %s", szHost);
		return 0;
	}

	if(connect(nSock,(struct sockaddr *)&addr, sizeof(addr))<0) {
		Log("Error: Couldn't connect socket to '%s' (%s)", szHost, roginet_ntoa(addr.sin_addr));
		return 0;
	}

	return nSock;
}

int _nHttpStatusCode = 0;
const char *_szHttpStatusText = NULL;

int http_GetLastStatusCode()				{ return _nHttpStatusCode; }
const char *http_GetLastStatusText()		{ return _szHttpStatusText; }

static int LogSslError(SSL *ssl, int ret)
// Logs the error that occurred.
// Returns	0	It was some miscellaneous error
//			1	Dropped line
//			2	Broken pipe (roughly equivalent to dropping the line)
{
	int nErr2 = SSL_get_error(ssl, ret);
	int nErr3;
	const char *szErr;
	int nResult;

	switch (nErr2) {
	case SSL_ERROR_NONE:				szErr="NONE";	break;
	case SSL_ERROR_SSL:					szErr="SSL"; break;
	case SSL_ERROR_WANT_READ:			szErr="WANT_READ"; break;
	case SSL_ERROR_WANT_WRITE:			szErr="WANT_WRITE"; break;
	case SSL_ERROR_WANT_X509_LOOKUP:	szErr="WANT_X509_LOOKUP"; break;
	case SSL_ERROR_SYSCALL:				szErr="SYSCALL"; break;
	case SSL_ERROR_ZERO_RETURN:			szErr="ZERO_RETURN"; break;
	case SSL_ERROR_WANT_CONNECT:		szErr="WANT_CONNECT"; break;
	case SSL_ERROR_WANT_ACCEPT:			szErr="WANT_ACCEPT"; break;
	default:	szErr="Dunno"; break;
	}

	if (nErr2 == SSL_ERROR_SYSCALL && !ret) {
		nResult=1;								// The far end dropped the line neatly
	} else if (nErr2 == SSL_ERROR_SYSCALL && ret == -1 && errno == 32) {
		nResult=2;								// Broken pipe - the far end broke off mid-protocol?
	} else {
		nResult=0;
		Log("SSL Error ret = %d -> %d \"SSL_ERROR_%s\" (errno = %d)", ret, nErr2, szErr, errno);
	}

	while ((nErr3 = ERR_get_error())) {
		Log("SSL: %s", ERR_error_string(nErr3, NULL));
	}

	return nResult;
}

const char *getCN(X509 *cert)
{
	static const char *result = NULL;

	szDelete(result);
	result=NULL;

	X509_NAME *subjName = X509_get_subject_name(cert);

	if (subjName) {
		int idx = X509_NAME_get_index_by_NID(subjName, NID_commonName, -1 );
		X509_NAME_ENTRY *entry = X509_NAME_get_entry(subjName, idx);
		ASN1_STRING *entryData = X509_NAME_ENTRY_get_data(entry);
		unsigned char *utf8;
		int length = ASN1_STRING_to_UTF8(&utf8, entryData);
		result = strnappend(NULL, (char*)utf8, length);
		OPENSSL_free(utf8);
	} else {
		result = strdup("X509_get_subject_name failed");
	}

	return result;
}

time_t timeFromSqlTime(const char* t, int dst)
// Takes YYYY-MM-DD HH:MM:SS and turns it into a time_t
{
	if (!t || (strlen(t) != 19 && strlen(t) != 10)) return 0;

	struct tm time_st;
	time_st.tm_isdst=dst;
	time_st.tm_year = atoi(t) - 1900;
	time_st.tm_mon = atoi(t+5) - 1;
	time_st.tm_mday = atoi(t+8);
	if (strlen(t) > 10) {
		time_st.tm_hour = atoi(t+11);
		time_st.tm_min = atoi(t+14);
		time_st.tm_sec = atoi(t+17);
	} else {
		time_st.tm_hour = 0;
		time_st.tm_min = 0;
		time_st.tm_sec = 0;
	}

	return mktime(&time_st);
}

time_t timeFromSSLTime(const char* sslTime)
// I found this somewhere and changed it to be less offensive.
// It doesn't handle timezones properly but will do for our purposes
// which is changing the odd semi-numeric version of time found in certificates into a unix time
// The input string is 12 digits being 2 digits of year then 2 digits for each of month, mday, hour, min, sec.
{
	int part[6];

	int m = 0;
	int n;
	for(n = 0; n < 12; n += 2) {
		part[m++]=(sslTime[n]-'0')*10 + sslTime[n+1]-'0';
	}

	struct tm time_st;
	time_st.tm_year = (part[0] >= 70 ) ? part[0] : part[0]+100;
	time_st.tm_mon = part[1]-1;
	time_st.tm_mday = part[2];
	time_st.tm_hour = part[3];
	time_st.tm_min = part[4];
	time_st.tm_sec = part[5];

	return mktime(&time_st);
}

long longFromASN1_INTEGER(ASN1_INTEGER *a)
{
	int neg=0,i;
	long r=0;

	if (a == NULL) return(0L);

	i=a->type;
	if (i == V_ASN1_NEG_INTEGER) {
		neg=1;
	}else if (i != V_ASN1_INTEGER) {				// What we have isn't an integer
		return 0;
	}

	if (a->length > sizeof(long)) return LONG_MAX;	// return biggest if it is too big

	if (a->data == NULL) return 0;					// No bits at all

	for (i=0; i<a->length; i++) {					// Shift big-endian bytes in one by one
		r<<=8;
		r|=(unsigned char)a->data[i];
	}

	return neg ? -r : r;
}

time_t isRevoked(const char *ca, long serial)
// Returns the date the certificate was revoked or 0 if it isn't
{
	time_t result = 0;
	S3 *s3 = NULL;

	const char *szRevokeDir="/usr/mt/spider";
	s3 = s3_Openf("%s/certs/certs.sl3", szRevokeDir);

	if (s3) {
		s3it *q = s3_Queryf(s3, "SELECT effective FROM revocation WHERE ca=%s AND serial=%d", ca, serial);
		int err = s3_ErrorNo(s3);
		if (err == 1) {
			// Table doesn't exist, in which case we don't care
		} else if (err) {
		    const char *errstr = s3_ErrorStr(s3);
		    Log("SQL Error %d: %s", err, errstr);
		    Log("QRY: %s", s3_LastQuery(s3));
		} else if (q) {
			const char **row = s3it_Next(q);

			if (row) {
				const char *szDate = row[0];
				result = timeFromSqlTime(szDate, 1);
			}
			s3it_Destroy(&q);
		}
		s3_Destroy(&s3);
	}

	return result;
}


static int verify_callback_rcv(int preverify_ok, X509_STORE_CTX *ctx)
// We get called for successive depth values finishing at 0 when we have the actual peer certificate
// We check:
// * Certificate depth isn't greater than SSL_MAXDEPTH
// * Certificate hasn't been revoked
// * Certificate doesn't have a future 'notBefore' date
// * Certificate hasn't expired
//
// We specifically ignore:
// * X509_V_ERR_SELF_SIGNED_CERT_IN_CHAIN
// * X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT_LOCALLY
// * X509_V_ERR_CERT_UNTRUSTED
//
// We also record the following global stuff:
// g_ssl_subject[] - The subject of the certifiate at each depth ([0] = peer certificate)
// The result is that we return with '1' if everything is ok, 0 otherwise
// If 0 then we will have called X509_STORE_CTX_set_error() with the error code.
{
	char    buf[256];
	int     err, depth;

//Log("================== verify_callback_rcv(%d, %p)", preverify_ok, ctx);
	X509 *peer = X509_STORE_CTX_get_current_cert(ctx);
	depth = X509_STORE_CTX_get_error_depth(ctx);
	err = X509_STORE_CTX_get_error(ctx);

//Log("peer=%p, depth=%d, err=%d", peer, depth, err);

	if (err == X509_V_ERR_SELF_SIGNED_CERT_IN_CHAIN)			// 19 - NHS certs are self-signed
		err = 0;

	if (err == X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT_LOCALLY)	// 20 - We don't currently store any public CAs locally
		err = 0;

	if (err == X509_V_ERR_UNABLE_TO_VERIFY_LEAF_SIGNATURE)		// 21 - We need to do this or we can't speak to sites?
		err = 0;

	if (err == X509_V_ERR_INVALID_PURPOSE)						// 26 - We don't care what the certificate was for(?)
		err = 0;

	if (err == X509_V_ERR_CERT_UNTRUSTED)						// 27 - We will trust anyone
		err = 0;

	// Get the common name of the peer's certificate
	X509_NAME_get_text_by_NID( X509_get_subject_name(peer), NID_commonName, buf, sizeof(buf));
	const char *szPeerCn = strdup(buf);

	// Get the common name of the peer's certificate issuer (its parent)
	X509_NAME_get_text_by_NID( X509_get_issuer_name(peer), NID_commonName, buf, sizeof(buf));
	const char *szPeerParent = strdup(buf);

	long nSerial = longFromASN1_INTEGER(X509_get_serialNumber(peer));
	X509_NAME_oneline(X509_get_issuer_name(peer), buf, 256);
	time_t tRevoked = isRevoked(buf, nSerial);

Log("TLS certificate %d: %s out of %s (serial=%d)", depth, szPeerCn, szPeerParent, nSerial);
	if (depth >= SSL_MAXDEPTH) {
		Log("Peer certificate depth (%d) exceeds maximum (%d)", depth, SSL_MAXDEPTH);
		if (!err) err = X509_V_ERR_CERT_CHAIN_TOO_LONG;
	} else {
		X509_NAME_oneline(X509_get_subject_name(peer), buf, 256);
		szDelete(g_ssl_subject[depth]);						// Save the subject so the end application can see it
		g_ssl_subject[depth]=strdup(buf);
	}

	if (tRevoked && tRevoked < time(NULL)) {
		Log("Peer certificate was revoked at %s", NiceTime(tRevoked));
		if (!err) err = X509_V_ERR_CERT_REVOKED;
	}

	// Get the validity dates of the peer's certificate
	const char *szValidFrom = (const char *)peer->cert_info->validity->notBefore->data;
	const char *szValidTo = (const char *)peer->cert_info->validity->notAfter->data;
	time_t tValidFrom = timeFromSSLTime(szValidFrom);
	time_t tValidTo = timeFromSSLTime(szValidTo);
	time_t tNow = time(NULL);

	if (tValidFrom > tNow) {
		Log("Peer certificate not valid until %s", NiceTime(tValidFrom));
		if (!err) err = X509_V_ERR_CERT_NOT_YET_VALID;
	}

	if (tValidTo < tNow) {
		Log("Peer certificate expired at %s", NiceTime(tValidTo));
		if (!err) err = X509_V_ERR_CERT_HAS_EXPIRED;
	}

//	if (err || depth == 0)			// 0 is the base certificate, which is interesting and so is an error
	if (err)						// Only log this if there is an error, otherwise the log is too chatty
		Log("SSL peer(%s) is valid %s - %s", szPeerCn, NiceTime(tValidFrom), NiceTime(tValidTo));

	X509_STORE_CTX_set_error(ctx, err);		// NB. Might be setting it to 0 when it wasn't 0 before

	if (err) {
		Log("verify error %d (%s) for %s", err, X509_verify_cert_error_string(err), szPeerCn);
		return 0;
	} else {
		return 1;
	}
}

static void AcceptPlainConnection(int fd)
// Using the same protocol as an incoming TLS connection, but without the TLS...
{
	BIO *bio;

	Log("External HTTP connection on %d (from %s)", _nIncomingPort, _szSenderIp);

	InfoStr("CLIENT-IP",_szSenderIp);
	InfoInt("CLIENT-PORT",_nIncomingPort);
	InfoStr("CLIENT-TYPE","HTTP");

	Note("S|%s|%d", _szSenderIp, _nSenderPort);
	Note("H|HTTP (%d)", _nIncomingPort);

	bio=BIO_new_socket(fd, BIO_NOCLOSE);
	ioset(bio);								// So ioprintf() works

	SetOrganisationProduct("Microtest","Product");
	ServeRequest(bio);

	shutdown(fd, SHUT_WR);
	close(fd);

	Log("Dialog with caller ended nicely");

	Exit(0);				// Only a child and we're done
}

static void AcceptTlsConnection(int fd)
{
	SSL_CTX *ctx;
	SSL *ssl;
	BIO *sbio;
	int r;
	int nOk;
	BIO *io,*ssl_bio;
	X509 *peer;
	const char *szCheck;
	int nMode = 0;								// Caller certificate verification mode

	Log("TLS connection on %d from %s", _nIncomingPort, _szSenderIp);

	InfoStr("CLIENT-IP",_szSenderIp);
	InfoInt("CLIENT-PORT",_nIncomingPort);
	InfoStr("CLIENT-TYPE","HTTPS");

	Note("S|%s|%d", _szSenderIp, _nSenderPort);
	Note("H|HTTPS (%d)", _nIncomingPort);

	/* Build our SSL context*/
//Log("Build connection (fd=%d)", fd);
	ctx=ctx_New(GetCertificateDir(), _szSenderIp, PASSWORD);
	if (!ctx) {
		FatalSSL(ctx_Error());
	}
	SSL_CTX_set_session_id_context(ctx, (void*)s_server_session_id_context, sizeof(s_server_session_id_context));

	// Arrange that we ask the remote end for their certificate
	szCheck=config_GetSetString("verify-tls", "None", "Ask, Must, Once or None");

szCheck="A";
//Log("Set verification to '%s'", szCheck);
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

//Log("Verify certificate (%p,%p) (mode=%d, verify=%p)", ssl, sbio, nMode, verify_callback_rcv);
	SSL_CTX_set_verify(ctx,nMode,verify_callback_rcv);

//Log("Create SSL connection");
	ssl=SSL_new(ctx);

	SSL_set_verify(ssl, nMode, verify_callback_rcv);

	sbio=BIO_new_socket(fd, BIO_NOCLOSE);
	ioset(sbio);								// So ioprintf() works
//Log("SSL_set_bio(%p,%p,%p)", ssl, sbio, sbio);
	SSL_set_bio(ssl, sbio, sbio);

//Log("Accept connection on %p", ssl);
	if (((r=SSL_accept(ssl)) <= 0)) {
//Log("Got a bit further");
		int nOk = LogSslError(ssl,r);
		if (!nOk) {
			FatalSSL("SSL accept error (r=%d, errno=%d)", r, errno);
		} else {
			NoteInhibit(1);
			ctx_Delete(ctx);				// Tidy up
			if (nOk == 1) {
				Log("Calling SSL connection immediately hung up");
			} else {
				Log("Calling SSL connection broke the connection");
			}
			Exit(0);						// Only a child and we're done
		}
	}

//Log("Build IO channel");
	io=BIO_new(BIO_f_buffer());
	ssl_bio=BIO_new(BIO_f_ssl());
	BIO_set_ssl(ssl_bio,ssl,BIO_CLOSE);
	BIO_push(io,ssl_bio);

	SSL_get_verify_result(ssl);
	peer=SSL_get_peer_certificate(ssl);

//Log("About to check the received certificate");
// Here is where we need to check the name returned below against the expected CN of the caller.
	const char *szOrganisation = NULL;
	const char *szProduct = NULL;
	if (peer) {
		char buf[256];

		X509_NAME_get_text_by_NID(X509_get_subject_name(peer), NID_commonName, buf, sizeof(buf));
//		Log("Peer identified as '%s'", buf);
		const char *colon = strchr(buf, ':');
		if (colon) {
			szOrganisation = strnappend(NULL, buf, colon-buf);
			szProduct = strdup(colon+1);
			SetOrganisationProduct(szOrganisation, szProduct);
		} else {
			Fatal("Invalid Organisation:Product (%s) in certificate", buf);
		}


//		X509_NAME_oneline(X509_get_subject_name(peer), buf, 256);
//		Log("Issuer of certificate was '%s'", buf);

		X509_free(peer);
	} else {
		if (nMode & SSL_VERIFY_PEER) Log("Client has not provided a certificate");
		SetOrganisationProduct("Alien","Infiltrator");
	}

	ServeRequest(io);

	// Under UNIX, we did the SSL_shutdown() first then did a shutdown() if it returned non-0
	// Under Linux, this causes the browser to suffer so we simply do shutdown() then SSL_shutdown() now
	// Ok, we're done so we'll close the socket now
	nOk=0;//SSL_shutdown(ssl);
	if(!nOk){
		// If we called SSL_shutdown() first then we always get return value of '0'.
		// In this case, try again, but first send a TCP FIN to trigger the other side's
		// close_notify.
		shutdown(fd, SHUT_WR);
//		nOk=SSL_shutdown(ssl);	// Calling this makes the LINUX version drop out mysteriously
	}

	switch(nOk){
		case 1:		break;		// Ok
		case 0:		break;		// Tried twice to shut down, call it ok.
		case -1:
		default: FatalSSL("Shutdown failed (code=%d)", nOk);
	}

	SSL_free(ssl);

	close(fd);

	ctx_Delete(ctx);				// Tidy up

	Log("Dialog with secret caller ended nicely");

	Exit(0);				// Only a child and we're done
}

static rogxml *Greeting(const char *szIp, const char *szDescr)
// Generates a greeting XML message to send over the local connection
{
	rogxml *rxGreeting;
	rogxml *rx;
	int day, mon, year, hour, min, sec;
	time_t now=time(NULL);
	struct tm *tm = gmtime(&now);
	const char szMon[]="JanFebMarAprMayJunJulAugSepOctNovDec";

	for (mon=0;mon<12;mon++) if (!memcmp(szMon+mon*3, __DATE__, 3)) break;
	mon++;
	day=atoi(__DATE__+4);
	year=atoi(__DATE__+7);
	hour=atoi(__TIME__);
	min=atoi(__TIME__+3);
	sec=atoi(__TIME__+6);

	// Create and send the greeting message
	rxGreeting = rogxml_NewRoot();
	rogxml_SetLinefeedString("\r\n");
	rogxml_SetIndentString("  ");

	rxGreeting=rogxml_AddChild(rxGreeting, "SPIDER-Hello");
	rogxml_SetAttr(rxGreeting, "version", VERSION);
	rogxml_SetAttr(rxGreeting, "host", szHostname);
	rogxml_SetAttrf(rxGreeting, "compiled", "%04d%02d%02d%02d%02d%02d", year, mon, day, hour, min, sec);
	rogxml_AddTextChildf(rxGreeting, "servertime",
			"%04d-%02d-%02d %02d:%02d:%02d",
			tm->tm_year+1900, tm->tm_mon+1, tm->tm_mday,
			tm->tm_hour, tm->tm_min, tm->tm_sec);
	rx=rogxml_AddTextChild(rxGreeting, "peer", szDescr);
	rogxml_AddAttr(rx, "ip", szIp);

	return rxGreeting;
}

static rogxml *PutXmlIntoDropDir(rogxml *rx, const char *szFilename)
// Create a file with the XML in the drop directory, setting the modify time from 'delay' attribute if there is one
// If the filename is provided then that is used, otherwise we'll make one up
// Any file of the same name will be over-written
// Returns	An Ack message suitable for returning to the user or an xml Error
{
	const char *szDelay = rogxml_GetAttr(rx, "delay");
	const char *szGuid;
	const char *szTmp;
	const char *szDest;
	time_t tDelay=0;									// Default to not timestamping
	time_t now=time(NULL);
	rogxml *rxAck;

	if (szDelay) {
		const char *chp;
		int bNumeric=1;

		for (chp=szDelay;*chp;chp++) {					// Check if delay is purely numeric
			if (!isdigit(*chp)) {
				bNumeric=0;
				break;
			}
		}
		if (bNumeric) {
			tDelay=time(NULL)+atol(szDelay);
		} else {
			tDelay=DecodeTimeStamp(szDelay);
			if (!tDelay) {
				return rogxml_NewError(111, "Delay time (%s) not understood", szDelay);
			}
		}
	}

	if (szFilename) {									// We've been given a name
		const char *szDot = strrchr(szFilename, '.');
		if (!szDot) {									// No extension, odd
			szGuid=strdup(szFilename);
		} else {
			szGuid=strnappend(NULL, szFilename, szDot-szFilename);
		}
	} else {
		szGuid = guid_ToText(NULL);						// Choose a random name
	}
	szTmp = hprintf(NULL, "%s/%s.dropping", szOutDir, szGuid);
	szDest = hprintf(NULL, "%s/%s.xml", szOutDir, szGuid);

	Log("Writing file as %s.xml", szGuid);
	rogxml_WriteFile(rx, szTmp);
	if (tDelay) {										// We want to post-date the file
		struct utimbuf utb;

		utb.actime=now;
		utb.modtime=tDelay;
		utime(szTmp, &utb);
		Log("Delaying file until %s GMT", TimeStamp(tDelay));
	}

	unlink(szDest);					// Get rid of any previous copy
	if (rename(szTmp, szDest)) {	// Renamed .xml to .tmp ok
		Log("Errno=%d writing %s", errno, szDest);
		return rogxml_NewError(110, "Failed to rename file in drop dir");
	}

	rxAck = AckMessage(tDelay ? "delayed" : "immediate", szGuid, NULL, NULL);
	if (tDelay) {
		rogxml_SetAttr(rxAck, "due", TimeStamp(tDelay));
	}

	szDelete(szTmp);
	szDelete(szDest);
	szDelete(szGuid);

	return rxAck;
}

////////////// Mersenne Twister Functions

#define MT_LEN			624
#define MT_IA			397
#define MT_IB			(MT_LEN - MT_IA)
#define MT_TWIST(b,i,j)	((b)[i] & 0x80000000) | ((b)[j] & 0x7FFFFFFF)
#define MT_MAGIC(s)		(((s)&1)*0x9908B0DF)

static int mt_index = MT_LEN*sizeof(unsigned long);
static unsigned long mt_state[MT_LEN];

unsigned long mt_random(unsigned long nMax) {
	unsigned long * b = mt_state;
	int idx = mt_index;
	unsigned long s;
	int i;
	unsigned long long n64;				// Use a 64-bit to scale the result

	if (idx == MT_LEN*sizeof(unsigned long)) {
		idx = 0;
		i = 0;
		for (; i < MT_IB; i++) {
			s = MT_TWIST(b, i, i+1);
			b[i] = b[i + MT_IA] ^ (s >> 1) ^ MT_MAGIC(s);
		}
		for (; i < MT_LEN-1; i++) {
			s = MT_TWIST(b, i, i+1);
			b[i] = b[i - MT_IB] ^ (s >> 1) ^ MT_MAGIC(s);
		}

		s = MT_TWIST(b, MT_LEN-1, 0);
		b[MT_LEN-1] = b[MT_IA-1] ^ (s >> 1) ^ MT_MAGIC(s);
	}
	mt_index = idx + sizeof(unsigned long);

	n64=*(unsigned long *)((unsigned char *)b + idx);
	n64*=nMax;

	return (unsigned long)(n64 >> 32);
}

static void AcceptIpConnection(int fd, int sock)
{
	rogxml *rx;
	const char *szError;
	int nErr;
	const char *szDelay;						// Delay from 'delay="whatever"' attribute
	int nSendErr = 0;

	const char *szPeerDescr = allow_Allowed(_szSenderIp);

	InfoStr("CLIENT-IP",_szSenderIp);
	InfoInt("CLIENT-PORT",_nIncomingPort);
	InfoStr("CLIENT-TYPE","Hello");

	if (!szPeerDescr) {							// We don't know this peer
		const char *szReject;

		szReject = hprintf(NULL, "Connection from unknown peer (%s) rejected", _szSenderIp);

		Note("S|%s|%d", _szSenderIp, _nSenderPort);
		Note("H|Rejected connection from unknown peer on application port (%d)", _nIncomingPort);
		Log("Rejecting connection from unknown peer");
		SendError(fd, 101, 1, "%s", szReject);
		close(fd);
		ReallyExit(0);
	}

	//// Greet the incoming connection
	rx=Greeting(_szSenderIp, szPeerDescr);					// Say hello to the guy knocking on our door
	nErr=SendXML(fd, rx, "hello");
	if (nErr) {												// Fail to send 'hello', just bail out
		Log("Error %d sending initial hello", nErr);
		ReallyExit(0);
	}
	rogxml_DeleteTree(rx);

	//// Accept an XML message from it
	rx=ReceiveXML(fd, 30);						// Accept an XML message back

	Note("S|%s|%d", _szSenderIp, _nSenderPort);
	Note("H|App (%d) %s", _nIncomingPort, szPeerDescr);

	//// See if it translates and if not, send an error back and quit
	szError=rogxml_ErrorText(rx);
	if (szError) {
		Log("XML Error %d: %s", rogxml_ErrorNo(rx), rogxml_ErrorText(rx));
		SendError(fd, 102, 1, "%s", szError);
		close(fd);
		ReallyExit(0);
	}

	const char *szPacked = rogxml_GetAttr(rx, "packed");	// Forcing packed mode or not
	if (szPacked) {
		_bPackXML = atoi(szPacked);
		rogxml *rxPackedAttr=rogxml_FindAttr(rx, "packed");			// Pretend we didn't see it
		rogxml_Delete(rxPackedAttr);
		Log("Packed response is turned %s", _bPackXML ? "on" : "off");
	}

													// Get default debug state from the config file
	int bDebug = config_GetBool("debug", 0, "Set to YES/NO to force/inhibit logging messages in msglog");
	bDebug=rogxml_GetAttrInt(rx, "debug", bDebug);	// Allow the xml to override it
	NoteInhibit(!bDebug);							// Don't store notes unless we're debugging

	//// Log the message away
	NoteMessageXML(rx, "rcv", "Caller request (XML)", 0);		// Save log of message before translation

	szDelay=rogxml_GetAttr(rx, "delay");		// See if a delay has been set
	if (szDelay) {
		rogxml *rxDropped=PutXmlIntoDropDir(rx, NULL);
		int nErr = rogxml_ErrorNo(rxDropped);

		if (rxDropped && nErr) {
			SendError(fd, nErr, 1, "%s", rogxml_ErrorText(rxDropped));
		} else {
			Log("Dropped delayed message into drop dir");
			SendXML(fd, rxDropped, "dropping");
		}
		rogxml_Delete(rxDropped);
		close(fd);
		ReallyExit(0);
	}

	if (!strcmp(rogxml_GetLocalName(rx), "SPIDER-RPC")) {			// It's a Remote Procedure Call
		rogxml *rxResponse;

		rxResponse=SPIDERRpc(rx);
		SendXML(fd, rxResponse, "response");
		rogxml_Delete(rxResponse);
		close(fd);
		Log("Internal Connection Terminated Normally");

		return;
	}

	SendError(fd, nErr, 2, "Unrecognised message type '%s'", rogxml_GetLocalName(rx));

	shutdown(fd, SHUT_WR);
	close(fd);
	if (nSendErr) {
		Log("Internal Connection Terminated - lost contact");
	} else {
		Log("Internal Connection Terminated ok");
	}
	Exit(0);
}

void BackMeUp()
// Makes a copy of this executable as spider.135 (or whatever) depending on version number
{
	const char *szLeaf = strrchr(argv0, '/');
	szLeaf = szLeaf ? szLeaf+1 : argv0;

	if (strchr(szLeaf, '.')) return;				// We have a dot and hence are probably a versioned binary already

	if (strlen(VERSION) > 20) return;				// VERSION is silly length so don't try to backup

	char *dest = malloc(strlen(argv0)+22);			// My filename plus 20 chars to fit the version plus '.' and '\0'
	strcpy(dest, argv0);							// Copy in my name
	char *d=dest+strlen(dest);						// Point to the end of it

	*d++='.';										// Add a dot
	const char *chp=VERSION;						// We want to append this without any dots

	for (chp=VERSION;*chp;chp++) {
		if (*chp != '.') {
			*d++=*chp;
		}
	}

	*d='\0';

	if (access(dest, 0)) {							// It doesn't exist
		int err = CopyFile(argv0, dest);
		if (!err) {
			chmod(dest, 0755);
		} else {
			Log("Failed to back myself up as %s", dest);
		}
	}
	szDelete(dest);

	// Now make a link from /usr/bin to our binary if it's where we expect it to be
	const char *szDest = hprintf(NULL, "/usr/mt/%s/bin/%s", szLeaf, szLeaf);

	if (!access(szDest, 1)) {
		const char *szSrc = hprintf(NULL, "/usr/bin/%s", szLeaf);
		symlink(szDest, szSrc);											// Make symlink - don't check error - if we fail, we fail.
		szDelete(szSrc);
	}

	szDelete(szDest);
}

static void StartDaemon(char bRestart)
// Become a daemon...
// bRestart indicates that we expect a server to be already running
{
//	int sock_int;									// For internal port
//	int sock_ext;									// For external port
	pid_t pid;
	time_t now;
	struct tm *tm;
	FILE *fp;
	char buf[1000];
	int i;
	int nOldPid;
	int nChildPid;

	const char *szStdOut, *szStdErr;

	nOldPid = StopAnyPrevious();
	if (nOldPid && !bRestart) printf("%s: Previous server %d stopped\n", szMyName, nOldPid);

//	printf("Network ports = %d\n", nNetworkPorts); for (i=0;i<nNetworkPorts;i++) { printf("Port %d: (%d,%d,%d,%d,%d,%d,%d)\n", i, NetworkPort[i].nPort, NetworkPort[i].bEnabled, NetworkPort[i].nProtocol, (int)NetworkPort[i].tRetry, NetworkPort[i].nRetries, NetworkPort[i].nSock, NetworkPort[i].nCount); } exit(0);

	if ((nChildPid = fork())) {
		printf("New SPIDER daemon process (v%s) is %d\n", VERSION, nChildPid);
		ReallyExit(0);							// Drop into the background
	}

	if (!(fp=fopen(szPidFile, "w"))) {
	    Log("Cannot open '%s' to store my pid - exiting", szPidFile);
	    fprintf(stderr, "%s: Cannot open '%s' to store my pid\n", szMyName, szPidFile);
		ReallyExit(6);
	}

	setpgrp();										// Release the terminal

	szStdOut = config_GetFile("stdout", "etc/stdout", "Leave blank to not redirect stdout");
	szStdErr = config_GetFile("stderr", "etc/stderr", "Leave blank to not redirect stderr");
	if (*szStdErr) freopen(szStdErr, "w", stderr);
	if (*szStdOut) freopen(szStdOut, "w", stdout);
	szDelete(szStdOut);
	szDelete(szStdErr);

	Log("===========================================================================");
	Log("Spider " VERSION " for " OS " (Made " __TIME__ " on " __DATE__ ", using %s)", SSLeay_version(SSLEAY_VERSION));

	ReadEnvironmentFile("etc/environment");
	allow_Init();

	contract_ReadAll(1);

//	RefreshAccessDatabase();

	gethostname(szHostname, sizeof(szHostname));

	time(&now);
	tm=gmtime(&now);
	snprintf(buf, sizeof(buf), "%d started on %02d/%02d/%04d at %02d:%02d:%02d UTC\n",
	        getpid(),
	        tm->tm_mday, tm->tm_mon+1, tm->tm_year+1900, tm->tm_hour, tm->tm_min, tm->tm_sec);
	fputs(buf, fp);
	fclose(fp);
	chmod(szPidFile, 0600);

	for (i=SIGHUP;i<=SIGUSR2;i++) {
		if (i < 4 || i > 12) {					// We want 4-12 to core dump us for debugging...
			if (i == SIGHUP) {
				signal(i, SIG_IGN);                                 // A problem otherwise
			} else if (i == SIGPIPE) {
				signal(i, SIG_IGN);									// So writes on a broken connection error
			} else {
				signal(i, HandleSignal);                            // Catch whatever we can
			}
		}
	}

	for (i=0;i<nNetworkPorts;i++) {
		int nSock = tcp_ListenOn(NetworkPort[i].nPort);

		if (nSock < 0) Log("Socket setup error %d - %s", GetErrorNo(), GetErrorStr());

		if (nSock == -1) {								// Failed to make socket (we won't recover from this)
			NetworkPort[i].nSock=0;
			NetworkPort[i].nRetries=0;
			NetworkPort[i].bEnabled=0;
			Log("Failed to create a socket for port %d", NetworkPort[i].nPort);
		} else if (nSock == -2) {						// Couldn't bind the socket (probably already in use)
			NetworkPort[i].nSock=0;
			NetworkPort[i].nRetries=5;
			NetworkPort[i].bEnabled=0;
			NetworkPort[i].tRetry=time(NULL)+5;			// Start trying again in five seconds
		} else if (nSock == -3) {						// The listen failed, which is most unusual
			NetworkPort[i].nSock=0;
			NetworkPort[i].nRetries=5;
			NetworkPort[i].bEnabled=0;
			NetworkPort[i].tRetry=time(NULL)+5;			// Start trying again in five seconds
		} else {
			NetworkPort[i].nSock = nSock;
			NetworkPort[i].bEnabled=1;
		}
	}

	Log("SPIDER " VERSION " is ready to receive connections");
	for (;;) {
		int fd;
		const char *szDropped = NULL;			// Name of file collected from drop dir
		int nMaxSock=-1;
		int nSrc;
		int nSrcProtocol;
		int nSock;

		struct timeval timeout;
		fd_set fdsetin, fdseterr;

		FD_ZERO(&fdsetin); FD_ZERO(&fdseterr);
		for (i=0;i<nNetworkPorts;i++) {
			if (NetworkPort[i].bEnabled) {
				FD_SET(NetworkPort[i].nSock, &fdsetin);
				FD_SET(NetworkPort[i].nSock, &fdseterr);
				nMaxSock = max(nMaxSock, NetworkPort[i].nSock);
			}
		}

		timeout.tv_sec=0;						// Seconds
		timeout.tv_usec=500000;					// Microseconds		// 500,000 = 1/2 second

		select(nMaxSock+1, &fdsetin, NULL, &fdseterr, &timeout);

		for (i=0;i<nNetworkPorts;i++) {
			if (FD_ISSET(NetworkPort[i].nSock, &fdseterr)) {		// Error on the port...???
				Log("Error listening on port %d - temporarily disabling", NetworkPort[i].nPort);
				NetworkPort[i].bEnabled=0;
				NetworkPort[i].nRetries=5;
				NetworkPort[i].tRetry=time(NULL)+20;
			}
		}

		nSrc=-1;
		for (i=0;i<nNetworkPorts;i++) {
			if (FD_ISSET(NetworkPort[i].nSock, &fdsetin)) {			// Found out input
				nSrc=i;
				break;
			}
		}

		if (nSrc == -1) {							// Nothing interesting happening
			Idle();
			continue;
		}

		_nTotalConnections++;
		szDelete(_szSenderIp);						// IP it's come from
		szDelete(_szIncomingIp);					// IP that we appear as

		if (nSrc == SRC_DROPPED) {
			_szSenderIp=strdup("0.0.0.0");
			_nSenderPort=0;

			_szIncomingIp=strdup("0.0.0.0");
			_nIncomingPort=0;

			_nDroppedCount++;
			nSrcProtocol=SRC_DROPPED;
			Log("File dropped (%s)", szDropped);
			fd=-1;
		} else {
			struct sockaddr_in sin;

#ifdef __SCO_VERSION__
			size_t sin_len = sizeof(sin);
#else
			socklen_t sin_len=(socklen_t)sizeof(sin);
#endif
			NetworkPort[nSrc].nCount++;
			nSrcProtocol=NetworkPort[nSrc].nProtocol;
			nSock=NetworkPort[nSrc].nSock;

			if ((fd=accept(nSock,0,0)) < 0) {
				Log("Problem accepting (errno=%d) on port %d", errno, _nIncomingPort);
				continue;
			}

			memset(&sin,0,sizeof(sin));

			getpeername(fd, (struct sockaddr*)&sin, &sin_len);
			_szSenderIp = strdup((char*)roginet_ntoa(sin.sin_addr));
			_nSenderPort = ((sin.sin_port << 8) & 0xff00) | ((sin.sin_port >> 8) & 0xff);

			getsockname(fd, (struct sockaddr*)&sin, &sin_len);
			_szIncomingIp = strdup((char*)roginet_ntoa(sin.sin_addr));
			_nIncomingPort = ((sin.sin_port << 8) & 0xff00) | ((sin.sin_port >> 8) & 0xff);

			Log("Connection on network port %d", _nIncomingPort);
		}

		msc_MessageId();
		if ((pid=fork())) {						// Master
			// Remember the child process so we can take it with us when we die etc.
			child_Add(pid, _szSenderIp, NULL);
			Log("Child %d processing connection from %s", pid, _szSenderIp);
			if (fd != -1)
				close(fd);							// Child will take care of it
			szDelete(szDropped);				// We don't need it here
		} else {
			SetEnvironment(_szSenderIp);		// Sets the environment if a directory exists env/ip

			NoteInhibit(-1);
			bIsDaemon=0;						// We are but a child...
			_tChildStart=time(NULL);
			_nIncomingProtocol = nSrcProtocol;
			mi_protocol = strdup(ProtocolName(_nIncomingProtocol));
			switch (nSrcProtocol) {
			case SRC_IP:						// 'Plain' application connection
				AcceptIpConnection(fd, nSock);
				break;
			case SRC_TLS:						// TLS connection
				AcceptTlsConnection(fd);
				break;
			case SRC_PLAIN:						// Plain connection
				AcceptPlainConnection(fd);
				break;
			case SRC_DROPPED:					// Dropped file
//				AcceptFileConnection(szDropped);
				break;
			}
			Exit(0);
		}
	}
}

static void Usage(int n)
{
	fprintf(stderr, "Usage: %s -anpde\n", szMyName);
	fprintf(stderr, "Where:\n"
		"  Ok, this bit needs revising...\n"
//		"  -a type         Required authentication level\n"
//		"     r              Request authentication\n"
//		"     n              Need authentication\n"
//		"     z              Zero authentication required\n"
//		"  -p              Local port on which to listen (%d)\n"
//		"  -P              Remote port on which to listen (%d)\n",
//			nListenPort_int,
//			nListenPort_ext
		);

	Exit(n);
}

void RAND_seed(const char *, int);			// Not sure where this is actually defined

void msc_ArchiveOldDirs(int verbose)
{
	time_t now=time(NULL);
	struct tm *tm = gmtime(&now);

	int fromDay = tm->tm_mday;
	int fromMonth = tm->tm_mon+1;
	int fromYear = tm->tm_year % 100;

	int days = config_GetSetInt("archive_days", 90, "Number of days before archiving messages to .zip");
	msc_SubtractDays(days, &fromDay, &fromMonth, &fromYear);
	long fromCode = fromYear * 10000 + fromMonth * 100 + fromDay;

	if (verbose) printf("Archiving messages older than %d-%.3s-20%d...\n", fromDay, MonthNames[fromMonth], fromYear);
	DIR *dd_msglog=opendir(szMsgLogDir);

	if (dd_msglog) {
		struct dirent *d_Year;
		while ((d_Year = readdir(dd_msglog))) {
			const char *year = d_Year->d_name;

			if (strlen(year) == 2 && isdigit(year[0]) && isdigit(year[1])) {
				char yearDir[50];

				snprintf(yearDir, sizeof(yearDir), "%s/%s", szMsgLogDir, year);
				DIR *dd_Year=opendir(yearDir);
				if (dd_Year) {
					struct dirent *d_Month;
					while ((d_Month = readdir(dd_Year))) {
						const char *month = d_Month->d_name;

						if (strlen(month) == 2 && isdigit(month[0]) && isdigit(month[1])) {
							char monthDir[50];

							snprintf(monthDir, sizeof(monthDir), "%s/%s", yearDir, month);
							DIR *dd_Month=opendir(monthDir);
							if (dd_Month) {
								struct dirent *d_Day;
								while ((d_Day = readdir(dd_Month))) {
									const char *day = d_Day->d_name;
									if (strlen(day) == 2 && isdigit(day[0]) && isdigit(day[1])) {
										char dayDir[50];

										snprintf(dayDir, sizeof(dayDir), "%s/%s", monthDir, day);

										long thisCode = atoi(year)*10000 + atoi(month)*100 + atoi(day);
										if (thisCode < fromCode && msc_isDir(dayDir)) {
											const char *szCommand = hprintf(NULL,
													"zip -mrq %s/%s/%s/mmts-%s%s%s.zip %s/%s/%s/%s",
													szMsgLogDir, year, month,
													year, month, day,
													szMsgLogDirLeaf, year, month, day);
											if (verbose) printf("Archiving %s-%s-20%s\n", day, month, year);
											Log("Archiving %s-%s-20%s", day, month, year);
											system(szCommand);
											szDelete(szCommand);
										}
									}
								}
								closedir(dd_Month);
							}
						}
					}
					closedir(dd_Year);
				}
			}
		}
		closedir(dd_msglog);
	}
}

static void Idle()
// We arrive here a couple times a second.
{
	int childpid;
	int status;
	struct rusage rusage;
	static int nCounter = 0; // This gives the first value as 1 so the '% xx == 0' checks don't all trigger immediately

	nCounter++;

	if (bIsDaemon) {
		// Don't put anything that doesn't relate to time here - look further down

		if (nCounter & 1) {								// Once a second
			ReadEnvironmentFile("etc/environment");
		}

		while ((childpid = wait3(&status, WNOHANG, &rusage)) > 0) {
			int nChildren;
			int code = WEXITSTATUS(status);
			int signal = WTERMSIG(status);
			const char *szStatus;

			if (!code && !signal) {
				szStatus=strdup("exited OK");
			} else if (code) {
				szStatus = hprintf(NULL, "exited with error %d", code);
			} else {
				szStatus = hprintf(NULL, "died by signal %d (%s)", code, FullSignalName(code));
			}
			nChildren = child_Forget(childpid);
			Log("Child %d %s (%d child%s now active)", childpid, szStatus, nChildren, nChildren==1?"":"ren");
			szDelete(szStatus);
		}

		if (nCounter %20 == 2) {						// Every 10 seconds, first check 1 second in
			CheckWeAreDaemon();
		}

		if (nCounter %12 == 0) {						// Once a minute
			contract_ReadAll(0);
		}

		if (nCounter % 120 == 1) {						// Once a minute (day changes are in here)
			static int daycode = 0;
			time_t now = time(NULL);
			struct tm *tm = gmtime(&now);
			long newDaycode = tm->tm_year * 10000 + tm->tm_mon * 100 + tm->tm_mday;

			if (daycode != newDaycode) {
				if (daycode) {							// *****  Here for DAILY
					// just daily
					if (!fork()) {
						msc_ArchiveOldDirs(0);
						ReallyExit(0);
					}
				}
														// *****  Here for DAILY AND STARTUP
				usage_Rollover();
				file_Rollover(szLogFile, LOGFILE_LIMIT);
				file_Rollover(szMiFile, MIFILE_LIMIT);
				if (!fork()) {
					UsageDaily();						// Roll over usage figures
					ReallyExit(0);
				}
				daycode=newDaycode;
			}
		}
	}
}

void CallSpider(const char *szApi, char *argv[])
{
	int nSock;
	rogxml *rxGreeting;
	rogxml *rxAck;
	rogxml *rxResponse;
	const char *szHost;
	const char *szResponse;
	const char *szFilename;
	FILE *fpin = NULL;

	bIsDaemon = 0;											// So that fatal errors are less fatal...

	szHost = "127.0.0.1";									// Always local host for now

	rogxml *rxRequest = rogxml_NewElement(NULL, "SPIDER-RPC");
	rogxml_SetAttr(rxRequest, "_function", szApi);

	char **arg = argv;
	int argc=0;
	while (*arg) {
		if (argc > 1) {
			char *chp=strchr(*arg, '=');
			if (chp && chp > *arg) {
				char *attr = strnappend(NULL, *arg, chp-*arg);
				rogxml_SetAttr(rxRequest, attr, chp+1);
				printf("rogxml_SetAttr(%p, %s, %s)\n", rxRequest, attr, chp+1);
				szDelete(attr);
			}
		}
		arg++;
		argc++;
	}

	const char *szRequest = rogxml_ToText(rxRequest);

	nSock = tcp_Connect(szHost, PORT_HTTP);
	if (!nSock) {
		Fatal("Could not create a connection to %s:%d\n", szHost, PORT_HTTP);
	}

	BIO *io=BIO_new_socket(nSock, BIO_NOCLOSE);

	BIO_putf(io, "POST /mtrpc HTTP/1.1\r\n");
	BIO_putf(io, "Content-Length: %d\r\n", strlen(szRequest));
	BIO_putf(io, "\r\n");
	BIO_putf(io, "%s", szRequest);
	BIO_flush(io);

	char ch;
	int count=1;
	char buf[1024];
	int c;
	while ((c = MyBIO_getc(io)) != EOF) {
		write(1, &c, 1);
	}

	rogxml_Delete(rxRequest);
	szDelete(szRequest);

	close(nSock);
}

int main(int argc, char *argv[])
{
	extern char *optarg;
	int c;
	int nErr=0;
	const char *szCommand;
	int bSend = 0;												// 1 if we're just sending a message

	argv0 = argv[0];											// Used for IPC

	// TODO: Really need to replace this!
	RAND_seed("ewrlgfuwheglithvglithirhvliewjhflewiuhffru", 40);

	_nStartTime = time(NULL);

	const char *szNewBaseDir = NULL;

	while((c=getopt(argc,argv,"b:sv"))!=-1){
		switch(c){
			case 'b': szNewBaseDir = optarg;				break;	// Set base directory
			case 's': bSend=1;								break;	// Send a message over app port
			case 'v': bVerbose=1;							break;
			case '?': nErr++;								break;	// Something wasn't understood
		}
	}

	if (nErr) Usage(1);

	if (szNewBaseDir) {
		char buf[512];

		if (!getcwd(buf, sizeof(buf))) Fatal("I can't find my current directory!");

		szNewBaseDir = QualifyInDir(buf, szNewBaseDir);
		SetBaseDir(szNewBaseDir);
		szDelete(szNewBaseDir);
	} else {
		SetBaseDir(szDefaultBaseDir);
	}

	BackMeUp();

	// Set the default ports only if we're using the default base directory
	int bDefaultBase = !strcmp(szBaseDir, szDefaultBaseDir);
	const char *szPortsApp = config_GetSetString("port-app",bDefaultBase ? "4509" : NULL, "Application protocol ports (4509)");
	const char *szPortsHttps = config_GetSetString("port-https",bDefaultBase ? "4510" : NULL, "HTTPS protocol ports (4510)");
	const char *szPortsHttp = config_GetSetString("port-http",bDefaultBase ? "4511" : NULL, "HTTP protocol ports (4511)");

	AddPorts(SRC_IP, szPortsApp);
	AddPorts(SRC_TLS, szPortsHttps);
	AddPorts(SRC_PLAIN, szPortsHttp);
	if (!nNetworkPorts)
		Fatal("No ports defined for any protocols - Use port-app, port-http or port-https in %s", szConfigFile);

	if (bSend) {							// We simply want to send a message
		int nSock;
		rogxml *rxGreeting;
		rogxml *rxAck;
		rogxml *rxResponse;
		const char *szHost;
		const char *szResponse;
		const char *szFilename;
		int got;
		char buf[1024];
		FILE *fpin = NULL;

		szHost = szCommand=argv[optind++];
		if (!szHost) Fatal("Usage: %s -s host file", szMyName);

		szFilename = szCommand=argv[optind++];
		if (!szFilename) Fatal("Usage: %s -s host file", szMyName);

		fpin=fopen(szFilename, "r");
		if (!fpin) {
			Fatal("Cannot open file '%s' for input",szFilename);
		}
		nSock = tcp_Connect(szHost, PORT_APP);
		if (!nSock) {
			Fatal("Could not create a connection to %s:%d\n", szHost, PORT_APP);
		}
		rxGreeting=ReceiveXML(nSock, 30);						// Accept an XML message back
		if (bVerbose) {
			const char *szGreeting;

			szGreeting = rogxml_ToNiceText(rxGreeting);
			fwrite(szGreeting, strlen(szGreeting), 1, stderr);
			szDelete(szGreeting);
		}
		if (rogxml_ErrorNo(rxGreeting)) ReallyExit(2);				// Error, which we've just printed
		rogxml_Delete(rxGreeting);

		while ((got=fread(buf, 1, sizeof(buf), fpin))) {
			int nSent = write(nSock, buf, got);
			if (nSent != got) {
				fprintf(stderr, "Error %d sending %d bytes to %s (sent %d)\n", errno, got, szHost, nSent);
			}
		}
		fclose(fpin);

		rxAck=ReceiveXML(nSock, 30);						// Accept an XML message back
		if (!rxAck) Fatal("No acknowledgement received");
		if (bVerbose) {
			const char *szAck;

			szAck = rogxml_ToNiceText(rxAck);
			fwrite(szAck, strlen(szAck), 1, stderr);
			szDelete(szAck);
		}
		rogxml_Delete(rxAck);

		rxResponse=ReceiveXML(nSock, 190);						// Accept an XML message back
		if (!rxResponse) Fatal("No response received");
		szResponse = rogxml_ToNiceText(rxResponse);
		fwrite(szResponse, strlen(szResponse), 1, stdout);
		szDelete(szResponse);
		rogxml_Delete(rxResponse);

		close(nSock);
		ReallyExit(0);
	}

	szPidFile=config_GetFile("pidfile", "etc/spider.pid", "Unique file per instance");

	szCommand=argv[optind++];
	if (!szCommand) szCommand="status";

	s3_OnError(NULL, s3ErrorHandler);

	if (!stricmp(szCommand, "start")) {
		StartDaemon(0);
	} else if (!stricmp(szCommand, "restart")) {
		StartDaemon(1);
	} else if (!stricmp(szCommand, "stop")) {
		int nPid = StopAnyPrevious();
		if (nPid) {
			printf("Daemon %d stopped\n", nPid);
		} else {
			printf("Daemon was not running\n");
		}
	} else if (!stricmp(szCommand, "status")) {
		int nFilePid = GetRecordedDaemonPid();
		int nDaemonPid = GetDaemonPid();
		if (nFilePid && !nDaemonPid) {          // There is a pid in the file but no daemon
			printf("Daemon %d was running but has terminated unexpectedly\n", nFilePid);
		} else if (!nFilePid) {
		   printf("There is no daemon running\n");
		    return 0;
		} else {
			printf("Daemon is running as process %d\n", nFilePid);
		}
	} else {
		const char *szBinary = BinaryFromApi(szCommand);

		if (szBinary) {
			CallSpider(szCommand, argv);
		} else {
			Fatal("Not a recognised function (%s)", szCommand);
		}
	}

	ReallyExit(0);

	return 0;
}
