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

#define VERSION				"2.00"

// TODO: Environment switching according to subsidiary software version and other parameters
// TODO: Fix isRevoked once environment switching is done - also rationalise the rest of the certificate checking
// TODO: Look for session.create and snaffle the session ID for the spider.im file

// TODO: Make msglog hierarchical like mmts
// TODO: Allow: spider some.api params

// TODO: Look at rog_MkDir as it seems flawed (there are comments there)

// xTODO: Clever stuff with certificates
// xTODO: Link spider to /usr/bin/spider
// xTODO: Copy binary to /usr/spider/bin/spider.134 etc.

// TODO: In all responses need to include: spider version, API version, message tracking id
// TODO: Message payloads need to be available for a year (zipped?), and immediately available for a month
// TODO: Need to keep retention period information on log files - this can live in sqlite
// TODO: Provide the version of the API in the response.  Might be file date/what/ADL
// TODO: keep a sqlite table of information for each api.  Check this on each API invocation and update if
// the API has been updated.  Keep at least: lastupdatetime,modify time of API exe, ADL information
// TODO: Include an attribute in all replies that provides the version of the API (either date of modify
// or version taken from ADL).
//
// TODO: Plan for archiving messages:
// Store messages in a directory structure similar to that used by MMTS year/month/day
// Process each night to zip those that are not in the current or previous month - zip up a day at a time
//    Actually, do those whose modify time is older than 6 weeks so that unzipped folders are effectively cached
// When looking back at old messages, unzip any day that is referenced so it's like it was
// This will get tidied up again overnight.

// TODO: Present session IDs to callers as GUIDs (translate user.login response, translate back on the way in)
// TODO: Safeguards against bad callers:
// TODO:	Have a per-API limit of maximum calls within a certain time
// TODO:	Have a limit to how long an API should be allowed to run before being killed
// TODO:	Authentication for 3rd party callers
// TODO:	Perhaps don't allow 3rdparty calls except on HTTPS ports
// TODO: Provide _function="spider.enumerateAPIs" that lists all APIs and do something to return per-API ADL info
// TODO: When catching SIGQUIT, set a flag and exit cleanly (and set an alarm for 4 seconds and force quit)
// TODO: Tidy up interaction with browser that shows the browser session in the display
// TODO: Tidy up the list of messages further
// TODO: [ See GetAdl() ] Add -ADL checking (look for 'mtADLParamList' in the binary first, then call with -ADL)
// TODO: Manage persistent dir for each API
// [ADL_SUPPORTED] [INSTALL_SUPPORTED]
// xTODO: When an API terminates with a s signal, report the signal name

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

static int _bPackXML = 0;					// 1 to pack XML sent to application

#define	XML_MIME_TYPE		"text/xml"		// Something may prefer 'text/xml'

#ifndef max
#define max(a,b) ((a)>(b)?(a):(b))
#endif

static int _nAlarmNumber = 0;						// Alarm number - 0 disables returning long jump when timer triggers
static jmp_buf jmpbuf_alarm;						// Used when alarm goes off for timeouts

static time_t _tChildStart=0;						// Child start time for summary

static const char *szDefaultBaseDir = "/usr/mt/spider";
static const char *szMyName = "spider";
static char szHostname[50];

static const char *g_szRpcError = NULL;				// Error returned from RPC call

static int bHandlingSessions = 0;					// We handle session IDs

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

static const char *szLogDir = NULL;				// Where logs go
static const char *szLogFile = NULL;			// The file into which logs are written...
static const char *szOutDir = NULL;				// Outgoing messages
static const char *szRpcDir = NULL;				// Where RPC binaries live
static const char *szRpcDataDir = NULL;			// Where RPC binaries live
static const char *szMsgLogDir = NULL;			// Logged messages

static const char *szPidFile = NULL;				// Location of pid file

static char bVerbose = 0;							// Quiet unless told otherwise (used with -s and -S)

#define PASSWORD "mountain"					// The password to access our key

static BIO *bio_err = NULL;

static time_t _nStartTime;				// Time server started

#define CLIENT_AUTH_NONE		0
#define CLIENT_AUTH_REQUEST		1
#define CLIENT_AUTH_NEED		2

#include <compile>
static const char _id[] = "@(#)SPIDER version " VERSION " compiled " __DATE__ " at " __TIME__;

static char s_server_session_id_context[] = "context1"; 
//static int s_server_auth_session_id_context = 2;

static const char *_szPassword = NULL;
static const char *argv0 = NULL;			// The location of our executable

static const char *szBaseDir = NULL;
static const char *szEtcDir = NULL;
static const char *szConfigFile = NULL;

static const char *GetEtcDir()					{ return szEtcDir; }

static void Log(const char *szFmt, ...);
static void Exit(int nCode);

// Management information to be written to spider.mi
int mi_inhibit = 0;						// Set by internal processes to inhibit logging of MI on exit
struct timeval mi_tv;					// Clock ticks since start of ServeRequest()
int mi_msecs = 0;						// Number of total elapsed milliseconds
struct tms mi_tms;						// Clock details
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

	while (c=*szPlain++) {
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

	while (chp=strchr(src, '\\')) {
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

		while (got=fread(buf+offset, 1, sizeof(buf)-offset-1, fp)) {
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
	va_list ap;
	char buf[1000];

	va_start(ap, szFmt);
	vsnprintf(buf, sizeof(buf), szFmt, ap);
	va_end(ap);

	fprintf(stderr,"%s: %s\n",szMyName, buf);

	Log("Error: %s", buf);

	Exit(99);
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
	while (got=fread(buf, 1, sizeof(buf), fpSrc)) {
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

	if (szName) {
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

static const char *SqlTimeStamp(time_t t)
// Returns a YYYY-MM-DD HH:MM:SS style timestamp for the time given.  '0' means 'now'.
{
	static char buf[20];
	struct tm *tm;

	if (!t) time(&t);
	tm=gmtime(&t);

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

static const char *ReadLine(FILE *fd)
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
	int err = s3_Dof(db, "CREATE TABLE IF NOT EXISTS %S (%S)", table, defn);
	if (err) {
		Fatal("Error %d (%s) creating table %s", s3_ErrorNo(db), s3_ErrorStr(db), table);
	}
}

void s3Main_CreateTable(const char *table, const char *defn)
{
	int err = s3_Dof(s3MainDb, "CREATE TABLE IF NOT EXISTS %S (%S)", table, defn);
	if (err) {
		Fatal("Error %d (%s) creating table %s", s3_ErrorNo(s3MainDb), s3_ErrorStr(s3MainDb), table);
	}
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

time_t FileTime(const char *szFile)
// Returns the time of the last update to a file or 0 if the file doesn't exist.
{
	struct stat64 st;
	time_t mtime=0;

	if (!stat64(szFile, &st)) mtime=st.st_mtime;

	return mtime;
}

int CreateAccessDb(const char *szName)
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
	char upToDate = 1;						// 1 when everything checks out as being up to date
	const char *szSrcDir = "/usr/mt/spider/etc/static";
	const char *szDestDir = "/usr/mt/spider/tmp";
	const char *szUpdateDir = "/usr/mt/spider/etc/updates";
	struct stat st;
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
}

const char *cbac_GetId(const char *organisation, const char *product)
{
	S3STRING(dbAccess, id);
	const char *result;

	s3it *q = s3_MustQueryf(dbAccess, "SELECT id FROM productId WHERE organisation = %s AND product = %s", organisation, product);

	result = id ? strdup(id) : id;

	return result;
}

int cbac_AllowedVersion(const char *cbacId, const char *szVersion)
{
	S3LONG(dbAccess, count);

	s3_MustQueryf(dbAccess, "SELECT COUNT(id) AS count FROM productVersion WHERE id = %s AND version = %s", cbacId, szVersion);

	return count;
}

int cbac_CanRunApi(const char *cbacId, const char *szApi)
{
	S3LONG(dbAccess, count);

	s3_MustQueryf(dbAccess, "SELECT COUNT(id) AS count FROM productApis WHERE id = %s AND api = %s", cbacId, szApi);

	return count;
}

static int cmp_str(const void *a, const void *b)
{
	return strcmp(*(char**)a, *(char**)b);
}

static int cmp_strr(const void *a, const void *b)
{
	return strcmp(*(char**)b, *(char**)a);
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
			const char *szNo = rogxml_GetAttr(rxBuild, "no");
			const char *szDate = rogxml_GetAttr(rxBuild, "date");		// Only using these two at present
			const char *szTime = rogxml_GetAttr(rxBuild, "time");
			const char *szTag = rogxml_GetAttr(rxBuild, "tag");
			const char *szBranch = rogxml_GetAttr(rxBuild, "branch");
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

		if (!*next) NULL;											// Incomplete - final component is a dir

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
	const char *szStamp = strdup(SqlTimeStamp(mtime));

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

			const char *szNow = SqlTimeStamp(0);
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

const char *xBinaryFromApi(const char *szApi)
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

		if (!*next) NULL;											// Incomplete - final component is a dir

		chp=next+1;												// Move on to next component
	}

	return szBinary;
}

const char **GetDirEntries(const char **list, const char *prefix, const char *szDir)
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

		while (d=readdir(dir)) {
			const char *szFilename = hprintf(NULL, "%s/%s", szDir, d->d_name);
			const char *szPrefix = hprintf(NULL, "%s%s%s", prefix, *prefix?".":"", d->d_name);
			if (*d->d_name == '.') continue;

			if (IsDir(szFilename)) {
				list=GetDirEntries(list, szPrefix, szFilename);
				l=list;				// Need to re-count the list
				nlist=0;
				while (*l) { l++; nlist++; }
				szDelete(szPrefix);
			} else {
				RENEW(list, const char*, nlist+2);
				list[nlist]=szPrefix;
				list[nlist+1]=NULL;
				nlist++;
			}
			szDelete(szFilename);
		}
		closedir(dir);
	}

	return list;
}

const char **GetAllApis()
{
	const char **list=NEW(const char*,1);
	list[0]=NULL;

	list = GetDirEntries(list, "", szRpcDir);

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

static const char *config_GetDir(const char *szName, const char *szDef, const char *szComment)
// Reads a directory name from the config file and returns it, making sure it exists.
// If the string is not in the config file then 'szDef' is used instead
// Returns a heap-based string
{
	const char *szPath = config_GetFile(szName, szDef, szComment);

	rog_MkDir("%s", szPath);

	return szPath;
}

static char _bNoteInhibit = 0;
static char *_szDir = NULL;

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

static const char *NoteDir()
{
	static char bInHere = 0;				// Flag to stop re-entrancy problems

	if (!bInHere) {
		bInHere=1;

		if (!_szDir && !_bNoteInhibit) {
			_szDir=hprintf(NULL, "%s/%s", szMsgLogDir, NoteDirSuffix());
			rog_MkDir("%s", _szDir);
			Log("MAIN: Message log directory is %s", _szDir);
		}

		bInHere=0;
	}

	return _szDir;
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
	if (bOpt && _szDir) {				// Need to delete any that have been created already
		const char *szCommand = hprintf(NULL, "rm -rf %s", _szDir);
		system(szCommand);
		szDelete(szCommand);
		_szDir=NULL;
	}
}

static void Note(const char *szFmt, ...)
{
	if (!_bNoteInhibit) {
		va_list ap;
		char buf[256];
		FILE *fp;
		const char *szFilename;

		if (bIsDaemon) return;									// Master daemon process doesn't make notes

		szFilename = hprintf(NULL, "%s/info", NoteDir());

		va_start(ap, szFmt);
		vsnprintf(buf, sizeof(buf), szFmt, ap);
		va_end(ap);

		fp=fopen(szFilename, "a");
		if (fp) {
			fprintf(fp, "%s\n", buf);
			fclose(fp);
		}

		szDelete(szFilename);
	}
}

static void NoteMessage(const char *szMessage, int nLen, const char *szTag, const char *szExt, const char *szDescr)
{
	if (!_bNoteInhibit) {
		const char *szFilename;
		FILE *fp;

		if (bIsDaemon) return;

		szFilename=hprintf(NULL, "%s/%s.%s", NoteDir(), szTag, szExt);
		if (nLen == -1) nLen=strlen(szMessage);
		fp=fopen(szFilename, "w");
		if (fp) {
			fwrite(szMessage, nLen, 1, fp);
			fclose(fp);
		}
		szDelete(szFilename);

		Note("M|%s.%s|%s|%d", szTag, szExt, szDescr, nLen);
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

		szFilename=hprintf(NULL, "%s/stderr.txt", NoteDir());
		fp=fopen(szFilename, "w");
		if (fp) {
			int len = strlen(g_szRpcError);
			fwrite(g_szRpcError, len, 1, fp);
			fclose(fp);

			Note("M|stderr.txt|Error returned from RPC handler|%d", len);
			Note("e|%d byte%s", len, len==1?"":"s");
		}
		szDelete(szFilename);
		char *chp=strchr(g_szRpcError, '\n');
		if (chp) *chp='\0';
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
	if (!_bNoteInhibit) {
		const char *szFilename;
		const char *szDir;
		FILE *fp;

		if (bIsDaemon) return;

		szDir = NoteDir();
		if (!szDir) return;

		szFilename = hprintf(NULL, "%s/log", szDir);
		fp=fopen(szFilename, "a");
		if (fp) {
			struct tm *tm;
			struct timeval tp;
			int msecs;

			gettimeofday(&tp, NULL);
			tm = gmtime(&tp.tv_sec);
			msecs=tp.tv_usec / 1000;

			fprintf(fp, "%02d-%02d-%02d %02d:%02d:%02d.%03d ",
					tm->tm_mday, tm->tm_mon+1, tm->tm_year % 100,
					tm->tm_hour, tm->tm_min, tm->tm_sec,
					msecs);
			fprintf(fp, "%s\n", szText);
			fclose(fp);
		}

		szDelete(szFilename);
	}
}

static void LogMi()
// Writes the management information to a file
{
	if (mi_inhibit) return;
	if (bIsDaemon) return;

	const char *szLog = hprintf(NULL, "%s/spider.mi", szLogDir);

	FILE *fp = fopen(szLog, "a");

	if (fp) {
		time_t now=time(NULL);
		struct tm *tm = gmtime(&now);
		struct timeval tp;
		int msecs;

		gettimeofday(&tp, NULL);
		msecs=tp.tv_usec / 1000;

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
		fprintf(fp, " %s", mi_caller ? mi_caller : "(UNKNOWN)");								// Caller ID
		fprintf(fp, " %d", mi_status);															// Returned status
		fprintf(fp, " %s", mi_function ? mi_function : "(NONE)");								// Function / URI
		fprintf(fp, "\n");
		fclose(fp);
	}
}

static void Log(const char *szFmt, ...)
// Sends a log entry to both main and message logs
// If 'szFmt' starts "MAIN: " then only sends to the main log
// If 'szFmt' starts "MESSAGE: " then only sends to the message log
{
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
		if (!szLogFile) {
			szLogFile=hprintf(NULL, "%s/%s.log", szLogDir, szMyName);
			chmod(szLogFile, 0666);
		}

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

typedef struct childinfo_t {
	int			pid;			// Process ID of the child
	time_t		tStarted;		// When it started
	const char *szIp;			// IP that triggered the child
	const char *szId;			// Message ID the child is dealing with
} childinfo_t;

static int nPids=0;
static childinfo_t *aPid = NULL;

static void Idle();

static void Exit(int nCode)
	// Quits the daemon nice and tidily
{
	LogMi();

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

	exit(nCode);
}

static const char *InternalId()
// This forms part of the directory name and should be unique on this server.
{
	static const char *szId = NULL;

	if (!szId) {
		time_t now = time(NULL);
		struct tm *tm = gmtime(&now);
		static time_t lasttime=0;
		static int nIndex = 1;

		if (lasttime == now) {				// Index is number of calls this second
			nIndex++;
		} else {
			nIndex=1;
			lasttime=now;
		}

		szDelete(szId);
		szId=hprintf(NULL, "%02d%02d%02d-%02d%02d%02d-%s%02x-%s-%d",
				tm->tm_year % 100, tm->tm_mon+1, tm->tm_mday,
				tm->tm_hour, tm->tm_min, tm->tm_sec,
				pidstr(), nIndex,
				_szIncomingIp, _nIncomingPort);
	}

	return szId;
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

	// Everything from here on is logging...

	if (!_bNoteInhibit) {
		static BIO *lastio = NULL;
		const char *szNoteDir = NoteDir();
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
			if (fpnote) Note("M|%s.%s|%s|%d", szBase, "txt", "Data sent", -1);
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

	exit(0);		// Don't use the usual exit as it tidies up the .pid file that might be in use...!
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
	exit(0);											// Shouldn't ever get here
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

Log("Building TLS/SSL context using authentication in %s for %s", szConfigDir, szPrefix);

	const char *szCertCA="/usr/mt/spider/etc/spiderca.cert.pem";
	const char *szCertLocal="/usr/mt/spider/etc/spider.cert.pem";
	const char *szKeyLocal="/usr/mt/spider/etc/spiderca.key.pem";
	_szPassword=strdup(szPassword);
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

static SSL_CTX *xctx_New(const char *szConfigDir, const char *szPassword)
// Create a new SSL context and return it
// The directory passed should contain mt.ca, mt.cert and mt.key being the certificate authority chain, the
// local certificate and the key respectively.
// On error, returns NULL - call ctx_Error() to pick up the error message
// NB. We keep szCertCA, szCertLocal and szKeyLocal around in case SSL_CTX_load_verify_locations() needs
//		them after the function exits.
{
	const SSL_METHOD *meth;
	SSL_CTX *ctx;
	const char *szCertCA = NULL;
	const char *szCertLocal = NULL;
	const char *szKeyLocal = NULL;

	szDelete(szCertCA);						// Tidy up the memory if we've been this way before
	szDelete(szCertLocal);
	szDelete(szKeyLocal);

Log("Building TLS/SSL context using authentication in %s", szConfigDir);

	szCertCA=hprintf(NULL, "%s/%s", szConfigDir, "spiderca.cert.pem");
	szCertLocal=hprintf(NULL, "%s/%s", szConfigDir, "spider.cert.pem");
	szKeyLocal=hprintf(NULL, "%s/%s", szConfigDir, "spiderca.key.pem");

	szDelete(_szCtxError);
	_szCtxError = NULL;

	if (!bio_err) {				// Need to initialise
		SSL_library_init();
		SSL_load_error_strings();

		bio_err=BIO_new_fp(stderr,BIO_NOCLOSE);		// For writing errors
	}

	meth=SSLv23_method();							// Create our context
	ctx=SSL_CTX_new(meth);
	if (!ctx) {
		_szCtxError = hprintf(NULL, "Failed to create an SSL/TLS context");
	}

	if (!_szCtxError) {								// Get our certificates from the key file
		if (!(SSL_CTX_use_certificate_chain_file(ctx, szCertLocal))) {
			_szCtxError = hprintf(NULL, "Can't read local certificate file '%s'", szCertLocal);
		}
	}

	if (!_szCtxError) {								// Set the password
		_szPassword=szPassword;
		SSL_CTX_set_default_passwd_cb(ctx, cb_Password);

		if (!access(szKeyLocal, 4)) {
			if(!(SSL_CTX_use_PrivateKey_file(ctx, szKeyLocal, SSL_FILETYPE_PEM))) {
				_szCtxError = hprintf(NULL, "Can't read key file '%s'", szKeyLocal);
			}
		}
	}

	if (!_szCtxError) {								// Get the list of trusted certificates
		if(!(SSL_CTX_load_verify_locations(ctx, szCertCA,0))) {
			_szCtxError = hprintf(NULL, "Can't read CA list file '%s'", szCertCA);
		}
	}

	if (_szCtxError) {						// We came unstuck somewhere
		return NULL;
	} else {
		load_dh_params(ctx);				// Exits on failure

		return ctx;
	}
}

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

static const char *note_GetMessage(note_t *n, int i)
{
	if (!n) return NULL;
	return (i<1 || i>n->nMessages) ? NULL : n->pszMessage[i-1];
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

static void TableMessageLog(BIO *io, int nMessages, const char *szFilter)
{
	struct dirent *d;
	DIR *dir=opendir(szMsgLogDir);
	const char **pszDir = NEW(const char*, nMessages);
	int nDir = 0;
	int nCount = 0;									// Number of messages actually found
	int i;
	int nRow=0;

	while ((d=readdir(dir))) {
		if (*d->d_name == '.') continue;		// Skip '.' files
		nCount++;
		if (nDir < nMessages) {						// List isn't full so just add it in
			pszDir[nDir++]=strdup(d->d_name);
		} else {									// List is full so replace oldest
			int i;
			int nOldest=0;

			for (i=1;i<nDir;i++) {
				if (strcmp(pszDir[i], pszDir[nOldest]) < 0) {
					nOldest=i;
				}
			}
			strset(&(pszDir[nOldest]), d->d_name);
		}
	}
	closedir(dir);
	qsort(pszDir,nDir,sizeof(*pszDir),cmp_strr);

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
	MyBIO_puts(io, "<table cellpadding=\"3\" cellspacing=\"0\" border=\"0\" bgcolor=#ffdd77 width=100%>\n");
	if (nCount) {
		if (nDir != 1) {
			if (nCount > nMessages) {
				BIO_putf(io, "<tr><th colspan=13><font size=+2>Last %d messages...</font>", nDir);
			} else {
				BIO_putf(io, "<tr><th colspan=13><font size=+2>All %d messages...</font>", nDir);
			}
		} else {
			BIO_putf(io, "<tr><th colspan=13><font size=+2>Last message...</font>");
		}
	} else {
		BIO_putf(io, "<tr><th colspan=13><font size=+2>There are no messages to display...</font>");
	}
	MyBIO_puts(io, "<tr>\n");
	MyBIO_puts(io, "<th align=left>Date</th>");
	MyBIO_puts(io, "<th align=left>UTC</th>");
	MyBIO_puts(io, "<th>Re-run</th>");
	MyBIO_puts(io, "<th align=left>ID</th>");
	MyBIO_puts(io, "<th align=left>Function</th>");
	MyBIO_puts(io, "<th align=left>Returned</th>");
	MyBIO_puts(io, "<th align=left>Stderr</th>");
	MyBIO_puts(io, "<th align=left>IP</th>");
	MyBIO_puts(io, "<th align=left>Source</th>");
	MyBIO_puts(io, "</tr>\n");
	for (i=0;i<nDir;i++) {
		const char *szDir = pszDir[i];
		const char *szDescription = "none";			// These defaults are for the case where there is no notes dir
		const char *szTmp = NULL;
		char *szIP = strdup("-");
		const char *szZInfo = NULL;
		const char *szStderr = NULL;
		const char *szReturn = NULL;
		const char *szErrSize = NULL;
		const char *szCode = NULL;
		int nCode = -2;
		int bInclude;

		note_t *note=note_LoadMessage(szDir);
		if (note) {
			szDescription = note_FindEntry(note, 'H');
			if (!szDescription) szDescription="Unknown";
			szZInfo = note_FindEntry(note, 'Z');	// Extra descriptive text to take place of message type
			szStderr = note_FindEntry(note, 'E');	// First line of stderr
			szErrSize = note_FindEntry(note, 'e');
			szReturn = note_FindEntry(note, 'R');
			szTmp = note_FindEntry(note, 'S');
			szCode = note_FindEntry(note, 'C');		// Error code returned from RPC binary
			nCode = szCode ? atoi(szCode) : -1;		// -1 should be for things that errored out completely or old logs
		}

		if (szTmp) {
			char *chp;
			szIP=strdup(szTmp);
			for (chp=szIP;*chp;chp++) if (*chp == '|') *chp='\0';		// Don't care about peer port number
		}
		if (!szErrSize) szErrSize = "-";

//		if (!szReturn) szReturn=szStderr;
		if (!szReturn) szReturn="-";

		bInclude=1;
		if (szFilter && *szFilter) {
			bInclude=0;
			if (stristr(szIP, szFilter)) bInclude=1;
			if (stristr(szDir, szFilter)) bInclude=1;
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
#if 0
			if (szStderr) {
				if (nRow & 1) {
					BIO_putf(io, "bgcolor=\"ffbbbb\">");
				} else {
					BIO_putf(io, "bgcolor=\"ff9999\">");
				}
			} else {
				if (nRow & 1) {
					BIO_putf(io, "bgcolor=\"ffffff\">");
				} else {
					BIO_putf(io, "bgcolor=\"dddddd\">");
				}
			}
#endif
			BIO_putf(io, "<td nowrap=1>%.2s-%.2s-%.4s", szDir+6, szDir+4, szDir);
			BIO_putf(io, "<td>%.2s:%.2s:%.2s", szDir+8, szDir+10, szDir+12);
			BIO_putf(io, "<td nowrap=1><a href=\"/getinteraction/%s\">%s</a>", szDir, szDir);
			const char *rcv = hprintf(NULL, "msglog/%s/rcv.xml", szDir);
			if (!access(rcv, 4)) {
				BIO_putf(io, "<td><a href=\"/rerunform/%s\">Rerun</a></td>", szDir);
			} else {
				MyBIO_puts(io, "<td></td>");
			}
			szDelete(rcv);

			// Unfortunately, I chose two different names for the input files so we need to identify which one we mean
			const char *szInputFile=hprintf(NULL, "msglog/%s/rpc-in.xml", szDir);
			if (!access(szInputFile, 0)) {		// It's called rpc-in.xml
				BIO_putf(io, "<td nowrap=1><a href=\"/getfile/%s/rpc-in.xml\">%s</a></td>", szDir, szZInfo, szZInfo);
			} else {							// It's called rcv.xml
				BIO_putf(io, "<td nowrap=1><a href=\"/getfile/%s/rcv.xml\">%s</a></td>", szDir, szZInfo, szZInfo);
			}
			szDelete(szInputFile);

			// Returned
			if (nCode >= 0) {
				BIO_putf(io, "<td nowrap=1>%d (<a href=\"/getfile/%s/rpc-out.xml\">%s</a>)</td>", nCode, szDir, szReturn);
			} else {
				BIO_putf(io, "<td nowrap=1><a href=\"/getfile/%s/rpc-out.xml\">%s</a></td>", szDir, szReturn);
			}

			// Stderr

			const char *szStderrFile=hprintf(NULL, "msglog/%s/stderr.txt", szDir);
			if (!access(szStderrFile, 0)) {		// We have stderr text
				BIO_putf(io, "<td nowrap=1><a href=\"/getfile/%s/stderr.txt\" title=\"%s\">%s</a></td>", szDir, szStderr, szErrSize);
			} else {
				BIO_putf(io, "<td>");
			}
			szDelete(szStderrFile);
			BIO_putf(io, "<td><a href=\"/status?n=%d&f=%s\">%s</a></td>", nDir, szIP, szIP);
			BIO_putf(io, "<td>%s", szDescription);
		}

		szDelete(szIP);
	}
	MyBIO_puts(io, "</table>");
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
	nResult = (nGot == 1) ? (unsigned char)c : EOF;
//Log("Done getc read nGot=%d, nResult = %d, c='%c', errno=%d, flags=%x", nGot, nResult, (c>=20&&c<=126)?c:'-', errno, io->flags);

	// Everything from here on is logging...!

	if (!_bNoteInhibit) {
		szNoteDir = NoteDir();
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
			if (fpnote) Note("M|%s.%s|%s|%d", szBase, "txt", "Data received", -1);
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

static void TableNotes(BIO *io, note_t *note, const char *szDir)
{
	const char *tmp = note_FindEntry(note, 'S');				// Formatted as IP|port
	char *szCaller = tmp ? strdup(tmp) : strdup("Unknown");
	char *chp=strchr(szCaller, '|');
	if (chp) *chp='\0';
	const char *szFunction = note_FindEntry(note, 'F');
	if (!szFunction) szFunction="<None>";

	BIO_putf(io, "<table bgcolor=#000077 bordercolor=blue border=1 cellspacing=1 width=100%>\r\n");
	BIO_putf(io, "<tr bgcolor=#aa88ff><th colspan=2><font size=5>Details</font>");
	BIO_putf(io, "<tr><td bgcolor=#aa88ff>Directory<td bgcolor=#ffffaa>%s\r\n", szDir);
	BIO_putf(io, "<tr><td bgcolor=#aa88ff>Processed<td bgcolor=#ffffaa>");
		BIO_putf(io, "%.2s-%.2s-%.4s ", szDir+6, szDir+4, szDir);
		BIO_putf(io, "%.2s:%.2s:%.2s\r\n", szDir+8, szDir+10, szDir+12);
	BIO_putf(io, "<tr><td bgcolor=#aa88ff>Function<td bgcolor=#ffffaa>%s\r\n", szFunction);
	BIO_putf(io, "<tr><td bgcolor=#aa88ff>Caller<td bgcolor=#ffffaa>%s\r\n", szCaller);
	BIO_putf(io, "<tr><td bgcolor=#aa88ff>SPIDER ID<td bgcolor=#ffffaa>%s\r\n", note_FindEntry(note, 'J'));
	BIO_putf(io, "</table>\r\n");
}

static void TableNoteMessages(BIO *io, note_t *note, const char *szDir)
{
	int i;

	BIO_putf(io, "<table bgcolor=#000077 bordercolor=blue border=1 cellspacing=1 width=100%>\r\n");
	BIO_putf(io, "<tr bgcolor=#aa88ff><th colspan=2><font size=5>Call Processing</font>");
	BIO_putf(io, "<tr bgcolor=#aa88ff><th>Type<th>Description");
	for (i=1;i<=note_GetMessageCount(note);i++) {
		const char *szMessage = note_GetMessage(note, i);
		char *szCopy = strdup(szMessage+2);
		char *szFile = strtok(szCopy, "|");
		char *szDescr = strtok(NULL, "");
		char *szName = strtok(szFile, ".");
		char *szExt = strtok(NULL, ".");
		BIO_putf(io, "<tr>");
		BIO_putf(io, "<td bgcolor=#aa88ff>%s", szExt);
		BIO_putf(io, "<td bgcolor=#ffffaa><a href=\"/getfile/%s/%s.%s\">%s</a>",
				szDir, szName, szExt, szDescr);
		szDelete(szCopy);
	}
	BIO_putf(io, "</table>");
}

static void TableNoteLogs(BIO *io, note_t *note, const char *szDir)
{
	const char *szLog = hprintf(NULL, "%s/%s/log", szMsgLogDir, szDir);
	FILE *fp=fopen(szLog, "r");

	if (fp) {
		const char *szLine;

		BIO_putf(io, "<table border=1 cellspacing=0 width=100%>\r\n");
//		BIO_putf(io, "<tr bgcolor=#44aaff><th colspan=3><font size=5>Log</font>\r\n");
//		BIO_putf(io, "<tr bgcolor=#44aaff><th>Date<th>Time<th>Event\r\n");
		BIO_putf(io, "<tr bgcolor=#44aaff><th>Time<th>Event\r\n");
		while ((szLine=ReadLine(fp))) {
			int bError = !!stristr(szLine, "error");
			char *szLineCopy = strdup(szLine);
			/* char *szDate= */strtok(szLineCopy, " ");		// We don't use the date any more, but need to skip it
			char *szTime=strtok(NULL, " ");
			char *szEvent=strtok(NULL, "");
//			BIO_putf(io, "<tr><td>%s<td>%s<td>%s", szDate, szTime, szEvent);
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

	BIO_putf(io, 
		"<META HTTP-EQUIV='Pragma' CONTENT='no-cache'>"
		"<hr>"
		"<table border=0 width=100%% bgcolor=#f57023>"
		"<tr><td rowspan=2 colspan=2 align=center><font size=6 color=white>SPIDER v" VERSION " - RPC Server</font>"
		"<td bgcolor=#2b509a><font size=3 color=white> &nbsp; %s</font>"
		"<tr><td bgcolor=#2b509a><font size=3 color=white> &nbsp; %s</font>"
		"<tr bgcolor=#b8541b><td>"
		"| <a href=/><font color=white>Home</font></a> "
		"| <a href=/status><font color=white>Messages</font></a> "
		"| <a href=\"https://%s\"><font color=white>MMTS</font></a> "
		"| <a href=/mtrpc><font color=white>Test</font></a> "
		"| <a href=/apis><font color=white>APIs</font></a> "
		"|"
		"<td><font color=white> &nbsp; UTC is %d-%02d-%04d %02d:%02d:%02d</font>"
		"</table>"
		"<hr>",
		szHostname, _szIncomingIp,
		szHostname,
		tm->tm_mday, tm->tm_mon+1, tm->tm_year+1900,
		tm->tm_hour, tm->tm_min, tm->tm_sec);
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
	BIO_flush(io);

	return 0;
}

static int SendHttpXml(BIO *io, int nCode, rogxml *rx)
{
	const char *szContent = rogxml_ToText(rx);

	SendHttpContent(io, nCode, "text/xml", szContent);

	szDelete(szContent);

	return 0;
}

static int FatalHttp(BIO *io, int nCode, const char *szFmt, ...)
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
		BIO_putf(io, "<td class=\"api\"><a href=\"/mtrpc?api=%s\">%s</a></td>", *l, *l);
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

			szInfo = hprintf(szInfo, "<font size=\"4\" face=\"verdana\" color=\"#0088dd\">API: <b>%s</b></font><br/>"
					"%s<br/>", szApi, szDescription);
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

static int DealWithGET(BIO *io, const char *szURI, const char *szOrganisation, const char *szProduct)
{
	MIME *mime=GetIOMIMEHeader(io);
	const char *szAddr = NULL;						// Address part of szURI (always starts with '/')
	const char **szNames = NULL;					// Vector of names of parameters
	const char **szValues = NULL;					// Vector of values of parameters
	int nParams=0;									// Number of parameters

	NoteInhibit(1);

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
	} else if (szOrganisation) {
		Log("HTTP GET %s [%s:%s]", szURI, szOrganisation, szProduct);
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
			while (got=fread(buf, 1, sizeof(buf), fp))
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
	} else if (!strcasecmp(szAddr, "/status")) {
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

	} else if (!strncasecmp(szAddr, "/getinteraction/", 16)) {
		const char *szDir=szAddr+16;
		note_t *note=note_LoadMessage(szDir);

		SendHttpHeader(io, 200, NULL, -1);
		SendHtmlHeader(io);

		BIO_putf(io, "<table width=100%% bgcolor=#0077ff>\r\n");
		BIO_putf(io, "<tr><td align=center><font size=5 color=white>");
		BIO_putf(io, "Call from ");
		BIO_putf(io, "%s", note_FindEntry(note, 'H'));
		BIO_putf(io, " on %.2s-%.2s-%.4s", szDir+6, szDir+4, szDir);
		BIO_putf(io, ", %.2s:%.2s:%.2s UTC", szDir+8, szDir+10, szDir+12);
		BIO_putf(io, "</font>\r\n");
		BIO_putf(io, "</table>\r\n");
		BIO_putf(io, "<hr>\r\n");

		BIO_putf(io, "<table width=100%>");

		BIO_putf(io, "<tr><td valign=top>");
		TableNotes(io, note, szDir);
		BIO_putf(io, "<hr>");
		TableNoteMessages(io, note, szDir);

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

static int DealWithPUT(BIO *io, const char *szURI, const char *szOrganisation, const char *szProduct)
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

	if (!stricmp(szFunction, "enumerateApis")) {
		const char **list = GetAllApis();
		const char **l = list;
		int napi=0;

		rogxml *rxResult = rogxml_NewElement(NULL, "result");		// Element to hang results from

		while (*l) {
			rogxml *rx=rogxml_AddTextChild(rxResult, "api",*l);
			rogxml_SetAttr(rx, "binary",api_Binary(*l));
			napi++;
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

static rogxml *SPIDERRPCFunction(rogxml *rxRpc, const char *szOrganisation, const char *szProduct)
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

	// Create three pipes for stdin, stdout and stderr
	if( pipe(fdin) == -1 || pipe(fdout) == -1 || pipe(fderr) == -1 ) {
		const char* err = strerror(errno);
		return rogxml_NewError(508, "Error %d creating pipe: %s", errno, err);
	}
	const char *szPort = hprintf(NULL, "%d", _nIncomingPort);
	const char *szProtocol = hprintf(NULL, "%s", ProtocolName(_nIncomingProtocol));
	const char *cbacId = cbac_GetId(szOrganisation, szProduct);

	EnvSet("SPIDER_VERSION", VERSION);
	EnvSet("SPIDER_BASEDIR", szBaseDir);
	EnvSet("SPIDER_RPCDIR", szRpcDir);
	EnvSet("SPIDER_RPCDATADIR", szRpcDataDir);
	EnvSet("SPIDER_SERVER", argv0);
	EnvSet("SPIDER_HOST", _szIncomingHost);
	EnvSet("SPIDER_IP", _szIncomingIp);
	EnvSet("SPIDER_PORT", szPort);
	EnvSet("SPIDER_PROTOCOL", szProtocol);

	EnvSet("RPC_SUPPLIER", szOrganisation);				// Should be ascertained from the TLS certificate
	EnvSet("RPC_PRODUCT", szProduct);
	// RPC_PRODUCT_VERSION is set elsewhere
	EnvSet("RPC_CBACID", cbacId);

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
		Note("M|stderr.txt|Error output from RPC|%d", nStderrLen);
		if (g_szRpcError) {											// Prepend any existing error message
			g_szRpcError = strappend(g_szRpcError, buf);
			szDelete(buf);
		} else {
			g_szRpcError = buf;
		}
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
Log("We have version '%s'", szVersion);

	// Stdout might contain a number of results and/or a number of errors, or something non-XML
	// This is where we interpret that stuff.
	int nStdoutLen = hbuf_GetLength(BufStdout);
//Log("We have stdout of length %d", nStdoutLen);
	if (nStdoutLen) {
		hbuf_AddChar(BufStdout, 0);									// Terminate the buffer
		const char * buf = hbuf_ReleaseBuffer(BufStdout);			// Get the buffer
		const char *next=SkipSpaces(buf);							// To allow reading of multiple elements
		rogxml *rxErrors = rogxml_NewElement(NULL, "errors");		// Element to hang errors from
		rogxml *rxResult = rogxml_NewElement(NULL, "result");		// Element to hang results from

		NoteMessage(buf, -1, "rpc-out2", "txt", "Raw output");

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
		rxRpcResult=SPIDERRPCFunction(rxRpc,"Microtest","Local");
		NoteMessageXML(rxRpcResult, "rpc-out", "Entire RPC response", 'R');

		int nErr = rogxml_ErrorNo(rxRpcResult);

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


static int DealWithPOST(BIO *io, const char *szURI, const char *szOrganisation, const char *szProduct)
// Post has come in over SSL/io addressed to szURI
{
	MIME *mime;

	const char *szAddr = NULL;						// Address part of szURI (always starts with '/')
	const char **szNames = NULL;					// Vector of names of parameters
	const char **szValues = NULL;					// Vector of values of parameters

	Log("HTTP POST %s from %s:%s", szURI, szOrganisation, szProduct);

//	bAuth=AuthenticatePeer(ssl, NULL, 1);
//	if (!bAuth) Fatal("POSTs only accepted from authenticated peers");
//	if (!bAuth) Log("Peer authentication problem ignored");

	mime_ParseHeaderValues(0);				// Don't want to parse header values
	mime=GetIOMIME(io);
	if (!mime) Fatal("Malformed MIME received");
	SetIncomingHost(mime);

	ParseAddress(szURI, &szAddr, &szNames, &szValues);

	szDelete(mi_caller);
	mi_caller=hprintf(NULL, "%s:%s", szOrganisation, szProduct);
//Log("ParseAddress(%s,%s,%s,%s)", szURI, szAddr, "...", "...");
	if (!strcasecmp(szAddr, "/rpc") || !strncasecmp(szAddr, "/rpc/", 5)) {
		if (!strcasecmp(szAddr, "/rpc")) Fatal("No API name specified in /rpc call");
		const char *szBody = mime_GetBodyText(mime);
		const char **name;
		const char **value;
		const char *szApi = szAddr+5;
		int i;

		const char *szSessionId = mime_GetHeader(mime, "X-MTRPC-SESSION-ID");
		const char *szSessionToken = mime_GetHeader(mime, "X-MTRPC-SESSION-TOKEN");
		const char *szProductVersion = mime_GetHeader(mime, "X-MTRPC-PRODUCT-VERSION");

		if (szProductVersion) {
			szDelete(mi_caller);
			mi_caller=hprintf(NULL, "%s:%s:%s", szOrganisation, szProduct, szProductVersion);
		}

		if (szSessionId) {
			szDelete(mi_session);
			mi_session = strdup(szSessionId);
		}

		if (!szOrganisation || !szProduct) {
			FatalHttp(io, 400, "Organisation and Product must be specified in rpc call");
		}
		const char *cbacId = cbac_GetId(szOrganisation, szProduct);

		if (!cbacId) FatalHttp(io, 403, "Access denied to %s:%s", szOrganisation, szProduct);

		if (!szProductVersion) FatalHttp(io, 400, "RPC call must have product version in header X-MTRPC-PRODUCT-VERSION");
		int versionAllowed = cbac_AllowedVersion(cbacId, szProductVersion);
		if (!versionAllowed) FatalHttp(io, 403, "Access denied to %s:%s, version %s", szOrganisation, szProduct, szProductVersion);

		int apiAllowed = cbac_CanRunApi(cbacId, szApi);
		if (!apiAllowed) FatalHttp(io, 403, "%s:%s is not authorised to access API '%s'", szOrganisation, szProduct, szApi);

		const char *szContentType=mime_GetContentType(mime);
		if (!szContentType) szContentType="text/xml";
		if (strcmp(szContentType, "text/xml")) FatalHttp(io, 400, "RPC call Content-Type must be text/xml (not %s)", szContentType);

		if (szBody) {
			rogxml *rxRpcResult = NULL;

			rogxml *rxRpc;
			if (*szBody) {
				rxRpc = rogxml_FromText(szBody);
			} else {
				rxRpc = rogxml_FromText("<spider/>");
			}
			rogxml *rxResult = NULL;

			if (rogxml_ErrorNo(rxRpc)) {
				FatalHttp(io, 501, "Error %d - %s", rogxml_ErrorNo(rxRpc), rogxml_ErrorText(rxRpc));
			} else {
				mi_function = strdup(szApi);
				Note("Z|%s", szApi);

				rogxml_SetAttr(rxRpc, "_function", szApi);
				const char **pname = szNames;
				const char **pvalue = szValues;
				while (*pname) {
					rogxml_SetAttr(rxRpc, *pname, *pvalue);
					pname++;
					pvalue++;
				}

				NoteMessageXML(rxRpc, "rcv", "XML sent to API", 0);

				EnvSet("RPC_SESSION_ID", szSessionId);
				EnvSet("RPC_SESSION_TOKEN", szSessionToken);
				EnvSet("RPC_PRODUCT_VERSION", szProductVersion);

				rxRpcResult=SPIDERRPCFunction(rxRpc, szOrganisation, szProduct);
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
				const char *szResult = NULL;

				szResult = rogxml_ToText(rxRpcResult);

				SendHttpHeader(io, 200, "text/xml", strlen(szResult));

				MyBIO_puts(io, szResult);
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
				ioprintf("Message log (%s) is <a href=\"/getinteraction/%s\">here</a>",
						NoteDirSuffix(), NoteDirSuffix());
			}
			BIO_putf(io, "<hr>");
		}

		if (szXML) {
			rogxml *rxRpc = rogxml_FromText(szXML);
			rogxml *rxResult = NULL;

			NoteMessageXML(rxRpc, "rcv", "Message from HTTP/S connection", 0);
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

static int ReceiveHTTP(BIO *io, const char *szMethod, const char *szURI, const char *szVersion, const char *szOrganisation, const char *szProduct)
// Dispatcher for all incoming HTTPS messaging
{
	int n;

	Note("S|%s|%d", _szSenderIp, _nSenderPort);
	Note("H|REQUEST (%d)", _nIncomingPort);
	Note("J|%s", InternalId());

	if (!stricmp(szMethod, "GET")) {		n=DealWithGET(io, szURI, szOrganisation, szProduct); }
	else if (!stricmp(szMethod, "PUT")) {	n=DealWithPUT(io, szURI, szOrganisation, szProduct); }
	else if (!stricmp(szMethod, "POST")) {	n=DealWithPOST(io, szURI, szOrganisation, szProduct); }
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
	clock_t elapsed = times(&tms);

	gettimeofday(&tv, NULL);
	int secs = tv.tv_sec - mi_tv.tv_sec;
	int msecs = (tv.tv_usec - mi_tv.tv_usec)/1000;
	mi_msecs = secs*1000+msecs;

	mi_tms.tms_utime = tms.tms_utime - mi_tms.tms_utime;
	mi_tms.tms_stime = tms.tms_stime - mi_tms.tms_stime;
	mi_tms.tms_cutime = tms.tms_cutime - mi_tms.tms_cutime;
	mi_tms.tms_cstime = tms.tms_cstime - mi_tms.tms_cstime;
}

static void ServeRequest(BIO *io, const char *szOrganisation, const char *szProduct)
// Talk to the Client that's calling us
{
	char buf[1024];
	int nGot;					// Bytes got from a read

	timer_Start();

	nGot=MyBIO_gets(io, buf, sizeof(buf)-1);
Log("Got initial %d bytes from caller", nGot);
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
			ReceiveHTTP(io, szMethod, szURI, szVersion, szOrganisation, szProduct);
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

time_t timeFromSqlTime(const char* t)
// Takes YYYY-MM-DD HH:MM:SS and turns it into a time_t
{
	if (!t || strlen(t) != 19) return 0;

	struct tm time_st;
	time_st.tm_year = atoi(t) - 1900;
	time_st.tm_mon = atoi(t+5) - 1;
	time_st.tm_mday = atoi(t+8);
	time_st.tm_hour = atoi(t+11);
	time_st.tm_min = atoi(t+14);
	time_st.tm_sec = atoi(t+17);

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
	for( int n = 0; n < 12; n += 2 )
	{
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

	const char *szEnvDir="/usr/mt/spider";
	s3 = s3_Openf("%s/certs/certs.sl3", szEnvDir);

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
				result = timeFromSqlTime(szDate);
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
	SSL    *ssl;

Log("================== verify_callback_rcv(%d, %p)", preverify_ok, ctx);
	X509 *peer = X509_STORE_CTX_get_current_cert(ctx);
	depth = X509_STORE_CTX_get_error_depth(ctx);
	err = X509_STORE_CTX_get_error(ctx);

Log("peer=%p, depth=%d, err=%d", peer, depth, err);

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

Log("Peer is %s out of %s (serial=%d)", szPeerCn, szPeerParent, nSerial);
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

	if (err || depth == 0)			// 0 is the base certificate, which is interesting and so is an error
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

	Note("S|%s|%d", _szSenderIp, _nSenderPort);
	Note("H|HTTP (%d)", _nIncomingPort);

	bio=BIO_new_socket(fd, BIO_NOCLOSE);
	ioset(bio);								// So ioprintf() works

	ServeRequest(bio, "Microtest", "Local");

	shutdown(fd, SHUT_WR);
	close(fd);

	Log("Natural end of external plain connection");

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
		Log("Peer identified as '%s'", buf);
		const char *colon = strchr(buf, ':');
		if (colon) {
			szOrganisation = strnappend(NULL, buf, colon-buf);
			szProduct = strdup(colon+1);
		}


//		X509_NAME_oneline(X509_get_subject_name(peer), buf, 256);
//		Log("Issuer of certificate was '%s'", buf);

		X509_free(peer);
	} else {
		if (nMode & SSL_VERIFY_PEER) Log("Client has not provided a certificate");
	}

	ServeRequest(io, szOrganisation, szProduct);

	// Under UNIX, we did the SSL_shutdown() first then did a shutdown() if it returned non-0
	// Under Linux, this causes the browser to suffer so we simply do shutdown() then SSL_shutdown() now
	// Ok, we're done so we'll close the socket now
	nOk=0;//SSL_shutdown(ssl);
	if(!nOk){
		// If we called SSL_shutdown() first then we always get return value of '0'.
		// In this case, try again, but first send a TCP FIN to trigger the other side's
		// close_notify.
		shutdown(fd, SHUT_WR);
		nOk=SSL_shutdown(ssl);
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

	Log("Natural end of external connection");

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

	if (!szPeerDescr) {							// We don't know this peer
		const char *szReject;

		szReject = hprintf(NULL, "Connection from unknown peer (%s) rejected", _szSenderIp);

		Note("S|%s|%d", _szSenderIp, _nSenderPort);
		Note("H|Rejected connection from unknown peer on application port (%d)", _nIncomingPort);
		Log("Rejecting connection from unknown peer");
		SendError(fd, 101, 1, "%s", szReject);
		close(fd);
		exit(0);
	}

	//// Greet the incoming connection
	rx=Greeting(_szSenderIp, szPeerDescr);					// Say hello to the guy knocking on our door
	nErr=SendXML(fd, rx, "hello");
	if (nErr) {												// Fail to send 'hello', just bail out
		Log("Error %d sending initial hello", nErr);
		exit(0);
	}
	rogxml_DeleteTree(rx);

	//// Accept an XML message from it
	rx=ReceiveXML(fd, 30);						// Accept an XML message back

	Note("S|%s|%d", _szSenderIp, _nSenderPort);
	Note("H|App (%d) %s", _nIncomingPort, szPeerDescr);
	Note("J|%s", InternalId());

	//// See if it translates and if not, send an error back and quit
	szError=rogxml_ErrorText(rx);
	if (szError) {
		Log("XML Error %d: %s", rogxml_ErrorNo(rx), rogxml_ErrorText(rx));
		SendError(fd, 102, 1, "%s", szError);
		close(fd);
		exit(0);
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
	NoteMessageXML(rx, "rcv", "Message from application", 0);		// Save log of message before translation

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
		exit(0);
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
		exit(0);							// Drop into the background
	}

	if (!(fp=fopen(szPidFile, "w"))) {
	    Log("Cannot open '%s' to store my pid - exiting", szPidFile);
	    fprintf(stderr, "%s: Cannot open '%s' to store my pid\n", szMyName, szPidFile);
		exit(6);
	}

	setpgrp();										// Release the terminal

	szStdOut = config_GetFile("stdout", "etc/stdout", "Leave blank to not redirect stdout");
	szStdErr = config_GetFile("stderr", "etc/stderr", "Leave blank to not redirect stderr");
	if (*szStdErr) freopen(szStdErr, "w", stderr);
	if (*szStdOut) freopen(szStdOut, "w", stdout);
	szDelete(szStdOut);
	szDelete(szStdErr);

	Log("===========================================================================");
	Log("Spider " VERSION " (Made "__TIME__" on "__DATE__", using %s)", SSLeay_version(SSLEAY_VERSION));

	ReadEnvironmentFile("etc/environment");
	allow_Init();

	RefreshAccessDatabase();

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
		timeout.tv_usec=500000;					// Microseconds

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

		if (nSrc == -1) {						// Nothing interesting happening
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

		if ((pid=fork())) {						// Master
			// Remember the child process so we can take it with us when we die etc.
			child_Add(pid, _szSenderIp, NULL);
			Log("Child %d processing connection from %s", pid, _szSenderIp);
			if (fd != -1)
				close(fd);							// Child will take care of it
			szDelete(szDropped);				// We don't need it here
		} else {
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
			exit(0);
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

			nChildren = child_Forget(childpid);
			Log("Child %d terminated (%d child%s now active)", childpid, nChildren, nChildren==1?"":"ren");
		}

		if (nCounter %20 == 2) {						// Every 10 seconds, first check 1 second in
			CheckWeAreDaemon();
		}

		if (nCounter % 12 == 0) {						// Once a minute (day changes are in here)
			static int day=-1;
			time_t now = time(NULL);
			struct tm *tm = gmtime(&now);

			if (tm->tm_mday != day) {
				day=tm->tm_mday;

				if (day == 1) {							// first of the month
				}

				if (!fork()) {							// New day - delete all but the most recent 5,000 logs
					int nMax = config_GetSetInt("maxmsglog",5000,"msglog is trimmed to this number each day");
					DIR *dir=opendir(szMsgLogDir);
					struct dirent *d;
					int ye=tm->tm_year+1900;
					int mo=tm->tm_mon;
					int da=tm->tm_mday;
					if (!mo) { ye--; mo=12; }			// The effect is 'this time last month'
					int nCount=0;
					int nDeleted=0;

					mi_inhibit=1;
					Log("Daily housekeeping of '%s'", szMsgLogDir);

					if (dir) {
						int nFiles=0;
						while (d=readdir(dir)) {
							if (!isdigit(*d->d_name)) continue;			// Only interested in dirs that start numeric
							nFiles++;
						}
						if (nFiles > nMax) {
							int i;

							rewinddir(dir);
							char **name=NEW(char*,nFiles);
							int count=0;

							while ((d=readdir(dir)) && count< nFiles) {		// Read in the dir names
								const char *szName = d->d_name;
								if (!isdigit(*szName)) continue;

								name[count++]=strdup(szName);
							}
Log("nFiles = %d, Count = %d, nMax = %d", nFiles, count, nMax);

							qsort(name,count,sizeof(*name),cmp_strr);		// Sort them into reverse order
FILE *fpx=fopen("/tmp/msgs","w"); for(i=0;i<count;i++) fprintf(fpx,"%4d: %s\n", i, name[i]); fclose(fpx);

							for (i=nMax;i<count;i++) {					// Delete all but the first '5,000'
								const char *szCommand = hprintf(NULL, "rm -rf '%s/%s'", szMsgLogDir, name[i]);
								system(szCommand);
								szDelete(szCommand);
								nDeleted++;
							}

							for (i=0;i<count;i++) {							// Free up our ram
								szDelete(name[i]);
							}
							free((char*)name);
							Log("Deleted %d old message%s, left %d", nDeleted,nDeleted==1?"":"s", count-nDeleted);
						} else {
							Log("Only have %d messages - deletion threshold is %d", nFiles, nMax);
						}

						closedir(dir);
					} else {
						Log("Could not open msglog (%s)", szMsgLogDir);
					}
					exit(0);
				}
			}

		}
	}
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

	while((c=getopt(argc,argv,"b:c:d:e:p:P:sSh:v"))!=-1){
		switch(c){
			case 'b': strset(&szBaseDir, optarg);			break;	// Set base directory
			case 'c': strset(&szConfigFile, optarg);		break;	// Set config file
			case 's': bSend=1;								break;	// Send a message over app port
			case 'v': bVerbose=1;							break;
			case '?': nErr++;								break;	// Something wasn't understood
		}
	}

	if (nErr) Usage(1);

	if (szConfigFile) {
		szBaseDir = config_GetDir("basedir", szDefaultBaseDir, "Every other file/dir is relative to this");
	} else {
		szBaseDir = strdup(szDefaultBaseDir);
		szConfigFile = strdup("etc/spider.conf");
		rog_MkDir("etc");
	}

	BackMeUp();

	FILE *fp = fopen(szConfigFile, "a");		// Ensure config file exists
	if (fp) fclose(fp);

	szEtcDir = config_GetDir("etcdir", "etc", "General miscellaneous spider-related data");
	szRpcDir = config_GetDir("rpcdir", "rpc", "The base dir for RPC binaries");
	szRpcDataDir = config_GetDir("rpcdatadir", "rpcdata", "The base dir for general RPC data");
	szLogDir = config_GetDir("logdir", "logs", "A directory into which log files are placed");
	szOutDir = config_GetDir("outdir", "out", "An artifact - don't use");
	szMsgLogDir = config_GetDir("msglogdir", "msglog", "Logs of messages");

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
		if (rogxml_ErrorNo(rxGreeting)) exit(2);				// Error, which we've just printed
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
		exit(0);
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
	}

	exit(0);

	return 0;
}
