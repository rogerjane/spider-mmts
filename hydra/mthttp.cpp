
#define API
#define STATIC

#include "mtmacro.h"
#include "smap.h"
#include "mtstrings.h"

#include <stdlib.h>
#include <netinet/in.h>
#include <stdarg.h>
#include <sys/socket.h>
#if IS_SCO
#include <sys/sockio.h>
#endif
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <net/if.h>
#include <arpa/inet.h>
#include <errno.h>
#include <unistd.h>

#include <ctype.h>
#include <string.h>

#include <hbuf.h>

#define FIREWALL_FILENAME	"/usr/mt/etc/firewall"

void Log(const char *szFmt, ...);

//START HEADER
#include <heapstrings.h>
//END HEADER

// Request:
//   RequestLine
//   Headers
//   Body
//
// Response:
//   StatusLine
//   Headers
//   Body

static inline char *SkipSpaces(char *text)
{
	while (isspace(*text)) text++;
	return text;
}

API const char *http_ResponseText(int code)
// Returns the text corresponding to a response code
// Although unofficial, this seems a good reference: http://www.whoishostingthis.com/resources/http-status-codes
{
	const char *s;

	switch (code) {
		case 100: s="Continue"; break;
		case 101: s="Switching Protocols"; break;
		case 102: s="Processing"; break;							// RFC 2518 WEBDAV

		case 200: s="OK"; break;
		case 201: s="Created"; break;
		case 202: s="Accepted"; break;
		case 203: s="Non-Authoritative Information"; break;
		case 204: s="No Content"; break;
		case 205: s="Reset Content"; break;
		case 206: s="Partial Content"; break;						// RFC 7233
		case 207: s="Multi-Status"; break;							// RFC 4918 WEBDAV
		case 208: s="Already Reported"; break;						// RFC 5842 WEBDAV
		case 226: s="IM Used"; break;								// RFC 3229

		case 300: s="Multiple Choices"; break;
		case 301: s="Moved Permanently"; break;
		case 302: s="Found"; break;
		case 303: s="See Other"; break;
		case 304: s="Not Modified"; break;							// RFC 7232
		case 305: s="Use Proxy"; break;
		case 306: s="Unused"; break;
		case 307: s="Temporary Redirect"; break;
		case 308: s="Permanent Redirect"; break;					// RFC 7538

		case 400: s="Bad Request"; break;
		case 401: s="Unauthorized"; break;							// RFC 7235
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
		case 418: s="I'm a teapot"; break;							// RFC 2324
		case 419: s="Authentication Timeout"; break;				// Not in RFC 2616
		case 420: s="Method failed"; break;							// Spring framework - deprecated
//		case 420: s="Enhance your calm"; break;						// Twitter when too many requests received (= 429)
		case 421: s="Misdirected Request"; break;					// RFC 7540
		case 422: s="Unprocessable Entity"; break;					// RFC 4918 WEBDAV
		case 423: s="Locked"; break;								// RFC 4918 WEBDAV
		case 424: s="Failed Dependency"; break;						// RFC 4918 WEBDAV
		case 426: s="Upgrade Required"; break;
		case 428: s="Precondition Required"; break;					// RFC 6586
		case 429: s="Too Many Requests"; break;						// RFC 6585
		case 431: s="Request Header Fields Too Large"; break;		// RFC 6585
		case 440: s="Login Timeout"; break;							// Microsoft
		case 444: s="No Response"; break;							// NGINX
		case 449: s="Retry with"; break;							// Microsoft
		case 450: s="Blocked by Parental Controls"; break;			// Microsoft (Actually, "Blocked by Windows Parental Controls")
		case 451: s="Unavailable For Legal Reasons"; break;			// RFC 7725 (as in Farenheit 451)
//		case 451: s="Redirect"; break;								// Microsoft
		case 494: s="Request Header Too Large"; break;				// NGINX
		case 495: s="Cert Error"; break;							// NGINX
		case 496: s="No Cert"; break;								// NGINX
		case 497: s="HTTP to HTTPS"; break;							// NGINX
		case 498: s="Token expired/invalid"; break;					// Esri
		case 499: s="Client Closed Request"; break;					// NGINX
//		csse 499: s="Token Required"; break;						// Esri

		case 500: s="Internal Server Error"; break;
		case 501: s="Not Implemented"; break;
		case 502: s="Bad gateway"; break;
		case 503: s="Service Unavailable"; break;
		case 504: s="Gateway Timeout"; break;
		case 505: s="HTTP Version Not Supported"; break;
		case 506: s="Variant Also Negotiates"; break;				// RFC 2295
		case 507: s="Insufficient Storage"; break;					// RFC 4918 WEBDAV
		case 508: s="Loop Detected"; break;							// RFC 5842 WEBDAV
		case 510: s="Not Extended"; break;							// RFC 2774
		case 511: s="Network Authentication Required"; break;		// RFC 6585
		case 520: s="Unknown Error"; break;
		case 598: s="Network read timeout error"; break;			// Microsoft
		case 599: s="Network connect timeout error"; break;			// Microsoft

		default:  s="UNKNOWN CODE"; break;
	}

	return s;
}

API const char *http_StatusLine(int code)
// Returns a line of the form: "HTTP/1.1 200 OK" (depending on the code!)
// Treat as a static
{
	static const char *result = NULL;

	if (result) szDelete(result);

	result = hprintf(NULL, "HTTP/1.1 %d %s", code, http_ResponseText(code));

	return result;
}

typedef struct {
	const char *name;				// The name of the interface
	unsigned long addr;
	unsigned long mask;
} ifinfo_t;

static int ifcount = -1;			// -1 indicates that it's not initialised
static ifinfo_t *ifinfo = NULL;

static unsigned long FlipBytes(unsigned long ul)
{
	unsigned char *a = (unsigned char*)&ul;
	unsigned long addr;

	((unsigned char*)&addr)[0] = a[3];
	((unsigned char*)&addr)[1] = a[2];
	((unsigned char*)&addr)[2] = a[1];
	((unsigned char*)&addr)[3] = a[0];

	return addr;
}

static const char *IpStr(unsigned long lip)
// Returns an essentially static string, which is the display dotted quad
// Is able to return two static strings so can be used twice in the same printf()
{
	static char *resulta = NULL;
	static char *resultb = NULL;
	static char **result = &resulta;

	result = (result == &resulta) ? &resultb : &resulta;
	szDelete(*result);

	*result = hprintf(NULL, "%u.%u.%u.%u", (lip>>24)&0xff, (lip>>16)&0xff, (lip>>8)&0xff, lip&0xff);

	return *result;
}

static int GetInterfaceInfo(int sock)
// WARNING for those of a nervous disposition:
// Some of the code in here is ugly - you probably don't want to look at it too hard.
// It differs between SCO and Linux and there are some nasty references to structures that seem variously
// defined in different places.  However, it seems to work.
//
// Sets up ifinfo to contain a list of network interfaces with their addresses and masks
// Returns the number of those interfaces.
{
	if (ifcount == -1) {
		char buf[4096];
		struct ifconf ifc;
		ifc.ifc_len = sizeof(buf);
		ifc.ifc_buf = buf;

		ifcount = 0;
		int err2 = ioctl(sock, SIOCGIFCONF, &ifc);
//		Log("A %d - %d %s (errno=%d)", err2, ifc.ifc_len, Printable(sizeof(ifc), (const char *)&ifc), errno);
		if (!err2) {
			ifc.ifc_buf = (char*)malloc(ifc.ifc_len);
			int err3 = ioctl(sock, SIOCGIFCONF, &ifc);

			int ifall = ifc.ifc_len / sizeof(struct ifreq);

			for (int i=0; i<ifall; i++) {
				struct ifreq *r = ifc.ifc_req+i;

				if (r->ifr_addr.sa_family == AF_INET) {
//					Log("Interface %i (%s) is %d", i, r->ifr_name, ifcount);
					ifcount++;
				}
			}

//			Log("ifcount = %d", ifcount);

			int ifno = 0;
			ifinfo = NEW(ifinfo_t, ifcount);
			for (int i=0; i<ifall; i++) {
				struct ifreq *r = ifc.ifc_req+i;

				if (r->ifr_addr.sa_family == AF_INET) {
#if IS_SCO
					unsigned char *addr = (unsigned char *)r->ifr_ifru.ifru_addr.sa_un.sa_s.sa_saus_data+2;
#else
					unsigned char *addr = (unsigned char *)&(r->ifr_addr.sa_data)+2;
#endif

					ifinfo_t *iface = ifinfo + ifno++;
					iface->name = strdup(r->ifr_name);
					iface->addr = FlipBytes(*(unsigned long*)addr);

//					Log("iface(%d): %s on %d.%d.%d.%d", i, r->ifr_name,addr[0], addr[1], addr[2], addr[3]);
//					Log("== %s", Printable(sizeof(*r), (const char *)r));

					struct ifreq conf;
					memcpy(&conf, r, sizeof(conf));
					int ierr = ioctl(sock, SIOCGIFNETMASK, &conf);
//					Log("I ioctl(%d, %x, %p) = %d - errno = %d", sock, SIOCGIFNETMASK, &conf, ierr, errno);
#if IS_SCO
					unsigned char *netmask = (unsigned char *)conf.ifr_ifru.ifru_broadaddr.sa_un.sa_s.sa_saus_data+2;
#else
					unsigned char *netmask = (unsigned char *)&(conf.ifr_addr.sa_data)+2;
#endif
//					Log("Subnet mask : %u.%u.%u.%u", netmask[0], netmask[1], netmask[2], netmask[3]);
					iface->mask = FlipBytes(*(unsigned long*)netmask);
				}
			}
		}

//		Log("Here are the results (%d)", ifcount);
//		for (int i=0;i<ifcount; i++) { Log("'%s' - %s mask %s", ifinfo[i].name, IpStr(ifinfo[i].addr), IpStr(ifinfo[i].mask)); }
//		Log("and done\n");
	}

	return ifcount;
}

typedef struct ip_match_t {
	int count;
	unsigned long *addr;
	unsigned long *mask;
} ip_match_t;

typedef struct fw_conn_t {
	int lineno;						// Line in firewall file
	int action;						// 0 = accept, -1 = drop
	ip_match_t match;				// IP specification
} fw_conn_t;

typedef struct fw_call_t {
	int lineno;						// Line in firewall file
	int action;						// 0 = accept, -1 = drop, 1.. = response code
	ip_match_t match;				// IP specification
	int protocol;					// 0 = HTTP, 1 = HTTPS
	const char *verb;				// GET etc.
	const char *uri;				// URI prefix
} fw_call_t;

typedef struct firewall_t {
	int port;
	int conns;						// Number of connection definitions
	int calls;						// Number of call definitions

	fw_conn_t **conn;				// Pointer to pointers to fw_conn_t structures
	fw_call_t **call;				// Pointer to pointers to fw_call_t structures
} firewall_t;

static time_t firewallTimestamp = 0;	// The modify date on the firewall file
static SPMAP *firewall = spmap_New();

static void Fatal(const char *fmt, ...)
{
	va_list ap;
	char buf[1000];

	va_start(ap, fmt);
	vsnprintf(buf, sizeof(buf), fmt, ap);
	va_end(ap);

	fprintf(stderr,"FATAL: %s\n", buf);

	Log("Error: %s", buf);

	exit(99);
}

static int IpSplit(const char *sip, unsigned long *ip, unsigned long *mask)
// Interprets the IP address given as a dotted quad with option /bits into
// an IP address and mask.
// Returns	1	Ok
//			0	Couldn't parse
{
	unsigned int a, b, c, d;
	unsigned int bits = 99;												// Slight kludge, means it'll be invalid if not parsed

	if (sscanf(sip, "%u.%u.%u.%u/%u", &a, &b, &c, &d, &bits) == 5) {		// a.b.c.d/n
	}  else if (sscanf(sip, "%u.%u.%u.%u", &a, &b, &c, &d) == 4) {		// a.b.c.d
		bits = 32;
	}

	if (a > 255 || b > 255 || c > 255 || d > 255 || bits > 32)
		return 0;

	if (ip) *ip = (a<<24) | (b<<16) | (c<<8) | d;
	if (mask) *mask = bits ? (unsigned long)((long)0x80000000 >> (bits-1)) : 0;

	return 1;
}

static ip_match_t ip_match_New(int wanted, const char *extra = NULL)
// Allocates an ip_match_t with enough room for 'wanted' entries
// If 'extra' is specified, "firewall.[extra]" is read and entries in there are put on the end.
{
	ip_match_t match;
	bool created = false;				// True if we've created our result in reading the file

	if (extra) {
		const char *filename = hprintf(NULL, "/usr/mt/etc/firewall.%s", extra);
		FILE *fp = fopen(filename, "r");
		if (fp) {
			char line[2014];

			for (int pass = 1; pass <= 2; pass++) {		// Count on pass 1, assign on pass 2
				rewind(fp);
				int count = 0;

				while (fgets(line, sizeof(line), fp)) {
					char *cp = strchr(line, '\n');
					if (cp) *cp = '\0';
					cp = strchr(line, '#');			// Trim a # and anything after it
					if (cp) *cp = '\0';

					cp = SkipSpaces((char*)line);

					unsigned long ip;
					unsigned long mask;
					if (IpSplit(cp, &ip, &mask)) {
						if (pass == 2) {
							match.addr[wanted+count] = ip;
							match.mask[wanted+count] = mask;
						}
						count++;
					}
				}
				if (pass == 1) {
					created = true;
					match.count = wanted+count;
					match.addr = NEW(unsigned long, wanted+count);
					match.mask = NEW(unsigned long, wanted+count);
				}
			}
			fclose(fp);
		}
		szDelete(filename);
	}

	if (!created) {									// Need to create an empty one
		match.count = wanted;
		match.addr = NEW(unsigned long, wanted);
		match.mask = NEW(unsigned long, wanted);
	}

	return match;
}

static int MaskBits(unsigned long mask)
// Returns the number of bits in the mask
{
	unsigned long bit = 1;
	int count = 0;

	while (bit) {
		if (mask & bit) count++;
		bit <<= 1;
	}

	return count;
}

static const char *ip_match_Match(ip_match_t match, unsigned long addr)
{
	static const char *result = NULL;
	szDelete(result);
	result = NULL;

//Log("Matching IPs to check: %d", match.count);
	for (int i=0;i<match.count;i++) {
//Log("match[%d] is checking %s against %s/%08x", i, IpStr(addr), IpStr(match.addr[i]), match.mask[i]);
		if ((addr & match.mask[i]) == (match.addr[i] & match.mask[i])) {
//Log("Got it!!!!");
			result = hprintf(NULL, "Matched on %s/%d", IpStr(match.addr[i]), MaskBits(match.mask[i]));
			break;
		}
	}

	return result;
}

static ip_match_t FwIpMatch(const char *match, int lineno)
// Returns an ip_match_t structure that describes the match word given
// The IP address/keys represent IP addresses or groups thereof as follows:
// localhost 127.0.0.1
// microtest 10.198.164/22
// lan       Any address on the same LAN as the server
// spine     A known spine address
// a.b.c.d   A specific IP address
// a.b.c.d/n A subnet with n significant bits
// *         Any address
{
	ip_match_t result;

	if (!match) Fatal("Firewall %d: FwIpMatch() passed a null match", lineno);

//Log("Line %d: adding match for '%s'", lineno, match);
	if (!stricmp(match, "*"))	match = "0.0.0.0/0";

	if (!stricmp(match, "localhost")) {
		result = ip_match_New(ifcount+1);
		result.addr[0]	= 0x7f000000;					// 127.0.0.1/8
		result.mask[0]	= 0xff000000;
		for (int i=0;i<ifcount;i++) {					// Add all the addresses we have for ourselves
			memcpy(&(result.addr[i+1]), &(ifinfo[i].addr), sizeof(unsigned long));
			result.mask[i+1] = 0xffffffff;
		}
	} else if (!stricmp(match, "microtest")) {
		result = ip_match_New(0, "microtest");
	} else if (!stricmp(match, "spine")) {
		result = ip_match_New(0, "spine");
	} else if (!stricmp(match, "LAN") || !stricmp(match, "local")) {
		result = ip_match_New(ifcount, "local");
		for (int i=0;i<ifcount;i++) {					// All our addresses along with the corresponding masks
			memcpy(&(result.addr[i]), &(ifinfo[i].addr), sizeof(unsigned long));
			memcpy(&(result.mask[i]), &(ifinfo[i].mask), sizeof(unsigned long));
		}
	} else {
		unsigned long ip;
		unsigned long mask;

		if (!IpSplit(match, &ip, &mask))
			Fatal("Firewall %d: Cannot interpret IP (%s)", lineno, match);

		result = ip_match_New(1);
		result.addr[0]	= ip;
		result.mask[0]	= mask;
	}

//Log("Result has %d entries for '%s'", result.count, match);
//for (int i=0;i<result.count;i++) { Log("Match %d = %s/%08x", i, IpStr(result.addr[i]), result.mask[i]); }

	return result;
}

static int FwAction(const char *action, int lineno)
// drop -> -1
// accept -> 0
// 1..	-> n
// Bad -> Fatal error
{
	int result;

	if (!action) Fatal("Firewall %d: No action given", lineno);
	if (!stricmp(action, "accept")) {
		result = 0;
	} else if (!stricmp(action, "drop") || !stricmp(action, "reject")) {
		result = -1;
	} else {
		result = atoi(action);
		if (!result) Fatal("Firewall %d: Invalid action (%s)", lineno, action);
	}

	return result;
}

static void firewall_Delete()
// Removes the firewall (in readiness for reading in a new one)
{
	// Need to tidy up a load of memory before executing the following...
	spmap_Delete(firewall);
	firewall = spmap_New();
}

API const char *firewall_Load(int sock)
// Parses the firewall file, returning 0 if ok
{
	static const char *result = NULL;
	szDelete(result);
	result = NULL;

	static bool firewallParsed = false;
	bool closeSocket = false;							// if we want to close the socket (because it was passed as 0)

	struct stat st;
	int err = stat(FIREWALL_FILENAME, &st);
	if (err) {
		result = hprintf(NULL, "Stat(%s) returned %d, errno = %d", FIREWALL_FILENAME, err, errno);
		return result;
	}
	if (st.st_mtime > firewallTimestamp) {
		firewall_Delete();
		firewallTimestamp = st.st_mtime;
		firewallParsed = false;							// Force it to reload
	}

	if (firewallParsed) return 0;						// Been here

	FILE *fp = fopen(FIREWALL_FILENAME, "r");
	if (!fp) {
		result = hprintf(NULL, "Could notopen (%s) for reading", FIREWALL_FILENAME);
		return result;
	}

	if (!sock) {
		sock = socket(AF_INET, SOCK_STREAM, 0);
		closeSocket = true;
	}
	GetInterfaceInfo(sock);

	int port = 0;
	int lineno = 0;

	firewall_t *entry = NULL;
	char line[1024];
	while (fgets(line, sizeof(line), fp)) {
		lineno++;

		char *cp = strchr(line, '\n');
		if (cp) *cp = '\0';
		cp = strchr(line, '#');			// Trim a # and anything after it
		if (cp) *cp = '\0';

		cp = SkipSpaces((char*)line);

		if (*cp) {
			char *a = strtok(cp, "\t ");
			if (*a == '[') {
				char buf[20];

				port = atoi(SkipSpaces(a+1));
				if (!port) Fatal("Firewall %d: Invalid port specification (%s)", lineno, a);
				snprintf(buf, sizeof(buf), "%d", port);
				entry = (firewall_t*)spmap_GetValue(firewall, buf);
				if (!entry) {
					entry = NEW(firewall_t, 1);
					spmap_Add(firewall, buf, entry);
					entry->port = port;
					entry->conn = NEW(fw_conn_t *, 1);
					entry->call = NEW(fw_call_t *, 1);
					entry->conns = 0;
					entry->calls = 0;
					entry->conn[0] = NULL;
					entry->call[0] = NULL;
				}
			} else {
				if (!port) Fatal("Firewall %d: No preceding section header", lineno);

				char *b = strtok(NULL, "\t ");
				if (!b) Fatal("Firewall %d: Only one word on the line", lineno);

				char *c = strtok(NULL, "\t ");
				if (!c) {							// A two-word entry (CONN)
					fw_conn_t *conn = NEW(fw_conn_t, 1);
					conn->lineno = lineno;
					conn->match = FwIpMatch(a, lineno);
					conn->action = FwAction(b, lineno);

					RENEW(entry->conn, fw_conn_t*, entry->conns+1);
					entry->conn[entry->conns]=conn;
					entry->conns++;
				} else {							// A five-word entry (CALL)
					char *d = strtok(NULL, "\t ");
					char *e = strtok(NULL, "\t ");

					if (!d) {						// Only three words "LAN HTTP accept" - Make it "LAN HTTP * * accept"
						e=c;
						c="*";
						d="*";
					} else if (!e) {				// Only four words "LAN HTTP GET accept" - Make it "LAN HTTP GET * accept"
						e=d;
						d="*";
					}

					int protocol = 0;
					if (!stricmp(b, "HTTP")) {
						protocol = 0;
					} else if (!stricmp(b, "HTTPS")) {
						protocol = 1;
					} else if (!strcmp(b, "*")) {
						protocol = -1;
					} else {
						Fatal("Firewall %d: Connection type must be HTTP, HTTPS or * (not %s)", lineno, b);
					}

					fw_call_t *call = NEW(fw_call_t, 1);
					call->lineno = lineno;
					call->match = FwIpMatch(a, lineno);
					call->protocol = protocol;			// -1 = *, 0 = HTTP, 1 = HTTPS
					call->verb = strdup(c);
					call->uri = strdup(d);
					call->action = FwAction(e, lineno);
//Log("Line %d: %d %s %s -> %d", lineno, protocol, c, d, call->action);

					RENEW(entry->call, fw_call_t*, entry->calls+1);
					entry->call[entry->calls]=call;
					entry->calls++;
				}
			}
		}
	}
	fclose(fp);

	firewallParsed = true;
	if (closeSocket) close(sock);

	Log("Firewall loaded");

	return NULL;
}

API int firewall_ConnectionDenied(int sock, const char **pmsg /* =NULL */, bool protocol /* =false */, const char *verb /* =NULL */, const char *uri /* =NULL */)
// Return	NULL	Ok
//			else	String of reason
{
	static const char *message = NULL;			// We want to keep a pseudo-static string
	szDelete(message);
	message = NULL;
	int result = -1;

	const char *firewallFail = firewall_Load(sock);
	if (firewallFail) Fatal("Cannot load firewall: %s", firewallFail);

	struct sockaddr_in sin;
	memset(&sin,0,sizeof(sin));
#ifdef IS_SCO
	size_t sin_len = sizeof(sin);
#else
	socklen_t sin_len=(socklen_t)sizeof(sin);
#endif
	int err2 = getsockname(sock, (struct sockaddr*)&sin, &sin_len);
	unsigned char *a = (unsigned char*)&sin.sin_port;
	unsigned short port ;
	((unsigned char*)&port)[0] = a[1];
	((unsigned char*)&port)[1] = a[0];

	int err8 = getpeername(sock, (struct sockaddr*)&sin, &sin_len);
	a = (unsigned char*)&sin.sin_addr;
	unsigned long addr;
	((unsigned char*)&addr)[0] = a[3];
	((unsigned char*)&addr)[1] = a[2];
	((unsigned char*)&addr)[2] = a[1];
	((unsigned char*)&addr)[3] = a[0];
//Log("addr of peer = %s coming in on %d", IpStr(addr), port);

	char sport[20];
	snprintf(sport, sizeof(sport), "%d", port);

	firewall_t *entry = (firewall_t *)spmap_GetValue(firewall, sport);
	if (!entry) {
		if (pmsg) {
			message = hprintf(NULL, "No entry for port %d in firewall", port);
			*pmsg = message;
		}
		return verb ? 403 : -1;				// Should never have got as far as a 'verb' call
	}

	if (verb) {
		for (int i=0;i<entry->calls;i++) {
			fw_call_t *call = entry->call[i];

//Log("Checking rule on line %d", call->lineno);
			const char *matchMessage = ip_match_Match(call->match, addr);		// First check for matching IP
			if (matchMessage) {													// We match, so check the other stuff
//Log("Now checking %d=%d, '%s'='%s', '%s'='%s'", call->protocol, protocol, call->verb, verb, call->uri, uri);
				if (
						(call->protocol == -1 || call->protocol == protocol) &&						// Matching protocol
						(!strcmp(call->verb,"*") || !stricmp(call->verb,verb)) &&					// Matching verb
						(!strcmp(call->uri,"*") || !strnicmp(call->uri,uri,strlen(call->uri)))		// Matching URI
				   ) {
					message = hprintf(NULL, "Firewall match line %d: %s", call->lineno, matchMessage);
					result = call->action;
//Log("Found our boy - action = %d", result);
					break;
				}
			}
		}
	} else {
		for (int i=0;i<entry->conns;i++) {
			fw_conn_t *conn = entry->conn[i];

//Log("Looking at connection %d on line %d", i, conn->lineno);
			const char *matchMessage = ip_match_Match(conn->match, addr);

//Log("Action = %d, Match = '%s'", conn->action, matchMessage);
			if (matchMessage) {
				message = hprintf(NULL, "Firewall match line %d: %s", conn->lineno, matchMessage);
				result = conn->action;
				break;
			}
		}
	}

	if (!message) {
		if (verb) {
			message = hprintf(NULL, "No matching firewall rule for %s HTTP%s on %d (%s %s)", IpStr(addr), protocol?"S":"", port, verb, uri);
		} else {
			message = hprintf(NULL, "No matching firewall rule for %s on %d", IpStr(addr), port);
		}
	}

	if (pmsg) *pmsg = message;

	return result;
}
