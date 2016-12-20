// MMTS - The Microtest Message Transfer System
//

// 24-01-09 RJ 0.18 Tidyups ready for implementation of EPS2
// 04-02-09 RJ 1.00 Overhaul to implement as MMTS2, a more robust solution.
// 07-06-09 RJ 1.01 Added facility to allow different environment configurations on a connection by connection basis
// 06-07-09 RJ 1.02 Take service from contract rather than guessing for NASP (line 2294)
// 06-07-09 RJ 1.03 Take service from contract rather than guessing for NASP (line 2343)
// 08-07-09 RJ 1.04 Re-compiled using the rogxml library updated to keep CDATA status of elements
// 14-07-09 RJ 1.05 Deals with post-dated input (delay attribute on MMTS-Package and post-dated files in drop dir)
// 19-07-09 RJ 1.06 Modified to accept multiple parameters in HTTPS GETs and implement filter on message list
// 22-07-09 SC 1.07 Mods to conform to PDSv4 requirements (marked with 'SC's mod')
// 15-10-09 RJ 1.08 Added SDS lookup functionality
// 10-11-09 RJ 1.09 Modifications to 'delay' functionality to enable control of 'dropped' messages
// 05-12-09 RJ 1.10 Switches TLS authentication depending on environment, tries environments in turn for TLS incoming
// 08-01-10 RJ 1.20 Rationalised all sorts and moved to /usr/mt/mmts
// 21-02-10 RJ 1.21 Added audit reporting readiness
// 07-03-10 SC 1.22 Added handling of multiple NACS codes in SDS queries
// 15-03-10 RJ 1.23 Added 'UUID' column to messages listing
// 27-03-10 RJ 1.24 Merged in Steve's changes (RPC mainly I think) and more information in message list
// 29-03-10 RJ 1.25 Added translation stage for miscellaneous items in ids
// 09-04-10 RJ 1.26 Added ability to connect to non-encrypted remote endpoints
// 12-04-10 RJ 1.27 More complete handling of incoming HTTP header and associated errors
// 12-04-10 RJ 1.28 Corrected lack of addressing information in acknowledgements
// 15-04-10 RJ 1.29 Added options in mmts.conf / env.conf to set the XML indent and linefeed strings
// 23-04-10 RJ 1.30 Fixed a corruption problem (no code change here, it was in the rogxml library)
// 20-07-10 RJ 1.31 Incoming NASP messages are now candidates for delivering to waiting sync application connections
// 20-07-10 RJ 1.32 Now determine environment before dropping off delayed messages (and improved logging)
// 27-07-10 RJ 1.33 Defaults to not asking for incoming certificate of connections, but is configurable (verify-tls)
// 31-07-10 RJ 1.34 Implemented IPC and added environment vars to RPC
// 12-08-10 SC 1.35 Added stderr stream to RPC return message
// 13-08-10 RJ 1.36 Remembered to set szEnvironment when picking up a dropped message
// 21-08-10 RJ 1.37 Revamp of SendMessage to handle fatal/nonfatal, fix number of retries and tidy up
// 23-08-10 RJ 1.38 Re-instated outgoing message validation AFTER partyids and ASIDs have been substituted
// 23-08-10 RJ 1.39 Now delivers responses to messages sent as dropped (delayed) files (AcceptFileConnection)
// 24-08-10 RJ 1.40 Fixed a problem whereby I szDelete()d the result of MessageDescription() and shouldn't have...
// 24-08-10 RJ 1.41 Reports any problem with writing back to the application
// 05-09-10 RJ 1.42 If a problem in writing back to the application, message is returned via 'indir'
// 06-09-10 RJ 1.43 Restrict retries to persistDuration and abort if we exceed it (16552)
// 07-09-10 RJ 1.44 Check for ErrorList ebXML responses (16558)
// 08-09-10 RJ 1.45 ErrorList on ebXML is now a fail
// 09-09-10 RJ 1.46 If 'mmts.conf:AckWait' is non-zero (or omitted), we wait for an ack to a returned message (13881)
// 14-09-10 RJ 1.47 Logic changed to persistDurection aborts rather than adjusting retry time
// 30-09-10 RJ 1.48 Made ReplyWait configurable (used to be 30 secs) and increased logging around response
// 13-10-10 RJ 1.49 Fail ebXML send if we don't receive an interaction back, and allow nRetryInterval timeout (16957)
// 14-10-10 RJ 1.50 Added ability to send miscellaneous WSDL messages
// 19-10-10 RJ 1.51 Fixed duplicate attribute error in <xml encoding="utf-8">
// 19-10-10 RJ 1.51b Recognises /Envelope/Header/Action as being alternate message type identifier
// 19-10-10 RJ 1.51c Takes note of SyncReplyMode of contract when determining whether to fail on no response
// 21-10-10 SC 1.51d Takes note of SyncReplyMode AND ackRequested of contract for above
// 07-12-10 RJ 1.52 Unbugged rpc timeout code, added ability to request a packed/non-packed response, fixed packing
// 09-12-10 RJ 1.53 Fixed a problem with invalid XML in stdout not being returned, fixed stdout buffer termination
// 18-12-10 RJ 1.54 Added _function style of RPC call
// 17-01-11 RJ 1.55 Allowed port 444 to be TLS as well as port 443 as QuickSilva's GP2DRS live server uses that port
// 03-02-11 SC 1.56 Fixed bug that valid xml passed to RPC on stdin not read.
// 15-05-11 RJ 1.57 No longer zaps the PID file if we bale and it's not ours, re-tries socket failures on startup
// 09-06-11 RJ 1.58 ntpservers is now configurable in mmts.conf
// 09-06-11 RJ 1.59 Have messages with an error display with a red ID in the table of messages
// 21-06-11 RJ 1.60 Allow setting of MessageId and ConversationId in the MMTS-Package atttributes
// 15-07-11 RJ 1.61 Passes XML to validator as being ISO-8859-1 so as not to bale on unicode problems such as '£'
// 16-07-11 RJ 1.62 Enable searching on NHS number
// 01-09-11 RJ 1.63 Accept posted messages for GURU updating
// 15-09-11 RJ 1.64 Expanded scope of NHS number location in messages (now searches 'value' as well as 'id')
// 15-09-11 RJ 1.65 Included time server information in log messages, extracts info from delayed messages for log/info
// 18-09-11 RJ 1.66 Some slight mods to smooth the path to GURU data submission (added EnvSet()) and set REQUEST_METHOD
// 09-10-11 RJ 1.67 Added more information into the 'control file' that accompanies incoming dropped files
// 10-10-11 RJ 1.68 If there is a mapping from ASID to partykey, now uses that (GetPartyKeyFromAsid() in mmts-message.c)
// 11-10-11 RJ 1.69 Enabled better debugging (look for bDebug)
// 04-11-11 MS 1.70 Amendments from Mark re. attachment types and information in control files
// 04-11-11 RJ 1.71 Fixed problem of port retry on failure being too quick (18925), and shows SDS query in log
// 08-11-11 RJ 1.72 Tidied up SDS query in logs a little, checks exit status of ntpdate
// 10-11-11 RJ 1.73 Sends <MMTS-Bye> element before breaking internal connection
// 12-11-11 RJ 1.74 Fixed display of delaying files and correspondingly picked up ones and fixed filter links
// 13-11-11 RJ 1.75 Some serious re-arrangement of time-checking functionality
// 18-11-11 RJ 1.76 Uses 'actor' if specified in contract rather than assuming toPartyMSH
// 20-11-11 RJ 1.77 Dynamically fetches contracts when needed
// 20-12-11 RJ 1.78 Better environment variables passed to 'doc' scripts and slight fix to attachment writing
// 13-01-12 RJ 1.79 Stopped sending message level acknowledgements back to application
// 27-01-12 RJ 1.80 Handles HTTP level error returns from far end with no data as SOAP
// 30-01-12 RJ 1.81 UK04 and above now marked as P1R2 whereas it used to be UK05 and above
// 01-02-12 RJ 1.82 21684 Added option in MMTS-Package not to wait for an application level reply
// 05-02-12 RJ 1.83 21709 Fixed bug where error wasn't written correctly (at all) if dropped into 'in'
// 07-02-12 RJ 1.84 21724 Added a copy of the original message into the 'in' dir when an 'Ack' as defaulted (as 21709)
// 12-02-12 RJ 1.85 21751 Caches all LDAP queries, changed fallback to 'nasp' condition on get contract
// 19-02-12 RJ 1.86 21812 Removed 'default' functionality, replaced it with 'overrides' files.  Added SDS "function"s
// 20-02-12 RJ 1.87 21822 Make MMTS-Bye conditional on a 'saybye="1"' in original request
// 23-02-12 RJ 1.88 21751 Only cache if no error
// 25-02-12 RJ 1.89 21856 Strip 'CR' in config files, dropped use of /usr/mt/mmts/ids and /usr/mt/mmts/etc/overrides
// 09-03-12 RJ 1.90 21897 Keeps CTRL chars in incoming XML, no longer relies on /.ldaprc
// 13-03-12 RJ 1.91 21992 Initial MMTS-Ack holds both MessageId and WrapperId
// 04-04-12 SC 1.92 22215 Introduce concept of MMTS protocol to select MMTS behaviour
// 04-04-12 RJ 1.93 22272 Checks responding Conv ID as well as RefTo ID
// 12-04-12 RJ 1.94 This may be simply a re-compile of 1.93
// 29-05-12 RJ 1.95 22377 Some NULL unsafeness disclosed and resolved with crashing under Linux...
// 11-06-12 RJ 1.96 22792 Use the wrapper ID when storing a message for duplicate elimination, which may be helpful
// 12-06-12 RJ 1.97 No code change, but compiled with rogxml that lets 8-bit chars through verbatim
// 13-06-12 RJ 1.97c Added a sleep in "Runaway attachment", only logs to /tmp/in.dat if config("binlog") = 1
// 16-06-12 RJ 1.98 Added msgdir to the control file for incoming messages, fixed Duplicate detection
// 18-06-12 RJ 1.98d Creates a symbolic link with internal ID to acknowledged messages in szAckedDir
// 30-11-12 RJ 1.98e 25017 DNS addresses are cached for 24h
// 06-12-12 RJ 1.98f 25018 When multiple contracts returned, use the one that matches '%gp2gp'
// 09-12-12 RJ 1.98g 25019 Add handling for multipart/form-data content types
// 06-01-13 RJ 2.00 Added in sqlite code, turned msglog into subdirectory structure
// 06-02-13 RJ 2.01 Updated with changes as per 1.98i
// 17-02-13 RJ 2.02 Updated with changes as per 1.98k
// 18-05-13 RJ 2.03 Updated with changes as per 1.98r
// 01-06-13 RJ 2.04 Slight change to recognise ebXML with no header
// 19-06-13 RJ 2.05 26981 Brought back rawin.txt
// 19-06-13 RJ 2.06 26982 Improved ebXML Manifest processing
// 17-07-13 RJ 2.07 Corrected fault with attached eb:Description in a random point in the message
// 31-08-13 RJ 2.08 Added summary log (/usr/mt/mmts/logs/summary.log)
// 03-10-13 RJ 2.09 Added wrapper-id to control file on incoming messages
// 17-11-13 RJ 2.10 Added attributes to msg structure to hold attributes such as ebxml information
// 19-11-13 RJ 2.11 Added ITK functionality
// 22-11-13 RJ 2.12 Made handling of 'received' messages synchronous (replaced 'doin')
// 02-12-13 RJ 2.13 Started proper sqlite support and removed the bug that kept losing 'default environment'
// 10-12-13 RJ 2.14 Amendments to handle ITK/CDA/111 messages
// 15-12-13 RJ 2.15 Where contracts are ambiguous, now tries pdsquery as well as gp2gp when attempting to resolve
// 02-01-14 RJ 2.16 Moved sqlite3 code out to a library so it's shared by spider
// 19-01-14 RJ 2.17 Put in and tidied much SSL code to cope with demands of ITK
// 22-01-14 RJ 2.18 Put the correct default password back in
// 24-01-14 RJ 2.19 Added fault reporting into soap/Envelope/Header/FaultDetail/ProblemHeaderQName for ITK
// 26-01-14 RJ 2.20 Modifying ebXML to cope with yellow cards
// 27-01-14 RJ 2.21 No longer include a 'To' address on a returned ebxml SOAP error (ITK)
// 28-01-14 RJ 2.22 Changed some faultcodes to comply with the whim of Jayne Murphy at 111 compliance.
// 30-01-14 RJ 2.23 Ignore problems of self-signed certs, unable to get local issuer, cert not trusted
// 01-02-14 RJ 2.24 Added ability to proxy for spider and other local services (& gpes & gpesspider)
// 05-02-14 RJ 2.25 29348 Updated the proxy ability and changed colour and now handle binary mime...
// 10-02-14 RJ 2.26 Added /file to fetch files from the file system and fixed date display issue
// 26-02-14 RJ 2.27 Added support for TLS over NHS111 port (1879), zip old msglog directories
// 11-03-14 RJ 2.28 Picks random env when there is no default, changed itk port to 1880, added ignore cert err 21
// 04-04-14 RJ 2.28a Dropped check for 'From' to see if NHS111 works without it
// 08-05-14 RJ 2.28b Experimenting with content-type etc. to make GP2GP messages accepted!
// 13-05-14 RJ 2.29 30320 Amended Acknowledgments to use wrapper ID as RefToMessageId rather than internal message ID
// 15-06-14 RJ 2.30 Compiles cleanly under Linux
// 07-07-14 RJ 2.31 30629 Inter-character timeout now reset to be the message receipt timeout
// 24-07-14 RJ 2.32 30862 Slightly tidier log of spine response and now return spine acknowledgement back to caller
// 28-07-14 RJ 2.33 30871 Return Acknowledgement to caller if requested in the header
// 25-09-14 RJ 2.34 31496 Writes raw input and app input to a file (in /tmp if there is no msglog dir)
// 06-10-14 RJ 2.35 31605 Roll over log and summary files if they get big (> 1GB)
// 29-01-15 RJ 2.35a Added some debugging around GetEnvironment() as there seems to be a memory problem
// 14-03-15 RJ 2.36 xxxxx Changed ValidateXml() to unlink the temporary file if validation is ok
// 14-09-15 RJ 2.37 xxxxx Put a five second timeout on SSL_CloseNicely() to combat strange problem at Wycliffe
// 08-01-16 RJ 2.38 39240 Removed ipc requirements - look for IPCREMOVED
// 21-01-16 RJ 2.39 39380 Changed SSL shutdown error to be 'problem' to stop it showing as an error
// 24-05-16 RJ 2.40 xxxxx Only sets LDAPTLS_RANDFILE if the 'random' file exists
// 03-06-16 RJ 2.41 Some fixes through linuxification - used for testing
// 06-06-16 RJ 2.42 40727 Checked in version with LINUX fixes
// 19-08-16 RJ 2.50 Achieved wamp

#define VERSION				"2.50"

// TODO: Method of 're-receiving' a message would be nice.
// TODO: When running, look for mmts.version and copy ourselves into it if necessary

// TODO: Mantis 25969: Validating ASIDs on incoming messages

// xTODO: zip up msglog directories when they get old:	zip -mr msglog/14/02/mmts-140227.zip msglog/14/02/27
//														unzip msglog/14/02/mmts-140227.zip 'msglog/14/02/27/*'
// xTODO: Display of messages:  Default to displaying today's.  Have links to display each of the months available
// xTODO: Along with the days in the current month:
// xTODO: 2012-Dec 01 02 03 04 07 08 09 12 13 14 15 16 17 19 20 22 23 24 25 28 29 30
// xTODO: 2012 Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec
// xTODO: 2011 Sep Oct Nov Dec
// xTODO: Consider handing off requests to spider that come via HTTPS port
// xTODO: when a child finishes, add a one-line entry to a log summarising it (include EPS id, NHS no etc. where available)
// TODO: Try to do away with the need for 'overrides'...!
// TODO: Make all ldap cache entries a GUID, using the whole query string
// xTODO: Incorporate admin of msglog (see spider for partial solution)
// TODO: Administration of log file - archive off when too large and zip second to last one (always keep 2 plain text)
// TODO: Accept internal requests using a HTTP protocol
// TODO: Consider separate configuration of local asids perhaps in /usr/mt/mmts/asids, maybe in /usr/mt/mmts/env/*
// TODO: Consider administration of local clients (setting of environment etc.) in separate folder
// TODO: Rationalise 'bestbefore' and 'sellby'.  If after bestbefore then refetch, if after sellby then refuse
// TODO: Cache DNS lookups (24h) into a mysqlite database within the env/cache directory so deleting it is legitimate
// TODO: Put simple log entries into mysqlite database and use this for html messages listing
// TODO: Allow MMTS to be stopped cleanly through a signal to the daemon

// TODO:
//   When returning a message to Evo, ensure that no errors occur in case the connection has just dropped.
//   Make the authentication of the incoming connection configurable (c.f. the 'old' mmts.c at SSL_VERIFY_PEER)
//   Tidy the whole thing up.
//   Make the internal message format neater.
//   Make serialisation of the internal message neater and more sensible (store in XML?)
//   MMTS should/could act as site client for administration
//   Leave message un-wrapped until the point of sending, strip it as soon as received.
//   When pulling all contract information to the msg, may need separate incoming and outgoing versions
//     of variables such as duplicate elimination depending on whether we're passing it on or accepting it (or both)
//  Log file administration: Keep mmts.log, mmts-prev.log and archive directory.  When a write to mmts.log makes it
//  too big (if mmts-prev.log exists, rename it to mmts-archive.log) rename it to mmts-prev.log.  Then if mmts-archive
//  exists, fire off a child to zip it into the archive directory.  This way we have current and previous uncompressed.

// Implementation of v2:
// Accept message from source (443(TLS), 10023(app), file, directory, command line)
// Put in internal generic format including addressing information
// Place in secondary storage if post-dated
// Send to destination
// Reply on incoming connection if necessary
// mmts-setup will arrange contracts to fall into contracts/$toparty/$interactionid
// Need to tidy up old messages according to their expiry date, leaving audit trail
// Implement audit trail
// Split msglog down by date
// Consider converting internal connection methodology to BIO instead of fd

// Specific mantis to address (may go into 0.x version)
// 11178 (09320) - Post dating of messages
// 12213 (12171) - Access to Gazeteer service
// 11704 - Ping/Pong service

#define _GNU_SOURCE

#include <arpa/inet.h>
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <grp.h>
#include <limits.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <pwd.h>
//#include <regexpr.h>
#include <regex.h>
#include <setjmp.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <sys/fcntl.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/resource.h>
#include <sys/select.h>
#include <sys/shm.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <utime.h>
#include <zlib.h>

#include <guid.h>
#include <hbuf.h>
#include <mtmacro.h>
#include <mtsl3.h>
#include <mtutil.h>
#include <rogxml.h>
#include <smap.h>
#include <szz.h>
#include <xstring.h>

#include "mmts-contract.h"
#include "mmts-message.h"
#include "mmts-utils.h"

#include "mtjson.h"
#include "mtchannel.h"
#include "mtwebsocket.h"
#include "mtwamp.h"
#include "mtwamputil.h"

#ifndef SHUT_RD
	#define SHUT_RD  0			// These should be in sys/socket.h but aren't...???
	#define SHUT_WR  1
	#define SHUT_RDWR 2
#endif

#define PORT_APP	10023
#define PORT_TLS	443
#define PORT_111	1880		// Port number for NHS 111 messaging

#define LOGFILE_LIMIT   1000000000          // Maximum size of corresponding log files
#define MIFILE_LIMIT    1000000000

#include <openssl/ssl.h>
#include <openssl/err.h>

#include <heapstrings.h>

#include <npfit.h>
#include <mime.h>
#include <varset.h>

static int _bPackXML = 0;					// 1 to pack XML sent to application

#define	XML_MIME_TYPE		"text/xml"		// Something may prefer 'text/xml'

#ifndef max
#define max(a,b) ((a)>(b)?(a):(b))
#endif

int _nAlarmNumber = 0;						// Alarm number - 0 disables returning long jump when timer triggers
int _nInterCharacterTimeout=0;				// 30629 If non-zero, alarm is rest to this on an incoming character
jmp_buf jmpbuf_alarm;						// Used when alarm goes off for timeouts

const char *szMyName = "mmts";

enum PROTOCOL_VERSION mmtsProtocol = NO_PROTOCOL;		// MMTS Protocol Version - set via MMTS-Package

const char *szRpcError = NULL;				// Error returned from RPC call

const char *szDebugTrigger = "/tmp/mmts.debug";	// If this file exists, we go into debug mode
char bDebug = 0;							// 1 if in debug mode

const char *MonthNames[] = {"?","January","February","March","April","May","June","July","August","September","October","November","December"};

// Bit messy at the moment but we have two protocols, one for external and one for internal (applications)
// It used to be that the external one would always be TLS encrypted, but this is no longer the case...
#define SRC_IP 1							// Internal - 'Plain' application connection
#define SRC_TLS 2							// External - TLS connection
#define SRC_PLAIN 3							// External - 'Plain' connection
#define SRC_RETRY 4							// Internal (to MMTS) - retry of message
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

NetworkPort_t NetworkPort[] = {
	{PORT_APP, 1, SRC_IP, 0, 0, 0, 0},		// Plain connection for apps
	{10024, 1, SRC_PLAIN, 0, 0, 0, 0},		// HTTP connection for pretend spines
	{PORT_TLS, 1, SRC_TLS, 0, 0, 0, 0},		// HTTPS connection for spine and monitoring
	{PORT_111, 1, SRC_TLS, 0, 0, 0, 0}		// HTTPS connection for NHS111
};
int nNetworkPorts=sizeof(NetworkPort)/sizeof(*NetworkPort);

const char *_szSenderIp = NULL;				// IP of the remote incoming connection
const char *_szReceiverIp = NULL;			// IP of the machine we're sending to
const char *_szIncomingIp = NULL;			// The IP address on which we accepted the connection
const char *_szOutgoingIp = NULL;			// The IP from which we're sending

int _nSenderPort = 0;						// Port from which incoming message came
int _nReceiverPort = 0;						// Port to which we're sending
int _nIncomingPort = 0;						// Port on which we accepted the message
int _nOutgoingPort = 0;						// Port from which we're sending

int _nTotalConnections = 0;					// Total connections accepted
int _nDroppedCount = 0;						// Number of dropped files picked up

HBUF *_inputBuf = NULL;						// Holds all data received from an external party
HBUF *_inputApp = NULL;						// Holds all data received from the application

SSMAP *wampRedirectMap = NULL;				// Map of redirects for WAMP calls (initially used for FHIR)

char bIsDaemon = 1;							// 1 if Daemon, 0 otherwise

const char *szMyUrl = NULL;					// Response address for Nasp2 (wsa) headers

const char *szEnvironment = NULL;			// The environment in which we're working
const char *szTmpDir = NULL;				// Temporary stuff
const char *szLogDir = NULL;				// Where logs go
const char *szInDir = NULL;					// Incoming messages
const char *szOutDir = NULL;				// Outgoing messages
const char *szTxDir = NULL;					// Translators
const char *szContractDir = NULL;			// Where contracts are
const char *szMsgLogDir = NULL;				// Logged messages
const char *szMsgLogDirLeaf = NULL;			// Last part of szMsgLogDir
const char *szStoreDir = NULL;				// Persistent Storage for messages
const char *szUnackedDir = NULL;			// Outgoing messages awaiting an ack
const char *szAckedDir = NULL;				// Outgoing messages either acked or not requiring an ack
const char *szFailedDir = NULL;				// Outgoing messages never acknowledged and hence given up on
const char *szReceivedDir = NULL;			// Incoming messages with 'DuplicateElimination' found
const char *szHandlerDir = NULL;			// Handlers for dealing with delivered messages
const char *szDoneDir = NULL;				// Where incoming messages end up after being handled
const char *szWaitingDir = NULL;			// Flags for sync/async messages
const char *szEnvDir = NULL;				// E.g. env/xxx (or /usr/mt/mmts/env/xxx)
const char *szDirWsdl = "wsdl";				// Where WSDL definitions are stored

const char *szPidFile = NULL;				// Location of pid file

const char *szXmlLinefeed = "\n";			// Default linefeed and indent strings
const char *szXmlIndent = "  ";

const char *szLogFile = NULL;

char bVerbose = 0;							// Quiet unless told otherwise (used with -s and -S)

#define PASSWORD "mountain"					// The password to access our key

CHANPOOL *daemon_channel_pool = NULL;           // Channel list used in daemon event loop()

BIO *bio_err = NULL;

static time_t _nStartTime;				// Time server started

#define CLIENT_AUTH_NONE		0
#define CLIENT_AUTH_REQUEST		1
#define CLIENT_AUTH_NEED		2

static const char _id[] = "@(#)MMTS version " VERSION " (" OS ") compiled " __DATE__ " at " __TIME__;

static int s_server_session_id_context = 1;

static char bSayBye = 0;						// 21822 Default to not saying goodbye

static const char *_szPassword = NULL;
static const char *argv0 = NULL;				// The location of our executable

static char _bNoteInhibit = 0;
static char *szGlobalNoteDir = NULL;
static char bNotedDate = 0;				// Set once we have added a 'D' field

static SSMAP *_incomingHeader = NULL;		// The (any) header received by requesting peer

void Exit(int nCode);

int g_sock = -1;							// Global things relating to the current SSL connection
SSL *g_ssl = NULL;
SSL_CTX *g_ctx = NULL;
const char *g_ssl_cn = NULL;				// The Cn of the SSL peer
#define SSL_MAXDEPTH  10
const char *g_ssl_subject[SSL_MAXDEPTH];

const char *ProtocolName(int nProtocol)
{
	switch (nProtocol) {
	case SRC_IP:		return "APP";
	case SRC_TLS:		return "HTTPS";
	case SRC_PLAIN:		return "HTTP";
	case SRC_RETRY:		return "RETRY";
	case SRC_DROPPED:	return "DROP";
	default:			return "???";
	}
}

const char *logPrefix = "BOOTER";

static int spiderFork(const char *name)
{
	int child = fork();
	if (!child) {
		logPrefix = name;
		SetLogPrefix(logPrefix);
	}

	return child;
}

void Fatal(const char *szFmt, ...)
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
	static int resno=0; resno=!resno;					// Alternating 0/1
	static const char *results[2] = {NULL,NULL};
	const char *result = results[resno];

	struct tm *tm = localtime(&t);

	szDelete(result);

	result = hprintf(NULL, "%02d-%02d-%04d %02d:%02d:%02d",
								tm->tm_mday, tm->tm_mon+1, tm->tm_year + 1900,
								tm->tm_hour, tm->tm_min, tm->tm_sec);

	return result;
}

int isDir(const char *szDir)
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

const char *MessageId()
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
		if (!isDir(szDir)) {										// No dir, must be a new day
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

struct timeval summaryStart;			// Process start time for summary
struct timeval summaryStop;
static const char *summaryLogfile=NULL;
static const char *summaryNoteDir=NULL;
int summaryProtocol=0;					// Message protocol invocation
const char *summaryMessage = NULL;
const char *summaryFrom = NULL;
const char *summaryTo = NULL;

void summ_ChildStart(int nProtocol)
{
	summaryProtocol=nProtocol;
	gettimeofday(&summaryStart, NULL);
}

void Summary(int nExitCode)
// Create log entry on exit that sums up the message (when,from,to,duration,type,status,dir)
// Logs a summary of the activity of this child process
// time_t _tChildStart=0;						// Child start time for summary

{
	gettimeofday(&summaryStop, NULL);

	if (!summaryLogfile) {
		summaryLogfile = hprintf(NULL, "%s/summary.log", szLogDir);
	}

	int elapsed=(summaryStop.tv_sec - summaryStart.tv_sec)*1000 +			// Seconds
				(summaryStop.tv_usec - summaryStart.tv_usec)/1000;			// plus milliseconds

	struct tm *tm = gmtime(&summaryStart.tv_sec);

	FILE *fp=fopen(summaryLogfile, "a");
	if (fp) {
		fprintf(fp, "%02d-%02d-%02d %02d:%02d:%02d.%03d ",
				tm->tm_mday, tm->tm_mon+1, tm->tm_year % 100,
				tm->tm_hour, tm->tm_min, tm->tm_sec,
				(int)(summaryStart.tv_usec / 1000)
				);

		fprintf(fp, "%6d ", elapsed);
		fprintf(fp, "%5s ", ProtocolName(summaryProtocol));
		fprintf(fp, "%s ", summaryFrom?summaryFrom:"-");
		fprintf(fp, "%s ", summaryTo?summaryTo:"-");
		fprintf(fp, "%s ", summaryMessage?summaryMessage:"-");
		fprintf(fp, "%s", summaryNoteDir?summaryNoteDir:"");
		fprintf(fp, "\n");

		fclose(fp);
	}

	Log("Elapsed time: %d.%03d", elapsed / 1000, elapsed % 1000);
}

int GetRecordedDaemonPid()
// Returns 0 if it isn't recorded - will return non-0 if the .pid file exists even if the process is dead
{
	FILE *fp=fopen(szPidFile, "r");
	char buf[30]="0";					// Used both for existing process's pid and our own
	int nPid = 0;

	if (fp) {
		char* szPid = fgets(buf, sizeof(buf), fp);
		fclose(fp);
		if (szPid)
			nPid=atoi(buf);
	}

	return nPid;
}

int GetDaemonPid()
// Returns	0		There is no daemon running
//			1...	The PID of the daemon
{
	int nPid=GetRecordedDaemonPid();
	if (nPid) {
		if (ProcessAlive(nPid)) return nPid;
	}

	// There is now no existing instance as there is no .pid file or we didn't see it on the kill

	return 0;
}

void CheckWeAreDaemon()
// Checks that we are the daemon - if not then quits...
{
	int nPid=GetDaemonPid();

	if (nPid == getpid()) return;			// That's what we're looking for...

	if (nPid) {								// There's a daemon, but it's not us...
		Log("Exit: Panic: Pid file says my process ID is %d", nPid);
	} else {
		Log("Exit: Panic: Can't read my pid from %s", szPidFile);
	}

	// If we're here, we're going to bale but we don't want to take the PID file with us
	// as it might belong to the ligitimate daemon...
	szPidFile=NULL;							// Stop PID file being deleted

	Exit(0);
}

//////////////////////////////////////////////////////////////


#if 0		// Removed the whole of the IPC handling code - IPCDELETE
typedef enum {
	ipc_log=1,								// A test - simply logs the message
	ipc_handling,							// Sent to daemon with message iD to indicate that this child is handling it
	ipc_whohas,								// Sent with id to daemon, reply is ipc_response(ipc_whohas:pid:messageid)
	ipc_response,							// Generic 'response' message (e.g. to 'ipc_whohas'
	ipc_sendmemory
} ipc_type;
const ipc_type ipc_waitreply = 256;

#define MAXIPCLEN						1000
typedef struct {
	ipc_type	mtype;						// Message type
	int			mpid;						// Process ID of sender
	char		mtext[MAXIPCLEN];			// Data sent
} myipc;

const char *ipc_Process();

int ipc_Id(int pid)
{
	int key = ('M' << 24) | (pid & 0xffffff);
	int id = msgget(key, 0666);
	if (id < 0 && pid == getpid()) {		// We're getting our own and it doesn't exist
		id = msgget(key, IPC_CREAT | 0666);
if (id < 0) {
	Fatal("I can't create an ipc queue.  Run 'ipcs' and kill some off please");
}
Log("MAIN: Created queue %d (key=%x)", id, key);
	}

	return id;
}

int ipc_Rm(int pid)
{
	int id = ipc_Id(pid);

	return msgctl(id, IPC_RMID, NULL);
}

const char *ipc_Send(int pid, ipc_type nType, int nLen, const char *szMessage)
// Sends a message (and waits for a reply if (nType | ipc_waitreply)
// Returns	NULL			Either no reply wanted or there wasn't one
//			const char *	String on the heap (i.e. free it yourself)
{
	myipc *msg;
	int nErr;
	const char *szResponse = NULL;

	if (nLen < 0) nLen=strlen(szMessage)+1;
	if (nLen > MAXIPCLEN) nLen=MAXIPCLEN;

	msg = NEW(myipc, 1);
	msg->mtype=nType;
	msg->mpid=getpid();
	memcpy(msg->mtext, szMessage, nLen);

	nErr=msgsnd(ipc_Id(pid), msg, nLen+4, IPC_NOWAIT);

	if (nErr) Log("msgsnd error %d (errno=%d) on ipc_Send(%d,%d,%d,%.10s...)", nErr, errno, pid, nType, nLen, szMessage);

	free(msg);
Log("MAIN: IPC: Sent type %d to %d - '%s'", nType, pid, szMessage);

	if (nType & ipc_waitreply) {									// Asked for a reply so wait for one
		int count = 0;

		while (!(szResponse = ipc_Process()) && count < 10) {		// Hard-wired 10 second maximum wait
			count++;
			sleep(1);
		}
Log("MAIN: IPC: Have response of '%s' (count = %d)", szResponse, count);
	}

	return szResponse;
}

const char *ipc_Sendf(int pid, ipc_type nType, const char *szFmt, ...)
{
	va_list ap;
	char buf[MAXIPCLEN];

	va_start(ap, szFmt);
	vsnprintf(buf, sizeof(buf), szFmt, ap);
	va_end(ap);

	return ipc_Send(pid, nType, -1, buf);
}

const char *ipc_SendDaemon(int nType, int nLen, const char *szMessage)
{
//Log("ipc_Send(%d, %d, %d, \"%.10s\")", GetDaemonPid(), nType, nLen, szMessage);
	const char *r = ipc_Send(GetDaemonPid(), nType, nLen, szMessage);
//Log("MAIN: ipc_SendDaemon(%d, %d, %s) = %s", nType, nLen, szMessage, r);
	return r;
}

int shm_Id(int pid, size_t size)
{
	int key = ('M' << 24) | (pid & 0xffffff);
	int id = shmget(key, size, 0666);
	if (id < 0 && pid == getpid()) {		// We're getting our own and it doesn't exist
		id = shmget(key, size, IPC_CREAT | 0666);
Log("MAIN: Created shared memory %d (key=%x)", id, key);
	}

	return id;
}

int shm_Detach(const char *mem)
{
	return shmdt(mem);
}

int shm_Rm(int id)
// Delete the shared memory pertaining to the given process id
{
	return shmctl(id, IPC_RMID, NULL);
}

static int nReceivedShm = 0;					// Size and data of more recently received shared memory block
static const char *pReceivedShm = NULL;

void shm_ReceivedDelete()
{
	if (pReceivedShm) {
		free((char*)pReceivedShm);
		pReceivedShm = NULL;
		nReceivedShm = 0;
	}
}

const char *shm_Receive(id)
// Pulls in some shared memory from another process
// Returns	NULL			Didn't receive any
//			const char *	Pointer to received memory (use shm_ReceivedSize() and shm_ReceivedData() if necessary)
{
	const char *mem = shmat(id, NULL, 0);

	if (mem) {
		shm_ReceivedDelete();
		nReceivedShm = *(long*)mem;
		pReceivedShm = malloc(nReceivedShm);
		if (pReceivedShm) {
			memcpy((char*)pReceivedShm, mem+sizeof(long), nReceivedShm);
		} else {
			nReceivedShm = 0;
		}
		shm_Detach(mem);
	}

	return pReceivedShm;
}

int shm_ReceivedSize()
{
	return nReceivedShm;
}

const char *shm_ReceivedData()
{
	return pReceivedShm;
}

const char *ipc_SendMemory(int pid, const char *szTag, int nLen, const char *mem)
// Sends a lump of data to another process using a shared memory segment.
// Creates shared memory segment and copies memory into it
// Sends ipc_sendmemory to the receiving process
// Receiving process copies memory out
// Receiving process sends back a response message
// We delete the shared memory
// Returns	const char *	A message from the other side...
//			NULL			Something went awry
{
	if (nLen < 0) nLen = strlen(mem)+1;

	int id = shm_Id(getpid(), nLen+sizeof(long));
	const char *szResponse = NULL;

	char *shmem=shmat(id, NULL, 0);
	if (shmem) {
		*(long*)shmem = nLen;
		memcpy(shmem+sizeof(long), mem, nLen);

		szResponse = ipc_Sendf(pid, ipc_sendmemory | ipc_waitreply, "%d:%s", id, szTag);
		shm_Detach(shmem);
		shm_Rm(id);
		Log("Response from shared memory offering = '%s'", szResponse);
	}

	return szResponse;
}
#endif

const char *UserName(int uid)
{
	static const char *result = NULL;
	static int prevUid = -1;

	if (uid == prevUid) return result;
	szDelete(result);

	struct passwd *pwd = getpwuid(uid);
	if (pwd) {
		result = strdup(pwd->pw_name);
	} else {
		result = hprintf(NULL, "%d", uid);
	}
	prevUid = uid;

	return result;
}

const char *GroupName(int gid)
{
	static const char *result = NULL;
	static int prevGid = -1;

	if (gid == prevGid) return result;
	szDelete(result);

	struct group *grp = getgrgid(gid);
	if (grp) {
		result = strdup(grp->gr_name);
	} else {
		result = hprintf(NULL, "%d", gid);
	}
	prevGid = gid;

	return result;
}

const char *CommaNum(long n)
{
	static char buf[20];
	char *bufp=buf+sizeof(buf)-1;
	*bufp='\0';

	if (n <= 0) return "0";                 // Special case

	while (n > 999) {
		char b[5];
		sprintf(b, ",%03ld", n%1000);
		bufp-=4;
		memcpy(bufp, b, 4);
		n /= 1000;
	}
	while (n) {
		*--bufp='0'+n%10;
		n/=10;
	}

	return bufp;
}

const char *SignalDescr(int sig)
{
	switch (sig) {
	case 1: return "hangup";
	case 2: return "interrupt";
	case 3: return "quit";
	case 4: return "illegal instruction";
	case 5: return "trace trap";
	case 6: return "ABORT";
	case 7: return "EMT instruction";
	case 8: return "Floating exception";
	case 9: return "KILL";
	case 10: return "bus error";
	case 11: return "segmentation violation";
	case 12: return "bad argument";
	case 13: return "broken pipe";
	case 14: return "alarm";
	case 15: return "termination";
	case 16: return "user 1";
	case 17: return "user 2";
	case 18: return "child crying";
	case 19: return "power fail";
	case 20: return "window size change";
	case 21: return "urgent socket";
	case 22: return "I/O allowed";
	case 23: return "STOP";
	case 24: return "user stop";
	case 25: return "continue";
	case 26: return "background read";
	case 27: return "background write";
	case 28: return "virtual alarm";
	case 29: return "profiling";
	case 30: return "exceeded CPU limit";
	case 31: return "exceeded file limit";
	case 32: return "waiting";
	case 33: return "thread";
	case 34: return "async IO";
	case 35: return "migrating";
	case 36: return "cluster reconfig";
	}

	static char buf[20];
	snprintf(buf, sizeof(buf), "Unknown signal %d", sig);

	return buf;
}

#if PID_MAX < 238328
const char *pidstr()
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

SSMAP *ssEnv = NULL;
void EnvSet(const char *szName, const char *szValue)
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

void EnvUnsetAll()
{
	char *szVariable;
	ssmap_Reset(ssEnv);

	while (ssmap_GetNextEntry(ssEnv, NULL, (const char **)&szVariable)) {
		char *chp=strrchr((char*)szVariable, '=');
		if (chp) {								// This really has to find one
			*chp='\0';							// Blatt it so we have "NAME="
			putenv((char*)szVariable);
		}
	}
	ssmap_Delete(ssEnv);						// Drop the entire map
	ssEnv=NULL;
}

const char *InternalId()
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

const char *UriDecode(char *str)
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

const char *UriEncode(const char *szText)
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

const char *rogxml_ToNiceText(rogxml *rx)
// Returns the string representation of 'rx', ensuring indentation etc. is nice
{
	rogxml_SetIndentString("  ");
	rogxml_SetLinefeedString("\n");

	return rogxml_ToText(rx);
}


const char *NoteDirSuffix()
{
	static char buf[30];
	static char bBeenHere=0;

	if (!bBeenHere) {
		snprintf(buf, sizeof(buf), "%s-%s", HL7TimeStamp(0), pidstr());
		bBeenHere=1;
	}

	return buf;
}

const char *NoteDir()
{
	static char bInHere = 0;				// Flag to stop re-entrancy problems

	if (!bInHere) {
		bInHere=1;

		if (!szGlobalNoteDir && !_bNoteInhibit) {
			const char *mid = MessageId();				// Always an 11 char string - YYMMDDxxxxx
			szGlobalNoteDir=hprintf(NULL, "%s/%.2s/%.2s/%.2s/%s", szMsgLogDir, mid, mid+2, mid+4, mid+6);
			summaryNoteDir=hprintf(NULL, "%.2s/%.2s/%.2s/%s", mid, mid+2, mid+4, mid+6);
			rog_MkDir(szGlobalNoteDir);
			Log("MAIN: Message log directory is %s", szGlobalNoteDir);
		}

		bInHere=0;
	}

	return szGlobalNoteDir;
}

void NoteInhibit(int bOpt)
// Call with non-0 to stop (and delete) notes
{
	_bNoteInhibit = bOpt;
	if (bOpt && szGlobalNoteDir) {				// Need to delete any that have been created already
//Log("Removing log dir (%s)", szGlobalNoteDir);
		const char *szCommand = hprintf(NULL, "rm -rf %s", szGlobalNoteDir);
		if( system(szCommand) == -1 ) {
			Log("Error executing: %s", szCommand);
		}
		szDelete(szCommand);
		szGlobalNoteDir=NULL;
	}
}

void Note(const char *szFmt, ...)
// make a note, and if this is our first one then add a 'date' row
{
	va_list ap;
	FILE *fp;
	const char *szFilename;

	char buf[40];
	va_start(ap, szFmt);								// Write our original thing
	vsnprintf(buf, sizeof(buf), szFmt, ap);
	if (strlen(buf)<3) strcat(buf, "-");
	switch (*buf) {
	case 'Y':	summaryMessage=strdup(buf+2);	break;
	case 'F':	summaryFrom=strdup(buf+2);	break;
	case 'T':	summaryTo=strdup(buf+2);	break;
	}
	va_end(ap);

	if (_bNoteInhibit) return;								// We're not taking notes
	if (bIsDaemon) return;									// Master daemon process doesn't make notes

	szFilename = hprintf(NULL, "%s/info", NoteDir());		// This will always set szGlobalNoteDir

	fp=fopen(szFilename, "a");
	if (fp) {
		if (!bNotedDate) {

			time_t now=time(NULL);							// Add the datetime entry
			struct tm *tm = gmtime(&now);
			struct timeval tp;
			int msecs;

			gettimeofday(&tp, NULL);
			msecs=tp.tv_usec / 1000;

			fprintf(fp, "D|%02d-%02d-%04d %02d:%02d:%02d.%03d %.3s\n",
				tm->tm_mday, tm->tm_mon+1, tm->tm_year + 1900,
				tm->tm_hour, tm->tm_min, tm->tm_sec,
				msecs,
				"SunMonTueWedThuFriSat"+tm->tm_wday*3);
			bNotedDate = 1;
		}

		va_start(ap, szFmt);								// Write our original thing
		vfprintf(fp, szFmt, ap);
		fputs("\n", fp);
		va_end(ap);

		fclose(fp);
	}

	szDelete(szFilename);
}

void NoteType(const char *szType, const char *szMessage)
// Shorthand for Note() - this version is NULL safe
{
	if (szType && szMessage) {
		Note("%s|%s", szType, szMessage);
	}
}

void NoteFromMsg(MSG *msg)
// Creates the I, F, T and Y notes using the message
{
	if (msg) {
		NoteType("I", msg_GetMessageId(msg));
		NoteType("F", msg_GetFromPartyId(msg));
		NoteType("T", msg_GetToPartyId(msg));
		NoteType("Y", msg_GetInteractionId(msg));
		NoteType("C", msg_GetConversationId(msg, NULL));
	}
}

void NoteLog(const char *szText)
{
	const char *szFilename;
	const char *szDir;
	FILE *fp;

	if (bIsDaemon) return;

	szDir = NoteDir();
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

		long msecs = tv.tv_sec * 1000 + tv.tv_usec / 1000;		// This works even if usec went negative

		fprintf(fp, "%02ld:%02ld.%03ld ", msecs / 60000, (msecs / 1000) % 60, msecs % 1000);
		fprintf(fp, "%s\n", szText);
		fclose(fp);
	}

	szDelete(szFilename);
}

void NoteMessage(const char *szMessage, int nLen, const char *szTag, const char *szExt, const char *szDescr)
{
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

	Note("M|%s.%s|%s", szTag, szExt, szDescr);
}

void NoteRpcError()
// I could have used NoteMessage() to do this then I was going to add a header to the file but then changed
// my mind as everything should already be in the note directory.
// NoteRpcMessage(szRpcError, -1, "stderr", "txt", "Stdout returned from RPC handler");
{
	if (szRpcError) {
		const char *szFilename;
		FILE *fp;

		szFilename=hprintf(NULL, "%s/stderr.txt", NoteDir());
		fp=fopen(szFilename, "w");
		if (fp) {
			fwrite(szRpcError, strlen(szRpcError), 1, fp);
			fclose(fp);

			Note("M|stderr.txt|Error returned from RPC handler");
		}
		szDelete(szFilename);
	}
}

void NoteMessageMsg(MSG *msg, const char *szTag, const char *szDescr)
{
//Log("NoteMessageMsg(%x, \"%s\", \"%s\")", msg, szTag, szDescr);
	const char *szFilename;
	const char *szExt="msg";

	if (msg) {
		szFilename=hprintf(NULL, "%s/%s.%s", NoteDir(), szTag, szExt);
		msg_Save(msg, szFilename);
		szDelete(szFilename);

		Note("M|%s.%s|%s", szTag, szExt, szDescr);
	} else {
		Log("Nearly saved NULL message as %s (%s)", szTag, szDescr);
	}
}

void NoteMessageXML(rogxml *rx, const char *szTag, const char *szDescr)
{
//Log("NoteMessageXML(%x, \"%s\", \"%s\")", rx, szTag, szDescr);
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
	szDelete(szXML);
}

void NoteMessageMime(MIME *mime, const char *szTag, const char *szDescr)
{
	const char *szMime=mime_RenderHeap(mime);
	NoteMessage(szMime, -1, szTag, "mime", szDescr);
	szDelete(szMime);
}

int _nError=0;
const char *_szError = NULL;

int SetError(int nErr, const char *szFmt, ...)
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

typedef struct childinfo_t {
	int			pid;			// Process ID of the child
	time_t		tStarted;		// When it started
	const char *szIp;			// IP that triggered the child
	const char *szId;			// Message ID the child is dealing with
} childinfo_t;

static int nPids=0;
static childinfo_t *aPid = NULL;

int child_Find(int pid)
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

int child_FindAdd(int pid)
// Finds the index of the child, adding a new one if not found
// I.e. always returns a slot
{
	int i = child_Find(pid);

	if (i >= 0) return i;

	if (!aPid) {
		aPid=NEW(childinfo_t, 1);
	} else {
		RENEW(aPid, childinfo_t, nPids+1);
	}
	aPid[nPids].pid=pid;
	aPid[nPids].szIp=NULL;
	aPid[nPids].szId=NULL;

	return nPids++;
}

int child_Add(int pid, const char *szIp, const char *szId)
// Adds to the list of child processes so we can report on them and tidy them up when we die
// If a directly incoming message is being handled then bExt, szIp and szId will be set
// Otherwise we're dealing with a retry so szIp will be blank
{
	int i = child_FindAdd(pid);

	aPid[i].tStarted=time(NULL);
	aPid[i].szIp=szIp ? strdup(szIp) : NULL;
	aPid[i].szId=szId ? strdup(szId) : NULL;

	return nPids;
}

int child_Forget(int pid)
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

void child_SetId(int pid, const char *szId)
// Sets the ID that a child is currently handling
{
	int i = child_Find(pid);

	if (i > -1) {
		szDelete(aPid[i].szId);
		aPid[i].szId = strdup(szId);
		Log("Child %d(%d) is handling %s", i, pid, szId);
	}
}

static int allow_n = 0;
static char **allow_ip = NULL;
static char **allow_descr = NULL;

void allow_Init()
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

const char *allow_Allowed(const char *szIp)
// Checks if a specific IP address is allowed to access the MMTS in a 'local' manner...
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

void TidyWaiting()
// Checks all the waiting files and removes any that are for dead processes
{
	DIR *dir=opendir(szWaitingDir);
	struct dirent *d;

	if (dir) {
		while ((d=readdir(dir))) {
			const char *szName = d->d_name;
			const char *szPathname;
			FILE *fp;
			if (*szName == '.') continue;			// Skip '.' files
			if (strlen(szName) < 8) continue;		// Too short to have .waiting on the end
			if (strcmp(szName+strlen(szName)-8, ".waiting")) continue;	// Doesn't end in .waiting

			szPathname=hprintf(NULL, "%s/%s", szWaitingDir, szName);
			fp=fopen(szPathname, "r");
			if (fp) {
				char buf[50];
				int bStopped=0;
				int nPid;

				if (fgets(buf, sizeof(buf), fp)) {
					nPid=atoi(buf);				// Ok, got the IP

					if (!ProcessAlive(nPid))	// Process doesn't exist
						bStopped=1;
				}
				fclose(fp);
				if (bStopped) {					// Process not running so we can remove the file
					if (!unlink(szPathname)) {
						char *szId = strdup(szName);
						char *chp=strrchr(szId, '.');
						if (chp) *chp='\0';			// Must have a '.' in fact or test above would have failed

						Log("Stopped waiting for %s (pid=%d)", szId, nPid);
						szDelete(szId);
					}
				}
			}
		}
		closedir(dir);
	}
}

int MyBIO_flush(BIO *io)
{
#if 0
	FILE *fp=fopen("/tmp/out.dat","a");
	if (fp) {
		fputs("\n----------------------------------------------------------- FLUSH\n", fp);
		fclose(fp);
	}
#endif

	return BIO_flush(io);
}

int MyBIO_write(BIO *io, const char *szData, int nLen)
{
	int nWritten= BIO_write(io, szData, nLen);		// Entire message

	// Everything from here on is logging...

	static BIO *lastio = NULL;
	const char *szNoteDir = NoteDir();
	FILE *fp=NULL;
	FILE *fpnote=NULL;
	static int nIndex = 1;
	const char *szNoteFile = NULL;
	const char *szBase = NULL;

// Commented out as these files regularly hit 2GB and shouldn't be required anyway
//	fp=fopen("/tmp/out.dat","a");
	if (szNoteDir) {						// We're keeping notes
		if (lastio != io) nIndex++;
		szBase=hprintf(NULL, "rawout-%d", nIndex);
		szNoteFile = hprintf(NULL, "%s/%s.txt", szNoteDir, szBase);
		fpnote=fopen(szNoteFile, "a");
		szDelete(szNoteFile);
	}

	if (lastio != io) {							// We're sending to someone new
		if (fpnote) Note("M|%s.%s|%s", szBase, "txt", "Data sent");
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

	return nWritten;
}

int MyBIO_puts(BIO *io, const char *szText)
{
//	return BIO_puts(io, szText);
	return MyBIO_write(io, szText, strlen(szText));
}

void FatalSSL(const char *szFmt, ...)
// SSL errors so we can return more information
{
	va_list ap;
	char buf[1000];

	va_start(ap, szFmt);
	vsnprintf(buf, sizeof(buf), szFmt, ap);
	va_end(ap);

	Log("Exit: SSL Problem: %s", buf);		// An error essentially, but we don't care enough to make everything pink
											// Which would happen if the word 'error' were used in the log message!

	BIO_printf(bio_err, "%s\n", buf);
	ERR_print_errors(bio_err);

	Exit(98);
}

int SendDaemonSignal(int nSignal)
// Sends the signal or (if nSignal == 0), checks that we can send signals
// Returns	0		Ok
//			1		There is no daemon running
//			2		There is a daemon running but we don't have permission to signal it
{
	int nPid = GetDaemonPid();
	if (nPid) {
		int nErr;

		nErr=kill(nPid, nSignal);
		if (nErr == EPERM) return 2;
		return 0;
	} else {
		return 1;
	}
}

int StopAnyPrevious()
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

void HandleSignal(int n)
{
	signal(n, HandleSignal);

	switch (n) {
	case SIGHUP:										// 1 Exit Hangup [see termio(7)]
		break;
	case SIGINT:										// 2 Exit Interrupt [see termio(7)]
		break;
	case SIGQUIT:										// 3 Core Quit [see termio(7)]
	case SIGTERM:										// 15 Exit Terminated
		Log("Exit: Signal %s (%s)", SignalName(n), SignalDescr(n));
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
		Log("Killed - %s (%s)", SignalName(n), SignalDescr(n));				// This order in case it was 'Log' that killed us!
//		ipc_Rm(getpid());								// Tidy up our message queue
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
	Log("Unexpected signal %s (%s) - quitting", SignalName(n), SignalDescr(n));		// This order in case it was 'Log' that killed us!
	kill(getpid(), n);									// Kill ourselves
	exit(0);											// Shouldn't ever get here
}

rogxml *package_New()
{
	return rogxml_NewElement(NULL, "MMTS-Package");
}

rogxml *package_AddXML(rogxml *rxPackage, rogxml *rx)
/// Adds an XML attachment to a package
{
	rogxml *rxAttachment = rogxml_NewElement(NULL, "MMTS-Attachment");
	rogxml_LinkChild(rxAttachment, rx);
	rogxml_LinkChild(rxPackage, rxAttachment);

	return rxAttachment;
}

rogxml *package_AddData(rogxml *rxPackage, int nLen, const char *pData)
// Adds a binary attachment to a package as a base-64 encoded chunk
{
	const char *szBase64;
	rogxml *rxAttachment = rogxml_NewElement(NULL, "MMTS-Attachment");
	rogxml_SetAttr(rxAttachment, "encoding", "base64");

	szBase64 = mime_Base64Enc(nLen, pData, 64, "\r\n");
	rogxml_AddText(rxAttachment, szBase64);
	szDelete(szBase64);

	return rxAttachment;
}

rogxml *package_AddText(rogxml *rxPackage, const char *szText)
// Adds a text string as an attachment to a package, which will become BASE64 encoded
{
	return package_AddData(rxPackage, strlen(szText), szText);
}

rogxml *package_FromMessage(MSG *m)
// Creates a MMTS-Package from a message, destroying the message in the process...
{
	rogxml *rxPackage = package_New();
	rogxml *rxMsg = msg_ReleaseXML(m);

	if (rxMsg) {
		package_AddXML(rxPackage, rxMsg);
	}

	while (msg_GetAttachmentCount(m)) {
		msg_attachment *a = msg_GetAttachment(m, 1);
		rogxml *rx;

		rx=rogxml_FromText(msg_GetAttachmentText(a));
		if (rogxml_ErrorNo(rx)) {
			package_AddText(rxPackage, msg_GetAttachmentText(a));
		} else {
			package_AddXML(rxPackage, rx);
		}

		msg_DeleteMessageAttachment(m, 1);
	}

	msg_Delete(m);

	return rxPackage;
}

static char szReadBuf[1024];
static char *szReadBufp=NULL;			// Pointer to next char in buffer
static char *szReadBufEnd;				// How many characters are actually in it
int nReadStream;						// The stream to read
FILE *fpDebugReadStream = NULL;			// Debug output if wanted

// NB. To capture input from the internal connection, create a file called /tmp/mmts.in.debug
//     This will get the next chunk of characters and then get renamed to /tmp/mmts.in

int RogRename(const char *szSrc, const char *szDest);

void CloseReadStreamDebug()
{
	if (fpDebugReadStream) {
		fclose(fpDebugReadStream);
		fpDebugReadStream=NULL;
	}
}

void InitReadStream(int fd)
{
	szReadBufp=szReadBufEnd=szReadBuf;
	nReadStream = fd;
	CloseReadStreamDebug();
	if (bDebug) {
		time_t now=time(NULL);
		struct tm *tm = gmtime(&now);
		struct timeval tp;
		int msecs;

		gettimeofday(&tp, NULL);
		msecs=tp.tv_usec / 1000;

		const char *szFilename = hprintf(NULL, "/tmp/mmts.in.%02d-%02d-%02d_%02d%02d%02d.%03d.txt",
				tm->tm_mday, tm->tm_mon+1, tm->tm_year % 100,
				tm->tm_hour, tm->tm_min, tm->tm_sec,
				msecs);
		fpDebugReadStream = fopen(szFilename, "w");
		if (fpDebugReadStream) {
			Log("Input stream from application copied to %s", szFilename);
		} else {
			Log("Failed to create %s to copy input stream from application", szFilename);
		}
		szDelete(szFilename);
	}
}

// TODO:	ReadStream returns one char, I could really do with something that returns a whole buffer full
//			as it would make the compressed file reading much more efficient.
int ReadStream()
// This function is called from the rogxml library to read XML from the input stream
{
	int nGot;

	if (szReadBufp != szReadBufEnd) {
		unsigned char c=*(unsigned char *)(szReadBufp++);

		if (fpDebugReadStream) {
			putc(c, fpDebugReadStream);
			fflush(fpDebugReadStream);
		}

		return c;
	}

	nGot = recv(nReadStream, szReadBuf, sizeof(szReadBuf), 0);
	if (nGot > 0) {					// Easy case, got some data...
		if (!_inputApp) _inputApp = hbuf_New();
		hbuf_AddBuffer(_inputApp, nGot, szReadBuf);

		szReadBufEnd=szReadBuf+nGot;
		szReadBufp=szReadBuf;
		alarm(_nInterCharacterTimeout ? _nInterCharacterTimeout : 10);	// 30629 Give us another 10 secs to deal with it
		return ReadStream();			// Was: return *(unsigned char *)(szReadBufp++);
	}

	CloseReadStreamDebug();

	return -1;							// Might just indicate end of file...
}

int SendError(int fd, int nErr, int nSeverity, const char *szFmt, ...);

int SendXML(int fd, rogxml *rx, const char *szDescription)
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

			Log("Sending MMTS-Packed");
			rxPacked = rogxml_NewElement(NULL, "MMTS-Packed");
			szXML = rogxml_ToText(rxPacked);
			nLen = strlen(szXML);
			nWritten = write(fd, szXML, nLen);
			szDelete(szXML);

			if (nWritten == nLen) {			// Only write the main message if we succeeded in writing the lead element
				if( write(fd, "\n", 1) == -1 ) {			// We need a linefeed after the initial element
					LogError(errno,"Error appending newline");
				}
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
		NoteMessageXML(rx, szNoteName, szNote);
		Log("Problem %d sending to application", nSendErr);
		szDelete(szNote);
	} else {
		const char *szNote = hprintf(NULL, "%s sent to app", szDescription);
		NoteMessageXML(rx, szNoteName, szNote);
		szDelete(szNote);
	}
	szDelete(szNoteName);

	return nSendErr;
}

rogxml *ReceiveXML(int fd, int nTimeout)
// Waits up to nTimeout seconds for an XML message from the application
// Returns the XML received or an error if a timeout occurred
{
	rogxml *rx;

	if (nTimeout) {							// Arrange that we get a wakeup call if necessary
		_nAlarmNumber=2;
		_nInterCharacterTimeout=nTimeout;	// 30629 If non-zero, alarm is rest to this on an incoming character
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

	if (!strcmp(rogxml_GetLocalName(rx), "MMTS-Packed")) {			// Special case of packed input
		int c;
		const char *unpacked;
		char buf[1024];
		int got;
		int len;
		int nPacking;					// Packing status - 0=still going, 1=done, >1=error
		int nCount=0;

		_bPackXML = rogxml_GetAttrInt(rx, "packed", 1);	// Send packed stuff back by default

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
	_nInterCharacterTimeout=0;

	return rx;
}

rogxml *ErrorMessage(int nErr, int nSeverity, const char *szFmt, ...)
{
	va_list ap;
	char szErr[4096];
	rogxml *rx;

	va_start(ap, szFmt);
	vsnprintf(szErr, sizeof(szErr), szFmt, ap);
	va_end(ap);

	rx=rogxml_NewElement(NULL, "MMTS-Ack");
	rogxml_AddText(rx, szErr);
	rogxml_SetAttr(rx, "type", "error");
	rogxml_SetAttrInt(rx, "code", nErr);
	rogxml_SetAttr(rx, "severity", nSeverity ? "fatal" : "warning");

	return rx;
}

int SendError(int fd, int nErr, int nSeverity, const char *szFmt, ...)
// Sends an error code in place of the 'happy' MMTS-Ack
// nErr is the error code, nSeverity is 0=warning, 9=fatal
// Returns	0	Seemed to send ok (may not have been received ok)
//			1	Error (see errno for details)

{
	va_list ap;
	char szErr[4096];
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

rogxml *AckMessage(const char *szMsgType, const char *szMsgId, const char *szWrapperId, const char *szMsgRcv, const char *szMsgSnd)
{
	rogxml *rx;

	rx=rogxml_NewElement(NULL, "MMTS-Ack");
	rogxml_SetAttr(rx, "type", "ack");
	rogxml_SetAttrInt(rx, "code", 0);

	if (szMsgType) rogxml_AddTextChild(rx, "MsgType", szMsgType);
	if (szMsgId) rogxml_AddTextChild(rx, "MsgId", szMsgId);
	if (szWrapperId) rogxml_AddTextChild(rx, "WrapperId", szWrapperId);
	if (szMsgRcv) rogxml_AddTextChild(rx, "MsgRcv", szMsgRcv);
	if (szMsgSnd) rogxml_AddTextChild(rx, "MsgSnd", szMsgSnd);

	return rx;
}

void SendAck(int fd, const char *szMsgType, const char *szMsgId, const char *szWrapperId, const char *szMsgRcv, const char *szMsgSnd)
// Send a happy MMTS-Ack to indicate that the message has been interpreted correctly.
// This is really a help to debugging the client application rather then rejecting messages out of hand.
{
	rogxml *rxAck = AckMessage(szMsgType, szMsgId, szWrapperId, szMsgRcv, szMsgSnd);

	SendXML(fd, rxAck, "ack");

	rogxml_DeleteTree(rxAck);
}

int SendInternalMessage(int fd, MSG *m)
// Sends the msg to the application over 'fd'.
// The message is sent as a single XML lump (MMTS-Package) or as an error.
// NB. This Deletes the message.
// Returns	0		All went ok
//			1...	An error happened
{
	int nSendErr;

	if (msg_GetErrorNo(m)) {
		nSendErr = SendError(fd, msg_GetErrorNo(m), 1, "%s", msg_GetErrorText(m));
		msg_Delete(m);
	} else {
		rogxml *rxPackage = package_FromMessage(m);
		nSendErr = SendXML(fd, rxPackage, "message");
		rogxml_Delete(rxPackage);
	}

	return nSendErr;
}

const char *Translator(const char *szDir, const char *szType, const char *szId)
// Returns the translator for a message given the dir, type and id
// Returns NULL if there isn't one
// I.e. Use as szMsg=Translate("io", "hl7", "Blurb", szMsg);
// The actual translation is performed by a file in the 'translators' directory.  We first look for
// a file of the name (given the above example) of 'io-hl7-Blurb' or, if that fails, 'io-hl7'.
// This is given the name of a file containing the message and the name of a non-existant temporary
// file.  If the temporary file is created by the translator, it is used in place of the original
// message.
// Although this function doesn't care about the content of szDir, szType and szId, they should be:
// szDir - "io" for messages coming from LAN -> WAN, "oi" for messages coming from WAN -> LAN or 'end' for
//				returning messages incoming over a synchronous link.
// szType - "hl7", "mime", etc...
// szId - dependant on szType - the identity of the message or NULL for 'any'
{
	const char *szTranslator = hprintf(NULL, "%s/%s-%s-%s", szTxDir, szDir, szType?szType:"any", szId?szId:"any");

	if (access(szTranslator, 1)) {				// Doean't exist, try without the ID
		szDelete(szTranslator);
		szTranslator = hprintf(NULL, "%s/%s-%s", szTxDir, szDir, szType);
		if (access(szTranslator, 1)) {
			szDelete(szTranslator);
			Log("No translator for %s-%s-%s (this is ok)", szDir, szType?szType:"any", szId?szId:"any");
			return NULL;						// There is no translator
		}
	}

	return szTranslator;
}

const char *Translate(const char *szTranslator, int nLen, const char *szMsg)
// Calls a translator if there is one, to translate the message.
// The return value will either be the passed string or a new one on the heap, in which case
// the passed value will have been freed.
// The translator should have been returned from 'Translator' (I don't free it here).
// nLen - the length of the message (or -1 will default to strlen(szMsg))
// szMsg - the message...
// NB. Returns szMsg IFF no translation has taken place.
{
	char szOriginal[L_tmpnam];
	char szTranslated[L_tmpnam];
	const char *szCommand;
	FILE *fp;

	if (!szTranslator) return szMsg;			// Nothing to do

	if (nLen < 0) nLen=strlen(szMsg);

	if( tmpnam(szOriginal) == NULL || tmpnam(szTranslated) == NULL ) {
		Log("Translation failed: Unable to provide unique temporary file name");
		return NULL;
	}

	fp=fopen(szOriginal, "w");
	fwrite(szMsg, nLen, 1, fp);
	fclose(fp);

	szCommand = hprintf(NULL, "%s %s %s", szTranslator, szOriginal, szTranslated);
	if( system(szCommand) == -1 ) {
		Log("Error executing: %s", szCommand);
		szDelete(szCommand);
		return NULL;
	}
	szDelete(szCommand);

	fp=fopen(szTranslated, "r");
	if (fp) {
		char *szResult;
		long nFileLen;

		fseek(fp, 0, SEEK_END);
		nFileLen=ftell(fp);
		rewind(fp);
		szResult=malloc(nFileLen);
		if( fread(szResult, 1, nFileLen, fp) != nFileLen ) {
			if( ferror(fp) ) {
				Log("Error reading translation: %s",szTranslated);
			}
		}
		fclose(fp);
		szDelete(szMsg);
		szMsg=szResult;
		Log("Translated using %s", szTranslator);
	}

	unlink(szOriginal);
	unlink(szTranslated);

	return szMsg;
}

rogxml *TranslateXML(const char *szDir, const char *szType, const char *szId, rogxml *rx)
// Works like Translate() but deals solely with XML messages.
{
	const char *szTranslator = Translator(szDir, szType, szId);
	const char *sz;
	const char *sz2;

	if (!szTranslator) return rx;			// Quickly return if there is no translator

	sz=rogxml_ToNiceText(rx);
	sz2=Translate(szTranslator, -1, sz);

	if (sz2 != sz) {						// Something translated it...!
		rogxml_Delete(rx);
		rx=rogxml_FromText(sz2);
	}
	szDelete(sz2);
	szDelete(szTranslator);

	return rx;
}

const char *ConvertLdapClause(const char *szText, int nLen)
// szText is the SQL-style clause, nLen is the number of chars (or -1 for the whole string)
// Turns something like postalCode like 'PL31%' into postalCode='PL31*'
// name!='(c)2009' into !(name='\28c\292009')
// Note the way that LDAP requires the negation to come before the operand
// Note also that brackets are not put in here (except for negation) so the results needs enclosing in some
// Returns static error expression starting with '*' or a nice result on the heap
{
	const char *szResult = NULL;
	const char *chp;
	const char *szConstant;
	int nOpLen=0;											// Length of the operator
	int bWildcard=0;										// 1 if we're treating % as a wildcard
	int bNegated=0;											// 1 if we're negating and thus need a closing bracket

//Log("Clause(%d): '%s'", nLen, szText);
	szText=SkipSpaces(szText);								// Tidy up parameters
	if (nLen < 0) nLen=strlen(szText);

	// Deal with the operand
	for (chp=szText;isalnum(*chp);chp++);					// Point chp past first 'word'
	szResult=strnappend((char*)szResult, szText, chp-szText);		// Copy into result

	// Deal with the operator
	chp=SkipSpaces(chp);
	if (*chp == '!' || !strncmp(chp, "not", 3)) {			// 'not like' or '!=' primarily, but !>= is accepted...
		const char *szOld=szResult;
		bNegated=1;											// Flag that we need a closing bracket
		szResult=hprintf(NULL, "!(%s", szResult);			// Prefix the operand with '!' (it's the way LDAP does it)
		szDelete(szOld);
		chp=SkipSpaces(chp+((*chp=='!')?1:3));				// Skip over it and subsequent spaces
	}

	if (!strncmp(chp, ">=", 2)) { nOpLen=2; }
	else if (!strncmp(chp, "<=", 2)) { nOpLen=2; }
	else if (!strncmp(chp, "=", 1)) { nOpLen=1; }
	else if (!strncmp(chp, ">", 1)) { nOpLen=1; }
	else if (!strncmp(chp, "<", 1)) { nOpLen=1; }
	else if (!strncasecmp(chp, "like", 4)) { nOpLen=4; bWildcard=1; }	// NB. Set 'bWildcard' flag here

	if (!nOpLen) {
		szDelete(szResult);
//Log("Op at '%s' in %s", chp, szText);
		return "*Unrecognised operator";
	}
	if (nOpLen == 4) {										// NB. Unspecific check for 'LIKE' keyword here
		szResult=strappend(szResult, "=");					// 'LIKE' is '=' but treating '%' as a wildcard
	} else {
		szResult=hprintf(szResult, "%.*s", nOpLen, chp);
	}

	// Deal with the constant
	szConstant=SkipSpaces(chp+nOpLen);
	if (!*szConstant) {
		szDelete(szResult);
		return "*Missing constant";
	}
	chp=szText+nLen-1;										// Point to last character
	while (isspace(*chp)) chp--;
	if ((*szConstant == '\'' && *chp != '\'') ||			// Starts with a quote but doesn't end with one
			(*szConstant != '\'' && *chp == '\'')) {		// or ends with a quote but doesn't start with one
		szDelete(szResult);
		return "*Quoting mis-match in constant";
	}
	if (*szConstant == '\'') {								// Constant is quoted so adjust again
		szConstant++;
		chp--;
	}
	nLen=chp-szConstant+1;									// Adjust so we have length of constant
	while (nLen) {
		char c=*szConstant;
		const char *szStr=NULL;

		if (c == '*') szStr="\\2a";
		if (c == '%' && bWildcard) szStr="*";				// Only translate '%' to '*' if we're wildcarding
		else if (c == '(') szStr="\\28";
		else if (c == ')') szStr="\\29";
		else if (c == '\\') {
			if (nLen>1) {									// Something following it
				c=*++szConstant;
				nLen--;
			} else {
				szDelete(szResult);
				return "*Cannot end constant with lone \\";
			}
		}
		nLen--;
		szConstant++;
		if (szStr) szResult=strappend(szResult, szStr);
		else szResult=hprintf(szResult, "%c", c);
	}

	if (bNegated) szResult=strappend(szResult, ")");

	return szResult;
}

const char *ConvertLdapExpression(const char *szText)
// Takes a clause like (a='won\'t%' or a='sh(*)an\'t' and b!='x') and translates it to
// (|(a=won't*)(&(a=sh\28\2a\29an't)(!b=x))
// In fact, only handles simple expressions that are "clause" or "clause AND clause..."
//    Where 'clause' is 'name op constant' (op is =, !=, >, <, >=, <=)
// Returns a static string starting with '*' on error
{
	const char *szResult = NULL;
	const char *szClause;

//Log("Expression: %s", szText);
	if (strcasestr(szText, " AND ")) {
		const char *szAnd;
		const char *szRover;
		const char *chp;

		szResult=hprintf(szResult, "'(&");
		szRover=szText;
		szAnd=(const char *)strcasestr(szRover, " AND ");
		for (;;) {							// Termination condition is in the following 'else'
			if (szAnd) {
				chp=szAnd;
				while (chp>szRover && isspace(chp[-1])) chp--;
				szClause=ConvertLdapClause(szRover, chp-szRover);
				if (*szClause == '*') {							// Darn
					szDelete(szResult);							// Abandon result
					return szClause;							// Return the error
				}
				szResult=hprintf(szResult, "(%s)", szClause);	// Add just up to the And
				szDelete(szClause);
				szRover=SkipSpaces((char*)szAnd+5);
				szAnd=(const char *)strcasestr(szRover, " AND ");
			} else {
				szClause=ConvertLdapClause(szRover, -1);
				if (*szClause == '*') {							// Darn
					szDelete(szResult);							// Abandon result
					return szClause;							// Return the error
				}
				szResult=hprintf(szResult, "(%s)", szClause);	// Add just up to the And
				szDelete(szClause);
				break;
			}
		}
		szResult=hprintf(szResult, ")'");
	}
	else if( strcasestr(szText, " OR ") ) {
		const char *szOr;
		const char *szRover;
		const char *chp;

		szResult=hprintf(szResult, "'(|");
		szRover=szText;
		szOr=(const char *)strcasestr(szRover, " OR ");
		for (;;) {							// Termination condition is in the following 'else'
			if (szOr) {
				chp=szOr;
				while (chp>szRover && isspace(chp[-1])) chp--;
				szClause=ConvertLdapClause(szRover, chp-szRover);
				if (*szClause == '*') {							// Darn
					szDelete(szResult);							// Abandon result
					return szClause;							// Return the error
				}
				szResult=hprintf(szResult, "(%s)", szClause);	// Add just up to the And
				szDelete(szClause);
				szRover=SkipSpaces((char*)szOr+4);
				szOr=(const char *)strcasestr(szRover, " OR ");
			} else {
				szClause=ConvertLdapClause(szRover, -1);
				if (*szClause == '*') {							// Darn
					szDelete(szResult);							// Abandon result
					return szClause;							// Return the error
				}
				szResult=hprintf(szResult, "(%s)", szClause);	// Add just up to the And
				szDelete(szClause);
				break;
			}
		}
		szResult=hprintf(szResult, ")'");
	}
	else {
		szClause=ConvertLdapClause(szText, -1);
		if (*szClause == '*') {							// Darn
			return szClause;							// Return the error
		}
		szResult=hprintf(szResult, "'(%s)'", szClause);	// Simple case with no ANDs
		szDelete(szClause);
	}

	return szResult;
}

const char *LdapFromSql(const char *szSql)
// Takes pseudo SQL and returns an LDAP query in its place
// SQL is of the form "SELECT a,b,c FROM place WHERE x='XX' AND y ='YY%' and z='ZZ'"
// Result is "-b 'ou=place,o=nhs' '(&(x=XX)(y=YY*)(z=ZZ))' a b c"
// A return string starting '*' indicates an error.
// Returns	"*Error message"	- A static string - do not delete
//			"..."				- A real response, on the heap - call szDelete() when you've finished with it
{
	const char *szFrom = (const char *)strcasestr(szSql, " FROM ");
	const char *szWhere = (const char *)strcasestr(szSql, " WHERE ");
	const char *szFromPart;									// These will point just beyond the above
	const char *szWherePart;
	const char *szFields;									// The fields we'll be collecting
	const char *chp;
	const char *szResult = NULL;
	const char *szExpression;

//Log("LdapFromSql: %s", szSql);
	if (strncasecmp(szSql, "SELECT ", 7)) return "*SELECT expected at start of query";
	if (!szFrom) return "*FROM not found in query";
	if (!szWhere) return "*WHERE not found in query";

	szFields=SkipSpaces((char *)szSql+7);				// Point each place holder to the start of their section
	szFromPart=SkipSpaces((char *)szFrom+6);
	szWherePart=SkipSpaces((char *)szWhere+7);
	while (isspace(szFrom[-1])) szFrom--;				// Back up each dividing pointer to remove spaces
	while (isspace(szWhere[-1])) szWhere--;
	chp=szWherePart+strlen(szWherePart);
	while (isspace(chp[-1])) chp--;						// trim spaces from the end of the query

	szFields=strnappend(NULL, szFields, szFrom-szFields);
	szFrom=strnappend(NULL, szFromPart, szWhere-szFromPart);
	szWhere=strnappend(NULL, szWherePart, chp-szWherePart);

//	Log("Fields = '%s'", szFields);
//	Log("From = '%s'", szFrom);
//	Log("Where = '%s'", szWhere);

	// Check for '.base', '.one', '.sub' or '.children' on the 'FROM' as it's a 'scope' spec.
	chp=strrchr(szFrom, '.');
	if (chp &&
			(!strcasecmp(chp, ".base") || !strcasecmp(chp, ".one") || !strcasecmp(chp, ".sub") || !strcasecmp(chp, ".children"))) {
		*(char*)chp++='\0';											// Terminate and move on
		szResult=hprintf(szResult, "-s %s ", chp);
	}

	// The 'base' taken from the 'FROM' section
	if (strchr(szFrom, '=')) {
		szResult=hprintf(szResult, "-b '%s,o=nhs'", szFrom);		// 'o' in the EIS is ALWAYS 'nhs'
	} else {														// Simple case of ou[.ou[.ou]]...
		szResult=hprintf(szResult, "-b '");
		while ((chp=strrchr(szFrom, '.'))) {							// We have sub components
			*(char*)chp++='\0';											// Terminate and move on
			szResult=hprintf(szResult, "ou=%s,", chp);
		}
		szResult=hprintf(szResult, "ou=%s,", szFrom);				// Add on the remaining non-dotted component
		szResult=hprintf(szResult, "o=nhs'");						// 'o' in the EIS is ALWAYS 'nhs'
	}

	// Now the query part made from the 'WHERE' section
	szExpression = ConvertLdapExpression(szWhere);
	if (*szExpression == '*') {								// Darn - error, tidy up and return it
		szDelete(szFields);
		szDelete(szFrom);
		szDelete(szWhere);
		szDelete(szResult);

		return szExpression;
	}
	szResult=hprintf(szResult, " %s", szExpression);
	szDelete(szExpression);

	// Now the fields taken from the fields section
	// Forgive me for not using 'strtok()' in the following...
	// Note, this may be '*' as a special case where we don't specify anything in the query

	if (strcmp(szFields, "*")) {							// Don't do anything if it's a "SELECT * FROM..."
		const char *szRover=szFields;
		chp=strchr(szRover, ',');

		while (chp) {
			szResult=hprintf(szResult, " %.*s", chp-szRover, szRover);
			szRover=SkipSpaces((char *)chp+1);
			chp=strchr(szRover, ',');
		}
		szResult=hprintf(szResult, " %s", szRover);
	}

	szDelete(szFields);
	szDelete(szFrom);
	szDelete(szWhere);

	return szResult;
}

const char *cache_Dir()
{
	static const char *dir = NULL;

	if (!dir) {
		dir=hprintf(NULL, "%s/cache", szEnvDir);
	}

	return dir;
}

void cache_DeleteFile(const char *szDir, const char *szFilename)
{
	const char *szCacheDir=hprintf(NULL, "%s/%s", cache_Dir(), szDir);
	const char *szPath=hprintf(NULL, "%s/%s", szCacheDir, szFilename);

	unlink(szPath);

	szDelete(szCacheDir);
	szDelete(szPath);

	return;
}

int cache_SaveToFile(SSMAP *map, const char *szDir, const char *szFilename)
{
	const char *szCacheDir=hprintf(NULL, "%s/%s", cache_Dir(), szDir);
	rog_MkDir(szCacheDir);							// Ensure directory exists
	const char *szPath=hprintf(NULL, "%s/%s", szCacheDir, szFilename);

	FILE *fp=fopen(szPath, "w");

	if (fp) {
		ssmap_Reset(map);
		const char *szName;
		const char *szValue;

		while (ssmap_GetNextEntry(map, &szName, &szValue)) {
			fprintf(fp, "%s=%s\n", szName, szValue);
		}
		fclose(fp);
	}

	szDelete(szCacheDir);
	szDelete(szPath);

	return !!fp;
}

SSMAP *cache_LoadFromFile(const char *szDir, const char *szFilename, int bCheckTime)
// Reads a map from a file
// Dir should be the containing directory relative to the 'cache' directory.
// Returns NULL if nothing there or bCheckTime is set and it's out of date
{
	SSMAP *map = NULL;

	const char *szPath=hprintf(NULL, "%s/%s/%s", cache_Dir(), szDir, szFilename);

	FILE *fp=fopen(szPath, "r");
	if (fp) {
		const char *szLine;
		map=ssmap_New();

		while ((szLine=hReadFileLine(fp))) {
			szLine=SkipSpaces(szLine);
			if (*szLine && *szLine != '#') {
				const char *szEquals=strchr(szLine, '=');

				if (szEquals) {
					char *szName=(char*)strnappend(NULL, szLine, szEquals-szLine);
					char *szValue=strdup(SkipSpaces(szEquals+1));

					mmts_TrimTrailing(szName);
					mmts_TrimTrailing(szValue);
					strlwr(szName);
					ssmap_Add(map, szName, szValue);
					szDelete(szName);
					szDelete(szValue);
				}
			}
		}

		fclose(fp);

		if (bCheckTime) {
			const char *szBestBefore = ssmap_GetValue(map, "bestbefore");

			if (szBestBefore && strcmp(msg_Now(), szBestBefore) > 0) {		// There is a BB and we're beyond it
				ssmap_Delete(map);
				map=NULL;
			}
		}
	}

	szDelete(szPath);

	return map;
}

void cache_Refresh(const char *szCache, const char *szStore)
// Refreshes a cache using whatever means is usual fo this cache type
{
	if (!strcasecmp(szCache, "asids")) {
		// do the ldap query for the given asid and write the results in to the file
	} else if (!strcasecmp(szCache, "partykeys")) {
		// do the ldap query for the given asid and write the results in to the file
	} else {
		Fatal("Asked to refresh the cache '%s' for store '%s' and I don't know how!", szCache, szStore);
	}
}

const char *cache_Get(const char *szCache, const char *szStore, const char *szName, int bCheckTime)
// Retrieves the named cache item from the store within the cache
// szCache is the directory containing cache files - generally constant, e.g. PartykeyFromAsid
// szStore is the file containing the information - generally the value supplied, e.g. '242535413523' (the asid)
// szName is the item being returned - generally constant, the name of the value, e.g. "partykey"
// Returns a string on the heap or a NULL if we have nothing valid cached
{
	return NULL;
#if 0
	// TODO - this function is still under development
	const char *szDir = hprintf(NULL, "%s/%s", cache_Dir(), szCache);
	const char *szResult = NULL;

	SSMAP *map = cache_LoadFromFile(szDir, szStore, bCheckTime);

	if (map) {
		szResult = strdup(ssmap_GetValue(map, szName));
		ssmap_Delete(map);
	}

	return szResult;
#endif
}

void cache_Put(const char *szCache, const char *szStore, const char *szName, const char *szValue)
{
	return;
#if 0
	// TODO - this function is still under development
	const char *szDir = hprintf(NULL, "%s/%s", cache_Dir(), szCache);
	const char *szResult = NULL;

	SSMAP *map = cache_LoadFromFile(szDir, szStore, 1);

	if (!map) {
		map = ssmap_New();

		const char *szBestBefore = config_EnvGetString("ldapbestbefore");
		if (!szBestBefore) szBestBefore = strdup("P1D");		// Default of 1 day best before
		const char *szSellBy = msg_AddPeriod(msg_Now(), szBestBefore);
		ssmap_Add(map, "cachetime", msg_Now());
		ssmap_Add(map, "bestbefore", szSellBy);
		szDelete(szBestBefore);
		szDelete(szSellBy);
	}
	ssmap_Add(map, szName, szValue);
#endif
//SSMAP *cache_LoadFromFile(const char *szDir, const char *szFilename, int bCheckTime)
//int cache_SaveToFile(SSMAP *map, const char *szDir, const char *szFilename)
}

const char *sds_FilenameFromRequest(const char *szRequest)
// Make a filename to use to store the cached results of the request
// The critieria here is that it's a legitimate filename and unique except that the same szRequest always returns
// the same result (there must be a briefer way of describing that!)
// The result is on the heap.
{
	const char *szFilename=strdup(szRequest);
	char *chp2=(char*)szFilename;
	const char *chp;
	char c;
	char lastc='_';

	for (chp=szFilename;(c=*chp);chp++) {
		if (strchr("'\"", c)) continue;			// Skip these
		if (strchr(" ()*[]:-", c)) {				// Replace these with _
			c='_';
		}
		if (c != '_' || lastc != '_')
			*chp2++=c;
		lastc=c;
	}
	if (chp2 > szFilename && chp2[-1]=='_') chp2--;	// Drop any trailing '_'
	*chp2='\0';

	szFilename=strsubst(szFilename, "&","AND");
	szFilename=strsubst(szFilename, "|","OR");

	if (strlen(szFilename) > 78) {					// Too long for a sensible filename so we'll use a hash instead
		szDelete(szFilename);
		szFilename=strdup(guid_ToText(guid_FromSeed(szRequest, -1)));
	}

	return szFilename;
}

rogxml *sds_LdapQuery(const char *szRequest, const char *szQuery, char bLog)
// Submits an LDAP style SDS query and returns the result as XML
// If 'bLog' is 1, makes log entries otherwise keeps quiet
// Notes that the resultant XML won't be NULL but might well be an error...
{
	rogxml *rxResults=NULL;
	int nResultCount=0;

	FILE *fp;
	char *szCommand;
	const char *szLdapHost = NULL;

	szLdapHost=config_EnvGetString("ldaphost");
	if (!szLdapHost) {							// Try to default to something sensible
		if (!szEnvironment || !*szEnvironment || !strcasecmp(szEnvironment, "live") || !strcmp(szEnvironment, "0")) {
			szLdapHost=strdup("ldap.national.ncrs.nhs.uk");
		} else {
			szLdapHost=hprintf(NULL, "ldap.%s.national.ncrs.nhs.uk", szEnvironment);
		}
	}

	// See if we can find a cached version
	const char *szCacheFilename = sds_FilenameFromRequest(szRequest);
	SSMAP *map=cache_LoadFromFile("ldap", szCacheFilename, 1);		// Read in any cached version
	const char *szResult = ssmap_GetValue(map, "result");			// The previous XML result
	if (szResult) {
		rxResults=rogxml_FromText(szResult);

		if (!rogxml_ErrorNo(rxResults)) {		// We've got valid XML from the cache
			ssmap_Delete(map);

			return rxResults;					// Return a cached result
		}
	}
	ssmap_Delete(map);

	szCommand=hprintf(NULL, "ldapsearch -x -H 'ldaps://%s' %s", szLdapHost, szRequest);
	if (bLog) Log("Issuing: %s", szCommand);

	errno=0;									// We want any errors to be from here down...

	// Set environment variables so that ldapsearch knows where to find the TLS details
	const char *szCa = hprintf(NULL, "%s/certs/mt.ca", szEnvDir);
	const char *szCert = hprintf(NULL, "%s/certs/mt.cert", szEnvDir);
	const char *szNaked = hprintf(NULL, "%s/certs/mt.key.naked", szEnvDir);
	const char *szRandom = hprintf(NULL, "%s/certs/random", szEnvDir);

	EnvSet("LDAPTLS_CACERT",szCa);
	EnvSet("LDAPTLS_CERT",szCert);
	EnvSet("LDAPTLS_KEY",szNaked);
	EnvSet("LDAPTLS_REQCERT","never");
	if (!access(szRandom, 0))
		EnvSet("LDAPTLS_RANDFILE",szRandom);

	fp=popen(szCommand, "r");					// Here is where we invoke the actual ldapsearch command

	// The command has been executed at this point so we can delete our local copies of environment variables
	EnvSet("LDAPTLS_CACERT",NULL);
	EnvSet("LDAPTLS_CERT",NULL);
	EnvSet("LDAPTLS_KEY",NULL);
	EnvSet("LDAPTLS_REQCERT",NULL);
	EnvSet("LDAPTLS_RANDFILE",NULL);
	szDelete(szCa);
	szDelete(szCert);
	szDelete(szNaked);
	szDelete(szRandom);

	if (fp) {									// Run the command ok so we read and XMLify the result
		const char *szLine;
		rogxml *rxLastValue = NULL;				// Value of previous line for continuation purposes
		rogxml *rxLastResult = NULL;			// Last result so we can drop it if no 'dn'
		int bNewResult = 1;						// Set to 1 when the next result will be in a new result set
		int bHasDn = 0;							// Set once a 'dn' has been added to a result

		while ((szLine=ReadLine(fp))) {
			const char *chp;
			rogxml *rxResult;					// Each result set hangs on one of these

			if (*szLine == '#') continue;		// Skip comment lines

			if (*szLine == ' ') {				// Starts with a space, so is a continuation line
				if (rxLastValue) {									// We have somewhere to put it
					const char *szOldText;
					const char *szNewText;

					szLine++;										// Skip the space
					szOldText = rogxml_GetValue(rxLastValue);		// Get the previous value
					szNewText = hprintf(NULL, "%s%s", szOldText, szLine);	// Make a new one
					rogxml_SetValue(rxLastValue, szNewText);		// Replace with our new appended one
					szDelete(szNewText);							// And drop our copy
				} else {
					rogxml_Delete(rxResults);
					rxResults=rogxml_NewError(121, "LDAP continuation line before data ('%s')", szLine);
					break;
				}
				continue;
			}
			if (*szLine == '\0') {				// Dividing line between two results
				bNewResult=1;					// Make sure we start a new result
				continue;
			}
			chp=strchr(szLine, ':');
			if (chp) {			// Generally assume it's "name: value"
				const char *cp;
				const char *szName;

				for (cp=szLine;cp<chp;cp++) {	// Need to check 'name' is a simple alphanumeric string
					if (!isalnum(*cp)) break;
				}
				if (cp >= chp) {				// Loop invariate being false means we reached the end ok
				} else {						// Otherwise the line seems invalid...
				}
				// TODO: Decide what to do if it's not valid...?
				// For now, we're carrying on regardless.
				szName=strnappend(NULL, szLine, chp-szLine);	// szName is now on the heap
				chp++;
				while (isspace(*chp)) chp++;	// Skip spaces after the ':'

				// At this point, we're going to add in a result line so build up a correct place to put it
				if (!rxResults) {
					rxResults=rogxml_NewElement(NULL, "results");
				}

				if (bNewResult) {				// We're starting a new result set
					rxResult=rogxml_AddChild(rxResults, "result");
					rxLastResult=rxResult;		// Last result must be this one
					bNewResult=0;
					bHasDn=0;					// We don't have a dn in here yet
					nResultCount++;
				}

				rogxml *rx=rogxml_NewElement(NULL, "value");
				rogxml_AddAttr(rx, "name", szName);
				szDelete(szName);
				rogxml_AddText(rx, chp);		// Add in the value
				rxLastValue=rx;
				rogxml_LinkChild(rxResult, rx);	// Add it in
				if (!strcmp(szName, "dn")) bHasDn = 1;		// Note we have a dn if indeed we have
			}
		}

		// Got all the result, but the 'search' and 'result' parts will have appeared to be a result set
		// response except that it won't have a 'dn' so if that's the case, we'll pull out the useful
		// parts of it (the search/result values), place them into the 'results' tag and drop this last element
		if (!bHasDn && rxLastResult) {			// Last result doesn't have a dn so extract info and drop it
			rogxml *rx;

			for (rx=rogxml_FindFirstChild(rxLastResult); rx; rx=rogxml_FindNextSibling(rx)) {
				const char *szName = rogxml_GetAttr(rx, "name");

				if (!strcmp(szName, "search")) {
					rogxml_AddAttr(rxResults, "search", rogxml_GetValue(rx));
				}
				if (!strcmp(szName, "result")) {
					rogxml_AddAttr(rxResults, "result", rogxml_GetValue(rx));
				}
			}

			rogxml_Delete(rxLastResult);		// Drop this so it doesn't look like an actual result
			nResultCount--;
		}
		if (rxResults) {
			rogxml_SetAttrInt(rxResults, "count", nResultCount);
		}
		fclose(fp);
		if (!rxResults) {				// Must mean we got nothing back
			rxResults=rogxml_NewError(133, "No data returned from ldapsearch (errno=%d)", errno);
		}
	} else {
		rxResults=rogxml_NewError(130, "Call to ldapsearch failed (errno=%d)", errno);
	}

	if (bLog && !rogxml_ErrorNo(rxResults)) {
		Log("Returned %d result%s", nResultCount, nResultCount==1?"":"s");
	}

	if (rxResults && !rogxml_ErrorNo(rxResults)) {	// We've got good results and they're not cached so write them
		SSMAP *map=ssmap_New();
		const char *szResult;
		int bVerbose;

		bVerbose = rogxml_SetVerbose(0);
		szResult=rogxml_ToText(rxResults);
		rogxml_SetVerbose(bVerbose);

		const char *szBestBefore = config_EnvGetString("ldapbestbefore");
		if (!szBestBefore) szBestBefore = strdup("P1D");		// Default of 1 day best before
		const char *szSellBy = msg_AddPeriod(msg_Now(), szBestBefore);
		szDelete(szBestBefore);

		ssmap_Add(map, "request", szRequest);
		ssmap_Add(map, "result", szResult);
		ssmap_Add(map, "query", szQuery);
		ssmap_Add(map, "cachetime", msg_Now());
		ssmap_Add(map, "bestbefore", szSellBy);

// Log("Cached LDAP query (%s)", szCacheFilename);
		cache_SaveToFile(map, "ldap", szCacheFilename);
		szDelete(szResult);

		ssmap_Delete(map);
	}

	szDelete(szCacheFilename);

	return rxResults;
}

rogxml *sds_Query(const char *szQuery, char bLog)
// Returns a XML result from a SQL_style SDS query
// If 'bLog' is 1, makes log entries otherwise keeps quiet
{
	const char *szRequest=LdapFromSql(szQuery);

	if (*szRequest == '*') {
		Log("SDS Query: %s", szQuery);
		Log("Error: %s", szRequest+1);
		return rogxml_NewError(140, szRequest+1);
	}

	rogxml *rxResult = sds_LdapQuery(szRequest, szQuery, bLog);
	szDelete(szRequest);

	return rxResult;
}

rogxml *MakeSdsResult(const char *szFunction, const char *szAttrib, const char *szInput, const char *szResult, const char *szValue)
{
	rogxml *rxReply=rogxml_NewElement(NULL, "MMTS-SDS-Result");
	rogxml_AddAttr(rxReply, szAttrib, szInput);
	rogxml_AddAttr(rxReply, szResult, szValue);

	Log("%s(%s)=%s", szFunction, szInput, szValue);

	return rxReply;
}

const char *sds_GetValue(const char *szTemplate, const char *szInput, const char *szLdapVar, rogxml **pError)
// Performs an LDAP query to fetch a single value from SDS
// szTemplate is best shown by example: "services WHERE uniqueIdentifier='%s' AND objectClass=nhsAs"
// szInput is the value to substitute into the template
// szLdapVar is the variable we're looking for
// Returns a string on the heap
// If an error occurs, NULL is returned and a pointer to the error XML is left in pError
{
	const char *szValue = NULL;										// Our resultant value

	const char *szQueryBit = hprintf(NULL, szTemplate, szInput);	// services WHERE ...
	const char *szQuery = hprintf(NULL, "SELECT * FROM %s", szQueryBit);	// SELECT * FROM ...
	rogxml *rxResults = sds_Query(szQuery, 1);						// <results><result>...
	szDelete(szQuery);
	szDelete(szQueryBit);

	if (rxResults && !rogxml_ErrorNo(rxResults)) {
		rogxml *rxTmp = rogxml_FindFirstChild(rxResults);			// <result><value>...
		rogxml *rxValue;

		if (rxTmp) {												// Step through the values looking for ours
			for (rxValue=rogxml_FindFirstChild(rxTmp);rxValue;rxValue=rogxml_FindNextSibling(rxValue)) {
				if (!strcasecmp(rogxml_GetAttr(rxValue, "name"), szLdapVar)) {		// Found it
					szValue=strdup(rogxml_GetValue(rxValue));					// Take a copy of the value
					break;
				}
			}
		}
		rogxml_Delete(rxResults);
		rxResults=NULL;
	}

	if (!szValue && pError) {					// No result and the caller wants to know about errors
		if (!rxResults) rxResults = rogxml_NewError(130, "No results from SDS query");	// No error, but no match
		*pError = rxResults;
	} else {
		rogxml_Delete(rxResults);
	}

	return szValue;
}

const char *sds_PartykeyFromAsid(const char *szAsid)
{
	const char *szPartykey = cache_Get("asids", szAsid, "partykey", 1);
	if (!szPartykey) {
		szPartykey = sds_GetValue("services WHERE uniqueIdentifier='%s' AND objectClass=nhsAs", szAsid, "nhsMHSPartyKey", NULL);
		cache_Put("asids", szAsid, "partykey", szPartykey);
	}

	return szPartykey;
}

const char *sds_PartykeyFromNacs(const char *szNacs)
{
	return sds_GetValue("services WHERE nhsIDCode='%s' AND objectClass=nhsAs", szNacs, "nhsMHSPartyKey", NULL);
}

rogxml *sds_GetSingle(rogxml *rx, const char *szFunction, const char *szTemplate, const char *szAttrib, const char *szLdapVar, const char *szResultName)
// Performs an LDAP query to fetch a single value from SDS
// The input value is the 'szAttrib' attribute from 'rx', the SDS variable name is 'szLdapVar' and we're going
// to return it as an attribute of the name 'szResultName'
{
	const char *szInput = rogxml_GetAttr(rx, szAttrib);
	const char *szValue = NULL;								// Our resultant value
	rogxml *rxError;

	szValue = sds_GetValue(szTemplate, szInput, szLdapVar, &rxError);

	if (szValue) {													// We managed to find a value
		return MakeSdsResult(szFunction, szAttrib, szInput, szResultName, szValue);
	}

	return rxError;
}

const char *GetPartyKeyFromAsid(const char *szAsid, const char **pFile)
// Returns a string on the heap or a NULL if the ASID is not recognised
// If pFile is non-null then the filename used to get the mapping is returned there (szDelete it when done)
{
	const char *szFilename=hprintf(NULL, "%s/asid-partyid.map", szEnvDir);
	const char *szPartyid;

	szPartyid=config_FileGetString(szFilename, szAsid);

	if (!szPartyid) {									// Not configured manually
		szPartyid=sds_PartykeyFromAsid(szAsid);			// Try fetching from SDS (or cache)
		if (szPartyid) {
			szDelete(szFilename);						// Drop the static filename
			szFilename=strdup("SDS");					// This is where the user is told it came from
		}
	}

	if (pFile) {
		*pFile=szFilename;
	} else {
		szDelete(szFilename);
	}

	return szPartyid;
}

const char *GetAsidFromPartyKey(const char *szPartyKey, const char **pFile)
// Returns a string on the heap or a NULL if the party ID is not recognised
// If pFile is non-null then the filename used to get the mapping is returned there (szDelete it when done)
{
	const char *szFilename=hprintf(NULL, "%s/asids", szEnvDir);
	const char *szAsid;

	szAsid=config_FileGetString(szFilename, szPartyKey);
//Log("DEBUG: Reading '%s' - ASID for '%s' = '%s'", szFilename, szPartyKey, szAsid);

	if (pFile) {
		*pFile=szFilename;
	} else {
		szDelete(szFilename);
	}

	return szAsid;
}

const char *TranslatePartyKey(const char *szAbbrev, const char **pFile)
// Translate application specified partykeys so that we can control them from here.
// Returns a string on the heap or NULL if there is no translation
// *pFile gets a malloced copy of the filename from which the results were sucked if non-NULL
{
	const char *szFilename=hprintf(NULL, "%s/partykeys", szEnvDir);
	const char *szPartyKey;

	szPartyKey=config_FileGetString(szFilename, szAbbrev);
//Log("DEBUG: Reading '%s' - PARTYKEY for '%s' = '%s'", szFilename, szAbbrev, szPartyKey);

	if (pFile) {
		*pFile = szFilename;
	} else {
		szDelete(szFilename);
	}

	return szPartyKey;
}

void RemovePartyIdsAt(rogxml *rx, const char *szPath)
// Removes the partyid elements at the path.  This is done just prior to sending a P1R2 message.
// The path should be that giving the 'id' elements (e.g. "/*/communicationFunctionRcv/device/id").
{
	int i;
	rogxpath *rxp = rogxpath_New(rx, szPath);
	int nCount = rogxpath_GetCount(rxp);

//Log("P1R2: checking %d Id%s matching '%s'", nCount, nCount==1?"":"s", szPath);
	for (i=1;i<=nCount;i++) {
		rogxml *rx=rogxpath_GetElement(rxp, i);
		const char *szRoot=rogxml_GetAttributeValue(rx, "root");

		if (!strcmp(szRoot, "2.16.840.1.113883.2.1.3.2.4.10") ||		// NPfIT Messaging Endpoint ids
				!strcmp(szRoot, "2.16.840.1.113883.2.1.3.2.4.11")) {	// NHS SDS
			Log("Removed PartyId %s:%s", szRoot, rogxml_GetAttributeValue(rx, "extension"));
			rogxml_Delete(rx);						// Delete the element
		}
	}

	rogxpath_Delete(rxp);
}

void ReplacePartyIdsByAsids(rogxml *rx)
// Checks the message for all partyids and replaces them by the corresponding ASIDs
{
	rogxpath *rxp = rogxpath_New(rx, "//id");			// Get a list of all ID elements
	int i;

	for (i=1;i<=rogxpath_GetCount(rxp);i++) {
		rogxml *rx=rogxpath_GetElement(rxp, i);
		const char *szRoot=rogxml_GetAttributeValue(rx, "root");

		if (!strcmp(szRoot, "2.16.840.1.113883.2.1.3.2.4.10") ||		// NPfIT Messaging Endpoint ids
				!strcmp(szRoot, "2.16.840.1.113883.2.1.3.2.4.11")) {	// NHS SDS

			const char *szPartyId = rogxml_GetAttributeValue(rx, "extension");		// Get the PartyId
			const char *szFilename = NULL;
			const char *szAsid=GetAsidFromPartyKey(szPartyId, &szFilename);

			if (szAsid) {
				Log("Mapped partykey '%s' to ASID '%s' (%s)", szPartyId, szAsid, szFilename);
				rogxml_SetAttr(rx, "root", "1.2.826.0.1285.0.2.0.107");	// Change it to an ASID
				rogxml_SetAttr(rx, "extension", szAsid);				// Drop the ASID in place
			} else {
				const char *szPath = rogxml_GetXPathH(rx);
				Log("Failed to map partykey '%s' at %s to an ASID", szPartyId, szPath);
				szDelete(szPath);
			}
			szDelete(szAsid);
			szDelete(szFilename);
		}
	}

	rogxpath_Delete(rxp);
}

void TranslatePartyIdsAt(rogxml *rx, const char *szPath)
// Changes the party ids at the given element.
{
	int i;
	rogxpath *rxp = rogxpath_New(rx, szPath);

	for (i=1;i<=rogxpath_GetCount(rxp);i++) {
		rogxml *rx=rogxpath_GetElement(rxp, i);
		const char *szRoot=rogxml_GetAttributeValue(rx, "root");

		if (!strcmp(szRoot, "2.16.840.1.113883.2.1.3.2.4.10") ||		// NPfIT Messaging Endpoint ids
				!strcmp(szRoot, "2.16.840.1.113883.2.1.3.2.4.11")) {	// NHS SDS
			const char *szNewExt;
			const char *szExtension;
			const char *szFilename = NULL;

			szExtension=rogxml_GetAttributeValue(rx, "extension");
			szNewExt=TranslatePartyKey(szExtension, &szFilename);

			if (szNewExt && strcmp(szExtension, szNewExt)) {
				Log("Endpoint '%s' translated to '%s' (%s)", szExtension, szNewExt, szFilename);
				rogxml_SetAttr(rx, "extension", szNewExt);
			} else {
				Log("Endpoint '%s' not translated (not in %s)", szExtension, szFilename);
			}
			szDelete(szNewExt);
			szDelete(szFilename);
		}
	}

	rogxpath_Delete(rxp);
}

SSMAP *ssmap_GetFile(const char *szFilename)
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

int HarvestNhsInElement(rogxml *rx, const char *szElement)
// Search the message for any reference to an NHS number in the element type given and record it
// Returns 1 if something recorded, otherwise not...
{
	const char *path = hprintf(NULL, "//%s", szElement);
	rogxpath *rxp = rogxpath_New(rx, path);			// Get a list of all ID elements
	int result=0;
	int i;

	if (rxp) {
		for (i=1;i<=rogxpath_GetCount(rxp);i++) {
			rogxml *rx=rogxpath_GetElement(rxp, i);
			const char *szRoot=rogxml_GetAttributeValue(rx, "root");

			if (szRoot && !strcmp(szRoot, "2.16.840.1.113883.2.1.4.1")) {		// We have an NHS number
				const char *szNhs = rogxml_GetAttributeValue(rx, "extension");

				if (szNhs) {
					Note("N|%s", szNhs);
					result=1;
					break;
				}
			}
		}
		rogxpath_Delete(rxp);
	}
	szDelete(path);

	return result;
}

void HarvestNhs(rogxml *rx)
// Search the message for any reference to an NHS number and record it if found in the notes file
{
	if (!HarvestNhsInElement(rx, "id"))					// Try to find one in an id
		HarvestNhsInElement(rx, "value");				// If nothing found, try value elements
}

int TranslateIds(rogxml *rx)
// Traverse the entire messgage looking for IDS that have corresponding extension:root in /usr/mt/mmts/ids or
// /usr/mt/mmts/env/???/ids
// Returns number of changes made to the message
{
	SSMAP *ssEnv;					// Map of mappings in /usr/mt/mmts/env/???/ids
	const char *szEnvIds;			// The file of id mappings
	int nCount=0;

	szEnvIds=hprintf(NULL, "%s/ids", szEnvDir);
	ssEnv=ssmap_GetFile(szEnvIds);


	if (ssEnv) {									// No point in mapping if we have no map files
		rogxpath *rxp = rogxpath_New(rx, "//id");			// Get a list of all ID elements
		int i;

		for (i=1;i<=rogxpath_GetCount(rxp);i++) {
			rogxml *rx=rogxpath_GetElement(rxp, i);
			const char *szRoot=rogxml_GetAttributeValue(rx, "root");
			const char *szExtension = rogxml_GetAttributeValue(rx, "extension");

			if (szRoot && szExtension) {
				const char *szKey=hprintf(NULL, "%s:%s", szRoot, szExtension);
				const char *szValue=ssmap_GetValue(ssEnv, szKey);

				if (szValue) {
					Log("Changed %s:%s to %s (%s)", szRoot, szExtension, szValue, szEnvIds);
					rogxml_SetAttr(rx, "extension", szValue);
					nCount++;
				}
			}
		}

		ssmap_Delete(ssEnv);
	}

	szDelete(szEnvIds);

	return nCount;
}

void TranslateAsids(rogxml *rx)
// Checks the entire message for ASID OIDs and translates any that it finds.
// Where the ASID in the message is given as "fred", we lookup "fred" in env/*/asids and
// translate using that.
{
	rogxpath *rxp = rogxpath_New(rx, "//id");			// Get a list of all ID elements
	int i;

	for (i=1;i<=rogxpath_GetCount(rxp);i++) {
		rogxml *rx=rogxpath_GetElement(rxp, i);
		const char *szRoot=rogxml_GetAttributeValue(rx, "root");

		if (!strcmp(szRoot, "1.2.826.0.1285.0.2.0.107")) {		// Got one...
			const char *szFrom = rogxml_GetAttributeValue(rx, "extension");
			const char *szFilename = NULL;
			const char *szAsid=GetAsidFromPartyKey(szFrom, &szFilename);

			if (szAsid) {
				Log("ASID '%s' translated to '%s' (%s)", szFrom, szAsid, szFilename);
				rogxml_SetAttr(rx, "extension", szAsid);
				szDelete(szAsid);
			} else {
				Log("ASID '%s' not translated (not in %s)", szFrom, szFilename);
			}
			szDelete(szFilename);
		}
	}
}

const char *WrapperGetCommsAddr(rogxml *rxWrapper, const char *szDirection)
// Pass the entire message along with either "Rcv" or "Snd" to
// get the 19-digit address
// Returns a pointer to a string within the message (rogxml_Delete(rxWrapper) will lose it)
//			NULL	- Can't find the address
{
	const char *szPath = *szDirection == 'R' ? "/*/communicationFunctionRcv/device/id"
											 : "/*/communicationFunctionSnd/device/id";
	rogxpath *rxp = rogxpath_New(rxWrapper, szPath);
	int i;
	const char *szResult = NULL;

	if (!rxp) return NULL;

	for (i=1;i<=rogxpath_GetCount(rxp);i++) {
		rogxml *rx=rogxpath_GetElement(rxp, i);
		const char *szRoot=rogxml_GetAttributeValue(rx, "root");
#if 0	// We don't really want an ASID...
		if (!strcmp(szRoot, "1.2.826.0.1285.0.2.0.107")) {
			szResult=rogxml_GetAttributeValue(rx, "extension");
			break;
		}
#endif
		if (!strcmp(szRoot, "2.16.840.1.113883.2.1.3.2.4.11")) {			// One old OID
			szResult=rogxml_GetAttributeValue(rx, "extension");
			break;
		}
		if (!strcmp(szRoot, "2.16.840.1.113883.2.1.3.2.4.10")) {			// Anther old OID
			szResult=rogxml_GetAttributeValue(rx, "extension");
			break;
		}
	}

	rogxpath_Delete(rxp);

	return szResult;
}

const char *WrapperGetInteractionId(rogxml *rxWrapper)
// Given a wrapped message, extracts the MessageType
{
	return rogxml_GetValueByPath(rxWrapper, "/*/interactionId/@extension");
}

const char *WrapperGetMessageId(rogxml *rxWrapper)
{
	return rogxml_GetValueByPath(rxWrapper, "/*/id/@root");
}

const char *WrapperGetAcknowledgingId(rogxml *rxWrapper)
// Returns the ID of the message to which this is an acknowledgement or NULL if it isn't anything of the kind
{
	const char *szResult = rogxml_GetValueByPath(rxWrapper, "/*/acknowledgement/messageRef/id/@root");
	if (!szResult)
		szResult = rogxml_GetValueByPath(rxWrapper, "/*/acknowledgment/messageRef/id/@root");

	return szResult;
}

int SendResponse(int fd, MSG *msg)
// Send an appropriate response to the XML received
// Returns 0 if an ack was sent, non-0 otherwise.
{
	int nErr = msg_GetErrorNo(msg);
	const char *szErr = msg_GetErrorText(msg);
	const char *szRetryInterval = msg_GetRetryInterval(msg);
	time_t nRetryInterval = szRetryInterval ? msg_PeriodToSeconds(szRetryInterval) : 0;
	int nMaxRetries = msg_GetMaxRetries(msg);
	int nMaxTimeout=(nMaxRetries+1)*nRetryInterval+30;		// Add 30 seconds standard timeout time...

	const char *szMsgType =	msg_GetInteractionId(msg);
	const char *szMsgId =	msg_GetInternalId(msg);			// Note this message use 'message-id' from internal message
	const char *szWrapId =	msg_GetMessageId(msg);			// and wrapper-id from the wrapper
	const char *szMsgRcv =	msg_GetToPartyId(msg);
	const char *szMsgSnd =	msg_GetFromPartyId(msg);

	if (!nErr) {
		if (!szMsgType)		{ szErr="Cannot discern Interaction ID"; }
		else if (!szMsgId)	{ szErr="Cannot discern Message ID"; }
		else if (!szMsgRcv)	{ szErr="Cannot discern recipient address (ToPartyId)"; }
		else if (!szMsgSnd) { szErr="Cannot discern sender address (FromPartyId)"; }

		if (szErr) {
			nErr = 100;
		}
		NoteMessageMsg(msg, "err", "Erroneous message");
	}

	if (nErr) {
		SendError(fd, nErr, 1, "%s", szErr);
	} else {
		rogxml *rxAck = AckMessage(szMsgType, szMsgId, szWrapId, szMsgRcv, szMsgSnd);

		rogxml_AddTextChildf(rxAck, "WaitFor", "%d", nMaxTimeout);

		SendXML(fd, rxAck, "acceptance");

		rogxml_DeleteTree(rxAck);
	}

	return nErr;
}

int BIO_putf(BIO *b, const char *szFmt, ...)
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

static int cb_Password(char *buf, int num, int rwflag, void *userdata)
// Callback function for returning the password to get into the DES3 encrypted private key file
{
//Log("We're being asked for the password to our key file (%p,%d,%d,%p)", buf,num,rwflag,userdata);
	if(!_szPassword) return 0;						// No password available

	if(num<strlen(_szPassword)+1) return 0;			// Too short a buffer

	strcpy(buf,_szPassword);						// Copy over plain text buffer???

//Log("Returned '%s' (%d)", buf, strlen(_szPassword));
	return(strlen(_szPassword));
}

DH *get_dh1024()
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

void load_dh_params(SSL_CTX *ctx)
{
	DH *ret=get_dh1024();

	if(SSL_CTX_set_tmp_dh(ctx,ret)<0)
		FatalSSL("Couldn't set DH parameters");
}

static char *_szCtxError = NULL;

const char *ctx_Error()
{
	return _szCtxError;
}

const char *GetCertificateDir()
// Depending on the environment, returns the directory with the certifcates etc. (mt.cert, mt.ca, mt.key)
// Returns a static string so no need to free it.
{
	static char dir[50];					// Only has to hold, for example, "tspine1/etc"

	if (szEnvironment) {
		static char file[50];

		snprintf(dir, sizeof(dir), "env/%s/certs-%d", szEnvironment, _nIncomingPort);
		if (!isDir(dir))
			snprintf(dir, sizeof(dir), "env/%s/certs", szEnvironment);
		snprintf(file, sizeof(file), "%s/mt.cert", dir);

		if (!access(file, R_OK)) {
			return dir;						// NB. Secondary exit point
		}
	}

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
	const SSL_METHOD *meth;
	SSL_CTX *ctx;
	static const char *szCertCA = NULL;
	static const char *szCertLocal = NULL;
	static const char *szKeyLocal = NULL;

	szDelete(szCertCA);						// Tidy up the memory if we've been this way before
	szDelete(szCertLocal);
	szDelete(szKeyLocal);

//Log("Building TLS/SSL context using authentication in %s for %s", szConfigDir, szPrefix);

	const char *szConfig = hprintf(NULL, "%s/certs.conf", szConfigDir);
	SSMAP *ssCerts = ssmap_GetFile(szConfig);
	szDelete(szConfig);
	int nBuf = strlen(szPrefix)+10;							// Prefix plus -cert and some spare
	char *buf = (char*)malloc(nBuf);

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

//Log("ca=%s, cert=%s, key=%s, pw=%s", szCaFile, szCertFile, szKeyFile, "xyzzy");
	szDelete(_szPassword);
	_szPassword=strdup(szPw);

	szCertCA=hprintf(NULL, "%s/%s", szConfigDir, szCaFile);
	szCertLocal=hprintf(NULL, "%s/%s", szConfigDir, szCertFile);
	szKeyLocal=hprintf(NULL, "%s/%s", szConfigDir, szKeyFile);

	szDelete(buf);
	ssmap_Delete(ssCerts);
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
	if (!ctx) {
		_szCtxError = hprintf(NULL, "Failed to create an SSL/TLS context (meth=%p)", meth);
	}

	if (!_szCtxError) {								// Get our certificates from the key file
		if (!(SSL_CTX_use_certificate_chain_file(ctx, szCertLocal))) {
			_szCtxError = hprintf(NULL, "Can't read local certificate file '%s'", szCertLocal);
		}
	}

//Log("Setting the password");
	if (!_szCtxError) {								// Set the password
		SSL_CTX_set_default_passwd_cb(ctx, cb_Password);

		if(!(SSL_CTX_use_PrivateKey_file(ctx, szKeyLocal, SSL_FILETYPE_PEM))) {
			_szCtxError = hprintf(NULL, "Can't read key file '%s'", szKeyLocal);
		}
	}

//Log("Getting the list of trusted certificates");
	if (!_szCtxError) {								// Get the list of trusted certificates
		if(!(SSL_CTX_load_verify_locations(ctx, szCertCA,0))) {
			_szCtxError = hprintf(NULL, "Can't read CA list file '%s'", szCertCA);
		}
	}

	if (_szCtxError)
		Log("Error = '%s'", _szCtxError ? _szCtxError : "None at all");

	if (_szCtxError) {						// We came unstuck somewhere
FatalSSL("We failed to build a connection (%s)", _szCtxError);
		return NULL;
	} else {
		load_dh_params(ctx);				// Exits on failure

		return ctx;
	}
}

void ctx_Delete(SSL_CTX *ctx)
// Delete an SSL context
{
	if (ctx) SSL_CTX_free(ctx);
}

void ShowCertificate(X509 *cert)
	// Display the content of the certificate
{
	if (cert) {
		X509_NAME *x509Name = X509_get_subject_name(cert);
		int nEntries = X509_NAME_entry_count(x509Name);
		int i;

		for (i=0;i<nEntries;i++) {
			char szNameBuf[500];
			unsigned char *pValue;
			X509_NAME_ENTRY *ne = X509_NAME_get_entry(x509Name, i);
			ASN1_OBJECT *obj = X509_NAME_ENTRY_get_object(ne);
			ASN1_STRING *str = X509_NAME_ENTRY_get_data(ne);

			i2t_ASN1_OBJECT(szNameBuf, sizeof(szNameBuf), obj);
			pValue=ASN1_STRING_data(str);
			printf("X509-CERT %s=%s\n", szNameBuf, (char*)pValue);
		}
	} else {
		printf("No certificate\n");
	}
}

int AuthenticatePeer(SSL *ssl, const char *szPeer, int bExt)
// Returns
//	0	Failed authentication
//	1	Yay, we know this peer
{
	X509 *peer;
	char peer_CN[256];
	int err;

	err = SSL_get_verify_result(ssl);
	peer=SSL_get_peer_certificate(ssl);

	if (!peer) {
		Log("Peer %s did not hand over a certificate", PeerIp(bExt));
		return 0;
	}

	if (err != X509_V_OK) {
		ShowCertificate(peer);
		Log("Certificate from %s doesn't verify (err=%d)", PeerIp(bExt), err);
		X509_free(peer);
		return 0;
	}

	X509_NAME_get_text_by_NID(X509_get_subject_name(peer), NID_commonName, peer_CN, sizeof(peer_CN));
	X509_free(peer);

	// Allow a match either on the host name we're using or the IP address of that host
	if (szPeer) {
		if (strcasecmp(peer_CN, szPeer) && strcasecmp(peer_CN, PeerIp(bExt))) {
			ShowCertificate(peer);
			Log("Certificate (%s) doesn't match peer (%s at %s)", peer_CN, szPeer, PeerIp(bExt));
			return 0;
		}

		Log("Authenticated peer as being '%s' on '%s' at '%s'", peer_CN, szPeer, PeerIp(bExt));
		return 1;
	} else {
		if (strcmp(peer_CN, PeerIp(bExt))) {
			ShowCertificate(peer);
			Log("Certificate (%s) doesn't match peer IP %s", peer_CN, PeerIp(bExt));
			if (!strcasecmp("devsys", peer_CN)) {
				Log("But it's DevSys so it's ok...!");
			} else if (!strcasecmp("localhost", peer_CN)) {
				Log("But it's localhost so it's ok...!");
			} else if (!strcasecmp("npfit.microtest.thirdparty.nhs.uk", peer_CN)) {
				Log("But it's us so it's ok...!");
			} else {
				return 0;
			}
		}

		Log("Authenticated peer as being '%s'", PeerIp(bExt));
		return 1;
	}
}

void EchoCertificate(BIO *io, X509 *cert)
// Echoes the certificate information back to the sender for debugging purposes
{
	if (cert) {
		X509_NAME *x509Name = X509_get_subject_name(cert);
		int nEntries = X509_NAME_entry_count(x509Name);
		int i;

		for (i=0;i<nEntries;i++) {
			char szNameBuf[500];
			unsigned char *pValue;
			int nValueLen;
			X509_NAME_ENTRY *ne = X509_NAME_get_entry(x509Name, i);
			ASN1_OBJECT *obj = X509_NAME_ENTRY_get_object(ne);
			ASN1_STRING *str = X509_NAME_ENTRY_get_data(ne);

			i2t_ASN1_OBJECT(szNameBuf, sizeof(szNameBuf), obj);
			pValue=ASN1_STRING_data(str);
			nValueLen = ASN1_STRING_length(str);
			BIO_putf(io, "X509 CERT(%s) = '%.*s'<br>\r\n", szNameBuf, nValueLen, pValue);
//			printf("X509 CERT(%s) = '%.*s'\n", szNameBuf, nValueLen, pValue);
		}
//		X509_NAME_get_text_by_NID(X509_get_subject_name(cert), NID_commonName, peer_CN, sizeof(peer_CN));
	} else {
		BIO_putf(io, "No certificate sent by the client<br>\r\n");
	}
}

typedef struct note_t {
	int nEntries;
	char **pszEntry;				// Most entries
	int nMessages;
	char **pszMessage;			// 'M' message entries
} note_t;

void note_Delete(note_t *n)
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

note_t *note_LoadMessage(const char *szDir)
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

int note_GetEntryCount(note_t *n)		{return n->nEntries;}
int note_GetMessageCount(note_t *n)		{return n->nMessages;}

const char *note_GetEntry(note_t *n, int i)
{
	return (i<1 || i>n->nEntries) ? NULL : n->pszEntry[i-1];
}

const char *note_GetMessage(note_t *n, int i)
{
	return (i<1 || i>n->nMessages) ? NULL : n->pszMessage[i-1];
}

const char *note_FindEntry(note_t *n, char c)
{
	int nNotes=note_GetEntryCount(n);
	int i;

	for (i=1;i<=nNotes;i++) {
		const char *szEntry = note_GetEntry(n, i);

		if (*szEntry == c) return szEntry+2;
	}

	return NULL;
}

int HasLogError(const char *szFile)
// Returns 1 if the file has the word 'error' in it, 0 otherwise
{
	int bResult=0;
	FILE *fp=fopen(szFile, "r");

	if (fp) {
		const char *szLine;

		while ((szLine=ReadLine(fp))) {
			if (strcasestr(szLine, "error")) {
				bResult=1;
				break;
			}
		}
		fclose(fp);
	}

	return bResult;
}

const char *StatusLink(int d, int m, int y, int bDeep, const char *szFilter)
{
	static const char *result=NULL;
	const char *suffix;

	szDelete(result);

	if (szFilter && *szFilter) {
		const char *szEncodedFilter = UriEncode(szFilter);

		suffix=hprintf(NULL,"?%sf=%s", bDeep?"deep=on&":"", szEncodedFilter);
		szDelete(szEncodedFilter);
	} else {
		suffix=strdup("");
	}

	if (d)		result=hprintf(NULL, "\"/status/20%02d/%02d/%02d%s\"", y, m, d, suffix);
	else if (m) result=hprintf(NULL, "\"/status/20%02d/%02d%s\"", y, m, suffix);
	else		result=hprintf(NULL, "\"/status/20%02d%s\"", y, suffix);

	szDelete(suffix);

	return result;
}

const char *FilterDayLink(int day, int month, int year, const char *szText)
// Return an effectively static string that will link to a search screen with items filtered by the string
{
	static const char *szResult = NULL;
	szDelete(szResult);

	if (year < 99) year+=2000;
	if (szText && *szText) {
		const char *szEncodedFilter = UriEncode(szText);
		szResult=hprintf(NULL, "<a href=\"/status/%04d/%02d/%02d?f=%s\">%s</a>",
				year, month, day, szEncodedFilter, szText);
		szDelete(szEncodedFilter);
	} else {
		szResult=hprintf(NULL, "<a href=\"/status/%04d/%02d/%02d\">%s</a>", year, month, day, szText);
	}

	return szResult;
}

int GrepFile(const char *szDir, const char *szFile, const char *szSearch)
// length(szSearch) must be <2k or it won't be found
// szDir and szFile must combine to produce a good filename or it'll return not found
// Returns	1	found
//			0	not found
{
	if (!szSearch || !*szSearch) return 1;

	int bFound=0;
	FILE *fp=NULL;

	if (szDir && szFile) {
		const char *szFilename = hprintf(NULL, "%s/%s", szDir, szFile);
		fp=fopen(szFilename, "r");
		szDelete(szFilename);
	}

	if (fp) {
		char buf[2048];								// We'll read this much at a time
		int len=strlen(szSearch);
		int offset=0;
		int got;

		while ((got=fread(buf+offset, 1, sizeof(buf)-offset-1, fp))) {
			buf[got]='\0';
			if (strcasestr(buf, szSearch)) {
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

const char *th(int d)
{
	const char *suffix[]={"th","st","nd","rd"};

	if (d > 19) d=d%10;
	if (d > 3 || d < 0) d=0;

	return suffix[d];
}

const char *DayZipFile(int day, int month, int year)
{
	static const char *result = NULL;
	szDelete(result);

	result = hprintf(NULL, "%s/%02d/%02d/mmts-%02d%02d%02d.zip",
					szMsgLogDir, year, month, year, month, day);
	return result;
}

int DirCount(const char *szDir)
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

void DisplayCalendar(BIO *io, int d, int m, int y, int bDeep, const char *szFilter)
// Displays a calendar and whatnot to enable speedy selection of messages
{
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
	BIO_putf(io, "td.month {font-family:verdana; font-size:14pt; text-align:center}");
	BIO_putf(io, "td.day {text-align:center; width:40px}");
	BIO_putf(io, "td.nonday {text-align:center; width:40px}");
	BIO_putf(io, "</style>\n");

	BIO_putf(io, "<table border>\n");
	BIO_putf(io, "<tr>");
	int year;
	for (year=year1;year<=yearn;year++) {
		BIO_putf(io, "<td colspan=\"2\" style=\"width:50px;text-align:center\"><a style=\"text-decoration:none; font-weight:bold;font-size:10pt;color:black\" href=%s>", StatusLink(0, 0, year, bDeep, szFilter));
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
				BIO_putf(io, "<td colspan=\"2\" style=\"text-align:center;background-color:%s\"><a style=\"text-decoration:none; font-weight:bold;font-size:10pt;color:%s\" href=%s>20%02d</a></td>",
						background,
						"white", 
						StatusLink(0, 0, myYear, bDeep, szFilter),
						myYear);
			} else {
				int month;
				for (month=r*2+1;month<=r*2+2;month++) {
					BIO_putf(io, "<td style=\"text-align:center;background-color:%s\"><a style=\"text-decoration:none; font-weight:bold;font-size:10pt;color:%s\" href=%s>%.3s</a></td>",
						background,
						(month == thisMonth && year == thisYear) ? "yellow" : "white", 
						StatusLink(0, month, year, bDeep, szFilter),
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
					int count=DirCount(szDay);
					char szCount[10];

					if (count) {
						snprintf(szCount, sizeof(szCount), "%d", count);
					} else {
						if (!access(DayZipFile(day, month, year), 4)) {
							strcpy(szCount, "(arc)");
						} else {
							strcpy(szCount, "&nbsp;");
						}
					}

					szDelete(szDay);
					BIO_putf(io, "<td class=\"day\" style=\"%s\">"
							"<a style=\"text-decoration:none; font-weight:bold;font-size:12pt;color:%s\" href=%s>"
							"%d"
							"<br/>"
							"<div style=\"font-size:10pt;color:black\">%s</div>"
//							"<font size=\"-1\">%s</font>"
							"</a>"
							"</td>",
							szStyle,
							colour,
							StatusLink(day, month, year, bDeep, szFilter),
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

void DisplayMsgLogLine(BIO *io, const char *dir, int bDeep, const char *szFilter, int day, int month, int year)
// start and end are yymmdd dates for filtering
{
	static int nRow = 1;
	const char *szType;
	const char *szDescription;
	const char *szTmp;
	char *szIP = NULL;
	const char *szInteractionName = NULL;
	const char *szInteractionSection = NULL;
	const char *szColour;
	const char *szUuid = NULL;						// UUID from the message
	const char *szConv = NULL;						// Conversation ID from the message
	const char *szProject = NULL;
	const char *szFrom = NULL;						// Addressing information
	const char *szTo = NULL;
	const char *szEnv = NULL;
	const char *szZInfo = NULL;
	const char *szNhs = NULL;
	const char *szDateTime = NULL;

	char fulldir[100];
	snprintf(fulldir, sizeof(fulldir), "%s/%s", szMsgLogDir, dir);

	int bInclude = 1;
	if (szFilter) {
		bInclude = GrepFile(fulldir, "info", szFilter);
//BIO_putf(io, "Looking in %s/%s for bollocks", dir, "log");
		if (bDeep && !bInclude)
			bInclude = GrepFile(fulldir, "rcv.xml", szFilter);
	}

	if (!bInclude) return;
	note_t *note=note_LoadMessage(dir);
	szType = note_FindEntry(note, 'Y');
	if (!szType) szType="Unknown";
	szDescription = note_FindEntry(note, 'H');
	if (!szDescription) szDescription="Unknown";
	szFrom = note_FindEntry(note, 'F');		if (!szFrom) szFrom = "-";		// Sender
	szTo = note_FindEntry(note, 'T');		if (!szTo) szTo = "-";			// Recipient
	szUuid = note_FindEntry(note, 'I');		if (!szUuid) szUuid="-";		// UUID
	szConv = note_FindEntry(note, 'C');		if (!szConv) szConv="-";		// Conversation ID
	szProject = note_FindEntry(note, 'P');									// Project
	szEnv = note_FindEntry(note, 'E');		if (!szEnv) szEnv="?";			// Environment
	szNhs = note_FindEntry(note, 'N');		if (!szNhs) szNhs="-";			// NHS Number
	szZInfo = note_FindEntry(note, 'Z');	// Extra descriptive text to take place of message type
	szTmp = note_FindEntry(note, 'S');
	szDateTime = note_FindEntry(note, 'D');	if (!szDateTime) szDateTime="00-00-0000 00:00:00.00 Sun";
	if (szTmp) {
		char *chp;
		szIP=strdup(szTmp);
		for (chp=szIP;*chp;chp++) if (*chp == '|') *chp='\0';		// Don't care about peer port number
	} else {
		szIP=strdup("-");
	}

	const char *szLogFile = hprintf(NULL, "%s/log", fulldir);
	int bErr = HasLogError(szLogFile);
	szDelete(szLogFile);

	szInteractionName = MessageDescription(szType, &szInteractionSection);
	if (!szInteractionName) szInteractionName="Unknown";
	if (!szInteractionSection) szInteractionSection="Unk";
	if (!szProject) szProject=szInteractionSection;

	if (!strcmp(szInteractionName, "RPC")) { szProject="RPC"; }
	if (!strncmp(szZInfo, "SDS Query", 9)) { szProject="SDS"; }		// Don't like this

	szColour = "white";
	if (!strcasecmp(szProject, "Gen")) szColour="#ffddff";
	if (!strcasecmp(szProject, "PDS")) szColour="#ffdddd";
	if (!strcasecmp(szProject, "CaB")) szColour="#ddffdd";
	if (!strcasecmp(szProject, "eTP")) szColour="#ffffdd";
	if (!strcasecmp(szProject, "GP2")) szColour="#ddffff";

	nRow++;
	BIO_putf(io, "<tr ");
	if (bErr) {
		BIO_putf(io, "bgcolor=\"ffbbbb\">");
	} else {
		if (nRow & 1) {
			BIO_putf(io, "bgcolor=\"ffffff\">");
		} else {
			BIO_putf(io, "bgcolor=\"dddddd\">");
		}
	}
	BIO_putf(io, "<td nowrap=1>%.2s-%.2s-%.4s", szDateTime+0, szDateTime+3, szDateTime+6);
	BIO_putf(io, "<td>%.2s:%.2s:%.2s", szDateTime+11, szDateTime+14, szDateTime+17);

	BIO_putf(io, "<td><a href=\"/getinteraction/%s\">", dir);
	BIO_putf(io, "%.2s%.2s%.2s%s", dir, dir+3, dir+6, dir+9);
	BIO_putf(io, "</a>");

	BIO_putf(io, "<td nowrap=1>%s</td>", FilterDayLink(day, month, year, szEnv));
	BIO_putf(io, "<td nowrap=1>%s</td>", FilterDayLink(day, month, year, szFrom));
	BIO_putf(io, "<td nowrap=1>%s</td>", FilterDayLink(day, month, year, szTo));
	BIO_putf(io, "<td nowrap=1>%s</td>", FilterDayLink(day, month, year, szNhs));
	if (*szUuid == '-') {
		BIO_putf(io, "<td>-</td>");
	} else {
		BIO_putf(io, "<td class='TD_MSGID'>%s</td>", FilterDayLink(day, month, year, szUuid));
	}
	if (*szConv == '-') {
		BIO_putf(io, "<td>-</td>");
	} else {
		BIO_putf(io, "<td class='TD_MSGID'>%s</td>", FilterDayLink(day, month, year, szConv));
	}
	BIO_putf(io, "<td class='TD_%s' nowrap=1 bgcolor=\"%s\">%s</td>", szProject, szColour, FilterDayLink(day, month, year, szProject));
	if (szZInfo) {
		BIO_putf(io, "<td nowrap=1 colspan=2>%s</td>", FilterDayLink(day, month, year, szZInfo));
	} else {
		BIO_putf(io, "<td nowrap=1>%s</td>", FilterDayLink(day, month, year, szType));
		BIO_putf(io, "<td nowrap=1>%s</td>", FilterDayLink(day, month, year, szInteractionName));
	}
	BIO_putf(io, "<td>%s</td>", FilterDayLink(day, month, year, szIP));
	BIO_putf(io, "<td nowrap=1>%s</td>", szDescription);

	szDelete(szIP);
}

int strrcmp(const char *a, const char *b)
{
	return strcmp(b, a);
}

void DisplayMessages(BIO *io, int bDeep, const char *szFilter, int fromDay, int fromMonth, int fromYear, int toDay, int toMonth, int toYear)
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

		if (!isDir(dir)) {
			const char *szZip = DayZipFile(day, month, year);

			if (!access(szZip, 4)) {
				Log("Unzipping messages for %d/%02d/%02d", day, month, year);
				BIO_putf(io, "<br><font color=red>Please wait while messages for %d-%.3s-20%02d are retrieved from archive...</font><br>", day, MonthNames[month], year);
				const char *szCommand = hprintf(NULL, "unzip -qqoXK %s '%s/%02d/%02d/%02d/*'",
						szZip, szMsgLogDirLeaf, year, month, day);
				system(szCommand);
				szDelete(szCommand);
			}
		}

		if (isDir(dir)) {
			DIR *fdir = opendir(dir);
			struct dirent *d;
			SSET *names = sset_New();

			while ((d = readdir(fdir))) {
				const char *name = d->d_name;
				char msgdir[100];

				snprintf(msgdir, sizeof(msgdir), "%s/%s", dir, name);

				if (*name != '.' && isDir(msgdir)) {
					sset_Add(names, name);
				}
			}
			closedir(fdir);

			int count = sset_Count(names);
			if (count) {
				sset_Sort(names, strrcmp);
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
					DisplayMsgLogLine(io, reldir, bDeep, szFilter, day, month, year);
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

	DisplayCalendar(io, fromDay, fromMonth, fromYear, bDeep, szFilter);

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
	MyBIO_puts(io, "<th>Time</th>");
	MyBIO_puts(io, "<th>Directory</th>");
	MyBIO_puts(io, "<th>Env</th>");
	MyBIO_puts(io, "<th>From</th>");
	MyBIO_puts(io, "<th>To</th>");
	MyBIO_puts(io, "<th>NHS</th>");
	MyBIO_puts(io, "<th>UUID</th>");
	MyBIO_puts(io, "<th>CONV</th>");
	MyBIO_puts(io, "<th>Proj</th>");
	MyBIO_puts(io, "<th>Int</th>");
	MyBIO_puts(io, "<th>Description</th>");
	MyBIO_puts(io, "<th>IP</th>");
	MyBIO_puts(io, "<th>Conn</th>");
	MyBIO_puts(io, "</tr>\n");

	DisplayMessages(io, bDeep, szFilter, fromDay, fromMonth, fromYear, toDay, toMonth, toYear);

	BIO_putf(io, "</table>");
}

const char *CreateReceiveDir(MSG *msg)
// Creates a receive dir for the message to be placed in.
// This is within 'in' if there is no URI in the message, otherwise it'll be in 'in/<URI>' if it exists or
// in/unknown otherwise
{
	const char *szParent;
	const char *szDir;
	const char *szURI = msg_GetURI(msg);

	if (szURI) {

		if (*szURI == '/') szURI++;

		szParent=hprintf(NULL, "%s/%s", szInDir, szURI);
		if (!isDir(szParent)) {
			szDelete(szParent);
			szParent=hprintf(NULL, "%s/unknown", szInDir);
			MkDir(szParent);
		}
	} else {
		szParent=hprintf(NULL, "%s/unspecified", szInDir);
		MkDir(szParent);
	}

	szDir=hprintf(szParent, "/%s.tmp", InternalId());

	MkDir(szDir);

	return szDir;
}

int MyBIO_getc(BIO *io)
// Gets a single character (0..255) or EOF
{
	unsigned char c;
	int nGot;
	int nResult;
	static int bLog = -1;

	static BIO *lastio = NULL;
	static int nIndex = 1;
	const char *szNoteDir;
	FILE *fp;
	FILE *fpnote;
	const char *szNoteFile;
	const char *szBase;

	nGot = BIO_read(io, (unsigned char *)&c, 1);
	if (nGot == 1) {
		nResult = c;
		if (!_inputBuf) _inputBuf = hbuf_New();
		hbuf_AddChar(_inputBuf, c);
	} else {
		nResult = EOF;
	}

	if (bLog == -1) {
		bLog = config_GetInt("binlog", 0);
	}
	bLog=0;					// TODO: Review this - currently this means no further logging in here
//	bLog=1;

	if (!bLog) return nResult;

	// Everything from here on is logging...!

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
		if (fpnote) Note("M|%s.%s|%s", szBase, "txt", "Data received");
		lastio=io;
	}
	if (nResult != EOF) {					// We read some stuff
		if (fp) fputc(c, fp);
		if (fpnote) fputc(c, fpnote);
	} else {								// End of file
		if (fp) fputs("\n----------------------------------------------------------- EOF (sleeping)\n", fp);
		sleep(1);
		nIndex++;
		if (nIndex > 101) {
			Log("Runaway attachment problem - quitting");
			Exit(0);
		}
	}
	if (fp) fclose(fp);
	if (szNoteDir) {
		szDelete(szBase);
		if (fpnote) fclose(fpnote);
	}

	return nResult;
}

int MyBIO_gets(BIO *io, char *buf, int nLen)
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

const char *BIO_GetLine(BIO *io)
// NB. This now returns the line ending characters as part of the string...
{
	static char buf[1024]="{empty}";
	int nGot;

	nGot=MyBIO_gets(io, buf, sizeof(buf)-1);

	if (nGot > 0) {
		return buf;
	} else {
		return NULL;
	}
}

BIO *mimeio = NULL;

int getiochar()
{
	return MyBIO_getc(mimeio);
}

MIME *GetIOMIME(BIO *io)
{
	mimeio=io;

	return mime_ReadFn(getiochar);
}

MIME *GetIOMIMEHeader(BIO *io)
{
	mimeio=io;

	return mime_ReadHeaderFn(getiochar);
}

void SplitEndpoint(const char *szEndpoint, const char **pszProtocol, const char **pszAddress, int *pnPort, const char **pszURI, int bAllowDefault)
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

int isValidEndpoint(const char *szEndpoint)
// Splits up the endpoint and sees if it goes ok
{
	const char *szProtocol = NULL;
	const char *szAddress = NULL;
	int nPort = 0;
	const char *szURI = NULL;

	SplitEndpoint(szEndpoint, &szProtocol, &szAddress, &nPort, &szURI, 0);
	szDelete(szProtocol);
	szDelete(szAddress);
	szDelete(szURI);

	return szProtocol && szAddress;
}

int GetAddressingData(MSG *msg, const char **pszEndpoint, const char **pszProtocol, const char **pszAddress, int *pnPort, const char **pszURI)
// Gets the endpoint addressing information
// *pszEndpoint, *pszProtocol, *pszAddress and *pszURI will need to be szDelete()'d
// Returns	0	Ok
//			1	No address available for receiving party
{
	const char *szToPartyId = msg_GetToPartyId(msg);		// TODO: Check why we need this...
	const char *szEndpoint = NULL;
	contract_t *c;

	msg_Save(msg, "/tmp/toparty.msg");
	contract_Delete(msg->contract);							// TODO: KLUDGE: Force re-load of contract...
	msg->contract=NULL;

	szEndpoint = msg->szEndpoint;
	if (!szEndpoint) {
		c = msg_GetContract(msg);
		if (c) {
			szEndpoint = contract_GetEndpoint(c);
			if (szEndpoint) szEndpoint=strdup(szEndpoint);
		} else {
			Log("Cannot find endpoint - no contract attached to message");
		}
	}

//Log("Endpoint = '%s' (%x)", szEndpoint, szEndpoint);
	if (szEndpoint) {
		if (pszProtocol || pszAddress || pnPort || pszURI) {	// Wants some subcomponent
			if (pszProtocol) *pszProtocol=NULL;
			if (pszAddress) *pszAddress=NULL;
			if (pnPort) *pnPort=0;
			if (pszURI) *pszURI=NULL;

			SplitEndpoint(szEndpoint, pszProtocol, pszAddress, pnPort, pszURI, 1);
		}
		if (pszEndpoint) {
			*pszEndpoint=szEndpoint;				// Caller wants it
		} else {
			szDelete(szEndpoint);
		}
		return 0;									// Don't really get problems or they'll be picked up later
	} else {
		if (szToPartyId)
			SetError(28, "No endpoint in contract");
		else
			SetError(29, "No ToParty ID in message");
		return 1;
	}
}

const char *MakeHttpMessage(MSG *msg, int bAck, int nCode, const char *szCode)
// nCode is the response code, szCode is the accompanying text:
//		0		This is a POST
//		200		Ok, etc...
{
	const char *szBody = NULL;
	const char *szResult = NULL;
	const char *szURI;					// = contract_GetURI(msg_GetContract(msg));
	const char *szHost;
	int i;
	rogxml *rx;
	int bIsEbXml = msg_IsEbXml(msg);
	const char *szSoapAction = NULL;
	MIME *mainpart;						// Pointer to first body part
	int bIsMultipart = 0;				// 1 iff a multipart message
	time_t now = time(NULL);
	struct tm *tm;
	const char *szAscTime;
	const char *szDate;
	const char *szService;				// The service (or pseudo-service) - ptr to a static buffer somewhere

	MIME *m;

//Log("Building MIME for code %d (%s)", nCode, szCode?szCode:"<outgoing>");
	// In days of old SoapAction was made like: "urn:<service>/<interactionid>" for ebXML and "" for NASP
	// Now it seems that it should be in the first format for both types.  Unfortunately, we don't know the
	// service type for NASP messages so we'll make it up from the 'etc/interactions' file.
	// TODO: SoapAction needs to be non-blank for both ebXML and NASP (how do we know the service?)
	// We'll try this - always get the information from the contract as they seem to be better nowadays

	if (bAck) {
		szService = "Acknoledgement";

		m = mime_New(XML_MIME_TYPE);
	} else if (bIsEbXml) {
		contract_t *contract = msg_GetContract(msg);
		szService=contract_GetService(contract);

//		m = mime_NewMultipart();
//		bIsMultipart = 1;
		m = mime_New(XML_MIME_TYPE);
	} else {
		contract_t *contract = msg_GetContract(msg);
		szService=contract_GetService(contract);

		m = mime_New(XML_MIME_TYPE);
	}

	szSoapAction = msg_GetSoapAction(msg);
//Log("Soap action = '%s'\n", szSoapAction);
	if (!szSoapAction) {
		if (!strcasecmp(msg_GetInteractionId(msg), "PAFToolQuery")) { // Gazetteer kludge...
			szSoapAction = hprintf(NULL, "\"urn:nhs:names:services:gazetteer\"");
		} else {
			// Used to add an extra 'urn:' in here (i.e. NULL, "\"urn:%s/%s\"")
			szSoapAction = hprintf(NULL, "\"%s/%s\"", szService, msg_GetInteractionId(msg));
		}
	}

	tm = gmtime(&now);
	szAscTime = asctime(tm);
	szDate = hprintf(NULL, "%.3s, %.2s %.3s %.4s %.8s GMT",
		szAscTime, szAscTime+8, szAscTime+4, szAscTime+20, szAscTime+11);
//Log("Message date = %s", szDate);

//	mime_AddHeader(m, "User-Agent", "Microtest-MMTS");
	mime_AddHeader(m, "SOAPAction", szSoapAction);
//	mime_AddHeader(m, "X-MICROTEST-LocalId", InternalId());
//	mime_AddHeader(m, "Connection", "Keep-Alive");
//	mime_AddHeader(m, "Accept", "*/*");
//	mime_AddHeader(m, "Date", szDate);

	szDelete(szDate);
	szDelete(szSoapAction);

	const char *szAuthType = msg_Attr(msg, "auth-type");
	const char *szAuthUser = msg_Attr(msg, "auth-user");
	const char *szAuthPassword = msg_Attr(msg, "auth-password");
	if (!szAuthType) szAuthType = "Basic";
	if (szAuthUser && szAuthPassword) {
		if (!strcasecmp(szAuthType, "Basic")) {
			const char *szUserPass = hprintf(NULL, "%s:%s", szAuthUser, szAuthPassword);
			const char *szBase64 = mime_Base64Enc(strlen(szUserPass), szUserPass, 0, NULL);
			const char *szValue = hprintf(NULL, "Basic %s", szBase64);

			mime_AddHeader(m, "Authorization", szValue);
			szDelete(szValue);
			szDelete(szBase64);
			szDelete(szUserPass);
		} else {
			Log("Unknown authentication type '%s'", szAuthType);
		}
	}

	rx=msg_ReleaseXML(msg);
	{
		rogxml *rxPI = rogxml_FindProlog(rx);		// sets the version as a side-effect
		rogxml_SetAttr(rxPI, "encoding", "utf-8");
	}
	msg_SetXML(msg, rogxml_FindRoot(rx));

	// Need to set XML output to specifications in case we're sending to something non-compliant
	// or someone is going to put the output into a Windows editor...
	rogxml_SetIndentString(szXmlIndent);
	rogxml_SetLinefeedString(szXmlLinefeed);

	mime_AddBodyPart(m, XML_MIME_TYPE, -1, rogxml_ToText(msg_GetXML(msg)));

	if (!bAck && bIsEbXml) {			// Maybe Nasp must be single part...
		if (msg_GetAttachmentCount(msg)) {
			mime_ForceMultipart(m);				// MUST be after adding first body part
			bIsMultipart = 1;
		}
	} else {
		bIsMultipart = 0;
	}
	mainpart=mime_GetBodyPart(m, 1);		// May have moved when forcing multipart

//{ const char *szBody = mime_RenderHeap(m);FILE *fp=fopen("/tmp/one.mime","w");fprintf(fp, "%s", szBody);fclose(fp);}
//Log("Multipart = %d", bIsMultipart);
	if (bIsMultipart) {						// They need to know which is the first bodypart...!
		const char *szInteraction = msg_GetInteractionId(msg);
		const char *szGuid;
		if (szInteraction) {
			szGuid = hprintf(NULL, "%s.xml@hl7.com", szInteraction);
		} else {
			szGuid = strdup(guid_ToText(NULL));
		}
		const char *szStart = hprintf(NULL, "<%s>", szGuid);
		const char *szStartRef = hprintf(NULL, "%s", szStart);		// SC's mod to remove containing '"'

		mime_SetBoundary(m, "MIME_boundary");
		mime_SetParameter(m, "Content-Type", "start", szStartRef);			// Put a reference to it here
		// Next line SC'smode to remove '"' around the text/xml part
		mime_SetParameter(m, "Content-Type", "type", "text/xml");			// Say what type everything is...??
		mime_SetParameter(m, "Content-Type", "charset", "UTF-8");			// Specify the character set

		mime_SetHeader(mainpart, "Content-ID", szStart);					// Mark the first bodypart
		mime_SetParameter(mainpart, "Content-Type", "charset", "UTF-8");	// Specify the character set
		mime_SetHeader(mainpart, "Content-Transfer-Encoding", "8bit");		// Specify the character set

		szDelete(szGuid);
		szDelete(szStart);
		szDelete(szStartRef);
	}

	for (i=1;i<=msg_GetAttachmentCount(msg);i++) {
		msg_attachment *a=msg_GetAttachment(msg, i);
		MIME *part;
		const char *szAttachmentName = msg_GetAttachmentName(a);
		const char *szAttachmentId = msg_GetAttachmentContentId(a);
		const char *szBase64;
		int bBase64 = 0;
		const char *szContentType = msg_GetAttachmentContentType(a);

//Log("Adding attachment %d", i);
		bBase64 = !strcasestr(szContentType, "/xml");		// We'll base64 it if it's not XML for now
		if (i > 1) bBase64 = 1;							// Base64 attachments 2 onwards anyway

		if (bBase64) {
			// NB.	szBase64 is on the heap but mime_AddBodyPart only takes a pointer to it as we can't
			//		free it here but we should later on.  Should probably add a mime_XXX call to mark
			//		the content as heap based so it gets freed when the MIME* is.
			szBase64 = mime_Base64Enc(msg_GetAttachmentLength(a), msg_GetAttachmentText(a), 64, "\n");
			part=mime_AddBodyPart(m, msg_GetAttachmentContentType(a), -1, szBase64);
			mime_AddHeader(part, "Content-Transfer-Encoding", "base64");
		} else {
			part=mime_AddBodyPart(m, msg_GetAttachmentContentType(a), -1, msg_GetAttachmentText(a));
			mime_AddHeader(part, "Content-Transfer-Encoding", "8bit");
		}

		if (szAttachmentId) {
			// Historically, we UriEncode()d the Content-ID but we no longer do that
			const char *szBracketedId=hprintf(NULL, "<%s>", szAttachmentId);

			mime_AddHeader(part, "Content-ID", szBracketedId);
			mime_SetParameter(part, "Content-Type", "charset", "UTF-8");	// Specify the character set
//			mime_SetParameter(part, "Content-Type", "name", szAttachmentName);
			mime_AddHeader(part, "Content-Disposition", "attachment");
			mime_SetParameter(part, "Content-Disposition", "filename", szAttachmentName);

			szDelete(szBracketedId);

			Log("Added attachment %s ('%s')", szAttachmentId, szAttachmentName);
		} else {
			Log("Added un-named attachment");
		}
	}

//Log("Getting addressing data");
	if (GetAddressingData(msg, NULL, NULL, &szHost, NULL, &szURI)) {
		szURI=strdup("/URI-NOT-KNOWN");
		szHost=NULL;
	}
	if (szHost) mime_AddHeader(m, "Host", szHost);
//Log("We're sending to %s (%s)", szHost, szURI);

	szBody = mime_RenderHeap(m);
//Log("Rendered the message");
	// This is horrible, but effective.
	// We need to ascertain the 'content length' field for the header and the only sure way to
	// do this without a lot of messing about is to render it, get a pointer to just beyond the
	// header then do a strlen from there.
	// THIS WILL NOT WORK IF WE SEND BINARY...!!!!!!!!!
	// TODO: Fix for the occasion that we send binary
	if (1) {
		const char *chp=strstr(szBody, "\r\n\r\n");

		if (chp) {
			char buf[20];

			sprintf(buf, "%u", (unsigned int)strlen(chp+4));
			mime_AddHeader(m, "Content-Length", buf);
			szDelete(szBody);
			szBody = mime_RenderHeap(m);
		}
	}

	if (nCode) {
		szResult=hprintf(NULL, "HTTP/1.1 %03d %s", nCode, szCode);
	} else {
		szResult=hprintf(NULL, "POST %s HTTP/1.1", szURI);
	}
	Log("Sending with %s", szResult);
	szResult=strappend(szResult, "\r\n");
	szResult=strappend(szResult, szBody);

	szDelete(szBody);

	return szResult;
}

rogxml *WrapSoap(rogxml *rxHead, rogxml *rxBody)
// Encases the head and body in a simple SOAP wrapper
{
	rogxml *rxSOAP;
	rogxml *rx;

	rxSOAP = rogxml_NewElement(NULL, "soap:Envelope");
	rogxml_AddNamespace(rxSOAP, "soap", "http://schemas.xmlsoap.org/soap/envelope/");
	rx=rogxml_AddChild(rxSOAP, "soap:Header");
	if (rxHead) rogxml_LinkChild(rx, rxHead);
	rx=rogxml_AddChild(rxSOAP, "soap:Body");
	if (rxBody) rogxml_LinkChild(rx, rxBody);

	return rxSOAP;
}

// Darn fool mime library can only read from a function or a file so here we have a function that
// simply reads the text a character at a time.

const char *_TextReaderText;

void SetTextReader(const char *szText)
{
	_TextReaderText = szText;
}

int textReader()
{
	return (*_TextReaderText) ? *_TextReaderText++ : -1;
}

SSMAP *NewHeaderMap()
// Returns an empty header map ready for headers to be added.
{
	SSMAP *header = ssmap_New();
	ssmap_IgnoreCase(header, 1);

	return header;
}

void SetIncomingHeader(SSMAP *header)
{
	if (_incomingHeader)
		ssmap_Delete(_incomingHeader);
	_incomingHeader = header;
}

void SetIncomingHeaderFromMime(MIME *mime)
{
	if (!mime) return;

	SSMAP *headers = NewHeaderMap();

	const char *szzHeaders = mime_GetHeaderList(mime);

	const char *name;
	for (name = szzHeaders; *name; name = szz_Next(name)) {

		const char *value = mime_GetFullHeaderValue(mime, name);
		if (value) {
//Log("Setting header %s: %s", name, value);
			ssmap_Add(headers, name, value);
			szDelete(value);
		}
	}

	SetIncomingHeader(headers);
}

const char *InputAcceptEncoding()
// Return the content enoding preferred for replying to our caller
{
	static const char *result = NULL;
	szDelete(result);
	result = NULL;

	if (_incomingHeader) {
		const char *acceptEncoding = ssmap_GetValue(_incomingHeader, "Accept-Encoding");

		if (acceptEncoding) {
			char *buf = strdup(acceptEncoding);
			char *chp;

			for (chp = strtok(buf, ","); chp; chp = strtok(NULL, ",")) {
				chp = SkipSpaces(chp);
				char *semicolon = strchr(chp, ';');

				if (semicolon) {					// Following the ; can be 'q values' (e.g. 'q=0.5' etc.)
					*semicolon = '\0';
				}
				char *end = chp+strlen(chp);
				while (isspace(end[-1])) end--;		// trim spaces from the end
				*end = '\0';
				strlwr(chp);						// Finally lower case version of simple string

				if (!strcmp(chp, "gzip")) {
					szDelete(result);
					result = strdup(chp);
				}
			}

			free(buf);
		}
	}

	if (!result) result = strdup("identity");		// The default encoding

	return result;
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
		case 422: s="Unprocessable Entity"; break;
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

const char *HttpHeader(int code)
// Returns a header of the form 'HTTP/1.1 200 OK' that should be considered static (i.e. don't free it)
{
	const char *result = NULL;
	szDelete(result);

	result = hprintf(NULL, "HTTP/1.1 %d %s\r\n", code, HttpResponseText(code));

	return result;
}

void SendHttpHeader(BIO *io, int code, const char *szContent)
{
	if (!szContent) szContent="text/html";

	// Send first line
	BIO_puts(io, HttpHeader(code));

	// sends header lines
	MyBIO_puts(io,"Server: Microtest Message Transport Server\r\n");
	BIO_putf(io, "Content-type: %s\r\n", szContent);
	MyBIO_puts(io, "\r\n");
}

//int deflateInit2 (z_streamp strm, int  level, int  method, int  windowBits, int  memLevel, int  strategy);

static const char *NiceBuf(int len, const char *buf)
// Returns a string (treat it as static) that has a printable form of the buffer passed (-1 means strlen)
{
	static char *result = NULL;

	if (result) free(result);
	if (len < 0) len = strlen(buf);

	HBUF *hbuf = hbuf_New();

	char *dest = result;
	while (len > 0) {
		if (*buf >= ' ' && *buf <= '~') {
			hbuf_AddChar(hbuf, *buf);
		} else {
			char tmp[10];
			snprintf(tmp, sizeof(tmp), "<%02x>", (unsigned char)*buf);
			hbuf_AddBuffer(hbuf, -1, tmp);
		}
		buf++;
		len--;
	}
	hbuf_AddChar(hbuf, '\0');
	result = (char*)hbuf_ReleaseBuffer(hbuf);
	hbuf_Delete(hbuf);

	return result;
}

void SendHttp(BIO *io, int code, SSMAP *header, int len, const char *content)
// NB. header should have been created with NewHeaderMap() so that it has the 'ignore case' setting.
//
// Call with len = -1 to ascertain the content length from strlen(content)
// Call with len = -2 to not send a content-length (or content) at all.
{
	if (!io) return;

	if (!content) content = "";
	if (len == -1) len = strlen(content);

	const char *firstLine = HttpHeader(code);
//Log("SEND: %s", firstLine);
	BIO_puts(io, firstLine);

	char *delContent = NULL;			// Will point to temporary content if we need to delete it
	SSMAP *delHeader = NULL;			// Will point to our created header if we need to delete it

	if (!header) {
		header = NewHeaderMap();
		delHeader = header;
	}

	if (len > 0 && !strcmp(InputAcceptEncoding(), "gzip")) {					// Compressing
		z_stream strm;
		strm.zalloc = Z_NULL;
	    strm.zfree = Z_NULL;
	    strm.opaque = Z_NULL;

		char outbuf[10240];
		char *output = NULL;
		int newLen = 0;


		deflateInit2(&strm, Z_DEFAULT_COMPRESSION, Z_DEFLATED, 16+15, 9, Z_DEFAULT_STRATEGY);
		strm.avail_in = len;
		strm.next_in = (Bytef*)content;
		strm.next_out = (Bytef*)outbuf;
		strm.avail_out = sizeof(outbuf);

		do {
			int ret = deflate(&strm, 1);
			int oldLen = newLen;

			newLen += sizeof(outbuf)-strm.avail_out;
			if (output) {
				output = realloc(output, newLen);
			} else {
				output = malloc(newLen);
			}
			memcpy(output+oldLen, outbuf, newLen-oldLen);
		} while (strm.avail_out == 0);

		len = newLen;
		content = output;
		delContent = (char*)content;

		ssmap_Add(header, "Content-Encoding","gzip");
	}

	if (len == -2) {
		ssmap_DeleteKey(header, "Content-Length");
	} else {
		char buf[20];
		snprintf(buf, sizeof(buf), "%d", len);
		ssmap_Add(header, "Content-Length", buf);
	}

	// Ensure that our 'Date' header is both present and correct
	time_t now = time(NULL);
	struct tm *tm=gmtime(&now);
	char datebuf[40];
	snprintf(datebuf, sizeof(datebuf), "%.3s, %02d %.3s %04d %02d:%02d:%02d +0000",
			"SunMonTueWedThuFriSat"+tm->tm_wday*3,
			tm->tm_mday, "JanFebMarAprMayJunJulAugSepOctNovDec"+tm->tm_mon*3, tm->tm_year+1900,
			tm->tm_hour, tm->tm_min, tm->tm_sec);
	ssmap_Add(header, "Date", datebuf);

	ssmap_Reset(header);
	const char *name;
	const char *value;
	while (ssmap_GetNextEntry(header, &name, &value)) {
		BIO_putf(io, "%s: %s\r\n", name, value);
Log("SEND: %s: %s", name, value);
	}

Log("SEND: Blank line");
	BIO_puts(io, "\r\n");

	if (len >= 0)
		BIO_write(io, content, len);
Log("SEND: (%d) %s", len, NiceBuf(len, content));

	if (delHeader)
		ssmap_Delete(header);

	if (delContent)
		free(delContent);
}

void bprint(HBUF *hbuf, const char *text)
{
	if (text) {
		hbuf_AddBuffer(hbuf, -1, text);
	}
}

void bprintf(HBUF *hbuf, const char *fmt, ...)
{
	va_list ap;
	char buf[10000];

	va_start(ap, fmt);
	vsnprintf(buf, sizeof(buf), fmt, ap);
	va_end(ap);

	bprint(hbuf, buf);
}

void SendHttpBuffer(BIO *io, int code, SSMAP *header, HBUF *hbuf)
// Sends an HTTP message that is held in an HHUF*
// This function takes ownership of the buffer so don't go freeing it afterwards
{
	if (!hbuf) return;

	int len = hbuf_GetLength(hbuf);
	hbuf_AddChar(hbuf, 0);
	const char *content = hbuf_ReleaseBuffer(hbuf);
	hbuf_Delete(hbuf);

	int delHeader = 0;
	if (!header) {
		header = NewHeaderMap();
		delHeader = 1;
	}
	ssmap_Add(header, "Server", "Microtest Message Transport Server");
	ssmap_Add(header, "Content-type", "text/html");

	SendHttp(io, code, header, len, content);

	if (delHeader) ssmap_Delete(header);
}

rogxml *WrapSoap00(rogxml *rxHead, rogxml *rxBody)
// Encases the head and body in a simple SOAP wrapper with NO namespace
{
	rogxml *rxSOAP;
	rogxml *rx;

	rxSOAP = rogxml_NewElement(NULL, "soap:Envelope");
	rx=rogxml_AddChild(rxSOAP, "soap:Header");
	if (rxHead) rogxml_LinkChild(rx, rxHead);
	rx=rogxml_AddChild(rxSOAP, "soap:Body");
	if (rxBody) rogxml_LinkChild(rx, rxBody);

	return rxSOAP;
}

rogxml *WrapSoap11(rogxml *rxHead, rogxml *rxBody)
// Encases the head and body in a simple SOAP wrapper with a 1.1 namespace
{
	rogxml *rxSOAP = WrapSoap00(rxHead, rxBody);

	rogxml_AddNamespace(rxSOAP, "soap", "http://schemas.xmlsoap.org/soap/envelope/");

	return rxSOAP;
}

rogxml *WrapSoap12(rogxml *rxHead, rogxml *rxBody)
// Encases the head and body in a simple SOAP wrapper with a 1.2 namespace
{
	rogxml *rxSOAP = WrapSoap00(rxHead, rxBody);

	rogxml_AddNamespace(rxSOAP, "soap", "http://www.w3.org/2003/05/soap-envelope");

	return rxSOAP;
}

rogxml *Soap12FaultBody(const char *szCode, const char *szSubcode, const char *szReason, const char *szDetail, rogxml *rxDetail)
// Returns the SOAP body for a SOAP 1.2 fault
// Note the 'detail' element that is there only because ITK seems to use the wrong name (should be Detail)
{
	rogxml *rxSoap = rogxml_NewElement(NULL, "soap:Fault");
	rogxml *rxCode = rogxml_AddChild(rxSoap, "soap:Code");
	rogxml_AddTextChild(rxCode, "soap:Value",szCode);
	if (szSubcode) {
		rogxml *rxSubcode = rogxml_AddChild(rxCode, "soap:Subcode");
		rogxml_AddTextChild(rxSubcode, "soap:Value",szSubcode);
	}
	rogxml *rxReason = rogxml_AddChild(rxSoap, "soap:Reason");
	rogxml *rxText = rogxml_AddTextChild(rxReason, "soap:Text", szReason);
	rogxml_SetAttr(rxText, "xml:lang", "en");
	if (szDetail) rogxml_AddTextChild(rxSoap, "soap:Detail", szDetail);
	if (rxDetail) {
		rogxml *rx = rogxml_AddChild(rxSoap, "soap:detail");
		rogxml_LinkChild(rx, rxDetail);
	}

	return rxSoap;
}

rogxml *Soap11FaultBody(const char *szCode, const char *szSubcode, const char *szReason, const char *szDetail, rogxml *rxDetail)
// Returns the SOAP body for a SOAP 1.1 fault
// Note that 1.1 doesn't use szCode, szDetail or rxDetail
{
	rogxml *rxSoap = rogxml_NewElement(NULL, "soap:Fault");
	rogxml_AddTextChild(rxSoap, "faultcode", szSubcode);
	rogxml_AddTextChild(rxSoap, "faultstring", szReason);
	if (rxDetail) {
		rogxml *rx = rogxml_AddChild(rxSoap, "detail");
		rogxml_LinkChild(rx, rxDetail);
	}

	return rxSoap;
}

int MakeHttpFromSoapText(const char **pResponse, const char **pMime, const char *szText, const char *szAction, const char *szTo, const char *szFrom)
// pResponse is either NULL or pointing to a string to receive the heap-based response string
// pMime must point to a string to receive the MIME or error message
// The passed text will probably come from a handler script and will look like:
// HTTP...			Everything from the first line so we'll just return it
// <soap:Envelope>	An entire SOAP envelope so we'll add a nice MIME header and return it all
// <Something>		Some other XML, we'll wrap in SOAP then treat as above
// other: something	MIME header so we'll add a 200 OK to the top and treat as the first option
// szAction, szTo, szFrom are only used in the <something> case
// Returns			0 - pResponse will have, e.g. "HTTP/1.1 200 OK\r\n" and pMime will have all the rest
//					1...	Error code and pMime will point to text (heap-based)
{
	const char *szResponse = NULL;					// Response line passed into *pResponse
	int nHttpCode = 200;							// Default HTTP code

	if (!pMime) return 1;
	if (!szText) { *pMime = strdup("No text passed"); return 2; }

	if (!szAction) szAction = "Acknowledgment";

	MIME *mime = NULL;
	rogxml *rxSoap = NULL;

	if (*szText == '<') {
		rxSoap = rogxml_FromText(szText);
		if (!rxSoap) {							// Error - 
			*pMime = strdup("Text starts '<' but isn't XML");
			return 3;
		}
		const char *szAttrHttpCode = rogxml_GetAttr(rxSoap, "httpcode");
		const char *szAttrHttpText = rogxml_GetAttr(rxSoap, "httptext");

		if (szAttrHttpCode) nHttpCode = atoi(szAttrHttpCode);

		rogxml_SetAttr(rxSoap, "httpcode", NULL);
		rogxml_SetAttr(rxSoap, "httptext", NULL);

		if (strcmp(rogxml_GetLocalName(rxSoap), "Envelope")) {		// Doesn't look like a SOAP envelope
			rogxml *rxBody = rxSoap;

			if (!strcmp(rogxml_GetLocalName(rxBody), "error")) {	// An error, this needs to be a SOAP fault
//				szAction = "http://schemas.xmlsoap.org/ws/2004/08/addressing/fault";
				const char *szErrorAction = rogxml_GetAttr(rxBody, "action");
				szAction = "http://www.w3.org/2005/08/addressing/fault";
				if (szErrorAction) szAction = szErrorAction;

				rogxml *rxError = rxBody;
				const char *szSubcode = NULL;
				const char *szReason = NULL;
//				szSubcode = rogxml_GetAttr(rxError, "code");
//				szReason = rogxml_GetAttr(rxError, "message");
				szSubcode="soap:Client";
				szReason="A client related error has occurred, see detail element for further information";
				if (!szReason) szReason = rogxml_GetAttr(rxError, "display");
				rogxml *rxDetail = rogxml_FindFirstChild(rxError);
				rxBody = Soap11FaultBody(NULL, szSubcode, szReason, szReason, rxDetail);
				nHttpCode = 500;
			}

			rxSoap = WrapSoap(NULL, rxBody);

			// If the return from the API is <result>...</result> then we need just the children of that
			if (!strcmp(rogxml_GetLocalName(rxBody), "result")) {
				rogxml_Remove(rxBody);
			}

			rogxml *rxHeader = rogxml_FindFirstChild(rxSoap);
			rogxml_AddNamespace(rxHeader, "wsa","http://www.w3.org/2005/08/addressing");
			rogxml_AddTextChild(rxHeader, "wsa:MessageID", guid_ToText(NULL));
			rogxml_AddTextChild(rxHeader, "wsa:Action", szAction);
			rogxml_AddTextChild(rxHeader, "wsa:To", szTo);
			rogxml *rxFrom = rogxml_AddChild(rxHeader, "wsa:From");
			rogxml_AddTextChild(rxFrom, "wsa:Address", szFrom);
		}
		mime = mime_New(XML_MIME_TYPE);

		rogxml *rxPI = rogxml_FindProlog(rxSoap);		// sets the version as a side-effect
		rogxml_SetAttr(rxPI, "encoding", "utf-8");

		rogxml_SetIndentString(szXmlIndent);
		rogxml_SetLinefeedString(szXmlLinefeed);
		mime_AddBodyPart(mime, XML_MIME_TYPE, -1, rogxml_ToText(rxSoap));
	} else {
		if (!strncmp(szText, "HTTP", 4)) {
			const char *chp=strchr(szText, '\n');

			if (chp) {
				szResponse = strnappend(NULL, szText, chp-szText);
				szText = chp+1;
			}
		}
		SetTextReader(szText);
		mime = mime_ReadFn(textReader);
	}

	if (!mime) {
		*pMime = hprintf(NULL, "MIME Error %d - %s", mime_GetLastError(), mime_GetLastErrorStr());
		return 4;
	}

	if (pResponse) {
		*pResponse = szResponse ? szResponse
							   : strdup(HttpHeader(nHttpCode));
	}

	const char *szBody = mime_RenderHeap(mime);
	const char *chp=strstr(szBody, "\r\n\r\n");

	if (chp) {
		char buf[20];

		sprintf(buf, "%u", (unsigned int)strlen(chp+4));
		mime_SetHeader(mime, "Content-Length", buf);
		szDelete(szBody);
		szBody = mime_RenderHeap(mime);
	}

	*pMime = szBody;

	mime_Delete(mime);

	return 0;
}

rogxml *SoapFault(int version, const char *szCode, const char *szSubcode, const char *szReason, const char *szDetail, rogxml *rxDetail, const char *szAction, const char *szTo, const char *szFrom, const char *szElement)
{
	rogxml *rxSoap;
	rogxml *rxBody = NULL;

	if (version == 1) {
		rxBody = Soap11FaultBody(szCode, szSubcode, szReason, szDetail, rxDetail);
	} else {
		rxBody = Soap12FaultBody(szCode, szSubcode, szReason, szDetail, rxDetail);
	}

	rxSoap = WrapSoap(NULL, rxBody);
	rogxml *rxHeader = rogxml_FindFirstChild(rxSoap);
	rogxml_AddNamespace(rxHeader, "wsa","http://www.w3.org/2005/08/addressing");
	if (strstr(szSubcode, "wsse:"))
		rogxml_AddNamespace(rxHeader, "wsse","http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd");
	rogxml_AddTextChild(rxHeader, "wsa:MessageID", guid_ToText(NULL));
	if (!szAction) szAction = "http://www.w3.org/2005/08/addressing/fault";
	if (szAction && *szAction) rogxml_AddTextChild(rxHeader, "wsa:Action", szAction);
	if (szElement) {
		rogxml *rx = rogxml_AddChild(rxHeader, "wsa:FaultDetail");
		rogxml_AddTextChild(rx, "wsa:ProblemHeaderQName", szElement);
	}
	if (szTo && *szTo) rogxml_AddTextChild(rxHeader, "wsa:To", szTo);
	if (szFrom && *szFrom) {
		rogxml *rxFrom = rogxml_AddChild(rxHeader, "wsa:From");
		rogxml_AddTextChild(rxFrom, "wsa:Address", szFrom);
	}

	return rxSoap;
}

const char *HttpSoapFault(int nHttpCode, int version, const char *szCode, const char *szSubcode, const char *szReason, const char *szDetail, rogxml *rxDetail, const char *szAction, const char *szTo, const char *szFrom, const char *szElement)
// Returns a string on the heap comprising the entire message including HTTP header
{
	const char *szResult = NULL;
	MIME *mime = mime_New(XML_MIME_TYPE);

	rogxml *rxSoap = SoapFault(version, szCode, szSubcode, szReason, szDetail, rxDetail, szAction, szTo, szFrom, szElement);
	const char *szBody = rogxml_ToText(rxSoap);
	char szContentLength[20];

	snprintf(szContentLength, sizeof(szContentLength), "%u", (unsigned int)strlen(szBody));
	mime_SetHeader(mime, "Content-Length", szContentLength);

	mime_AddBodyPart(mime, XML_MIME_TYPE, -1, szBody);
	if (nHttpCode) {
		const char *szHead = strdup(HttpHeader(nHttpCode));
		const char *szBody = mime_RenderHeap(mime);

		szResult = strappend(szHead, szBody);
		szDelete(szBody);
	} else {
		szResult = mime_RenderHeap(mime);
	}

	return szResult;
}

int AddebXML(MSG *msg)
// Puts a soapy ebXML wrapper in place of the HL7 message and places the HL7 as the
// first attachment.
// If this is an acknowledgement, szInReferenceTo should be non-NULL and hold the message id being replied to
// Both may be set...
{
	rogxml *rx;
	rogxml *rxMsgData;
	rogxml *rxebHead;
	rogxml *rxebMan;
	rogxml *rxHL7;
	rogxml *rxSoap;
	contract_t *c;
	const char *szRef;
	msg_attachment *a;
	int nAttach = msg_GetAttachmentCount(msg);
	int i;
	int nLevel = msg_GetLevel(msg);

	const char *szInReferenceTo;

	rxebHead = rogxml_NewElement(NULL, "eb:MessageHeader");
	rogxml_AddAttr(rxebHead, "eb:version", "2.0");
	rogxml_AddAttr(rxebHead, "soap:mustUnderstand", "1");
	rxHL7 = msg_GetXML(msg);
	c=msg_GetContract(msg);

	szInReferenceTo = WrapperGetAcknowledgingId(rxHL7);

	rx=rogxml_AddChild(rxebHead, "eb:From");
	rx=rogxml_AddChild(rx, "eb:PartyId");
	rogxml_AddAttr(rx, "eb:type", "urn:nhs:names:partyType:ocs+serviceInstance");
//	rogxml_AddText(rx, WrapperGetCommsAddr(rxHL7, "Snd"));
	rogxml_AddText(rx, msg_GetFromPartyId(msg));

	rx=rogxml_AddChild(rxebHead, "eb:To");
	rx=rogxml_AddChild(rx, "eb:PartyId");
	rogxml_AddAttr(rx, "eb:type", "urn:nhs:names:partyType:ocs+serviceInstance");
	rogxml_AddText(rx, msg_GetToPartyId(msg));

	rogxml_AddTextChild(rxebHead, "eb:CPAId", contract_GetId(c));
//	rogxml_AddTextChild(rxebHead, "eb:ConversationId", WrapperGetMessageId(rxHL7));
	rogxml_AddTextChild(rxebHead, "eb:ConversationId", msg_GetConversationId(msg, WrapperGetMessageId(rxHL7)));
	rogxml_AddTextChild(rxebHead, "eb:Service", contract_GetService(c));

	rogxml_AddTextChild(rxebHead, "eb:Action", WrapperGetInteractionId(rxHL7));

	rxMsgData = rogxml_AddChild(rxebHead, "eb:MessageData");
//	rogxml_AddTextChild(rxMsgData, "eb:MessageId", WrapperGetMessageId(rxHL7));
	rogxml_AddTextChild(rxMsgData, "eb:MessageId", msg_GetMessageId(msg));
	rogxml_AddTextChild(rxMsgData, "eb:Timestamp", TimeStamp(0));
	if (szInReferenceTo) rogxml_AddTextChild(rxMsgData, "eb:RefToMessageId", szInReferenceTo);
	if (!strcasecmp(contract_GetDuplicateElimination(c), "always")) {
		rogxml_AddTextChild(rxebHead, "eb:DuplicateElimination", "always");
	}
//	rogxml_AddTextChild(rxMsgData, "eb:TimeToLive", "0");		// Optional in P1R1?

	rxebMan = rogxml_NewElement(NULL, "eb:Manifest");
	rogxml_AddAttr(rxebMan, "eb:version", "2.0");				// May come from message ID suffix

	for (i=0;i<=nAttach;i++) {
		if (i == 0) {											// HL7 part (not an attachment yet - added below)
			const char *szDescription=msg_GetDescription(msg);

			szRef=msg_GetContentId(msg, NULL);
			rogxml *rxRef=rogxml_AddChild(rxebMan, "eb:Reference");
			rogxml_SetAttrf(rxRef, "xlink:href", "cid:%s", szRef);
			const char *szReferenceId = msg_GetReferenceId(msg);

			if (szReferenceId) rogxml_SetAttr(rxRef, "eb:id", szReferenceId);
			rogxml_AddChildAttr(rxRef, "eb:Schema", "eb:location", "http://www.nhsia.nhs.uk/schemas/HL7-Message.xsd");
			if (szDescription) {
				rogxml *rx = rogxml_AddTextChild(rxRef, "eb:Description", szDescription);
				rogxml_SetAttr(rx, "xml:lang", "en-GB");
			}
			rx=rogxml_AddChild(rxRef, "hl7eb:Payload");			// SC's mod to capitalise 'P' in Payload
			rogxml_SetAttr(rx, "style", "HL7");
			rogxml_SetAttr(rx, "encoding", "XML");
			rogxml_SetAttr(rx, "version", "3.0");				// SC's mode to remove 'eb:' namespace from version
		} else {
			msg_attachment *a = msg_GetAttachment(msg, i);
			const char *szName = msg_GetAttachmentName(a);
			const char *szCID = msg_GetAttachmentContentId(a);
			const char *szDescription = msg_GetAttachmentDescription(a);

			rogxml *rxRef=rogxml_AddChild(rxebMan, "eb:Reference");
			const char *szReferenceId = msg_GetAttachmentReferenceId(a);
			if (szReferenceId) rogxml_SetAttr(rxRef, "eb:id", szReferenceId);
			rogxml_SetAttrf(rxRef, "xlink:href", "cid:%s", szCID);
			rogxml_SetAttr(rxRef, "xml:lang", "en-GB");
			if (szDescription || szName) {
				rogxml *rx = rogxml_AddTextChild(rxRef, "eb:Description", szDescription ? szDescription : szName);
				rogxml_SetAttr(rx, "xml:lang", "en-GB");
			}
		}
	}

	rxSoap = WrapSoap(rxebHead, rxebMan);
	rogxml_AddNamespace(rxSoap, "hl7eb", "urn:hl7-org:transport/ebxml/DSTUv1.0");
	rogxml_AddNamespace(rxSoap, "xlink", "http://www.w3.org/1999/xlink");
	rogxml_AddNamespace(rxSoap, "eb", "http://www.oasis-open.org/committees/ebxml-msg/schema/msg-header-2_0.xsd");

	if (strcasecmp(contract_GetSyncReplyMode(c), "None")) {			// Anything but 'None'
		rx=rogxml_AddSibling(rxebHead, "eb:SyncReply");
		rogxml_SetAttr(rx, "eb:version", "2.0");
		rogxml_SetAttr(rx, "soap:mustUnderstand", "1");
		rogxml_SetAttr(rx, "soap:actor", "http://schemas.xmlsoap.org/soap/actor/next");
	}

//	if (bAckRequested) {						// Needs slightly kludgey, post-soapy way to link in ack request
	if (strcasecmp(contract_GetAckRequested(c), "Never")) {			// Anything but 'Never'
		rogxml *rx=rogxml_NewElement(NULL, "eb:AckRequested");
		rogxml_AddAttr(rx, "eb:signed", "false");
		rogxml_AddAttr(rx, "eb:version", "2.0");
		const char *szActor = contract_GetActor(c);
		if (!szActor) szActor="urn:oasis:names:tc:ebxml-msg:actor:toPartyMSH";
		rogxml_AddAttr(rx, "soap:actor", szActor);
		rogxml_AddAttr(rx, "soap:mustUnderstand", "1");

		rogxml_LinkSibling(rxebHead, rx);
	}

//	Log("Message is level %d (Looking for %d)", nLevel, LEVEL_P1R2);
	if (nLevel == LEVEL_P1R2) {					// Remove any extraneous IDs that are not ASIDs
		RemovePartyIdsAt(rxHL7, "/*/communicationFunctionRcv/device/id");
		RemovePartyIdsAt(rxHL7, "/*/communicationFunctionSnd/device/id");
	}

	rogxml_SetIndentString(szXmlIndent);
	rogxml_SetLinefeedString(szXmlLinefeed);

	a=msg_NewAttachment("Message", XML_MIME_TYPE, szRef, -1, rogxml_ToText(rxHL7));			/// was application/
	msg_InsertAttachment(msg, a);

	msg_SetXML(msg, rxSoap);					// NB. Deletes the existing XML
	msg_SetWrapped(msg, WRAP_EBXML);
	msg_SetHasPayload(msg, 1);

	return 1;
}

rogxml *MakeItkAcknowledgement(const char *szTo, const char *szId)
{
	if (!szId) {
		szId = guid_ToText(NULL);
	}

	rogxml *rx = rogxml_NewElement(NULL, "s:Envelope");
	rogxml_AddNamespace(rx, "s", "http://schemas.xmlsoap.org/soap/envelope/");
	rogxml_AddNamespace(rx, "addr", "http://schemas.xmlsoap.org/ws/2004/08/addressing");
	rogxml_AddNamespace(rx, "ack", "http://www.openuri.org/2003/02/soap/acknowledgement/");

	rogxml *rxHeader = rogxml_AddChild(rx, "s:Header");
	rogxml_AddChild(rx, "s:Body");

	rogxml_AddTextChildf(rxHeader, "addr:MessageID", szId);
	rogxml_AddTextChild(rxHeader, "addr:To", szTo);
	rogxml *rxFrom=rogxml_AddChild(rxHeader, "addr:From");
	rogxml_AddTextChild(rxFrom, "addr:Address", szMyUrl);

	return rx;
}

rogxml *MakeAcknowledgement(MSG *msg)
// Generates the acknowledgement message to match the message given.
{
	rogxml *rx;
	rogxml *rxMsgData;
	rogxml *rxebHead;
	rogxml *rxSoap;
	rogxml *rxHL7;
	contract_t *contract;			// Contract of messaging being replied to

	const char *szWrapperId = msg_Attr(msg, "ebxml-MessageData-MessageId");		// 30320 - Most reliable (only?) way of finding it
	rxebHead = rogxml_NewElement(NULL, "eb:MessageHeader");
	rogxml_AddAttr(rxebHead, "eb:version", "2.0");
	rogxml_AddAttr(rxebHead, "soap:mustUnderstand", "1");
	rxHL7 = msg_GetXML(msg);
	contract = msg_GetContract(msg);					// Use CPAID of original message

	rx=rogxml_AddChild(rxebHead, "eb:From");
	rx=rogxml_AddChild(rx, "eb:PartyId");
	rogxml_AddAttr(rx, "eb:type", "urn:nhs:names:partyType:ocs+serviceInstance");
	rogxml_AddText(rx, msg_GetToPartyId(msg));
//	rogxml_AddText(rx, WrapperGetCommsAddr(rxHL7, "Rcv"));

	rx=rogxml_AddChild(rxebHead, "eb:To");
	rx=rogxml_AddChild(rx, "eb:PartyId");
	rogxml_AddAttr(rx, "eb:type", "urn:nhs:names:partyType:ocs+serviceInstance");
	rogxml_AddText(rx, msg_GetFromPartyId(msg));
//	rogxml_AddText(rx, WrapperGetCommsAddr(rxHL7, "Snd"));

	rogxml_AddTextChild(rxebHead, "eb:CPAId", contract_GetId(contract));
	rogxml_AddTextChild(rxebHead, "eb:ConversationId", msg_GetConversationId(msg, NULL));
	rogxml_AddTextChild(rxebHead, "eb:Service", "urn:oasis:names:tc:ebxml-msg:service");

	rogxml_AddTextChild(rxebHead, "eb:Action", "Acknowledgment");

	rxMsgData = rogxml_AddChild(rxebHead, "eb:MessageData");
	rogxml_AddTextChild(rxMsgData, "eb:MessageId", guid_ToText(NULL));
	rogxml_AddTextChild(rxMsgData, "eb:Timestamp", TimeStamp(0));
	rogxml_AddTextChild(rxMsgData, "eb:RefToMessageId", szWrapperId);		// 30320 Changed from msg_GetMessageId()
//	rogxml_AddTextChild(rxMsgData, "eb:TimeToLive", "0");		// Optional in P1R1?

	rogxml *rxAck = rogxml_NewElement(NULL, "eb:Acknowledgment");
	rogxml_AddAttr(rxAck, "eb:version", "2.0");				// May come from message ID suffix
	rogxml_AddAttr(rxAck, "soap:mustUnderstand", "1");
	const char *szActor = contract_GetActor(contract);
	if (!szActor) szActor="urn:oasis:names:tc:ebxml-msg:actor:toPartyMSH";
	rogxml_AddAttr(rxAck, "soap:actor", szActor);
	rogxml_AddTextChild(rxAck, "eb:Timestamp", TimeStamp(0));
	rogxml_AddTextChild(rxAck, "eb:RefToMessageId", szWrapperId);		// 30320 Changed from WrapperGetMessageId()

	const char *szFrom = WrapperGetCommsAddr(rxHL7, "Rcv");	// Only add 'from' if it's in the message
	if (szFrom) {
		rx=rogxml_AddChild(rxAck, "eb:From");
		rx=rogxml_AddChild(rx, "eb:PartyId");
		rogxml_AddAttr(rx, "type", "urn:nhs:names:partyType:ocs+serviceInstance");
		rogxml_AddText(rx, szFrom);
	}

	rxSoap = WrapSoap(rxebHead, NULL);						// Place in SOAP
	rogxml_LinkSibling(rxebHead, rxAck);					// Sneak in the acknowledgement element as a brother

	rogxml_AddNamespace(rxSoap, "eb", "http://www.oasis-open.org/committees/ebxml-msg/schema/msg-header-2_0.xsd");

	return rxSoap;
}

rogxml *MakeNegAcknowledgement(MSG *msg, const char *szCode, int nErr, const char *szErr)
{
	rogxml *rxFault = rogxml_NewElement(NULL, "soap:Fault");

	if (!szCode) szCode = "soap:ServiceError/Misc";
	rogxml_AddTextChild(rxFault, "faultcode", szCode);
	rogxml_AddTextChildf(rxFault, "faultstring", "%d: %s", nErr, szErr);
	if (msg) {
		const char *szMessageId = msg_GetMessageId(msg);
		const char *szInteractionId = msg_GetInteractionId(msg);
		rogxml *rxDetail;

		rxDetail = rogxml_AddChild(rxFault, "detail");
		rogxml_AddTextChild(rxDetail, "messageId", szMessageId);
		rogxml_AddTextChild(rxDetail, "InteractionId", szInteractionId);
	}

	return WrapSoap(NULL, rxFault);						// Place in SOAP
}

rogxml *MakeDuplicateAcknowledgement(MSG *msg)
{
	return MakeAcknowledgement(msg);
}

rogxml *MakePong(MSG *msg)
{
	rogxml *rx;
	rogxml *rxMsgData;
	rogxml *rxebHead;
	rogxml *rxSoap;
	rogxml *rxHL7;
	contract_t *contract;			// Contract of messaging being replied to

	rxebHead = rogxml_NewElement(NULL, "eb:MessageHeader");
	rogxml_AddAttr(rxebHead, "eb:version", "2.0");
	rogxml_AddAttr(rxebHead, "soap:mustUnderstand", "1");
	rxHL7 = msg_GetXML(msg);
	contract = msg_GetContract(msg);					// Use CPAID of original message

	rx=rogxml_AddChild(rxebHead, "eb:From");
	rx=rogxml_AddChild(rx, "eb:PartyId");
	rogxml_AddAttr(rx, "eb:type", "urn:nhs:names:partyType:ocs+serviceInstance");
	rogxml_AddText(rx, WrapperGetCommsAddr(rxHL7, "Rcv"));

	rx=rogxml_AddChild(rxebHead, "eb:To");
	rx=rogxml_AddChild(rx, "eb:PartyId");
	rogxml_AddAttr(rx, "eb:type", "urn:nhs:names:partyType:ocs+serviceInstance");
	rogxml_AddText(rx, WrapperGetCommsAddr(rxHL7, "Snd"));

	rogxml_AddTextChild(rxebHead, "eb:CPAId", contract_GetId(contract));
	rogxml_AddTextChild(rxebHead, "eb:ConversationId", msg_GetConversationId(msg, NULL));
	rogxml_AddTextChild(rxebHead, "eb:Service", "urn:oasis:names:tc:ebxml-msg:service");

	rogxml_AddTextChild(rxebHead, "eb:Action", "Pong");

	rxMsgData = rogxml_AddChild(rxebHead, "eb:MessageData");
	rogxml_AddTextChild(rxMsgData, "eb:MessageId", guid_ToText(NULL));
	rogxml_AddTextChild(rxMsgData, "eb:RefToMessageId", msg_GetMessageId(msg));
	rogxml_AddTextChild(rxMsgData, "eb:Timestamp", TimeStamp(0));

	rxSoap = WrapSoap(rxebHead, NULL);						// Place in SOAP

	rogxml_AddNamespace(rxSoap, "eb", "http://www.oasis-open.org/committees/ebxml-msg/schema/msg-header-2_0.xsd");

	return rxSoap;
}

MSG *MakeAcknowledgementMessage(MSG *msg)
{
	rogxml *rx=MakeAcknowledgement(msg);

	MSG *msgAck = msg_New(rx);
	msg_SetIntAttr(msgAck, "positive", 1);

	return msgAck;
}

MSG *MakeItkAcknowledgementMessage(const char *szTo, const char *szId)
{
	rogxml *rx=MakeItkAcknowledgement(szTo, szId);

	MSG *msgAck = msg_New(rx);
	msg_SetIntAttr(msgAck, "positive", 1);

	return msgAck;
}

MSG *MakeNegAcknowledgementMessage(MSG *msg, const char *szCode, int nErr, const char *szErr)
{
	rogxml *rx=MakeNegAcknowledgement(msg, szCode, nErr, szErr);

	MSG *msgAck = msg_New(rx);
	msg_SetIntAttr(msgAck, "positive", 0);

	return msgAck;
}

MSG *MakeDuplicateAcknowledgementMessage(MSG *msg)
{
	rogxml *rx=MakeDuplicateAcknowledgement(msg);

	return msg_New(rx);
}

MSG *MakePongMessage(MSG *msg)
{
	rogxml *rx=MakePong(msg);

	return msg_New(rx);
}

void AddNasp1(MSG *msg)
// Wraps the HL7 in a soapy NASP wrapper and puts it in place of the existing XML in the message.
{
	rogxml *rx;
	rogxml *rxHL7;
	rogxml *rxSoap;
	rogxml *rxNasp;

	rxHL7 = msg_ReleaseXML(msg);

	rxNasp = rogxml_NewElement(NULL, "hl7:naspHeader");
	rogxml_AddChildAttr(rxNasp, "hl7:id", "root", msg_GetMessageId(msg));
	rogxml_AddChildAttr(rxNasp, "hl7:creationTime", "value", HL7TimeStamp(0));
	rogxml_AddChildAttr(rxNasp, "hl7:versionCode", "code", "2.0");
	rx=rogxml_AddChildAttr(rxNasp, "hl7:interactionId", "root","2.16.840.1.113883.2.1.3.2.4.12");
	rogxml_AddAttr(rx, "extension", msg_GetInteractionId(msg));
	rogxml_AddChildAttr(rxNasp, "hl7:processingCode", "code", "P");
	rogxml_AddChildAttr(rxNasp, "hl7:processingModeCode", "code", "T");
	rogxml_AddChildAttr(rxNasp, "hl7:acceptAckCode", "code", "NE");
	rx=rogxml_AddChild(rxNasp, "hl7:communicationFunctionRcv");
	rx=rogxml_AddChild(rx, "hl7:device");
	rx=rogxml_AddChildAttr(rx, "hl7:id", "root", "2.16.840.1.113883.2.1.3.2.4.10");
	rogxml_SetAttr(rx, "extension", msg_GetToPartyId(msg));
	rx=rogxml_AddChild(rxNasp, "hl7:communicationFunctionSnd");
	rx=rogxml_AddChild(rx, "hl7:device");
	rx=rogxml_AddChildAttr(rx, "hl7:id", "root", "2.16.840.1.113883.2.1.3.2.4.10");
	rogxml_SetAttr(rx, "extension", msg_GetFromPartyId(msg));
	rogxml_AddChild(rxNasp, "ControlActProcess");

	rxSoap = WrapSoap(rxNasp, rxHL7);
	rogxml_AddNamespace(rxSoap, "hl7", "urn:hl7-org:v3");

	msg_SetXML(msg, rxSoap);							// Existing XML was unset (see msg_ReleaseXML() above)
	msg_SetWrapped(msg, WRAP_NASP);
	msg_SetHasPayload(msg, 1);
}

void AddNasp2(MSG *msg)
// Wraps the HL7 in a soapy NASP P1R2 wrapper and puts it in place of the existing XML in the message.
// Note that P1R2 soap is a different flavour than P1R1...
{
	rogxml *rx;
	rogxml *rxHL7;
	rogxml *rxSoap;
	rogxml *rxHeader;
	rogxml *rxBody;
	contract_t *c;
	const char *szEndpoint;

	if (GetAddressingData(msg, &szEndpoint, NULL, NULL, NULL, NULL)) {
		msg_SetError(msg, GetErrorNo(), "%s", GetErrorStr());
		return;
	}
	c=msg_GetContract(msg);
	rxHL7 = msg_ReleaseXML(msg);

	rxSoap = rogxml_NewElement(NULL, "soap2:Envelope");
// Made this change due to email from Steve Denner 05/02/09 10:47
//	rogxml_AddNamespace(rxSoap, "soap2", "http://www.w3.org/2003/05/soap-envelope");
	rogxml_AddNamespace(rxSoap, "soap2", "http://schemas.xmlsoap.org/soap/envelope/");
	rogxml_AddNamespace(rxSoap, "wsa", "http://schemas.xmlsoap.org/ws/2004/08/addressing");
	rogxml_AddNamespace(rxSoap, "hl7", "urn:hl7-org:v3");
	rxHeader = rogxml_AddChild(rxSoap, "soap2:Header");
	rogxml_AddTextChildf(rxHeader, "wsa:MessageID", "uuid:%s", msg_GetMessageId(msg));
	rogxml_AddTextChildf(rxHeader, "wsa:Action", "%s/%s", contract_GetService(c), msg_GetInteractionId(msg));
	rogxml_AddTextChild(rxHeader, "wsa:To", szEndpoint);
	rx=rogxml_AddChild(rxHeader, "wsa:From");
	rogxml_AddTextChild(rx, "wsa:Address", szMyUrl);

	rx=rogxml_AddChild(rxHeader, "hl7:communicationFunctionRcv");
	rx=rogxml_AddChild(rx, "hl7:device");
	rx=rogxml_AddChildAttr(rx, "hl7:id", "root", "2.16.840.1.113883.2.1.3.2.4.10");
	rogxml_SetAttr(rx, "extension", msg_GetToPartyId(msg));
	rx=rogxml_AddChild(rxHeader, "hl7:communicationFunctionSnd");
	rx=rogxml_AddChild(rx, "hl7:device");
	rx=rogxml_AddChildAttr(rx, "hl7:id", "root", "2.16.840.1.113883.2.1.3.2.4.10");
	rogxml_SetAttr(rx, "extension", msg_GetFromPartyId(msg));

	rx=rogxml_AddChild(rxHeader, "wsa:ReplyTo");
	rogxml_AddTextChild(rx, "wsa:Address", szMyUrl);

	rxBody = rogxml_AddChild(rxSoap, "soap2:Body");
	rogxml_LinkChild(rxBody, rxHL7);

	msg_SetXML(msg, rxSoap);							// Existing XML was unset (see msg_ReleaseXML() above)
	msg_SetWrapped(msg, WRAP_NASP);
	msg_SetHasPayload(msg, 1);

	szDelete(szEndpoint);
}

void AddNasp(MSG *msg)
{
	int nLevel = msg_GetLevel(msg);

	if (nLevel == LEVEL_P1R1) {
		AddNasp1(msg);
	} else if (nLevel == LEVEL_P1R2) {
		AddNasp2(msg);
	} else {
		Log("Internal fault - Level = %d", nLevel);
	}
}

void TableNotes(BIO *io, note_t *note, const char *szDir)
{
	const char *szInteractionName;
	const char *szInteractionSection;
	const char *szInteraction = note_FindEntry(note, 'Y');
	const char *szDatetime = note_FindEntry(note, 'D');

	szInteractionName = MessageDescription(szInteraction, &szInteractionSection);
	if (!szInteractionSection) szInteractionSection = "";
	if (!szInteractionName) szInteractionName = "";

	BIO_putf(io, "<table bgcolor=#000077 bordercolor=blue border=1 cellspacing=1 width=100%>\r\n");
	BIO_putf(io, "<tr bgcolor=#aa88ff><th colspan=2><font size=5>Details</font>");
	BIO_putf(io, "<tr><td bgcolor=#aa88ff>Directory<td bgcolor=#ffffaa>%s\r\n", szDir);
	BIO_putf(io, "<tr><td bgcolor=#aa88ff>Processed<td bgcolor=#ffffaa>%s</td></tr>\r\n", szDatetime);
	if (*szInteractionSection || *szInteractionName)
		BIO_putf(io, "<tr><td bgcolor=#aa88ff>Interaction type<td bgcolor=#ffffaa>%s: %s\r\n", szInteractionSection, szInteractionName);
	BIO_putf(io, "<tr><td bgcolor=#aa88ff>Interaction ID<td bgcolor=#ffffaa>%s\r\n", szInteraction);
	BIO_putf(io, "<tr><td bgcolor=#aa88ff>Message ID<td bgcolor=#ffffaa>%s\r\n", note_FindEntry(note, 'I'));
	BIO_putf(io, "<tr><td bgcolor=#aa88ff>Conversation ID<td bgcolor=#ffffaa><a href=\"/status?f=%.8s\">%s</a>\r\n", note_FindEntry(note, 'C'), note_FindEntry(note, 'C'));
	BIO_putf(io, "<tr><td bgcolor=#aa88ff>From<td bgcolor=#ffffaa>%s\r\n", note_FindEntry(note, 'F'));
	BIO_putf(io, "<tr><td bgcolor=#aa88ff>To<td bgcolor=#ffffaa>%s\r\n", note_FindEntry(note, 'T'));
	BIO_putf(io, "<tr><td bgcolor=#aa88ff>MMTS ID<td bgcolor=#ffffaa>%s\r\n", note_FindEntry(note, 'J'));
	BIO_putf(io, "</table>\r\n");
}

void TableNoteMessages(BIO *io, note_t *note, const char *szDir)
{
	int i;

	BIO_putf(io, "<table bgcolor=#000077 bordercolor=blue border=1 cellspacing=1 width=100%>\r\n");
	BIO_putf(io, "<tr bgcolor=#aa88ff><th colspan=2><font size=5>Message Processing</font>");
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
		BIO_putf(io, "<td bgcolor=#ffffaa><a href=\"/message/%s/%s.%s/show\">%s</a>",
				szDir, szName, szExt, szDescr);
		szDelete(szCopy);
	}
	BIO_putf(io, "</table>");
}

void TableNoteLogs(BIO *io, note_t *note, const char *szDir)
{
	const char *szLog = hprintf(NULL, "%s/%s/log", szMsgLogDir, szDir);
	FILE *fp=fopen(szLog, "r");

	if (fp) {
		const char *szLine;

		BIO_putf(io, "<table border=1 cellspacing=0 width=100%>\r\n");
		BIO_putf(io, "<tr bgcolor=#44aaff><th>Time<th>Event\r\n");
		while ((szLine=ReadLine(fp))) {
			int bError = !!strcasestr(szLine, "error");
			char *szLineCopy = strdup(szLine);
			szLineCopy = (char*)strsubst(szLineCopy, "&", "&amp;");
			szLineCopy = (char*)strsubst(szLineCopy, "<", "&lt;");
			szLineCopy = (char*)strsubst(szLineCopy, ">", "&gt;");
			char *szTime = strtok(szLineCopy, " ");
			char *szEvent=strtok(NULL, "");
			BIO_putf(io, "<tr><td valign=top>%s</td>", szTime);
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

void SendHtmlHeader(BIO *io)
{
#define MMTS_BGCOL1	"#4466aa"			// Main MMTS title background
#define MMTS_BGCOL2	"#7799bb"			// details of connection
#define MMTS_BGCOL3	"#224466"			// Links etc. under main title
#define MMTS_FGCOL1	"#ffffff"
#define MMTS_FGCOL2	"#ffffff"
#define MMTS_FGCOL3	"#ffffff"

// $mtorange="#f57023"; $mtblue="#2b509a";
	char szHostname[50];
	time_t now = time(NULL);
	struct tm *tm=gmtime(&now);

	gethostname(szHostname, sizeof(szHostname));

	BIO_putf(io,
		"<META HTTP-EQUIV='Pragma' CONTENT='no-cache'>"
		"<hr>"
		"<table border=0 width=100%% bgcolor=" MMTS_BGCOL1 ">"
		"<tr><td rowspan=2 colspan=2 align=center><font face=\"calibri,verdana,arial\" size=5 color=" MMTS_FGCOL1 ">MMTS v" VERSION " - The Microtest Message Transfer System (" OS ")</font>"
		"<td bgcolor=" MMTS_BGCOL2 "><font size=3 color=" MMTS_FGCOL1 "> &nbsp; %s</font>"
		"<tr><td bgcolor=" MMTS_BGCOL2 "><font size=3 color=" MMTS_FGCOL1 "> &nbsp; %s</font>"
		"<tr bgcolor= " MMTS_BGCOL3 "><td>"
		"| <a href=/><font color=white>Home</font></a> "
		"| <a href=/status><font color=white>Messages</font></a> "
		"| <a href=/spider><font color=white>Spider</font></a> "
		"|"
		"<td align=right width=1><a href=\"/setenv\"><font color=white>%s</font></a>"
		"<td><font color=" MMTS_FGCOL1 "> &nbsp; UTC is %d-%02d-%04d %02d:%02d:%02d</font>"
		"</table>"
		"<hr>",
		szHostname, _szIncomingIp,
		szEnvironment?szEnvironment:"none",
		tm->tm_mday, tm->tm_mon+1, tm->tm_year+1900,
		tm->tm_hour, tm->tm_min, tm->tm_sec);
}

void AddHtmlHeader(HBUF *hbuf)
{
#define MMTS_BCOL1	"#555555"			// Main MMTS title background
#define MMTS_BCOL2	"#7799bb"			// details of connection
#define MMTS_BCOL3	"#224466"			// Links etc. under main title
#define MMTS_FCOL1	"#ffffff"
#define MMTS_FCOL2	"#ffffff"
#define MMTS_FCOL3	"#ffffff"

// $mtorange="#f57023"; $mtblue="#2b509a";
	char szHostname[50];
	time_t now = time(NULL);
	struct tm *tm=gmtime(&now);

	gethostname(szHostname, sizeof(szHostname));

	bprintf(hbuf, 
		"<META HTTP-EQUIV='Pragma' CONTENT='no-cache'>"
		"<hr>"
		"<table border=0 width=100%% bgcolor=" MMTS_BCOL1 ">"
		"<tr><td rowspan=2 colspan=2 align=center><font face=\"calibri,verdana,arial\" size=5 color=" MMTS_FCOL1 ">MMTS v" VERSION " - The Microtest Message Transfer System (" OS ")</font>"
		"<td bgcolor=" MMTS_BCOL2 "><font size=3 color=" MMTS_FCOL1 "> &nbsp; %s</font>"
		"<tr><td bgcolor=" MMTS_BCOL2 "><font size=3 color=" MMTS_FCOL1 "> &nbsp; %s</font>"
		"<tr bgcolor= " MMTS_BCOL3 "><td>"
		"| <a href=/><font color=white>Home</font></a> "
		"| <a href=/status><font color=white>Messages</font></a> "
		"| <a href=/spider><font color=white>Spider</font></a> "
		"|"
		"<td align=right width=1><a href=\"/setenv\"><font color=white>%s</font></a>"
		"<td><font color=" MMTS_FCOL1 "> &nbsp; UTC is %d-%02d-%04d %02d:%02d:%02d</font>"
		"</table>"
		"<hr>",
		szHostname, _szIncomingIp,
		szEnvironment?szEnvironment:"none",
		tm->tm_mday, tm->tm_mon+1, tm->tm_year+1900,
		tm->tm_hour, tm->tm_min, tm->tm_sec);
}

int IsValidEnvironment(const char *szEnv)
// Checks if 'szEnv' is a valid environment by seeing if there is a directory under 'env' with that name
// or that it is NULL or "default".
{
	const char *szDir;
	int bResult=0;

	if (!szEnv) return 0;					// NULL environments can't be good

	szDir = hprintf(NULL, "env/%s", szEnv);
	bResult = isDir(szDir);
	szDelete(szDir);

	return bResult;
}

SIMAP *GetEnvironments()
// Returns a map of the available environments
{
	struct dirent *d;
	DIR *dir;
	SIMAP *si = simap_New();

	dir=opendir("env");

	if (dir) {
		while ((d=readdir(dir))) {
			if (*d->d_name == '.') continue;		// Skip '.' files
			if (IsValidEnvironment(d->d_name)) {
				simap_Add(si, d->d_name, 1);
			}
		}
		closedir(dir);
	}

	return si;
}

void RememberEnvironment(const char *szIp, int nPort, const char *szEnvironment, int nGood)
// Sets an environment as either being good or not good.
// If 'port' is 0 then the basic IP environment will be set, if szIP is NULL then the default environment is set
// Entry in config file is deleted if szEnvironment is NULL.
// Now ignores requests to set a bad environment and thus the existing configurations are respected.
{
	if (!nGood) return;			// Ignore if we're not setting a good environment

	const char *szVar;
//Log("RememberEnvironment('%s:%d', '%s', %s)", szIp, nPort, szEnvironment, nGood?"Good":"Bad");

	if (szIp) {
		if (nPort) {
			szVar=hprintf(NULL, "environment-%s:%d", szIp, nPort);
		} else {
			szVar=hprintf(NULL, "environment-%s", szIp);
		}
	} else {
			szVar=strdup("environment");
	}

	if (nGood) {
		config_SetString(szVar, szEnvironment);
	} else {							// Bad then add to the list already there
		const char *szEnv;
		const char *szBad = NULL;							// WIll be our list of bad boys

		Log("Marking %s as not valid for %s", szEnvironment, szVar);
		szEnv = config_GetString(szVar);

		if (szEnv && *szEnv == '~') {						// A previous negative list
			char *szList=strdup(SkipSpaces(szEnv+1));		// Take all but the initial ~
			char *chp;
			SIMAP *m = simap_New();
			const char *szKey;

			for (chp=strtok(szList,",");chp;chp=strtok(NULL, ",")) {	// Add the previous attempts to a list
				simap_Add(m, chp, 1);
			}
			simap_Add(m, szEnvironment, 1);					// Add in our new black sheep

			while (simap_GetNextEntry(m, &szKey, NULL)) {	// Convert the map into a string
				if (szBad) szBad=strappend(szBad, ",");
				szBad=hprintf(szBad, "%s", szKey);
			}
			szDelete(szList);
			simap_Delete(m);
		} else {
			szBad=strdup(szEnvironment);					// Make us the sole bad boy
		}
		szDelete(szEnv);

		szEnv=hprintf(NULL, "~%s", szBad);					// Set to ~bad,bad,bad...
		config_SetString(szVar, szEnv);
		szDelete(szEnv);
		szDelete(szBad);
	}

	szDelete(szVar);
}

void SetEnvironment(const char *szEnv)
// Sets the current environment including setting a couple of dependant variables
{
	const char *szTmp;

	szDelete(szEnvironment);
	szEnvironment=strdup(szEnv);
	SetUtilsEnvironment(szEnvironment);

	// Now set up some peripheral variables that depend on the environment
	szDelete(szContractDir);
	szDelete(szEnvDir);

	szEnvDir=hprintf(NULL, "%s/env/%s", GetBaseDir(), szEnvironment);
	szContractDir=hprintf(NULL, "%s/contracts", szEnvDir);
	if (!isDir(szEnvDir)) {
		Fatal("Environment directory (%s) doesn't exist", szEnvDir);
	}

//	contract_SetBaseDir(szBaseDir);
	contract_SetContractDir(szContractDir);
	contract_SetEnvDir(szEnvDir);						// Tell contracts system where env dir is
	szTmp = config_EnvGetString("xml-linefeed");
	if (szTmp) {
		const char *chp=szTmp;

		szXmlLinefeed = strdup("");

		while (*chp) {
			while (*chp == '"' || *chp == '\'' || isspace(*chp)) chp++;
			if (strlen(chp) >= 2) {
				int a=toupper(chp[0]);
				int b=toupper(chp[1]);
				if (a >= 'A') a -= ('A' - '0' - 10);
				if (b >= 'A') b -= ('A' - '0' - 10);
				a-='0'; b-='0';
				if (a < 0 || a > 15 || b < 0 || b > 15) {
					Fatal("Bad HEX (%.2s) in xml-linefeed setting", chp);
				}
				szXmlLinefeed = hprintf(szXmlLinefeed, "%c", a*16+b);
				chp+=2;
			} else {
				break;
			}
		}
	}
	szTmp = config_EnvGetString("xml-indent");
	if (szTmp) {
		int nTmp = atol(szTmp);
		char *szIndent;
		char *chp;

		if (nTmp < 0) nTmp=0;

		szIndent=malloc(nTmp+1);
		for (chp=szIndent;nTmp>0;nTmp--) *chp++=' ';
		*chp='\0';

		szXmlIndent = szIndent;				// This is never free'd but it's a small price to pay
		szDelete(szTmp);
	}
}

const char *GetEnvironment(const char *szIp, int nPort)
// Reads the environment for the IP address or the default if there isn't a specific one
// If 'port' is 0 then the basic IP environment will be got
// Sets the global 'szEnvironment'
// szEnvironment will be set to a string on exit from here or a fatal error will have occurred
{
	const char *szVar=NULL;
	const char *szEnv=NULL;

	if (nPort) {
		szVar=hprintf(NULL, "environment-%s:%d", szIp, nPort);
		szEnv = config_GetString(szVar);

		if (szEnv && *szEnv == '~') {								// In negotiation phase
			SIMAP *m=GetEnvironments();								// Get list of environments
			char *szList;
			char *chp;
			int nCount;

			szList=strdup(SkipSpaces(szEnv+1));						// Take all but the initial ~
			szDelete(szEnv);

			for (chp=strtok(szList,",");chp;chp=strtok(NULL, ",")) {	// Drop the previous attempts from the list
				simap_DeleteKey(m, chp);							// Remove it from our list
			}
			szDelete(szList);

			nCount=simap_Count(m);									// See how many we have left
			if (nCount) {											// Good, at least one...!
				const char *szKey = "BUG_1";

				simap_GetNextEntry(m, &szKey, NULL);
				szEnv=strdup(szKey);
			} else {												// All the possibilities have been tried
				config_SetString(szVar, "");						// Erase them so we start again
				szEnv=NULL;
			}
			simap_Delete(m);
		}
	}

	if (!szEnv && szIp) {										// Port was 0 or no entry found for specific port
		szDelete(szVar);
		szVar=hprintf(NULL, "environment-%s", szIp);
		szEnv = config_GetString(szVar);
	}

	if (!szEnv) {												// Nothing specific found at all so take the default
		szDelete(szVar);
		szVar=strdup("environment");
		szEnv = config_GetString(szVar);
	}

	if (!szEnv) {												// Nothing for this address or port or even a default
		SIMAP *m=GetEnvironments();								// Get list of environments
		int nCount = simap_Count(m);

		if (nCount == 0) {
			Fatal("No environments set up in 'env'");
		}

		if (nCount > 1) {
			Log("Multiple environments available, but no default - picking one");
		}

		const char *szKey = "BUG_2";

		simap_GetNextEntry(m, &szKey, NULL);
		szEnv=strdup(szKey);
		simap_Delete(m);

		config_SetString("environment", szEnv);					// Set it for next time
		Log("Default environment set to '%s' in %s", szEnv, GetConfigFile());
		szDelete(szVar);
		szVar=NULL;
	}

	if (szVar) {
		Log("Environment set to '%s' for %s:%d (%s:%s)", szEnv, szIp, nPort, GetConfigFile(), szVar);
		szDelete(szVar);
	}

	SetEnvironment(szEnv);

	szDelete(szEnv);

	return szEnvironment;
}

const char *FileContentType(const char *szFilename)
{
	const char *szContentType = NULL;
	const char *ext = strrchr(szFilename, '.');
	static SSMAP *map = NULL;

	if (ext) {
		char *extCopy = strdup(ext+1);
		int len = strlen(extCopy);
		if (len && extCopy[len-1] == '/') extCopy[len-1]='\0';		// Strip any trailing /
		strlwr(extCopy);

		if (!map) {
			map = ssmap_New();
			ssmap_Add(map, "dat", "application/octet-stream");
			ssmap_Add(map, "jpg", "image/jpeg");
			ssmap_Add(map, "jpeg", "image/jpeg");
			ssmap_Add(map, "tif", "image/tiff");
			ssmap_Add(map, "tiff", "image/tiff");
			ssmap_Add(map, "gif", "image/gif");
			ssmap_Add(map, "png", "image/png");
			ssmap_Add(map, "bmp", "image/bmp");
			ssmap_Add(map, "csv", "text/csv");
			ssmap_Add(map, "xml", "text/xml");
			ssmap_Add(map, "htm", "text/html");
			ssmap_Add(map, "html", "text/html");
			ssmap_Add(map, "zip", "application/zip");
			ssmap_Add(map, "pdf", "application/pdf");
			ssmap_Add(map, "xls", "application/vnd.ms-excel");
			ssmap_Add(map, "xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
			ssmap_Add(map, "doc", "application/msword");
			ssmap_Add(map, "docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document");
			ssmap_Add(map, "txt", "text/plain");
		}

		szContentType = ssmap_GetValue(map, extCopy);
		szDelete(extCopy);
	}

	return szContentType;
}

int DeliverFile(BIO *io, const char *szFilename, const char *szContentType)
{
	FILE *fp = fopen(szFilename, "r");

	if (!fp) {
		MyBIO_puts(io, HttpHeader(403));		// Forbidden
		return 0;
	}

	fseek(fp, 0L, SEEK_END);
	unsigned long size = ftell(fp);
	fseek(fp, 0L, SEEK_SET);

	if (!szContentType) szContentType = FileContentType(szFilename);
	if (!szContentType) szContentType = "text/plain";

	MyBIO_puts(io, HttpHeader(200));
	MyBIO_puts(io, "Server: Microtest Message Transport Server\r\n");
	BIO_putf(io, "Content-Type: %s\r\n", szContentType);
	BIO_putf(io, "Content-Length: %ld\r\n", size);
	BIO_putf(io, "\r\n", size);

	int got;
	char buf[32768];
	while ((got=fread(buf, 1, sizeof(buf), fp)))
		MyBIO_write(io, buf, got);
	fclose(fp);

	Log("Delivered file %s", szFilename);

	return 1;
}

const char *FileDateString(time_t t)
// Note these are for html output so may have a &nbsp; in them
{
	static const char *result = NULL;
	struct tm *tm=localtime(&t);

	szDelete(result);
	if (t < time(NULL) - 60*60*24*180) {
		result = hprintf(NULL, "%.3s %2d  %04d", MonthNames[tm->tm_mon+1], tm->tm_mday, tm->tm_year+1900);
	} else {
		result = hprintf(NULL, "%.3s %2d %02d:%02d", MonthNames[tm->tm_mon+1], tm->tm_mday, tm->tm_hour, tm->tm_min);
	}

	return strsubst(result, " ", "&nbsp;");
}

int MonthLen(int month, int year)
{
	static int lens[]={0,31,28,31,30,31,30,31,31,30,31,30,31};

	month = (month+11)%12+1;
	return (month == 2 && !(year & 3)) ? 29 : lens[month];
}

void SubtractDays(int period, int *day, int *month, int *year)
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
		*day=MonthLen(*month, *year);
	}
}

int LogSslError(SSL *ssl, int ret)
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

typedef struct {
	int verbose_mode;
	int verify_depth;
	int always_continue;
} mydata_t;
int mydata_index;
static int verify_depth = 5;			// Allow certificates 5 deep, which should be way ample

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
return (time_t)0;				// TODO: BUG - This should be removed if the SL3 routines are not at fault
	time_t result = 0;
	S3 *s3 = NULL;

	s3 = s3_Openf("%s/certs/certs.sl3", szEnvDir);

	if (s3) {
		s3_Queryf(s3, "SELECT effective FROM revocation WHERE ca=%s AND serial=%d", ca, serial);
		int err = s3_ErrorNo(s3);
		if (err == 1) {
			// Table doesn't exist, in which case we don't care
		} else if (err) {
		    const char *errstr = s3_ErrorStr(s3);
		    Log("SQL Error %d: %s", err, errstr);
		    Log("QRY: %s", s3_LastQuery(s3));
		} else {
			const char **row = s3_Next(s3);

			if (row) {
				const char *szDate = row[0];
				result = timeFromSqlTime(szDate);
			}
		}
		s3_Close(s3);
	}

	return result;
}

struct in_addr GetHostAddress(const char *szHost)
// Get a host address given a string of either it's name or IP address
// in_addr is a struct holding a union.  The nicest bit of it is addr.s_addr, which is an 'unsigned long'
// returns INADDR_NONE if nothing can be found.
{
	struct in_addr add;
	struct hostent *hp;

	const char *szCacheFilename = szHost;
	SSMAP *map=cache_LoadFromFile("dns", szCacheFilename, 1);		// Read in any cached version

	const char *szResult = ssmap_GetValue(map, "ip");				// The previous result
	if (szResult) {
		int ok = inet_aton(szResult, &add);
		if (ok) {
			ssmap_Delete(map);

			return add;
		} else {
			Log("Cached DNS address of '%s' is '%s', which is invalid!", szHost, szResult);
			cache_DeleteFile("dns", szCacheFilename);
			ssmap_Delete(map);
			map=NULL;
		}
	}

	time_t nStarted, nFinished;
	nStarted=time(NULL);
	hp=gethostbyname(szHost);
	nFinished=time(NULL);

	if (nFinished - nStarted > 5) {
		Log("Lookup of '%s' was slow (%d seconds) - DNS Problem?", szHost, nFinished-nStarted);
	}

	if (hp) {
		add=*(struct in_addr*)hp->h_addr_list[0];
	} else {
		unsigned int ad=inet_addr(szHost);
		add=*(struct in_addr*)&ad;
	}

	if (add.s_addr != INADDR_NONE) {
		if (!map) map=ssmap_New();

		const char *szBestBefore = config_EnvGetString("dnsbestbefore");
		if (!szBestBefore) szBestBefore = strdup("P1D");		// Default of 1 day best before
		const char *szSellBy = msg_AddPeriod(msg_Now(), szBestBefore);
		szDelete(szBestBefore);

		ssmap_Add(map, "ip", (const char *)inet_ntoa(add));
		ssmap_Add(map, "host", szHost);
		ssmap_Add(map, "cachetime", msg_Now());
		ssmap_Add(map, "bestbefore", szSellBy);

		cache_SaveToFile(map, "dns", szCacheFilename);

		ssmap_Delete(map);
	}

	return add;
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

	X509 *peer = X509_STORE_CTX_get_current_cert(ctx);
	depth = X509_STORE_CTX_get_error_depth(ctx);
	err = X509_STORE_CTX_get_error(ctx);

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

	if (!err && !depth) {
	// TODO:
	// At this point we should be applying a check on the CN.  For example, the SSP presents a certificate with a CN
	// of "msg-out.dev.spine2.ncrs.nhs.uk" - we should do a dns lookup on this and fail it if we don't recognise it.
	// Ok, done that but it may be that it should be conditional...
		struct in_addr cnAddress = GetHostAddress(szPeerCn);

		if (cnAddress.s_addr == INADDR_NONE) {
			Log("DNS lookup of connecting CN (%s) failed", szPeerCn, depth);
			err = X509_V_ERR_CERT_UNTRUSTED;
		} else if (strcmp(inet_ntoa(cnAddress), _szSenderIp)) {
			Log("Connecting CN (%s) resolves to %s, while their IP address is %s", szPeerCn, inet_ntoa(cnAddress), _szSenderIp);
			err = X509_V_ERR_CERT_UNTRUSTED;
		}
	}

	// Get the common name of the peer's certificate issuer (its parent)
	X509_NAME_get_text_by_NID( X509_get_issuer_name(peer), NID_commonName, buf, sizeof(buf));

	long nSerial = longFromASN1_INTEGER(X509_get_serialNumber(peer));
	X509_NAME_oneline(X509_get_issuer_name(peer), buf, 256);
	time_t tRevoked = isRevoked(buf, nSerial);

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

static int verify_callback_snd(int preverify_ok, X509_STORE_CTX *ctx)
{
	int err = X509_STORE_CTX_get_error(ctx);
	if (err == 19) {
		Log("SSL Ignoring self-signed certificate (preverify=%d)", preverify_ok);
		X509_STORE_CTX_set_error(ctx, 0);
	}
	return verify_callback_rcv(preverify_ok, ctx);
}

mydata_t mydata;

int tcp_ListenOn(int nPort)
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

int tcp_Connect(const char *szHost, int nPort)
// Creates an outgoing connection to the given host and port
// Returns	0		Failed (0 can't be valid outgoing as it would already have been used for an incoming...!)
//			1...	The socket number
{
	struct sockaddr_in addr;
	int nSock;

	memset(&addr,0,sizeof(addr));
	addr.sin_family=AF_INET;
	addr.sin_port=htons(nPort);
	addr.sin_addr=GetHostAddress(szHost);

	if (addr.sin_addr.s_addr == INADDR_NONE) {
		LogError(12, "Could not get IP address from host '%s'", szHost);
		return 0;
	}

	if((nSock=socket(AF_INET,SOCK_STREAM, IPPROTO_TCP))<0) {
		LogError(10, "Couldn't create socket to connect to %s", szHost);
		return 0;
	}

	if(connect(nSock,(struct sockaddr *)&addr, sizeof(addr))<0) {
		LogError(11, "Couldn't connect socket to '%s' (%s)", szHost, inet_ntoa(addr.sin_addr));
		return 0;
	}

	return nSock;
}

BIO *BuildConnection(const char *szAddress, int nPort)
// Builds a connection using SSL if the port number is 443
{
	BIO *io_socket;								// The way SSL talks to the port
	BIO *io_buffer;								// Buffered BIO - the way we talk through the SSL
	int nErr;

	// Create the SSL/TLS connection to the spine
//	Log("Making TCP connection");
	g_sock = tcp_Connect(szAddress, nPort);
	if (!g_sock) {
		SetError(5, "Failed to open TCP/IP connection to %s:%d", szAddress, nPort);
		return NULL;
	}

//	Log("Making socket");
	io_socket=BIO_new_socket(g_sock, BIO_NOCLOSE);
	if (!io_socket) {
		SetError(7, "Failed to create new BIO socket");
		return NULL;
	}

	if (nPort == 443 || nPort == 444 || nPort == 1880) { // 444 - QuickSilva's GP2DRS port usage, 1880 - NHS111
//	Log("Creating TLS/SSL context");
		BIO *io_ssl;								// SSL BIO talks directly to SSL

		g_ctx=ctx_New(GetCertificateDir(), szAddress, PASSWORD);
		// TODO: Make this better and less intrusive!
		if (!g_ctx) {
			SetError(4, "Crypto Config Error: %s", ctx_Error());
			return NULL;
		}

//	Log("Making TLS/SSL connection");
		g_ssl=SSL_new(g_ctx);
		if (!g_ssl) {
			SetError(6, "Failed to setup a new SSL/TLS connection");
			return NULL;
		}

		SSL_set_bio(g_ssl, io_socket, io_socket);					// g_ssl = SSL <-> socket

#if 1
	mydata_index = SSL_get_ex_new_index(0, "mydata index snd", NULL, NULL, NULL);
//Log("send mydata_index = %d", mydata_index);

//	SSL_CTX_set_verify(g_ctx,SSL_VERIFY_PEER,verify_callback_snd);
	SSL_set_verify(g_ssl,SSL_VERIFY_PEER,verify_callback_snd);
	SSL_CTX_set_verify_depth(g_ctx,verify_depth + 1);
	mydata.verbose_mode = 1;
	mydata.verify_depth = verify_depth;
	mydata.always_continue = 1;
	SSL_set_ex_data(g_ssl, mydata_index, &mydata);
#endif
		nErr=SSL_connect(g_ssl);
		if (nErr <= 0) {
			int nErr2 = SSL_get_error(g_ssl, nErr);
			int nErr3;

			Log("SSL Connection error %d: %s", nErr2, ERR_error_string(nErr2, NULL));
			while ((nErr3 = ERR_get_error())) {
				Log("SSL: %s", ERR_error_string(nErr3, NULL));
			}
			SetError(8, "Error %d(%d) opening SSL/TLS connection to %s:%d", nErr, nErr2, szAddress, nPort);
			return NULL;
		}
// Connect a BIO that we can neatly talk to SSL with
		io_ssl=BIO_new(BIO_f_ssl());
		BIO_set_ssl(io_ssl, g_ssl, BIO_CLOSE);				// io_ssl = g_ssl <-> socket

		io_buffer=BIO_new(BIO_f_buffer());
		BIO_push(io_buffer, io_ssl);						// buffer -> g_ssl <-> socket
	} else {
		g_ssl=NULL;
		g_ctx=NULL;
		io_buffer=BIO_new(BIO_f_buffer());
		BIO_push(io_buffer, io_socket);						// buffer -> connection
	}

	return io_buffer;
}

void BreakConnection(BIO *io)
// NB. Nothing is actually done with 'io' in here although we should probably be using it to ensure
// the underlying socket is shutdown and closed properly.  However, at this time (12-04-2010) I'm unsure
// as to whether we should be doing this so I've left it possibly open...
//
// In fact I suspect we don't need to have g_ssl, g_ctx etc. as we could call a shutdown on 'io' and have
// it all done for us.
{
	if (g_ssl) {
		SSL_shutdown(g_ssl);
		SSL_free(g_ssl);
		g_ssl=NULL;
	}

	if (g_sock > -1) {
		close(g_sock);
		g_sock=-1;
	}

	if (g_ctx) {
		ctx_Delete(g_ctx);
		g_ctx=NULL;
	}
}

int RogZap(const char *szDir)
// Recursively removes a directory or just a file
// Returns	0	Ok
//			-1	Error - see errno
{
	int nErr = 0;
	struct stat st;

//Log("Zapping '%s'", szDir);

	if (!szDir || !*szDir || !strcmp(szDir, "/"))		// A little paranoia can be a good thing!
		return -1;

	if (!stat(szDir, &st)) {
		if (st.st_mode & S_IFDIR) {						// It's a directory so delete its contents
			struct dirent *d;
			DIR *dir=opendir(szDir);

			if (dir) {
				while ((d=readdir(dir)) && !nErr) {		// Get each file within the directory
					const char *szName = d->d_name;
					const char *szSubFile;

					if (!strcmp(szName, ".") || !strcmp(szName, ".."))		// Leave "." and ".."
						continue;

					szSubFile = hprintf(NULL, "%s/%s", szDir, szName);
					nErr = RogZap(szSubFile);								// Recurse to delete subdirectories/files
					szDelete(szSubFile);
				}
				closedir(dir);
				rmdir(szDir);
			} else {									// Couldn't open directory
				nErr = -1;
			}
		} else {
			nErr = unlink(szDir);						// It's a file so just unlink it
		}
	} else {											// Couldn't stat()
		nErr = -1;
	}

	return nErr;
}

int RogRename(const char *szSrc, const char *szDest)
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

static void file_Rollover(const char *szFilename, unsigned int limit)
// Checks the size of the log file and retires it if it's getting too big
// NB. This is only called by the daemon and might exit if the log file can't be rolled over
{
	struct stat st;

	if (!szFilename || !limit) return;

	if (!stat(szFilename, &st) && st.st_size > limit) {     // Log file exists and is big
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
			Log("MMTS " VERSION " for " OS " (Made " __TIME__ " on " __DATE__ ", using %s)", SSLeay_version(SSLEAY_VERSION));
			Log("Previous log file archived as %s", rollover);
		}
		szDelete(rollover);
	}
}

int msg_SaveAsFiles(MSG *msg, const char *szPrefix, const char *szDir)
// Saves the message as separate files into the directory given
// szPrefix (if non-NULL) is prepended to the control and data filenames
// Returns	0	Ok
//			1	Failed to create control file (directory doesn't exist / isn't writeable?)
//			2	Couldn't create a data file - very peculiar
{
	FILE *fpCtl;
	const char *szFile;
	int i;
	int nErr = 0;

	if (!szPrefix) szPrefix="";
	szFile = hprintf(NULL, "%s/%scontrol", szDir, szPrefix);
	fpCtl=fopen(szFile, "w");
	szDelete(szFile);

	if (!fpCtl) return 1;			// Failed to create control file - directory doesn't exist?

	fprintf(fpCtl, "mmts-version=%s\n", VERSION);
	if (NoteDir()) fprintf(fpCtl, "msgdir=%s\n", NoteDir());
	if (msg_GetURI(msg)) fprintf(fpCtl, "uri=%s\n", msg_GetURI(msg));
	if (msg_GetMessageId(msg)) fprintf(fpCtl, "message-id=%s\n", msg_GetMessageId(msg));
	if (msg_GetWrapperId(msg)) fprintf(fpCtl, "wrapper-id=%s\n", msg_GetWrapperId(msg));
	if (msg_GetRefToMessageId(msg)) fprintf(fpCtl, "refto-message-id=%s\n", msg_GetRefToMessageId(msg));
	if (msg_GetToPartyId(msg)) fprintf(fpCtl, "to-party-id=%s\n", msg_GetToPartyId(msg));
	if (msg_GetFromPartyId(msg)) fprintf(fpCtl, "from-party-id=%s\n", msg_GetFromPartyId(msg));
	if (msg_GetToAsid(msg)) fprintf(fpCtl, "to-asid=%s\n", msg_GetToAsid(msg));
	if (msg_GetFromAsid(msg)) fprintf(fpCtl, "from-asid=%s\n", msg_GetFromAsid(msg));
	if (msg_GetInteractionId(msg)) fprintf(fpCtl, "interaction-id=%s\n", msg_GetInteractionId(msg));
	if (msg_GetConversationId(msg, NULL)) fprintf(fpCtl, "conversation-id=%s\n", msg_GetConversationId(msg, NULL));

	SSMAP *attrs = msg->attrs;
	if (attrs) {
		const char *szName;
		const char *szValue;

		ssmap_Reset(attrs);
		while (ssmap_GetNextEntry(attrs, &szName, &szValue)) {
			fprintf(fpCtl, "attr-%s=%s\n", szName, szValue);
		}
	}

	for (i=0;i<=msg_GetAttachmentCount(msg);i++) {
		const char *szFile=hprintf(NULL, "%s/%sdata%d", szDir, szPrefix, i+1);
		FILE *fp=fopen(szFile, "w");
		const char *szContentType;
		const char *szContentId;
		const char *szName;

		if (!fp) {					// Couldn't create data file (very peculiar as control file was ok)
			nErr=2;
			break;
		}

		if (i==0) {
			int nErr = msg_GetErrorNo(msg);
			const char *szText = NULL;						// Text on heap to write to file

			if (nErr) {										// 21709 - Write some useful data
				const char *szErr = msg_GetErrorText(msg);

				rogxml *rx=ErrorMessage(nErr, 1, "%s", szErr);// <MMTS-Ack type="error" code="n" severity="fatal">...</>
				szText=rogxml_ToNiceText(rx);
				rogxml_Delete(rx);

				fprintf(fpCtl, "data1-error-no=%d\n", nErr);
				fprintf(fpCtl, "data1-error-text=%s\n", szErr);
			} else {
				rogxml *rx=msg_GetXML(msg);
				szText=rogxml_ToNiceText(rx);
			}

			fwrite(szText, strlen(szText), 1, fp);
			szDelete(szText);
			szContentType=XML_MIME_TYPE;
			szContentId=msg_GetContentId(msg, NULL);
			szName="Message";
		} else {
			msg_attachment *a=msg_GetAttachment(msg, i);

			const char *szText=msg_GetAttachmentText(a);
			fwrite(szText, msg_GetAttachmentLength(a), 1, fp);
			szContentType=msg_GetAttachmentContentType(a);
			szContentId=msg_GetAttachmentContentId(a);
			szName=msg_GetAttachmentName(a);
			if (!szName) szName=msg_GetAttachmentDescription(a);

			char buf[30];
			if (!szName) {
				snprintf(buf, sizeof(buf), "Attachment %d.txt", i);

				szName=buf;
			}
		}
		fclose(fp);
		szDelete(szFile);
		fprintf(fpCtl, "data%d-content-type=%s\n", i+1, szContentType);
		fprintf(fpCtl, "data%d-content-id=%s\n", i+1, szContentId);
		fprintf(fpCtl, "data%d-name=%s\n", i+1, szName);
	}

	fclose(fpCtl);

	return nErr;
}

int CopyFile(const char *szSrc, const char *szDest)
// Copies szSrc to szDest
// Returns		-1		Could not open szDest
//				-2		Could not open szSrc
//				0		Ok
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

int MoveFile(const char *szSrc, const char *szDirDest, const char *szDest)
// Moves szSrc to the directory DirDest, calling it 'szDest' if it is non-NULL
{
	char szDestName[MAXNAMLEN];
	int err;

	if (szDest) {
		snprintf(szDestName, sizeof(szDestName), "%s/%s", szDirDest, szDest);
	} else {
		const char *chp=strrchr(szSrc, '/');
		if (chp) chp++; else chp=szSrc;
		snprintf(szDestName, sizeof(szDestName), "%s/%s", szDirDest, chp);
	}

	err=RogRename(szSrc, szDestName);

	return err;
}

int StoreMessage(const char *szId, MSG *m)
// Store the message as having been successfully received
// 22792 Now uses szId parameter rather than msg_GetMessageId()
{
	char *szBase;
	const char *szFilename;
	int nErr;

	szBase=strdup(szId ? szId : "NOID");
	strupr(szBase);
	szFilename=hprintf(NULL, "%s/%s.msg", szReceivedDir, szBase);
	szDelete(szBase);

	nErr=msg_Save(m, szFilename);

	szDelete(szFilename);

	return nErr;
}

int RawSendMessage(BIO *io, MSG *msg, int bAck, int nCode, const char *szCode)
// io is the IO channel on which the message is to be sent
// msg is the message (needs to be MIMEd first)
// nCode is the response code, szCode is the accompanying text:
//		0		This is a POST
//		200		Ok, etc...
// Returns the number of bytes sent except:
//		0		Failed to send at all for some reason
//		1		Didn't send due to an option in the option file
{
	const char *szMessage;
	int nLen;
	int nWritten;

	if (MessageOption(msg_GetInteractionId(msg), "nosend")) {
		Log("messageopt: Message not sent");
		return 1;												// Pretend it was sent (although it was short!)
	}

	// TODO: This will not work with binary content...
	szMessage = MakeHttpMessage(msg, bAck, nCode, szCode);
	nLen=strlen(szMessage);

//Log("Sending: %d '%s'", nLen, szMessage);
//	SSL_set_mode(ssl, SSL_MODE_ENABLE_PARTIAL_WRITE);
	nWritten=MyBIO_write(io, szMessage, nLen);		// Entire message
	szDelete(szMessage);
	MyBIO_flush(io);

	return (nWritten == nLen);
}

int SendAcknowledgementMessage(BIO *io, MSG *msg)
{
	int nCode = msg_IntAttr(msg, "positive", 1) ? 202 : 500;

	return RawSendMessage(io, msg, 1, nCode, "Accepted");
}

void RememberMessageId(const char *szId)
// Makes a note of the message ID so that HaveReceived will recognise it in the future
{
	if (szId) {
		const char *szFilename;
		char *szBase = strdup(szId);

		strupr(szBase);
		szFilename=hprintf(NULL, "%s/%s.rcvd", szReceivedDir, szBase);
		szDelete(szBase);

		int fd = creat(szFilename, 0666);
		if (fd >= 0) close(fd);

		szDelete(szFilename);
	}

	return;
}

int HaveReceived(const char *szId)
// Returns 1 if the message has already been received (i.e. we're receiving a duplicate)
{
	int bResult = 0;

	if (szId) {
		const char *szFilename;
		char *szBase = strdup(szId);

		strupr(szBase);													// Ensure guid is upper case
		szFilename=hprintf(NULL, "%s/%s.rcvd", szReceivedDir, szBase);
		szDelete(szBase);

		bResult=!access(szFilename, 0);

		szDelete(szFilename);
	}

	return bResult;
}

const char *ReadTextFile(const char *szFilename)
// Reads an entire file into a string.
// Returns NULL if the file can't be read, otherwise the entire file contents
{
	FILE *fp = fopen(szFilename, "r");
	const char *result = NULL;

	if (fp) {
		HBUF *h = hbuf_New();
		char buf[10240];
		int got;

		while ((got=fread(buf,1,sizeof(buf),fp))) {
			hbuf_AddBuffer(h, got, buf);
		}
		fclose(fp);

		hbuf_AddChar(h, 0);
		result = hbuf_ReleaseBuffer(h);
		hbuf_Delete(h);
	}

	return result;
}

const char *ReceiveMessage(const char *szDir)
// Does what 'doin' used to do, only synchronously
// The directory should contain 'data1' and 'control'.
// In here, we decide what sort of message it is and call a corresponding script/binary in the handlers directory
{
	const char *data1 = hprintf(NULL, "%s/data1", szDir);
	const char *control = hprintf(NULL, "%s/control", szDir);
	const char *output = NULL;

	rogxml *rxData1 = rogxml_ReadFile(data1);

	if (rxData1) {
		// type MUST end up being heap based
		const char *type = rogxml_GetValueByPath(rxData1, "/*/interactionId/@extension");

		if (type) {									// This is a plain HL7 type message - e.g. PRPA_IN010000IN05
			type = hprintf(NULL, "hl7-%s", type);
		} else {
			const char *ack = rogxml_GetValueByPath(rxData1, "/Envelope/Header/Acknowledgment");
			const char *fault = rogxml_GetValueByPath(rxData1, "/Envelope/Header/Fault");
			const char *errorlist = rogxml_GetValueByPath(rxData1, "/Envelope/Header/ErrorList");

			if (!fault) fault = rogxml_GetValueByPath(rxData1, "/Envelope/Body/Fault");

			if (ack) {
				type = strdup("soap-ack");
			} else if (fault) {
				type = strdup("soap-fault");
			} else if (errorlist) {
				type = strdup("soap-errorlist");
			} else {												// This is probably an ITK message
				FILE *fp = fopen(control, "r");
				const char *line;

				if (fp) {
					while ((line = ReadLine(fp))) {
						char *chp = strchr(line, '=');

						if (chp) {
							*chp='\0';

							if (!strcmp(line, "attr-soap-action")) {
								 type = SkipSpaces(chp+1);

								if (type && *type)
									type = hprintf(NULL, "soap-%s", type);
								break;
							}
						}
					}
					fclose(fp);
				}
			}
		}
		if (!type) type=strdup("unknown");

		// At this point, we might have a colon separated soap action so we'll check for ever more generic handlers

		const char *handler = hprintf(NULL, "%s/%s", szHandlerDir, type);
		const char *slash = strrchr(handler, '/');								// Cannot be NULL at this point
		char *colon = strrchr(handler, ':');	// NB. Effective quite <const_cast> going on here

		while (access(handler, 1) && colon && colon > slash) {				// While the handler isn't an executable
			*colon='\0';
			colon = strrchr(handler, ':');
		}

		if (access(handler, 1) && !strcmp(type, "unknown")) {		// Some confusion over unknown/default
			szDelete(type);
			szDelete(handler);
			type = strdup("default");
			handler = hprintf(NULL, "%s/%s", szHandlerDir, type);
		}

		if (!access(handler, 1)) {
			const char *returnfile = hprintf(NULL, "%s/%s", szDir, "return-data");

			const char *command = hprintf(NULL, "%s '%s' '%s' '%s' '%s'",
					handler, data1, type, control, returnfile);

Log("Command: %s", command);
			FILE *fp = popen(command, "r");
			if (fp) {
				char buf[10240];

				while (fgets(buf, sizeof(buf), fp)) {
					char *chp;
					chp = strchr(buf, '\n');
					if (chp) *chp='\0';
					Log(">> %s", buf);
				}
				pclose(fp);
				if (!access(returnfile, 4)) {
					output = ReadTextFile(returnfile);
					const char *szCommand = hprintf(NULL, "cp %s %s/%s", returnfile, NoteDir(), "handler-output.txt");
					system(szCommand);
					szDelete(szCommand);
					Note("M|%s|%s", "handler-output.txt", "Response from handler");
				}
				if (output)
					Log("Return data of length %d", strlen(output));
				else
					Log("No output returned from delivery");

				const char *szCommand = hprintf(NULL, "mv %s %s", szDir, szDoneDir);
				Log("Processed '%s' moved to done", szDir);
				system(szCommand);
				szDelete(szCommand);
			} else {
				Log("Error unable to handle message (give errno)");
				// Error unable to handle message (give errno)
			}
		} else {
			output = strdup(
					"<error source='doc.cdaHandler' code='unknown' display='I do not know how to handle this service or interaction'>"
					"</error>");
			Log("Error cannot handle given service or interaction");
		}

		szDelete(handler);
		rogxml_Delete(rxData1);
	}

	szDelete(control);
	szDelete(data1);

	return output;
}

const char *indir_Enable(const char *szDir)
// Accepts an 'in' directory that is expected to end '.tmp' and renames it to not have the '.tmp' on the end
// This means that the 'doin' external management script will take notice of it
// szDir MUST (REALLY, IT MUST) be allocated on the heap as it's deleted if successful.
// The right way of calling this is:	szDir=indir_Enable(szDir);
// Returns	const char *	New directory name (may be same as existing one if failed to rename)
{
//Log("indir_Enable(\"%s\")", szDir);
	if (strlen(szDir)>4 && !strcmp(szDir+strlen(szDir)-4, ".tmp")) {		// Ends in .tmp
		const char *szNewDir = hprintf(NULL, "%.*s", strlen(szDir)-4, szDir);	// Same without the .tmp

		if (!RogRename(szDir, szNewDir)) {
			szDelete(szDir);
			szDir=szNewDir;
			Log("Delivered incoming message to %s", szDir);
// TODO: There should be an external trigger to indicate there's a message to pick up
		} else {
			Log("Error %d renaming temp dir '%s' to '%s'", errno, szDir, szNewDir);
			szDelete(szNewDir);
		}
	}

	return szDir;
}

const char *msg_DropAsTmpIn(MSG *msg, MSG *msgOrig)
// Creates a temporary 'in' directory for the message and writes it there
// If there is a 'msgOrig' then that is also placed in the directory as an additional file
// Returns	const char *	The directory name (ends in .tmp so needs enabling with indir_Enable())
//			NULL			Failed to create directory or store the message
{
	const char *szRcvDir;

	szRcvDir = CreateReceiveDir(msg);
	if (szRcvDir) {
		if (!msg_SaveAsFiles(msg, NULL, szRcvDir)) {
			if (msgOrig) {
				if (!msg_SaveAsFiles(msgOrig, "original-", szRcvDir)) {
					Log("Delivered into %s with original message", szRcvDir);
				} else {
					Log("Delivered into %s, but failed to write original message", szRcvDir);
				}
			} else {
				Log("Delivered into %s", szRcvDir);
			}
		} else {
			Log("Failed to deliver message into '%s'", szRcvDir);
			RogZap(szRcvDir);
			szDelete(szRcvDir);
			szRcvDir = NULL;
		}
	} else {
		Log("Failed to create receive dir for message");
	}

	return szRcvDir;
}

int IsWaitingFor(MSG *msg)
// Returns the process id of the waiting instance if something is waiting for this message
// 22272 Attempts match on conversation ID as well as RefTo ID
// Returns 1 if we've fallen back to the old '.waiting' method
{
	const char *szFilename;
	char *szCopyId;
	int bResult;
	const char *szRefId = msg_GetRefToMessageId(msg);
	const char *szConvId = msg_GetConversationId(msg, NULL);
	const char *szReply = NULL;

	// We don't return application level acknowledgements to the application (1.79)
	if (!strcasecmp(msg_GetInteractionId(msg), "Acknowledgment") || !strcasecmp(msg_GetInteractionId(msg), "Acknowledgement"))
		return 0;

	if (!szRefId) return 0;						// This isn't referring to any previous message

// TODO: XYZZY Uncomment this and complete other TODO: XYZZY so that messages can be transferred by shm
#if 0
	szReply = ipc_SendDaemon(ipc_whohas | ipc_waitreply, -1, szRefId);
	if (szReply) {
Log("MAIN: Daemon says '%s'", szReply);
		const char *szType = strtok((char*)szReply, ":");
		const char *szPid = strtok(NULL, ":");
		const char *szId = strtok(NULL, ":");
		int nPid = atoi(szPid);

		if (strcmp(szId, szRefId) || atoi(szType) != ipc_whohas) {		// Check sanity of return message
			Log("!!! I asked the daemon who had %s and it said '%s:%s:%s'!", szType, szPid, szId);
		} else {
			Log("Sending to %d, '%s'", nPid, szRefId);					// We've got the pid of the handler
		}
		szDelete(szReply);
		return nPid;
	}
#endif

	szCopyId = strdup(szRefId);
	strupr(szCopyId);
	szFilename = hprintf(NULL, "%s/%s.waiting", szWaitingDir, szCopyId);
	bResult=!access(szFilename, 0);
	szDelete(szFilename);
	szDelete(szCopyId);

	if (!bResult && szConvId) {					// 22272 Check conversation ID
		szCopyId = strdup(szConvId);
		strupr(szCopyId);
		szFilename = hprintf(NULL, "%s/%s.waiting", szWaitingDir, szCopyId);
		bResult=!access(szFilename, 0);
		szDelete(szFilename);
		szDelete(szCopyId);
	}

	return bResult;
}

int DeliverMessage(MSG *msg, const char **pReply)
// Drop message into receiver's lap or deliver to waiting synchronous local connection
// If pReply is non-NULL, it is returned with either NULL or a heap-based string of returned string
// Returns	1	Delivered to waiting synchoronous process
//			2	Delivered to a 'receive dir'
//			3	Delivered directly to handler
//			0	Failed to deliver - handler not found?
{
	int nTaken=0;										// 1 if taken by a synchronous process
	int nResult = 0;
	int nWaiting;

	if (pReply) *pReply = NULL;

	nWaiting = IsWaitingFor(msg);

	if (nWaiting == 1) {								// Old waiting method with .waiting files
		int i;

		const char *szRefId = msg_GetRefToMessageId(msg);
		char *szCopyId = strdup(szRefId);
		strupr(szCopyId);
		const char *szTmp=hprintf(NULL, "%s/%s.tmp", szWaitingDir, szCopyId);
		const char *szMsg=hprintf(NULL, "%s/%s.msg", szWaitingDir, szCopyId);

		msg_Save(msg, szTmp);
		RogRename(szTmp, szMsg);

		const char *szConvId = msg_GetConversationId(msg, NULL);
		char *szCopyConvId = strdup(szConvId);
		strupr(szCopyConvId);
		const char *szTmpConv=hprintf(NULL, "%s/%s.tmp", szWaitingDir, szCopyConvId);
		const char *szMsgConv=hprintf(NULL, "%s/%s.msg", szWaitingDir, szCopyConvId);

		msg_Save(msg, szTmpConv);
		RogRename(szTmpConv, szMsgConv);

		for (i=0;i<10;i++) {							// Wait 10 seconds
			if (access(szMsg, 0)) {						// It's been taken up - hallelulah
				nTaken=1;
				break;
			}
			if (access(szMsgConv, 0)) {					// It's been taken up - hallelulah
				nTaken=2;
				break;
			}
			sleep(1);
		}

		szDelete(szTmp);
		szDelete(szMsg);
		szDelete(szTmpConv);
		szDelete(szMsgConv);

		if (nTaken) {									// Celebrate (Easter eggs and such)
			Log("Picked up by sync process with %s", nTaken == 1 ? szCopyId : szCopyConvId);
			nResult = 1;
		} else {										// Need to tidy up
			Log("%s and %s waited for, not picked up", szCopyId, szCopyConvId);

			const char *szWaiting=hprintf(NULL, "%s/%s.waiting", szWaitingDir, szCopyId);
			unlink(szWaiting);
			szDelete(szWaiting);

			szWaiting=hprintf(NULL, "%s/%s.waiting", szWaitingDir, szCopyConvId);
			unlink(szWaiting);
			szDelete(szWaiting);
		}
#if 0				// Never happens while we're not using ipc IPCREMOVED
	} else if (nWaiting > 1) {							// ipc waiting method
		rogxml *rx = msg_GetXML(msg);
		const char *szXml = rogxml_ToText(rx);

		ipc_SendMemory(nWaiting, "ack", -1, szXml);		// Send this message to the one waiting for it
		szDelete(szXml);
		nResult = 1;
#endif
	}

	if (!nTaken) {										// Not received by a waiting process
		const char *szDir = msg_DropAsTmpIn(msg, NULL);	// 'Delivers' incoming message to 'szInDir'
		if (szDir) {
			const char *szReply = ReceiveMessage(szDir);
			if (pReply) *pReply = szReply;
			else szDelete(szReply);						// Nobody wants it!
//			szDir = indir_Enable(szDir);				// Enables the 'indir' for processing by 'doin'
			szDelete(szDir);
			nResult = 2;
		} else {
			nResult = 0;
		}
	}

	return nResult;
}

int MarkAcknowledged(const char *szId)
// Mark the message as having been delivered so we don't try again
{
	const char *szSrc;
	int nErr;
	char *szCopyId = strdup(szId);

	strupr(szCopyId);
	szSrc=hprintf(NULL, "%s/%s.msg", szUnackedDir, szCopyId);
	nErr=MoveFile(szSrc, szAckedDir, NULL);
	if (nErr && nErr != ENOENT) {				// Don't complain if it wasn't there, it's already been moved
		Log("Error %d moving %s to %s", nErr, szSrc, szAckedDir);
	} else {									// We need to create a link with the internal ID
		const char *szDest = hprintf(NULL, "%s/%s.msg", szAckedDir, szCopyId);	// Where the file has just moved to
		MSG *msg = msg_Load(szDest);
		Log("Moved message wrapped as %s to acknowledged", szCopyId);
		if (msg) {
			const char *szInternalId = msg_GetInternalId(msg);
			if (szInternalId) {
				char *szCopyInternalId = strdup(szInternalId);
				strupr(szCopyInternalId);
				const char *szLink = hprintf(NULL, "%s/%s.msg", szAckedDir, szCopyInternalId);
				if (symlink(szDest, szLink) && errno != EEXIST) {	// An error other than 'already there'
					Log("Error %d creating link from %s to %s", errno, szDest, szLink);
				}
				szDelete(szLink);
				szDelete(szCopyInternalId);
			}
		} else {
			Log("Could not load %s to create symlink", szSrc);
		}
		msg_Delete(msg);
		szDelete(szDest);
	}

	szDelete(szSrc);
	szDelete(szCopyId);

	return nErr;
}

int MarkFailed(const char *szId)
// Mark the message as having failed so that we can tidy up manually if necessary
{
	const char *szSrc;
	int nErr;
	char *szCopyId = strdup(szId);

	strupr(szCopyId);
	szSrc=hprintf(NULL, "%s/%s.msg", szUnackedDir, szCopyId);
	nErr=MoveFile(szSrc, szFailedDir, NULL);
	if (nErr) {
		Log("Error %d moving %s to %s", nErr, szSrc, szFailedDir);
		if (unlink(szSrc))
			Log("Error %d deleting %s", errno, szSrc);
	}

	szDelete(szSrc);
	szDelete(szCopyId);

	return nErr;
}

MSG *ReceiveAsyncMessage(MSG *msg)
// Deals with an incoming async message and returns any acknowledgement message that needs to be sent back
{
	MSG *msgAck = NULL;
	rogxml *rx;
	int bDealtWith = 0;
	const char *szDuplicateCheckId = strdup(msg_GetMessageId(msg));

	msg_Unwrap(msg);
	msg_ExtractMessageInfo(msg);
	rx = msg_GetXML(msg);

//Log("IsAck = %d, HasPayload = %d", msg_IsAck(msg), msg_HasPayload(msg));

	if (msg_IsAck(msg)) {								// ASYNC ACK MESSAGE
		const char *szReferring = msg_GetRefToMessageId(msg);

		Log("Received Ack for %s", szReferring);
		MarkAcknowledged(szReferring);
		bDealtWith = 1;
	}

	// The documentation says that the message may be EITHER an ack OR a message but the
	// structure of the message would allow both so I don't see any harm in handling this

	if (msg_HasPayload(msg)) {							// Has a payload that we'd better deliver
		const char *szId = WrapperGetMessageId(rx);

		if (!szId) {
			Log("Received message with no ID");
		} else if (HaveReceived(szDuplicateCheckId)) {						// Duplicate
			msgAck = MakeDuplicateAcknowledgementMessage(msg);

			Log("Duplicate of %s", szDuplicateCheckId);
		} else {										// Nice, normal, unique message for delivery
			if (DeliverMessage(msg, NULL)) {			// Deliver to application (or waiting sync process)
				Log("Delivered message %s", msg_GetMessageId(msg));

				// TODO: Should only be stored if 'DuplicateElimination' is set
				// I don't think this is true as handlers sometimes need to find it
				StoreMessage(msg_GetInternalId(msg), msg);			// Remember it for doin (22792 added szid parameter)
				RememberMessageId(szDuplicateCheckId);				// This is for duplicate checking...!

				if (msg_AckRequested(msg)) {
					msgAck = MakeAcknowledgementMessage(msg);
				}
			} else {
				msgAck = MakeNegAcknowledgementMessage(msg, NULL, 100, "Could not deliver message");

				Log("Failed to deliver %s", msg_GetMessageId(msg));
			}
		}
		bDealtWith = 1;
	}

	if( strcmp(msg_GetInteractionId(msg), "Ping" ) == 0 ) {
		msgAck = MakePongMessage(msg);
		Log("Received a Ping message");
		bDealtWith = 1;
	}

	if (!bDealtWith) {
		Log("Received a non-Payload, non-Ack message!");
		msgAck = MakeNegAcknowledgementMessage(msg, NULL, 102, "I do not recognise this message type");
	}

	szDelete(szDuplicateCheckId);

	return msgAck;
}

const char *IsRunnable(const char *szPath)
// Tries different runnable versions of path (by using it plain, with .pl, with .php etc.)
// and returns an effectively static copy of the working path of NULL if nothing worked.
{
	static const char *szResult = NULL;
	static const char *suffixes[] = {"",".pl",".php",".sh",NULL};
	const char **suffix;
	struct stat st;

	for (suffix=suffixes;*suffix;suffix++) {
		szDelete(szResult);

		szResult = hprintf(NULL, "%s%s", szPath, *suffix);
		if (!stat(szResult, &st)) {
			if ((st.st_mode & S_IFREG) && !access(szResult, 1)) {		// Plain file and runnable
				return szResult;
			}
		}
	}
	szDelete(szResult);

	return NULL;
}

int DealWithPostedForm(BIO *io, const char *szURI, MIME *mime)
// Look for an executable.  If szURI = /abc/def then look for docs/abc/def, docs/abc/def.pl, docs/abc/def.php
// docs/abc/def.sh, docs/abc, docs/abc.pl, docs/abc.php, docs/abc.sh
{
	char *szPath=strdup(szURI);
	const char *szBaseDir = GetBaseDir();			// Should always be /usr/mt/mmts
	const char *szExecutable = NULL;				// The candidate executable to handle this request
	const char *szPathInfo = NULL;					// The PATH_INFO value to pass on
	FILE *pipefp=NULL;								// Pipe pointer if we successfully run a command

	Log("Dealing with posted form (%s)", szURI);
	while (*szPath) {
		const char *szExec=hprintf(NULL, "%s/docs%s", szBaseDir, szPath);
		szExecutable = IsRunnable(szExec);

		if (szExecutable) {
			szPathInfo=szURI+strlen(szPath);
			break;
		}
		char *chp=strrchr(szPath, '/');
		if (!chp) break;
		*chp='\0';
	}

	if (szExecutable) {
		const char *szStdin=NULL;						// We place the mime content in here and '<' it to the command
		const char *szBodyOnHeap = NULL;				// Where the message body is on the heap, it's referenced here
		const char *szEnv=NULL;							// Used to store PATH_INFO env string
		const char *szCommandLine=NULL;					// Command line including any "/usr/bin/perl " etc. prefix

		EnvSet("REQUEST_METHOD","POST");
		EnvSet("PATH_INFO",szPathInfo);

		FILE *fp=fopen(szExecutable, "r");				// Check for a #!/usr/bin/perl or similar line in the file
		if (fp) {
			char buf[2];
			int got=fread(buf,1,sizeof(buf),fp);
			if (got == 2 && buf[0]=='#' && buf[1]=='!') {
				szCommandLine=hprintf(NULL, "%s %s", SkipSpaces(ReadLine(fp)), szExecutable);
			}
			fclose(fp);
		}
		if (!szCommandLine) szCommandLine=strdup(szExecutable);

		int count = mime_GetBodyCount(mime);
		const char *szBody=NULL;
		char *szTempDir = NULL;
		if (!count) {
			szBody=mime_GetBodyText(mime);
		} else {									// We must be multi-part so act differently...
			szTempDir = tempnam(NULL, "POST_");
			MkDir(szTempDir);
			int part;

			for (part=1;part<=count;part++) {
				MIME *m = mime_GetBodyPart(mime, part);
				const char *szPart = mime_GetBodyText(m);
				int nPartLen = mime_GetBodyLength(m);
				const char *szName = UriEncode(mime_GetParameter(m, "Content-Disposition", "name"));
				const char *szFilename = mime_GetParameter(m, "Content-Disposition", "filename");
				const char *szValue = NULL;

				if (szFilename) {
					const char *szSlash = strrchr(szFilename, '/');
					if (szSlash) szFilename=szSlash+1;
					const char *szBackSlash = strrchr(szFilename, '\\');
					if (szBackSlash) szFilename=szBackSlash+1;
					const char *szPartFile = hprintf(NULL, "%s/%s", szTempDir, szFilename);
					FILE *fp=fopen(szPartFile, "w");

					if (fp) {
						fwrite(szPart, nPartLen, 1, fp);
						fclose(fp);
					}
					szValue = UriEncode(szPartFile);
					szDelete(szPartFile);
				} else {
					szValue = UriEncode(szPart);
				}
				if (szBody) szBody = hprintf(szBody, "&");
				szBody = hprintf(szBody, "%s=%s", szName, szValue);
				szDelete(szName);
				szDelete(szValue);
				szBodyOnHeap = szBody;						// So it gets itself freed later
			}
		}

		szStdin = tempnam(NULL, "MMTS-");
		fp=fopen(szStdin, "w");
		if (fp) {
			fwrite(szBody, strlen(szBody), 1, fp);
			fclose(fp);
		}
		szCommandLine=hprintf(szCommandLine, " < '%s'", szStdin);
		pipefp=popen(szCommandLine, "r");
		Log("Executing: %s", szCommandLine);

		if (pipefp) {
			int got;
			char buf[10240];

			MyBIO_puts(io, "HTTP/1.1 200 OK\r\n");
			while ((got=fread(buf, 1, sizeof(buf), pipefp))) {
				MyBIO_write(io, buf, got);
			}
			pclose(pipefp);
		} else {
			MyBIO_puts(io, "HTTP/1.1 403 Forbidden\r\n");
			MyBIO_puts(io, "Server: Microtest\r\n");
			MyBIO_puts(io, "\r\n");
			MyBIO_puts(io, "I was unable to execute the form handler for you - sorry.");
			Log("Failed to execute: %s", szCommandLine);
		}

		EnvSet("REQUEST_METHOD",NULL);
		EnvSet("PATH_INFO",NULL);

		if (szTempDir) {
			const char *szCommand = hprintf(NULL, "rm -r %s", szTempDir);
			system(szCommand);
			szDelete(szCommand);
		}
		unlink(szStdin);

		szDelete(szTempDir);
		szDelete(szStdin);
		szDelete(szBodyOnHeap);
		szDelete(szEnv);
		szDelete(szCommandLine);
	} else {
		Log("Failed to run '%s'", szURI);
		MyBIO_puts(io, "HTTP/1.1 403 Forbidden\r\n");
		MyBIO_puts(io, "Server: Microtest\r\n");
		MyBIO_puts(io, "\r\n");
		MyBIO_puts(io, "The form handler was not found - really sorry.");
	}

	szDelete(szPath);

	return 0;
}

int _nHttpStatusCode = 0;
const char *_szHttpStatusText = NULL;

void http_ParseReceivedHeader(const char *szHTTP)
// Parses an HTTP header line, extracting the code and message so that the following
// functions can return them.
// A duff string will set us up with '0, NULL'.
{
	const char *chp;

	chp=strchr(szHTTP, ' ');
	if (chp)
		_nHttpStatusCode = atoi(++chp);
	else
		_nHttpStatusCode = 0;				// Something screwy

	if (chp) chp=strchr(chp, ' ');
	if (chp) {								// Otherwise NULL, which works out fine
		char *chp2;

		++chp;
		chp2=strchr(chp, '\n'); if (chp2) *(char*)chp2='\0';
		chp2=strchr(chp, '\r'); if (chp2) *(char*)chp2='\0';
	}

	strset(&_szHttpStatusText, chp);
}

int http_GetLastStatusCode()				{ return _nHttpStatusCode; }
const char *http_GetLastStatusText()		{ return _szHttpStatusText; }

MIME *HttpRequest(const char *szAddr, int nPort, const char *szHeader, MIME *request, const char **pHeader)
// Send a request to a server and return a result.
// The header returned by the remote server will be placed in *pHeader
{
	BIO *io = BuildConnection(szAddr, nPort);
	if (!io) {
		if (pHeader) *pHeader = strdup(HttpHeader(503));
		MIME *m = mime_New(NULL);
		mime_SetHeader(m, "Server","Microtest");
		const char *szContent = hprintf(NULL, "Server %s is not available", szAddr);
		mime_SetContentHeap(m, -1, szContent);
		szDelete(szContent);

		return m;
	}

	// Render the message to send and set the content length correctly
	mime_SetHeader(request, "Host", szAddr);
	const char *szBody = mime_RenderHeap(request);
	const char *chp=strstr(szBody, "\r\n\r\n");
	if (chp) {
		char buf[20];

		sprintf(buf, "%u", (unsigned int)strlen(chp+4));
		mime_SetHeader(request, "Content-Length", buf);
		szDelete(szBody);
		szBody = mime_RenderHeap(request);
	}

//Log("Sending to %s:%s", szAddr, nPort);
//Log("Sending header %s", szHeader);
//Log("Sending %s", szBody);

	MyBIO_puts(io, szHeader);
	MyBIO_puts(io, "\r\n");
	MyBIO_puts(io, szBody);
	MyBIO_flush(io);

	// Get the response
	char buf[200];
	MyBIO_gets(io, buf, sizeof(buf));
	MIME *m=GetIOMIME(io);
	if (mime_GetLastError()) {
		Log("Mime error %d - '%s'", mime_GetLastError(), mime_GetLastErrorStr());
		mime_Dump(m);
	}

	// The content we receive may be chunked.  We effectively de-chunk it so we don't want that propogating
	if (mime_GetHeader(m, "Transfer-Encoding"))
		mime_SetHeader(m, "Transfer-Encoding","identity");

	if (pHeader) *pHeader = strdup(buf);

	return m;
}

const char *AdjustHttpReferences(const char *szBody, const char *szReference, const char *szServer, const char *szPath)
// szReference will be 'href' or 'src' for example
// Let's assume szServer = "https://server", szPath = "/path"	(/ at start of path and end of server are optional)
// Absolute: /abs                -> https://server/abs
// Relative: rel                 -> https://server/path/rel
// Remote:   http://other/page   -> http://other/page
// Returns	szBody		Nothing was changed
//			const char*	A new version of szBody on the heap (old szBody is gone)
{
	if (!szBody || !szReference || !szServer || !szPath) return szBody;
	int len=strlen(szReference);

	if (*szPath == '/') szPath++;
	char lastInServer = *szServer ? szServer[strlen(szServer)-1] : '\0';

	const char *szAbsPrefix = hprintf(NULL, "%.*s", strlen(szServer) - (lastInServer == '/'), szServer);
	const char *szRelPrefix = hprintf(NULL, "%s/%s%s", szAbsPrefix, szPath, *szPath ? "/" : "");

//Log("ADJUST: (%d, \"%s\", \"%s\", \"%s\")", strlen(szBody), szReference, szServer, szPath);
//Log("Abs = '%s'", szAbsPrefix);
//Log("Rel = '%s'", szRelPrefix);
	const char *href = (const char *)strcasestr(szBody, szReference);
	while (href) {
		char *cp = SkipSpaces(href+len);
		if (*cp == '=') {
			cp = SkipSpaces(cp+1);
			if (*cp == '"' || *cp == '\'') cp++;
			// Now at the start of the target so it might be xyzzy://, /xyzzy or xyzzy
			const char *chp = cp;
			while (isalnum(*chp)) chp++;
			if (*chp != ':'								// http:, ftp: etc. we leave alone, also # references
					&& *cp != '#'
					&& *cp != '"' && *cp != '\'') {
				const char *szInsert;

				if (*cp == '/') {
					szInsert = szAbsPrefix;
				} else {
					szInsert = szRelPrefix;
				}

//Log("Inserting '%s' before '%.20s", szInsert, cp);
//Log("BEFORE: %s", szBody);
				int start = cp-szBody;				// How much before we want
				int rest = strlen(cp)+1;			// How much there is after (include '\0')
				char *szNewBody = malloc(start+strlen(szInsert)+rest);	// A whole new body
				memcpy(szNewBody, szBody, start);					// Copy in the before
				strcpy(szNewBody+start, szInsert);					// Copy in the prefix
				memcpy(szNewBody+start+strlen(szInsert), cp, rest);	// Copy in the after
				szDelete(szBody);									// Lose the old body
				szBody=szNewBody;									// Replace it with the new one
				href=szBody+start;					// Adjust the 'cursor' to approximately the right place
//Log("AFTER: %s", szBody);
			}
		}
		href=(const char *)strcasestr(href+1, szReference);
	}

	szDelete(szAbsPrefix);
	szDelete(szRelPrefix);

	return szBody;
}

void SendSoapFault(BIO *io, int nHttpCode, int version, const char *szCode, const char *szSubcode, const char *szReason, const char *szDetail, rogxml *rxDetail, const char *szAction, const char *szTo, const char *szFrom, const char *szElement)
{
	const char *text = HttpSoapFault(nHttpCode, version, szCode, szSubcode, szReason, szDetail, rxDetail, szAction, szTo, szFrom, szElement);

	if (text) {
		Log("sending SOAP fault (len=%d)", strlen(text));
		MyBIO_puts(io, text);
		szDelete(text);
	} else {
		Log("Didn't get anything to send from HttpSoapFault()");
	}
}

const char *CheckSoapMustUnderstand(rogxml *rxHeader)
// Checks for any mustUnderstand attributes in the head and fails in some way if we don't understand any.
// Returns	NULL - All is good
//			const char *	Local name of thing we don't understand (namespace will always be SOAP equivalent)
{
	static const char *szzUnderstood = "MessageID\0Action\0To\0From\0Security\0FaultTo\0ReplyTo\0MessageHeader\0AckRequested\0SyncReply\0Acknowledgment\0";
	rogxml *rx;

	for (rx = rogxml_FindFirstChild(rxHeader); rx; rx=rogxml_FindNextSibling(rx)) {
		rogxml *rxAttr;

		for (rxAttr = rogxml_FindFirstAttribute(rx); rxAttr; rxAttr=rogxml_FindNextSibling(rxAttr)) {
			if (!strcmp(rogxml_GetLocalName(rxAttr), "mustUnderstand")) {
				const char *szValue = rogxml_GetValue(rxAttr);

				if (szValue && atoi(szValue)) {
					const char *szName = rogxml_GetLocalName(rx);
					const char *szz;

					for (szz=szzUnderstood;*szz;szz=szz_Next(szz)) {
						if (!strcmp(szz, szName)) break;
					}

					if (!*szz) {
						return szName;
					}
				}
			}
		}
	}

	return NULL;
}

int Proxy(BIO *io, const char *szAddr, int nPort, const char *type, const char *szServer, const char *szURI)
// szAddr and nPort are where we are proxying to
// Type would be GET or POST
// szServer is the part that came before it and is used if we need to re-write
// szURI is the place we want to go
// http output.
{
	MIME *m;

	if (!io) return 1;
	if (!type) type="GET";
	if (!szURI || !*szURI) szURI="/";

	Log("Acting as proxy to %s:%d for a %s", szAddr, nPort, type);

	// Get what message we need
	if (!strcmp(type, "GET")) {
		m=GetIOMIMEHeader(io);
	} else {
		m=GetIOMIME(io);
	}
	if (!m) Fatal("Malformed MIME received for proxied server (error %d - %s)", mime_GetLastError(), mime_GetLastErrorStr());

	const char *szHeader = hprintf(NULL, "%s %s HTTP/1.1", type, szURI);

	mime_SetHeader(m, "Host", szAddr);
	const char *szReturnedHeader;
	Log("Sending stuff to proxied server at %s:%d", szAddr, nPort);
	Log("Sending: %s", szHeader);
	MIME *pm = HttpRequest(szAddr, nPort, szHeader, m, &szReturnedHeader);

	if (mime_GetLastError()) {
		Log("Malformed MIME received (error %d - %s)", mime_GetLastError(), mime_GetLastErrorStr());
	}

	http_ParseReceivedHeader(szReturnedHeader);
	if (http_GetLastStatusCode() == 301) {			// We're being redirected
		const char *szLocation = mime_GetHeader(pm, "Location");

		if (szLocation) {
			const char *szPrevLocation = hprintf(NULL, "%s%s", szAddr, szURI);
			const char *szRedirProtocol;
			const char *szRedirAddr;
			int nRedirPort = 80;
			const char *szRedirUri;

			SplitEndpoint(szLocation, &szRedirProtocol, &szRedirAddr, &nRedirPort, &szRedirUri, 0);
			if (nRedirPort == 0) nRedirPort=!strcasecmp(szRedirProtocol, "https") ? 443 : 80;	// It wasn't specified
//Log("Split %s to (%s,%s,%d,%s)", szLocation, szRedirProtocol, szRedirAddr, nRedirPort, szRedirUri);
			if (!strcasecmp(szRedirProtocol, "http") && strcmp(szRedirAddr, szAddr)) {
				const char *szRedirHeader = hprintf(NULL, "%s %s HTTP/1.1", type, szRedirUri);
				mime_SetHeader(m, "Host",szRedirAddr);
				mime_Delete(pm);
				Log("Redirected - sending to %s:%d instead", szRedirAddr, nRedirPort);
				pm = HttpRequest(szRedirAddr, nRedirPort, szRedirHeader, m, &szReturnedHeader);
			}
			szDelete(szPrevLocation);
		}
	}

	int len;
	const char *szBody = mime_RenderContentBinary(pm, &len);
//Log("Body = '%s'", szBody);

	char buf[20];

	const char *szContentType = mime_GetHeader(pm, "Content-Type");
	if (szContentType && !strcasecmp(szContentType, "text/html")) {		// Need to translate hrefs...
		char *szUriBit = strdup(szURI + (*szURI == '/'));
		char *mark = strchr(szUriBit, '?');
		if (mark) *mark='\0';
		char *slash = strrchr(szUriBit, '/');
		if (slash) *slash='\0';

		szBody = AdjustHttpReferences(szBody, "href", szServer, szUriBit);
		szBody = AdjustHttpReferences(szBody, "src", szServer, szUriBit);
		szBody = AdjustHttpReferences(szBody, "action", szServer, szUriBit);		// Forms
		szDelete(szUriBit);

		len = strlen(szBody);
	}
	sprintf(buf, "%d", len);
	mime_SetHeader(pm, "Content-Length", buf);

	Log("Returning result to caller '%s' (+%d byte body)", szReturnedHeader, len);
	szHeader = mime_RenderHeaderHeap(pm);

	MyBIO_puts(io, szReturnedHeader);
	MyBIO_puts(io, "\r\n");
	MyBIO_puts(io, szHeader);
	MyBIO_puts(io, "\r\n");
	MyBIO_write(io, szBody, len);

	szDelete(szHeader);
	szDelete(szBody);
	mime_Delete(pm);

	return 0;
}

int DealWithGET(BIO *io, const char *szURI)
{
	Log("HTTP GET %s", szURI);

	if (!strncasecmp(szURI, "/spider", 7))			return Proxy(io, "127.0.0.1", 4511, "GET", "/spider", szURI+7);
	if (!strncasecmp(szURI, "/local", 6))			return Proxy(io, "127.0.0.1", 80, "GET", "/local", szURI+6);
	if (!strncasecmp(szURI, "/gpesmmts", 9))		return Proxy(io, "gpes", 10024, "GET", "/gpesmmts", szURI+9);
	if (!strncasecmp(szURI, "/gpesspider", 11))	return Proxy(io, "gpes", 4511, "GET", "/gpesspider", szURI+11);
	if (!strncasecmp(szURI, "/gpes", 5))			return Proxy(io, "gpes", 80, "GET", "/gpes", szURI+5);

	MIME *mime=GetIOMIMEHeader(io);
//	const char *szStr=mime_RenderHeap(mime);
	char *szParams = NULL;
	const char *szAddr = NULL;
	const char *szParam = NULL;
	const char *szValue = NULL;
	const char *chp;
	const char **szNames = NULL;
	const char **szValues = NULL;
	int nParams=0;

	SetIncomingHeaderFromMime(mime);

	NoteInhibit(1);

	// This is a web request from a workstation so we want to use the environment configured for messages
	// coming from that workstation, not those coming in from the spine.
	GetEnvironment(_szSenderIp, _nIncomingPort);

	chp=strchr(szURI, '?');								// Look for parameters...
	if (chp) {
		char *pA;
		char *pEq;

		szAddr=strnappend(NULL, szURI, chp-szURI);
		szParams=strdup(chp+1);
		chp=strchr(szParams, '=');
		if (chp) {
			szParam=strnappend(NULL, szParams, chp-szParams);
			szValue=strdup(chp+1);
		}

		for (;;) {
			pA = strchr(szParams, '&');
			if (pA) *pA='\0';

			pEq=strchr(szParams, '=');
			if (pEq) {
				*pEq='\0';
				if (nParams) {
					RENEW(szNames, const char *, nParams+1);
					RENEW(szValues, const char *, nParams+1);
				} else {
					szNames=NEW(const char*, 1);
					szValues=NEW(const char*, 1);
				}
				szNames[nParams]=szParams;
				szValues[nParams]=UriDecode(pEq+1);
				nParams++;
			}
			if (!pA) break;
			szParams=pA+1;
		}

	} else {
		szAddr=strdup(szURI);
		szParams=NULL;
	}

	if (!strcasecmp(szAddr, "/") || !strcasecmp(szAddr, "/index.html")) {
		const char *szMime;
		char szHostname[50];
		int nSecs=time(NULL)-_nStartTime;
		int nHours=nSecs/3600;
		int nMins=nSecs/60%60;
		HBUF *h = hbuf_New();

		nSecs %= 60;

		gethostname(szHostname, sizeof(szHostname));

		AddHtmlHeader(h);

		bprint(h, "<table border=0 bgcolor=#ddddff>");
		bprintf(h, "<tr><td width=150>Uptime<td bgcolor=#eeeeff>%d:%02d:%02d", nHours, nMins, nSecs);
		bprintf(h, "<tr><td width=150>Server<td bgcolor=#eeeeff>%s on %s", szHostname, _szIncomingIp);
		bprintf(h, "<tr><td>Client (you)<td bgcolor=#eeeeff>%s:%d", PeerIp(1), PeerPort(1));
		if (szEnvironment) {
			bprintf(h, "<tr><td>Environment<td bgcolor=#eeeeff><a href=\"/setenv\">%s</a>", szEnvironment);
		} else {
			bprintf(h, "<tr><td>Environment<td bgcolor=#eeeeff>%s", "Unknown");
		}
		bprintf(h, "<tr><td>Server parent process<td bgcolor=#eeeeff>%d", getppid());
		bprintf(h, "<tr><td>Server child process<td bgcolor=#eeeeff>%d", getpid());
		bprintf(h, "<tr><td>Compiled<td bgcolor=#eeeeff>%s at %s", __DATE__, __TIME__);
//		bprintf(h, "<tr><td>Application port<td bgcolor=#eeeeff>%d", nListenPort_int);
//		bprintf(h, "<tr><td>TLS/SSL port<td bgcolor=#eeeeff>%d", nListenPort_ext);
//		bprintf(h, "<tr><td>Connections on application port<td bgcolor=#eeeeff>%d", _nInternalCount);
//		bprintf(h, "<tr><td>Connections on TLS/SSL port<td bgcolor=#eeeeff>%d", _nExternalCount);
		bprintf(h, "<tr><td>Files from drop directory<td bgcolor=#eeeeff>%d", _nDroppedCount);
		bprintf(h, "<tr><td>Total connections processed<td bgcolor=#eeeeff>%d (<a href=/status>click for details</a>)", _nTotalConnections);
		bprint(h, "</table>");

		bprint(h, "<hr>");
		bprint(h, "Your request was:<xmp>\r\n");
		szMime=mime_RenderHeap(mime);
		bprint(h, szMime);
		bprint(h, "</xmp>\r\n");
		szDelete(szMime);

		SendHttpBuffer(io, 200, NULL, h);
	} else if (!strcasecmp(szAddr, "/favicon.ico")) {
		const char *szIconFile = hprintf(NULL, "%s/docs/favicon.ico", GetBaseDir());
		FILE *fp=fopen(szIconFile, "r");

		if (fp) {
			char buf[1024];
			int got;

			SendHttpHeader(io, 200, "image/x-icon");
			while ((got=fread(buf, 1, sizeof(buf), fp)))
				MyBIO_write(io, buf, got);
			fclose(fp);
			Log("FAVICON %s", szIconFile);
		}
		szDelete(szIconFile);
	} else if (!strncasecmp(szAddr, "/file/", 6) || !strcasecmp(szAddr, "/file")) {
		const char *szPath = hprintf(NULL, "%s%s", szAddr+5, szAddr[strlen(szAddr)-1] == '/' ? "" : "/");

		struct stat st;
		int err = stat(szPath, &st);

		if (err) {
			MyBIO_puts(io, HttpHeader(404));		// Not found
			return 0;
		}

		if (st.st_mode & S_IFDIR) {
			struct dirent *d;
			DIR *dir=opendir(szPath);
			SSET *names = sset_New();
			sset_Sort(names, strcmp);

			if (dir) {
				while ((d=readdir(dir))) {
					if (*d->d_name == '.') continue;		// Skip '.' files
					sset_Add(names, d->d_name);
				}
				closedir(dir);
			}

			SendHttpHeader(io, 200, "text/html");
			SendHtmlHeader(io);

			BIO_putf(io, "<h1>Listing of ");
			BIO_putf(io, "<a href=\"/file/\">/</a> ");
			char *pathCopy = strdup(szPath);
			char *component;
			char *pathSoFar = NULL;
			for (component = strtok(pathCopy, "/"); component; component=strtok(NULL, "/")) {
				if (pathSoFar) BIO_putf(io, " / ");

				pathSoFar = hprintf(pathSoFar, "/%s", component);

				BIO_putf(io, "<a href=\"/file%s\">%s</a>", pathSoFar, component);
			}
			szDelete(pathCopy);
			BIO_putf(io, " (%d)", sset_Count(names));
			BIO_putf(io, "</h1>");

			BIO_putf(io, "<style>\n");
			BIO_putf(io, "td.fixed {font-family:monospace}");
			BIO_putf(io, "td.number {text-align:right}");
			BIO_putf(io, "</style>\n");
			BIO_putf(io, "<table>\n");
			const char *szName=NULL;
			while (sset_GetNextEntry(names, &szName)) {
				const char *szFilename = hprintf(NULL, "%s/%s", szPath, szName);
				struct stat64 st;
				char firstChar = '-';

				stat64(szFilename, &st);
				if (S_ISDIR(st.st_mode)) firstChar = 'd';
				if (S_ISFIFO(st.st_mode)) firstChar = 'p';
				if (S_ISCHR(st.st_mode)) firstChar = 'c';
				if (S_ISBLK(st.st_mode)) firstChar = 'b';
				if (S_ISSOCK(st.st_mode)) firstChar = 's';
				if (S_ISLNK(st.st_mode)) firstChar = 'l';
				BIO_putf(io, "<tr>");
				BIO_putf(io, "<td class=\"fixed\">%c%c%c%c%c%c%c%c%c%c</td>",
						firstChar,
						st.st_mode & 0400 ? 'r' : '-',
						st.st_mode & 0200 ? 'w' : '-',
						st.st_mode & 0100 ? 'x' : '-',
						st.st_mode & 0040 ? 'r' : '-',
						st.st_mode & 0020 ? 'w' : '-',
						st.st_mode & 0010 ? 'x' : '-',
						st.st_mode & 0004 ? 'r' : '-',
						st.st_mode & 0002 ? 'w' : '-',
						st.st_mode & 0001 ? 'x' : '-');
				BIO_putf(io, "<td class=\"number\">%d</td>", st.st_nlink);
				BIO_putf(io, "<td>%s</td>", UserName(st.st_uid));
				BIO_putf(io, "<td>%s</td>", GroupName(st.st_gid));
				if (firstChar == 'c' || firstChar == 'b') {
					int major = st.st_rdev >> 18;
					int minor = st.st_rdev & 0x3ffff;
					BIO_putf(io, "<td class=\"number\">%d, %d</td>", major, minor);
				} else {
					BIO_putf(io, "<td class=\"number\">%s</td>", CommaNum(st.st_size));
				}
				BIO_putf(io, "<td class=\"fixed\">%s</td>", FileDateString(st.st_mtime));
				BIO_putf(io, "<td><a href=\"/file%s%s\">%s</a></td>", szPath, szName, szName);
				BIO_putf(io, "</tr>\n");
				szDelete(szFilename);
			}
			BIO_putf(io, "</table>\n");
			sset_Delete(names);
		} else if (st.st_mode & S_IFREG) {
			DeliverFile(io, szPath, NULL);
		} else {										// Can only deliver files and directories
			HBUF *h = hbuf_New();
			bprint(h, "You can't touch this...");
			SendHttpBuffer(io, 403, NULL, h);
			return 0;
		}

	} else if (!strcasecmp(szAddr, "/setenv")) {
		struct dirent *d;
		DIR *dir;
		const char *szMessage = NULL;

		if (szParam && !strcasecmp(szParam, "env")) {
			if (IsValidEnvironment(szValue)) {
				RememberEnvironment(PeerIp(1), 0, szValue, 1);
			} else {
				szMessage = hprintf(NULL, "<div style=\"font-size:16pt;background-color:red;color:white\">Attempt to set environment to '%s' failed as it's not recognised</div>", szValue);
			}
		}
//		GetEnvironment(PeerIp(1), 0);
		GetEnvironment(PeerIp(1), _nIncomingPort);

		// These are down here so that the header reflects the new environment if it is changed
		SendHttpHeader(io, 200, NULL);
		SendHtmlHeader(io);
		BIO_putf(io, "<div style=\"font-size:20pt;background-color:yellow;color:black\">Environment: %s (for %s)</div>", szEnvironment, PeerIp(1));
		if (szMessage) MyBIO_puts(io, szMessage);

		dir=opendir("env");

		if (dir) {
			BIO_putf(io, "<hr>Click one of the following to set your environment: ");
			while ((d=readdir(dir))) {
				if (*d->d_name == '.') continue;		// Skip '.' files
				if (IsValidEnvironment(d->d_name)) {
					BIO_putf(io, ", <a href=\"/setenv?env=%s\">%s</a>", d->d_name, d->d_name);
				}
			}
			closedir(dir);
			BIO_putf(io, " or <a href=\"/setenv?env=none\">none</a> (deprecated)");
		}

		dir=opendir(szContractDir);
		if (dir) {
			BIO_putf(io, "<hr>Outward contracts available for %s (in %s) are:<br>", szEnvironment, szContractDir);
			BIO_putf(io, "<table bgcolor=#ffff77 bordercolor=blue border=1 cellspacing=1>\r\n");
			BIO_putf(io, "<tr bgcolor=\"#cccc77\"><th>Interaction<th>Domain<th>Description<th>CpaID<th>Endpoint</tr>\n");
			while ((d=readdir(dir))) {
				const char *szDescription;
				const char *szSection;
				const char *szName = d->d_name;
				contract_t *contract;

				if (*szName == '.') continue;		// Skip '.' files
				szDescription=MessageDescription(szName, &szSection);
				if (!szDescription) szDescription="-";
				if (!szSection) szSection="?";
				contract=contract_New(NULL, szName, NULL);
				BIO_putf(io, "<tr><td>%s</td><td>%s</td><td>%s</td>\n", szName, szSection, szDescription);
				if (contract) {
					BIO_putf(io, "<td>%s<td>%s", contract_GetId(contract), contract_GetEndpoint(contract));
					contract_Delete(contract);
				}
			}
			BIO_putf(io, "</table>");
			closedir(dir);
		}
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

		SendHttpHeader(io, 200, NULL);
		SendHtmlHeader(io);
		int nPeriod=1;

		for (i=0;i<nParams;i++) {
			if (!strcasecmp(szNames[i], "f")) szFilter=szValues[i];
			if (!strcasecmp(szNames[i], "deep")) bDeep=1;
			if (!strcasecmp(szNames[i], "period")) nPeriod=atoi(szValues[i]);
//BIO_putf(io, "('%s'='%s') '%s'<br>", szNames[i], szValues[i], szFilter);
		}

		const char *sel1=(nPeriod==1)?"selected":"";
		const char *sel7=(nPeriod==7)?"selected":"";
		const char *sel31=(nPeriod==31)?"selected":"";
		const char *sel91=(nPeriod==91)?"selected":"";


		if (szFilter && !*szFilter) szFilter=NULL;

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
		SubtractDays(nPeriod-1, &fromDay,&fromMonth,&fromYear);
		TableMessageLog(io, bDeep, szFilter, fromDay, fromMonth, fromYear, d, m, y);

		MyBIO_puts(io, "<table width=100% bgcolour=#eeddee>");
		MyBIO_puts(io, "<tr><td valign=top>");

		MyBIO_puts(io, "<td valign=top>");
		MyBIO_puts(io, "</table>");
	} else if (!strncasecmp(szAddr, "/getinteraction/", 16)) {
		const char *szDir=szAddr+16;
		note_t *note=note_LoadMessage(szDir);
		const char *szInteractionName;
		const char *szInteractionSection;
		const char *szInteraction = note_FindEntry(note, 'Y');
		const char *szDatetime = note_FindEntry(note, 'D');

		szInteractionName = MessageDescription(szInteraction, &szInteractionSection);
		if (!szInteractionSection) szInteractionSection="";
		if (!szInteractionName) szInteractionName="";

		SendHttpHeader(io, 200, NULL);
		SendHtmlHeader(io);

		BIO_putf(io, "<table width=100%% bgcolor=#0077ff>\r\n");
		BIO_putf(io, "<tr><td align=center><font size=5 color=white>");
		if (*szInteractionSection || *szInteractionName) {
			BIO_putf(io, "%s %s on ", szInteractionSection, szInteractionName);
		} else {
			BIO_putf(io, "Connection on ");
		}
		BIO_putf(io, "%s", note_FindEntry(note, 'H'));
		BIO_putf(io, " at %s", szDatetime);
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
	} else if (!strncasecmp(szAddr, "/getmessage/", 12)) {
		const char *szFilename=hprintf(NULL, "%s/%s", szMsgLogDir, szAddr+12);
		FILE *fp=fopen(szFilename, "r");
		char buf[1024];
		int got;

		SendHttpHeader(io, 200, FileContentType(szFilename));

		while ((got=fread(buf, 1, sizeof(buf), fp)))
			MyBIO_write(io, buf, got);
		fclose(fp);
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
		const char *szBaseDir = GetBaseDir();

		if (szParams) {
			char *chp=szParams+1;								// param1=a&param2=b

			while (*chp) {
				if (*chp == '&') *chp=' ';
				chp++;
			}
			*szParams++='\0';									// param1=a param2=b
		}

		chp=strchr(szRequest, '/');								// /path1/path2
		if (chp) {
			szPathInfo=hprintf(NULL, "PATH_INFO=%s", chp);
			*chp='\0';
		} else {
			szPathInfo=strdup("PATH_INFO=");
		}
		putenv(szPathInfo);
		if( chdir(szBaseDir) != 0 ) {
			// Failed to change the cwd to BaseDir - something is horribly wrong
			LogError(errno,"Failed to change cwd to %s", szBaseDir);
		}

		// We'll try php, perl and bourne shell in that order
		if (!szCommand) {
			szDelete(szScript);
			szScript=hprintf(NULL, "%s/docs/%s.php", szBaseDir, szRequest);		// Try it as php
			if (!access(szScript, 1)) {						// Can execute it as php
				szCommand="php";
			}
		}

		if (!szCommand) {
			szDelete(szScript);
			szScript=hprintf(NULL, "%s/docs/%s.pl", szBaseDir, szRequest);		// Try it as perl
			if (!access(szScript, 1)) {					// Can execute it as perl
				szCommand="perl";
			}
		}

		if (!szCommand) {
			szDelete(szScript);
			szScript=hprintf(NULL, "%s/docs/%s.sh", szBaseDir, szRequest);		// Try it as bourne shell
			if (!access(szScript, 1)) {					// Can execute it as bourne shell
				szCommand="sh";
			}
		}

		if (szCommand) {
			const char *szQueryString=NULL;
			EnvSet("REQUEST_METHOD","GET");
			EnvSet("REQUEST_URI", szURI);
			szQueryString=strchr(szURI, '?');
			szQueryString=szQueryString?szQueryString+1:"";		// Bit after the '?' or blank
			EnvSet("QUERY_STRING", szQueryString);
			szCommandLine = hprintf(NULL, "%s %s %s < /dev/null", szCommand, szScript, szParams);
			fp=popen(szCommandLine, "r");
			EnvSet("QUERY_STRING",NULL);
			EnvSet("REQUEST_URI",NULL);
			EnvSet("REQUEST_METHOD",NULL);
//			szDelete(szCommandLine);
		}

		if (fp) {
			MyBIO_puts(io, "HTTP/1.1 200 OK\r\n");
//BIO_putf(io, "Content-type: text/html\r\n\r\nCommand line = '%s'<hr>", szCommandLine);
			while ((got=fread(buf, 1, sizeof(buf), fp))) {
				MyBIO_write(io, buf, got);
			}
			fclose(fp);
		} else {
			MyBIO_puts(io, "HTTP/1.1 403 Forbidden\r\n");
			MyBIO_puts(io, "Server: Microtest\r\n");
			MyBIO_puts(io, "\r\n");
			BIO_putf(io, "MMTS: You are not allowed to do whatever it was you just tried to do (%s).", szURI);
		}
		putenv("PATH_INFO=");
	}

	mime_Delete(mime);

	return 0;
}

int DealWithPUT(BIO *io, const char *szURI)
{
	Log("HTTP PUT %s", szURI);
	NoteInhibit(1);

	return 0;
}

int DealWithPOST(BIO *io, const char *szURI)
// Post has come in over SSL/io addressed to szURI
{
	MIME *mime;
	MSG *msg;									// Message we've received
	int i;
	rogxml *rx;

	Log("HTTP POST %s", szURI);

	if (!strncasecmp(szURI, "/spider", 7))			return Proxy(io, "127.0.0.1", 4511, "POST", "/spider", szURI+7);
	if (!strncasecmp(szURI, "/local", 6))			return Proxy(io, "127.0.0.1", 80, "POST", "/local", szURI+6);
	if (!strncasecmp(szURI, "/gpesspider", 11))	return Proxy(io, "gpes", 4511, "POST", "/gpesspider", szURI+11);
	if (!strncasecmp(szURI, "/gpes", 5))			return Proxy(io, "gpes", 80, "POST", "/gpes", szURI+5);

//	bAuth=AuthenticatePeer(ssl, NULL, 1);
//	if (!bAuth) Fatal("POSTs only accepted from authenticated peers");
//	if (!bAuth) Log("Peer authentication problem ignored");

	msg_SetLogDir(NoteDir());

	mime=GetIOMIME(io);
	if (!mime) Fatal("Malformed MIME received (error %d - %s)", mime_GetLastError(), mime_GetLastErrorStr());

	const char *szContentType = mime_GetContentType(mime);

	SetIncomingHeaderFromMime(mime);

//Log("Content type = '%s'", szContentType);
	if (!strcasecmp(szContentType, "application/x-www-form-urlencoded")
		|| !strcasecmp(szContentType, "multipart/form-data")) {
		return DealWithPostedForm(io, szURI, mime);
	}

	msg = msg_NewFromMime(mime);							// Extract the message from the MIME
	if (!msg) {
		Log("Could not interpret MIME message - see /tmp/duff.mime");
		{
			const char *szStr=mime_RenderHeap(mime);
			FILE *fp=fopen("/tmp/duff.mime", "w");
			fwrite(szStr, strlen(szStr), 1, fp);
			fclose(fp);
			NoteMessage(szStr, -1, "duffmime", "mime", "Uninterpretable MIME received");
		}
		return 0;
	}
	msg_SetURI(msg, szURI);
	Log("Received posted %s interaction", msg_GetInteractionId(msg));
	msg_Save(msg, "/tmp/attach1.msg");
	rogxml *rxSoap = msg_GetXML(msg);
	NoteMessageXML(rxSoap, "asyncin", "Message received async");

	if (g_ssl_cn) msg_SetAttr(msg, "peer-ssl-cn", g_ssl_cn);		// In case something wants to check it
	for (i=0;i<SSL_MAXDEPTH;i++) {
		if (g_ssl_subject[i]) {
			const char *szName = hprintf(NULL,"peer-cert-subject-%d", i);
			msg_SetAttr(msg, szName, g_ssl_subject[i]);
			szDelete(szName);
		}
	}
	msg_SetAttr(msg, "peer-ip", _szSenderIp);
	msg_SetIntAttr(msg, "local-port", _nIncomingPort);

	const char *szReason = NULL;
	const char *szSubCode = NULL;
	const char *szFaultElement = NULL;		// Finds its way to soap/Envelope/Header/FaultDetail/ProblemHeaderQName

	if (!strcmp(rogxml_GetLocalName(rxSoap), "Envelope")) {	// Something soapy this way cometh
		rogxml *rxHeader = rogxml_FindChild(rxSoap, "Header");

		if (rxHeader) {
			const char *szNotUnderstood = CheckSoapMustUnderstand(rxHeader);

			if (szNotUnderstood) {
				// This triggers the error handling below, which must be after msg_Unwrap() so we know to/from addresses
				szReason = hprintf(NULL, "I do not understand '%s'", szNotUnderstood);
				szSubCode = "MustUnderstand";
			}
		}
	}

	if (!msg_Unwrap(msg)) {									// Get rid of the SOAP and ebXML or nasp
		Log("Unwrapping error %d: %s", msg_GetErrorNo(msg), msg_GetErrorText(msg));
	}

	const char *szInteraction = msg_GetInteractionId(msg);
	int nDelay;
	if ((nDelay = MessageOption(szInteraction, "delay"))) {
		Log("messageopt(%s): Delaying %d second%s", szInteraction, nDelay, nDelay==1?"":"s");
		sleep(nDelay);
	}

	NoteFromMsg(msg);
	if (MessageOption(szInteraction, "drop")) {
		Log("messageopt(%s): Message dropped", szInteraction);
		return 0;
	}
//	if (!msg_FindContract(msg)) { Log("Cannot find contract for %s", msg_GetInteractionId(msg)); }

	if (szReason) {
		Log("Soap error: %s", szReason);
		SendSoapFault(io, 500, 1, NULL, szSubCode, szReason, szReason, NULL, NULL, NULL, msg_Attr(msg, "soap-to"), szFaultElement);
		return 0;
	}

	rogxml *rxMessage = msg_GetXML(msg);
	NoteMessageXML(rxMessage, "asyncnaked", "Message received async, unwrapped");

	rogxml *rxAsid = rogxml_FindByPath(rxMessage, "/*/communicationFunctionRcv/device/id/@extension");
	if (rxAsid) {
		const char *szAsid = rogxml_GetValue(rxAsid);

		Log("Received message for ASID %s wrapped in %s", szAsid, msg_IsNasp(msg) ? "nasp" : "ebXML");
	} else {
		Log("Received message wrapped as %s", msg_IsNasp(msg) ? "nasp" : msg_IsItk(msg) ? "ITK" : "ebXML");
	}

	if (msg_IsNasp(msg)) {									// SYNCHRONOUS (NASP) MESSAGE
		MSG *msgReply;

// Mantis 15333 - I'm unsure whether this approach is correct on incoming NASP, but I can't really see why not
		DeliverMessage(msg, NULL);							// Offers to a waiting sync process before delivering

		rx=msg_ReleaseXML(msg);

		rogxml *rxResponse=TranslateXML("end","xml",NULL, rx);

		if (rxResponse != rx) {								// Something translated it
			msgReply=msg_NewReply(msg);

			NoteMessageXML(rxResponse, "reply", "Reply");
			msg_SetXML(msgReply, rxResponse);
			AddNasp(msgReply);
			NoteMessageXML(msg_GetXML(msgReply), "reply", "Nasp wrapped reply");
			Log("Sending reply to NASP %s", msg_GetInteractionId(msg));
		} else {											// No translator
			rogxml_Delete(rx);								// Dispose of it

			msgReply = MakeNegAcknowledgementMessage(msg, NULL, 200, "No application to handle message");
		}
		msg_Delete(msg);									// Drop the original message

		if (!RawSendMessage(io, msgReply, 0, 200, "OK")) {
			Log("Could not send synchronous reply");
		} else {
			Log("Synchronous reply sent");
		}
	} else if (msg_IsItk(msg)) {
		const char *szReply;
		const char *szReason = NULL;
		const char *szSubCode = NULL;

		const char *szAction = msg_Attr(msg, "soap-action");
		if (szAction) {
			if (!strncmp(szAction, "urn:", 4)) szAction+=4;
			const char *chp = strchr(szAction, ':');
			if (chp) {
				const char *szRealm = strnappend(NULL, szAction, chp-szAction);

				NoteType("P",szRealm);
				// This may be, for example, "nhs-itk" but not sure what to do with it at the moment
				szDelete(szRealm);
				szAction=chp+1;
			}
			if (!strncmp(szAction, "services:", 9)) szAction+=9;
			NoteType("Y", szAction);
		}

		const char *szTo = msg_Attr(msg, "soap-to");
		const char *szFrom = msg_Attr(msg, "soap-from");
		const char *szReplyTo = msg_Attr(msg, "soap-reply-to");
		const char *szCreated = msg_Attr(msg, "soap-security-created");
		const char *szExpires = msg_Attr(msg, "soap-security-expires");
		const char *szElement = NULL;

		if (!szReason && !szTo) {
			szReason = hprintf(NULL, "No 'To' address");
			szSubCode = "wsa:MessageAddressingHeaderRequired";
			szElement = "wsa:To";
		}

#if 0			// Dropped check for NHS111 - 2.28a
		if (!szReason && !szFrom) {
			szReason = hprintf(NULL, "No 'From' address");
			szSubCode = "Client.FromAddressMissing";
			szElement = "wsa:From";
		}
#endif

		if (!szReason && szTo && !isValidEndpoint(szTo)) {
			szReason = hprintf(NULL, "To address is not a valid URI");
			szSubCode = "wsa:InvalidAddressingHeader";
			szElement = "wsa:To";
		}

		if (!szReason && szFrom && !isValidEndpoint(szFrom)) {
			szReason = hprintf(NULL, "From address is not a valid URI");
			szSubCode = "Client.FromAddress";
			szElement = "wsa:From";
		}

		if (!szReason && szReplyTo && !isValidEndpoint(szReplyTo)) {
			szReason = hprintf(NULL, "Reply-To address is not a valid URI");
			szSubCode = "Client.ReplyToAddress";
			szElement = "wsa:ReplyTo";
		}

		if (!szReason && szCreated && !DecodeTimeStamp(szCreated)) {
			szReason = hprintf(NULL, "Security creation date invalid");
			szSubCode = "Client.SecurityCreated";
			szElement = "wsa:ReplyTo";
		}

		if (!szReason && szExpires && !DecodeTimeStamp(szExpires)) {
			szReason = hprintf(NULL, "Security expiry date invalid");
			szSubCode = "Client.SecurityExpires";
		}

		if (!szReason && szCreated && szExpires && strcmp(szCreated, szExpires) > 0) {
			szReason = hprintf(NULL, "Created %s but expires earlier (%s)", szCreated, szExpires);
			szSubCode = "wsse:InvalidSecurity";
		}

		if (szReason) {
			Log("Incoming SOAP error: %s", szReason);
			SendSoapFault(io, 500, 1, NULL, szSubCode, szReason, szReason, NULL, NULL, NULL, szTo, szElement);
			szDelete(szReason);
			return 0;
		}

		int delivered = DeliverMessage(msg, &szReply);			// Offers to a waiting sync process before delivering
		if (delivered) {
			if (szReply) {
				const char *szResponse;
				const char *szMime;

//				int err = MakeHttpFromSoapText(&szResponse, &szMime, szReply, "urn:nhs-itk:services:201005:SendInfrastructureAck-v1-0", szFrom, szTo);
				int err = MakeHttpFromSoapText(&szResponse, &szMime, szReply, "urn:nhs-itk:services:201005:SendNHS111Report-v2-0Response", "http://www.w3.org/2005/08/addressing/anonymous", szTo);

				if (!err) {
					MyBIO_puts(io, szResponse);						// Send the response header line
					MyBIO_puts(io, szMime);							// Send the MIME portion
					MyBIO_flush(io);
				} else {
					MyBIO_puts(io, HttpHeader(503));			// Service Unavailable
					Log("Bad response from handler: %s", szMime);	// szMime holds the error string
				}

				szDelete(szResponse);
				szDelete(szMime);
			} else {
				MyBIO_puts(io, HttpHeader(200));				// OK
			}
		} else {
			MyBIO_puts(io, HttpHeader(405));					// Method not allowed
		}
	} else {												// ASYNC (ebXML) MESSAGE (ACK, DATA or PING)
		MSG *msgAck = ReceiveAsyncMessage(msg);

		if (msgAck) {
			NoteMessageXML(msg_GetXML(msgAck), "reply", "Acknowledgement");
			if (MessageOption(msg_GetInteractionId(msg), "noack")) {
				Log("messageopt: Not sending ack");
			} else {
				Log("Sending acknowledgement");
				SendAcknowledgementMessage(io, msgAck);
			}
			msg_Delete(msgAck);
		}
	}

	return 0;
}

static BIO *fhir_io = NULL;
static const char *fhir_verb = NULL;
static const char *fhir_uri = NULL;
static const char *fhir_api = NULL;
static const char *fhir_wampHost = NULL;
static const char *fhir_realm = NULL;

int OnWampError(WAMP *wamp, int type, JSON *json)
{
	const char *errorUri = json_ArrayStringzAt(json, 4);

	SSMAP *header = NewHeaderMap();
	Log("Ah - an error");

	ssmap_Add(header, "WAMP-ERROR", errorUri);

	SendHttp(fhir_io, 500, header, -1, errorUri);
	ssmap_Delete(header);

	Log("Error: %s", errorUri);

	wamp_Delete(wamp);
}

int OnWampResult(WAMP *wamp, int type, JSON *json)
// We have a result returned from the API call so we need to pass it back to our original caller.
{
	JSON *resultList = json_ArrayAt(json, 3);
	JSON *resultDict = json_ArrayAt(json, 4);

	int contentLength = 0;
	const char *content = NULL;
	SSMAP *header = NewHeaderMap();			// All our header elements
	int contentTypeSet = 0;					// 1 if content-type is set by the API response
	int httpCode = 0;

	if (resultList) {
		content = json_ArrayStringAt(resultList, 0, &contentLength);			// First element of array is always the content

		// WampSharp isn't able to return a resultDict so it returns it as the second resultList element
		// The following test triggers if we have more than one element in the resultList and resultDict has no content (or is NULL)
		if (json_ArrayCount(resultList) > 1 && json_ObjectCount(resultDict) == 0) {
			JSON *secondElement = json_ArrayAt(resultList, 1);

			if (json_IsObject(secondElement)) {									// Second array element is an object
				resultDict = secondElement;
			} else {															// Treat second element as string containing object
				const char *jsonText = json_AsStringz(secondElement);

				resultDict = json_Parse(&jsonText);
			}
		}

		if (json_IsObject(resultDict)) {
			httpCode = json_ObjectIntegerCalled(resultDict, "http_code");
			if (!httpCode) httpCode = 200;

			SPMAP *jsonHeaders = json_AsObject(json_ObjectElementCalled(resultDict, "http_headers"));
			if (jsonHeaders) {
				spmap_Reset(jsonHeaders);
				const char *name;
				JSON *value;
				while (spmap_GetNextEntry(jsonHeaders, &name, (void**)&value)) {
					if (name) {
						char *tmp = strdup(name);
						char *ch = tmp;
						while (*ch) {
							if (*ch == '_') *ch = '-';
							ch++;
						}

						if (!strcasecmp(tmp, "Content-Type"))
							contentTypeSet = 1;

						ssmap_Add(header, tmp, json_AsStringz(value));
						szDelete(tmp);
					}
				}
			}
		}

		if (!contentTypeSet) {
			if (*content == '<') {
				ssmap_Add(header, "Content-Type", "application/xml+fhir");
			} else {
				ssmap_Add(header, "Content-Type", "application/json+fhir");
			}
		}
	}

	if (contentLength) {
		SendHttp(fhir_io, httpCode, header, contentLength, content);
	} else {
		Log("Error - no result returned from API");
		Log("Returned: %s", json_Render(json));

		SendHttp(fhir_io, 500, NULL, -1, "I'm terribly sorry but the FHIR API didn't return anything.");
	}

	ssmap_Delete(header);

	wamp_Delete(wamp);				// Disconnect us
}

int OnWampConnection(WAMP *wamp, const char *mesg)
{
	if (wamp) {
		Log("Connected to WAMP router at %s via %s", fhir_wampHost, wamp_Name(wamp));
	} else {
		Log("Failed to make a WAMP connection to %s: %s", fhir_wampHost, mesg);

		const char *msg = hprintf(NULL, "Cannot connect to internal WAMP router at %s: %s", fhir_wampHost, mesg);
		SendHttp(fhir_io, 500, NULL, -1, msg);
		szDelete(msg);

		return 0;
	}

	wamp_RegisterHandler(wamp, WAMP_ERROR, OnWampError);
	wamp_RegisterHandler(wamp, WAMP_RESULT, OnWampResult);

	JSON *jHello = wamp_NewMessage(WAMP_HELLO, 0);
	json_ArrayAddString(jHello, -1, fhir_realm);
	JSON *opts = json_NewObject();

	JSON *roles = json_NewObject();
	json_ObjectAdd(roles, "caller", json_NewObject());

	json_ObjectAddStringz(opts, "agent", "MMTS");
	json_ObjectAdd(opts, "roles", roles);
	json_ArrayAdd(jHello, opts);

	wamp_SendJson(wamp, jHello);

	JSON *jCall = wamp_NewMessage(WAMP_CALL, wamp_RandomId());
	json_ArrayAdd(jCall, json_NewObject());
	json_ArrayAddString(jCall, -1, fhir_api);
	JSON *argList = json_NewArray();
	JSON *argDict = json_NewObject();

	MIME *mime;
	mime_ParseHeaderValues(0);
	// With some verbs we don't expect a body
	if (!strcmp(fhir_verb, "GET") || !strcmp(fhir_verb, "HEAD") || !strcmp(fhir_verb, "DELETE") || !strcmp(fhir_verb, "OPTIONS")) {
		mime=GetIOMIMEHeader(fhir_io);
	} else {
		mime=GetIOMIME(fhir_io);
	}

	if (!mime) Fatal("Malformed MIME received on FHIR call (error %d - %s)", mime_GetLastError(), mime_GetLastErrorStr());

	SetIncomingHeaderFromMime(mime);

	const char *szzHeaders = mime_GetHeaderList(mime);

	json_ObjectAddStringz(argDict, "verb", fhir_verb);
	json_ObjectAddStringz(argDict, "uri", fhir_uri);

	const char *headerName;
	for (headerName = szzHeaders; *headerName; headerName = szz_Next(headerName)) {
		char *lwrName = strdup(headerName);
		strlwr(lwrName);
		char *ch = lwrName;
		while (*ch) {
			if (*ch == '-') *ch = '_';
			ch++;
		}
		const char *value = mime_GetFullHeaderValue(mime, headerName);
		const char *param = hprintf(NULL, "%s", lwrName);
		szDelete(lwrName);

		json_ObjectAddStringz(argDict, param, value);
		szDelete(param);
		szDelete(value);
	}

//Log("MIME: '%s'", mime_RenderHeaderHeap(mime));
	int len;
	const char *body = mime_RenderContentBinary(mime, &len);
//Log("Len of content = %d: '%s'", len, body);
	json_ArrayAddString(argList, len, body);

	json_ArrayAdd(jCall, argList);
	json_ArrayAdd(jCall, argDict);

	wamp_SendJson(wamp, jCall);
}

int DealWithApiRedirect(BIO *io, const char *verb, const char *uri, const char *entry)
{
	Log("Redirecting to API (%s)", entry);
	const char *space = strchr(entry, ' ');
	if (!space) {
		Log("API redirect entry must have a space");
	}

	fhir_wampHost = strnappend(NULL, entry, space-entry);
	const char *call = SkipSpaces(space+1);
	const char *colon = strchr(call, ':');
	const char *realm;
	if (colon) {
		realm = strnappend(NULL, call, colon-call);
		fhir_api = colon+1;
	} else {
		realm = strdup("spider");
		fhir_api = call;
	}

	fhir_io = io;
	fhir_verb = verb;
	fhir_uri = uri;
	fhir_realm = realm;

Log("Connecting to WAMP server at %s to call %s in '%s'", fhir_wampHost, fhir_api, fhir_realm);
	CHANPOOL *pool = chan_PoolNew();
	wamp_Connect(pool, fhir_wampHost, OnWampConnection);

	chan_EventLoop(pool);
	Log("Event loop ended");

	szDelete(fhir_wampHost);
	fhir_wampHost = NULL;

	return 0;
}

int ReceiveHTTP(BIO *io, const char *szMethod, const char *szURI, const char *szVersion)
// Dispatcher for all incoming HTTPS messaging
{
	int n;

Log("HTTP Request: '%s' : '%s' : '%s'", szMethod, szURI, szVersion);
	Note("S|%s|%d", _szSenderIp, _nSenderPort);
	Note("H|TLS (%d)", _nIncomingPort);
	Note("J|%s", InternalId());

	const char *prefix;
	const char *api;
	int done = 0;
	ssmap_Reset(wampRedirectMap);
#ifdef __SCO_VERSION__
	while (ssmap_GetNextEntry(wampRedirectMap, &prefix, &api)) {
		char *expbuf = compile(prefix, NULL, NULL);
		if (!expbuf) {
			char buf[50];

			regerror(err, &preg, buf, sizeof(buf);
			Log("Regex error in '%s' (%s) - skipping", prefix, buf);
			continue;
		}

		if (advance(szURI, expbuf)) {
//		if (!strncasecmp(szURI, prefix, strlen(prefix))) {
			n = DealWithApiRedirect(io, szMethod, szURI, api);
			done = 1;
			break;
		}
		free(expbuf);
	}
#else
	
	regex_t preg;

	while (ssmap_GetNextEntry(wampRedirectMap, &prefix, &api)) {
		int err = regcomp(&preg, prefix, REG_NOSUB);
		if (err) {
			Log("Regex error in '%s' - skipping", prefix);
			continue;
		}

		if (!regexec(&preg, szURI, 0, NULL, 0)) {
			n = DealWithApiRedirect(io, szMethod, szURI, api);
			done = 1;
			break;
		}
		regfree(&preg);
	}
#endif

	if (!done) {
		if (!strcasecmp(szMethod, "GET")) {
			n=DealWithGET(io, szURI);
		} else if (!strcasecmp(szMethod, "PUT")) {
			n=DealWithPUT(io, szURI);
		} else if (!strcasecmp(szMethod, "POST")) {
				n=DealWithPOST(io, szURI);
		} else {
			Log("Unrecognised HTTP method: '%s'", szMethod);
			NoteInhibit(1);
			n=0;
		}
	}

	return n;
}

int DecodeRequestHeader(const char *szHeader, const char **pMethod, const char **pURI, const char **pVersion)
// Splits up something like "GET /fred HTTP/1.1"
// Returns	0	Ok
//			1	szHeader was NULL
//			2	Malformed (not three words)
{
	if (!szHeader) return 1;

	char *szMethod = NULL;
	char *szURI = NULL;
	char *szVersion = NULL;
	char *szLine=strdup(szHeader);
	char *szCopyLine = szLine;
	char *szSpace;
	char *chp;
	int nRetval = 2;						// Assume the worst until we see the version

	while (isspace(*szLine)) szLine++;
	chp=strchr(szLine, '\n');
	if (chp) *chp='\0';

	szSpace = strchr(szLine, ' ');			// Following method

	if (szSpace) {
		char *chp;
		szMethod=strnappend(NULL, szLine, szSpace-szLine);
		strupr(szMethod);										// Protect against get / HTTP/1.1
		while (*szSpace == ' ') szSpace++;
		chp=szSpace;
		szSpace=strchr(chp, ' ');
		if (szSpace) {
			szURI=strnappend(NULL, chp, szSpace-chp);
		}
		while (*szSpace == ' ') szSpace++;
		szVersion=strdup(szSpace);
		strupr(szVersion);										// Protect against GET / http/1.1
		if (*szSpace) nRetval = 0;
	}

	if (pMethod) *pMethod = szMethod; else szDelete(szMethod);
	if (pURI) *pURI = szURI; else szDelete(szURI);
	if (pVersion) *pVersion = szVersion; else szDelete(szVersion);

	szDelete(szCopyLine);

	return nRetval;
}

static void ServeRequest(BIO *io)
// Talk to the Client that's calling us
{
	char buf[1024];
	int nGot;					// Bytes got from a read

	nGot=MyBIO_gets(io, buf, sizeof(buf)-1);
	// The spec is very specific here but we'll be lax and allow extra spaces.
	// The spec (RFC 2616) recommends allowing a blank line at the top of the request
	// We don't allow that.
	if (nGot) {
		const char *szMethod = NULL;
		const char *szURI = NULL;
		const char *szVersion = NULL;

		DecodeRequestHeader(buf, &szMethod, &szURI, &szVersion);
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

	return;
}

MSG *http_GetMessage(BIO *io, int nTimeout)
// Wait up to nTimeout seconds for an incoming HTTP message
// If the timeout expires, returns NULL otherwise waits forever.
{
	const char *szHTTP;
	MIME *mime;
	MSG *msg;
	int nErr = 0;
	const char *szErrStr = NULL;
	int nErrType = 0;

	if (nTimeout) {							// Arrange that we get a wakeup call if necessary
		_nAlarmNumber=1;
		_nInterCharacterTimeout=nTimeout;	// 30629 If non-zero, alarm is rest to this on an incoming character
		if (setjmp(jmpbuf_alarm)) {
			Log("Timed out waiting for response");
			return NULL;
		}
	}

	szHTTP=BIO_GetLine(io);
	http_ParseReceivedHeader(szHTTP);
	mime=GetIOMIME(io);
	Log("Received entire response");
	alarm(0);
	_nAlarmNumber = 0;						// Stop any jump back
	_nInterCharacterTimeout=0;

	nErr=http_GetLastStatusCode();
	nErrType = nErr/100;								// Errors are in chapters of 100.
	if (nErrType == 2) nErrType = 0;					// 2xx means 'ok' so we'll translate it to 0
	if (nErrType) {										// Arrange that we have some sort of error message
		szErrStr = http_GetLastStatusText();
		if (!szErrStr || !*szErrStr) {					// We've got nothing worth having
			if (mime) {
				const char *ch;
				szErrStr=mime_GetBodyText(mime);
				ch=strchr(szErrStr, '\n');
				if (ch) {
					szErrStr=strnappend(NULL, szErrStr, ch-szErrStr);
				} else {
					szErrStr=strdup(szErrStr);
				}
				if (!*szErrStr) {
					szDelete(szErrStr);
					szErrStr=strdup("Blank error");
				}
			} else {
				szErrStr=strdup("Unspecified error");
			}
		} else {
			szErrStr=strdup(szErrStr);					// Ensure it's on the heap
		}
	} else {
		szErrStr = http_GetLastStatusText();
		if (szErrStr && *szErrStr) {
			szErrStr=strdup(szErrStr);
		} else {
			szErrStr=NULL;
		}
	}

	if (nErrType) {
		Log("Reply negative (%d) - %s", nErr, szErrStr);
	} else {
		if (szErrStr) {
			Log("Reply ok (%d) - %s", nErr, szErrStr);
		} else {
			Log("Reply ok (%d)", nErr);
		}
	}

	msg=msg_NewFromMime(mime);
	if (msg) {
		msg_Save(msg, "/tmp/httpin.msg");
//		msg_SetHttpCode(msg, http_GetLastStatusCode(), http_GetLastStatusText());
		msg_SetHttpCode(msg, nErr, szErrStr);		// Might work better than the above, which seems to return 0
	} else {
		msg=msg_NewError(1000+nErr, "%s", szErrStr);
	}

	szDelete(szErrStr);

	return msg;
}

int CreateWaitingFile(const char *id)
{
	const char *szFilename;
	FILE *fp;

	if (id && *id) {
		char *szCopyId = strdup(id);
		strupr(szCopyId);
		szFilename = hprintf(NULL, "%s/%s.waiting", szWaitingDir, szCopyId);
		fp=fopen(szFilename, "w");					// Create the file
		if (fp) {
			Log("Created '%s'", szFilename);
			fprintf(fp, "%d\n", getpid());
			fclose(fp);
		}
		szDelete(szCopyId);
		szDelete(szFilename);
	}

	return !!fp;
}

void MarkWaiting(MSG *msg)
// Leaves a file to note that we're waiting for a response on this message
{
	msg_Save(msg, "/tmp/wait.msg");
	CreateWaitingFile(msg_GetMessageId(msg));			// We might wait for the message ID
	CreateWaitingFile(msg_GetConversationId(msg, ""));	// Or we might match on the conversation ID
}

MSG *WaitForAsyncReply(MSG *msg, int fd, int sock)
// Returns the message that's being waited for
// If the 'fd' connection is lost then we return immediately (fd is the connection that is waiting for the reply)
// We'll use a counter over the number of waits here to avoid any problem with radical changes in the time/date.
{
	const char *szId = msg_GetMessageId(msg);
	const char *szConvId = msg_GetConversationId(msg, NULL);
	MSG *msgReply = NULL;						// Default reply indicates failure
	int nLimit = config_GetInt("AsyncTimeout", 120);	// Wait approximately this number of secs before getting fed up
	int nCount = 0;

	fd_set fdseterr;
	struct timeval timeout;

	fd=sock;
	FD_ZERO(&fdseterr);
	FD_SET(fd, &fdseterr);

	timeout.tv_sec=0;						// Seconds
	timeout.tv_usec=0;						// Microseconds

	if (!szId && !szConvId) return msg_NewError(30, "No id in incoming message");

	Log("Waiting for a reply to %s or %s", szId, szConvId);

	char *szCopyId=strdup(szId);
	strupr(szCopyId);
	const char *szWaiting = hprintf(NULL, "%s/%s.waiting", szWaitingDir, szCopyId);
	const char *szMessage = hprintf(NULL, "%s/%s.msg", szWaitingDir, szCopyId);

	char *szCopyConvId=strdup(szConvId);
	strupr(szCopyConvId);
	const char *szWaitingConv = hprintf(NULL, "%s/%s.waiting", szWaitingDir, szCopyConvId);
	const char *szMessageConv = hprintf(NULL, "%s/%s.msg", szWaitingDir, szCopyConvId);
	for (;;) {
#if 0			// IPCREMOVED
		const char *szIncoming = ipc_Process();							// Give a chance for our boat to come in

		if (szIncoming) {
			if (!strcmp(szIncoming, "ack")) {
				Log("Memory size = %d, content = %.30s", shm_ReceivedSize(), shm_ReceivedData());
			} else {
				Log("Waiting for acknowledgement and received '%s'?", szIncoming);
			}
			szDelete(szIncoming);
//TODO: XYZZY Need to do soemthing what the XML we've just received...!
		}
#endif

		if (!access(szMessage, 0)) {			// The file's appeared
			Log("Found incoming reply to %s", szCopyId);
			msgReply=msg_Load(szMessage);
			unlink(szWaiting);
			unlink(szMessage);
			unlink(szWaitingConv);
			unlink(szMessageConv);
			break;
		}
		if (!access(szMessageConv, 0)) {			// The file's appeared
			Log("Found incoming reply to %s", szCopyConvId);
			msgReply=msg_Load(szMessageConv);
			unlink(szWaiting);
			unlink(szMessage);
			unlink(szWaitingConv);
			unlink(szMessageConv);
			break;
		}
		if (access(szWaiting, 0)) {				// The waiting message has gone...???
			Log("Wait for %s aborted externally", szCopyId);
			break;
		}
		if (access(szWaitingConv, 0)) {				// The waiting message has gone...???
			Log("Wait for %s aborted externally", szCopyConvId);
			break;
		}

		select(fd+1, NULL, NULL, &fdseterr, &timeout);

		if (!fd) {
			Log("Internal connection has gone away");
			break;
		}
		if (nCount++ > nLimit) {
			Log("Waited too long for reply to %s", szId);
			break;
		}
		// NB. If this delay changes, so will the meaning of 'nLimit'
		sleep(1);								// Doze for a second
	}

	szDelete(szCopyId);
	szDelete(szWaiting);
	szDelete(szMessage);

	szDelete(szCopyConvId);
	szDelete(szWaitingConv);
	szDelete(szMessageConv);

	return msgReply;
}

int WrapMessage(MSG *msg, int bTranslate)
// Ensures the message is wrapped and fills in the information in the msg structure
// If 'bTranslate' then also translates the partyids according to the 'synchronicity' of
// the message.
// Returns
//	0	No wrapper - can't find a corresponding contract
//	1	Now wrapped
{
	if (msg_IsWrapped(msg)) return 1;						// Not much to do then

	if (msg_IsNasp(msg)) {
		AddNasp(msg);							// Add NASP wrapper
	} else if (msg_IsEbXml(msg)) {
		AddebXML(msg);							// Add ebXML wrapper
	} else {									// If it's not Nasp and not ebXML then I'm stuck.
		return 0;
	}

	return 1;
}

int SaveUnacked(MSG *msg)
// Saves the unacknowledged message
{
	rogxml *rx=msg_GetXML(msg);
	const char *szId;

	if (msg_IsWrapped(msg)) {
		szId=msg_GetMessageId(msg);
	} else {
		szId=WrapperGetMessageId(rx);
	}
	if (szId) {
		const char *szFilename;
		char *szCopyId = strdup(szId);

		strupr(szCopyId);
		szFilename = hprintf(NULL, "%s/%s.msg", szUnackedDir, szCopyId);
		msg_Save(msg, szFilename);

		szDelete(szCopyId);
		szDelete(szFilename);
	} else {
		Log("ERROR: Tried to save unacked msg with no id...");
	}

	return 1;
}

MSG *SendMessage(MSG *msg)
// Sends the message over the link and returns any acknowledgement and/or reply
// If the message is of a type that we should keep trying at (ebXML and retryable), we keep trying
// if we can do this in a reasonable time.
// The returned message should be checked with msg_GetErrorNo() on return as it might be rotten...
{
	MSG *msgReply = NULL;
	int nTries = 0;															// Attempts made so far
//	int nNaspTimeout = config_GetInt("NASP Timeout", 300);
//	int nebXMLTimeout = config_GetInt("ebXML Timeout", 300);
	const char *szInteraction = msg_GetInteractionId(msg);
	const char *szInteractionName = NULL;
	MSG *msgFatal = NULL;												// Set if a non-recoverable error found
	MSG *msgFail = NULL;												// Set if a re-tryable error found
	time_t tCutoff = 0;													// Don't send after this time
	time_t tStart = 0;													// Time of first attempt
	int nTimeout = config_EnvGetInt("ReplyWait", 30);	// How long to wait for a response

	if (!szInteraction) szInteraction="NoInteraction???";
	szInteractionName = strdup(MessageDescription(szInteraction, NULL));

	NoteMessageXML(msg_GetXML(msg), "snd", "Message sent to peer");			// Save log of message sent
	NoteMessageMsg(msg, "snd", "Message in full, sent to peer");			// Save log of message sent

	for (;;) {
		int tNextTry = 0;													// Time that we'll try again (if we do)
		int nWait;															// Seconds to sleep until tNextTry
		const char *szProtocol;												// The protocol we'll use (just for logging)
		const char *szAddress;												// Where we're sending the message
		int nPort;															// And the port number to use (443 usu.)
		BIO *io;
		char bLastTry;														// This is the last try
		char bException = 1;												// Loop control (1 on exit if excepted)
		msgFatal = NULL;													// Set if a non-recoverable error found
		msgFail = NULL;														// Set if a re-tryable error found

		int nRetryInterval = msg_PeriodToSeconds(msg_GetRetryInterval(msg));

		for (bException=1;bException;bException=0) {			// If we break, bException will be 1
			nTries++;
			bLastTry = nTries >= msg_GetMaxRetries(msg)+1;		// Makes re-triable situations fatal

			Log("Sending %s (%s) - attempt %d/%d", szInteraction, szInteractionName, nTries, msg_GetMaxRetries(msg)+1);

			// Sort out next retry time before we try to send this one
			tNextTry = time(NULL) + nRetryInterval;				// tNextTry is now the next time to try

			if (!WrapMessage(msg, 0)) {
				msgFatal = msg_NewError(1, "Cannot find contract information");
				break;
			}

			if (!tStart) {
				contract_t *c = msg_GetContract(msg);

				tStart = time(NULL);
				if (c) {
					const char *szPersistDuration = contract_GetPersistDuration(c);
					if (szPersistDuration) {
						tCutoff = tStart + msg_PeriodToSeconds(szPersistDuration);
					}
				}
			}

			if (tCutoff && tNextTry > tCutoff) {					// Scheduled next try would exceed persist duration
				Log("Last try as we would exceed persistDuration");
				bLastTry=1;
//				tNextTry = tCutoff;
			}

			if (GetAddressingData(msg, NULL, &szProtocol, &szAddress, &nPort, NULL)) {
				msgFatal = msg_NewError(GetErrorNo(), "%s", GetErrorStr());
				break;
			}

			Log("Making %s connection to %s:%d", szProtocol, szAddress, nPort);

			io=BuildConnection(szAddress, nPort);	// Give us a connection
			if (!io) {								// Couldn't for some reason
				msgFail = msg_NewError(GetErrorNo(), "Can't build connection (%s)", GetErrorStr());
				break;
			}

			if (!RawSendMessage(io, msg, 0, 0, NULL)) {
				Log("Error writing message on established connection");
				msgFail = msg_NewError(9, "Failed to write HTTPS message");
			} else {
				int nWait = nRetryInterval ? nRetryInterval : nTimeout;
				Log("Waiting for response (up to %d second%s)", nWait, nWait==1?"":"s");
				msgReply=http_GetMessage(io, nWait);
			}

			BreakConnection(io);									// Make a new one if we want to try again
			io=NULL;												// Just neatness

			if (msgFail) {											// Catch the failure while connected
				break;
			}

			if (msgReply && MessageOption(msg_GetInteractionId(msg), "ignoreresponse")) {
				msg_Delete(msgReply);
				msgReply=NULL;
				Log("messageopt: Response to %s thrown away", msg_GetInteractionId(msg));
			}

			if (msgReply && msg_GetErrorNo(msgReply)) {
				msgFail = msgReply;
				msgReply = NULL;			// Already logged error, but we'll see it again
				break;
			}

// TODO: I think I now need to check for a SOAP error/warning and respond accordingly
// For SOAP:Fault, I should NOT retry (EIS 11.5 2.6.7.1)
// For SOAP:Warning (if this exists), I should back off and retry

			if (msgReply) {											// Got a good response so happy (probably)
// This bit is misleading as it refers to the wrapped message (the SOAP part) and hence won't be an interaction
				const char *szType = NULL;
				rogxml *rx = msg_GetXML(msgReply);
				const char *szErrorListDetail;

				msg_ExtractMessageInfo(msgReply);
				szType = msg_GetInteractionId(msgReply);
				if (!szType) {
					szType = rogxml_GetValueByPath(rx, "/Envelope/Header/Action");
				}
				if (!szType) {		// Mantis 16957 - trigger failure on non-response
// This is still actually incorrect, since the behaviour is dependant on the message pattern which cannot be
// determined from these contract properties.
					contract_t *c = msg_GetContract(msg);
					if( strcasecmp(contract_GetSyncReplyMode(c),"none") && strcasecmp(contract_GetAckRequested(c),"never") ) {
						Log("Response (%d) doesn't contain an interaction - failed", szType, msg_GetHttpCode(msgReply));
						msgFail = msg_NewError(1000, "No interaction in response");;
						break;
					}
				}
				Log("Reply received: %s, status %d", szType, msg_GetHttpCode(msgReply));

				// Check if it's an errorlist (mantis 16558)
				szErrorListDetail = rogxml_GetValueByPath(rx, "/Envelope/Header/ErrorList/Error/Description");
				if (szErrorListDetail) {		// We have an errorlist returned
					const char *szErrorNo = rogxml_GetValueByPath(rx, "/Envelope/Header/ErrorList/Error/@errorCode");
					int nError = szErrorNo ? atoi(szErrorNo) : 999;

					Log("ErrorList %d - %s", nError, szErrorListDetail);
					msgFatal = msg_NewError(nError+1000, szErrorListDetail);
					break;
				}

			} else {
				msgFail = msg_NewError(7, "No good response received");
				break;
			}
		}

		if (!bException) break;										// We got a good response

		if (!bLastTry && time(NULL) >= tCutoff) {					// This attempt has pushed us beyond our cutoff
			Log("Sending aborted as already beyond persistDuration");
			bLastTry = 1;
		}

		if (msgFail && bLastTry) {									// Fail on last try is fatal
			msgFatal=msgFail;
			msgFail=NULL;
		}

		if (msgFatal) {
			break;
		}

		Log("Attempt %d failed with error %d - %s", nTries, msg_GetErrorNo(msgFail), msg_GetErrorText(msgFail));

		msg_Delete(msgFail);

		nWait = tNextTry - time(NULL);						// How long to wait until we can try again

		Log("Due to try again in %d second%s", nWait, nWait==1?"":"s");

		if (nWait > 0) sleep(nWait);						// Have a nap before we try again
	}

	szDelete(szInteractionName);

	// Here we have either been sent an ok (msgReply != NULL) or given up trying because we have run
	// out of allowed retries or if it is a NASP message that we never retry or we've got a fatal error.

	if (msgFatal) {						// Fatal error and retries exhausted or not applicable
		Log("Send failed: Error %d - %s", msg_GetErrorNo(msgFatal), msg_GetErrorText(msgFatal));
		msgReply = msgFatal;
	} else {
		SaveUnacked(msg);									// Otherwise handlers etc. will never find it...
		msg_ExtractMessageInfo(msgReply);
		NoteMessageMsg(msgReply, "reply", "Response from server");			// Save log of message sent
		NoteMessageXML(msg_GetXML(msgReply), "reply", "XML returned from server");
	}

	return msgReply;
}

void AcceptPlainConnection(int fd)
// Using the same protocol as an incoming TLS connection, but without the TLS...
{
	BIO *bio;

	GetEnvironment(_szSenderIp, _nIncomingPort);
	Log("External HTTP connection on %d from %s (assuming environment %s)", _nIncomingPort, _szSenderIp, szEnvironment);

	Note("S|%s|%d", _szSenderIp, _nSenderPort);
	Note("H|HTTP (%d)", _nIncomingPort);
	Note("E|%s", szEnvironment);

	bio=BIO_new_socket(fd, BIO_NOCLOSE);

	RememberEnvironment(_szSenderIp, _nIncomingPort, szEnvironment, 1);	// Assert that this barbeque tastes good...

	ServeRequest(bio);

	shutdown(fd, SHUT_WR);
	close(fd);
//	BIO_close(bio);

	Log("Natural end of external plain connection");

	Exit(0);				// Only a child and we're done
}

int SSL_CloseNicely(SSL *ssl, int fd)
// Closing the socket is slightly less simple than it might be
// Call with fd=-1 if you don't have one
{
	_nAlarmNumber=3;
	if (setjmp(jmpbuf_alarm)) {
		return -1;					// Failed
	}
	alarm(5);						// Give us five seconds to close the connection nicely

	int nOk=SSL_shutdown(ssl);
	if (!nOk) {
		// If we called SSL_shutdown() first then we always get return value of '0'.
		// In this case, try again, but first send a TCP FIN to trigger the other side's
		// close_notify.
		if (fd > -1) shutdown(fd, SHUT_WR);
		nOk=SSL_shutdown(ssl);
	}

	alarm(0);						// Abort the countdown
	_nAlarmNumber=0;

	return nOk;
}

void AcceptTlsConnection(int fd)
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

	GetEnvironment(_szSenderIp, _nIncomingPort);
	Log("TLS connection on %d from %s (assuming environment %s)", _nIncomingPort, _szSenderIp, szEnvironment);

	Note("S|%s|%d", _szSenderIp, _nSenderPort);
	Note("H|HTTPS (%d)", _nIncomingPort);
	Note("E|%s", szEnvironment);

	/* Build our SSL context*/
	ctx=ctx_New(GetCertificateDir(), _szSenderIp, PASSWORD);
	if (!ctx) {
		RememberEnvironment(_szSenderIp, _nIncomingPort, szEnvironment, 0);	// Stop liking this environment
		FatalSSL(ctx_Error());
	}
	SSL_CTX_set_session_id_context(ctx, (void*)&s_server_session_id_context, sizeof s_server_session_id_context);

	// Arrange that we ask the remote end for their certificate
	szCheck=config_EnvGetString("verify-tls");
	if (!szCheck) szCheck="No";

szCheck="A";
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

	ssl=SSL_new(ctx);
	sbio=BIO_new_socket(fd, BIO_NOCLOSE);
	SSL_set_bio(ssl, sbio, sbio);

	SSL_set_verify(ssl,nMode,verify_callback_rcv);

//Log("---------- About to accept the connection (SSL=%p, ctx=%p)", ssl, ctx);
// All decisions on making / rejecting connection should come here
	if (((r=SSL_accept(ssl)) <= 0)) {
//Log("%d=SSL_accept(%p)", r, ssl);
		int nOk = LogSslError(ssl,r);
		SSL_CloseNicely(ssl, fd);
		if (!nOk) {
			RememberEnvironment(_szSenderIp, _nIncomingPort, szEnvironment, 0);	// Stop liking this environment
			FatalSSL("SSL accept error (r=%d, errno=%d)", r, errno);
		} else {
			NoteInhibit(1);
			ctx_Delete(ctx);				// Tidy up
			if (nOk == 1) {
				Log("Calling SSL connection immediately hung up");
			} else {
				Log("Calling SSL connection broke the connection during handshake");
			}
			Exit(0);						// Only a child and we're done
		}
	}
//Log("Accept successful - %d", r);

	RememberEnvironment(_szSenderIp, _nIncomingPort, szEnvironment, 1);	// Assert that this barbeque tastes good...

	io=BIO_new(BIO_f_buffer());
	ssl_bio=BIO_new(BIO_f_ssl());
	BIO_set_ssl(ssl_bio,ssl,BIO_CLOSE);
	BIO_push(io,ssl_bio);

//	int nErr = SSL_get_error(ssl, nGot);

	SSL_get_verify_result(ssl);
	peer=SSL_get_peer_certificate(ssl);

// Here is where we need to check the name returned below against the expected CN of the caller.
	if (peer) {
		char buf[256];

		X509_NAME_get_text_by_NID(X509_get_subject_name(peer), NID_commonName, buf, sizeof(buf));
		g_ssl_cn = strdup(buf);
Log("Peer identified as '%s'", buf);

X509_NAME_oneline(X509_get_subject_name(peer), buf, 256);
Log("Issuer of certificate was '%s'", buf);

		X509_free(peer);
	} else {
		if (nMode & SSL_VERIFY_PEER) Log("Caller refused to identify themselves");
	}

	ServeRequest(io);

	// Ok, we're done so we'll close the socket now
	nOk = SSL_CloseNicely(ssl, fd);

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

rogxml *Greeting(const char *szIp, const char *szDescr)
// Generates a greeting XML message to send over the local connection
{
	rogxml *rxGreeting;
	rogxml *rx;
	int day, mon, year, hour, min, sec;
	time_t now=time(NULL);
	struct tm *tm = gmtime(&now);
	const char szMon[]="JanFebMarAprMayJunJulAugSepOctNovDec";
	char szHostname[255];

	for (mon=0;mon<12;mon++) if (!memcmp(szMon+mon*3, __DATE__, 3)) break;
	mon++;
	day=atoi(__DATE__+4);
	year=atoi(__DATE__+7);
	hour=atoi(__TIME__);
	min=atoi(__TIME__+3);
	sec=atoi(__TIME__+6);

	gethostname(szHostname, sizeof(szHostname));

	// Create and send the greeting message
	rxGreeting = rogxml_NewRoot();
	rogxml_SetLinefeedString("\r\n");
	rogxml_SetIndentString("  ");

	rxGreeting=rogxml_AddChild(rxGreeting, "MMTS-Hello");
	rogxml_SetAttr(rxGreeting, "version", VERSION);
	rogxml_SetAttr(rxGreeting, "host", szHostname);
	rogxml_SetAttrf(rxGreeting, "compiled", "%04d%02d%02d%02d%02d%02d", year, mon, day, hour, min, sec);
	rogxml_AddTextChildf(rxGreeting, "servertime",
			"%04d-%02d-%02d %02d:%02d:%02d",
			tm->tm_year+1900, tm->tm_mon+1, tm->tm_mday,
			tm->tm_hour, tm->tm_min, tm->tm_sec);
	rogxml_SetAttr(rxGreeting, "messageid", MessageId());
	rx=rogxml_AddTextChild(rxGreeting, "peer", szDescr);
	rogxml_AddAttr(rx, "ip", szIp);

	return rxGreeting;
}

int ProcessAcknowledgement(MSG *msg)
// If an acknowledgement then mark the corresponding message as acknowledged
// Returns	1	Acknowledgement
//			0	Not an ack
{
	msg_Save(msg, "/tmp/ack.msg");
	if (msg_IsAck(msg)) {
		const char *szReferring = msg_GetRefToMessageId(msg);

		MarkAcknowledged(szReferring);
		Log("Acknowledged %s", szReferring);
		return 1;
	} else {
		return 0;
	}
}

MSG *SendAsyncMessage(MSG *msg)
// Takes a wrapped message and sends it asynchronously
// NB. The message itself might want to be synchronous
{
	MSG *msgReply = NULL;
	MSG *msgAck = NULL;

	msg_SetLocalSync(msg, 0);					// This isn't going to be synchronous...

//	WrapMessage(msg, 1);  // Messages are always wrapped at this point
	NoteFromMsg(msg);

	msgReply = SendMessage(msg);
	msg_Delete(msg);

	if (msgReply) {
		if (msg_GetErrorNo(msgReply) == 0) {	// No error, phew
			Log("Received synchronous response to async message (this is ok)");
			msgAck = ReceiveAsyncMessage(msgReply);

			NoteMessageXML(msg_GetXML(msgReply), "asyncreply", "Response received back from peer");	// Save log of message sent
			if (msgAck) {
				Log("Ignoring peculiar request for an ack to the ack");
				msg_Delete(msgAck);
			}
		} else {
			Log("Async send error %d - %s", msg_GetErrorNo(msgReply), msg_GetErrorText(msgReply));
		}
	}

	return msgReply;
}

rogxml *MMTSResponse(rogxml *rxRequest)
// Generates responses to miscellaneous requests.
// Currently only handles requests for GUIDs (a random one is generated and sent back)
{
	const char *szRequest = rogxml_GetAttr(rxRequest, "request");

	if (!szRequest) return rogxml_NewError(99, "No request attribute");

	if (!strcasecmp(szRequest, "uuid") || !strcasecmp(szRequest, "guid")) {
		const char *szGuid = guid_ToText(NULL);
		rogxml *rxResponse;

		rxResponse = rogxml_NewElement(NULL, "MMTS-Response");
		rogxml_SetAttr(rxResponse, "response", szGuid);
		szDelete(szGuid);

		return rxResponse;
	}

	return rogxml_NewError(99, "Request '%s' not recognised", szRequest);
}

rogxml *PutXmlIntoDropDir(rogxml *rx, const char *szFilename)
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

	if (!strcmp(rogxml_GetLocalName(rx), "MMTS-Package")) {
		if (szEnvironment) {
			rogxml_SetAttr(rx, "environment", szEnvironment);
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

	rxAck = AckMessage(tDelay ? "delayed" : "immediate", szGuid, NULL, NULL, NULL);
	if (tDelay) {
		rogxml_SetAttr(rxAck, "due", TimeStamp(tDelay));
	}

	{	// This code in its own block because I really don't like it...
		MSG *msg;

		TranslateIds(rx);
		HarvestNhs(rx);								// Collect any NHS number for future searching
		msg = msg_NewFromInternal(rx);
		NoteFromMsg(msg);
		Note("Z|Delayed %s (%s)", msg_GetInteractionId(msg), MessageDescription(msg_GetInteractionId(msg), NULL));
		msg_Delete(msg);
	}

	szDelete(szTmp);
	szDelete(szDest);
	szDelete(szGuid);

	return rxAck;
}

const char *FindLocalMessage(const char *id)
// Finds a message in the 'out' directory.
// Returns the filename if found or NULL.  The filename will have to be Delete'd
{
	struct dirent *d;
	DIR *dir=opendir(szOutDir);
	const char *szName=NULL;
	const char *szFilename = NULL;					// Our eventual result

	// First we'll look for a file with the name '<id>.xml'
	if (dir) {
		while ((d=readdir(dir))) {
			int nNameLen;

			if (*d->d_name == '.') continue;		// Skip '.' files
			nNameLen=strlen(d->d_name);
			if (nNameLen < 4 || strcasecmp(d->d_name+nNameLen-4, ".xml"))
				continue;						// Skip non .xml
			if (!strncmp(d->d_name, id, nNameLen-4)) {
				szName=d->d_name;
				break;
			}
		}
		if (!szName) {								// Not found yet so we'll load each one and look inside
			rewinddir(dir);
			while ((d=readdir(dir))) {
				int nNameLen;
				rogxml *rx;
				const char *szFilename = NULL;

				if (*d->d_name == '.') continue;		// Skip '.' files
				nNameLen=strlen(d->d_name);
				if (nNameLen < 4 || strcasecmp(d->d_name+nNameLen-4, ".xml"))
					continue;						// Skip non .xml
				szFilename=hprintf(NULL, "%s/%s", szOutDir, d->d_name);
				rx=rogxml_ReadFile(szFilename);
				szDelete(szFilename);
				if (rx) rx=rogxml_FindDocumentElement(rx);
				if (rx && rogxml_ErrorNo(rx) == 0) {
					const char *szMessageId;

					if (!strcmp(rogxml_GetLocalName(rx), "MMTS-Package")) {		// If wrapped (as it should be), dig in
						rx=rogxml_FindFirstChild(rx);
					}
					szMessageId = rogxml_GetValueByPath(rx, "id/@root");
					if (szMessageId && !strcmp(szMessageId, id)) {
						szName=d->d_name;
						break;
					}
				}
				rogxml_DeleteTree(rx);			// Remembering that rx might be 'dug into'
			}
		}
		closedir(dir);
	}

	if (szName) {								// We found one...!
		szFilename=hprintf(NULL, "%s/%s", szOutDir, szName);
	}

	return szFilename;
}

rogxml *CancelLocalMessage(const char *id)
// Cancels the pending message matching the ID and returns either NULL or an error XML
{
	const char *szFilename = FindLocalMessage(id);
	rogxml *rxResult = NULL;


	if (szFilename) {
		if (unlink(szFilename)) {
			rxResult=ErrorMessage(325, 1, "System errno %d removing pending message", errno);
		} else {
			rxResult=NULL;
		}
		szDelete(szFilename);
	} else {
		rxResult=ErrorMessage(324, 1, "Cannot find message '%s'", id);
	}

	return rxResult;
}

rogxml *ScheduleLocalMessage(const char *id, const char *szDelay)
{
	const char *szFilename = FindLocalMessage(id);
	rogxml *rxResult;

	if (szFilename) {
		rogxml *rx;

		rx=rogxml_ReadFile(szFilename);
		if (rx) rx=rogxml_FindDocumentElement(rx);
		rogxml_Unlink(rx);								// Drop the root element

		if (rx && rogxml_ErrorNo(rx) == 0) {
			const char *szFile;										// Last portion of filename

			if (strcmp(rogxml_GetLocalName(rx), "MMTS-Package")) {	// Wrap it in an MMTS-Package if it's not already
				rogxml *rxTmp=rogxml_NewElement(NULL, "MMTS-Package");

				rogxml_LinkChild(rxTmp, rx);
				rx=rxTmp;
			}
			rogxml_SetAttr(rx, "delay", szDelay);
			szFile=strrchr(szFilename, '/');						// Find last '/' in filename
			if (szFile) {											// Should always be non-NULL
				szFile++;											// Point to final filename portion
			} else {
				szFile=NULL;
			}

			rxResult=PutXmlIntoDropDir(rx, szFile);
			rogxml_DeleteTree(rx);
		}
		szDelete(szFilename);
	} else {
		rxResult=ErrorMessage(324, 1, "Cannot find message '%s'", id);
	}

	return rxResult;
}

rogxml *MMTSRPCFunction(rogxml *rxRpc)
// Handles the NEW style RPC with a _function attribute.
// Note there is a subtle side-effect in here...  If szRpcError is non-NULL on exit, the content is written to
// the log AFTER the result is returned.  This should be the stderr of the executed binary.
{
	rogxml *rx = NULL;
	const char *szFunction;
	const char *szBinary;
	const char *stdin_buf = NULL;
	HBUF	*BufStdout = NULL;				// Incoming buffers for stdout and stderr
	HBUF	*BufStderr = NULL;
	rogxml *rxResultSet = rogxml_NewElement(NULL,"MMTS-RPC-Result");
	int retval;
	pid_t pid;
	int fdin[2];							// Pipe for stdin
	int fdout[2];							// Pipe for stdout
	int fderr[2];							// Pipe for stderr
	const char **args;						// Program arguments passed through
	const char *szError=NULL;				// Internally generated prefix to stderr if we can't read stdout
	int nErrno = 0;							// Error number tacked onto the result

	szFunction = strdup(rogxml_GetAttr(rxRpc, "_function"));
	if (!szFunction || !*szFunction) {		// We should only get here if it's defined but it may be blank...
		return ErrorMessage(501, 1, "No _function attribute provided");
	}

	// Take the function component by component and look for a binary
	szBinary=hprintf(NULL, "%s/rpc", GetBaseDir());
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

	if (access( szBinary, R_OK|X_OK)) return rogxml_NewError(506, "Function '%s' not executable (at %s)", szFunction, szBinary);

	// Build the argument list, which looks as if the rpc is called as:
	// /usr/mt/mmts/rpc/fred _function=fred attr1=whatever attr2=whatever
	// Where 'attr1' etc. are all attributes passed in the root element

	int nArgs=0;												// Count of arguments
	rogxml *rxAttr;
	for (rxAttr=rogxml_FindFirstAttribute(rxRpc);rxAttr;rxAttr=rogxml_FindNextSibling(rxAttr)) {
		nArgs++;
	}

	args = NEW(const char *, nArgs+2);				// +2 is program, and NULL
	args[0]=strdup(szBinary);
	int nArg=1;

	for (rxAttr=rogxml_FindFirstAttribute(rxRpc);rxAttr;rxAttr=rogxml_FindNextSibling(rxAttr)) {
		const char *szName = rogxml_GetLocalName(rxAttr);
		args[nArg++]=hprintf(NULL, "%s=%s", szName, rogxml_GetValue(rxAttr));
	}
	args[nArg]=NULL;

	// Look for contained elements that we currently support (just "input")
	for(rx = rogxml_FindFirstChild(rxRpc); rx != NULL; rx = rogxml_FindNextSibling(rx) ) {
		const char *name = rogxml_GetLocalName(rx);
		if (!strcmp( name, "input" )) {
			if (stdin_buf) return rogxml_NewError(507, "Multiple input elements are not allowed");
			rogxml *rxChild = rogxml_FindFirstChild(rx);

			while (rxChild) {										// Need to render the children in case it's XML
				const char *szElement = rogxml_ToText(rxChild);
				stdin_buf=hprintf(stdin_buf, "%s", szElement);
				szDelete(szElement);

				rxChild=rogxml_FindNextSibling(rx);
			}
		}
	}

	// Create three pipes for stdin, stdout and stderr
	if( pipe(fdin) == -1 || pipe(fdout) == -1 || pipe(fderr) == -1 ) {
		const char* err = strerror(errno);
		return rogxml_NewError(508, "Error %d creating pipe: %s", errno, err);
	}

	if (!(pid = spiderFork("Legacy"))) {								// Child process
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

		Log("Executing RPC command: %s", szBinary);
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
	if (stdin_buf) {
		int written;

		do {
			written = write( fdin[1], stdin_buf + w, strlen(stdin_buf) - w );
			if (written == -1) return rogxml_NewError(511, "Error %d (%s) writing to RPC stdin", errno, strerror(errno));
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

	if (!WIFEXITED(status)) {									// Non-standard exit from RPC binary
		if (WIFSIGNALED(status)) {								// Someone killed the child (may be it errored)
			int nSig = WTERMSIG(status);

			return rogxml_NewError(514, "RPC %s was killed with %s - \"%s\"", szBinary, SignalName(nSig), SignalDescr(nSig));
		} else {
			return rogxml_NewError(515, "RPC %s exited oddly", szBinary);
		}
	}

	retval = WEXITSTATUS(status);
	Log("RPC call exited with status = %d", retval);
	if (retval > 1 && retval < 10)
		return rogxml_NewError(516, "Invalid return code (%d) from RPC '%s' (must not be 2..9)", retval, szFunction);
	if (retval == 1) {											// Indicates error returned from binary
		nErrno=500;
		szError=hprintf(NULL, "System error returned by RPC process '%s'", szFunction);
	}

	// Stdout might contain a number of results and/or a number of errors, or something non-XML
	// This is where we interpret that stuff.
	int nStdoutLen = hbuf_GetLength(BufStdout);
	if (nStdoutLen) {
		hbuf_AddChar(BufStdout, 0);									// Terminate the buffer
		const char * buf = hbuf_ReleaseBuffer(BufStdout);			// Get the buffer
		const char *next=SkipSpaces(buf);							// To allow reading of multiple elements
		rogxml *rxErrors = rogxml_NewElement(NULL, "errors");		// Element to hang errors from
		rogxml *rxResult = rogxml_NewElement(NULL, "result");		// Element to hang results from

		while (*next) {
			rogxml *rx=rogxml_FromText(next);						// Try and interpret it as XML

			if (rx) {
				if (rogxml_ErrorNo(rx)) {								// Some duff XML from the binary
					Log("Bad XML from RPC: %s", rogxml_ErrorText(rx));
					szRpcError=hprintf(NULL, "Bad XML in stdout: %s\n---- Stdout from here\n%s\n---- Stderr from here\n", rogxml_ErrorText(rx), buf);
					retval=1;
					nErrno=517;

					rogxml *rx=rogxml_NewElement(NULL, "error");		// Prepend to any other errors
					rogxml_SetAttr(rx, "source", "MMTS");
					rogxml_AddTextf(rx, "Invalid XML returned from RPC '%s' (retval was %d)", szFunction, retval);
					rogxml_LinkChild(rxErrors, rx);
					break;
				}

				const char *szName = rogxml_GetLocalName(rx);

				if (!strcmp(szName, "error")) {						// Child is returning an error
					if (!retval) {									// Returning error elements on no error...
						retval=1;
						nErrno=518;
						rogxml *rx=rogxml_NewElement(NULL, "error");		// Prepend to any other errors
						rogxml_SetAttr(rx, "source", "MMTS");
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

	/* package stderr */
	int nStderrLen = hbuf_GetLength(BufStderr);						// See if we had any stderr
	if (nStderrLen) {
		hbuf_AddChar(BufStderr, 0);									// Terminate the buffer
		const char * buf = hbuf_ReleaseBuffer(BufStderr);			// Get the buffer

		if (szRpcError) {												// Prepend any existing error message
			szRpcError = strappend(szRpcError, buf);
			szDelete(buf);
		} else {
			szRpcError = buf;
		}
	}
	hbuf_Delete(BufStderr);

	if (szError) {											// We have our own error we'd like to add
		rogxml *rx=rogxml_NewElement(NULL, "error");		// Prepend to any other errors
		rogxml_SetAttr(rx, "source", "MMTS");
		rogxml_AddText(rx, szError);
		rogxml_LinkChild(rxResultSet, rx);
	}

	if (retval == 0 || retval >= 10) {
		rogxml_SetAttrInt(rxResultSet, "status", retval ? 2 : 0);		// Always return 2 for a business error
		if (retval >= 10) rogxml_SetAttrInt(rxResultSet, "error", retval);	// Return the actual exit code here
	} else {
		rogxml_SetAttrInt(rxResultSet, "status", retval);
		if (nErrno) rogxml_SetAttrInt(rxResultSet, "error", nErrno);
	}

	return rxResultSet;
}

/**
 * Handles an MMTS-rpc message
 * Execute the nominated script/program and return the stdout
 */
rogxml *MMTSRpcName(rogxml *rxRpc)
// Old style RPC handling with the name attribute
{
	rogxml *rx = NULL;
	char   *pgname = NULL;
	char   *basepgname;
	const char *baseDir;
	const char *param[128]; /* max 128 parameters */
	char       *stdin_buf = NULL;
	size_t      stdin_buf_len = 0;
	HBUF	*BufStdout = NULL;				// Incoming buffers for stdout and stderr
	HBUF	*BufStderr = NULL;
	rogxml *rxResultSet = rogxml_NewElement(NULL,"MMTS-RPC-Result");
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
	for( i = 0; basepgname[i] != '\0'; ++i ) {
		if( basepgname[i] == '.' )
			basepgname[i] = '/';
	}

	baseDir = GetBaseDir();
	pgname  = (char*)malloc(strlen(basepgname)+strlen(baseDir)+strlen("/rpc/")+1);
	sprintf( pgname, "%s/rpc/%s", baseDir, basepgname );
	param[0] = strdup( pgname );

	if( access( pgname, R_OK|X_OK ) != 0 )
	{	/* error checking file */
		rogxml* err = ErrorMessage(502, 1, "Procedure '%s' not known", pgname);
		// cleanup
		free(pgname);
		free(basepgname);
		for( i=0; param[i] != '\0'; ++i ) {
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
		else if( strcmp( name, "stdin" ) == 0 )
		{
			/* extract stdin data */
			rogxml *node;
			const char *buf;
			for( node = rogxml_FindFirstChild(rx); node != NULL; node = rogxml_FindNextSibling(node) )
			{
				if( rogxml_IsElement(node) ) {
					buf = rogxml_ToText(node);
				}
				else if( rogxml_IsText(node) ) {
					buf = rogxml_GetValue(node);
				}
				else {
					buf = NULL;
				}
				if( buf != NULL ) {
					stdin_buf_len = strlen(buf) + stdin_buf_len + 1;
					stdin_buf     = (char*)realloc(stdin_buf,stdin_buf_len);
					strcat( stdin_buf, buf );
				}
			}
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

	if( (pid = spiderFork("NAME-LEG")) == 0 ) {
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
	for( i=0; param[i] != NULL; ++i ) {
		free( (void*)param[i] );
	}
	if( stdin_buf != NULL ) {
		free(stdin_buf);
	}

	return rxResultSet;
}

rogxml *MMTSRpc(rogxml *rxRpc)
// Dual purpose rpc handling.  If there is a '_function' attribute then we're using the new style
// argument passing and return mechanism, if instead there's a 'name' attribute then we're using
// the old one, if there neither then it's an error.
{
	const char *function = rogxml_GetAttr(rxRpc, "_function");

	if (function) {									// New style call with _function
		Log("MMTS-RPC Function=%s", function);
		rogxml *rxRpcResult=MMTSRPCFunction(rxRpc);

		int nErr = rogxml_ErrorNo(rxRpcResult);

		Note("Z|%s", function);
		if (nErr) {						// We've returned an error, which will be a system error
			const char *szErr = rogxml_ErrorText(rxRpcResult);

			Log("RPC Error %d: %s", nErr, szErr);
			rogxml *rxResult = rogxml_NewElement(NULL, "MMTS-RPC-Result");
			rogxml_SetAttr(rxResult, "status", "1");					// System error
			rogxml_SetAttrInt(rxResult, "error", nErr);					// Error number
			rogxml *rxError = rogxml_AddTextChild(rxResult, "error", szErr);
			rogxml_SetAttr(rxError, "source", "MMTS");
			rogxml_Delete(rxRpcResult);
			rxRpcResult=rxResult;
		}

		return rxRpcResult;
	}

	const char *name = rogxml_GetAttr(rxRpc, "name");
	if (!name) return rogxml_NewError(101, "RPC call must include either _function or name attribute");

	Log("MMTS-RPC Name=%s", name);
	Note("Z|%s", name);

	return MMTSRpcName(rxRpc);
}

rogxml *MMTSControl(rogxml *rxCtrl)
// Handles an MMTS-Control message.
// Note that multiple commands can be placed in here, in which case an MMTS-Result is returned with
// the results in it.
{
	rogxml *rx;
	int nCount=0;
	rogxml *rxResultSet = rogxml_NewElement(NULL, "MMTS-Result");

	for (rx=rogxml_FindFirstChild(rxCtrl);rx;rx=rogxml_FindNextSibling(rx)) {
		const char *szType = rogxml_GetLocalName(rx);
		rogxml *rxResult = NULL;

		nCount++;
		if (!strcmp(szType, "cancel")) {
			const char *szId = rogxml_GetAttr(rx, "msgid");

			if (!szId) {
				rxResult = ErrorMessage(321, 1, "No msgid in MMTS-Control/cancel message");
			} else {
				rxResult = CancelLocalMessage(szId);
				if (!rxResult) {						// All went well...
					rxResult=AckMessage("cancelled", szId, NULL, NULL, NULL);
				}
			}
		} else if (!strcmp(szType, "reschedule")) {
			const char *szId = rogxml_GetAttr(rx, "msgid");
			const char *szDelay = rogxml_GetAttr(rx, "delay");

			if (!szId) {
				rxResult = ErrorMessage(321, 1, "No msgid in MMTS-Control/reschedule message");
			} else if (!szDelay) {
				rxResult = ErrorMessage(325, 1, "No delay in MMTS-Control/reschedule message");
			} else {
				rxResult = ScheduleLocalMessage(szId, szDelay);
				if (!rxResult) {						// All went well...
					rxResult=AckMessage("cancelled", szId, NULL, NULL, NULL);
				}
			}
		} else {
			rxResult = ErrorMessage(322, 1, "Unrecognised MMTS-Control type '%s'", szType);
		}
		rogxml_LinkChild(rxResultSet, rxResult);
	}

	if (nCount == 1) {									// Only one result so just return that
		rogxml *rx=rogxml_FindFirstChild(rxResultSet);
		rogxml_Unlink(rx);
		rogxml_Delete(rxResultSet);
		rxResultSet=rx;
	} else {
		rogxml_SetAttrInt(rxResultSet, "count", nCount);
	}

	return rxResultSet;
}

#if 0
///////////////// More full try at expression parsing for the SQL -> LDAP translator

const char *GetToken(const char **pExpr, int *pLen)
// Given a pointed to a string and a place to return the length of the result
// Returns a pointer to a token (often the entry value of *pExpr) and adjusts
// pExpr to be ready for the next one.
// Returns nLen=0 and "" when at the end of the string.
// NB. 'like' as an operator is translated into '~'.
// The token type can be determined by the first character of the return value.
// NB. There are multiple exit points from this function.
//
{
	const char *szText=SkipSpaces(*pExpr);
	char char *ops[] = {"<=", ">=", "!=", "<", ">", "=", NULL};
	const char **op;

	if (!*szText) { *pLen=0; return szText; }

	if (!strncasecmp(szText, "like") && !isalnum(szText[4])) {	// Special case for 'like'
		*pExpr=szText+4; *pLen=1; return "~";
	}

	for (op=ops;*op;op++) {									// Normal operator
		int nLen = strlen(*op);

		if (!strncasecmp(szText, *op, nLen)) {
			*pExpr=szText+nLen; *plen=nLen; return szText;
		}
	}

	if (*szText == '\'' || *szText == '"') {									// Quoted
HERE

}

const char *GetLdapExpression(const char *szExpr)
{
	int nLevel = 0;					// Bracket level

	while (szExpr) {
		szToken = GetToken(&szExpr, &nLen);
	}
}

#endif
/////////////////

rogxml *MMTSSDS(rogxml *rxRequest)
// Performs a general SDS lookup and returns the result
{
	rogxml *rxReply=NULL;
	const char *szFunction=rogxml_GetAttr(rxRequest, "function");

	if (szFunction) {
		if (!strcasecmp(szFunction, "PartykeyFromAsid")) {
			return sds_GetSingle(rxRequest, "PartykeyFromAsid", "services WHERE uniqueIdentifier='%s' AND objectClass=nhsAs","asid","nhsMHSPartyKey","partykey");
		} else if (!strcasecmp(szFunction, "PartykeyFromNacs")) {
			return sds_GetSingle(rxRequest, "PartykeyFromNacs", "services WHERE nhsIDCode='%s' AND objectClass=nhsAs","nacs","nhsMHSPartyKey","partykey");
		} else if (!strcasecmp(szFunction, "NacsFromPartykey")) {
			return sds_GetSingle(rxRequest, "NacsFromPartykey", "services WHERE nhsMHSPartyKey='%s' AND objectClass=nhsAs","partykey","nhsIDCode","nacs");
		}
	}

	rogxml *rx=rogxml_FindFirstChild(rxRequest);
	if (!strcmp(rogxml_GetLocalName(rx), "query")) {
		const char *szQuery = rogxml_GetElementText(rx);

		Log("Query = %s", szQuery);
		Note("Z|SDS Query (%.30s...)", szQuery);

		rogxml *rxResults = sds_Query(szQuery, 1);
		if (rogxml_ErrorNo(rxResults)) {
			Log("Error %d: %s", rogxml_ErrorNo(rxResults), rogxml_ErrorText(rxResults));
			rxReply=rxResults;
		} else {						// Got back results ok so wrap them in something nice
			rxReply=rogxml_NewElement(NULL, "MMTS-SDS-Result");
			rogxml_LinkChild(rxReply, rxResults);
		}
	} else {
		rxReply=rogxml_NewError(131, "No <query> element in SDS request");
	}

	return rxReply;
}

int DoWrapMessage(MSG *msg)
// Take message from unwrapped and naked to fully wrapped and ready to go.
// Returns	1	Wrapped ok
//			0	Failed to be wrapped (message has error)
{
	rogxml *rx, *rx2;
	int bWrapped = 0;

	rx=msg_ReleaseXML(msg);						// rx is now basic HL7

	// Translate the to and from PartyIds
	TranslatePartyIdsAt(rx, "/*/communicationFunctionSnd/device/id");
	TranslatePartyIdsAt(rx, "/*/communicationFunctionRcv/device/id");

	// Translate any ASIDs in the message
	TranslateAsids(rx);

	rx2=TranslateXML("io", NULL, NULL, rx);
	if (rx2 != rx) {							// Some translating happened
		rx=rx2;
		NoteMessageXML(rx, "txl", "Message after translations");			// Save log of message after translation
	}

	int nErr=ValidateXml(rx);
	if (nErr) {
		msg_SetError(msg, nErr, "XML Failed validation - details in log");
	} else {
		msg_SetXML(msg, rx);

		msg_ExtractMessageInfo(msg);				// Pull out Party ids etc. before we possibly take them away

		if (msg_FindMethod(msg)) {
/// NB. NB. This is done twice now so that we catch the main message and the wrapper - it's kludgey and
///         should be changed.
			if (msg_GetLevel(msg) == LEVEL_P1R2)	// Need to sneakily twiddle the PartyIds into ASIDs
				ReplacePartyIdsByAsids(msg_GetXML(msg));
			bWrapped = WrapMessage(msg, 1);			// Wrap it as Nasp or ebXML
/// NB. Just switched this around so ASID translation is done after wrapping so wrapper is included.
			if (msg_GetLevel(msg) == LEVEL_P1R2) {	// Need to sneakily twiddle the PartyIds into ASIDs
				ReplacePartyIdsByAsids(msg_GetXML(msg));
			}
		}
	}

	return bWrapped;
}

int QueueProcess(const char *szProcess, SSMAP *ssParams)
// Queues an external process with the parameters given
// Returns	0		All went well
//			1...	Error
{
	const char *szFilename = hprintf(NULL, "%s/todo/%s", GetBaseDir(), InternalId());
	const char *szTmp = hprintf(NULL, "%s.tmp", szFilename);
	const char *szTodo = hprintf(NULL, "%s.todo", szFilename);
	FILE *fp;
	int nResult=0;

	fp=fopen(szTmp, "w");
	if (fp) {
		const char *szKey, *szValue;
		int nErr;

		Log("Queuing process %s (%s)", szProcess, szTodo);
		Log("Environment = %s", szEnvironment);

		fprintf(fp, "Process=%s\n", szProcess);
		fprintf(fp, "Environment=%s\n", szEnvironment);
		ssmap_Reset(ssParams);
		while (ssmap_GetNextEntry(ssParams, &szKey, &szValue)) {	// Convert the map into a string
			fprintf(fp, "param-%s=", szKey);
			fprintf(fp, "%s\n", szValue);					// Needs encoding if funny chars
			Log("Parameter %s = %s", szKey, szValue);
		}
		fclose(fp);
		nErr=RogRename(szTmp, szTodo);
		if (nErr) {
			Log("Error %d renaming '%s' to '%s'", nErr, szTmp, szTodo);
			nResult=2;
		}
	} else {
		Log("Failed to create file %s", szTmp);
		nResult=1;
	}

	szDelete(szTodo);
	szDelete(szTmp);
	szDelete(szFilename);

	return nResult;
}

////////////// Mersenne Twister Functions

#define MT_LEN			624
#define MT_IA			397
#define MT_IB			(MT_LEN - MT_IA)
#define MT_TWIST(b,i,j)	((b)[i] & 0x80000000) | ((b)[j] & 0x7FFFFFFF)
#define MT_MAGIC(s)		(((s)&1)*0x9908B0DF)

int mt_index = MT_LEN*sizeof(unsigned long);
unsigned long mt_state[MT_LEN];

void mt_init()
{
	int i;
	for (i = 0; i < MT_LEN; i++)
		mt_state[i] = rand();
	mt_index = MT_LEN*sizeof(unsigned long);
}

void mt_seed(const char *data, int len)
{
	while (len > 0) {
		unsigned long s;
		int i;

		if (len >= 4) {
			s=*(unsigned long*)data;
		} else if (len == 3) {
			s=*data+(data[1]<<8)+(data[2]<<16);
		} else if (len == 2) {
			s=*(unsigned short*)data;
		} else {
			s=*data;
		}
		len-=4; data+=4;

		mt_state[0] ^= s;
		for (i = 1; i < MT_LEN; i++) {
			mt_state[i] = 1812433253UL * (mt_state[i - 1] ^ (mt_state[i - 1] >> 30)) + i;
			mt_state[i] &= 0xFFFFFFFFUL; // for > 32 bit machines
		}
	}
	mt_index = MT_LEN*sizeof(unsigned long);
}

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

const char *GenPassword(int nBits)
// Generates a password with at least nBits bits of entropy
{
	const char *szPassword = strdup("");
	const char *szGuid = guid_ToText(NULL);
	time_t now = time(NULL);
	double fBits = nBits;

	mt_seed(szGuid, 36);
	mt_seed((const char *)&now, sizeof(now));

	while (fBits > 0) {
		if (fBits >= 3.322*2) {
			char buf[3];

			buf[0]="bcdfghjklmnpqrstvwxyz"[mt_random(21)];
			buf[1]="aeiou"[mt_random(5)];
			buf[2]="bcdfghjklmnpqrstvwxyz"[mt_random(21)];
			szPassword=strnappend((char*)szPassword, buf, 3);
			fBits-=11.722;
		} else {
			char buf[1];

			buf[0]="0123456789"[mt_random(10)];
			szPassword=strnappend((char*)szPassword, buf, 1);
			fBits-=3.322;
		}
	}

	return szPassword;
}

const char *GenFilename()
{
	const char *szPassword = GenPassword(32);			// Gives cvccvccvc
	const char *szFilename = hprintf(NULL, "Audit-%s", szPassword);

	szDelete(szPassword);

	return szFilename;
}

time_t nAuditTimeDt;
const char *szAuditSourceProduct = NULL;
const char *szAuditSourceModule = NULL;
const char *szAuditUserId = NULL;
const char *szAuditUserName = NULL;
const char *szAuditPatientId = NULL;
const char *szAuditDeviceIp = NULL;

void audit_ExtractBasics(rogxml *rx)
{
	const char *szTimeDt=rogxml_GetValueByPath(rx, "/*/time/@dt");
	szAuditSourceProduct=rogxml_GetValueByPath(rx, "/*/source/@product");
	szAuditSourceModule=rogxml_GetValueByPath(rx, "/*/source/@module");
	szAuditUserId=rogxml_GetValueByPath(rx, "/*/user/@id");
	szAuditUserName=rogxml_GetValueByPath(rx, "/*/user/@name");
	szAuditPatientId=rogxml_GetValueByPath(rx, "/*/patient/@intid");
	szAuditDeviceIp=rogxml_GetValueByPath(rx, "/*/device/@ip");

	nAuditTimeDt = DecodeTimeStamp(szTimeDt);

	Log("dt=%d,sp='%s',sm='%s',ui='%s',un='%s',pi='%s',di='%s'", nAuditTimeDt, szAuditSourceProduct, szAuditSourceModule,szAuditUserId, szAuditUserName, szAuditPatientId, szAuditDeviceIp);
}

void audit_EntryUser(int fd, rogxml *rx)
{
	audit_ExtractBasics(rx);
	SendAck(fd, NULL, NULL, NULL, NULL, NULL);
	Log("Audit entry (User)");
}

void audit_EntryField(int fd, rogxml *rx)
{
	audit_ExtractBasics(rx);
	SendAck(fd, NULL, NULL, NULL, NULL, NULL);
	Log("Audit entry (Field)");
}

rogxml *audit_Report(rogxml *rxAudit)
// Accepts an audit-report XML (<MMTS-Audit>) and deals with it.  Returning the correct response to send back
// to the user - either an Ack or an error.
{
	const char *szFormat=rogxml_GetAttr(rxAudit, "format");
	const char *szFilename;
	const char *szPassword;
	const char *szRequesterId;
	const char *szRequesterName;
	const char *szDatabase;
	SSMAP *ss=ssmap_New();
	rogxml *rx;
	rogxml *rxUser = rogxml_FindChild(rxAudit, "user");
	rogxml *rxDatabase = rogxml_FindChild(rxAudit, "database");
	rogxml *rxCriteria = rogxml_FindChild(rxAudit, "criteria");
	rogxml *rxAck;

	if (!szFormat) szFormat="html";

	if (!rxUser) return ErrorMessage(401, 1, "No requesting user in MMTS-Audit report message");
	if (!rxDatabase) return ErrorMessage(402, 1, "No database given in MMTS-Audit report message");
	if (!rxCriteria) return ErrorMessage(403, 1, "No criteria section in MMTS-Audit report message");

	szRequesterId = rogxml_GetAttr(rxUser, "id");
	szRequesterName = rogxml_GetAttr(rxUser, "name");
	if (!szRequesterId) return ErrorMessage(404, 1, "No requesting user id in MMTS-Audit report message");

	szDatabase = rogxml_GetAttr(rxDatabase, "name");
	if (!szDatabase) return ErrorMessage(405, 1, "No database name given in MMTS-Audit report message");

	szFilename = GenFilename();
	szPassword = GenPassword(30);

	rxAck = AckMessage(NULL, NULL, NULL, NULL, NULL);
	rogxml_AddTextChild(rxAck, "Filename", szFilename);
	rogxml_AddTextChild(rxAck, "Authentication", szPassword);

	ssmap_Add(ss, "requester-id", szRequesterId);
	ssmap_Add(ss, "requester-name", szRequesterName);
	ssmap_Add(ss, "database", szDatabase);

	for (rx=rogxml_FindFirstChild(rxCriteria); rx; rx=rogxml_FindNextSibling(rx)) {
		rogxml *rxAttr=rogxml_FindFirstAttribute(rx);

		const char *szName = hprintf(NULL, "criteria-%s", rogxml_GetLocalName(rx));
		const char *szValue = rogxml_GetValue(rx);
		if (szValue) ssmap_Add(ss, szName, szValue);

		while (rxAttr) {
			const char *szValue = rogxml_GetValue(rxAttr);
			const char *szParam = hprintf(NULL, "%s-%s", szName, rogxml_GetLocalName(rxAttr));

			ssmap_Add(ss, szParam, szValue);
			szDelete(szParam);
			rxAttr = rogxml_FindNextSibling(rxAttr);
		}
		szDelete(szName);
	}
	ssmap_Add(ss, "filename", szFilename);
	ssmap_Add(ss, "password", szPassword);
	ssmap_Add(ss, "format", szFormat);
	QueueProcess("audit-report",ss);
	ssmap_Delete(ss);

	Note("T|%s", szFilename+6);					// szFilename always starts with 'Audit-'
	Note("F|%s", szRequesterId);
	Note("Y|%s", "AuditRequest");

	Log("Audit report (%s) to %s", szFormat, szFilename);
	szDelete(szFilename);
	szDelete(szPassword);

	return rxAck;
}

int DealWithEbxml(int fd, rogxml *rxEbxml)
// The Ebxml we have to send is contained within the wrapper element passed here
{
	rogxml *rxBody = rogxml_FindFirstChild(rxEbxml);
	const char *szWsdl = rogxml_GetAttr(rxEbxml, "wsdl");
	rogxml *rxWsdl = NULL;
	const char *szEndpoint = rogxml_GetAttr(rxEbxml, "endpoint");
	const char *szSoapAction = rogxml_GetAttr(rxEbxml, "soapaction");
	const char *szUser = rogxml_GetAttr(rxEbxml, "user");
	const char *szPassword = rogxml_GetAttr(rxEbxml, "password");
	int nTimeout = rogxml_GetAttrInt(rxEbxml, "timeout", config_EnvGetInt("ReplyWait", 30));
	// Timeout is timeout="xx" attribute, or config 'ReplyWait' or 30 seconds.

//	NoteFromMsg(msg);							// Note I, F, T, Y
//		Note("I|%s", msg_GetMessageId(msg));
//		Note("F|%s", msg_GetFromPartyId(msg));
//		Note("T|%s", msg_GetToPartyId(msg));
//		Note("Y|%s", msg_GetInteractionId(msg));
//		Note("C|%s", msg_GetConversationId(msg));

	if (!rxBody) {
		SendError(fd, 401, 1, "Empty EbXML message");
		return 1;
	}

	if (szWsdl) {
		const char *szWsdlFile = hprintf("%s/%s.wsdl", szDirWsdl, szWsdl);
		rxWsdl = rogxml_ReadFile(szWsdlFile);

		if (!rxWsdl) {
			SendError(fd, 402, 1, "Unable to read WSDL from %s", szWsdlFile);
			return 2;
		}
		szDelete(szWsdlFile);
		Note("Y|%s", szWsdl);
	}

	if (!szEndpoint && rxWsdl) {		// I think the following might take us to multiple locations
		szEndpoint = rogxml_GetValueByPath(rxWsdl, "/definitions/service/port/address/location");
	}

	if (!szEndpoint) {
		SendError(fd, 403, 1, "Cannot determine endpoint (neither in MMTS-ebxml not WSDL)");
		return 3;
	}

	if (rxBody) {
		rogxml *rxSoap = WrapSoap(NULL, rxBody);
		const char *szProtocol;
		const char *szAddress;
		int nPort;
		const char *szUri;

		int nErr=ValidateXml(rxBody);
		if (nErr) {
			SendError(fd, 409, 1, "XML Failed validation - details in log");
		}

		SplitEndpoint(szEndpoint, &szProtocol, &szAddress, &nPort, &szUri, 1);
		if (!szAddress || !nPort || !szUri) {
			SendError(fd, 404, 1, "Could not decode endpoint '%s'", szEndpoint);
			return 4;
		}

		Log("Sending SOAP over %s connection to %s:%d", szProtocol, szAddress, nPort);

		BIO *io=BuildConnection(szAddress, nPort);	// Give us a connection
		if (!io) {								// Couldn't for some reason
			SendError(fd, 406, 1, "Can't build connection (%s)", GetErrorStr());
			return 6;
		}
//Log("Connection established to %s:%d", szAddress, nPort);

		MSG *msgReply=NULL;
		MSG *msg=msg_New(rxSoap);
		NoteMessageMsg(msg, "sent", "SOAP sent");

		msg_SetEndpoint(msg, szEndpoint);
		if (szSoapAction) {
			msg_SetSoapAction(msg, szSoapAction);
		} else {
			SendError(fd, 407, 1, "No SOAPAction supplied");
			return 7;
		}

		if (szUser && szPassword) {
			msg_SetAttr(msg, "auth-type", "Basic");
			msg_SetAttr(msg, "auth-user", szUser);
			msg_SetAttr(msg, "auth-password", szPassword);
		}

		if (!RawSendMessage(io, msg, 0, 0, NULL)) {
			SendError(fd, 408, 1, "Error writing message on established connection");
			return 8;
		} else {
			Log("Waiting for response (up to %d second%s)", nTimeout, nTimeout==1?"":"s");
			msgReply=http_GetMessage(io, nTimeout);
		}

		if (msgReply) {
			NoteMessageMsg(msgReply, "response", "SOAP response");
			rogxml *rxResponse = msg_GetXML(msgReply);
			const char *szFault = rogxml_GetValueByPath(rxResponse, "/Envelope/Body/Fault/faultstring");
			if (szFault) {
				const char *szCode = rogxml_GetValueByPath(rxResponse, "/Envelope/Body/Fault/faultcode");

				if (szCode) {
					SendError(fd, 410, 1, "%s: %s", szCode, szFault);
				} else {
					SendError(fd, 410, 1, "%s", szFault);
				}
				return 10;
			}
			rogxml *rxBody = rogxml_FindByPath(rxResponse, "/Envelope/Body/*");
			rogxml_LocaliseNamespaces(rxBody);
			if (rxBody) {
				SendXML(fd, rxBody, "Response");
			} else {
				SendError(fd, 0, 0, "Received a non-SOAP result...");
			}
		} else {
			SendError(fd, 411, 1, "No SOAP response before timeout (%d sec%s)", nTimeout, nTimeout==1?"":"s");
		}
	}

	return 0;
}

void CloseInternalConnection(int fd)
{
	if (bSayBye) {													// 21822 - Say 'bye' only if requested
		rogxml *rxBye=rogxml_NewElement(NULL, "MMTS-Bye");
		SendXML(fd, rxBye, "goodbye");
		rogxml_Delete(rxBye);
	}

	shutdown(fd, SHUT_WR);
	close(fd);
}

void AcceptIpConnection(int fd, int sock)
{
	rogxml *rx;
	MSG *msgReply = NULL;
	const char *szError;
	MSG *msg;
	const char *szOutgoingMessageId = NULL;		// Very handy thing to have later on (when we want to ack)
	int nErr;
	const char *szDelay;						// Delay from 'delay="whatever"' attribute
	int nSendErr = 0;

	const char *szPeerDescr = allow_Allowed(_szSenderIp);

	if (!szPeerDescr) {							// We don't know this peer
		const char *szReject;

		szReject = hprintf(NULL, "Connection from unknown peer (%s) rejected", _szSenderIp);

		Note("S|%s|%d", _szSenderIp, _nSenderPort);
		Note("H|Rejected (%d)", _nIncomingPort);
		Log("Rejecting connection from unknown peer");
		SendError(fd, 101, 1, "%s", szReject);
		CloseInternalConnection(fd);
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

	Note("S|%s|%d", _szSenderIp, _nSenderPort);
	Note("H|App %s", szPeerDescr);
	Note("J|%s", InternalId());

	msg_SetLogDir(NoteDir());

	//// Accept an XML message from it
	rx=ReceiveXML(fd, 30);						// Accept an XML message back

	//// See if it translates and if not, send an error back and quit
	szError=rogxml_ErrorText(rx);
	if (szError) {
		if (rx) {
			Log("XML Error %d: %s", rogxml_ErrorNo(rx), rogxml_ErrorText(rx));
			Note("Z|%s", rogxml_ErrorText(rx));
		} else {
			Log("XML Error %d: No XML received", rogxml_ErrorNo(rx));
			NoteInhibit(1);						// Get rid of the evidence
		}
		SendError(fd, 102, 1, "%s", szError);
		CloseInternalConnection(fd);
		Exit(0);
	}

	bSayBye = rogxml_GetAttrInt(rx, "saybye", bSayBye);		// Arrange to send 'MMTS-Bye' if requested (21822)

	szEnvironment=rogxml_GetAttr(rx, "environment");		// See if we're forcing an environment
	if (!IsValidEnvironment(szEnvironment)) {				// Get environment if there wasn't a valid one
		GetEnvironment(_szSenderIp, _nIncomingPort);
	}
	Note("E|%s", szEnvironment);

	const char *szPacked = rogxml_GetAttr(rx, "packed");	// Forcing packed mode or not
	if (szPacked) {
		_bPackXML = atoi(szPacked);
		rogxml *rxPackedAttr=rogxml_FindAttr(rx, "packed");			// Pretend we didn't see it
		rogxml_Delete(rxPackedAttr);
		rx=ReceiveXML(fd, 30);								// Accept a second message
		Log("Packed response is turned %s", _bPackXML ? "on" : "off");
	}

	//// Log the message away
	NoteMessageXML(rx, "rcv", "Message as received from application");		// Save log of message before translation

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
		CloseInternalConnection(fd);
		exit(0);
	}

	if (!strcmp(rogxml_GetLocalName(rx), "MMTS-RPC")) {			// It's a Remote Procedure Call
		rogxml *rxResponse;

		rxResponse=MMTSRpc(rx);
		Note("Y|%s", "RPC");
		if (szRpcError) {
			rogxml_SetAttr(rxResponse, "errorlog", NoteDirSuffix());
		}
		SendXML(fd, rxResponse, "response");
		rogxml_Delete(rxResponse);
		CloseInternalConnection(fd);
		NoteRpcError();
		Log("Internal Connection Terminated Normally");

		return;
	}

	if (!strcmp(rogxml_GetLocalName(rx), "MMTS-Control")) {		// It's an administration request
		rogxml *rxResponse;

		Log("MMTS-Control received");
		rxResponse=MMTSControl(rx);
		SendXML(fd, rxResponse, "response");
		rogxml_Delete(rxResponse);
		CloseInternalConnection(fd);
		Log("Internal Connection Terminated");

		return;
	}

	if (!strcmp(rogxml_GetLocalName(rx), "MMTS-Request")) {		// It's a special (request for GUID etc)
		rogxml *rxResponse;

		Log("MMTS-Request received");
		rxResponse = MMTSResponse(rx);
		SendXML(fd, rxResponse, "response");
		rogxml_Delete(rxResponse);
		CloseInternalConnection(fd);
		Log("Internal Connection Terminated");
		return;
	}

	if (!strcmp(rogxml_GetLocalName(rx), "MMTS-SDS")) {
		rogxml *rxResponse;

		Log("MMTS-SDS Lookup");
		rxResponse = MMTSSDS(rx);
		SendXML(fd, rxResponse, "response");
		rogxml_Delete(rxResponse);
		CloseInternalConnection(fd);
		Log("Internal Connection Terminated");
		return;
	}

	if (!strcmp(rogxml_GetLocalName(rx), "MMTS-Audit")) {		// Audit message - just log and ack for now
		const char *szAction;

		szAction=rogxml_GetAttr(rx, "action");
		if (!strcmp(szAction, "entry")) {
			const char *szType=rogxml_GetAttr(rx, "type");

			if (!strcmp(szType, "field")) {
				audit_EntryField(fd, rx);
			} else if (!strcmp(szType, "user")) {
				audit_EntryUser(fd, rx);
			} else if (szType) {
				SendError(fd, 201, 1, "Unrecognised audit type '%s' (should be field or user)", szType);
			} else {
				SendError(fd, 202, 1, "No audit type given (should be field or user)");
			}
		} else if (!strcmp(szAction, "report")) {
			rogxml *rxResponse = audit_Report(rx);

			SendXML(fd, rxResponse, "response");
			rogxml_Delete(rxResponse);
		} else if (szAction) {
			SendError(fd, 204, 1, "Unrecognised audit action '%s' (should be entry or report)", szAction);
		} else {
			SendError(fd, 205, 1, "No audit action given (should be entry or report)");
		}
		CloseInternalConnection(fd);
		Log("Internal Connection Terminated");
		return;
	}

	if (!strcmp(rogxml_GetLocalName(rx), "MMTS-ebxml")) {		// Miscellaneous ebXML message, deal with it
		DealWithEbxml(fd, rx);
		return;
	}

//
// At this point, we have received a message that we're going to send to the spine
//

	TranslateIds(rx);
	HarvestNhs(rx);								// Collect any NHS number for future searching

	//// Turn it into a 'msg' (unwrapped for now)
//	Log("Transforming to internal message");
	char bWaitForReply = rogxml_GetAttrInt(rx, "waitforreply", 1);
	char bWantAck = rogxml_GetAttrInt(rx, "wantack", 0);

	// Get the MMTS Protocol version
	const char* szMmtsProtocol = rogxml_GetAttr(rx, "protocol");
	if( szMmtsProtocol && strcmp(szMmtsProtocol,"EVO12") == 0 )
		mmtsProtocol = EVO12;
	Log("mmtsProtocol set to %s", szMmtsProtocol);

	msg = msg_NewFromInternal(rx);
	if (!msg_GetErrorNo(msg)) {
		DoWrapMessage(msg);
	}

	NoteFromMsg(msg);							// Note I, F, T, Y

	if (msg_GetErrorNo(msg)) {
		SendError(fd, msg_GetErrorNo(msg), 1, "%s", msg_GetErrorText(msg));
		CloseInternalConnection(fd);
		Fatal("Error %d packing message - %s", msg_GetErrorNo(msg), msg_GetErrorText(msg));
	}

	// Respond to caller after initial processing and drop out if it was a flop
	if ((nErr = SendResponse(fd, msg))) {
		Fatal("Error %d processng local message", nErr);
	}

	szOutgoingMessageId = strdup(msg_GetMessageId(msg));
// IPCDELETE - this is never used by the recipient
//ipc_SendDaemon(ipc_handling, -1, szOutgoingMessageId);		// Tell the boss what we're doing

	const char *szInteraction = msg_GetInteractionId(msg);
	int nDelay;
	if ((nDelay = MessageOption(szInteraction, "delay"))) {
		Log("messageopt(%s): Delaying %d second%s", szInteraction, nDelay, nDelay==1?"":"s");
		sleep(nDelay);
	}

	if (MessageOption(szInteraction, "drop")) {
		Log("messageopt(%s): Dropping message", szInteraction);
		CloseInternalConnection(fd);
		exit(0);
	}

	//// If the caller doesn't want to wait, treat it like a dropped file
	if (!msg_IsLocalSync(msg)) {				// Async send
		MSG *msgReply = SendAsyncMessage(msg);
		rogxml *rx=msg_GetXML(msgReply);
		int nErr = msg_GetErrorNo(msgReply);

		if (nErr) {
			const char *szErr = msg_GetErrorText(msgReply);

			Log("Reporting error %d (%s) to application", nErr, szErr);
			SendError(fd, nErr, 1, "%s", szErr);
		} else {
			NoteMessageMsg(msgReply, "appreply", "Application level response from server");	// Save log of message sent
			Log("Returning application level response to application");
			SendXML(fd, rx, "application response");			// Send the reply back before shutting the door
		}
		CloseInternalConnection(fd);
		Exit(0);
	}

	//// If Nasp then just send and collect ack, if ebXML mark it as waiting and wait for a real reply
	if (msg_IsNasp(msg)) {							// Nasp so wait for a response
		msgReply = SendMessage(msg);
//TODO: Only want one of the following two!
		if (msg_GetErrorNo(msgReply)) {
			Log("NASP: Error in reply %d: %s", msg_GetErrorNo(msgReply), msg_GetErrorText(msgReply));
		}
		if (msg_GetErrorNo(msg)) {
			Log("NASP: Error %d: %s", msg_GetErrorNo(msg), msg_GetErrorText(msg));
		}
	} else {
		MSG *msgResponse = NULL;
		int nResponseType = 0;									// Why didn't I make this an enum...?
		int bNeedRetry = 0;										// We want to try sending again

		MarkWaiting(msg);
		msgResponse = SendMessage(msg);
		if (msgResponse == NULL) {
			nResponseType = 0;									// 0 - No response
		} else if (msg_GetHttpCodeMajor(msgResponse) != 0) {	// Something was amiss with the message
			nResponseType = 1;									// 1 - Error code from other end
			rogxml *rx=msg_GetXML(msgResponse);
			if (!rx) {
				rx=MakeNegAcknowledgement(msg, "microtest:102", msg_GetHttpCodeMajor(msgResponse), msg_GetHttpCodeText(msgResponse));
				msg_SetXML(msgResponse, rx);
			}
		} else {
			if (msg_GetErrorNo(msgResponse)) {
				nResponseType = 2;								// 2 - Error within response
			} else {
				rogxml *rx=msg_GetXML(msgResponse);

				if (!rx) {
					nResponseType = 3;							// 3 - Couldn't get any XML from response
				} else {
					if (msg_IsAck(msgResponse)) {
						nResponseType = 4;						// 4 - Ack
					}
					if (msg_HasPayload(msgResponse)) {
						nResponseType = 5;						// 5 - Has payload
					}
				}
			}
		}

		if ((nResponseType == 4 || nResponseType == 5) &&
			MessageOption(msg_GetInteractionId(msg), "ignoreresponse")) {
			Log("Acknowledgement message ignored due to msgopt setting");
			nResponseType = 3;
		}

		switch (nResponseType) {
		case 0:									// 0 - Too many retries or too long to hang around
			Log("Response: No valid reply - needs a retry");
			bNeedRetry = 1;							// We check if we've tried enough already further down
			break;
		case 1:									// 1 - HTTP error returned from the far end
			Log("Response: Non-OK response received (%d), returning it to the app", msg_GetHttpCode(msgResponse));
			MarkAcknowledged(szOutgoingMessageId);			// Stop any re-trying activity

			msgReply = msgResponse;				// Message for app is in the response hopefully
			break;
		case 2:									// 2 - Message returned with error in it (can't really happen)
			// In fact it does, for example if a route can't be found to the endpoint
			Log("Response: MMTS Error %d - %s", msg_GetErrorNo(msgResponse), msg_GetErrorText(msgResponse));
			msgReply = msgResponse;				// Message for app is in the response hopefully
			break;
		case 3:									// 3 - NULL message returned (i.e. blank HTTP message - normal)
			Log("Response: None (OK)");
			break;
		case 4:									// 4 - Response came back with an acknowledgement
			if (bWantAck) {
				Log("Response: Acknowledgement (passing to caller)");
				msgReply = msgResponse;				// This will trigger the marking of acknowledgement
			} else {
				Log("Response: Acknowledgement (dropping)");
				MarkAcknowledged(szOutgoingMessageId);			// Stop any re-trying activity
			}
			break;
		case 5:									// 5 - Response came back with a payload (implies acknowledgement)
			Log("Response: Message with payload");
			MarkAcknowledged(szOutgoingMessageId);			// Stop any re-trying activity
			msgReply = msgResponse;					// We have our reply
			break;
		}

		if (bNeedRetry) {
			if (msg_GetSends(msg) >= msg_GetMaxRetries(msg)) {	// We've already tried enough times
				MarkAcknowledged(szOutgoingMessageId);		// Stop any re-trying activity
			} else {
				// Now, this might seem silly...
				// The problem is, we've failed to send so we're going to have to let the re-try logic
				// pick up the unacknowledged message and keep banging away with it until it works or
				// fails.  However, the saved message has our process id stamped on it so the re-try part
				// will assume we're dealing with it.  We need to have a dead process number on it.
				if (!spiderFork("RESEND")) {								// Create us a new process, which is this block
					SaveUnacked(msg);						// Save the message with the child's process ID
					exit(0);								// Kill the child
				}
			}
		}

		if (bWaitForReply && !msgReply) {					// Don't have a reply yet so wait for it to arrive
			msgReply = WaitForAsyncReply(msg, fd, sock);
		}

		if (msgReply != msgResponse) msg_Delete(msgResponse);
	}

	if (msgReply && MessageOption(msg_GetInteractionId(msgReply), "drop")) {
		Log("messageopt: Reply message dropped");			// After all that, they don't want it!
		msg_Delete(msgReply);
		msgReply=NULL;
	}

	//// Something has returned from the far end so deal with it
	if (msgReply) {
		rogxml *rx;
		const char *szInteractionName = NULL;
		const char *szInteraction = NULL;
		const char *szDescription = NULL;
		int nErr = msg_GetErrorNo(msgReply);					// Non-0 if response was an error

		MarkAcknowledged(szOutgoingMessageId);					// Stop any re-trying activity

		msg_Unwrap(msgReply);
		if (!nErr) {											// No error, phew
			rx=msg_ReleaseXML(msgReply);
			NoteMessageXML(rx, "rsp", "Message received from peer");
			rx=TranslateXML("oi", "any", NULL, rx);
			NoteMessageXML(rx, "rtx", "Message received and translated");
			msg_SetXML(msgReply, rx);
		}

		szInteraction = msg_GetInteractionId(msgReply);
		szInteractionName = MessageDescription(szInteraction, NULL);
		if (szInteractionName) {
			szDescription = hprintf(NULL, "%s (%s)", szInteraction, szInteractionName);
		} else if (szInteraction) {
			szDescription = hprintf(NULL, "%s (unknown)", szInteraction);
		} else if (nErr) {
			szDescription = hprintf(NULL, "error %d (%s)", nErr, msg_GetErrorText(msgReply));
		} else {
			szDescription = strdup("unknown message type");
		}

		Log("Sending internal %s", szDescription);
		szDelete(szDescription);

		szTmpDir = msg_DropAsTmpIn(msgReply, msg);			// 21724: Drop as incoming, pending successful send to app
		nSendErr = SendInternalMessage(fd, msgReply);			// Destroys msgReply
		if (nSendErr) {
			Log("Error %d sending final response to internal connection", nSendErr);
			indir_Enable(szTmpDir);								// Make our internal drop live
		} else {
			// Collect an acknowledgement from Evolution and still drop if we don't get one. (Mantis 13881)
			char bAcked = 1;

			int nAckWait = config_EnvGetInt("AckWait", 10);	// Setting this to 0 uses old method

			if (nAckWait) {
				bAcked = 0;
				rogxml *rxAck=ReceiveXML(fd, nAckWait);			// Accept an XML message back

				if (!rogxml_ErrorNo(rxAck)) {					// No error
					if (!strcmp(rogxml_GetLocalName(rxAck), "MMTS-Ack")) {		// And it was an ack - good
						bAcked=1;
						NoteMessageXML(rxAck, "intack", "Internal acknowledgement");
					}
				}
				if (!bAcked) {
					NoteMessageXML(rxAck, "intack", "Bad internal acknowlegdement");
				}
				rogxml_Delete(rxAck);
			}

			if (bAcked) {
				if (nAckWait) {
					Log("Received acknowledgement from internal connection");
				} else {
					Log("Appears delivered internally and not configured for positive internal acknowledgements");
				}
				RogZap(szTmpDir);								// Forget about internal drop
			} else {
				Log("No confirmation of receipt of internal message, dropping instead");
				indir_Enable(szTmpDir);							// Make our internal drop live
			}
		}
		msgReply=NULL;
	} else if (bWaitForReply) {
		nSendErr = SendError(fd, 104, 1, "No reply received");
		if (nSendErr) {
			Log("Error %d sending error 104 (No reply received) to internal connection", nSendErr);
		}
	}

	CloseInternalConnection(fd);
	if (nSendErr) {
		Log("Internal Connection Terminated - lost contact");
	} else {
		Log("Internal Connection Terminated ok");
	}

	msg_Delete(msg);							// No longer need it

	Exit(0);
}

void AcceptFileConnection(const char *szFilename)
{
	rogxml *rx;
	const char *szError;

	rx=rogxml_ReadFile(szFilename);
	unlink(szFilename);

	Note("H|File drop");
	Note("J|%s", InternalId());

	rx=rogxml_FindDocumentElement(rx);
	szError=rogxml_ErrorText(rx);
	if (szError) {
		Log("%s", szError);
	} else {
		MSG *msg;
		MSG *msgReply;
		const char *szEnv = rogxml_GetAttr(rx, "environment");

		//// Log the message away
		NoteMessageXML(rx, "rcv", "Message as dropped");			// Save log of message before translation
		if (szEnv) SetEnvironment(szEnv);
		Note("E|%s", szEnvironment);

		// Purely to make the display more descriptive if this is actually a previous delayed message
		const char *szDelay = rogxml_GetAttr(rx, "delay");
		if (szDelay) Note("H|Delayed");				// Make it easier to spot

		TranslateIds(rx);
		HarvestNhs(rx);								// Collect any NHS number for future searching
		msg = msg_NewFromInternal(rx);								// NB. rx is dead from here on!

		// Wrap it up
		if (!msg_GetErrorNo(msg))
			DoWrapMessage(msg);

		if (msg_GetErrorNo(msg)) {
			Log("Error %d packing message - %s", msg_GetErrorNo(msg), msg_GetErrorText(msg));
			Exit(0);
		}

		msgReply = SendAsyncMessage(msg);
		if (msgReply) {
			if (msg_GetErrorNo(msgReply)) {
				Log("Error %d - %s received", msg_GetErrorNo(msgReply), msg_GetErrorText(msgReply));
				DeliverMessage(msgReply, NULL);
			} else {
				DeliverMessage(msgReply, NULL);
			}
		}
		Exit(0);
	}

	Exit(0);
}

const char *CheckDropDir()
// Checks the drop dir for any outgoing messages.
// If one is found, it is renamed to stop it being found twice and the new name is returned.
// If something fails, we create 'DISABLED' in the drop dir and avoid reading while it is there.
// If we can't create the DISABLED file, an internal flag is set and we simply don't try again.
// Returns	const char *	Name of file
//			NULL			No file found
{
	int nMaxNameLen = strlen(szOutDir)+MAXNAMLEN+4;
	static char bDisabled = 0;
	char *szDisabled;
	char *szResult = NULL;
	static time_t tLastFound = 0;				// So we dont' get them too often
	int nDropRate = config_GetInt("DropRate", 30);	// Minimum number of seconds between accepting dropped files

	if (bDisabled) return NULL;					// Been disabled due to a major problem

	if (tLastFound + nDropRate > time(NULL))	// Not ready to find any more yet
		return NULL;

	szDisabled = malloc(nMaxNameLen);				// Filename of disablement file

	snprintf(szDisabled, nMaxNameLen, "%s/DISABLED", szOutDir);

	if (access(szDisabled, 0)) {					// Not been disabled
		char *szSrcName = malloc(nMaxNameLen);
		char *szDestName = malloc(nMaxNameLen);
		struct dirent *d;
		DIR *dir=opendir(szOutDir);
		time_t now = time(NULL);

		if (dir) {
			while ((d=readdir(dir))) {
				int nNameLen;
				struct stat st;

				if (*d->d_name == '.') continue;		// Skip '.' files
				nNameLen=strlen(d->d_name);
				if (nNameLen < 4 || strcasecmp(d->d_name+nNameLen-4, ".xml"))
						continue;						// Skip non .xml
				snprintf(szSrcName, nMaxNameLen, "%s/%s", szOutDir, d->d_name);
				if (!stat(szSrcName, &st)) {			// Check if it's to be delayed
					if (st.st_mtime > now) {			// File time is in the future
						continue;						// I can't help thinking this should be logged in some way
					}
				}
				snprintf(szDestName, nMaxNameLen, "%s/%-.*s.tmp", szOutDir, nNameLen-4, d->d_name);
				if (!rename(szSrcName, szDestName)) {	// Renamed .xml to .tmp ok
					szResult = strdup(szDestName);		// Our result
					tLastFound = time(NULL);
				} else {								// Won't normally happen, but awful if it does...!
					FILE *fp;

					Log("Rename failed (error %d) - drop dir disabled", errno);
					Log("Source: %s", szSrcName);
					Log("Dest: %s", szDestName);
					fp=fopen(szDisabled, "w");
					if (fp) {
						fprintf(fp, "Disabled due to error %d renaming from '%s' to '%s'",
								errno, szSrcName, szDestName);
						fclose(fp);
					} else {
						bDisabled=1;					// Disable really nastily
					}
				}
				break;
			}
			closedir(dir);
		}

		free(szSrcName);
		free(szDestName);
	}

	free(szDisabled);

	return szResult;
}

int GetIntervalSeconds(const char *szInterval)
{
	return 20;				// Assume PT20S for now
}

void RetryUnacknowledged(const char *szName, MSG *msg)
// szName is the name of the file with no path (it's in szUnackedDir)
// msg is the conveniently already loaded message
{

	Log("Retrying %s", szName);

	if (msg_GetSends(msg) >= msg_GetMaxRetries(msg)) {
		const char *szId = msg_GetMessageId(msg);
		MSG *msgNack;

		Note("H|Last retry");
		NoteFromMsg(msg);							// Note I, F, T, Y
		Log("MAX RETRIES (%d): %s", msg_GetMaxRetries(msg), szId);
		MarkFailed(szId);
		msgNack = MakeNegAcknowledgementMessage(msg, "microtest:101", 101, "Exceeded maximum retries");
		DeliverMessage(msgNack, NULL);
		return;
	}

	Note("H|Retry");
	NoteFromMsg(msg);								// Note I, F, T, Y
	SendAsyncMessage(msg);							// send it again (or try to)
	Exit(0);
}

void MaybeRetryUnacknowledged()
// Consider retrying unacknowledged messages
{
	static int nNextCheck = 0;						// How long we'll go before we actually check
	DIR *dir;

	nNextCheck--;
//Log("Next check in %d", nNextCheck);
	if (nNextCheck > 0) return;						// Not yet...

	dir=opendir(szUnackedDir);

	nNextCheck = 30;								// 30 seconds unless there is a shorter requirement
	if (dir) {
		struct dirent *d;

		while ((d=readdir(dir))) {
			const char *szName = d->d_name;
			const char *szPathname;
			const char *szNow = TimeStamp(0);
			MSG *msg;

			if (*szName == '.') continue;			// Skip '.' files
			if (strlen(szName) < 4) continue;		// Too short to have .msg on the end
			if (strcmp(szName+strlen(szName)-4, ".msg")) continue;	// Doesn't end in .msg

			szPathname=hprintf(NULL, "%s/%s", szUnackedDir, szName);
//Log("Looking at %s", szPathname);
			msg = msg_Load(szPathname);
			if (msg) {
				if (strcmp(msg_GetNextSend(msg), szNow) <= 0) {			// It's due
					int pid;
//Log("%s is due...", szName);
					pid=msg_GetPid(msg);								// See if it's being dealt with
					if (ProcessAlive(pid)) {
						Log("%d already dealing with %s", pid, szName);
					} else {
						int pid;

						if ((pid=spiderFork("RETRY"))) {
							int nChildren = child_Add(pid, NULL, msg_GetMessageId(msg));
							Log("Spawned child pid (%d child%s now) to process %s",
									nChildren, nChildren==1?"":"ren", msg_GetMessageId(msg));
						} else {
							bIsDaemon=0;						// We are but a child...
							summ_ChildStart(SRC_RETRY);
//							ipc_Id(getpid());					// Create our IPC channel
							RetryUnacknowledged(szName, msg);
							exit(0);
						}
					}
				} else {
					int nSecs = GetIntervalSeconds(msg_GetRetryInterval(msg));
					Log("%s is happy (%s>%s) - retry in %d", szName,msg_GetNextSend(msg), szNow, nSecs);
					if (nSecs < nNextCheck) nNextCheck=nSecs;			// Check sooner if necessary
				}
				msg_Delete(msg);
			}
		}
		closedir(dir);
	}
}

#if 0 // Removed all IPC code IPCREMOVED
const char *ipc_Process()
// Checks for any incoming messages and processes them.
// Will process as many messages as there are in the queue unless an ipc_response or ipc_sendmemory is found,
// in which case it returns
// Returns non-NULL in the following cases:
//	An ipc_response was received
//	An ipc_sendmemory was received (and memory collected).  Use shm_ReceivedSize() and shm_ReceivedData() to pick it up
{
	int nGot = 0;
	const char *szResponse = NULL;

	while (nGot != -1 && !szResponse) {
		myipc msg;

		msg.mtype=0;
		nGot = msgrcv(ipc_Id(getpid()), &msg, sizeof(msg), 0, IPC_NOWAIT);
		if (nGot >= 0) {
Log("MAIN: Have message %d - '%s'", msg.mtype, msg.mtext);
			switch (msg.mtype & 0xff) {
			case ipc_log:									// Experimental message
				Log("[%d] %s", msg.mpid, msg.mtext);
				break;
#if 0		// IPCDELETE - The ID set below is never used
			case ipc_handling:								// Being told that something is being handled
				child_SetId(msg.mpid, msg.mtext);
				break;
#endif
			case ipc_whohas:								// Process asking who is processing a message
#if 0		// IPCDELETE - there are no other live references to ipc_whohas
				{
					int i;

					for (i=0;i<nPids;i++) {
						if (!strcmp(msg.mtext, aPid[i].szId)) {
							ipc_Sendf(msg.mpid, ipc_response, "%d:%d:%s", ipc_whohas, aPid[i].pid, aPid[i].szId);
							break;
						}
					}
				}
#endif
				break;
			case ipc_response:								// A generic message response string
				szResponse = strdup(msg.mtext);				// Setting this non-null triggers exit
				break;
			case ipc_sendmemory:							// We've been sent some memory
			{
				int id = atoi(msg.mtext);
				const char *mem=shm_Receive(id);
				if (!mem) {
					Log("Sent memory from %d (%s) but can't collect it", msg.mpid, msg.mtext);
				}
//				ipc_Sendf(msg.mpid, ipc_response, "thanks");
				if (mem) {
					const char *chp = strchr(msg.mtext, ':');		// Format is memid:tag

					szResponse = strdup(chp?chp+1:msg.mtext);		// Return tag only if memory received ok
				}
			}
				break;
			default:
				Log("Message from %d, type %d = '%s'", msg.mpid, msg.mtype, msg.mtext);
				break;
			}
		}
	}

	return szResponse;
}
#endif

void ArchiveOldDirs(int verbose)
{
	time_t now=time(NULL);
	struct tm *tm = gmtime(&now);

	int fromDay = tm->tm_mday;
	int fromMonth = tm->tm_mon+1;
	int fromYear = tm->tm_year % 100;

	int days = config_GetInt("archive_days", 7);
//Log("Now is %d/%d/%d", fromDay, fromMonth, fromYear);
	SubtractDays(days, &fromDay, &fromMonth, &fromYear);
//Log("%d days ago is %d/%d/%d", days, fromDay, fromMonth, fromYear);
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
										if (thisCode < fromCode && isDir(dayDir)) {
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

void Idle()
// It is important that we visit here approximately twice a second as there are timer
// comparisons that depend on it.
{
	int childpid;
	int status;
	static int nCounter = 0;
	static int nTimeCheck = 0;							// Do a time check at this time
	time_t now=time(NULL);
	static time_t nTimeTrack = 0;						// Our rough time
	int i;
	static const char *szTimeServers=NULL;
	static time_t tConfigChange = 0;
	time_t tNewConfigChange = config_LastChange();
	time_t tLastTimeWarning = 0;						// When we last warned about ntpdate being slow
	time_t tLastTimeSevereWarning = 0;					// When we last warned about ntpdate being slow

	static int nTimerPid = 0;							// Process id of timer process
	static int nTimerProcessStarted = 0;				// Timer process start in relation to nTimeTrack
	static time_t tTimerProcessStarted = 0;				// Timer process start in relation to time()

	static long daycode = 0;							// Code for the day that changes at midnight

	if (tNewConfigChange != tConfigChange || !szTimeServers) {	// Config changed or time servers not yet set
		tConfigChange = tNewConfigChange;
		const char *szNewTimeServers = config_GetString("ntpservers");
		if (!szNewTimeServers) szNewTimeServers=strdup("cns0.nhs.uk cns1.nhs.uk");

		if (!szTimeServers || strcmp(szTimeServers, szNewTimeServers)) {	// Current unset or they've changed
			szDelete(szTimeServers);					// Drop any old ones
			szTimeServers = szNewTimeServers;
			tLastTimeWarning=0;							// Re-enable trouble warnings
			tLastTimeSevereWarning=0;					// Re-enable trouble warnings
			if (!nTimerPid) {							// Schedule a time check if not currently doing one
				Log("Time servers set to %s - scheduling time check now", szTimeServers);
				nTimeCheck = nTimeTrack;				// Set next time check due to now
			} else {
				Log("Time servers set to %s - change will occur at next time check (timerpid=%d)", szTimeServers, nTimerPid);
			}
		} else {										// Didn't change so forget new ones
			szDelete(szNewTimeServers);
		}
	}

	if (!nTimeTrack) nTimeTrack = now;					// Initialise to current time
	nCounter++;

#if 0 		// IPCREMOVED
	const char *szMessage = ipc_Process();				// Check any IPC
	if (szMessage) {
		Log("Daemon received unsolicited message '%s'");
		szDelete(szMessage);
	}
#endif

	if (bIsDaemon) {
		// Don't put anything that doesn't relate to time here - look further down

		if (nCounter & 1) {								// Once a second
// Don't do this any more as we don't leave these things there any longer
//			MaybeRetryUnacknowledged();					// Consider retrying unacknowledged messages

			nTimeTrack++;								// Keep our idea of the time up to date

			if (!nTimerPid) {								// Not currently running the timer
				if (nTimeTrack > nTimeCheck) {				// The time to do a time check is nigh
					nTimerProcessStarted = nTimeTrack;		// Remember when we started
					tTimerProcessStarted = now;
					nTimerPid = spiderFork("TIMECHECK");

					if (!nTimerPid) {						// We're the child process
						char *servers=strdup(szTimeServers);
						char *param[20];					// NB. Value in loop below must be one less than this
						int i=0;
						char *arg;

						param[0]="/etc/ntpdate";
						param[1]="-b";
						i=2;
						while ((arg=strtok(servers, " ,;")) && i<19) {	// This must be one less than size of param[]
							if (*arg) param[i++] = arg;
							servers=NULL;
						}
						param[i]=NULL;
						// { char **p=param; while (*p) { Log("Arg: '%s'", *p); p++; } }
						int err=execv(param[0], param);
						Log("Error %d (%d) executing time server (%s) failed", errno, err, param[0]);
						exit(0);
					} else {
						if (bDebug) Log("Time being checked by process %d using %s", nTimerPid, szTimeServers);
					}
				}
			} else {										// Timer is running - are we worried yet?
				if (ProcessAlive(nTimerPid)) {				// It's still running
					if (nTimeTrack-nTimerProcessStarted == 10) {			// Been 10 secs...
						if (now-tLastTimeWarning > 3600)
							Log("Time check has taken over %d seconds - may be a problem...", now-tTimerProcessStarted);
						tLastTimeWarning=now;
					} else if (nTimeTrack - nTimerProcessStarted == 30) {	// Way too long (30 secs)
						if (now-tLastTimeSevereWarning > 3600)
							Log("Time check has taken over %d seconds - not trustable, killing it", now-tTimerProcessStarted);
						tLastTimeSevereWarning=now;
						kill(nTimerPid, 9);					// Kill it
					} else if (nTimeTrack - nTimerProcessStarted == 40) {	// 40 secs and it didn't die???
						struct tm *tm;

						nTimerPid = 0;						// Forget the process
						nTimeTrack = time(NULL);			// Re-align our idea of the time
						nTimeCheck = nTimeTrack+1800;		// Re-schedule for 1/2 hour
						tm=gmtime((time_t*)&nTimeCheck);			// Next try
						Log("*** Time process has run amock - will try again in 1/2 hour");
						Log("*** Time servers in use: %s", szTimeServers);
						Log("*** Next time check will be at %02d-%02d-%02d %02d:%02d:%02d",
								tm->tm_mday, tm->tm_mon+1, tm->tm_year % 100,
								tm->tm_hour, tm->tm_min, tm->tm_sec);
					}
				} else {									// Was alive, now dead - it's finished then
				}
			}
		}

		// Only put things that need to be run/checked regularly here as there is a chance the time/date
		// will be screwy beforehand.

		errno=0;
		while ((childpid = waitpid(-1, &status, WNOHANG)) > 0) {
			int nChildren;

			if (childpid == nTimerPid) {					// Timer process terminating so deal with it
				time_t now=time(NULL);
				time_t tPredicted = tTimerProcessStarted + nTimeTrack - nTimerProcessStarted;
				if (WIFSIGNALED(status)) {
					Log("Timer process %d was killed (-%d)", nTimerPid, WTERMSIG(status));
				} else {
					int exitstatus = WEXITSTATUS(status);
					if (exitstatus != 0) {						// Something went wrong with our child!
						Log("*** Error %d running ntpdate -b %s", exitstatus, szTimeServers);
					} else {								// Normal timer process exit with status=0 - yay!
						if (tPredicted < now-2 || tPredicted > now+2) {		// Something worth reporting
							struct tm tmPredicted;
							struct tm tmActual;

							gmtime_r(&tPredicted, &tmPredicted);
							gmtime_r(&now, &tmActual);

							Log("*** I expected it to be %02d-%02d-%02d %02d:%02d:%02d",
									tmPredicted.tm_mday, tmPredicted.tm_mon+1, tmPredicted.tm_year % 100,
									tmPredicted.tm_hour, tmPredicted.tm_min, tmPredicted.tm_sec);
							Log("*** It turns out to be  %02d-%02d-%02d %02d:%02d:%02d",
									tmActual.tm_mday, tmActual.tm_mon+1, tmActual.tm_year % 100,
									tmActual.tm_hour, tmActual.tm_min, tmActual.tm_sec);
							Log("*** Time servers in use: %s", szTimeServers);
						}
					}
				}
				nTimeTrack = now;						// Keep our idea straight
				nTimeCheck = nTimeTrack+120;			// Check again in a couple minutes
				nTimerPid = 0;							// We're no longer running
				continue;								// Don't need all the child tidyup
			}

			nChildren = child_Forget(childpid);
//			ipc_Rm(childpid);
			Log("Child %d terminated (%d child%s now active)", childpid, nChildren, nChildren==1?"":"ren");
		}

		// See if there are any ports that aren't listening that we want to retry
		for (i=0;i<nNetworkPorts;i++) {
			if (!NetworkPort[i].bEnabled && NetworkPort[i].nRetries > 0) {
				if (time(NULL) >= NetworkPort[i].tRetry) {
					int nSock = tcp_ListenOn(NetworkPort[i].nPort);
					if (nSock >= 0) {
						Log("Socket enabled on port %d", NetworkPort[i].nPort);
						NetworkPort[i].nSock=nSock;
						NetworkPort[i].nRetries=0;
						NetworkPort[i].bEnabled=1;
						NetworkPort[i].tRetry=0;
					} else {
						NetworkPort[i].nRetries--;
						if (NetworkPort[i].nRetries) {
							Log("Socket setup error again on port %d: %d - %s (%d)", NetworkPort[i].nPort, GetErrorNo(), GetErrorStr(), nSock);
							NetworkPort[i].tRetry=time(NULL)+10;	// Try again in 10
							struct tm *tm = gmtime(&(NetworkPort[i].tRetry));

							Log("Retry again at %d/%d/%d %d:%02d:%02d",
								tm->tm_mday, tm->tm_mon+1, tm->tm_year % 100,
								tm->tm_hour, tm->tm_min, tm->tm_sec);
						} else {
							Log("Given up trying to liston on port %d", NetworkPort[i].nPort);
						}
					}
				}
			}
		}

		if (nCounter %20 == 0) {						// Do every 10 seconds
			TidyWaiting();
		}

		if (nCounter %20 == 2) {						// Every 10 seconds, first check 1 second in
			CheckWeAreDaemon();
		}

		time_t now = time(NULL);
		struct tm *tm = gmtime(&now);
		long newDaycode = tm->tm_year * 10000 + tm->tm_mon * 100 + tm->tm_mday;
		if (!daycode) daycode = newDaycode;				// This line stops us running once on startup
		if (newDaycode != daycode) {					// A new day has dawned
			daycode=newDaycode;

			file_Rollover(szLogFile, LOGFILE_LIMIT);
			file_Rollover(summaryLogfile, MIFILE_LIMIT);

			if (!spiderFork("ARCHIVE")) {
				ArchiveOldDirs(0);
				exit(0);
			}
		}
	}
}

void SaveBuffer(HBUF *buffer, const char *prefix, const char *description)
{
	if (buffer) {							// We have a buffer we want to write out
		const char *szFilename = NULL;
		int len = hbuf_GetLength(buffer);

		if (!_bNoteInhibit) {
			szFilename=hprintf(NULL, "%s/%s.txt", NoteDir(), prefix);
		} else {
			const char *szDestDir = hprintf(NULL, "/tmp/mmts-%s", prefix);

			if (!access(szDestDir, 2)) {
				szFilename=hprintf(NULL, "%s/%s-%s.txt", szDestDir, MessageId(), _szSenderIp);
			} else {
				Log("%d byte%s of input not saved as %s directory doesn't exist", len, len==1?"":"s", szDestDir);
			}
		}

		if (szFilename) {
			FILE *fp = fopen(szFilename, "w");
			if (fp) {
				const char *buf = hbuf_ReleaseBuffer(buffer);
				fwrite(buf, len, 1, fp);
				szDelete(buf);
				fclose(fp);
			} else {
				Log("Could not write buffer to %s (%s)", szFilename, description);
			}

			if (!_bNoteInhibit) {
				const char *file = hprintf(NULL, "%s.txt", prefix);

				Note("M|%s|%s", file, description);
				szDelete(file);
			} else {
				Log("No msglog - %s holds %s", szFilename, description);
			}
			szDelete(szFilename);
		}
	}
}

void Exit(int nCode)
	// Quits the daemon nice and tidily
{
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
	} else {
		SaveBuffer(_inputBuf, "rawin", "Raw incoming data (on the line)");
		hbuf_Delete(_inputBuf);
		_inputBuf=NULL;

		SaveBuffer(_inputApp, "appin", "Raw incoming data from the application");
		hbuf_Delete(_inputApp);
		_inputApp=NULL;

		Summary(nCode);
	}
//	ipc_Rm(getpid());

	exit(nCode);
}

void BackMeUp()
// Makes a copy of this executable as mmts.229 (or whatever) depending on version number
{
	if (strlen(VERSION) > 20) return;				// VERSION is silly length so don't try to backup
	if (strchr(argv0, '.')) return;					// We have a dot and hence are probably a versioned binary already

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

	if (!access(dest, 0)) return;					// It already exists
	int err = CopyFile(argv0, dest);
	if (!err) {
		chmod(dest, 0755);
	} else {
		Log("Failed to back myself up as %s", dest);
	}
}

int OnChannelConnected(CHAN *chan)
{
	NetworkPort_t *port = (NetworkPort_t*)chan_Info(chan);
	int fd;
	int childPid;

	struct sockaddr_in sin;

#ifdef __SCO_VERSION__
	size_t sin_len = sizeof(sin);
#else
	socklen_t sin_len=(socklen_t)sizeof(sin);
#endif
	port->nCount++;
	int nSrcProtocol=port->nProtocol;
	int nSock=port->nSock;

	if ((fd=accept(nSock,0,0)) < 0) {
		Log("Problem accepting (errno=%d) on port %d", errno, port->nPort);
		return 1;
	}

	memset(&sin,0,sizeof(sin));

	getpeername(fd, (struct sockaddr*)&sin, &sin_len);
	_szSenderIp = strdup((char*)inet_ntoa(sin.sin_addr));
	_nSenderPort = ((sin.sin_port << 8) & 0xff00) | ((sin.sin_port >> 8) & 0xff);

	getsockname(fd, (struct sockaddr*)&sin, &sin_len);
	_szIncomingIp = strdup((char*)inet_ntoa(sin.sin_addr));
	_nIncomingPort = ((sin.sin_port << 8) & 0xff00) | ((sin.sin_port >> 8) & 0xff);

	Log("Connection %d on channel %s - %s on port %d", port->nCount, chan_Name(chan), ProtocolName(nSrcProtocol), port->nPort);
	MessageId();
	if (!(childPid=spiderFork("AGENT"))) {
		bIsDaemon=0;						// We are but a child...
		Log("MESSAGE: MMTS version " VERSION " (" OS "), compiled " __DATE__ " at " __TIME__);
		if (!access(szDebugTrigger, 0)) {
			bDebug=1;   // Enter debug mode if the file exists
			Log("WARNING! IN DEBUG MODE AS %s EXISTS!", szDebugTrigger);
		}

		summ_ChildStart(nSrcProtocol);
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
//			AcceptFileConnection(szDropped);
			break;
		}
		Exit(0);
	}

	// Remember the child process so we can take it with us when we die etc.
	child_Add(childPid, _szSenderIp, NULL);
	Log("Agent %d processing connection from %s", childPid, _szSenderIp);
	if (fd != -1)
		close(fd);							// Child will take care of it

	return 0;
}

static void Idler(CHANPOOL *pool)
{
	Idle();
}

void BeDaemon()
{
	int i;
	int pid;

	Log("MMTS " VERSION " is entering %s mode", "daemon");

	daemon_channel_pool = chan_PoolNew();

	for (i=0;i<nNetworkPorts;i++) {             // Add a channel for each network socket
		if (NetworkPort[i].bEnabled) {
			CHAN *chan = chan_NewFd(daemon_channel_pool, NetworkPort[i].nSock, CHAN_IN);
			chan_SetInfo(chan, NetworkPort+i);
			chan_RegisterReceiver(chan, OnChannelConnected, 0);
		}
	}

	chan_PoolRegisterIdler(daemon_channel_pool, 500, Idler);

//	mtpost_Init(main_channel_pool);

	chan_EventLoop(daemon_channel_pool);
	Log("Daemon finished");

	Exit(0);
}

void StartDaemon(char bRestart)
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

	if ((nChildPid = spiderFork("DAEMON"))) {
		printf("New MMTS daemon process (v%s) is %d\n", VERSION, nChildPid);
		exit(0);							// Drop into the background
	}

	setpgrp();										// Release the terminal

	const char *wampRedirect = hprintf(NULL, "%s/api_redirect.map", GetEtcDir());
	wampRedirectMap = ssmap_GetFile(wampRedirect);
	szDelete(wampRedirect);

	szStdOut = config_GetFile("stdout", "tmp/stdout");
	szStdErr = config_GetFile("stderr", "tmp/stderr");
	if( !freopen(szStdErr, "w", stderr) ) {
		Log("Failed to open %s for logging stderr", szStdErr);
		fprintf(stderr,"%s: Failed to open %s for logging stderr", szMyName, szStdErr);
		exit(6);
	}
	if( !freopen(szStdOut, "w", stdout) ) {
		Log("Failed to open %s for logging stdout", szStdOut);
		fprintf(stderr,"%s: Failed to open %s for logging stdout", szMyName, szStdOut);
		exit(6);
	}
	szDelete(szStdOut);
	szDelete(szStdErr);

	Log("===========================================================================");
	Log("MMTS " VERSION " for " OS " (Made " __TIME__ " on " __DATE__ ", using %s)", SSLeay_version(SSLEAY_VERSION));
	if (szEnvironment) {
		Log("Using default environment '%s'", szEnvironment);
	}

	if (!(fp=fopen(szPidFile, "w"))) {
		    Log("Cannot open '%s' to store my pid - exiting", szPidFile);
		    fprintf(stderr, "%s: Cannot open '%s' to store my pid\n", szMyName, szPidFile);
			exit(6);
	}
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

		if (nSock < 0) Log("Socket setup error on port %d: %d - %s (%d)", NetworkPort[i].nPort, GetErrorNo(), GetErrorStr(), nSock);

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

	BeDaemon();
	exit(0);
}

void Usage(int n)
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

int main(int argc, char *argv[])
{
	extern char *optarg;
	int c;
	int nErr=0;
	const char *szCommand;
	int bSend = 0;												// 1 if we're just sending a message
	int bSendSync;
	const char *szBaseDir = NULL;

	argv0 = argv[0];											// Used for IPC

//#include <v.c>

	rogxml_RetainControls(1);									// 21897 Keep control chars in XML text

	// TODO: Really need to replace this!
//	RAND_seed("ewrlgfuwheglithvglithirhvliewjhflewiuhffru", 40);

	_nStartTime = time(NULL);

	while((c=getopt(argc,argv,"b:c:d:e:p:P:sSh:v"))!=-1){
		switch(c){
			case 'b': strset(&szBaseDir, optarg);			break;	// Set base directory
			case 'e': strset(&szEnvironment, optarg);		break;	// Set default environment
//			case 'p': nListenPort_int = atoi(optarg);		break;	// Set local network port (default is 10023)
//			case 'P': nListenPort_ext = atoi(optarg);		break;	// Set external port (default is 443)
			case 's': bSend=1; bSendSync=1;					break;	// Send a message over app port
			case 'S': bSend=1; bSendSync=0;					break;	// Send via Drop Dir
			case 'v': bVerbose=1;							break;
			case '?': nErr++;								break;	// Something wasn't understood
		}
	}

	if (nErr) Usage(1);

	if (!szBaseDir) szBaseDir = "/usr/mt/mmts";		// THE base directory

	SetBaseDir(szBaseDir);
	SetEtcDir(config_GetDir("etcdir", "etc"));
	SetConfigFile("mmts.conf");

	if (!szTmpDir) szTmpDir=config_GetDir("tmpdir", "tmp");
	if (!szLogDir) szLogDir=config_GetDir("logdir", "logs");
	if (!szInDir) szInDir=config_GetDir("indir", "in");
	if (!szOutDir) szOutDir=config_GetDir("outdir", "out");
	if (!szHandlerDir) szHandlerDir=config_GetDir("handlerdir", "handlers");
	if (!szDoneDir) szDoneDir=config_GetDir("donedir", "done");
	if (!szTxDir) szTxDir=config_GetDir("translatedir", "translators");
	if (!szMsgLogDir) szMsgLogDir=config_GetDir("msglogdir", "msglog");
	if (!szStoreDir) szStoreDir=config_GetDir("storedir", "store");
	szMsgLogDirLeaf = strrchr(szMsgLogDir, '/');
	szMsgLogDirLeaf = szMsgLogDirLeaf ? szMsgLogDirLeaf+1 : szMsgLogDir;

	szLogFile = hprintf(NULL, "%s/mmts.log", szLogDir);
	summaryLogfile = hprintf(NULL, "%s/summary.log", szLogDir);


	GetEnvironment(NULL, 0);					// Get default environment

	szMyUrl = config_GetString("myurl");								// For Nasp2 headers

	szUnackedDir=hprintf(NULL, "%s/unacked", szStoreDir);
	szAckedDir=hprintf(NULL, "%s/acked", szStoreDir);
	szFailedDir=hprintf(NULL, "%s/failed", szStoreDir);
	szReceivedDir=hprintf(NULL, "%s/received", szStoreDir);
	szWaitingDir=hprintf(NULL, "%s/waiting", szStoreDir);
	MkDir(szUnackedDir);
	MkDir(szAckedDir);
	MkDir(szFailedDir);
	MkDir(szReceivedDir);
	MkDir(szWaitingDir);

	allow_Init();

	BackMeUp();


	if (bSend) {							// We simply want to send a message
		if (bSendSync) {					// Send by transmitting to the host
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
			if (!szHost) Fatal("Usage: mmts -s host file");

			szFilename = szCommand=argv[optind++];
			if (!szFilename) Fatal("Usage: mmts -s host file");

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
		} else {							// Send by copying to the out dir
			const char *szDest;
			const char *szFile;
			int err;
			const char *szFilename;

			szFilename = szCommand=argv[optind++];
			if (!szFilename) Fatal("Usage: mmts -S file");

			szFile = strrchr(szFilename, '/');
			if (szFile) szFile++; else szFile=szFilename;

			szDest = hprintf(NULL, "%s/%s.xml", szOutDir, szFile);
			err = CopyFile(szFilename, szDest);
			if (!err) {
				if (bVerbose) printf("File deposited in Drop Dir as '%s'\n", szDest);
			} else {
				Fatal("Error %d copying %s to %s", err, szFilename, szDest);
			}
		}
		exit (0);
	}

	szPidFile=config_GetFile("pidfile", "etc/mmts.pid");

#if 0
	s3 *s = s3_Open(0, NULL);
	printf("s = %p\n", s);
	int err=s3_Query(s, "CREATE TABLE IF NOT EXISTS dupcheck (uuid CHAR(36), dt DATETIME DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (uuid))");
	if (err) printf("Error %d: %s\n", s3_ErrorNo(s), s3_ErrorStr(s));
	err=s3_Query(s, "INSERT INTO dupcheck VALUES ('00000000-0000-0000-0000-000000000000',NULL)");
	if (err) printf("Error %d: %s\n", s3_ErrorNo(s), s3_ErrorStr(s));
	err=s3_Query(s, "INSERT INTO dupcheck VALUES ('00000000-0000-0000-0000-000000000001',NULL)");
	if (err) printf("Error %d: %s\n", s3_ErrorNo(s), s3_ErrorStr(s));
	err=s3_Query(s, "INSERT INTO dupcheck VALUES ('00000000-0000-0000-0000-000000000002',NULL)");
	if (err) printf("Error %d: %s\n", s3_ErrorNo(s), s3_ErrorStr(s));
	err=s3_Query(s, "INSERT INTO dupcheck VALUES ('00000000-0000-0000-0000-000000000003',NULL)");
	if (err) printf("Error %d: %s\n", s3_ErrorNo(s), s3_ErrorStr(s));

	const char **row;
	s3_Query(s, "SELECT uuid FROM dupcheck");
	if (it) {
		while (row=s3_Next(s)) {
			printf("GUID = '%s'\n", row[0]);
		}
	} else {
		printf("Error %d: %s\n", s3_ErrorNo(s), s3_ErrorStr(s));
	}

	s3_Close(s);
	exit(0);
#endif

	szCommand=argv[optind++];
	if (!szCommand) szCommand="status";

	if (!strcasecmp(szCommand, "archive")) {
		ArchiveOldDirs(1);
	} else if (!strcasecmp(szCommand, "start")) {
		StartDaemon(0);
	} else if (!strcasecmp(szCommand, "restart")) {
		StartDaemon(1);
	} else if (!strcasecmp(szCommand, "stop")) {
		int nPid = StopAnyPrevious();
		if (nPid) {
			printf("Daemon %d stopped\n", nPid);
		} else {
			printf("Daemon was not running\n");
		}
	} else if (!strcasecmp(szCommand, "status")) {
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
