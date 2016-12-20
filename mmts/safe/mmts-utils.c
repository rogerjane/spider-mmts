
// Various utility functons used in the MMTS and it's subsiduaries

// 14-03-15 RJ 0.00 Decided some comments might be nice in here
// 14-03-15 RJ 0.01 unlink the temporary file used for validation if the validation went ok

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <time.h>
#include <signal.h>
#include <sys/time.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>
#include <zlib.h>

#include <guid.h>
#include <hbuf.h>
#include <heapstrings.h>
#include <mmts-utils.h>
#include <mtsl3.h>
#include <rogxml.h>

static const char *szBaseDir = NULL;
static const char *szEtcDir = NULL;
static const char *szConfigFile = NULL;
static const char *szEnvironment = NULL;				// Our idea of the environment

static const char *szLog = "logs/mmts.log";              // General log

void SetBaseDir(const char *szDir)		{ szBaseDir = szDir; }
const char *GetBaseDir()				{ return szBaseDir; }
void SetEtcDir(const char *szDir)		{ szEtcDir = szDir; }
const char *GetEtcDir()					{ return szEtcDir; }
const char *GetConfigFile()				{ return szConfigFile; }

extern int         _nSenderPort;
extern const char* _szSenderIp;

void NoteLog(const char *szText);

int PeerPort(bExt) { return _nSenderPort; }

#ifdef __SCO_VERSION__
// SCO Specific stuff
#else
// Non-SCO specific stuff
#if 0
void strlwr(char *s)
{
	char c;

	while ((c=*s)) {
		*s=tolower(c);
		++s;
	}
}

void strupr(char *s)
{
	char c;

	while ((c=*s)) {
		*s=toupper(c);
		++s;
	}
}
#endif

#endif

const char *PeerIp(char bExt)
{
	static char buf[20];
	if (!PeerPort(bExt)) {
		return "-";
	}
	snprintf(buf, sizeof(buf), "%s", _szSenderIp);
	return buf;
}

void mmts_TrimTrailing(char *szText)
{
	int len=strlen(szText);
	while (--len >= 0) {
		if (!isspace(szText[len]))
			break;
	}
	szText[len+1]='\0';

	return;
}

void SetUtilsEnvironment(const char *szEnv)
{
	szDelete(szEnvironment);
	szEnvironment = strdup(szEnv);
}

void SetConfigFile(const char *szFile)
{
	const char *szConfig = QualifyFile(szFile);
	szDelete(szConfigFile);
	szConfigFile = szConfig;
}

time_t DecodeTimeStamp(const char *szDt)
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
		} else if (nLen == 19 || (nLen == 20 && szDt[19] == 'Z')) {	// YYYY-MM-DDTHH:MM:SS or YYYY-MM-DDTHH:MM:SSZ
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

const char *TimeStamp(time_t t)
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

const char *HL7TimeStamp(time_t t)
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

const char *QualifyFile(const char *szName)
// Returns a string on the heap that is the name passed if it is an absolute filename or the
// same string relative to the base directory if it is relative.
{
	if (*szName == '/') return strdup(szName);

	return hprintf(NULL, "%s/%s", szBaseDir, szName);
}

int rog_MkDir(const char *fmt, ...)
// Ensures that the given path exists
// Returns  1   The path now exists
//          0   Something went really wrong and I couldn't create it
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

int MkDir(const char *szDir)
{
	mkdir(szDir, 0777);
	chmod(szDir, 0777);

	return 1;
}

const char *SignalName(int n)
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

char *SkipSpaces(const char *t)
{
	while (isspace(*t)) t++;

	return (char*)t;
}

char *ReadLine(FILE *fd)
// Reads a line from a file, stripping trailing linefeeds (and carriage returns)
// Returns a pointer to a static buffer.
{
	static char buf[1024];

	if (fgets(buf, sizeof(buf), fd)) {
		char *chp=strchr(buf, '\n');			// Should always find one of these except possibly on the last line
		if (chp) {
			*chp='\0';							// Zap the linefeed
			if (chp > buf && chp[-1] == '\r')	// Look to see if the preceeding character is a 'CR'
				chp[-1]='\0';					// and zap it
		}
		return buf;
	} else {
		return NULL;
	}
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

void pack_InitPack()
{
//Log("pack_InitPack()");
	hPackBuf = hbuf_New();
	packstrm.zalloc=Z_NULL;
	packstrm.zfree=Z_NULL;
	packstrm.opaque=Z_NULL;
	deflateInit(&packstrm, Z_BEST_COMPRESSION);				// << Level?
}

int pack_Pack(const char *data, int nLen)
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

const char *pack_GetPacked(int *pnLen)
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

void pack_InitUnpack()
{
	hUnpackBuf = hbuf_New();
	unpackstrm.zalloc = Z_NULL;
	unpackstrm.zfree = Z_NULL;
	unpackstrm.opaque = Z_NULL;
	unpackstrm.avail_in = 0;
	unpackstrm.next_in = Z_NULL;
	inflateInit(&unpackstrm);
}

int pack_Unpack(const char *data, int nLen)
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

const char *pack_GetUnpacked(int *pnLen)
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

void Log(const char *szFmt, ...)
// Sends a log entry to both main and message logs
// If 'szFmt' starts "MAIN: " then only sends to the main log
// If 'szFmt' starts "MESSAGE: " then only sends to the message log
{
	va_list ap;
	char buf[1000];
	char bForMessage = 1;
	char bForMain = 1;
	const char *szMessage = szFmt;

	if (!strncasecmp(szMessage, "MESSAGE: ", 9)) {			// Only for message log
		szMessage += 9;
		bForMain = 0;
	}

	if (!strncasecmp(szMessage, "MAIN: ", 6)) {			// Only for main log
		szMessage += 6;
		bForMessage = 0;
	}

	if (bForMain) {										// Wants to be logged into main log
		const char *szLogFile=QualifyFile(szLog);
		FILE *fp=fopen(szLogFile, "a");
//{FILE *fp=fopen("/tmp/fred","w");fprintf(fp, "Log file is '%s'\n", szLogFile);fclose(fp);}
		if (fp) {
			time_t now=time(NULL);
			struct tm *tm = gmtime(&now);
			struct timeval tp;
			int csecs;

			gettimeofday(&tp, NULL);
			csecs=tp.tv_usec / 10000;

			fprintf(fp, "%02d-%02d-%02d %02d:%02d:%02d.%02d ",
					tm->tm_mday, tm->tm_mon+1, tm->tm_year % 100,
					tm->tm_hour, tm->tm_min, tm->tm_sec,
					csecs);
			fprintf(fp, "%s ", PeerIp(0));
			fprintf(fp, "%s ", PeerIp(1));
			fprintf(fp, "%d ", getpid());
			va_start(ap, szFmt);
			vfprintf(fp, szMessage, ap);
			va_end(ap);
			fprintf(fp, "\n");

			fclose(fp);
			chmod(szLogFile, 0666);
		}
		szDelete(szLogFile);
	}

	if (bForMessage) {												// Wants to go into message log
		va_start(ap, szFmt);
		vsnprintf(buf, sizeof(buf), szMessage, ap);
		va_end(ap);
		NoteLog(buf);
	}
}

int LogError(int err, const char *szFmt, ...)
{
	va_list ap;
	char buf[1000];

	va_start(ap, szFmt);
	vsnprintf(buf, sizeof(buf), szFmt, ap);
	va_end(ap);

	fprintf(stderr,"%s: %s\n","MMTS", buf);

	Log("Error: %s", buf);

	return 0;
}

const char *MessageDescription(const char *szInteractionId, const char **pszSection)
// Gets a short description of the message and, if pszSection != NULL, gets the
// section (CaB, PDS etc.)
// Note that the return is a pointer to a static buffer that is likely to be over-written
// very quickly (next read of a text file).
{
	const char *szFilename = hprintf(NULL, "%s/%s", szEtcDir, "interactions");
	FILE *fp=fopen(szFilename, "r");
	const char *szDescription = NULL;
	const char *szSection = NULL;
	char *szCopyId = strdup(szInteractionId);

	// Trim off the 'UKnn' if there is one
	if (strlen(szCopyId) == 17 && szCopyId[13] == 'U' && szCopyId[14] == 'K')
		szCopyId[13]='\0';
	if (strlen(szCopyId) == 17 && szCopyId[13] == 'G' && szCopyId[14] == 'B')
		szCopyId[13]='\0';

	if (fp) {
		const char *szLine;

		while ((szLine = ReadLine(fp))) {
			if (*szLine && *szLine != '#') {
				char *szFileId = strtok((char*)szLine, ":");
				if (!strcasecmp(szFileId, szCopyId)) {
					szSection = strtok(NULL, ":");
					szDescription = strtok(NULL, ":");
					break;
				}
			}
		}
		fclose(fp);
	}

	szDelete(szFilename);
	szDelete(szCopyId);

	if (pszSection) *pszSection = szSection;

	return szDescription ? szDescription : szInteractionId;
}

int MessageOption(const char *szInteractionId, const char *szTest)
// Checks the message options file for a specific option being set against the message.
// Returns true if it is, false if is isn't.
// The file is in the format:
// interactionid:options
// Where 'options' are a number of words that may match 'szTest'
// The file is searched from the top until an interactionid matches up to the length
// given in the file so the following:
// PRPA_IN01:
// PRPA:drop
// Sets 'drop' for all PRPA messages except for PRPA_IN01
// Now has extension such that delay=30 is detected by szTest="delay" and returns 30.
{
	const char *szFilename = hprintf(NULL, "%s/%s", szEtcDir, "messageopt");
	FILE *fp=fopen(szFilename, "r");
	int nResult = 0;					// Default if we don't find file or entry

	if (fp) {
		for (;;) {
			const char *szLine = ReadLine(fp);

			if (szLine) {
				if (*szLine && *szLine != '#') {										// Not a comment line
					char *szFileId = strtok((char*)szLine, ":");
					const char *szOption;

					if (!strncasecmp(szFileId, szInteractionId, strlen(szFileId))) {		// Matches id
						while ((szOption = strtok(NULL, " "))) {							// scan options
							const char *chp;

							if (!strcasecmp(szOption, szTest)) {
								nResult=1;												// Record a match
								break;
							} else if ((chp=strchr(szOption, '='))) {						// We've got "delay=nnn"
								if (!strncasecmp(szOption, szTest, chp-szOption)) {		// Check for 'delay'
									nResult=atoi(chp+1);								// Result is nnn
									break;
								}
							}
						}
						break;
					}
				}
			} else {							// No line - end of file
				break;
			}
		}
		fclose(fp);
	}

	szDelete(szFilename);

	return nResult;
}

int ProcessAlive(int nPid)
// Returns 1 if the process is still alive, otherwise 0
{
	int nErr=kill(nPid, 0);
	return (!nErr || errno == EPERM);		// No error or EPERM so the process exists
}

int config_FileSetString(const char *szFilename, const char *szName, const char *szValue)
// Sets a string in the config file or deletes it if szValue is NULL
// This is done with as little 'messing up' of the original file as possible.
// Method is to copy the file, with amendment, to a temporary file then copy that back over the original.
// If we notice that the value we're setting is the one already in the file, set bAbort and do no more.
// Returns	1	All went ok.
//			0	Failed - variable not set
{
	const char *szTmp;
	FILE *fpt;
	FILE *fp;
	const char *szLine;
	int nLen=strlen(szName);
	S3 *s3 = NULL;

	fp=fopen(szFilename, "r");					// Can't open the file we're meant to update so give up
	if (!fp) return 0;

//Log("config_FileSetString(\"%s\",\"%s\",\"%s\")\n",szFilename,szName,szValue);

#if 0		// TODO: re-enable this if the s3 stuff isn't at fault
	s3 = s3_Openf("%s.sl3", szFilename);

	if (s3) {
	    int err=s3_Do(s3, "CREATE TABLE IF NOT EXISTS config (n TEXT, v TEXT, PRIMARY KEY (n))");
		if (err) Log("Create error %d: %s", s3_ErrorNo(s3), s3_ErrorStr(s3));

		err = s3_Dof(s3, "REPLACE INTO config (n,v) VALUES (%s,%s)", szName, szValue);
		if (err && s3_ErrorNo(s3)) Log("Query error %d: %s", s3_ErrorNo(s3), s3_ErrorStr(s3));
		s3_Destroy(&s3);
	}
#endif

	szTmp=hprintf(NULL, "/tmp/mmts.tmp.%d", getpid());
	fpt=fopen(szTmp, "w");
	if (!fpt) {									// Couldn't open temp file - something is amiss...
		szDelete(szTmp);
		fclose(fp);
		return 0;
	}

	int bAbort=0;								// 1 if we see the value hasn't changed and we can do nothing
	while ((szLine=ReadLine(fp))) {
		const char *szEquals, *chp;

		szEquals=strchr(szLine, '=');

		if (!*szLine || *szLine == '#' || !szEquals) {
			fputs(szLine, fpt);
			fputc('\n', fpt);
			continue;			// Blank or comment
		}
		chp=szEquals-1;

		while (chp>szLine && isspace(*chp))											// rtrim
			chp--;

		if (chp-szLine+1 == nLen && !strncasecmp(szName, szLine, chp-szLine+1)) {		// Already in the file
			if (szValue) {
				if (!strcmp(szValue, szEquals+1)) {									// Nothing has changed
					bAbort=1;
					break;
				} else {
					const char *buf=hprintf(NULL, "%s=%s\n", szName, szValue);			// ...so replace it
					fputs(buf, fpt);
					szDelete(buf);
				}
			}
			szName=NULL;
		} else {
			fputs(szLine, fpt);
			fputc('\n', fpt);
		}
	}

	if (!bAbort && szName && szValue) {									// It didn't exist in the file
		const char *buf=hprintf(NULL, "%s=%s\n", szName, szValue);		// ...so add a new entry at the end
		fputs(buf, fpt);
		szDelete(buf);
	}

	fclose(fpt);
	fclose(fp);

	if (!bAbort) {						// Don't copy the aborted file back!
		fpt=fopen(szTmp, "r");
		if (!fpt) {
			szDelete(szTmp);
			return 0;
		}

		fp=fopen(szFilename, "w");
		if (!fp) {
			szDelete(szTmp);
			fclose(fpt);
			return 0;
		}

		while ((szLine=ReadLine(fpt))) {
			fputs(szLine, fp);
			fputc('\n', fp);
		}

		fclose(fpt);
		fclose(fp);
	}

	unlink(szTmp);
	szDelete(szTmp);

	return 1;
}

time_t config_LastChange()
// Returns the last modify time of the configuration file
{
	if (szConfigFile) {
		struct stat st;

		if (!stat(szConfigFile, &st)) {
			return st.st_mtime;								// Return the time
		} else {
			return 0;										// Cannot read config file
		}
	} else {
		return 0;											// Config file not set
	}
}

int config_SetString(const char *szName, const char *szValue)
{
	if (!szConfigFile) return 0;

	return config_FileSetString(szConfigFile, szName, szValue);
}

const char *config_FileGetString(const char *szFilename, const char *szName)
// Gets a string from the given file.
// returns a string on the heap if successful or NULL if there is
// no string or no config file...
{
	FILE *fp;
	const char *szLine;
	int nLen=strlen(szName);

	fp=fopen(szFilename, "r");
	if (!fp) return NULL;

	while ((szLine=ReadLine(fp))) {
		const char *szEquals, *chp;

		if (!*szLine || *szLine == '#') continue;			// Blank or comment
		szEquals=strchr(szLine, '=');
		if (!szEquals) continue;							// No equals on line
		chp=szEquals-1;
		while (chp>szLine && isspace(*chp)) chp--;
		if (chp-szLine+1 == nLen && !strncasecmp(szName, szLine, chp-szLine+1)) {
			szEquals++;
			while (isspace(*szEquals)) szEquals++;
			fclose(fp);
//Log("%s:%s = '%s'",szFilename,szName,szEquals);
			return strdup(szEquals);
		}
	}

//Log("%s:%s not found",szFilename,szName);
	fclose(fp);

	return NULL;
}

const char *config_GetString(const char *szName)
// Gets a string from the config file.
// returns a string on the heap if successful or NULL if there is
// no string or no config file...
{
	if (!szConfigFile) return NULL;

	return config_FileGetString(szConfigFile, szName);
}

int config_GetInt(const char *szName, int nDef)
// Returns an integer setting from the config file or 'nDef' if it isn't in there
{
	const char *szStr = config_GetString(szName);

	if (szStr) {
		nDef=atoi(szStr);
		szDelete(szStr);
	}

	return nDef;
}

char config_GetBool(char *szName, char bDef)
// Accepts 'Y...', 'T...', 1+ as true, anything else as false
{
	const char *szValue = config_GetString(szName);

	if (szValue) {
		char c=toupper(*szValue);

		bDef = (c == 'Y' || c == 'T' || atoi(szValue));
	}

	return bDef;
}

const char *config_GetFile(const char *szName, const char *szDef)
// Gets a fully qualified filename which is 'szName' from the config file if it
// is in the config file or 'szDef' if it isn't found.
{
	const char *szPath = NULL;
	const char *szDir = config_GetString(szName);

	if (!szDir && szDef) szDir=strdup(szDef);

	if (szDir) szPath=QualifyFile(szDir);

	szDelete(szDir);

	return szPath;
}

const char *config_GetDir(const char *szName, const char *szDef)
// Reads a directory name from the config file and returns it, making sure it exists.
// If the string is not in the config file then 'szDef' is used instead
// Returns a heap-based string
{
	const char *szPath = config_GetFile(szName, szDef);

	rog_MkDir("%s", szPath);

	return szPath;
}

const char *config_EnvGetString(const char *szName)
// Gets a string from the environment's config file if it's there, defaulting to the main config if it isn't.
{
	const char *szResult = NULL;

	if (szEnvironment && strcasecmp(szEnvironment, "default")) {
		const char *szEnvConfig=hprintf(NULL, "env/%s/env.conf", szEnvironment);

		szResult=config_FileGetString(szEnvConfig, szName);
		szDelete(szEnvConfig);
	}

	if (!szResult) szResult=config_GetString(szName);

	return szResult;
}

int config_EnvGetInt(const char *szName, int nDef)
// Returns an integer setting from the environment config file or 'nDef' if it isn't in there
{
	const char *szStr = config_EnvGetString(szName);

	if (szStr) {
		nDef=atoi(szStr);
		szDelete(szStr);
	}

	return nDef;
}

char config_EnvGetBool(const char *szName, char bDef)
// Accepts 'Y...', 'T...', 1+ as true, anything else as false
{
	const char *szValue = config_EnvGetString(szName);

	if (szValue) {
		char c=toupper(*szValue);

		bDef = (c == 'Y' || c == 'T' || atoi(szValue));
	}

	return bDef;
}

const char *QualifyFilename(const char *szDir, const char *szFilename)
// Qualifies a filename as if it were relative to 'szDir' below the
// base directory.  'szDir' can be NULL to indicate the base dir itself.
// Returns a string on the heap.  If szDir doesn't start with a '/' then it
// is taken relative to the base dir.
// Always returns a string on the heap.
{
	char buf[1000];
	static const char *szDirBase = ".";

	if (*szFilename == '/') return strdup(szFilename);	// Absolute

	if (szDir) {
		if (*szDir == '/') {
			snprintf(buf, sizeof(buf), "%s/%s", szDir, szFilename);
		} else {
			snprintf(buf, sizeof(buf), "%s/%s/%s", szDirBase, szDir, szFilename);
		}
	} else {
		snprintf(buf, sizeof(buf), "%s/%s", szDirBase, szFilename);
	}

	return strdup(buf);
}

int Execute(const char *szDirBin, const char *szFmt, ...)
// Execute an external program, logging output.
// If 'szDirBin' is non-NULL, it's used as the base for relative pathnames.
{
	va_list ap;
	char buf[1000];
	FILE *p;
	const char *szExecutable;
	int nLen;
	const char *szTmpCommand;
	char *szTmp;
	char *szCommand;
	const char *szParams;
	int nStatus = 0;

	va_start(ap, szFmt);
	vsprintf(buf, szFmt, ap);
	va_end(ap);

	szTmpCommand = buf;
	szParams=strchr(szTmpCommand, ' ');
	if (!szParams) szParams=szTmpCommand+strlen(szTmpCommand);
	nLen=szParams-szTmpCommand;
	szTmp=malloc(nLen+1);
	memcpy(szTmp, szTmpCommand, nLen);
	szTmp[nLen]='\0';
	szExecutable=QualifyFilename(szDirBin, szTmp);
	szParams = SkipSpaces(szParams);

	free(szTmp);

	if (access(szExecutable, 1)) {
		Log("Cannot find executable '%s'", szExecutable);
		szDelete(szExecutable);
		return -1;
	}

	szCommand=hprintf(NULL, "%s %s", szExecutable, szParams);
	szDelete(szExecutable);

	p=popen(szCommand, "r");
	fflush(stdout);								// I'm not convinced this does anything but it might flush
												// any error output from the command

	if (p) {
		char buf[1000];
		char *szRemainder = NULL;				// Used to store incomplete lines at the ends of buffers
		int got;

		while ((got=fread(buf, 1, sizeof(buf)-1, p))) {
			char *bufp=buf;
			buf[got]='\0';

			while (*bufp) {
				char *chp=strchr(bufp, '\n');							// Find end of line
				if (chp) {												// We have a tail end
					if (szRemainder) {									// We already have the start of a line
						Log("%s%-.*s", szRemainder, chp-bufp, bufp);	// Put the two into the log
						szDelete(szRemainder);							// Drop the previous start
						szRemainder=NULL;
					} else {											// A complete line
						if (chp > bufp) {								// Only log non-blank lines
							Log("%-.*s", chp-bufp, bufp);
						}
					}
					bufp=chp+1;											// Point to the start of the next line
				} else {
					szRemainder=strdup(bufp);							// First part of a line
					bufp="";											// Force breaking from the loop
				}
			}

		}
		if (szRemainder) {												// Incomplete last line needs flushing
			Log("%s", szRemainder);
			szDelete(szRemainder);
		}
		nStatus=pclose(p);
//		if (nStatus) Log("Exit status = %d", nStatus);
	} else {
		nStatus=1;
		Log("Unable to execute: %s (errno=%d)", szCommand, errno);
	}

	szDelete(szCommand);

	return nStatus;
}

int ValidateXml(rogxml *rx)
// Validates the XML passed
// Returns	0		Ok or no validator available
//			1...	Error
{
	int nResult=0;

	if (!access("bin/mmtsvalidatexml", 1)) {
		const char *szGuid;
		const char *szFilename;
		const char *szCommand;
		int nErr;

		szGuid = guid_ToText(NULL);
		szFilename = hprintf(NULL, "/tmp/%s.xml", szGuid);
		szCommand = hprintf(NULL, "mmtsvalidatexml '%s'", szFilename);

		rogxml *rxProlog=rogxml_FindProlog(rx);
		rogxml_SetAttr(rxProlog, "encoding", "ISO-8859-1");		// Tell the validator that it's 8-bit
		rogxml_WriteFile(rogxml_FindRoot(rx), szFilename);
		nErr = Execute("bin", szCommand);
		if (nErr == -1) {						// Failed to execute the command
			Log("Failed to execute validator: %s", szCommand);
		} else {
			nResult = WEXITSTATUS(nErr);
			if (nResult) {
				Log("Error %d validating XML in %s", nResult, szFilename);
			} else {
				unlink(szFilename);				// Tidy up the file
//				Log("XML Validated OK");		// We don't need to know this
			}
		}

		szDelete(szGuid);
		szDelete(szFilename);
		szDelete(szCommand);
	} else {
		Log("No validator available to check message");
	}

	return nResult;
}
