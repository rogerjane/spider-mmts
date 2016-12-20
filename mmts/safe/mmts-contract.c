
// All functionality dealing with the contracts that control message
// transfer of specific message types.
// See the External Interface Specification for details

// At this stage, I'm simply storing the contracts in files so that
// we can easily play with them, in the fullness of time they might
// turn into SQL databases but it's handy to keep them in this very
// fluid storage for now and build the database later.

// We'll make no attempt to cache properties at this stage and it is
// probably pointless anyway as each process will deal with a single
// message then terminate.

#include <errno.h>
#include <sys/stat.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <heapstrings.h>
#include <mtmacro.h>
#include <rogxml.h>
#include <smap.h>
#include <xstring.h>

#include "mmts-contract.h"
#include "mmts-message.h"
#include "mmts-utils.h"

#define bFunctions	0									// Records some function calls in the log

static const char *_szContractDir = NULL;				// Where to find the contracts
static const char *_szEnvDir = NULL;				// Where to find the contracts

static void SplitEndpoint(contract_t *c)
// Splits a protocol://address:port/URI strings into the bits
{
	char *szCopy;
	char *szEndpoint;
	char *chp;

	if (!c->szEndpoint) {
		strset(&c->szProtocol, NULL);
		strset(&c->szAddress, NULL);
		strset(&c->szURI, NULL);
		c->nPort = 0;
		return;
	}

	szEndpoint=szCopy=strdup(c->szEndpoint);
	chp = strstr(szEndpoint, "://");							// First take the protocol off the front
	if (chp) {
		*chp='\0';
		strset(&c->szProtocol, strdup(szEndpoint));
		szEndpoint=chp+3;
	} else {
		strset(&c->szProtocol, NULL);
	}
	chp=strchr(szEndpoint, '/');								// Then snaffle the URI
	if (chp) {
		strset(&c->szURI, strdup(chp));
		*chp='\0';
	} else {
		strset(&c->szURI, "/");
	}
	chp=strchr(szEndpoint, ':');								// Then look for a port number
	if (chp) {
		c->nPort=atoi(chp+1);
		*chp='\0';
	} else {
		c->nPort=443;
	}
	strset(&c->szAddress, strdup(szEndpoint));					// Whatever's left is the address

	free(szCopy);
}

const char *cmap_DefaultFile(const char *szTo, const char *szInteraction)
// Returns a heap-based string with the location of the default contract details.
{
	FILE *fp = NULL;
	const char *szFilename;

if (bFunctions) Log("cmap_DefaultFile(\"%s\", \"%s\")", szTo, szInteraction);
	if (szTo) {
		szFilename = hprintf(NULL, "%s/default/contracts/%s/%s", _szEnvDir, szTo, szInteraction);
		fp = fopen(szFilename, "r");
		if (fp)
			fclose(fp);
		else
			szDelete(szFilename);
	}

	if (!fp) {
		szFilename = hprintf(NULL, "%s/default/contracts/%s", _szEnvDir, szInteraction);
		fp = fopen(szFilename, "r");
		if (fp) fclose(fp);
	}

if (bFunctions) Log("cmap_DefaultFile() returning \"%s\"", szFilename?szFilename:"NULL");
	return szFilename;
}

const char *cmap_Filename(const char *szTo, const char *szInteraction)
// Returns a heap-based string with the 'correct' location for the contract.
{
	if (szTo) {
		return hprintf(NULL, "%s/cache/contracts/%s/%s", _szEnvDir, szTo, szInteraction);
	} else {
		return hprintf(NULL, "%s/cache/contracts/%s", _szEnvDir, szInteraction);
	}
}

const char *contract_Filename(const char *szTo, const char *szInteraction)
// Returns a heap-based string with the 'correct' location for the contract.
{
	if (szTo) {
		return hprintf(NULL, "%s/%s/%s", _szContractDir, szTo, szInteraction);
	} else {
		return hprintf(NULL, "%s/%s", _szContractDir, szInteraction);
	}
}

static int contract_Blank(contract_t *c)
{
	c->nRetries = 0;
	c->szCacheTime = NULL;
	c->szId = NULL;
	c->szService = NULL;
	c->szRetryInterval = NULL;
	c->szSyncReplyMode = NULL;
	c->szPersistDuration = NULL;
	c->szIsAuthenticated = NULL;
	c->szDuplicateElimination = NULL;
	c->szAckRequested = NULL;
	c->szActor = NULL;
	c->szEndpoint = NULL;
	c->szIn = NULL;
	c->szPartyKey = NULL;
	c->szSvcIa = NULL;
	c->szProtocol = NULL;
	c->szAddress = NULL;
	c->szURI = NULL;
	c->nPort = 0;
	c->szWrapper = NULL;
	c->szLevel = NULL;

	return 1;
}

SSMAP *cmap_ReadFile(SSMAP *cm, const char *szFilename)
// Gets a contract file to a map (existing if cm != NULL)
// Returns	SSMAP*	Pointer to map (=cm if cm isn't NULL)
//			NULL	Couldn't read the file (even if cm != NULL)
{
	FILE *fp=fopen(szFilename, "r");

if (bFunctions) Log("cmap_ReadFile(%x, \"%s\" (%s))", cm, szFilename, fp?"opened":"not opened");
	if (fp) {
		const char *szLine;
		if (!cm) cm=ssmap_New();

		while ((szLine=hReadFileLine(fp))) {
			const char *szOriginal = szLine;

			szLine=SkipSpaces(szLine);
			if (*szLine && *szLine != '#') {
				const char *szEquals=strchr(szLine, '=');

				if (szEquals) {
					char *szName=(char*)strnappend(NULL, szLine, szEquals-szLine);
					char *szValue=strdup(SkipSpaces(szEquals+1));
					mmts_TrimTrailing(szName);
					mmts_TrimTrailing(szValue);
					strlwr(szName);
//Log("Setting %s=%s to %x", szName, szValue, cm);
					ssmap_Add(cm, szName, szValue);
					szDelete(szName);
					szDelete(szValue);
				}
			}
			szDelete(szOriginal);
		}

		fclose(fp);
	}

	return cm;
}

int cmap_WriteFile(SSMAP *cm, const char *szFilename)
// Writes a cmap out to a file
// Returns	1	Went ok
//			0	Could not open file
{
	FILE *fp=fopen(szFilename, "w");

	if (fp) {
		ssmap_Reset(cm);
		const char *szName;
		const char *szValue;

		while (ssmap_GetNextEntry(cm, &szName, &szValue)) {
			fprintf(fp, "%s=%s\n", szName, szValue);
		}
		fclose(fp);
	}

	return !!fp;
}

rogxml *sds_Query(const char *szQuery, char bLog);

// Cache algorithm...
// Each cache pool (e.g. dns, ldap) has four parameters:
// callback function, best_before period, sell_by period, retry_period
// The callback function accepts a key and returns a map associated with that key, generally with entries of:
// cache_BestBefore, cache_SellBy, cache_RetryPeriod (generally set using 'cache_SetStale()'
// value - the return value for the cases where there is only one (e.g. an IP address for a DNS fetch)
// Add a pool to the cache system using cache_AddPool(name, callback_ function);
// The callback function takes care of the other parameters itself.
//
// Attempt to read map from file
// If successful and now < cache_BestBefore then return map
// If now > cache_TryAgain or failed to fetch from file
//    Try to fetch from wherever we get things from (LDAP, DNS etc.), replacing map if successful
//      If successful:
//        Replace the map with the new one fetched, setting cache_BestBefore and cache_SellBy to future values
//    If not successful, set cache_TryAgain to now + whatever-period
//    Write the map back to the file
// If now < cache_SellBy then return map
// free map
// return NULL
//
// A better way, given we can fork off another process, is to simply return the cached version if we're beyond
// the BestBefore but fetch in the background.  That way the results will always tend to be returned
// immediately.
//
// SSMAP *dnsGetter(const char *szKey);						// Takes a key and returns a corresponding map
// cache_SetStale(SSMAP *map, int nBest, int nSell, int nRetry); // Sets cache_BestBefore, cache_SellBy and cache_Retry
// void cache_AddPool("dns", dnsGetter);						// Sets the getter for dns pool
// const char *cache_Get("dns","www.microtest.co.uk","ip");	// Get a single value from dns pool
// const char *cache_GetValue("dns","www.microtest.co.uk");	// As above with 'value' at as the last parameter
// SSMAP *cache_GetMap("dns","www.microtest.co.uk");		// Get a map from dns pool
// void cache_Reset("dns","www.microtest.co.uk");			// Drops the cache for given value
// void cache_ResetPool("dns");								// Drops entire cache pool
// void cache_ResetAll();									// Drops the entire cache

int cmap_RefreshCache(const char *szTo, const char *szInteraction, const char *szService)
// Basically goes and fetches a contract from SDS if we want it
// Includes dealing with the default file if there is one
// NOTES...
// There is a theory that the cpaid should be got using:
//    SELECT nhsmhscpaid from services where objectclass='nhsmhscp' AND nhsmhsactionname='$int'
// for MIM 2 interactions (i.e. UK01 - UK04)
// Also, CaB and GP2GP should use ExpressIntermediary (MIM 2) or ReliableIntermediary (MIM 3+)
// rather than the interaction in the contract fetches.
// Until 6-12-12 this function only used szTo and szInteraction to choose the contract.  If multiple
// were returned, it would log it and return the first.  This buggers gp2gp in some instances so
// we now use Service (defaulting to gp2gp) if multiple contracts are returned.
{
if (bFunctions) Log("cmap_RefreshCache(\"%s\", \"%s\", \"%s\")", szTo, szInteraction, szService);

	const char *szQuery = hprintf(NULL, "SELECT * from services WHERE objectclass='nhsmhs' AND nhsmhspartykey='%s' AND nhsmhsin='%s'", szTo, szInteraction);
	rogxml *rxResults = sds_Query(szQuery, 1);
	char ok=1;				// Various sanity checks might set this to zero, stopping us storing the contract
	rogxml *rxResult=NULL;

	szDelete(szQuery);

	if (rogxml_ErrorNo(rxResults)) {
		Log("Error %d fetching contract from SDS: %s", rogxml_ErrorNo(rxResults), rogxml_ErrorText(rxResults));
		ok=0;
	}

	int count=rogxml_GetAttrInt(rxResults, "count", 0);
	if (count != 1) {
		if (count == 0) {
			Log("Failed to fetch a contract for %s/%s from SDS", szTo, szInteraction);
			ok=0;
		} else {
			Log("Found %d duplicate contracts for %s/%s in SDS!", count, szTo, szInteraction);
			if (!szService || !*szService) szService = "gp2gp";
			const char *szQuery = hprintf(NULL, "SELECT * from services WHERE objectclass='nhsmhs' AND nhsmhspartykey='%s' AND nhsmhsin='%s' AND nhsmhsSN like '%%%s'", szTo, szInteraction, szService);
			rogxml *rxServiceResults = sds_Query(szQuery, 1);
			szDelete(szQuery);
			int count=rogxml_GetAttrInt(rxServiceResults, "count", 0);
			if (!count && !strcmp(szService, "gp2gp")) {		// trying %gp2gp failed, we'll try %pdsquery
				szService="pdsquery";
				rogxml_Delete(rxServiceResults);
				const char *szQuery = hprintf(NULL, "SELECT * from services WHERE objectclass='nhsmhs' AND nhsmhspartykey='%s' AND nhsmhsin='%s' AND nhsmhsSN like '%%%s'", szTo, szInteraction, szService);
				rxServiceResults = sds_Query(szQuery, 1);
				szDelete(szQuery);
				count=rogxml_GetAttrInt(rxServiceResults, "count", 0);
			}
			if (count == 0) {
				Log("Found multiple contracts (and none match %%gp2gp or %%pdsquery), using first found");
				rogxml_Delete(rxServiceResults);
			} else if (count == 1) {
				Log("Using contract matching '%%%s'", szService);
				rogxml_Delete(rxResults);
				rxResults=rxServiceResults;
			} else {
				Log("Still found %d contracts even matching on '%%%s' - using the first", count, szService);
				rogxml_Delete(rxResults);
				rxResults=rxServiceResults;
			}
		}
	}

	if (ok) {
		rxResult =rogxml_FindFirstChild(rxResults);		// First (and only) child of results is result

		if (!rxResult) {
			Log("Internal error - results with count=1 returned with no 'result' child");
			ok=0;
		}
	}

	SSMAP *cm = NULL;							// We'll read into a new one in case it all goes wrong

//	const char *szDefault = cmap_DefaultFile(szTo, szInteraction);	// Open any default contract properties
//	if (szDefault) cm=cmap_ReadFile(cm, szDefault, 0);				// Read in default properties

	if (ok) {
		rogxml *rx;

		if (!cm) cm=ssmap_New();									// In case there's no default file
		ssmap_Add(cm, "cachetime",msg_Now());
		const char *szWrapper="nasp";								// Nasp unless syncreplymode isn't 'none'

		for (rx=rogxml_FindFirstChild(rxResult);rx;rx=rogxml_FindNextSibling(rx)) {	// Iterate the results
			const char *szName = rogxml_GetAttr(rx, "name");

			if (!strncasecmp(szName, "nhsmhs", 6)) {
				char *name=strdup(szName+6);
				strlwr(name);
				const char *szValue = rogxml_GetValue(rx);
				ssmap_Add(cm, name, szValue);
// 21751: Commented out next line on the grounds that if there are contract properties available, it can't be nasp
//				if (!strcmp(name, "syncreplymode") && stricmp(szValue, "none"))	// name = syncreplymode, value != none
					szWrapper="ebXML";
//				Log("Contract map value %s = %s", name, szValue);
			}
		}
		ssmap_Add(cm, "wrapper", szWrapper);						// Set appropriate wrapper
		const char *chp=strstr(szInteraction, "UK");				// UK04+ is P1R2, otherwise P1R1
		if (chp) {
			ssmap_Add(cm, "level", (atoi(chp+2) >= 4) ? "P1R2" : "P1R1");
		}
	}

//	if (szDefault) cm = cmap_ReadFile(cm, szDefault, 1);			// Over-ride properties in CAPS
//	szDelete(szDefault);

	if (cm) {
		const char *szFilename = cmap_Filename(szTo, szInteraction);
		if (szFilename) {
			char *tmp=strdup(szFilename);
			char *slash=strrchr(tmp, '/');
			if (slash) {							// Directory needs creating
				*slash='\0';
				if (!rog_MkDir(tmp)) {
					Log("Cannot create directory (%s) for contract %s/%s", tmp, szTo, szInteraction);
					ok=0;
				}
			}
			szDelete(tmp);
			chmod(szFilename, 0666);			// Ensure anyone can read it
			if (!cmap_WriteFile(cm, szFilename))
				Log("Error: Could not write cache at \"%s\"", szFilename);
			szDelete(szFilename);
		}
		ssmap_Delete(cm);
	}

	rogxml_Delete(rxResults);

	return 1;
}

SSMAP *GetIniSection(const char *szFilename, const char *szSection)
// Gets a section from an ini file as a map
// All name parts are lower cased
// Returns NULL if there is no file or no section
// An empty section will return an empty map
{
	SSMAP *map = NULL;

	FILE *fp = fopen(szFilename, "r");

	if (fp) {
		const char *szLine;
		int found=0;								// Found our section

		while ((szLine = ReadLine(fp))) {
			const char *chp=SkipSpaces(szLine);

			if (*chp == '[') {
				const char *szKet = strchr(chp+1, ']');

				if (szKet && !strncasecmp(chp+1, szSection, szKet-chp-1)) {
					found=1;
					break;
				}
			}
		}

		if (found) {								// Found the section, the next line read will be the first value
			map = ssmap_New();						// We need an actual map to read into

			while ((szLine = ReadLine(fp))) {
				const char *chp=SkipSpaces(szLine);

				if (*chp == '[') break;				// Run into another section
				if (*chp == '#' || *chp == '\0') continue;	// Comment or blank
				char *szValue = strchr(chp, '=');	// Minor cheat here as chp is const and szValue isn't...
				if (szValue) {
					*szValue='\0';
					szValue=SkipSpaces(szValue+1);
					char *szName=strdup(chp);
					rtrim(szName);
					strlwr(szName);
					ssmap_Add(map, szName, szValue);
					szDelete(szName);
				}
			}
		}

		fclose(fp);
	}

	return map;
}

void Override(SSMAP *cm, SSMAP *ov, const char *szName)
// Updates the value in cm with that in ov
{
	const char *szValueNew = ssmap_GetValue(ov, szName);
	const char *szValueOld = ssmap_GetValue(cm, szName);

	if (szValueNew) {
		if (szValueOld && !stricmp(szValueOld, szValueNew)) {
			Log("WARNING: Contract property '%s' over-ridden by same value ('%s')", szName, szValueNew);
		} else {
			ssmap_Add(cm, szName, szValueNew);
		}
	}
}

void cmap_OverrideFile(SSMAP *cm, const char *szTo, const char *szInteraction, const char *szFilename)
{
	SSMAP *map_Override = GetIniSection(szFilename, szInteraction);

	if (map_Override) {
		Override(cm, map_Override, "retries");
		Override(cm, map_Override, "cpaid");
		Override(cm, map_Override, "sn");
		Override(cm, map_Override, "retryinterval");
		Override(cm, map_Override, "syncreplymode");
		Override(cm, map_Override, "persistduration");
		Override(cm, map_Override, "isauthenticated");
		Override(cm, map_Override, "duplicateelimination");
		Override(cm, map_Override, "ackrequested");
		Override(cm, map_Override, "actor");
		Override(cm, map_Override, "endpoint");
		Override(cm, map_Override, "in");
		Override(cm, map_Override, "partykey");
		Override(cm, map_Override, "svcia");
		Override(cm, map_Override, "wrapper");
		Override(cm, map_Override, "level");
		ssmap_Delete(map_Override);
	}
}

void cmap_Overrides(SSMAP *cm, const char *szTo, const char *szInteraction)
// Over-rides contract values first using the environment specific one
{
	const char *szEnvOverride = hprintf(NULL, "%s/overrides", _szEnvDir);
	cmap_OverrideFile(cm, szTo, szInteraction, szEnvOverride);
	szDelete(szEnvOverride);
}

SSMAP *cmap_Load(const char *szTo, const char *szInteraction, const char *szService)
// Main contract load function, which returns an SSMAP* containing the contract properties
// Will pick up the cached contract if it is recent or gets a new one from SDS if it is out of date
// returns	SSMAP*	Contract property map
//			NULL	Nothing in cache and could not get from SDS
{
	SSMAP *cm = NULL;
	const char *szCacheTime = NULL;						// When the cached contract was last refreshed
	int ok=0;

if (bFunctions) Log("cmap_Load(\"%s\", \"%s\", \"%s\")", szTo, szInteraction, szService);
	const char *szFilename = cmap_Filename(szTo, szInteraction);

	cm = cmap_ReadFile(NULL, szFilename);				// Read the cached file
	if (cm) {
		szCacheTime=ssmap_GetValue(cm, "cachetime");
		ok=1;
	}

	// Constant in next line determines what happens if the contract isn't stamped with a cache time
	// 0000-00-00T00:00:00 will force a re-fetch, 9999-99-99T99:99:99 will inhibit one
	if (!szCacheTime) szCacheTime=strdup("0000-00-00T00:00:00");

	const char *szBestBefore = config_EnvGetString("contractbestbefore");
	if (!szBestBefore) szBestBefore = strdup("P1D");		// Default of 1 day best before
	const char *szSellBy = msg_AddPeriod(szCacheTime, szBestBefore);

	if (strcmp(msg_Now(), szSellBy) > 0) {			// We're beyond our sell by date...
		ok=0;
	}
	szDelete(szBestBefore);

	if (!ok) {
		cmap_RefreshCache(szTo, szInteraction, szService);
		SSMAP *cm2=cmap_ReadFile(NULL, szFilename);		// Use a temporary so we don't destroy or original if it fails

		if (cm2) {										// We read a new one in ok
			ssmap_Delete(cm);
			cm=cm2;
		}
	}

	szDelete(szFilename);

	if (!cm) cm=ssmap_New();							// Give ourselves a blank to start with

	cmap_Overrides(cm, szTo, szInteraction);			// Apply amendments from over-ride files

	return cm;
}

static int contract_FromCmap(contract_t *c, SSMAP *cm)
// Given a SSMAP* with contract properties, fills a contract_t* from it
// Returns	1	Loaded ok
//			0	Duff cm
{
	int nResult;

	if (cm) {
		ssmap_Reset(cm);
		const char *szName;
		const char *szValue;

		strset(&c->szWrapper, "ebXML");				// The defaults if we find a file
		strset(&c->szLevel, "P1R1");

		while (ssmap_GetNextEntry(cm, &szName, &szValue)) {
			if (!stricmp(szName, "Retries")) { c->nRetries=atoi(szValue);
			} else if (!stricmp(szName, "cpaId")) {					strset(&c->szId, szValue);
			} else if (!stricmp(szName, "sn")) {					strset(&c->szService, szValue);
			} else if (!stricmp(szName, "RetryInterval")) {			strset(&c->szRetryInterval, szValue);
			} else if (!stricmp(szName, "SyncReplyMode")) {			strset(&c->szSyncReplyMode, szValue);
			} else if (!stricmp(szName, "PersistDuration")) {		strset(&c->szPersistDuration, szValue);
			} else if (!stricmp(szName, "IsAuthenticated")) {		strset(&c->szIsAuthenticated, szValue);
			} else if (!stricmp(szName, "DuplicateElimination")) {	strset(&c->szDuplicateElimination, szValue);
			} else if (!stricmp(szName, "AckRequested")) {			strset(&c->szAckRequested, szValue);
			} else if (!stricmp(szName, "Actor")) {					strset(&c->szActor, szValue);
			} else if (!stricmp(szName, "Endpoint")) {				strset(&c->szEndpoint, szValue);
			} else if (!stricmp(szName, "in")) {					strset(&c->szIn, szValue);
			} else if (!stricmp(szName, "partykey")) {				strset(&c->szPartyKey, szValue);
			} else if (!stricmp(szName, "svcia")) {					strset(&c->szSvcIa, szValue);
			} else if (!stricmp(szName, "Wrapper")) {				strset(&c->szWrapper, szValue);
			} else if (!stricmp(szName, "Level")) {					strset(&c->szLevel, szValue);
			}
		}

		SplitEndpoint(c);
		nResult = 1;
	} else {
		nResult = 0;
	}

	return nResult;
}

static int contract_Load(contract_t *c, const char *szTo, const char *szInteraction, const char *szService)
{
if (bFunctions) Log("contract_Load(%x, \"%s\", \"%s\",\"%s\")", c, szTo, szInteraction, szService);
	SSMAP *cm = cmap_Load(szTo, szInteraction, szService);

	if (cm) {
		contract_FromCmap(c, cm);
		ssmap_Delete(cm);
	}

	return !!cm;
}

static int contractLoad(contract_t *c, const char *szTo, const char *szInteraction, const char *szService)
// Loads contract details.
// If szInteraction is NULL then the content is cleared to 'nothingy' values
// Returns	1	Loaded ok
//			0	Didn't load
{
	const char *szFilename = NULL;
	FILE *fp = NULL;
	const char *szLine;
	int nResult;

	if (!szInteraction) return contract_Blank(c);

if (bFunctions) Log("contractLoad(%x, \"%s\", \"%s\", \"%s\")", c, szTo, szInteraction, szService);
	char bStaticContracts = config_EnvGetBool("staticcontracts", 0);
	if (!bStaticContracts) return contract_Load(c, szTo, szInteraction, szService);	// Do contracts the new way

//////////////////////////////////////
// NB. Code below here only used if 'staticcontracts' is set above
//////////////////////////////////////

	if (!_szContractDir) return 0;				// Contract dir has not been set

	if (!fp && szTo) {							// contracts/szTo/szInteraction
		szDelete(szFilename);
		szFilename=hprintf(NULL, "%s/%s/%s", _szContractDir, szTo, szInteraction);
		fp=fopen(szFilename, "r");
//Log("Result of '%s' = %x", szFilename, fp);
	}

	if (!fp) {									// contracts/default/szInteraction
		szDelete(szFilename);
		szFilename=hprintf(NULL, "%s/default/%s", _szContractDir, szInteraction);
		fp=fopen(szFilename, "r");
//Log("Result of '%s' = %x", szFilename, fp);
	}

	if (!fp) {									// contracts/szInteraction
		szDelete(szFilename);
		szFilename=hprintf(NULL, "%s/%s", _szContractDir, szInteraction);
		fp=fopen(szFilename, "r");
//Log("Result of '%s' = %x", szFilename, fp);
	}

	if (fp) {
		strset(&c->szWrapper, "ebXML");				// The defaults if we find a file
		strset(&c->szLevel, "P1R1");

		while ((szLine=hReadFileLine(fp))) {
			const char *szOriginal = szLine;

			szLine=SkipSpaces(szLine);
			if (*szLine && *szLine != '#') {
				const char *szEquals=strchr(szLine, '=');

				if (szEquals) {
					char *szName=(char*)strnappend(NULL, szLine, szEquals-szLine);
					char *szValue=strdup(SkipSpaces(szEquals+1));

					mmts_TrimTrailing(szName);
					mmts_TrimTrailing(szValue);
					if (!stricmp(szName, "Retries")) { c->nRetries=atoi(szValue);
					} else if (!stricmp(szName, "cpaId")) {					strset(&c->szId, szValue);
					} else if (!stricmp(szName, "sn")) {					strset(&c->szService, szValue);
					} else if (!stricmp(szName, "RetryInterval")) {			strset(&c->szRetryInterval, szValue);
					} else if (!stricmp(szName, "SyncReplyMode")) {			strset(&c->szSyncReplyMode, szValue);
					} else if (!stricmp(szName, "PersistDuration")) {		strset(&c->szPersistDuration, szValue);
					} else if (!stricmp(szName, "IsAuthenticated")) {		strset(&c->szIsAuthenticated, szValue);
					} else if (!stricmp(szName, "DuplicateElimination")) {	strset(&c->szDuplicateElimination, szValue);
					} else if (!stricmp(szName, "AckRequested")) {			strset(&c->szAckRequested, szValue);
					} else if (!stricmp(szName, "Actor")) {					strset(&c->szActor, szValue);
					} else if (!stricmp(szName, "Endpoint")) {				strset(&c->szEndpoint, szValue);
					} else if (!stricmp(szName, "in")) {					strset(&c->szIn, szValue);
					} else if (!stricmp(szName, "partykey")) {				strset(&c->szPartyKey, szValue);
					} else if (!stricmp(szName, "svcia")) {					strset(&c->szSvcIa, szValue);
					} else if (!stricmp(szName, "Wrapper")) {				strset(&c->szWrapper, szValue);
					} else if (!stricmp(szName, "Level")) {					strset(&c->szLevel, szValue);
					} else {
						// ERROR - Unknown name
					}
					szDelete(szName);
					szDelete(szValue);
				}
			}
			szDelete(szOriginal);
		}

		fclose(fp);
		SplitEndpoint(c);
		nResult = 1;
	} else {
//Log("Could not find contract for %s to %s", szInteraction, szTo);
		nResult = 0;
	}

	szDelete(szFilename);

	return nResult;
}

void contract_Delete(contract_t *c)
{
	if (c) {
		szDelete(c->szId);
		szDelete(c->szService);
		szDelete(c->szRetryInterval);
		szDelete(c->szSyncReplyMode);
		szDelete(c->szPersistDuration);
		szDelete(c->szIsAuthenticated);
		szDelete(c->szDuplicateElimination);
		szDelete(c->szAckRequested);
		szDelete(c->szActor);
		szDelete(c->szEndpoint);
		szDelete(c->szIn);
		szDelete(c->szPartyKey);
		szDelete(c->szSvcIa);
		szDelete(c->szProtocol);
		szDelete(c->szAddress);
		szDelete(c->szURI);
		szDelete(c->szLevel);
		free((char*)c);
	}
}

contract_t *contract_New(const char *szTo, const char *szInteraction, const char *szService)
// Note the comment below relates to times when we didn't pull contracts from SDS
//
// Gets the contract for the given InteractionId to the party.
// This will exist as a file under the contracts directory in one of the following places
//		contracts/szTo/szInteraction
//		contracts/default/szInteraction
//		contracts/szInteraction									// Deprecated
// If the contract isn't found then NULL values will be used instead.
// Returns	contract_t*	Ok, contract loaded (may be complete default though)
//			NULL		Out of memory
{
	contract_t *c = NULL;

	c = NEW(contract_t, 1);
	if (c) {
		contractLoad(c, szTo, NULL, NULL);					// Set us a clean contract
		if (szInteraction) {								// Load a specific one if it's specified
			if (!contractLoad(c, szTo, szInteraction, szService)) {	// Failed to load
				contract_Delete(c);
				c=NULL;
			}
		}
	}

	return c;
}

int contract_IsEbXml(contract_t *c)						{ return !stricmp(c->szWrapper, "ebxml");}
int contract_IsNasp(contract_t *c)						{ return !stricmp(c->szWrapper, "nasp");}

const char *contract_GetProtocol(contract_t *c)			{ return c->szProtocol; }
const char *contract_GetAddress(contract_t *c)			{ return c->szAddress; }
int contract_GetPort(contract_t *c)						{ return c->nPort; }
const char *contract_GetURI(contract_t *c)				{ return c->szURI; }
const char *contract_GetId(contract_t *c)				{ return c->szId; }
const char *contract_GetService(contract_t *c)			{ return c->szService; }
const char *contract_GetEndpoint(contract_t *c)			{ return c->szEndpoint; }
const char *contract_GetWrapper(contract_t *c)			{ return c->szWrapper; }
const char *contract_GetRetryInterval(contract_t *c)	{ return c->szRetryInterval; }
const char *contract_GetDuplicateElimination(contract_t *c)	{ return c->szDuplicateElimination; }
const char *contract_GetLevel(contract_t *c)			{ return c->szLevel; }
const char *contract_GetAckRequested(contract_t *c)		{ return c->szAckRequested; }
const char *contract_GetActor(contract_t *c)			{ return c->szActor; }
const char *contract_GetSyncReplyMode(contract_t *c)	{ return c->szSyncReplyMode; }
const char *contract_GetPersistDuration(contract_t *c)	{ return c->szPersistDuration; }
int contract_GetRetries(contract_t *c)					{ return c->nRetries; }

void contract_SetContractDir(const char *szContractDir)
{
	szDelete(_szContractDir);
	_szContractDir = szContractDir ? strdup(szContractDir) : NULL;
}

void contract_SetEnvDir(const char *szEnvDir)
{
	szDelete(_szEnvDir);
	_szEnvDir = szEnvDir ? strdup(szEnvDir) : NULL;
}

