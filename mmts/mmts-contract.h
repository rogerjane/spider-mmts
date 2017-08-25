
#ifndef __MMTS_CONTRACT_H
#define __MMTS_CONTRACT_H

// These properties are defined from page 30 of the external interface spec v5 or p47 of v6.1
typedef struct contract_t {
	int nRetries;						// 0...
	const char *szCacheTime;			// When this contract was cached
	const char *szId;					// "ebs0001" Corresponds to ebXML CPAId
	const char *szService;				// "urn:nhs:names:services:*" where *=pds,lrs,ebs,psis,mm,poc,mh.gp2gp...
	const char *szRetryInterval;		// Duration - e.g. PT10S
	const char *szSyncReplyMode;		// "None", "MHSSignalsOnly", "SignalsOnly", "ResponseOnly", "SignalsAndResponce"
	const char *szPersistDuration;		// Duration - e.g. PT1D (> (nRetries + 1) * szRetryInterval)
	const char *szIsAuthenticated;		// "none", "transient" or "persistent"
	const char *szDuplicateElimination;	// "always", "never" or "perMessage"
	const char *szAckRequested;			// "always", "never" or "perMessage"
	const char *szActor;				// "urn:oasis:names:tc:ebxml-msg:actor:*" where *="nextMSH" and/or "ToPartyMSH"
	const char *szEndpoint;				// URL destination eg. "https://async.national.carerecords.nhs.uk:543/Thingy"
	const char *szIn;					// Message Identifier - e.g. "PRPA_IN010000UK02"
	const char *szPartyKey;				// PartyKey - e.g. "T141A-0000532"
	const char *szSvcIa;				// Not sure - e.g. "urn:nhs:names:services:ebs:PRPA_IN010000UK02"
	const char *szProtocol;				// Protocol (without trailing ':')
	const char *szAddress;				// Port number
	int nPort;							// Address as dotted quad or DNS recognisable
	const char *szURI;					// URI with leading '/'
	const char *szWrapper;				// Wrapper type = 'Nasp' or otherwise
	const char *szLevel;				// NPfIT level (P1R1, P1R2 etc) - determines wrapping etc.
} contract_t;

void contract_Delete(contract_t *c);
contract_t *contract_New(const char *szTo, const char *szType, const char *szService);
const char *contract_GetProtocol(contract_t *c);
const char *contract_GetAddress(contract_t *c) ;
int contract_GetPort(contract_t *c);
const char *contract_GetURI(contract_t *c);
const char *contract_GetId(contract_t *c);
const char *contract_GetService(contract_t *c);
const char *contract_GetEndpoint(contract_t *c);
const char *contract_GetWrapper(contract_t *c);
const char *contract_GetRetryInterval(contract_t *c);
const char *contract_GetDuplicateElimination(contract_t *c);
const char *contract_GetSyncReplyMode(contract_t *c);
const char *contract_GetPersistDuration(contract_t *c);
const char *contract_GetAckRequested(contract_t *c);
const char *contract_GetActor(contract_t *c);
const char *contract_GetLevel(contract_t *c);
int contract_GetRetries(contract_t *c);
void contract_SetContractDir(const char *szContractDir);

int contract_IsEbXml(contract_t *c);
int contract_IsNasp(contract_t *c);
void contract_SetEnvDir(const char *szEnvDir);

#endif
