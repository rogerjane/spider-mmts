
#define _GNU_SOURCE

#include <ctype.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

#include <guid.h>
#include <mtmacro.h>
#include <heapstrings.h>
#include <mmts-message.h>
#include <mmts-utils.h>

#include <time.h>

#define STATIC static

static const char *szLogDir = NULL;

// This variable is declared in mmts.c
extern enum PROTOCOL_VERSION mmtsProtocol;

void msg_SetLogDir(const char *szDir)
{
	szDelete(szLogDir);
	szLogDir = szDir ? strdup(szDir) : NULL;
}

const char *msg_LogFile(const char *szFilename)
// Returns a, short lived, filename consisting of the filename passed within the current log directory.
// Don't even dream of freeing this yourself.
{
	static const char *szPath = NULL;
	const char *szDir=szLogDir ? szLogDir : "/tmp";

	szDelete(szPath);

	if (szFilename) {
		szPath = hprintf(NULL, "%s/%s", szDir, szFilename);
	} else {
		szPath = strdup(szDir);
	}

	return szPath;
}

void msg_Log(const char *szFilename, const char *szFmt, ...)
// Really this is just used for debugging purposes.
// Give it a filename and a printf() type list and it'll stuff it in a file in the local directory if it has been
// set or /tmp if it hasn't.
{
	const char *szLogFile;
	FILE *fp;

	szLogFile = msg_LogFile(szFilename);
	fp=fopen(szLogFile, "a");
	if (fp) {
		va_list ap;

		va_start(ap, szFmt);
		vfprintf(fp, szFmt, ap);
		va_end(ap);

		fclose(fp);
		chmod(szLogFile, 0666);
	}
}

void msg_DeleteAttachment(msg_attachment *a)
{
	if (a) {
		szDelete(a->szName);
		szDelete(a->szContentType);
		szDelete(a->szContentId);
		szDelete(a->szDescription);
		szDelete(a->szReferenceId);
		free((char*)a->data);
		free((char*)a);
	}
}

msg_attachment *msg_NewAttachment(const char *szName, const char *szContentType, const char *szContentId, int len, const char *data)
// NB.  If non-NULL data is passed, it MUST be on the heap and it's responsibility passes to this
//		structure.  I.e. DO NOT DELETE THE DATA YOURSELF - msg_Delete will do it when it deletes the attachment.
{
	msg_attachment *a = NEW(msg_attachment, 1);

	if (len == -1) len=data?strlen(data):0;

	a->nLen=len;
	a->data=(char*)data;
	a->szName = szName ? strdup(szName) : NULL;
	a->szContentType = szContentType ? strdup(szContentType) : NULL;
	a->szContentId = szContentId ? strdup(szContentId) : NULL;
	a->szDescription = NULL;
	a->szReferenceId = NULL;

	return a;
}

void msg_SetAttr(MSG *m, const char *szName, const char *szValue)
// Set a named attribute in the message
{
	if (m && szName) {
		if (szValue) {
			ssmap_Add(m->attrs, szName, szValue);
		} else {
			ssmap_DeleteKey(m->attrs, szName);
		}
	}
}

const char *msg_Attr(MSG *m, const char *szName)
// Get a named attribute from the message
{
	return (m && szName) ? ssmap_GetValue(m->attrs, szName) : NULL;
}

void msg_SetIntAttr(MSG *m, const char *szName, int nValue)
{
	char buf[20];

	snprintf(buf, sizeof(buf), "%d", nValue);
	msg_SetAttr(m, szName, buf);
}

int msg_IntAttr(MSG *m, const char *szName, int nDef)
{
	const char *szValue = msg_Attr(m, szName);

	return szValue ? atoi(szValue) : nDef;
}

void msg_Delete(MSG *m)
{
	if (m) {
		int i;

		rogxml_Delete(m->xml);
		contract_Delete(m->contract);
		ssmap_Delete(m->attrs);
		for (i=0;i<m->nAttach;i++)
			msg_DeleteAttachment(m->attachment[i]);
		free((char*)m->attachment);
		free((char*)m);
		szDelete(m->szError);
		szDelete(m->szHttpCodeText);
		szDelete(m->szInteractionId);
		szDelete(m->szConversationId);
		szDelete(m->szContentId);
		szDelete(m->szDescription);
		szDelete(m->szReferenceId);
		szDelete(m->szEndpoint);
		szDelete(m->szSoapAction);
		szDelete(m->szContract);
		szDelete(m->szURI);
	}
}

void msg_SetError(MSG *m, int nErr, const char *szFmt, ...)
{
	m->nError = nErr;
	szDelete(m->szError);
	if (szFmt) {
		char buf[1000];
		va_list ap;

		va_start(ap, szFmt);
		vsnprintf(buf, sizeof(buf), szFmt, ap);
		va_end(ap);
		m->szError=strdup(buf);
	} else {
		m->szError=NULL;
	}
}

void msg_SetMessageId(MSG *m, const char *szMessageId)				{ strset(&m->szMessageId, szMessageId); }
void msg_SetWrapperId(MSG *m, const char *szWrapperId)				{ strset(&m->szWrapperId, szWrapperId); }
void msg_SetInternalId(MSG *m, const char *szInternalId)			{ strset(&m->szInternalId, szInternalId); }
void msg_SetConversationId(MSG *m, const char *szConversationId)	{ strset(&m->szConversationId, szConversationId); }
void msg_SetContentId(MSG *m, const char *szContentId)				{ strset(&m->szContentId, szContentId); }
void msg_SetDescription(MSG *m, const char *szDescription)			{ strset(&m->szDescription, szDescription); }
void msg_SetReferenceId(MSG *m, const char *szReferenceId)			{ strset(&m->szReferenceId, szReferenceId); }

const char *GetPartyKeyFromAsid(const char *szAsid, const char **pFile);

static const char *GetPartyIdAt(rogxml *rx, const char *szPath)
// Given a path to an id, gets the partyid (OID of "2.16.840.1.113883.2.1.3.2.4.10" or "...11".
{
	int i;
	rogxpath *rxp = rogxpath_New(rx, szPath);
	const char *szPartyId = NULL;

	for (i=1;i<=rogxpath_GetCount(rxp);i++) {
		rogxml *rx=rogxpath_GetElement(rxp, i);
		const char *szRoot=rogxml_GetAttributeValue(rx, "root");

		if (!strcmp(szRoot, "1.2.826.0.1285.0.2.0.107")) {				// ASID
			const char *szAsid=rogxml_GetAttributeValue(rx, "extension");
			const char *szFile = NULL;
			szPartyId=GetPartyKeyFromAsid(szAsid, &szFile);				// See if we have a mapping to a partykey
			if (szPartyId) {
				Log("Ascertained partykey (%s) from ASID %s (%s)", szPartyId, szAsid, szFile);
				szDelete(szFile);
				break;										// Got it...
			}
			szDelete(szFile);
		}

		if (!strcmp(szRoot, "2.16.840.1.113883.2.1.3.2.4.10") ||		// NPfIT Messaging Endpoint ids
				!strcmp(szRoot, "2.16.840.1.113883.2.1.3.2.4.11")) {	// NHS SDS
			szPartyId=rogxml_GetAttributeValue(rx, "extension");
			break;														// Got it...
		}
	}
	rogxpath_Delete(rxp);

	return szPartyId;
}

static const char *GetAsidAt(rogxml *rx, const char *szPath)
// Given a path to an id, gets the partyid (OID of "1.2.826.0.1285.0.2.0.107"
{
	int i;
	rogxpath *rxp = rogxpath_New(rx, szPath);
	const char *szAsid = NULL;

	for (i=1;i<=rogxpath_GetCount(rxp);i++) {
		rogxml *rx=rogxpath_GetElement(rxp, i);
		const char *szRoot=rogxml_GetAttributeValue(rx, "root");

		if (!strcmp(szRoot, "1.2.826.0.1285.0.2.0.107")) {				// ASID
			szAsid=rogxml_GetAttributeValue(rx, "extension");
			break;														// Got it...
		}
	}
	rogxpath_Delete(rxp);

	return szAsid;
}

int msg_AscertainWrapping(MSG *m)
// Ensures wrapped and wrapper (if wrapped) status is correct
// Returns the result and stores it in the message
{
	rogxml *rxXML=m->xml;
	const char *szURI;
	const char *szLocalName;

	m->nWrapped=WRAP_UNWRAPPED;												// Not wrapped until proven otherwise

	if (!rxXML) return -1;

	szURI=rogxml_GetURI(rxXML);
	szLocalName=rogxml_GetLocalName(rxXML);
	if (!strcmp(szURI, "http://schemas.xmlsoap.org/soap/envelope/") &&		// In SOAP namespace
			!strcmp(szLocalName, "Envelope")) {								// And it's ':Envelope'
		const char *szHeaderURI;
		rogxml *rxHead, *rxChild, *rxBody;

		rxHead=rogxml_FindFirstChild(rxXML);								// SOAP Header
		rxChild=rogxml_FindFirstChild(rxHead);								// SOAP Header/*[1]
		if (rxChild) {
			szHeaderURI=rogxml_GetURI(rxChild);
			if (!strcmp(szHeaderURI, "hl7") || !strcmp(szHeaderURI, "urn:hl7-org:v3")) { // Directly embedded HL7 (NASP)
				m->nWrapped=WRAP_NASP;
			} else {
				m->nWrapped=WRAP_EBXML;
			}
		} else {															// No SOAP header content, SOAP:Fault?
			rxBody = rogxml_FindNextSibling(rxHead);
			rxChild=rogxml_FindFirstChild(rxBody);							// SOAP Body/*[1]

			if (!strcmp(rogxml_GetLocalName(rxChild), "Fault")) {			// Fault type
				m->nWrapped=WRAP_FAULT;
			} else {
				m->nWrapped=WRAP_EBXML;										// RJ 01-06-13 No header, but is still ebXML
			}
		}
		m->nWrapper=m->nWrapped;
	}

	return m->nWrapped;
}

int msg_ExtractMessageInfo(MSG *m)
// Delves into the wrapper (or message) in the message and pulls out...
// 0: Plain HL7 (unwrapped)
//   szInteractionId	= /*/interactionId/@extension
//   szInternalId		= /*/id/@root - don't get this as we want the wrapper id here
//   szToPartyId		= GetPartyIdAt(/*/communicationFunctionRcv/device/id)
//   szFromPartyId		= GetPartyIdAt(/*/communicationFunctionSnd/device/id)
//   szToAsid			= GetAsidAt(/*/communicationFunctionRcv/device/id)
//   szFromAsid			= GetAsidAt(/*/communicationFunctionSnd/device/id)
//   szService			=
// 1a: Nasp-P1R1:
//   szInteractionId	= /Envelope/Header/*/interactionId/@extension
//   szMessageId		= /Envelope/Header/*/id/@root
//   szRefToMessageId	= /Envelope/Header/*/Acknowledgement/MessageRef/id/@root
//   szToPartyId		= GetPartyIdAt(/Envelope/Header/*/communicationFunctionRcv/device/id)
//   szFromPartyId		= GetPartyIdAt(/Envelope/Header/*/communicationFunctionSnd/device/id)
//   bHasPayload		= 1
//   szService			=
// 1b: Nasp-P1R2:
//   szInteractionId	= /Envelope/Header/Action (part after last '/')
//   szMessageId		= /Envelope/Header/MessageID (excluding leading 'uuid:')
//   szRefToMessageId	= /Envelope/Header/RelatesTo (excluding leading 'uuid:')
//   szToPartyId		= GetPartyIdAt(/Envelope/Header/communicationFunctionRcv/device/id)
//   szFromPartyId		= GetPartyIdAt(/Envelope/Header/communicationFunctionSnd/device/id)
//   bHasPayload		= 1
//   szService			=
// 2: ebXML:
//   szInteractionId	= /Envelope/Header/MessageHeader/Action
//   szConversationId	= /Envelope/Header/MessageHeader/ConversationId
//   szMessageId		= /Envelope/Header/MessageHeader/MessageData/MessageId
//   szRefToMessageId	= /Envelope/Header/MessageHeader/MessageData/RefToMessageId
//       or				= /Envelope/Header/MessageHeader/ConversationId (if different to MessageId)
//   szToPartyId		= /Envelope/Header/MessageHeader/To/PartyId
//   szFromPartyId		= /Envelope/Header/MessageHeader/From/PartyId
//   bAckRequested		= /Envelope/Header/AckRequested
//   bIsAck				= /Envelope/Header/Acknowledgement
//   bHasPayload		= 1 iff SOAP body contains anything
//   szService			=
// 3: Fault:
//   nError             = /Body/Fault/faultString NNNN: text error
//   szError            = /Body/Fault/faultString nnnn: TEXT ERROR
// 5: ITK (may not be strictly ITK, but looks like it)
//
{
	rogxml *rxXML=m->xml;

	const char *szInteractionId = NULL;
	const char *szConversationId = NULL;
	const char *szMessageId = NULL;
	const char *szInternalId = NULL;
	const char *szRefToMessageId = NULL;
	const char *szToPartyId = NULL;
	const char *szFromPartyId = NULL;
	const char *szToAsid = NULL;
	const char *szFromAsid = NULL;
	const char *szWrapperId = NULL;

	int nWrapped = m->nWrapped;

//Log("Wrapped = %d, ", nWrapped);
	if (nWrapped == WRAP_NONE) {
		nWrapped = msg_AscertainWrapping(m);
	}

	if (nWrapped == WRAP_UNWRAPPED) {					// Message is unwrapped
//Log("msg_ExtractMessageInfo(%x) - WRAP_UNWRAPPED", m);
		// These two may not actually be unique...
		if (!strcmp(rogxml_GetLocalName(m->xml), "PAFToolQuery") || !strcmp(rogxml_GetLocalName(m->xml), "getAddressListRequest")) {
			szInteractionId = "PAFToolQuery";
			if( mmtsProtocol == EVO12 ) {
				szInternalId = rogxml_GetValueByPath(m->xml, "/*/messageHeader/messageId");	// 21992: Internal from msg
			}
			else {
				// Default (NO_PROTOCOL)
				szMessageId = rogxml_GetValueByPath(m->xml, "/*/messageHeader/messageId");
				szInternalId = szMessageId;
			}
			szToPartyId = "GazetteerService";
			szFromPartyId = "practice";
		} else {
			szInteractionId = rogxml_GetValueByPath(m->xml, "/*/interactionId/@extension");
			if( mmtsProtocol == EVO12 ) {
				szInternalId = rogxml_GetValueByPath(m->xml, "/*/id/@root");				// 21992: Internal from msg
			}
			else {
				// Default (NO_PROTOCOL)
				szMessageId = rogxml_GetValueByPath(m->xml, "/*/id/@root");
				szInternalId = szMessageId;
			}
			szToPartyId = GetPartyIdAt(m->xml, "/*/communicationFunctionRcv/device/id");
			szFromPartyId = GetPartyIdAt(m->xml, "/*/communicationFunctionSnd/device/id");
		}
		szToAsid = GetAsidAt(m->xml, "/*/communicationFunctionRcv/device/id");
		szFromAsid = GetAsidAt(m->xml, "/*/communicationFunctionSnd/device/id");
//		szToPartyId = rogxml_GetValueByPath(m->xml, "/*/communicationFunctionRcv/device/id/@extension");
//		szFromPartyId = rogxml_GetValueByPath(m->xml, "/*/communicationFunctionSnd/device/id/@extension");
	} else if (nWrapped == WRAP_ITK) {								// ITK (or similar)
		szMessageId = rogxml_GetValueByPath(rxXML, "/Envelope/Header/MessageID");
		// Example ITK message has <wsa:To>addr</wsa:To> but <wsa:From><wsa:Address>addr</wsa:Address></wsa:From>
		szToPartyId = rogxml_GetValueByPath(rxXML, "/Envelope/Header/To");
		if (!szToPartyId) szToPartyId = rogxml_GetValueByPath(rxXML, "/Envelope/Header/To/Address");
		szFromPartyId = rogxml_GetValueByPath(rxXML, "/Envelope/Header/From");
		if (!szFromPartyId) szFromPartyId = rogxml_GetValueByPath(rxXML, "/Envelope/Header/From/Address");
		const char *szReplyTo = rogxml_GetValueByPath(rxXML, "/Envelope/Header/ReplyTo");
		if (!szReplyTo) szReplyTo = rogxml_GetValueByPath(rxXML, "/Envelope/Header/ReplyTo/Address");
		msg_SetAttr(m, "soap-to",szToPartyId);
		msg_SetAttr(m, "soap-from",szFromPartyId);
		msg_SetAttr(m, "soap-reply-to",szReplyTo);
		msg_SetAttr(m, "soap-action",rogxml_GetValueByPath(rxXML, "/Envelope/Header/Action"));
		msg_SetAttr(m, "soap-security-created",rogxml_GetValueByPath(rxXML, "/Envelope/Header/Security/Timestamp/Created"));
		msg_SetAttr(m, "soap-security-expires",rogxml_GetValueByPath(rxXML, "/Envelope/Header/Security/Timestamp/Expires"));
		msg_SetAttr(m, "soap-security-username",rogxml_GetValueByPath(rxXML, "/Envelope/Header/Security/UsernameToken/Username"));
	} else if (nWrapped == WRAP_NASP) {								// Nasp
		if (m->nLevel == LEVEL_P1R1) {								// Nasp - P1R1
//Log("msg_ExtractMessageInfo(%x) - WRAP_NASP, LEVEL_P1R1", m);
			szInteractionId = rogxml_GetValueByPath(rxXML, "/Envelope/Header/*/interactionId/@extension");
			szMessageId = rogxml_GetValueByPath(rxXML, "/Envelope/Header/*/id/@root");
			szWrapperId = rogxml_GetValueByPath(rxXML, "/Envelope/Header/*/id/@root");
			szRefToMessageId = rogxml_GetValueByPath(rxXML, "/Envelope/Header/*/Acknowledgement/MessageRef/id/@root");
			if (!szRefToMessageId)
				szRefToMessageId = rogxml_GetValueByPath(rxXML, "/Envelope/Header/*/Acknowledgment/MessageRef/id/@root");
			szToPartyId = GetPartyIdAt(rxXML, "/Envelope/Header/*/communicationFunctionRcv/device/id");
			szFromPartyId = GetPartyIdAt(rxXML, "/Envelope/Header/*/communicationFunctionSnd/device/id");
//			szToPartyId = rogxml_GetValueByPath(rxXML, "/Envelope/Header/*/communicationFunctionRcv/device/id/@extension");
//			szFromPartyId = rogxml_GetValueByPath(rxXML, "/Envelope/Header/*/communicationFunctionSnd/device/id/@extension");
		} else {													// Nasp - P1R2
//Log("msg_ExtractMessageInfo(%x) - WRAP_NASP, LEVEL_P1R2", m);
			szInteractionId = rogxml_GetValueByPath(rxXML, "/Envelope/Header/Action");
			if (szInteractionId) {
				const char *chp=strrchr(szInteractionId, '/');

				if (chp) szInteractionId = chp+1;
			}
			szMessageId = rogxml_GetValueByPath(rxXML, "/Envelope/Header/MessageID");
			szWrapperId = rogxml_GetValueByPath(rxXML, "/Envelope/Header/MessageID");
			if (szMessageId && !strncmp(szMessageId, "uuid:", 5)) szMessageId += 5;
			szRefToMessageId = rogxml_GetValueByPath(rxXML, "/Envelope/Header/RelatesTo");
			if (szRefToMessageId && !strncmp(szRefToMessageId, "uuid:", 5)) szRefToMessageId += 5;
			szToPartyId = GetPartyIdAt(rxXML, "/Envelope/Header/communicationFunctionRcv/device/id");
			szFromPartyId = GetPartyIdAt(rxXML, "/Envelope/Header/communicationFunctionSnd/device/id");
//			szToPartyId = rogxml_GetValueByPath(rxXML, "/Envelope/Header/communicationFunctionRcv/device/id/@extension");
//			szFromPartyId = rogxml_GetValueByPath(rxXML, "/Envelope/Header/communicationFunctionSnd/device/id/@extension");
		}
		szConversationId = NULL;
		m->bAckRequested = 0;
		m->bIsAck = 0;
		m->bHasPayload = 1;
	} else if (nWrapped == WRAP_EBXML) {						// ebXML
		rogxml *rx;

//Log("msg_ExtractMessageInfo(%x) - WRAP_EBXML", m);
		szInteractionId = rogxml_GetValueByPath(rxXML, "/Envelope/Header/MessageHeader/Action");
		szConversationId = rogxml_GetValueByPath(rxXML, "/Envelope/Header/MessageHeader/ConversationId");
		szMessageId = rogxml_GetValueByPath(rxXML, "/Envelope/Header/MessageHeader/MessageData/MessageId");
		szWrapperId = rogxml_GetValueByPath(rxXML, "/Envelope/Header/MessageHeader/MessageData/MessageId");
		szRefToMessageId = rogxml_GetValueByPath(rxXML, "/Envelope/Header/MessageHeader/MessageData/RefToMessageId");
		if (!szRefToMessageId && strcasecmp(szMessageId, szConversationId)) {
			szRefToMessageId = szConversationId;		// Don't necessarily get 'RefTo' in the message...
		}
		szToPartyId = rogxml_GetValueByPath(rxXML, "/Envelope/Header/MessageHeader/To/PartyId");
		szFromPartyId = rogxml_GetValueByPath(rxXML, "/Envelope/Header/MessageHeader/From/PartyId");

		rx = rogxml_FindByPath(rxXML, "/Envelope/Header/AckRequested");
		m->bAckRequested = !!rx;

		rx = rogxml_FindByPath(rxXML, "/Envelope/Header/Acknowledgement");
		if (!rx) rx = rogxml_FindByPath(rxXML, "/Envelope/Header/Acknowledgment");
		m->bIsAck = !!rx;
		rx = rogxml_FindByPath(rxXML, "/Envelope/Body");
		m->bHasPayload = 0;
		if (rx) {
			rx = rogxml_FindFirstChild(rx);
			if (rx) {									// There is a child to the SOAP Body element
				m->bHasPayload = 1;
			}
		}
	} else if (nWrapped == WRAP_FAULT) {						// Fault
		const char *szFaultText = rogxml_GetValueByPath(rxXML, "/Envelope/Body/Fault/faultString");

//Log("msg_ExtractMessageInfo(%x) - Fault (%s)", m, szFaultText ? szFaultText : "NULL");
		m->nError=0;
		m->szError=NULL;
		if (szFaultText) {
			const char *szColon = strchr(szFaultText, ':');

			if (szColon) {
				szColon++;
				while (isspace(*szColon)) szColon++;

				msg_SetError(m, atoi(szFaultText), "%s", szColon);
			}
		}
	} else {
//Log("msg_ExtractMessageInfo(%x) - UNKNOWN", m);
		return 0;
	}

	if (nWrapped == WRAP_UNWRAPPED || nWrapped == WRAP_NASP || nWrapped == WRAP_EBXML || nWrapped == WRAP_ITK) {
		strset(&m->szInteractionId, szInteractionId);
		if (szMessageId) strset(&m->szMessageId, szMessageId);
		msg_SetWrapperId(m, szWrapperId);
		if (szInternalId) strset(&m->szInternalId, szInternalId);
		strset(&m->szToPartyId, szToPartyId);
		strset(&m->szFromPartyId, szFromPartyId);
		strset(&m->szToAsid, szToAsid);
		strset(&m->szFromAsid, szFromAsid);
		if (nWrapped != WRAP_UNWRAPPED) {
			strset(&m->szConversationId, szConversationId);
			strset(&m->szRefToMessageId, szRefToMessageId);
		}
	}

	return 1;
}

MSG *msg_New(rogxml *xml)
// Creates a new message from an HL7 wrapper.
// It's quite ok to pass NULL in here if you're going to add the HL7 later.
// NB.	If you do pass xml in here, it MUST be on the heap and control passes to this structure
//		I.E. YOU MUST NOT free() IT YOURSELF - this happens when msg_Delete() is called.
{
	MSG *m = NEW(MSG, 1);

	m->attrs = ssmap_New();
	m->xml=xml;
	m->szContract=NULL;
	m->contract=NULL;
	m->nAttach=0;
	m->attachment = NEW(msg_attachment*, 1);
	m->attachment[0]=NULL;
	m->nError=0;
	m->szError=NULL;
	m->nHttpCode=0;
	m->szHttpCodeText=NULL;
	m->szInteractionId=NULL;
	m->szConversationId=NULL;						// Set when needed
	m->szContentId=NULL;
	m->szDescription=NULL;
	m->szReferenceId=NULL;
	m->szMessageId=strdup(guid_ToText(NULL));
	m->szWrapperId=NULL;
	m->szInternalId=NULL;
	m->szRefToMessageId=NULL;
	m->szToPartyId=NULL;
	m->szFromPartyId=NULL;
	m->szService=NULL;
	m->szToAsid=NULL;
	m->szFromAsid=NULL;
	m->szURI=NULL;
	m->nLevel = LEVEL_P1R1;						// Assume P1R1 until proven otherwise
	m->szEndpoint=NULL;
	m->szSoapAction=NULL;

	m->szFirstSend=NULL;
	m->szLastSend=NULL;
	m->szNextSend=NULL;
	m->nSends=0;
	m->nMaxRetries=0;
	m->szRetryInterval=strdup("PT10S");

	m->nWrapper = WRAP_NONE;
	m->nWrapped = WRAP_NONE;
	m->bAckRequested = 0;
	m->bHasPayload = 0;
	m->bIsAck = 0;
	m->bIsLocalSync = 1;

	msg_ExtractMessageInfo(m);

	return m;
}

void msg_SetAttachmentDescription(msg_attachment *a, const char *szDescription)
{
	strset(&a->szDescription, szDescription);
}

void msg_SetAttachmentReferenceId(msg_attachment *a, const char *szReferenceId)
{
	strset(&a->szReferenceId, szReferenceId);
}

MSG *msg_NewReply(MSG *mr)
{
	MSG *m=msg_New(NULL);
	strset(&m->szInteractionId, mr->szInteractionId);
	strset(&m->szConversationId, mr->szConversationId);
	strset(&m->szRefToMessageId, mr->szMessageId);
	strset(&m->szToPartyId, mr->szFromPartyId);
	strset(&m->szFromPartyId, mr->szToPartyId);
	strset(&m->szToAsid, mr->szFromAsid);
	strset(&m->szFromAsid, mr->szToAsid);
	strset(&m->szService, mr->szService);
	m->nWrapper = mr->nWrapper;

	return m;
}

int msg_GetAttachmentCount(MSG *m)
{
	return m->nAttach;
}

msg_attachment *msg_GetAttachment(MSG *m, int n)
{
	if (n >= 1 && n <= m->nAttach)
		return m->attachment[n-1];
	else
		return NULL;
}

msg_attachment *msg_GetAttachmentByContentId(MSG *m, const char *id)
{
	int i;
	msg_attachment *a = NULL;

	if (m) {
		for (i=0;i<m->nAttach;i++) {
			if (!strcmp(m->attachment[i]->szContentId, id)) {
				a = m->attachment[i];
				break;
			}
		}
	}

	return a;
}

const char *msg_GetAttachmentContentType(msg_attachment *a)
{
	return a->szContentType;
}

const char *msg_GetAttachmentContentId(msg_attachment *a)
{
	if (!a->szContentId) a->szContentId = strdup(guid_ToText(NULL));

	return a->szContentId;
}

const char *msg_GetAttachmentDescription(msg_attachment *a)
{
	return a->szDescription;
}

const char *msg_GetAttachmentReferenceId(msg_attachment *a)
{
	return a->szReferenceId;
}

const char *msg_GetAttachmentName(msg_attachment *a)
{
	return a->szName;
}

const char *msg_GetAttachmentText(msg_attachment *a)
{
	return a->data;
}

int msg_GetAttachmentLength(msg_attachment *a)
{
	return a->nLen;
}

void msg_DeleteMessageAttachment(MSG *m, int n)
// Ensures that the attachment numbered does not exist - closes others up.
// If 'n' is outside the range 1..number of attachments, nothing happens
// NB. I don't bother to re-allocate the array to save space - this doesn't happen often enough!
{
	if (n < 1 || n > m->nAttach) return;

	n--;
	msg_DeleteAttachment(m->attachment[n]);
	while (n<m->nAttach) {
		m->attachment[n]=m->attachment[n+1];
		n++;
	}
	m->nAttach--;
}

void msg_AppendAttachment(MSG *m, msg_attachment *a)
// Adds an attachment to the end
{
	RENEW(m->attachment, msg_attachment*, m->nAttach+2);
	m->attachment[m->nAttach]=a;
	m->attachment[++m->nAttach]=NULL;
}

void msg_InsertAttachment(MSG *m, msg_attachment *a)
// Adds an attachment at the head of the list
{
	int i;

	RENEW(m->attachment, msg_attachment*, m->nAttach+2);
	for (i=++m->nAttach;i>0;i--)
		m->attachment[i]=m->attachment[i-1];
	m->attachment[0]=a;
}

MSG *msg_NewError(int nErr, const char *szFmt, ...)
{
	MSG *m = msg_New(NULL);

	if (szFmt) {
		char buf[1000];
		va_list ap;

		va_start(ap, szFmt);
		vsnprintf(buf, sizeof(buf), szFmt, ap);
		va_end(ap);

		msg_SetError(m, nErr, "%s", buf);
	} else {
		msg_SetError(m, nErr, NULL);
	}

	return m;
}

int msg_GetErrorNo(MSG *m)					{ return m->nError; }
const char *msg_GetErrorText(MSG *m)		{ return m->szError; }

void msg_SetEndpoint(MSG* m, const char *szEndpoint)	{ szDelete(m->szEndpoint); m->szEndpoint = szEndpoint ? strdup(szEndpoint) : NULL; }
const char *msg_GetEndpoint(MSG* m)				{ return m->szEndpoint; }
void msg_SetSoapAction(MSG* m, const char *szSoapAction)	{ szDelete(m->szSoapAction); m->szSoapAction = szSoapAction ? strdup(szSoapAction) : NULL; }
const char *msg_GetSoapAction(MSG* m)				{ return m->szSoapAction; }

int msg_GetHttpCode(MSG *m)					{ return m->nHttpCode; }
int msg_GetHttpCodeMajor(MSG *m)			{ return (m->nHttpCode / 100) *100; }
const char *msg_GetHttpCodeText(MSG *m)		{ return m->szHttpCodeText;}

void msg_SetHttpCode(MSG *m, int nCode, const char *szText)
{
	if (nCode / 100 == 2) nCode-=200;
	if (nCode != -1) m->nHttpCode = nCode;
	if (szText) strset(&m->szHttpCodeText, szText);
}

MSG *msg_NewFromInternal(rogxml *rx)
// Passed either a MMTS-Package or a plain HL7 wrapper, create a message from it.
// The 'rx' passed in becomes the responsibility of the message - i.e. DO NOT free() IT!
{
	MSG *m = NULL;

	if (!strcmp(rogxml_GetLocalName(rx), "MMTS-Package")) {		// Packaged stuff
		rogxml *xml;
		rogxml *rxAttach;
		int bIsSync;

		bIsSync = rogxml_GetAttrInt(rx, "sync", 1);
		const char *szContract = rogxml_GetAttr(rx, "contract");
		const char *szMessageId = rogxml_GetAttr(rx, "messageid");
		const char *szWrapperId = rogxml_GetAttr(rx, "wrapperid");			// Not currently used (03-10-13)
		const char *szConversationId = rogxml_GetAttr(rx, "conversationid");
		if (szContract) {
// TODO: HERE - load specific contract if in header of message
		}
		rxAttach=rogxml_FindFirstChild(rx);
		if (!rxAttach) return NULL;								// No attachments...
		xml=rogxml_FindFirstChild(rxAttach);					// Get the content, which should be the HL7
		rogxml_Unlink(xml);										// Stop it getting deleted when we delete the package
		m=msg_New(xml);
		if( mmtsProtocol == EVO12 ) {
			// Set the messageId or the conversationId if they are passed to us
			if (szMessageId) msg_SetMessageId(m, szMessageId);
			if (szConversationId) msg_SetConversationId(m, szConversationId);
		}
		else if (szMessageId || szConversationId) {					// Either one is given
			if (!szMessageId) szMessageId = szConversationId;
			if (!szConversationId) szConversationId = szMessageId;	// Default conversation ID to given message ID

			msg_SetMessageId(m, szMessageId);
			msg_SetConversationId(m, szConversationId);
		}
		msg_SetWrapperId(m, szWrapperId);
		const char *szContentId = rogxml_GetAttr(rxAttach, "content-id");
		if (szContentId) msg_SetContentId(m, szContentId);

		const char *szDescription = rogxml_GetAttr(rxAttach, "description");
		if (szDescription) msg_SetDescription(m, szDescription);

		const char *szReferenceId = rogxml_GetAttr(rxAttach, "reference-id");
		if (szReferenceId) msg_SetReferenceId(m, szReferenceId);

		m->bIsLocalSync = !!bIsSync;
		rxAttach=rogxml_FindNextSibling(rxAttach);
		while (rxAttach) {
			rogxml *rx=rogxml_FindFirstChild(rxAttach);
			msg_attachment *a;
			const char *szEncoding = rogxml_GetAttr(rxAttach, "encoding");
			const char *szContentType = rogxml_GetAttr(rxAttach, "content-type");
			const char *szContentId = rogxml_GetAttr(rxAttach, "content-id");
			const char *szDescription = rogxml_GetAttr(rxAttach, "description");
			const char *szReferenceId = rogxml_GetAttr(rxAttach, "reference-id");
			const char *szName = rogxml_GetAttr(rxAttach, "name");
			int nLen;
			const char *szData;

			if (!szEncoding) szEncoding = "plain";
			if (!szName) szName = "gerald";

			if (!strcasecmp(szEncoding, "xml")) {					// XML to copy
				if (!szContentType) szContentType="application/xml";
				nLen=-1;
				szData=rogxml_ToText(rx);
			} else if (!strcasecmp(szEncoding, "base64")) {		// Best decode it first then
				if (!szContentType) szContentType="application/octet-stream";	// Unknown data...
				szData = mime_Base64Dec(&nLen, rogxml_GetValue(rxAttach));
			} else {											// Assume it's just plain
				if (!szContentType) szContentType="text/plain";	// Unknown data really...
				nLen=-1;
				szData=strdup(rogxml_GetValue(rxAttach));
			}
			a=msg_NewAttachment(szName, szContentType, szContentId, nLen, szData);
			if (szDescription) msg_SetAttachmentDescription(a, szDescription);
			if (szReferenceId) msg_SetAttachmentReferenceId(a, szReferenceId);
			msg_AppendAttachment(m, a);

			rxAttach=rogxml_FindNextSibling(rxAttach);
		}
		rogxml_Delete(rx);
	} else {													// Just assume HL7 for now
		m=msg_New(rx);
		m->bIsLocalSync=1;										// Synchronous in this case
	}

	return m;
}

MSG *msg_NewFromMime(MIME *mime)
// Passed MIME message, create a message from it.
// The 'MIME*' passed in becomes the responsibility of the message - i.e. DO NOT free() IT!
{
	MSG *m = NULL;
	int nBodyCount;											// Number of parts in the message

	nBodyCount = mime_GetBodyCount(mime);

	if (nBodyCount) {										// Multipart message
		int i;

		for (i=1;i<=nBodyCount;i++) {
			MIME *sub=mime_GetBodyPart(mime, i);
			const char *pMessage = mime_GetBodyText(sub);			// NB. Is not \0 terminated...
			int nMessageLength = mime_GetBodyLength(sub);

			char *szMessage = NEW(char, nMessageLength+1);			// Create a 'clean' copy (\0 terminated)
			char *szMessageStart = szMessage;
			memcpy(szMessage, pMessage, nMessageLength);
			szMessage[nMessageLength]='\0';

			if (i==1) {
				rogxml *xml;

				while (isspace(*szMessage)) szMessage++;	// Ignore any blank lines, spaces etc.
				if (*szMessage && *szMessage != '<') {		// Doesn't look very soapy...
					xml=rogxml_FromText("<Error>Non-XML where SOAP expected</Error>");
				} else {
					xml=rogxml_ReadFile(szMessage);
					if (xml) {
						rogxml *rxDoc = rogxml_FindDocumentElement(xml);
						if (rxDoc != xml) {					// Need to drop any <?xml...?>
							rogxml_Unlink(rxDoc);			// Take off document element
							rogxml_Delete(xml);				// Delete the rest
							xml=rxDoc;						// Replace it with just the document
						}
						rogxml_WriteFile(xml, msg_LogFile("got.xml"));
					}
				}

				if (!rogxml_ErrorNo(xml)) {
					m=msg_New(xml);
				} else {
					rogxml_Delete(xml);
					break;
				}
				if (!m) break;								// Failed to read any decent XML
			} else {
				msg_attachment *a;
				char *pMessageCopy = NEW(char, nMessageLength);
				const char *contentType = mime_GetHeader(sub, "Content-Type");
				const char *contentId = mime_GetHeader(sub, "Content-ID");
				const char *filename = mime_GetParameter(sub, "Content-Disposition", "filename");

				memcpy(pMessageCopy, szMessage, nMessageLength);

				a=msg_NewAttachment(filename, contentType, contentId, nMessageLength, pMessageCopy);

				msg_AppendAttachment(m, a);
			}
			szDelete(szMessageStart);					// Finished with the copy
		}
	} else {												// Plain, boring message
		const char *szMessage = mime_GetBodyText(mime);
		int nLen = mime_GetBodyLength(mime);
		rogxml *xml;							// =rogxml_FromText(szMessage);
		char *szCopy = NEW(char, nLen+1);

		memcpy(szCopy, szMessage, nLen);
		szCopy[nLen]='\0';

		if (*szCopy) {							// We have some content
			xml=rogxml_FromText(szCopy);
			while (rogxml_IsInstruction(xml) || rogxml_IsComment(xml)) {
				const char *szNext = SkipSpaces(rogxml_GetNextText());		// SS to go over \n, which messes the log!
				Log("Skipping %s (next is '%.20s')", rogxml_IsInstruction(xml) ? "instruction" : "comment", szNext);
				xml = rogxml_FromText(szNext);
			}
//			xml=rogxml_ReadFile(szCopy);
			if (xml) {
				if (rogxml_ErrorNo(xml)) {
					Log("XML interpretation error %d: %s", rogxml_ErrorNo(xml), rogxml_ErrorText(xml));
					FILE *fp=fopen("/tmp/badxml.xml", "w");
					if (fp) {
						fwrite(szCopy, 1, nLen, fp);
						fclose(fp);
						Log("Written 'XML' to /tmp/badxml.xml");
					}
				}
				rogxml *rxDoc = rogxml_FindDocumentElement(xml);
				if (rxDoc != xml) {					// Need to drop any <?xml...?>
					rogxml_Unlink(rxDoc);			// Take off document element
					rogxml_Delete(xml);				// Delete the rest
					xml=rxDoc;						// Replace it with just the document
				}
			} else {
				Log("Content, but not interprettable as XML");
			}
		} else {
			Log("No content");
			xml=NULL;
		}
		if (!xml || !rogxml_ErrorNo(xml)) {			// No XML or it's legit
			m=msg_New(xml);
		} else {									// Duff XML in which case we don't have a decent message
			rogxml_Delete(xml);
		}
	}

	if (m) {
		msg_Save(m, msg_LogFile("1.msg"));
		msg_AscertainWrapping(m);						// It's wrapped...
		msg_Save(m, msg_LogFile("2.msg"));
	}

	return m;
}

void msg_SetXML(MSG *m, rogxml *rx)
// Set the XML, deleting any that came before it.
// Pass i NULL for rx to remove it completely.
{
	if (m->xml != rx) {
		rogxml_Delete(m->xml);
		m->xml=rx;
	}
}

rogxml *msg_GetXML(MSG *m)
// Fetch a pointer to the XML, leaving it in situ
{
	return m->xml;
}

rogxml *msg_ReleaseXML(MSG *m)
// Release the XML from the message (bit like rogxml_Unlink())
{
	rogxml *rx=m->xml;
	m->xml = NULL;

	return rx;
}

int msg_IsWrapped(MSG *m)
{
	return m->nWrapped != WRAP_NONE && m->nWrapped != WRAP_UNWRAPPED;
}

int msg_IsItk(MSG *m)
{
	if (m->nWrapped == WRAP_ITK || m->nWrapper == WRAP_ITK) return 1;			// All wrapped and known

	return 0;
}

int msg_IsNasp(MSG *m)
{
	if (m->nWrapped == WRAP_NASP) return 1;			// All wrapped and known

	msg_GetContract(m);							// Find the contract
	if (m->contract) return contract_IsNasp(m->contract);	// Get it from there if we found one

	return 0;									// Give up and claim we know nothing (which is true)
}

int msg_IsEbXml(MSG *m)
{
	if (m->nWrapped == WRAP_EBXML) return 1;			// All wrapped and known

	msg_GetContract(m);							// Find the contract
	if (m->contract) return contract_IsEbXml(m->contract);	// Get it from there if we found one

	return 0;									// Give up and claim we know nothing (which is true)
}

int msg_AckRequested(MSG *m)					{ return m->bAckRequested; }
int msg_HasPayload(MSG *m)						{ return m->bHasPayload; }
int msg_IsAck(MSG *m)							{ return m->bIsAck; }
int msg_IsLocalSync(MSG *m)						{ return m->bIsLocalSync; }
int msg_GetPid(MSG *m)							{ return m->nPid; }

int msg_GetWrapper(MSG *m)						{ return m->nWrapper; }
int msg_GetWrapped(MSG *m)						{ return m->nWrapped; }
int msg_GetLevel(MSG *m)						{ return m->nLevel; }

const char *msg_GetInteractionId(MSG *m)		{ return m->szInteractionId; }

const char *msg_GetConversationId(MSG *m, const char *def)
// Get the conversation ID from the message.  If none has currently been set then returns the
// one passed in or a random one if none (NULL) is passed in.
{
	if (!m->szConversationId) {
		m->szConversationId = strdup(def ? def : guid_ToText(NULL));		// Assign one if we don't have one
	}

	return m->szConversationId;
}

const char *msg_GetContentId(MSG *m, const char *def)
// Get the conversation ID from the message.  If none has currently been set then returns the
// one passed in or a random one if none (NULL) is passed in.
{
	if (!m->szContentId) {
		m->szContentId = strdup(def ? def : guid_ToText(NULL));		// Assign one if we don't have one
	}

	return m->szContentId;
}

const char *msg_GetDescription(MSG *m)
// Get the description from the message.  If none has currently been set then returns NULL
{
	return m->szDescription;
}

const char *msg_GetReferenceId(MSG *m)
// Get the reference id from the message.  If none has currently been set then returns NULL
{
	return m->szReferenceId;
}

const char *msg_GetMessageId(MSG *m)
{
	return m->szMessageId;
}

const char *msg_GetWrapperId(MSG *m)
{
	return m->szWrapperId;
}

const char *msg_GetInternalId(MSG *m)			{ return m->szInternalId; }
const char *msg_GetURI(MSG *m)					{ return m->szURI; }
const char *msg_GetRefToMessageId(MSG *m)		{ return m->szRefToMessageId; }
const char *msg_GetToPartyId(MSG *m)			{ return m->szToPartyId; }
const char *msg_GetFromPartyId(MSG *m)			{ return m->szFromPartyId; }
const char *msg_GetService(MSG *m)				{ return m->szService; }
const char *msg_GetToAsid(MSG *m)				{ return m->szToAsid; }
const char *msg_GetFromAsid(MSG *m)				{ return m->szFromAsid; }
const char *msg_GetFirstSend(MSG *m)			{ return m->szFirstSend; }
const char *msg_GetNextSend(MSG *m)				{ return m->szNextSend; }
const char *msg_GetLastSend(MSG *m)				{ return m->szLastSend; }
const char *msg_GetRetryInterval(MSG *m)		{ return m->szRetryInterval; }

int msg_GetSends(MSG *m)						{ return m->nSends; }
int msg_GetMaxRetries(MSG *m)					{ return m->nMaxRetries; }

void msg_SetLocalSync(MSG *m, int bSetting)		{ m->bIsLocalSync=bSetting; }

void msg_SetWrapped(MSG *m, int nWrapper)		{ m->nWrapper = m->nWrapped = nWrapper; }
void msg_SetWrapper(MSG *m, int nWrapper)		{ m->nWrapper = nWrapper; }
void msg_SetHasPayload(MSG *m, int bSetting)	{ m->bHasPayload = bSetting; }
void msg_SetURI(MSG *m, const char *szURI)		{ strset(&m->szURI, szURI); }

contract_t *msg_GetContract(MSG *m)
{
	if (!m->contract) {
		const char *szType = m->szInteractionId;
		const char *szService = m->szService;
		const char *szTo = m->szToPartyId;

		if (szType) m->contract = contract_New(szTo, szType, szService);

		if (m->contract) {							// Need to pick a couple things out of the contract...
			const char *szRetryInterval = contract_GetRetryInterval(m->contract);
			int nRetries = contract_GetRetries(m->contract);
			const char *szLevel = contract_GetLevel(m->contract);

			strset(&m->szRetryInterval, szRetryInterval);
			m->nMaxRetries=nRetries;

			if (szLevel) {
				if (!strcasecmp(szLevel, "P1R1")) m->nLevel = LEVEL_P1R1;
				else if (!strcasecmp(szLevel, "P1R2")) m->nLevel = LEVEL_P1R2;
			}
		}
	}

	return m->contract;
}

void msg_SetEbxmlAttrs(MSG *m, rogxml *rxHeader, rogxml *rxContent)
{
	// I feel these paths should start /MessageHeader/ but that doesn't seem to work...
	msg_SetAttr(m, "ebxml-From", rogxml_GetValueByPath(rxHeader, "//From/PartyId"));
	msg_SetAttr(m, "ebxml-To", rogxml_GetValueByPath(rxHeader, "//To/PartyId"));
	msg_SetAttr(m, "ebxml-CPAId", rogxml_GetValueByPath(rxHeader, "//CPAId"));
	msg_SetAttr(m, "ebxml-ConversationId", rogxml_GetValueByPath(rxHeader, "//ConversationId"));
	msg_SetAttr(m, "ebxml-Service", rogxml_GetValueByPath(rxHeader, "//Service"));
	msg_SetAttr(m, "ebxml-Action", rogxml_GetValueByPath(rxHeader, "//Action"));
	msg_SetAttr(m, "ebxml-MessageData-MessageId", rogxml_GetValueByPath(rxHeader, "//MessageData/MessageId"));
	msg_SetAttr(m, "ebxml-MessageData-Timestamp", rogxml_GetValueByPath(rxHeader, "//MessageData/Timestamp"));

	rogxpath *rxp=rogxpath_New(rxContent, "//Reference");
	int count=rogxpath_GetCount(rxp);
	int i;

	for (i=1;i<=count;i++) {
		char name[100];
		rogxml *rx=rogxpath_GetElement(rxp, i);

		snprintf(name, sizeof(name), "ebxml-manifest%d-href", i);
		msg_SetAttr(m, name, rogxml_GetValueByPath(rx, "//@href"));
		snprintf(name, sizeof(name), "ebxml-manifest%d-description", i);
		msg_SetAttr(m, name, rogxml_GetValueByPath(rx, "//Description"));
		snprintf(name, sizeof(name), "ebxml-manifest%d-schema-location", i);
		msg_SetAttr(m, name, rogxml_GetValueByPath(rx, "//Schema/@location"));
		snprintf(name, sizeof(name), "ebxml-manifest%d-payload-style", i);
		msg_SetAttr(m, name, rogxml_GetValueByPath(rx, "//Payload/@style"));
		snprintf(name, sizeof(name), "ebxml-manifest%d-payload-encoding", i);
		msg_SetAttr(m, name, rogxml_GetValueByPath(rx, "//Payload/@encoding"));
		snprintf(name, sizeof(name), "ebxml-manifest%d-payload-version", i);
		msg_SetAttr(m, name, rogxml_GetValueByPath(rx, "//Payload/@version"));
	}

	rogxpath_Delete(rxp);

}

int msg_Unwrap(MSG *m)
// Removes any SOAP, NASP, ebXML wrapper from the message, hopefully leaving some plain HL7 in pole position
// and setting any other flags in the message that the wrapper provided.
{
	rogxml *rxMsg=m->xml;
	const char *szURI = NULL;
	const char *szHeaderURI = NULL;					// URI of content of SOAP header (ebxml or HL7)
	const char *szSoapEnvelope = NULL;				// Should always be 'Envelope'

	if (!rxMsg) { return 0; }						// Nothing to unwrap...?
	if (msg_GetWrapped(m) == WRAP_UNWRAPPED) return 0;		// Don't unwrap if it isn't wrapped...

	if (rogxml_ErrorNo(rxMsg)) {
		msg_SetError(m, 1, rogxml_ErrorText(rxMsg));
		return 0;
	}

	// Dispose of any pesky <?...?> bit at the top
	if (rxMsg) {
		rogxml *rxDoc = rogxml_FindDocumentElement(rxMsg);
		if (rxDoc != rxMsg) {			// Need to drop any <?xml...?>
			rogxml_Unlink(rxDoc);		// Take off document element
			rogxml_Delete(rxMsg);		// Delete the rest
			rxMsg=rxDoc;				// Replace it with just the document
		}
	}

	szURI=rogxml_GetURI(rxMsg);						// The URI of the main message ("http://...soap.../")
	szSoapEnvelope=rogxml_GetLocalName(rxMsg);		// The element name ("Envelope")

//Log( "szURI(%s)=rogxml_GetURI(%s)", szURI,rxMsg->nsName );
//Log( "szSoapEnvelope(%s)=rogxml_GetLocalName(%s)", szSoapEnvelope,rxMsg->szName );

	if (szURI && szSoapEnvelope &&
			!strcmp(szURI, "http://schemas.xmlsoap.org/soap/envelope/") &&		// In SOAP namespace
			!strcmp(szSoapEnvelope, "Envelope")) {								// And it's ':Envelope'
		// We have a soap message so under here will be a Header and a Body in that order
		rogxml *rxHeader = rogxml_FindChild(rxMsg, "Header");
		rogxml *rxBody = rogxml_FindChild(rxMsg, "Body");

		rogxml *rxHeaderContent = rogxml_FindFirstChild(rxHeader);			// Manifest or HL7
		rogxml *rxBodyContent = rogxml_FindFirstChild(rxBody);				// Manifest, HL7, Fault...

		const char *szBodyURI=rogxml_GetURI(rxBodyContent);
		const char *szBodyContent=rogxml_GetLocalName(rxBodyContent);
		msg_SetAttr(m, "soap-body-uri", szBodyURI);

//Log("Body URI = '%s'", szBodyURI);
//Log("Body content = '%s'", szBodyContent);
		if (rxHeader) {
			szHeaderURI=rogxml_GetURI(rxHeaderContent);
			// If there is no namespace on the element then we'll treat it as "hl7"
			if(!szHeaderURI || !*szHeaderURI) szHeaderURI = "hl7";

			msg_SetAttr(m, "soap-header-uri", szHeaderURI);
//Log("Header URI = '%s'", szHeaderURI);

			if (!strcmp(szHeaderURI, "hl7") || !strcmp(szHeaderURI, "urn:hl7-org:v3")) {	// Directly embedded HL7
				m->nWrapper=WRAP_NASP;													// We're Nasp
				m->nWrapped=WRAP_NASP;
				m->nLevel=LEVEL_P1R1;
				msg_ExtractMessageInfo(m);

				rogxml_LocaliseNamespaces(rxBodyContent);						// Bring up any necessary namespaces
				rogxml_Unlink(rxBodyContent);									// Pull the content out
				msg_SetXML(m, rxBodyContent);									// Replace the original with it

				m->nWrapped=WRAP_UNWRAPPED;
			} else if (!strcmp(szHeaderURI, "http://schemas.xmlsoap.org/ws/2004/08/addressing")) {	// Nasp2
				m->nWrapper=WRAP_NASP;
				m->nWrapped=WRAP_NASP;
				m->nLevel=LEVEL_P1R2;
				msg_ExtractMessageInfo(m);

				rogxml_LocaliseNamespaces(rxBodyContent);						// Bring up any necessary namespaces
				rogxml_Unlink(rxBodyContent);									// Pull the content out
				msg_SetXML(m, rxBodyContent);									// Replace the original with it

				m->nWrapped=WRAP_UNWRAPPED;
			} else if (!strcmp(szHeaderURI, "http://www.w3.org/2005/08/addressing")) {				// ITK?
				m->nWrapper=WRAP_ITK;
				m->nWrapped=WRAP_ITK;
				msg_ExtractMessageInfo(m);

				rogxml_LocaliseNamespaces(rxBodyContent);						// Bring up any necessary namespaces
				rogxml_Unlink(rxBodyContent);									// Pull the content out
				msg_SetXML(m, rxBodyContent);									// Replace the original with it

				m->nWrapped=WRAP_UNWRAPPED;
			} else if (!strcmp(szHeaderURI, "http://www.oasis-open.org/committees/ebxml-msg/schema/msg-header-2_0.xsd") &&
							!strcmp(szBodyContent, "Manifest")) {					// ebXML Manifest
				// All we want to do is copy the first attachment as the XML
				// after extracting anything useful from the header
				msg_attachment *a;
				rogxml *rxReference = rogxml_FindFirstChild(rxBodyContent);
				int nReferences = 0;

				msg_SetEbxmlAttrs(m, rxHeaderContent, rxBodyContent);
				// xmlns:xlink="http://www.w3.org/1999/xlink"
				// xmlns:eb="http://www.oasis-open.org/committees/ebxml-msg/schema/msg-header-2_0.xsd"
				while (rxReference) {
					const char *szHref = rogxml_GetAttr(rxReference, "href");	// xlink:href

					nReferences++;

					if (szHref) {
						if (!strncasecmp(szHref, "cid:", 4)) {
							const char *szContentId = hprintf(NULL, "<%s>", szHref+4);
							msg_attachment *a=msg_GetAttachmentByContentId(m, szContentId);

							if (!a) {
								msg_SetError(m, 6, "Manifest refers to '%s' but there is no corresponding attachment", szHref);
								return 0;
							}
							rogxml *rxDescription = rogxml_FindChild(rxReference, "Description");
							if (rxDescription) {
								const char *szDescription = rogxml_GetValue(rxDescription);

								msg_SetAttachmentDescription(a, szDescription);
							}
						} else {
							msg_SetError(m, 7, "Manifest refers to external entity (%s)", szHref);
							return 0;
						}
					} else {
					}
					rxReference = rogxml_FindNextSibling(rxReference);
				}
				m->nWrapper=WRAP_EBXML;													// We're ebXML
				m->nWrapped=WRAP_EBXML;
				msg_ExtractMessageInfo(m);

				a=msg_GetAttachment(m, 1);
				if (a) {
					const char *szAttach = msg_GetAttachmentText(a);
					int nLen = msg_GetAttachmentLength(a);
					rogxml *rxAttach;
					char *szAttachCopy = NEW(char, nLen+1);

					memcpy(szAttachCopy, szAttach, nLen);	// Make a \0 terminated copy
					szAttachCopy[nLen]='\0';

					rxAttach = rogxml_ReadFile(szAttachCopy);
					if (rxAttach) {
						rogxml *rxDoc = rogxml_FindDocumentElement(rxAttach);
						if (rxDoc != rxAttach) {			// Need to drop any <?xml...?>
							rogxml_Unlink(rxDoc);			// Take off document element
							rogxml_Delete(rxAttach);		// Delete the rest
							rxAttach=rxDoc;					// Replace it with just the document
						}
					}
					msg_DeleteMessageAttachment(m, 1);							// 'a' is extinct now
					msg_SetXML(m, rxAttach);

					szDelete(szAttachCopy);
				} else {
					msg_SetError(m, 2, "ebXML Manifest but no 2nd body part");
					return 0;
				}

				m->nWrapped=WRAP_UNWRAPPED;
			} else if (!strcmp(szHeaderURI, "http://www.oasis-open.org/committees/ebxml-msg/schema/msg-header-2_0.xsd")) {
																				// ebXML with no manifest...
				rogxml *rxAck;
				const char *szType = rogxml_GetValueByPath(rxMsg, "/Envelope/Header/MessageHeader/Action");

				msg_SetEbxmlAttrs(m, rxHeaderContent, rxBodyContent);
				if (szType && (!strcasecmp(szType, "Acknowledgement") || !strcasecmp(szType, "Acknowledgment"))) {
					m->nWrapper=WRAP_EBXML;										// We're ebXML
					msg_ExtractMessageInfo(m);									// Extract the wrapper info
					m->nWrapper=WRAP_FAULT;										// We're an Ack really!
					rxAck = rogxml_FindByPath(rxMsg, "/Envelope/Header/Acknowledgement");
					if (!rxAck) rxAck = rogxml_FindByPath(rxMsg, "/Envelope/Header/Acknowledgment");
					rogxml_LocaliseNamespaces(rxAck);							// Bring up any necessary namespaces
					rogxml_Unlink(rxAck);										// Pull the content out
					msg_SetXML(m, rxAck);										// Replace the original with it
					m->nWrapped=WRAP_UNWRAPPED;									// We're an Ack really!
				}
			} else {		// FALLEN THROUGH - we shouldn't be seeing a message like this!
				Log("ERROR - UNEXPECTED MESSAGE NAMESPACE (%s)", szHeaderURI);
				msg_SetError(m, 3, "Unrecognised namespace '%s' in message", szHeaderURI);
				return 0;
			}
		} else if (!strcmp(szBodyContent, "Fault")) {						// A SOAP Fault
			m->nWrapped=WRAP_FAULT;											// Say what it is
			m->nWrapper=WRAP_FAULT;											// We'll leave it wrapped
		} else {															// Something alien
			Log("we have fallen into the block called 'something alien', check out file {/tmp/duff.msg}");
			msg_Save(m, "/tmp/duff.msg");
			msg_SetError(m, 4, "SOAP header is neither HL7 nor ebXML Manifest");
			return 0;
		}
	} else {
		Log("ERROR - UNEXPECTED MESSAGE STRUCTURE");
		Log("Message URI: %s", szURI ? szURI : "{none}");
		Log("Root element: %s", szSoapEnvelope ? szSoapEnvelope : "{none}");
		msg_SetError(m, 5, "Unrecognised message structure");
		return 0;
	}

	return 1;
}

////// Next section deals with reading/writing msg structures from a file
//////
STATIC int GetHeader(FILE *fp, const char *szNameCheck, char **pszName, char **pszType, int *pnNum)
// Gets a header line from the file and returns 1 if it is ok, otherwise 0.
// *pnNum, *pszName and *pszType may be written to so they MUST contain valid pointers
{
	static char buf[80];
	char *chp;

	chp=fgets(buf, sizeof(buf), fp);

	if (!chp) {
		*pszName="";
		*pszType="";
		*pnNum=0;
		return 0;
	}
	*pszName=chp;
	chp=strchr(chp, ':');
	if (chp) {
		*chp='\0';
		*pszType=++chp;
		chp=strchr(chp, ':');
		if (chp) {
			*chp='\0';
			*pnNum=atoi(chp+1);
			if (szNameCheck && strcmp(szNameCheck, *pszName)) {
				fprintf(stderr, "ERROR: Attempted to read '%s', got '%s'\n", szNameCheck, *pszName);
				return 0;
			}
			return 1;
		}
	}

	return 0;
}

STATIC void PutHeader(FILE *fp, const char *szName, const char *szType, int n)
{
	fprintf(fp, "%s:%s:%d\n", szName, szType, n);
}

STATIC int GetInt(FILE *fp)
{
	char *szName;
	char *szType;
	int n;

	if (GetHeader(fp, NULL, &szName, &szType, &n)) {
		return n;
	} else {
		return -1;
	}
}

STATIC const char *GetString(FILE *fp)
{
	char *szName;
	char *szType;
	int nLen;
	char *szText;
	size_t itemsRead;

	if (GetHeader(fp, NULL, &szName, &szType, &nLen)) {
		if (nLen >= 0) {
			szText=malloc(nLen+1);
			itemsRead = fread(szText, 1, nLen, fp);
			szText[itemsRead]='\0';
			if( itemsRead == nLen )
				getc(fp);						// Get the trailing '\n'
		} else {
			szText=NULL;
		}
	}

	return szText;
}

STATIC char *GetData(FILE *fp, const char *szNameCheck, int *pnLen)
{
	char *szName;
	char *szType;
	int nLen;
	char *data;
	size_t itemsRead;

	if (GetHeader(fp, szNameCheck, &szName, &szType, &nLen)) {
		if (nLen >= 0) {
			data=malloc(nLen+1);
			itemsRead = fread(data, 1, nLen, fp);
			data[itemsRead]='\0';
			if( itemsRead == nLen )
				getc(fp);						// Get the trailing '\n'
			*pnLen=itemsRead;
		} else {
			data=NULL;
			*pnLen=0;
		}
	}

	return data;
}

STATIC SSMAP *GetMap(FILE *fp)
{
	SSMAP *map = ssmap_New();
	char *szName;
	char *szType;
	int nLen;
	char *data;
	size_t itemsRead;


	if (GetHeader(fp, NULL, &szName, &szType, &nLen)) {
		if (nLen >= 0) {
			data=malloc(nLen+1);
			itemsRead = fread(data, 1, nLen, fp);
			data[itemsRead]='\0';
			char *d=data;
			while (*d) {
				char *nl=strchr(d, '\n');
				if (nl) *nl='\0';
				char *eq=strchr(d, '=');
				if (eq) {
					*eq = '\0';
					ssmap_Add(map, d, eq+1);
				}
				d = nl ? nl+1 : "";
			}
		}
	}

	return map;
}

STATIC msg_attachment *GetAttach(FILE *fp)
{
	msg_attachment *a;
	const char *szName;
	const char *szContentType;
	const char *szContentId;
	const char *szDescription;
	const char *szReferenceId;
	char *data;
	int nLen;

	szName=GetString(fp);
	szContentType=GetString(fp);
	szContentId=GetString(fp);
	szDescription=GetString(fp);
	szReferenceId=GetString(fp);
	data=GetData(fp, "AttachmentContent", &nLen);

	a=msg_NewAttachment(szName, szContentType, szContentId, nLen, data);
	if (szDescription) msg_SetAttachmentDescription(a, szDescription);
	if (szReferenceId) msg_SetAttachmentReferenceId(a, szReferenceId);
	szDelete(szName);
	szDelete(szContentType);
	szDelete(szContentId);
	szDelete(szDescription);
	szDelete(szReferenceId);

	return a;
}

STATIC void PutInt(FILE *fp, const char *szName, int n)
{
	PutHeader(fp, szName, "N", n);
}

STATIC void PutString(FILE *fp, const char *szName, const char *szText)
{
	if (szText) {
		PutHeader(fp, szName, "S", strlen(szText));
		fputs(szText, fp);
		putc('\n', fp);
	} else {
		PutHeader(fp, szName, "S", -1);
	}
}

STATIC void PutData(FILE *fp, const char *szName, int nLen, const char *szData)
{
	if (szData) {
		PutHeader(fp, szName, "D", nLen);
		fwrite(szData, 1, nLen, fp);
		putc('\n', fp);
	} else {
		PutHeader(fp, szName, "D", -1);
	}
}

STATIC void PutMap(FILE *fp, const char *szName, SSMAP *map)
{
	if (map) {
		const char *szMap = strdup("");

		ssmap_Reset(map);
		const char *szVar;
		const char *szValue;
		while (ssmap_GetNextEntry(map, &szVar, &szValue)) {
			szMap = hprintf(szMap, "%s=%s\n", szVar, szValue);
		}

		PutHeader(fp, szName, "M", strlen(szMap));
		fputs(szMap, fp);
		szDelete(szMap);
	}
}

STATIC void PutAttach(FILE *fp, msg_attachment *a)
{
	PutString(fp, "AttachName", a->szName);
	PutString(fp, "AttachContentType", a->szContentType);
	PutString(fp, "AttachContentId", a->szContentId);
	PutString(fp, "AttachDescription", a->szDescription);
	PutString(fp, "AttachReferenceId", a->szReferenceId);
	PutData(fp, "AttachmentContent", a->nLen, a->data);
}

int msg_Save(MSG *m, const char *szFilename)
{
//Log("msg_Save(%x, \"%s\")", m, szFilename);
	const char *szXML = NULL;
	int i;
	FILE *fp=fopen(szFilename, "w");

	if (!fp) return 1;

	m->nPid=getpid();										// Update controlling PID here

	fprintf(fp, "MMTS0001\n");

	if (m->xml) szXML = rogxml_ToText(m->xml);				// NULL if no XML
	PutString(fp, "XML", szXML);
	szDelete(szXML);

	PutInt(fp, "ErrorNo", m->nError);
	PutString(fp, "ErrorStr", m->szError);

	PutInt(fp, "HttpCode", m->nHttpCode);
	PutString(fp, "HttpCodeText", m->szHttpCodeText);

	PutString(fp, "InteractionId", m->szInteractionId);
	PutString(fp, "ConversationId", m->szConversationId);
	PutString(fp, "ContentId", m->szContentId);
	PutString(fp, "Description", m->szDescription);
	PutString(fp, "ReferenceId", m->szReferenceId);
	PutString(fp, "MessageId", m->szMessageId);
	PutString(fp, "WrapperId", m->szWrapperId);
	PutString(fp, "InternalId", m->szInternalId);
	PutString(fp, "RefToMessageId", m->szRefToMessageId);
	PutString(fp, "ToPartyId", m->szToPartyId);
	PutString(fp, "FromPartyId", m->szFromPartyId);
	PutString(fp, "Service", m->szService);
	PutString(fp, "ToAsid", m->szToAsid);
	PutString(fp, "FromAsid", m->szFromAsid);

	PutMap(fp, "attrs", m->attrs);

	PutInt(fp, "Wrapper", m->nWrapper);
	PutInt(fp, "Wrapped", m->nWrapped);
	PutInt(fp, "Level", m->nLevel);
	PutInt(fp, "AckRequested", m->bAckRequested);
	PutInt(fp, "HasPayload", m->bHasPayload);
	PutInt(fp, "IsAck", m->bIsAck);
	PutInt(fp, "IsLocalSync", m->bIsLocalSync);

	PutString(fp, "FirstSend", m->szFirstSend);
	PutString(fp, "LastSend", m->szLastSend);
	PutString(fp, "NextSend", m->szNextSend);
	PutInt(fp, "Sends", m->nSends);
	PutInt(fp, "MaxSends", m->nMaxRetries);
	PutString(fp, "RetryInterval", m->szRetryInterval);

	PutInt(fp, "Pid", m->nPid);

	PutInt(fp, "AttachCount", m->nAttach);
	for (i=0;i<m->nAttach;i++) {
		PutAttach(fp, m->attachment[i]);
	}

	fclose(fp);

	return 0;
}

MSG *msg_Load(const char *szFilename)
{
	MSG *m;
	const char *szXML;
	int nAttach;
	int i;
	FILE *fp=fopen(szFilename, "r");
	char buf[50];
	rogxml *rx;

	if (!fp) return NULL;

	fgets(buf, sizeof(buf), fp);
	if (strncmp(buf, "MMTS0001", 8)) {
		fclose(fp);
		return NULL;			// Incorrect signature
	}

	m=msg_New(NULL);
	szXML=GetString(fp);
	if (szXML) {
		rx=rogxml_FromText(szXML);
		if (rogxml_ErrorNo(rx)) {
			rogxml_WriteFile(rx, msg_LogFile("duffmsg.xml"));
			msg_Log("duffmsg.txt", "%s", szXML);
		}
		msg_SetXML(m, rx);
		szDelete(szXML);
	}

	m->nError=GetInt(fp);
	m->szError=GetString(fp);

	m->nHttpCode=GetInt(fp);
	m->szHttpCodeText=GetString(fp);

	m->szInteractionId = GetString(fp);
	m->szConversationId = GetString(fp);
	m->szContentId = GetString(fp);
	m->szDescription = GetString(fp);
	m->szReferenceId = GetString(fp);
	m->szMessageId = GetString(fp);
	m->szWrapperId = GetString(fp);
	m->szInternalId = GetString(fp);
	m->szRefToMessageId = GetString(fp);
	m->szToPartyId = GetString(fp);
	m->szFromPartyId = GetString(fp);
	m->szService = GetString(fp);
	m->szToAsid = GetString(fp);
	m->szFromAsid = GetString(fp);

	m->attrs = GetMap(fp);

	m->nWrapper = GetInt(fp);
	m->nWrapped = GetInt(fp);
	m->nLevel = GetInt(fp);
	m->bAckRequested = GetInt(fp);
	m->bHasPayload = GetInt(fp);
	m->bIsAck = GetInt(fp);
	m->bIsLocalSync = GetInt(fp);

	m->szFirstSend = GetString(fp);
	m->szLastSend = GetString(fp);
	m->szNextSend = GetString(fp);
	m->nSends = GetInt(fp);
	m->nMaxRetries = GetInt(fp);
	m->szRetryInterval = GetString(fp);

	m->nPid = GetInt(fp);

	nAttach=GetInt(fp);
	for (i=0;i<nAttach;i++) {
		msg_AppendAttachment(m, GetAttach(fp));
	}

	fclose(fp);

	return m;
}

int msg_DecodePeriod(const char *szPeriod, int *pnYears, int *pnMonths, int *pnDays, int *pnHours, int *pnMins, int *pnSecs)
// Decodes a 'PT10S' type string (schema time period) to its component parts
// If any of pnYears etc is NULL then they're simply omitted - the others ARE NOT adjusted to compensate
// Returns		1	Ok
//				0	Error in string
{
	int bInTime = 0;					// 1 when in time section
	int nYears=0, nMonths=0, nDays=0;
	int nHours=0, nMins=0, nSecs=0;
	int bResult = 1;					// Assume OK until proven otherwise

	if (*szPeriod == 'P') {
		szPeriod++;
		while (*szPeriod) {
			int n;

			if (*szPeriod == 'T') {
				bInTime=1;
				szPeriod++;
				continue;
			}
			n=atoi(szPeriod);
			while (isdigit(*szPeriod)) szPeriod++;
			switch (*szPeriod) {
			case 'Y':	nYears=n;	break;
			case 'M':	if (bInTime) nMins = n; else nMonths=n;	break;
			case 'D':	nDays=n;	break;
			case 'H':	nHours=n;	break;
			case 'S':	nSecs=n;	break;
			default:	bResult=0;	break;
			}
			szPeriod++;
		}
	} else {
		bResult = 0;					// Didn't start with 'P'
	}

	if (bResult) {
		if (pnYears) *pnYears=nYears;
		if (pnMonths) *pnMonths=nMonths;
		if (pnDays) *pnDays=nDays;
		if (pnHours) *pnHours=nHours;
		if (pnMins) *pnMins=nMins;
		if (pnSecs) *pnSecs=nSecs;
	} else {
		nYears=nMonths=nDays=nHours=nMins=nSecs=0;
	}

//msg_Log("ap", "1 %d-%d-%d %d:%d:%d\n", nDays, nMonths, nYears, nHours, nMins, nSecs);
//msg_Log("ap", "2 %d-%d-%d %d:%d:%d\n", *pnDays, *pnMonths, *pnYears, *pnHours, *pnMins, *pnSecs);

	return bResult;
}

time_t msg_PeriodToSeconds(const char *szPeriod)
{
	int nYears=0, nMonths=0, nDays=0;
	int nHours=0, nMins=0, nSecs=0;

	msg_DecodePeriod(szPeriod, &nYears, &nMonths, &nDays, &nHours, &nMins, &nSecs);

	nDays+=nMonths*12/365;				// Coalesce months and years into days
	nDays+=nYears*365;

	nSecs+=nMins*60;					// Coalesce minutes and hours into seconds
	nSecs+=nHours*3600;

	nSecs+=nDays*3600*24;				// Add in the days

	return (time_t)nSecs;
}

const char *msg_AddPeriod(const char *szTime, const char *szPeriod)
// Returns a pointer to a static string that represents the time given with the
// period added
{
	static char buf[20];
	int tye, tmo, tda, tho, tmi, tse;						// Time split into parts
	int pye, pmo, pda, pho, pmi, pse;						// Period split into parts
	static int mlen[]={0,31,28,31,30,31,30,31,31,30,31,30,31};

	sscanf(szTime, "%4d-%2d-%2dT%2d:%2d:%2d", &tye, &tmo, &tda, &tho, &tmi, &tse);
	pye=pmo=pda=pho=pmi=pse=0;
	msg_DecodePeriod(szPeriod, &pye, &pmo, &pda, &pho, &pmi, &pse);

	tse+=pse; tmi+=pmi; tho+=pho;							// Do raw adjustment of time
	tda+=pda; tmo+=pmo; tye+=pye;
	if (tse>59) {tmi+=tse/60; tse%=60;}						// Adjust for overflowing seconds
	if (tmi>59) {tho+=tmi/60; tmi%=60;}						// ... minutes
	if (tho>23) {tda+=tho/24; tho%=24;}						// ... hours
	if (tmo>12) {tye+=(tmo-1)/12; tmo=(tmo-1)%12+1;}		// ... months (days has to be done afterwards)
	for (;;) {												// ... days
		int dim=mlen[tmo];									// Get month length
		if (tmo==2 && tye%4 == 0) dim=29;
		if (tda<=dim) break;
		tmo++;
		if (tmo>12) {tmo-=12; tye++;}
		tda-=dim;
	}

	sprintf(buf, "%04d-%02d-%02dT%02d:%02d:%02d", tye, tmo, tda, tho, tmi, tse);

	return buf;
}

const char *msg_Now()
{
	time_t now=time(NULL);
	struct tm *tm=gmtime(&now);
	static char szNow[40];

	sprintf(szNow, "%04d-%02d-%02dT%02d:%02d:%02d",
			tm->tm_year+1900, tm->tm_mon+1, tm->tm_mday,
			tm->tm_hour, tm->tm_min, tm->tm_sec);

	return szNow;
}

int msg_FindMethod(MSG *m)
// Find the transmission method (ebXML/Nasp) and level (P1R1, P1R2 etc)
// Returns	1	Everything went ok, message is now marked up with Wrapping and Level
//			0	Something went awry, message has embedded error
{
	const char *szType = NULL;				// Either "ebXML" or "Nasp" (case may differ)
	const char *szLevel = "UNSET";			// "P1R1", "P1R2" etc.
	const char *szMethods;					// As read from file ("ebXML:P1R2")
	const char *szMatch;					// The match made in 'methods' (IntID-ToParty or just IntID)
	const char *szMethodFile;				// The filename of the method file

	contract_t *contract = msg_GetContract(m);

	szMethodFile = hprintf(NULL, "%s/methods", GetEtcDir());
	szMatch = hprintf(NULL, "%s-%s", m->szInteractionId, m->szToPartyId);
	szMethods = config_FileGetString(szMethodFile, szMatch);
	if (!szMethods) {
		szDelete(szMatch);
		szMatch = strdup(m->szInteractionId);
		szMethods = config_FileGetString(szMethodFile, szMatch);
	}
	// Don't go deleting 'szMatch' here - it's used further down
	// szMethods is used implicitly by szType, szLevel (they might point into it)
	// Also, note that it will be chopped up by strtok below so don't try using it as a whole afterwards

	if (szMethods) {
		char *szType = strtok((char*)szMethods, ":");
		szLevel = strtok(NULL, ":");

		if (!szType) {
			msg_SetError(m, 101, "Bad type entry in methods file for %s", szMatch);
		}
		if (!szLevel) szLevel = "P1R1";
	} else {										// Nothing in 'methods' file - try the contract
		if (contract) {
			szLevel = contract_GetLevel(contract);

			if (contract_IsEbXml(m->contract)) {	// The contract knows
				szType = "ebXML";
			} else {
				szType = "Nasp";
			}
			if (!szLevel) szLevel = "P1R1";
		} else {									// No entry in methods, no contract...  Something's amiss
			msg_SetError(m, 102, "Can't find contract to send '%s' to '%s'", m->szInteractionId, m->szToPartyId);
		}
	}

	if (m->nError == 0) {
		if (!strcasecmp(szType, "ebxml")) m->nWrapper = WRAP_EBXML;
		else if (!strcasecmp(szType, "nasp")) m->nWrapper = WRAP_NASP;
		else msg_SetError(m, 103, "Unknown transmission method '%s'", szType);
	}

	if (m->nError == 0) {
		if (!strcasecmp(szLevel, "P1R1")) m->nLevel = LEVEL_P1R1;
		else if (!strcasecmp(szLevel, "P1R2")) m->nLevel = LEVEL_P1R2;
	}

	szDelete(szMethodFile);
	szDelete(szMatch);
	szDelete(szMethods);		// NB. szType and szLevel are invalid now

	return !m->nError;			// 1=success, 0=failure
}
