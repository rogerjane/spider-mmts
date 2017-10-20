/*! \file rogxml.c rogxml - A library for reading/writing XML.

	rogxml is a C library for creating XML structures, writing them
	to file and reading them in.  Facilities are provided for various
	manipulations of the data.
*/

// 16-08-13 RJ 0.00 a library for creating XML structures, writing them to file and reading them in.
// 16-08-13 RJ 0.01 This has been around for years, just added better handling of errors reading strings in attrs
// 08-07-14 RJ 0.02 30667 Added code to force output to always be UTF-8 compliant
// 09-07-14 RJ 0.03 30667 Modified a little more after I realised it was wrong and referring to http://en.wikipedia.org/wiki/UTF-8

#include <assert.h>
#include <ctype.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>

#ifndef __APPLE__
#include <malloc.h>
#endif

#include "heapstrings.h"
#include "hbuf.h"
#include "mtmacro.h"
#include "rogxml.h"

#define STATIC static
#define API

#if 0
// START HEADER

#include <stdio.h>

#define ROGXML_E_NOERROR	0		// No error at all
#define ROGXML_E_ISNULL		1		// rogxml* passed is NULL
#define ROGXML_E_NOTLT		2		// Expecting a '<'
#define ROGXML_E_NOTGT		3		// Expecting a '>'
#define ROGXML_E_NOTEQ		4		// Expecting a '='
#define ROGXML_E_NONAME		5		// Expecting an element name
#define ROGXML_E_NOVALUE	6		// Expecting an attribute value
#define ROGXML_E_MISNEST	7		// Tags mis-nested
#define ROGXML_E_DUPATTR	8		// Duplicate attribute
#define ROGXML_E_EOF		9		// The file finished early
#define ROGXML_E_OPEN		10		// Trying to read/write from a file twice...
#define ROGXML_E_NOTOPEN	11		// Failed to open file
#define ROGXML_E_NOTPI		12		// Seeing terminating '?' when not in prolog
#define ROGXML_E_NOHYPHEN	13		// Expected '--' after '!' in comment
#define ROGXML_E_NONS		14		// Namespace not found
#define ROGXML_E_DUPNS		15		// Duplicate namespace declaration
#define ROGXML_E_BADAXIS	16		// Unknown axis used in XPath
#define ROGXML_E_BADPATHFN	17		// Unknown path function
#define ROGXML_E_NOTBRA		18		// Expecting a '['
#define ROGXML_E_NOTIMPL	19		// Not implemented

#define ROGXML_F_CDATA		1		// Encloses text in <[CDATA[...]]>
#define ROGXML_F_LITERAL	2		// Output text verbatim not escaped

typedef int readfn_t();

typedef struct rogxml {
	int				nType;			// Element type
	struct rogxml	*nsList;		// List of defined namespaces
	struct rogxml	*nsName;		// Namespace for this element (NULL if none)
	char			*szName;		// Name of this element
	char			*szValue;		// Attribute value or namespace URI
	struct rogxml	*aAttrs;		// First attribute (NULL if none)
	struct rogxml	*rxFirstChild;	// First child element (NULL if childless)
	struct rogxml	*rxNext;		// Next sibling element (NULL if last child)
	struct rogxml	*rxParent;		// Parent element (NULL if root)
	int				fFlags;			// Various flags
} rogxml;

typedef rogxml *rogxmlWalkFn(rogxml *);	// Used in rogxml_WalkPrefix()

typedef struct rogxitem {
	rogxml			*rxElement;		// An element, attribute or namespace
	struct rogxitem	*iNext;			// Next element
	struct rogxitem	*iPrev;			// Previous element
} rogxitem;

typedef struct rogxlist {			// A list of elements
	int				nSize;			// Number of items in the set
	rogxitem		*iFirst;		// List of items
	rogxitem		*iLast;			//
} rogxlist;

typedef struct rogxpath {			// An ordered, unique set of elements
	int				nSize;			// Number of items
	rogxml			**prxElement;	// Array of elements in the list
	rogxlist		*lList;
} rogxpath;

// END HEADER
#endif

// Values less than 0 are errors
#define ROGXML_T_NULL	0				// NULL
#define ROGXML_T_ROOT	1				// The root node
#define ROGXML_T_ELEM	2				// Normal element node
#define ROGXML_T_TEXT	3				// Text node
#define	ROGXML_T_ATTR	4				// Attribute node
#define ROGXML_T_NS		5				// Namespace node
#define ROGXML_T_PI		6				// Prolog (processing instruction) node
#define ROGXML_T_COM	7				// Comment node

static readfn_t *_rogxml_readfn = NULL;	// Used when reading input from a function
static FILE* _rogxml_fpin = NULL;		// Used when reading input from a file
static const char *_rogxml_txin = NULL;	// Used when reading input from a string
static FILE* _rogxml_fpout = NULL;		// For output to a file
static char* _rogxml_txout = NULL;		// For output to an internal buffer
int _count_txout = 0;					// Number of chars in buffer
static FILE* _rogxml_fpdump = NULL; 
static int _rogxml_lineno = 0;			// Line number on input file
static int _rogxml_unread = 0;			// UnRead buffer
static int _rogxml_last_char = 0;		// Real last char read
static int _rogxml_verbose = 1;			// 1 pads with spaces, indent
static int _rogxml_show_comments = 1;	// 1 to include comments on output
static int _rogxml_read_comments = 0;	// 1 to include comments on input
static const char *_rogxml_entity_error = NULL;	// Entity error string
const char *_rogxml_Indent = "  ";		// Indent characters
const char *_rogxml_Linefeed = "\n";	// Linefeed characters

static char g_bStripComments;				// Global signal to rogxml_ReadElement() that comments need stripping

static int _rogxml_bOutputXHTML = 0;		// 1 to output in XHTML mode
static int _rogxml_bBlankCloseTags = 0;		// 1 to show close tags as </>
static int _rogxml_bRetainControls = 0;		// 1 to retain control characters in the middle of text fields

static int isUTF8 = 1;						// 1 to pass thru 8-bit chars, 0 to make them &#xYY;

STATIC const char *SkipSpace(const char *szText)
{
	if (szText)
		while (*szText == ' ' || *szText == '\t')
			szText++;

	return szText;
}

API int rogxml_SetVerbose(int bState)
//! \brief Set verbose mode for file output.
//! Enables or inhibits the generation of leading indentation and white space on output
//! XML.  The rogxml_Dump() function will always put indentation in
//! regardless of this setting.
//! \param bState 0 to disable leading spaces, 1 to enable them
{
	int prev=_rogxml_verbose;

	if (bState != -1) _rogxml_verbose=bState;

	return prev;
}

API int rogxml_SetReadComments(int bState)
//! \brief Sets whether to read comments on input
//! \param bState 0 to disable reading comments, 1 to allow them
{
	int prev = _rogxml_read_comments;

	if (bState != -1) _rogxml_read_comments=bState;

	return prev;
}

API int rogxml_SetUtf8(int bState)
//! \brief Sets whether we're working in UTF8 mode
//! \param bState 0 to disable reading comments, 1 to allow them
{
	int prev = isUTF8;

	if (bState != -1) isUTF8=bState;

	return prev;
}

API const char *rogxml_SetIndentString(const char *szIndent)
{
	const char *prev = _rogxml_Indent;

	_rogxml_Indent = szIndent;

	return prev;
}

API const char *rogxml_SetLinefeedString(const char *szLinefeed)
{
	const char *prev = _rogxml_Linefeed;

	_rogxml_Linefeed = szLinefeed;

	return prev;
}

API int rogxml_RetainControls(int bState)
{
	int prev = _rogxml_bRetainControls;

	if (bState != -1) _rogxml_bRetainControls = bState;

	return prev;
}

STATIC char *SplitName(const char *szName, const char **pszLocal)
//! \brief Splits a name to it's local name and namespace.
//! \param szName - Name to split
//! \param pszLocal - place to put pointer to local name (pointer into szName)
//! \return pointer to namespace name (on heap) or NULL if none
{
	char *szSpace=NULL;
	if (szName) {
		const char *pColon=strchr(szName, ':');

		if (pColon) {
			int len=pColon-szName;
			szSpace=(char*)malloc(len+1);
			memcpy(szSpace, szName, len);
			szSpace[len]='\0';
			szName=pColon+1;
		}

	}
	if (pszLocal) *pszLocal=szName;

	return szSpace;
}

// Predefined namespaces
static rogxml _ns_default[] = {
	{ROGXML_T_NS,NULL,NULL,(char *)"xsi",(char *)"http://www.w3.org/2001/XMLSchema",NULL,NULL,&_ns_default[1],NULL},
	{ROGXML_T_NS,NULL,NULL,(char *)"xsl",(char *)"http://www.w3.org/1999/XSL/Transform",NULL,NULL,&_ns_default[2],NULL},
	{ROGXML_T_NS,NULL,NULL,(char *)"xml",(char *)"http://www.w3.org/XML/1998/namespace",NULL,NULL,NULL,NULL},
};

STATIC rogxml *rogxml_FindNamespace(rogxml *rx, const char *szName)
// Searches for a namespace in this element and its parents
{
	rogxml *ns=NULL;

	if (!rx) {				// Passed a NULL - end of the line
		ns = rogxml_FindSelfOrSibling(_ns_default, szName);
	} else {
		ns=rogxml_FindSelfOrSibling(rx->nsList, szName);
		if (!ns)
			ns = rogxml_FindNamespace(rx->rxParent, szName);
	}

	return ns;
}

API int rogxml_IsElement(rogxml *rx)
{
	return (rx && rx->nType == ROGXML_T_ELEM);
}

API int rogxml_IsText(rogxml *rx)
//! \brief Check if the element is text
//! Text within an element is stored as a text element.  This function
//! is a test whether the element is of this type.
//! \return 1	Element is text
//! \return 0	Element is not text
{
	return (rx && rx->nType == ROGXML_T_TEXT);
}

API int rogxml_IsInstruction(rogxml *rx)
//! \brief Check if the element a processing instruction "<?...?>"
//! A prolog element is not seen if a file is read using rogxml_ReadFile()
//! but may be seen if a file is opened with rogxml_OpenInputFile() then read
//! using rogxml_ReadElement().
//! \return 1	Element is a prolog entry
//! \return 0	Element is not a prolog entry
{
	return (rx && rx->nType == ROGXML_T_PI);
}

API int rogxml_IsRoot(rogxml *rx)
{
	return (rx && rx->nType == ROGXML_T_ROOT);
}

API int rogxml_IsAttr(rogxml *rx)
{
	return (rx && rx->nType == ROGXML_T_ATTR);
}

API int rogxml_IsNamespace(rogxml *rx)
{
	return (rx && rx->nType == ROGXML_T_NS);
}

API int rogxml_IsComment(rogxml *rx)
{
	return (rx && rx->nType == ROGXML_T_COM);
}

API const char *rogxml_GetValue(rogxml *rx)
// Slight almost kludge here in that _GetValue() of an odinary element
// will return the child text if there is any.
{
	if (rx) {
		if (rx->nType == ROGXML_T_ELEM) {		// Normal element
			if (rx->rxFirstChild && rx->rxFirstChild->rxNext == NULL) {	// 1 kid
				rx=rx->rxFirstChild;
				if (rx->nType == ROGXML_T_TEXT)		// That only child is text
					return rx->szValue;				// Return it!
			}
		} else {
			return rx->szValue;
		}
	}

	return NULL;		// rx is NULL or is an element with no child
}

API const char *rogxml_GetValueByPath(rogxml *rx, const char *szPath)
{
	rx = rogxml_FindByPath(rx, szPath);
	return rogxml_GetValue(rx);
}

API const char *rogxml_GetLocalName(rogxml *rx)
// Returns the local name, ignoring any namespace
{
	return rx ? rx->szName : "{NULL}";
}

API const char *rogxml_GetNamespace(rogxml *rx)
// Returns the namespace id that was used when defining the element and
// which would normally be the 'space' in <space:element>...
// Returns	const char *	The namespace ID
//			""				No namespace
{
	return (rx && rx->nsName) ? rogxml_GetLocalName(rx->nsName) : "";
}

API const char *rogxml_GetURI(rogxml *rx)
// Returns the actual namespace of the element.  This will generally be
// some awful URI that is locally referred to using the ID.
// Returns	const char *	The namespace ID
//			""				No namespace
//			NULL			Allocated namespace is temporary
{
	return (rx && rx->nsName) ? rogxml_GetValue(rx->nsName) : "";
}

API const char *rogxml_GetFullName(rogxml *rx)
// Given both a namespace and local name part, returns a nice string
// combining both.
// NB. The return result of this function may be allocated on the heap and
//     destroyed on the next call to this or any of the rogxml_
//     functions that also call it.
//     DO NOT FREE IT YOURSELF!!!!!!
{
	static char *szResult=NULL;	// Returned value
	const char *szNamespace = rogxml_GetNamespace(rx);
	const char *szLocalName = rogxml_GetLocalName(rx);

	if (!*szNamespace) return szLocalName;

	if (szResult) free(szResult);
	szResult=(char*)malloc(strlen(szNamespace)+strlen(szLocalName)+2);
	sprintf(szResult, "%s:%s", szNamespace, szLocalName);

	return szResult;
}

STATIC rogxml *rogxml_New(const char *szName)
// Create a completely empty rogxml record.
// This is the only place one should be allocated
{
	rogxml *rx = (rogxml*)malloc(sizeof(rogxml));
	rx->nType=ROGXML_T_NULL;
	rx->szName=strdup(szName?szName:"");
	rx->szValue=NULL;
	rx->aAttrs=NULL;
	rx->rxFirstChild=NULL;
	rx->rxNext=NULL;
	rx->rxParent=NULL;
	rx->nsName=NULL;
	rx->nsList=NULL;
	rx->fFlags=0;

	return rx;
}

API rogxml *rogxml_NewRoot()
{
	rogxml *rx=rogxml_New("{root}");
	rx->nType=ROGXML_T_ROOT;

	return rx;
}

STATIC rogxml *rogxml_NewNamespace(const char *szName, const char *szURI)
{
	rogxml *rx=rogxml_New(szName);
	rx->nType=ROGXML_T_NS;
	rx->szValue = szURI ? strdup(szURI) : NULL;

	return rx;
}

API rogxml *rogxml_NewElement(rogxml *rxParent, const char *szName)
//! \brief Creates a new element with no children
//! This is generally only used for the root element of a document,
//! all others will generally be added with rogxml_AddChild(),
//! rogxml_AddText() and rogxml_AddTextChild().
//! \param szName	- The name to give the element
//! \return	The resultant element handle
//!	NB. This can be an error element if a namespace lookup fails!
// NULL SAFE - but don't do it.

{
	if (!szName) szName = "{NULL}";

	rogxml *rx;
	rogxml *ns = NULL;
	const char *szLocal;
	char *szSpace=SplitName(szName, &szLocal);
	rx=rogxml_New(szLocal);
	rx->nType=ROGXML_T_ELEM;
	rx->rxParent=rxParent;			// Else we can't find namespaces...

	if (szSpace) {					// Has a namespace
		ns=rogxml_FindNamespace(rxParent, szSpace);	// See if it is defined
//printf("FindNamespace(parent(%s)[%s], '%s') = %s:%s\n",szName,rxParent->szName,szSpace,ns?ns->szName:"NULL",ns?ns->szValue:"NULL");
		if (!ns) {
			ns=rogxml_NewNamespace(szSpace, NULL); // Create a temporary namespace
		}
		free(szSpace);
	}
	rx->nsName=ns;
//printf("%s -> [%x]%s:%s\n", szName,ns,ns?ns->szName:"NULL",ns?ns->szValue:"NULL");

	return rx;
}

API rogxml *rogxml_NewComment(const char *szText)
{
	rogxml *rx=rogxml_New("{COMMENT}");
	rogxml_SetValue(rx, szText);

	rx->nType=ROGXML_T_COM;

	return rx;
}

STATIC rogxml *rogxml_NewText(const char *szText)
{
	rogxml *rx=rogxml_New("{TEXT}");
	rogxml_SetValue(rx, szText);

	rx->nType=ROGXML_T_TEXT;

	return rx;
}

STATIC rogxml *rogxml_NewAttr(rogxml *rxParent, const char *szName, const char *szValue)
{
	rogxml *rx=rogxml_NewElement(rxParent, szName);
	rx->nType=ROGXML_T_ATTR;
	rx->szValue=strdup(szValue ? szValue : "{NULL}");

	return rx;
}

STATIC void rogxml_UnlinkFromChain(rogxml *rx, rogxml **pChain)
// Unlink the rx from the chain pointed to.  It's assumed that the
// rx is in the chain, no error if given if its not.
// Normally called with the chain as the parent's rxFirstChild, aAttrs
// or nsList pointers.
{
	if (pChain && *pChain) {
		rogxml *rxChain = *pChain;

		if (rxChain == rx) {					// We're first in the chain
			*pChain = rx->rxNext;				// Start the chain with our bro'
			rx->rxNext = NULL;					// No longer goes anywhere
		} else {
			while (rxChain) {					// While we have brothers
				if (rxChain->rxNext == rx) {	// This one's brother is us
					rxChain->rxNext = rx->rxNext;	// Make it our brother
					rx->rxNext = NULL;				// No longer goes anywhere
					break;
				}
				rxChain=rxChain->rxNext;		// Try the next
			}
		}
	}
}

API void rogxml_Unlink(rogxml *rx)
// Neatly removes the node from the tree
// Basically disowns us from the parent list (and hence we no longer have any siblings)
{
	if (!rx) return;

	if (rx->rxParent) {
		switch (rx->nType) {
		case ROGXML_T_NS:			// Namespaces need any references removing
			rogxml_UnlinkFromChain(rx, &rx->rxParent->nsList);
			break;
		case ROGXML_T_ATTR:			// Unlink attibute from siblings and parent
			rogxml_UnlinkFromChain(rx, &rx->rxParent->aAttrs);
			break;
		case ROGXML_T_TEXT:			// Remove elements and text from siblings
		case ROGXML_T_ELEM:			// and parent
		case ROGXML_T_COM:
		case ROGXML_T_PI:
			rogxml_UnlinkFromChain(rx, &rx->rxParent->rxFirstChild);
			break;
		default: ;					// Nothing to do with others
		}
	}

	rx->rxParent = NULL;
}

API void rogxml_LocaliseNamespaces(rogxml *rx)
// Makes a copy of any name spaces that are higher in the tree into this node and
// adjusts any lower elements so that they look at them.
// Method: Look for ancestral namespaces and for each one:
//           Search through every node from here down, looking for a reference to it.
//           On finding any, copy the namespace locally and point the found one at it.
{
	if (!rx) return;

	rogxml *rxAncestor;

	rxAncestor = rx->rxParent;

	while (rxAncestor) {						// While there are further ancestors
		rogxml *nsList = rxAncestor->nsList;

		while (nsList) {						// For each namespace in this ancestor
			const char *szName = nsList->szName;
			const char *szURI = nsList->szValue;
			rogxml *nsAddedName = NULL;			// New one added
			rogxml *rxScan = rx;						// Walks the tree from here down
			rogxml *rxStop = rogxml_FindNextIfDeleted(rx);

			if (!rogxml_FindSelfOrSibling(rx->nsList, szName)) {				// Don't already have it
				while (rxScan != rxStop) {
					if (rxScan->nsName && !strcmp(rxScan->nsName->szName, szName)) {	// Something uses it...
						if (!nsAddedName) {										// Still need to add it here
							rogxml_AddNamespace(rx, szName, szURI);
							nsAddedName = rogxml_FindSelfOrSibling(rx->nsList, szName);		// Find it again
						}
						rxScan->nsName = nsAddedName;							// Point at the new one
					}
					if (rxScan -> aAttrs) {										// Walk the attributes if there are any
						rxScan = rxScan -> aAttrs;
					} else {
						rxScan=rogxml_FindNextItem(rxScan);						// Works even for attributes... (?)
					}
				}
			}
			nsList=nsList->rxNext;
		}
		rxAncestor=rxAncestor->rxParent;
	}
}

API void rogxml_Delete(rogxml *rx)
//! \brief Deletes a rogxml record and any children.
//! \param rx	- The element to delete
//! Like C++'s delete, it is safe to pass a NULL into here.
//! NB. This is the way to remove an element from a tree and
//! tidy it up.
{
	if (rx) {
//printf("Deleting {%x:%d:%-12s ch=%7x at=%7x ns=%7x Dad=%7x nx=%7x - %s}\n", rx, rx->nType, rx->szName, rx->rxFirstChild, rx->aAttrs, rx->nsList, rx->rxParent, rx->rxNext, rx->szValue); if (!strcmp(rx->szName, "td")) nap(250);
		rogxml_Unlink(rx);						// Take it out the tree

		while (rx->rxFirstChild) {				// Remove any child nodes
			rogxml_Delete(rx->rxFirstChild);	// It will unlink itself
		}
		while (rx->aAttrs) {					// Remove any attributes
			rogxml_Delete(rx->aAttrs);			// It will unlink itself
		}
		while (rx->nsList) {					// Remove any namespaces
			rogxml_Delete(rx->nsList);			// It will unlink itself
		}
		if (rx->szName) free(rx->szName);
		if (rx->szValue) free(rx->szValue);
		free(rx);
	}
}

API void rogxml_Remove(rogxml *rx)
// Removes an element, leaving any children in its place
{
	if (!rx) return;

	rogxml *rxParent = rogxml_FindParent(rx);

	if (rxParent == rx || !rxParent) return;			// We are at the root

	rogxml *rxCursor = rx;

	// Promote any children to be brothers of our element
	rogxml *rxChild = rogxml_FindFirstChild(rx);
	while (rxChild) {
		rogxml_LinkSibling(rxCursor, rxChild);
		rxCursor=rxChild;
		rxChild = rogxml_FindFirstChild(rx);
	}

	rogxml_Delete(rx);									// Remove the element
}

API void rogxml_DeleteTree(rogxml *rx)
// Deletes the entirety of the tree containing rx
{
	rogxml_Delete(rogxml_FindTopNode(rx));
}

STATIC rogxml *rogxml_LinkAttr(rogxml *rx, rogxml *aAttr)
// Links a new attribute to the node
// Modified now so that it adds to the end of the list...
{
	if (rx && aAttr) {
		rogxml *a=rx->aAttrs;

		rogxml_Unlink(aAttr);		// Unlink if necessary

		aAttr->rxParent=rx;
		aAttr->rxNext=NULL;
		if (a) {					// Already have attributes
			while (a->rxNext)		// Find the last
				a=a->rxNext;
			a->rxNext=aAttr;		// and link in this one
		} else {
			rx->aAttrs=aAttr;		// Otherwise it is the first and only attr
		}
	}

	return rx;
}

STATIC rogxml *rogxml_LinkNamespace(rogxml *rx, rogxml *ns)
{
	if (rx) {
		rogxml_Unlink(ns);			// Unlink if necessary

		ns->rxNext=rx->nsList;		// Add elements namespace to this one
		ns->rxParent=rx;			// Need to know my dad for when I'm deleted
		rx->nsList=ns;				// Attach new list
	}

	return rx;
}

API rogxml *rogxml_FindLastSibling(rogxml *rx)
{
	if (rx)
		while (rx->rxNext)
			rx=rx->rxNext;

	return rx;
}

API rogxml *rogxml_LinkFirstChild(rogxml *rx, rogxml *rxChild)
// Links the child as the first child of the parent
// This currently only forces first position for normal elements, not
// attributes or namespaces.
// This can be a normal element or text or a namespace or attribute
{
	if (rogxml_ErrorNo(rx) || rogxml_ErrorNo(rxChild)) return rx;

	if (rxChild->nType == ROGXML_T_ATTR) {
		return rogxml_LinkAttr(rx, rxChild);
	} else if (rxChild->nType == ROGXML_T_NS) {
		return rogxml_LinkNamespace(rx, rxChild);
	}

	rogxml_Unlink(rxChild);					// Unlink if necessary

	rxChild->rxNext=rx->rxFirstChild;		// Make original child a brother
	rx->rxFirstChild = rxChild;				// Make this the first child

	rxChild->rxParent=rx;					// Who's your daddy...?

	return rx;
}

API rogxml *rogxml_LinkChild(rogxml *rx, rogxml *rxChild)
// Links the child to the parent
// This can be a normal element or text or a namespace or attribute
{
	if (rogxml_ErrorNo(rx) || rogxml_ErrorNo(rxChild)) return rx;

	if (rxChild->nType == ROGXML_T_ATTR) {
		return rogxml_LinkAttr(rx, rxChild);
	} else if (rxChild->nType == ROGXML_T_NS) {
		return rogxml_LinkNamespace(rx, rxChild);
	}

	rogxml_Unlink(rxChild);					// Unlink if necessary

	if (rx->rxFirstChild) {
		rogxml_FindLastSibling(rx->rxFirstChild)->rxNext = rxChild;
	} else {
		rx->rxFirstChild=rxChild;
	}
	rxChild->rxParent=rx;

	return rx;
}

API rogxml *rogxml_LinkSibling(rogxml *rx, rogxml *rxSibling)
// Links a sibling to the current element
// This works for normal elements, namespaces and attributes
{
	if (rogxml_ErrorNo(rx) || rogxml_ErrorNo(rxSibling)) return rx;

	rogxml_Unlink(rxSibling);					// Unlink if necessary

	rxSibling->rxNext=rx->rxNext;
	rxSibling->rxParent=rx->rxParent;
	rx->rxNext=rxSibling;

	return rx;
}

API rogxml *rogxml_FindHeadElement(rogxml *rx)
// Finds the head element of the tree.
// NB.	This is never the root element, it is the element that hangs
//		below that.  If there is none then NULL is returned.
{
	if (rogxml_ErrorNo(rx)) return rx;	// Just send errors back

	while (rx->rxParent)				// Climb the tree (to the root...?!?)
		rx=rx->rxParent;

	if (rogxml_IsRoot(rx)) {			// If there is a root,
		rx=rx->rxFirstChild;			// drop down to the first child
		while (rx && rx->nType != ROGXML_T_ELEM)	// First 'real' one
			rx=rx->rxNext;
	}

	return rx;
}

API rogxml *rogxml_FindTopNode(rogxml *rx)
// Finds the highest element in the tree
{
	if (rx)
		while (rx->rxParent)
			rx=rx->rxParent;

	return rx;
}

API rogxml *rogxml_FindRoot(rogxml *rx)
// Forcibly finds the root of the tree in which 'rx' is an element.
// If there is none then one is added and that's returned.
// If the rx passed is an error, it is returned unharmed.
{
	rogxml *root;

	if (rogxml_ErrorNo(rx)) return rx;

	while (rx->rxParent)
		rx=rx->rxParent;

	if (!rogxml_IsRoot(rx)) {
		root=rogxml_NewRoot();
		rogxml_LinkChild(root, rx);
	} else {
		root=rx;
	}

	return root;
}

API rogxml *rogxml_FindDocumentElement(rogxml *rx)
{
	rx=rogxml_FindTopNode(rx);				// Get top-most element

	if (!rogxml_IsRoot(rx)) return rx;		// Not root, must be document (or NULL or ERROR)

	rx=rogxml_FindFirstChild(rx);
	while (rx) {
		if (rogxml_IsElement(rx)) return rx;	// Must be document element
		rx=rogxml_FindNextSibling(rx);
	}

	return NULL;							// There is no document element!
}

API rogxml *rogxml_FindInstructionChild(rogxml *rx)
// Finds the first child of this node that is a processing instruction
{
	if (rx)
		for (rx=rx->rxFirstChild;rx;rx=rx->rxNext)
			if (rx->nType == ROGXML_T_PI) break;

	return rx;
}

API rogxml *rogxml_FindProlog(rogxml *rx)
// Finds the prolog entry for the tree, creates one if there isn't one
// Can (and will) only return NULL if NULL or error is passed in
{
	rogxml *rxProlog = NULL;

	if (!rogxml_ErrorNo(rx)) {
		rx=rogxml_FindRoot(rx);

		rxProlog = rogxml_FindInstructionChild(rx);
		if (!rxProlog) {
			rxProlog = rogxml_New("xml");
			rxProlog->nType = ROGXML_T_PI;
			rogxml_AddAttr(rxProlog, "version", "1.0");
			rogxml_LinkFirstChild(rx, rxProlog);
		}
	}

	return rxProlog;
}

API void rogxml_SetVersion(rogxml *rx, const char *szText)
// Sets the version of the XML to that given ("1.0" if NULL or blank).
// This is done by adding a processing instruction to the root node.
{
	rogxml *rxProlog = rogxml_FindProlog(rx);
	rogxml_SetAttr(rxProlog, "version", (szText && *szText) ? szText : "1.0");
}

API void rogxml_SetValue(rogxml *rx, const char *szValue)
// Either sets the value for an attribute etc. or sets the 'content' of
// an ordinary element.
// Note that in the case of ordinary elements, this will zap any existing
// children of that element.
// NULL SAFE
{
	if (rogxml_ErrorNo(rx)) return;

	if (rx->nType == ROGXML_T_ELEM) {
		while (rx->rxFirstChild) {				// Remove any child nodes
			rogxml_Delete(rx->rxFirstChild);	// It will unlink itself
		}
		if (szValue) rogxml_AddText(rx, szValue);
	} else {
		if (rx->szValue) free(rx->szValue);
		rx->szValue = szValue ? strdup(szValue) : NULL;
	}
}

// Setting 'CDATA' on a text element makes it render as <![CDATA[...]]> with unescaped content.
API void rogxml_SetCData(rogxml *rx)
// Sets CDATA state on the element
{
	if (rx) rx->fFlags = (rx->fFlags & ~ROGXML_F_CDATA) | ROGXML_F_CDATA;
}

API void rogxml_ResetCData(rogxml *rx)
// Resets CDATA state on the element
{
	if (rx) rx->fFlags &= ~ROGXML_F_CDATA;
}

// Setting literal on a text element makes it render with no unescaping but without CDATA.  This is
// illegal for XML, but is used sometimes within HTML in, for example, <script>...</script> elements.
API void rogxml_SetLiteral(rogxml *rx)
// Sets literal state on the element
{
	if (rx) rx->fFlags = (rx->fFlags & ~ROGXML_F_LITERAL) | ROGXML_F_LITERAL;
}

API void rogxml_ResetLiteral(rogxml *rx)
// Resets CDATA state on the element
{
	if (rx) rx->fFlags &= ~ROGXML_F_LITERAL;
}

STATIC rogxml *rogxml_SetError(rogxml *rx, int nErr, const char *szFmt, ...)
// Changes the element to be an error with 'in <name>' on the end
{
	char buf[1000];
	const char *szName;

	if (rx) {
		if (!szFmt) szFmt = "";
		va_list ap;
		va_start(ap, szFmt);
		vsnprintf(buf, sizeof(buf), szFmt, ap);
		va_end(ap);
		szName=rogxml_GetLocalName(rx);
		if (szName)
			sprintf(buf+strlen(buf), " in <%s>", szName);

		rx->nType=-nErr;
		if (rx->szName) free(rx->szName);
		rx->szName=strdup(buf);
	}

	return rx;
}

API rogxml *rogxml_NewError(int nErr, const char *szFmt, ...)
{
	char buf[1000];
	rogxml *rx;

	if (szFmt) {
		va_list ap;
		va_start(ap, szFmt);
		vsnprintf(buf, sizeof(buf), szFmt, ap);
		va_end(ap);
	} else {
		*buf = '\0';
	}

	rx = rogxml_New(buf);

	rx->nType=-nErr;

	return rx;
}

STATIC rogxml *rogxml_ReturnError(rogxml *rx, int nErr, const char *szFmt, ...)
// Delete rx and return an erronous rogxml.
{
	char buf[1000];
	va_list ap;

	if (szFmt) {
		va_start(ap, szFmt);
		vsnprintf(buf, sizeof(buf), szFmt, ap);
		va_end(ap);
	} else {
		*buf = '\0';
	}

	if (rx->szName)
		snprintf(buf+strlen(buf), sizeof(buf)-strlen(buf), " while parsing '%s'", rogxml_GetFullName(rx));

	rogxml_Delete(rx);						// May need it in above bit...
	return rogxml_NewError(nErr, "%s", buf);
}

API const char *rogxml_SplitName(rogxml *rx, const char *szName, rogxml **pns)
//! \brief Splits a full element name into the namespace and local name
//! Returns a pointer to the local name and puts a pointer to the
//! namespace in '*pns'.
//! (Rather like SplitName() but returns the actual namespace in *pns, not just the name of it
//! \param szName	- The name (containing ':' if it has a namespace)
//! \param pns		- A pointer to somewhere to put the namespace
//! \return char*	- A pointer into szName (may be start if no namespace)
//! \return NULL	- Namespace not found
{
	rogxml *ns = NULL;			// Namespace we've found
	const char *szLocal;
	char *szSpace=SplitName(szName, &szLocal);

	if (szSpace) {
		ns=rogxml_FindNamespace(rx, szSpace);
		free(szSpace);
	}

	if (pns) *pns=ns;

	return szLocal;
}

API int rogxml_AddNamespace(rogxml *rx, const char *szName, const char *szURI)
// Adds a namespace definition to an element
{
	if (!rx || !szName || !szURI)
		return 0;

	rogxml *ns = rogxml_FindSelfOrSibling(rx->nsList, szName);

	if (ns) return 0;			// Already there

	ns=rogxml_NewNamespace(szName, szURI);
	rogxml_LinkNamespace(rx, ns);

// This is the case, for example of:
// <rog:elem xmlns:rog="something"> ...
// or even
// <rog:first xmlns:rog="something">
//   <rog:second xmlns:rog="something else">...
	if (rx->nsName && !strcmp(rx->nsName->szName, szName)) {
		if (!rx->nsName->szValue)
			rogxml_Delete(rx->nsName);
		rx->nsName=ns;
	}

	return 1;
}

STATIC int rogxml_NamespaceDefined(rogxml *rx)
//! \brief Checks the namespace of an element has been set on it's completion
{
	// No namespace or it is defined (not temporary)
	return !rx || !rx->nsName || rx->nsName->szValue;
}

STATIC void rogxml_ValidateNamespace(rogxml *rx)
// Checks for a valid namespace and sets rx to error if there isn't one
{
	if (!rogxml_NamespaceDefined(rx)) {
		rogxml_SetError(rx, ROGXML_E_NONS, "Undefined namespace '%s'", rx->nsName->szName);
	}
}

// These 'Heap()' functions could now be superceded by the hbuf_ functions

STATIC char *rogxml_AddHeapTextLen(char *szText, const char *szExtra, int nLen)
// Where szText is already allocated on the heap, add nLen characters
// of szExtra to it.
{
	int nOldLen = szText ? strlen(szText) : 0;
	char *szNew=(char *)malloc(nOldLen+nLen+1);

	if (nOldLen) strcpy(szNew, szText);
	memcpy(szNew+nOldLen, szExtra, nLen);
	szNew[nOldLen+nLen]='\0';
	if (szText) free(szText);

	return szNew;
}

STATIC char *rogxml_AddHeapText(char *szText, const char *szExtra)
// Where szText is already allocated on the heap, add szExtra to it.
{
	return rogxml_AddHeapTextLen(szText, szExtra, strlen(szExtra));
}

STATIC char *rogxml_AddHeapChar(char *szText, char c)
{
	return rogxml_AddHeapTextLen(szText, &c, 1);
}

static const char *szFunnies = "&\"'<>";

API rogxml *rogxml_FindAttr(rogxml *rx, const char *szName)
{
	return rx ? rogxml_FindSelfOrSibling(rx->aAttrs, szName) : NULL;
}

API const char *rogxml_GetAttr(rogxml *rx, const char *szName)
{
	rogxml *rxAttr=rogxml_FindAttr(rx, szName);

	return rxAttr ? rxAttr->szValue : NULL;
}

API int rogxml_GetAttrInt(rogxml *rx, const char *szName, int nDef)
// Returns the integer value of the attribute if defined, otherwise returns nDef.
// NB. If the attr value is non-numeric, returns 0.
{
	const char *szValue = rogxml_GetAttr(rx, szName);

	return szValue ? atoi(szValue) : nDef;
}

API int rogxml_AddAttr(rogxml *rx, const char *szName, const char *szValue)
//! \brief Adds a new attribute to an element.
//! The attribute to be added must not already exist in the element or
//! rx is set to error with an appropriate message.  To change a previously
//! set attribute, use rogxml_SetAttr() instead.
//! This function will add a namespace if szName starts 'xmlns'
//! \param szName	-	The name of the attribute
//! \param szValue	-	The value of the attribute
//! \return	1	Attribute added ok
//! \return	0	Attribute exists/namespace error (rx set to error)
{
	if (!rx) return 0;

	rogxml *aNew = rogxml_FindAttr(rx, szName);
	int ok = 1;				// Indicates no error

	if (!szName || !*szName) {
		rogxml_SetError(rx, ROGXML_E_NONAME, "No attribute name");
		ok = 0;
	} else if (aNew) {									// Exists...
		rogxml_SetError(rx, ROGXML_E_DUPATTR, "Duplicate attribute");
		ok = 0;
	} else {
		const char *szURI;
		const char *szLocal;
		const char *szSpace=SplitName(szName, &szLocal);

		if (szSpace) {							// Check if it's "xmlns"
			if (!strcmp(szSpace, "xmlns")) {
				ok=rogxml_AddNamespace(rx, szLocal, szValue);
				if (!ok)
					rogxml_SetError(rx, ROGXML_E_DUPNS, "Duplicate namespace definition");
				return ok;
			}
		}
		aNew=rogxml_NewAttr(rx, szName, szValue);
		szURI = rogxml_GetNamespace(aNew);
		if (szURI) {
			rogxml_LinkAttr(rx, aNew);
		} else {
			rogxml_SetError(rx, ROGXML_E_NONS, "Attribute '%s' - namespace undefined", szName);
			ok=0;
		}
	}

	return ok;
}

API rogxml *rogxml_SetAttr(rogxml *rx, const char *szName, const char *szValue)
//! \brief Sets the value of an attribute in an element
//! If the attribute does not already exist in the element, it is added.
//! If it already exists, its value is set to 'szValue'.
//! Attributes can be deleted by passing szValue as NULL
//! If the intention is to set a unique attribute (as when parsing an XML
//! file for example), use rogxml_AddAttr() as this will return an error if
//! you try to set an existing atribute.
//! \param szName	-	The name of the attribute.
//! \param szValue	-	The value of the attribute.
{
	rogxml *aNew = NULL;

	if (rogxml_ErrorNo(rx)) return NULL;

	if (szName && *szName) {
		aNew = rogxml_FindAttr(rx, szName);
		if (szValue) {
			if (aNew) {									// Exists...
				rogxml_SetValue(aNew, szValue);
			} else {
				aNew = rogxml_NewAttr(rx, szName, szValue);
				rogxml_LinkAttr(rx, aNew);
			}
		} else {
			rogxml_Delete(aNew);
		}
	}

	return aNew;
}

API rogxml *rogxml_SetAttrInt(rogxml *rx, const char *szName, int nValue)
{
	char buf[20];

	sprintf(buf, "%d", nValue);

	return rogxml_SetAttr(rx, szName, buf);
}

API rogxml *rogxml_AddText(rogxml *rx, const char *szText)
//! \brief Add text to an element
//! Adds the text given to the element, appending it to any existing text
//! or child elements already there.  If multiple texts are added to a single
//! element, they will remain distinct in the internal structure but there is
//! no delimiter when they are written so they will effectively merge into
//! one.
//! \param szText The text to add
//! \return	A handle to the new text element added.
//! \return NULL	rx passed was not valid
{
	rogxml *rxNew = NULL;

	if (!rogxml_ErrorNo(rx) && szText) {
		rxNew = rogxml_NewText(szText);

		rogxml_LinkChild(rx, rxNew);
	}

	return rxNew;
}

API rogxml *rogxml_SetText(rogxml *rx, const char *szText)
// Sets the text under an element.  This replaces any existing children of
// the element with the single text given.
{
	if (rogxml_ErrorNo(rx) || !szText) return NULL;

	while (rx->rxFirstChild) {				// Remove any child nodes
		rogxml_Delete(rx->rxFirstChild);	// It will unlink itself
	}

	return rogxml_AddText(rx, szText);
}

API rogxml *rogxml_AddFirstChild(rogxml *rx, const char *szName)
//! \brief Add a new element as a child to an existing one
//! Creates a new element and adds it to element given.  The element will
//! be empty until something is added to it.
//! \param szName - The name for the new element
//! \return A handle to the new element
//! \return NULL	rx passed was not valid
//! \return Error	Generally namespace not recognised in szName
{
	rogxml *rxNew;

	if (rogxml_ErrorNo(rx)) return NULL;

	rxNew = rogxml_NewElement(rx, szName);

	if (!rogxml_ErrorNo(rx))
		rogxml_LinkFirstChild(rx, rxNew);

	return rxNew;
}

API rogxml *rogxml_AddChild(rogxml *rx, const char *szName)
//! \brief Add a new element as a child to an existing one
//! Creates a new element and adds it to element given.  The element will
//! be empty until something is added to it.
//! \param szName - The name for the new element
//! \return A handle to the new element
//! \return NULL	rx passed was not valid
//! \return Error	Generally namespace not recognised in szName
{
	rogxml *rxNew;

	if (rogxml_ErrorNo(rx)) return NULL;

	rxNew = rogxml_NewElement(rx, szName);

	if (!rogxml_ErrorNo(rx))
		rogxml_LinkChild(rx, rxNew);

	return rxNew;
}

API rogxml *rogxml_AddSibling(rogxml *rx, const char *szName)
// Adds a brother to the given element
{
	rogxml *rxNew;

	if (rogxml_ErrorNo(rx)) return NULL;

	rxNew = rogxml_NewElement(rx, szName);

	if (!rogxml_ErrorNo(rx))
		rogxml_LinkSibling(rx, rxNew);

	return rxNew;
}


API rogxml *rogxml_AddTextChild(rogxml *rx, const char *szName, const char *szText)
//! \brief Add a new child element with text content
//! This is really a shorthand for calls to rogxml_AddChild() followed by
//! rogxml_AddText().
//! \param szName - The name for the new element
//! \param szText - The textual content
//! \return A handle to the newly created element, <u>not</u> the text.
//! \return NULL if the passed rx was erroneous
{
	rogxml *rxElement;

	if (rogxml_ErrorNo(rx)) return NULL;

	rxElement = rogxml_AddChild(rx, szName);
	rogxml_AddText(rxElement, szText);

	return rxElement;
}

API rogxml *rogxml_AddChildComment(rogxml *rx, const char *szComment)
{
	rogxml *r;

	if (rogxml_ErrorNo(rx)) return NULL;

	r=rogxml_NewComment(szComment);
	rogxml_LinkChild(rx, r);

	return r;
}

API rogxml *rogxml_AddSiblingComment(rogxml *rx, const char *szComment)
{
	rogxml *r;

	if (rogxml_ErrorNo(rx)) return NULL;

	r=rogxml_NewComment(szComment);
	rogxml_LinkSibling(rx, r);

	return r;
}

API rogxml *rogxml_AddChildAttr(rogxml *rx, const char *szElement, const char *szAttr, const char *szValue)
{
	rogxml *r;

	if (rogxml_ErrorNo(rx)) return NULL;

	r=rogxml_AddChild(rx, szElement);
	rogxml_AddAttr(r, szAttr, szValue);

	return r;
}

API rogxml *rogxml_AddTextChildCond(rogxml *rx, const char *szName, const char *szText)
{
	if (szText && *szText)
		return rogxml_AddTextChild(rx, szName, szText);
	else
		return NULL;
}

#define CHUNK	1024				// MUST BE 2^n - increment for each output chunk in _rogxml_txout

STATIC int isValidUTF8(const char *szText)
// 30667 Look at the table here: http://en.wikipedia.org/wiki/UTF-8
// Returns 1 if valid, 0 if not
{
	const unsigned char *chp;
	int bytes = 0;									// Character bytes we're expecting
	unsigned char c;

	for (chp=(const unsigned char *)szText;(c=*chp);chp++) {
		if (bytes) {								// We're walking over a multi-byte character
			if ((c & 0xc0) != 0x80) return 0;		// Bytes 2-n MUST be 0b10xxxxxx
			bytes--;
			continue;
		}

		if (c & 0x80) {								// Top bit set so first byte in sequence
			if ((c & 0xc0) == 0x80) return 0;		// Non-continuation byte must not be 0b10xxxxxx
			if (c <= 0xc1) return 0;				// C0, C1 mean an 'over-long' character (though perhaps should accept C0 80)
			if (c >= 0xf5) return 0;				// F5... would encode characters over 0x10FFFF
			bytes=1;								// Number of extra bytes we're expecting in the sequence
			if ((c & 0xf0) == 0xe0) bytes=2;		// Matches 0b1110_xxxx
			if ((c & 0xf8) == 0xf0) bytes=3;		// Matches 0b1111_0xxx
			if ((c & 0xfc) == 0xf8) bytes=4;		// Matches 0b1111_10xx
			if ((c & 0xfe) == 0xfc) bytes=5;		// Matches 0b1111_110x
		}
	}

	return bytes == 0;								// Not valid if we still have bytes left at the end
}

STATIC void rogxml_ShowText(const char *szText)
{
	if (!szText) return;							// Nothing to do

	char bHeapCopy = 0;								// 1 if szText becomes a heap-based copy of the original

	if (!isValidUTF8(szText)) {						// 30667 The text is not valid UTF-8 so we'll make sure it is
		char *szCopy = (char *)malloc(strlen(szText)*2+1);	// Maximum we'll need
		char *dest=szCopy;
		char c;
		const char *chp;

		for (chp=szText;(c=*chp);chp++) {
			if (c & 0x80) {
				char prefix = (c & 0x40) ? 0xc3 : 0xc2;
				*dest++=prefix;						// Put in our prefix byte
				c &= 0xbf;							// Force character to be 0b10xxxxxx
			}
			*dest++=c;
		}
		*dest='\0';
		szText = szCopy;
		bHeapCopy = 1;
	}

	if (_rogxml_fpout) {
		fputs(szText, _rogxml_fpout);
	} else {								// Must be to string then - note complex chunk allocation method for speed
		int nTextLen = strlen(szText);
		int nNewLen = _count_txout + nTextLen;
		int nLimit = (_count_txout + CHUNK - 1) & ~(CHUNK-1);
		int nNewLimit = (nNewLen + CHUNK - 1) & ~(CHUNK-1);

		if (nNewLimit > nLimit) {		// We need more space in the buffer
			if (_count_txout == 0) {
				_rogxml_txout = (char*)malloc(nNewLimit);		// Allocate initial space
			} else {
				_rogxml_txout = (char*)realloc(_rogxml_txout, nNewLimit);		// Allocate new space
			}
		}
		memcpy(_rogxml_txout+_count_txout, szText, nTextLen);	// Copy in the new text
		_count_txout += nTextLen;								// Remember where we are
//		_rogxml_txout = rogxml_AddHeapText(_rogxml_txout, szText);
	}

	if (bHeapCopy) szDelete(szText);						// szText is actually a local, heap-based copy
}

STATIC void rogxml_ShowChar(char c)
{
	char buf[2];
	buf[0]=c;
	buf[1]='\0';
	rogxml_ShowText(buf);
}

STATIC void rogxml_ShowLinefeed()
{
	if (_rogxml_verbose) rogxml_ShowText(_rogxml_Linefeed);
}

STATIC void rogxml_ShowIndent(int nSpaces)
{
	if (!_rogxml_verbose || !_rogxml_Indent) return; // Don't want extra spaces

	while (nSpaces-- > 0)
		rogxml_ShowText(_rogxml_Indent);
}

STATIC const char *XmlEscapeText(const char *szText)
{
	if (!szText) return strdup("");						// NULL safety as Linux is NOT forgiving on this point

	HBUF *buf = hbuf_New();
	char c;

	while ((c = *szText++)) {
		if (((c & 128) && !isUTF8) || (c>0 && c<32)) {	// High bit set char or ctrl
			char szBuf[10];

			snprintf(szBuf, sizeof(szBuf), "&#x%x;", (unsigned int)(c & 0xff));
			hbuf_AddBuffer(buf, strlen(szBuf), szBuf);
		} else if (strchr(szFunnies, c)) {		// A special one
			char szBuf[10];
			switch (c) {
			case '&':	strcpy(szBuf, "&amp;");		break;
			case '"':	strcpy(szBuf, "&quot;");	break;
			case '\'':	if (_rogxml_bOutputXHTML) {
							strcpy(szBuf, "&#39;");
						} else {
							strcpy(szBuf, "&apos;");
						}
						break;
			case '<':	strcpy(szBuf, "&lt;");		break;
			case '>':	strcpy(szBuf, "&gt;");		break;
			}
			hbuf_AddBuffer(buf, strlen(szBuf), szBuf);
		} else {
			hbuf_AddChar(buf, c);
		}
	}

	hbuf_AddChar(buf, 0);
	const char *result = hbuf_ReleaseBuffer(buf);

	hbuf_Delete(buf);

	return result;
}

STATIC void rogxml_ShowEscapedText(const char *szText)
{
	if (szText) {
		const char *escaped = XmlEscapeText(szText);
		rogxml_ShowText(escaped);
		szDelete(escaped);
	}

	return;
}

STATIC void rogxml_ShowElementText(rogxml *rx)
// Outputs as plain text or escaped text depending on the
// states of the CDATA and LITERAL flags in the flags word
{
	int bCData = rx->fFlags & ROGXML_F_CDATA;
	int bLiteral = rx->fFlags & ROGXML_F_LITERAL;

	if (bCData) rogxml_ShowText("<![CDATA[");
	if (bLiteral || bCData)
		rogxml_ShowText(rx->szValue);
	else
		rogxml_ShowEscapedText(rx->szValue);
	if (bCData) rogxml_ShowText("]]>");
}

STATIC void rogxml_ShowNamespaces(rogxml *rx)
{
	rogxml *ns=rx->nsList;

	while (ns) {
		rogxml_ShowText(" xmlns:");
		rogxml_ShowText(ns->szName);
		rogxml_ShowText("=\"");
		rogxml_ShowText(ns->szValue);
		rogxml_ShowChar('"');

		ns=ns->rxNext;
	}
}

STATIC void rogxml_ShowAttributes(rogxml *rx)
// Perhaps have the option here of writing the attributes in alphabetical order
{
	rogxml *a=rx->aAttrs;

	while (a) {
		rogxml_ShowChar(' ');
		rogxml_ShowText(rogxml_GetFullName(a));
		rogxml_ShowText("=\"");
		rogxml_ShowElementText(a);
		rogxml_ShowText("\"");
		a=a->rxNext;
	}
}

STATIC void rogxml_ShowOpenTag(rogxml *rx)
{
	rogxml_ShowChar('<');
	if (rx->nType == ROGXML_T_PI)
		rogxml_ShowChar('?');
	rogxml_ShowText(rogxml_GetFullName(rx));
	rogxml_ShowNamespaces(rx);
	rogxml_ShowAttributes(rx);
	if (!rx->rxFirstChild)
		rogxml_ShowChar(rx->nType == ROGXML_T_PI ? '?' : '/');
	rogxml_ShowChar('>');
}

STATIC void rogxml_ShowCloseTag(rogxml *rx)
{
	rogxml_ShowText("</");
	if (!_rogxml_bBlankCloseTags) rogxml_ShowText(rogxml_GetFullName(rx));
	rogxml_ShowChar('>');
	rogxml_ShowLinefeed();
}

API void rogxml_DumpElement(rogxml *rx)
{
	if (_rogxml_fpdump ==NULL) {
		_rogxml_fpdump = stdout;
	}
	if (!rx) {
		fprintf(_rogxml_fpdump, "{NULL}\n");
		return;
	}
	fprintf(_rogxml_fpdump, "Location:   %p\n", rx);
	fprintf(_rogxml_fpdump, "Sibling:    %p\n", rx->rxNext);
	fprintf(_rogxml_fpdump, "Parent:     %p\n", rx->rxParent);
	fprintf(_rogxml_fpdump, "Type:       %d\n", rx->nType);
	fprintf(_rogxml_fpdump, "Name:       %s\n", rogxml_GetFullName(rx));
	fprintf(_rogxml_fpdump, "Value:      %s\n", rogxml_GetValue(rx));
	fprintf(_rogxml_fpdump, "Namespace:  ");
	if (rx->nsName) {
		fprintf(_rogxml_fpdump, "%s=%s\n", rogxml_GetLocalName(rx->nsName), rogxml_GetValue(rx->nsName));
	} else {
		fprintf(_rogxml_fpdump, "None\n");
	}
	fprintf(_rogxml_fpdump, "Namespaces: ");
	if (rx->nsList) {
		rogxml *r=rx->nsList;
		while (r) {
			fprintf(_rogxml_fpdump, "%s=%s\n", rogxml_GetLocalName(r), rogxml_GetValue(r));
			r=r->rxNext;
			if (r) fprintf(_rogxml_fpdump, "            ");
		}
	} else {
		fprintf(_rogxml_fpdump, "None\n");
	}
	fprintf(_rogxml_fpdump, "First born: %p\n", rx->rxFirstChild);
	fprintf(_rogxml_fpdump, "Attrs:      ");
	if (rx->aAttrs) {
		rogxml *r=rx->aAttrs;
		while (r) {
			fprintf(_rogxml_fpdump, "%s=%s\n", rogxml_GetFullName(r), rogxml_GetValue(r));
			r=r->rxNext;
			if (r) fprintf(_rogxml_fpdump, "            ");
		}
	} else {
		fprintf(_rogxml_fpdump, "None\n");
	}
}

API void rogxml_DumpRaw(rogxml *rx)
{
	if (_rogxml_fpdump ==NULL) {
		_rogxml_fpdump = stdout;  
	}
	fprintf(_rogxml_fpdump, "%p '%s(%d)' Attrs: %p, Parent: %p, Children %p, Next %p\n",
		rx, rx->szName, rx->nType,
		rx->aAttrs,
		rx->rxParent,
		rx->rxFirstChild,
		rx->rxNext);
	fflush(_rogxml_fpdump);
}

STATIC void rogxml_Writex(int nIndent, rogxml *rx)
{
	if (!_rogxml_show_comments && rx->nType == ROGXML_T_COM)
		return;

	if (rx && rx->rxFirstChild) {						// We have children
		rogxml *rxChild = rx->rxFirstChild;
		char bText=1;									// Assume simple text for a moment

		// Special case if all children are text (including CDATA) as we don't want to show any extra white space
		while (rxChild) {
			if (rxChild->nType != ROGXML_T_TEXT) {		// Isn't text
				bText = 0;
				break;
			}
			rxChild=rxChild->rxNext;
		}

		rxChild = rx->rxFirstChild;						// Reset to first child again
		if (bText) {									// Text so no white-space inside
			rogxml_ShowIndent(nIndent);
			rogxml_ShowOpenTag(rx);
			while (rxChild) {
//rogxml_ShowText("{");
				rogxml_ShowElementText(rxChild);
//rogxml_ShowText("}");
				rxChild=rxChild->rxNext;
			}
			rogxml_ShowCloseTag(rx);
		} else {
			rogxml_ShowIndent(nIndent);
			rogxml_ShowOpenTag(rx);
			rogxml_ShowLinefeed();
			while (rxChild) {
				rogxml_Writex(nIndent+1, rxChild);
				rxChild=rxChild->rxNext;
			}
			rogxml_ShowIndent(nIndent);
			rogxml_ShowCloseTag(rx);
		}
	} else {										// Children
		if (!rx || rx->nType < 0) {	// An Error...
			char buf[10];

			sprintf(buf, "%d", rogxml_ErrorNo(rx));
			rogxml_ShowText("** Error ");
			rogxml_ShowText(buf);
			rogxml_ShowText(": ");
			rogxml_ShowText(rogxml_ErrorText(rx));
			rogxml_ShowLinefeed();
		} else if (rx->nType == ROGXML_T_TEXT) {	// Text node
			rogxml_ShowIndent(nIndent);
			rogxml_ShowElementText(rx);
			rogxml_ShowLinefeed();
		} else if (rx->nType == ROGXML_T_COM) {		// Comment node
			rogxml_ShowIndent(nIndent);
			rogxml_ShowText("<!--");
			rogxml_ShowText(rx->szValue);
			rogxml_ShowText("-->");
			rogxml_ShowLinefeed();
		} else {									// Empty child of some type
			rogxml_ShowIndent(nIndent);
			rogxml_ShowOpenTag(rx);
			rogxml_ShowLinefeed();
		}
	}
}

API void rogxml_CloseInputFile()
//! \brief Closes an input file
//! Files opened using rogxml_OpenInputFile() should be closed after use
//! with this //! function.  Only one file can be open at a time.
//! It is <u>not</u> an error to call this function if there is no file
//! open.
{
	if (_rogxml_fpin && _rogxml_fpin != stdin) {
		fclose(_rogxml_fpin);
	}
	_rogxml_fpin = NULL;
	_rogxml_txin = NULL;
}

API char *rogxml_CloseOutputFile()
// Ensures the output file is closed
// Unless _rogxml_fpout is fopen()'d, all subsequent writes will be to
// _rogxml_txout.
// Note that this returns the text written to _rogxml_txout if the previous writes were memory based.
{
	char *szResult = NULL;

	if (_rogxml_fpout && _rogxml_fpout != stdout) {
		fclose(_rogxml_fpout);	// Let's tidy it up
	}
	if (_count_txout) {					// Was text based so need to copy before we zap it
		szResult = (char*)malloc(_count_txout+1);
		memcpy(szResult, _rogxml_txout, _count_txout);
		szResult[_count_txout]='\0';
		free(_rogxml_txout);
	}
	_rogxml_fpout = NULL;
	_rogxml_txout = NULL;
	_count_txout = 0;

	return szResult;
}

API const char *rogxml_ToText(rogxml *rx)
{
	return rogxml_WriteFile(rx, ">");
}

static const char *_rogxml_NextText = NULL;

API const char *rogxml_GetNextText()
// Returns a pointer to the next piece of text that 'rogxml_FromText()' would have got.
// Can be used to call rogxml_FromText() again to get the next bit of XML from a buffer or to
// give further context if an error is returned.
// Always returns a pointer into the text most recently passed to rogxml_FromText() so will point
// Somewhere silly if that has since been deleted (or NULL if rogxml_FromText() has never been called).
{
	return _rogxml_NextText;
}

API rogxml *rogxml_FromText(const char *szText)
{
	rogxml *rx;
	FILE *fpin = _rogxml_fpin;		// Need to temporarily close file

	_rogxml_fpin = NULL;
	_rogxml_txin = szText;

	rx=rogxml_ReadElement(NULL);
	_rogxml_NextText = _rogxml_txin;
	_rogxml_txin = NULL;

	_rogxml_fpin = fpin;

	return rx;
}

API void rogxml_Dump(rogxml *rx)
//! \brief Dumps the element to the current dump file
//! This function is designed for use in debugging the application.  It
//! simply dumps the element in the same format as it would go
//! to a file if rogxml_WriteFile() were used.  Indenting is always
//! enabled as if rogxml_Verbose(1) had been called.

{
	int bOldVerbose = _rogxml_verbose;
	_rogxml_verbose = 1;
	if (_rogxml_fpdump ==NULL) {
		_rogxml_fpdump = stdout;  
	}   
	_rogxml_fpout = _rogxml_fpdump;
	rogxml_Writex(0, rx);
	_rogxml_fpout = NULL;
	_rogxml_verbose = bOldVerbose;
}

API int rogxml_WriteFp(rogxml *rx, FILE *fp)
{
	_rogxml_fpout=fp;

	if (rogxml_IsRoot(rx)) {				// Root so write the children
		rx=rogxml_FindFirstChild(rx);
		while (rx) {
			rogxml_Writex(0,rx);
			rx=rx->rxNext;
		}
	} else {
		rogxml_Writex(0,rx);
	}

	return 1;
}

API const char *rogxml_WriteFile(rogxml *rx, const char *szFilename)
//! \brief Write the element to a file
//! This is the general method of creating an XML file from the internal
//! structure.  The element given (and all child elements) are written
//! to the file (indented if rogxml_Verbose(1) has been called).
//! If the filename starts with '>' then an in-memory copy of the text
//! will be produced and a pointer to it returned.
//! \param szFilename - The name of the file to write or ">"
//! \return NULL	Problem creating file
//! \return ""		'Normal' file created ok
//! \return other	A pointer to in-memory text (filename starts '>')
{
	int bUsingFile = *szFilename != '>';
	char *szOldText;
	FILE *fp;

	if (!strcmp(szFilename, "-")) return rogxml_WriteFp(rx, stdout), "";

	if (bUsingFile) {
		fp=fopen(szFilename, "w");
		if (!fp) return NULL;
	} else {
		fp=NULL;
	}

	szOldText = rogxml_CloseOutputFile();		// Make sure nothing still hanging around
	if (szOldText) free(szOldText);				// Drop it if there is anything

	rogxml_WriteFp(rx, fp);

	if (bUsingFile) {
		rogxml_CloseOutputFile();
		return "";
	} else {
		return rogxml_CloseOutputFile();
//		return _rogxml_txout;
	}
}

const char *rogxml_WriteHTMLFile(rogxml *rx, const char *szFilename)
//! \brief Write the element to a file in XHTML format
//! This is the general method of creating an XML file from the internal
//! structure.  The element given (and all child elements) are written
//! to the file (indented if rogxml_Verbose(1) has been called).
//! If the filename starts with '>' then an in-memory copy of the text
//! will be produced and a pointer to it returned.
//! \param szFilename - The name of the file to write or ">"
//! \return NULL	Problem creating file
//! \return ""		'Normal' file created ok
//! \return other	A pointer to in-memory text (filename starts '>')
{
	const char *szResult;

	_rogxml_bOutputXHTML = 1;
	szResult = rogxml_WriteFile(rx, szFilename);
	_rogxml_bOutputXHTML = 0;

	return szResult;
}

API int rogxml_ErrorNo(rogxml *rx)
//! \brief Tests an element handle for an error
//! Whenever an error is produced by a function that returns a rogxml handle,
//! the returned value will hold an error code.  Use this function to test
//! for the existance of an error.  This should be used for any function that
//! could return an error and particularly for rogxml_ReadFile().
//! \return	0 - (ROGXML_E_NOERROR) No error has occurred
//! \return 1... - A non-zero error code
{
	if (rx) {
		if (rx->nType >= 0)
			return 0;				// rx is non-NULL and non-Error
		else
			return -rx->nType;		// rx is a real error
	} else {
		return ROGXML_E_ISNULL;		// rx is NULL
	}
}

API const char *rogxml_ErrorText(rogxml *rx)
//! \brief Extracts the error text from an erroneous handle
//! Where a rogxml handle contains an error (as indicated by rogxml_ErrNo()),
//! the text of the error can be extracted by this function.
//! \return char*	- A pointer to the error text
//! \return NULL	- There is no error
{
	if (rx) {
		if (rx->nType >= 0)
			return NULL;			// rx is non-NULL and non-Error
		else
			return rx->szName;		// rx is a real error
	} else {
		return "{NULL}";			// rx is NULL
	}
}

STATIC int rogxml_UnReadChar(int c)
{
	return _rogxml_unread = c;
}

STATIC int rogxml_ReadCharRaw()
// Gets the next character from the input with no translation etc.
// returns either the character or -1 for end of file.
{
	static int nLast = 0;
	int c;

	if (_rogxml_readfn) {			// Reading from a function
		c=(*_rogxml_readfn)();
	} else if (_rogxml_txin) {				// Reading from memory
		c=(unsigned char)*_rogxml_txin++;
		if (!c) {					// Hit end of string
			c=-1;
			_rogxml_txin--;			// Back up in case we read again
		}
	} else {
		if (_rogxml_fpin) {			// Reading from file
			c=getc(_rogxml_fpin);
			if (c == EOF) c=-1;		// I think EOF is -1 anyway...
			else c &= 0xff;			// Ensure it's +ve if it's a real char
		} else {
			c=-1;					// Nowhere to read from
		}
	}

	if (c == '\r' ||
			(c == '\n' && nLast != '\r')) {
		_rogxml_lineno++;
	}
	nLast=c;

	return c;
}

STATIC int rogxml_ReadCharCooked()
// Same as Raw but never returns CR characters...
// From the W3C spec:
// To simplify the tasks of applications, the characters passed to an
// application by the XML processor must be as if the XML processor
// normalized all line breaks in external parsed entities (including
// the document entity) on input, before parsing, by translating both
// the two-character sequence #xD #xA and any #xD that is not followed
// by #xA to a single #xA character.
{
	static int nLast = 0;
	int c;

	c=rogxml_ReadCharRaw();

	if (c == '\n' && nLast == '\r') {	// Skip LFs following CRs
		c=rogxml_ReadCharRaw();
	}
	nLast=c;

	if (c == '\r') c = '\n';

	return c;
}

STATIC int rogxml_ReadChar()
// Read a character from input
// This is basically a ReadCharRoasted() as it handles unreadchar and
// multiple white space mapping.
// Returns	char	Character read
//			-1		End of file
{
	int c;

	if (_rogxml_unread) {
		c=_rogxml_unread;
		_rogxml_unread = 0;
	} else {
		c=rogxml_ReadCharCooked();
	}
	if (c == -1 ) return -1;
	if (!_rogxml_bRetainControls) {
		if (c <= ' ' && c >= 0) {
			c = ' ';		// Map all white space to spaces
			if (_rogxml_last_char == ' ') {
// Uncomment the next line instead of the one after to compress multiple spaces
//			return rogxml_ReadChar();
				return c;
			}
		}
	}
	_rogxml_last_char = c;

	return c;
}

STATIC int rogxml_ReadNonSpace()
{
	int c;

	do {
		c=rogxml_ReadChar();
	} while (c <= ' ' && c > 0);

	return c;
}

STATIC char *rogxml_ReadWord()
// Reads the next word on the input.
// Skips any leading spaces and reads until an invalid characters
// Words start with an alpha or '_' and can contain alphanums, _, -, : and .
// Returns	NULL	There was no legitimate word
//			char*	The word allocated on the heap (call free on it!)
{
	int c=rogxml_ReadNonSpace();
	char *szResult = NULL;

	if (c <= 0 || (!isalpha(c) && c != '_')) {
		rogxml_UnReadChar(c);
		return NULL;
	}

	for (;;) {
		if (!isalnum(c) &&
				c != '_' && c != '-' && c != '.' && c != ':') break;
		szResult=rogxml_AddHeapChar(szResult, c);
		c=rogxml_ReadChar();
	}

	rogxml_UnReadChar(c);
	return szResult;
}

STATIC char *rogxml_ReadUntil(const char *szMatch)
// Reads text until 'szMatch' or end of file.
// Note that this uses ReadCharCooked() so white spaces are not compressed,
// but CRLF handling is done so we only see LF characters.
// ALSO NOTE that an UnReadChar() before this call is ineffective!!!!!
//
// NB. If you're trying to understand this code...  Consider szMatch="]]>" and a string containing "]]]>",
// We start to match but fail on the third ']' but we need to notice that we are still in a valid matching situation.
// This is covered in the for loop in the middle.  This has been tested for "]]>" situations but NOT with "ababc"
// sitations as we're never called with those sorts of strings.  Hence this function will work in here where we
// have a limited number of 'szMatch' strings to deal with but it might not work in a more general case so be
// careful to unit test if you pinch this code!
//
// Returns	NULL	We got end of file first
//			char*	The text allocated on the heap (call free on it!)
{
	const char *szCursor=szMatch;				// Pointer into terminating text
	char *szResult = NULL;
	int c;

	for (;;) {
		c=rogxml_ReadCharCooked();
		if (c == -1) {						// Reached end of file
			if (szResult) free(szResult);
			return NULL;
		}

		if (c == (int)(*szCursor)) {		// Matching
			szCursor++;
			if (!*szCursor) return szResult;	// Yay, finished!
		} else {
			if (szCursor != szMatch) {		// Been collecting end chars...
				const char *szRover=szCursor;
				char bMatched=0;

				// Need to see if those collected last could be the start of a new match
				for (szRover=szMatch+1;szRover<szCursor;szRover++) {
					if (!strncmp(szMatch,szRover,szCursor-szRover) && c == szMatch[szCursor-szRover]) {
						bMatched=1;
						break;
					}
				}
				szResult=rogxml_AddHeapTextLen(szResult, szMatch, szRover-szMatch);
				if (bMatched) {									// We found we matched further along so set the cursor
					szCursor=szMatch+(szCursor-szRover)+1;
				} else {										// No more match so reset the cursor and copy in 'c'
					szCursor=szMatch;
					szResult=rogxml_AddHeapChar(szResult, (char)c);
				}
			} else {
				szResult=rogxml_AddHeapChar(szResult, (char)c);
			}
		}
	}
}

STATIC int rogxml_ReadEscapedChar()
// Reads a character, interpreting entities
// After this call, _rogxml_entity_error will be NULL or point to an error string
// Returns	-1		End of file
//			-2		Malformed sequence
//			-3		Unknown entity
//			char	The character
{
	int c=rogxml_ReadChar();
	char *szName;
	int nCount = 0;					// Count of chars in second part

	_rogxml_entity_error = NULL;	// Start off with no error

	if (c != '&') {
		if (c < 0) _rogxml_entity_error = "Unexpected end of file";
		return c;
	}

	szName=strdup("");
	for (;;) {
		c=rogxml_ReadChar();
		if (c == ';') break;		// Completed sequence
		if (nCount > 10) {			// Too long, drop out before we go wild
			free(szName);
			_rogxml_entity_error = "Entity too long";
			return -2;
		}
		if ((c == '#' && nCount == 0) || isalnum(c)) {
			szName=rogxml_AddHeapChar(szName, c);
			nCount++;
			continue;
		}
	}

	if (*szName == '#') {
		if (szName[1] == 'x') {		// Hex '&#x'
			sscanf(szName+2, "%x", &c);
		} else {
			sscanf(szName+1, "%d", &c);
		}
	} else if (!strcmp(szName, "lt"))	c='<';
	else if (!strcmp(szName, "gt"))		c='>';
	else if (!strcmp(szName, "amp"))	c='&';
	else if (!strcmp(szName, "quot"))	c='"';
	else if (!strcmp(szName, "apos"))	c='\'';
	else {
		_rogxml_entity_error = "Entity unrecognised";
		c=-3;						// Unrecognised
	}

	free(szName);

	return c;
}

STATIC const char *szReadStringError = NULL;

STATIC const char *rogxml_ReadString()
// Reads a string which is quoted with either ' or ".
// Returns	char*				String allocated on heap (call free() on it sometime!)
//			szReadStringError	No string, end of file, malformed entity etc. - described in szReadStringError
{
	char cQuote=rogxml_ReadNonSpace();
	char *szResult = NULL;

	if (cQuote != '\'' && cQuote != '"') return NULL;
	for (;;) {
		int c=rogxml_ReadEscapedChar();

// Note, the next line stops &#0; from inserting something into one of
// our strings, which is probably just as well in C...
		if (c <= 0) {					// EOF before reaching end of string
			char buf[100];
			const char *err = NULL;

			if (c == 0) err = "Zero character";
			else if (c == -1) err = "End of file";
			else if (c == -2) err = "Malformed sequence";
			else if (c == -3) err = "unknown entity";
			else {
				snprintf(buf, sizeof(buf), "Error %d returned from rogxml_ReadEscapedChar()", c);
				err = buf;
			}
			szDelete(szReadStringError);
			if (szResult) {
				szReadStringError = hprintf(NULL, "%s after [%s]", err, szResult);
			} else {
				szReadStringError = hprintf(NULL, "%s at start of string", err);
			}

			if (szResult) free(szResult);
			return szReadStringError;
		}
		if (_rogxml_last_char == cQuote)
			break;
		szResult=rogxml_AddHeapChar(szResult, c);
	}

	if (!szResult)
		szResult=strdup("");

	return szResult;
}

STATIC rogxml *rogxml_ReadElementX(rogxml *rxParent)
// Read an element but start just after the initial '<' character
{
	int c;
	char *szName;
	rogxml *rx;
	int bIsProcessing = 0;

	c=rogxml_ReadChar();
	if (c == '!') {				// Comment, DOCTYPE or CDATA?
		c=rogxml_ReadChar();	// Get next to check for '-' (comment), or '['
		if (c == '-') {			// Probably a comment then '<!-'
			const char *szComment;

			c=rogxml_ReadChar();	// Take second hyphen
			if (c != '-') return rogxml_NewError(ROGXML_E_NOHYPHEN, "Expected '--' at start of comment");
			szComment = rogxml_ReadUntil("-->");
			if (!szComment)
				return rogxml_NewError(ROGXML_E_EOF, "End of comment never reached");
			if (!_rogxml_read_comments)
				g_bStripComments = 1;
			return rogxml_NewComment(szComment);	// Return it
		} else if (c == '[') {	// <![CDATA or similar
			szName=rogxml_ReadWord();				// Get the word then
			if (!strcmp(szName, "CDATA")) {			// CDATA section
				const char *szText;
				rogxml *rx;
				free(szName);						// Won't be needing it
				c=rogxml_ReadChar();
				if (c != '[') {						// <![CDATAx
					return rogxml_NewError(ROGXML_E_NOTBRA, "Expected '[' after '<![CDATA'");
				}
				szText=rogxml_ReadUntil("]]>");		// Get the text
				rx = rogxml_NewText(szText);
				rogxml_SetCData(rx);
				return rx;
			} else {
				free(szName);
				return rogxml_NewError(ROGXML_E_NOTIMPL, "Nothing other than CDATA accepted after '<!['");
			}
		} else {				// Perhaps <!DOCTYPE etc.
			const char *szText;

			rogxml_UnReadChar(c);
			szName=rogxml_ReadWord();
			szText=rogxml_ReadUntil(">");			// Finish the element
//printf("Quietly dumping '<!%s \"%s\">'\n", szName, szText);
			szDelete(szName);
			szDelete(szText);
			return rogxml_ReadElement(rxParent);	// Return the next one
		}
	} else if (c == '?') {		// Processing Instruction
		bIsProcessing = 1;		// Remember it's a PI and leave it skipped
	} else {
		rogxml_UnReadChar(c);	// Put it back as it's start of tag name
	}
	szName=rogxml_ReadWord();
	if (!szName) return rogxml_NewError(ROGXML_E_NONAME, "Expected root element name");
	rx = rogxml_NewElement(rxParent, szName);
	if (rogxml_ErrorNo(rx)) return rx;
	if (bIsProcessing)
		rx->nType = ROGXML_T_PI;
	free(szName);
	for (;;) {
		c=rogxml_ReadNonSpace();
		if (c == '/') {							// Empty element
			c=rogxml_ReadNonSpace();
			if (c == '>') {
				rogxml_ValidateNamespace(rx);
				return rx;
			} else {							// Expecting '/>'
				return rogxml_ReturnError(rx, ROGXML_E_NOTGT,
					"Expected '>'");
			}
		} else if (c == '?') {					// End of prolog
			if (!bIsProcessing)
				return rogxml_ReturnError(rx, ROGXML_E_NOTPI, "Found '?' in non-processing instruction");
			c=rogxml_ReadNonSpace();
			if (c == '>') {
				return rx;
			} else {							// Expecting '?>'
				return rogxml_ReturnError(rx, ROGXML_E_NOTGT,
					"Expected '>' after '?' in prolog");
			}
		} else if (c == '>') {					// End of start tag
			rogxml_ValidateNamespace(rx);
			if (rogxml_ErrorNo(rx)) return rx;
			for (;;) {
				c=rogxml_ReadNonSpace();		// This skips white space at start of element text
				if (c == '<') {
					c=rogxml_ReadNonSpace();
					if (c == '/') {
						const char *szEndTag=rogxml_ReadWord();
						if (!szEndTag)
#if 1
							szEndTag=strdup(rx->szName);		// </> - fake a proper close tag
#else
							return rogxml_ReturnError(rx, ROGXML_E_NONAME,
									"Missing closing tag for <%s>", rx->szName);
#endif
						if (strcmp(szEndTag, rogxml_GetFullName(rx))) {
							return rogxml_ReturnError(rx, ROGXML_E_MISNEST,
									"Mis-nested tags <%s>...</%s>",
										rogxml_GetFullName(rx), szEndTag);
						}
						szDelete(szEndTag);		// Leak if error above
						c=rogxml_ReadNonSpace();
						if (c == '>')
							return rx;			// We're done!!!
						return rogxml_ReturnError(rx, ROGXML_E_NOTGT,
									"Found '%c', expected '>' on close tag", c);
					} else {
						rogxml *rxChild;
						rogxml_UnReadChar(c);
						rxChild=rogxml_ReadElementX(rx);
						if (rogxml_ErrorNo(rxChild)) {	// Naughty child...
							rogxml_Delete(rx);
							return rxChild;
						}
						rogxml_LinkChild(rx, rxChild);
					}
				} else {						// Plain text...
					int nStart=_rogxml_lineno;	// In case it never ends
					char *szText=NULL;
					char *szCopyText;
					int nChars = 0;

					rogxml_UnReadChar(c);
					for (;;) {
						c=rogxml_ReadEscapedChar();
						if (c <= 0 || _rogxml_last_char=='<') break;
						if (nChars == 0) {
							szText=(char*)malloc(1024);
						} else if (nChars % 1024 == 0) {
							szText=(char*)realloc(szText, (size_t)(nChars+1024));
						}
						szText[nChars++]=c;
					}
					if (nChars) {										// Trim any trailing spaces
						while (nChars && isspace(szText[nChars-1]))
							nChars--;
					}
					// Create a nice, '\0' terminated copy
					szCopyText = (char*)malloc(nChars+1);
					if (nChars) {
						memcpy(szCopyText, szText, nChars);
						free(szText);
						szText=NULL;
					}
					szCopyText[nChars]='\0';
					szText=szCopyText;

					if (c <= 0) {
						if (szText) free(szText);
						if (c == 0)
							_rogxml_entity_error = "File prematurely ends";
						if (nStart == _rogxml_lineno) {
							return rogxml_ReturnError(rx, ROGXML_E_EOF, "%s", _rogxml_entity_error);
						} else {
							return rogxml_ReturnError(rx, ROGXML_E_EOF,
								"%s after text starting on line %d",
								_rogxml_entity_error, nStart);
						}
					}
					rogxml_UnReadChar(c);
					// Remove any trailing space
					if (szText && szText[strlen(szText)-1] == ' ')
						szText[strlen(szText)-1]='\0';
					rogxml_AddText(rx, szText);
					free(szText);
				}
			}
		} else {								// Must be an attribute then
			char *szAttr;
			const char *szValue;
			rogxml_UnReadChar(c);
			szAttr=rogxml_ReadWord();			// Get the name
			c=rogxml_ReadNonSpace();
			if (c != '=')
				return rogxml_ReturnError(rx, ROGXML_E_NOTEQ,
						"Missing '=' after attribute '%s'", szAttr);
			szValue=rogxml_ReadString();
			if (szValue == szReadStringError)
				return rogxml_ReturnError(rx, ROGXML_E_NOVALUE,
						"%s in attribute '%s'", szReadStringError, szAttr);
			if (!rogxml_AddAttr(rx, szAttr, szValue)) {
				return rx;
			}
			free(szAttr);
			szDelete(szValue);
		}
	}
}

API rogxml *rogxml_ReadElement(rogxml *rxParent)
//! \brief Read an element from a file opened using rogxml_OpenInputFile()
//! Reads an entire element (and any child elements) from a file.
//! This function is not generally used directly, rogxml_ReadFile() will
//! be used instead.  The difference is that rogxml_ReadFile() repeatedly
//! calls this function until it returns something other that a prolog
//! element.  This function will return the next element from the file
//! whether or not it is part of the prolog.
//! \param rxParent parent node from which to glean namespace info the
//! element read IS NOT linked to this element.
//! \return	The element read.  This should always be checked for being an
//! error using rogxml_ErrorNo().
//! If it is NULL then there was nothing in the input stream.
{
	char c;
	rogxml *rx;
	char bOldStripComments = g_bStripComments;	// I don't think it needs saving like this, but seems sensible

	g_bStripComments = 0;						// Global flag to strip comments

//	if (!_rogxml_fpin) return rogxml_NewError(ROGXML_E_NOTOPEN,"File not opened");

	int loop;									// Set if we want to drop the element and read another (unwanted comment)
	do {
		loop = 0;
		c=rogxml_ReadNonSpace();
		if (c == '<') {
			rx=rogxml_ReadElementX(rxParent);
		} else {
			if (c == -1) {
				rx=NULL;
			} else {
				if (c >= 20 && c <= 126) {
					rx=rogxml_NewError(ROGXML_E_NOTLT, "Expected '<', not '%c'", c);
				} else {
					rx=rogxml_NewError(ROGXML_E_NOTLT, "Expected '<', not 'char %d'", c);
				}
			}
		}
		if (rx && rx -> nType == ROGXML_T_COM && g_bStripComments) {
			rogxml_Delete(rx);
			loop=1;
		}
	} while (loop);

	if (rx) {
	   if (rogxml_ErrorNo(rx)) {				// Add line info to error
			char *szError=(char*)malloc(strlen(rx->szName)+20);

			sprintf(szError, "%s at line %d", rx->szName, _rogxml_lineno);
			free(rx->szName);
			rx->szName=szError;
	   } else if (g_bStripComments) {			// Check for comments and strip them
		   rogxml *rxTmp = rx;

		   while (rxTmp) {
			   rogxml *rxNext = rogxml_FindNextItem(rxTmp);

			   if (rxTmp -> nType == ROGXML_T_COM) {
					rxNext = rogxml_FindNextIfDeleted(rxTmp);
					rogxml_Delete(rxTmp);
			   }
			   rxTmp = rxNext;
		   }
	   }
	}

	g_bStripComments = bOldStripComments;

	return rx;
}

API int rogxml_OpenInputFile(const char *szFilename)
//! \brief Opens a file for input
//! To read raw (i.e. including prolog) elements from a file, use this
//! function to open the file then call rogxml_ReadElement() to read
//! each element.  Only one file can be open at a time so a second call
//! to this function will fail if rogxml_CloseInputFile() has not been called
//! between times.
//! If the filename starts with a '<' then it is actually taken to be
//! a pointer to some XML text and that will be used instead.
//! \return	0	- File opened ok
//! \return	1	- File failed to open (check errno)
//! \return	2	- File already open
{
	if (_rogxml_fpin || _rogxml_txin) return 2;

	if (*szFilename == '<') {					// Reading from memory
		_rogxml_txin = szFilename;
	} else {
		_rogxml_fpin=fopen(szFilename, "r");
	}
	if (!_rogxml_fpin && !_rogxml_txin) return 1;

	return 0;
}

API rogxml *rogxml_ReadFp(FILE *fp)
{
	rogxml *rxRoot;
	FILE *oldfp = _rogxml_fpin;

	_rogxml_fpin = fp;

	_rogxml_lineno=1;
	rxRoot = rogxml_NewRoot();
	for (;;) {
		rogxml *rx=rogxml_ReadElement(NULL);
		if (!rx) break;					// End of input
		if (rogxml_ErrorNo(rx)) {		// Error, arrange to return it
			rogxml_Delete(rxRoot);
			rxRoot=rx;
			break;
		}
		rogxml_LinkChild(rxRoot, rx);
	}

	_rogxml_fpin = oldfp;

	return rxRoot;
}

API rogxml *rogxml_ReadFile(const char *szFilename)
//! \brief Read an element from an external file
//! This is the general function used to read XML from an external file.
//! Any prolog elements are skipped and only the main element is returned.
//! \return rogxml*	The document in internal form.
//! This should ALWAYS be checked for errors by calling rogxml_ErrorNo().
{
	rogxml *rxRoot = NULL;
	int nErr;

	if (!strcmp(szFilename, "-")) {				// Stdin instead
		return rogxml_ReadFp(stdin);
	}

	nErr=rogxml_OpenInputFile(szFilename);
	if (nErr == 1)
		return rogxml_NewError(ROGXML_E_NOTOPEN, "Couldn't open file '%s'", szFilename);
	if (nErr == 2)
		return rogxml_NewError(ROGXML_E_OPEN, "Already reading from a file");

	rxRoot = rogxml_ReadFp(_rogxml_fpin);

	rogxml_CloseInputFile();

	return rxRoot;
}

API rogxml *rogxml_ReadFn(readfn_t *fn)
// Reads a single element (and all children) from a function.
// The function should take no parameters and return an INT, being each successive
// character, returning -1 for EOF.
{
	rogxml *rx=NULL;

	if (fn) {
		_rogxml_lineno=1;
		_rogxml_readfn = fn;
		rx = rogxml_ReadElement(NULL);
		_rogxml_readfn = NULL;
	}

	return rx;
}

API rogxml *rogxml_FindLastChild(rogxml *rx)
{
	if (!rx) return NULL;

	if (rx && rx->rxFirstChild)
		return rogxml_FindLastSibling(rx->rxFirstChild);
	else
		return NULL;
}

API rogxml *rogxml_FindParent(rogxml *rx)
//! \brief Find the parent element
//! Given a source element, returns the parent of that element.
//! \return	NULL	- Element passed was erroneous or is a root node
//! \return	rogxml*	- Parent of the passed element
{
	return rx ? rx->rxParent : NULL;
}

API rogxml *rogxml_FindFirstChild(rogxml *rx)
//! \brief Find the first child element
//! Given a source element, returns the first child of that element.
//! To find all the children of an element, call rogxml_FindFirstChild() then
//! repeatedly call rogxml_FindNextSibling() on the returned handle to get the
//! next until something returns NULL.
//! \return	NULL	- Element passed was erroneous or has no children
//! \return	rogxml*	- First child of the passed element
{
	return rx ? rx->rxFirstChild : NULL;
}

API rogxml *rogxml_FindNextSibling(rogxml *rx)
//! \brief Find the next sibling to this element
//! In layman's terms, this is the next brother or sister of the element
//! given.  I.e. the next element that has the same parent as the one
//! passed.
//! \sa rogxml_FindFirstChild()
//! \return	NULL	Element passed was erroneous or was the last child
//! \return	rogxml*	Next child element after the one passed
{
	return rx ? rx->rxNext : NULL;
}

API rogxml *rogxml_FindPrevSibling(rogxml *rx)
{
	rogxml *rxSelf = rx;				// Nicked from the 'XPath' code...

	if (rxSelf->rxParent) {
		rogxml *rx = rxSelf->rxParent->rxFirstChild;
		if (rx != rxSelf) {					// As long as we're not first child
			while (rx->rxNext != rxSelf)	// Find previous child
				rx=rx->rxNext;
			return rx;						// Return it
		}
	}

	return NULL;
}

API rogxml *rogxml_FindPrevItem(rogxml *rx)
{
	rogxml *rxSelf = rx;
	if (!rx)				return rx;				// Passed a NULL
	rx=rogxml_FindPrevSibling(rxSelf);
	if (rx) {
		while (rx->rxFirstChild) {					// Get the youngest
			rx=rogxml_FindLastChild(rx);			// descendant
		}
	} else {
		rx=rxSelf->rxParent;
	}

	return rx;
}

API rogxml *rogxml_FindNextIfDeleted(rogxml *rx)
//! \brief Find the item that will be after this one if this one is deleted.
//!
//! This is used if you are traversing the tree, deleting elements.
//! It gives the next sibling to this one or the parent's sibling if
//! there is one etc.
//!
//! \return	NULL	The element passed was erroneous or the last in the tree
//! \return	rogxml*	The next element on from the last one
{
	if (!rx)				return rx;					// Passed a NULL
	if (rx->rxNext)			return rx->rxNext;			// Or next sibling
	// Ok, we need to find the nearest uncle...
	while (rx) {
		rx=rx->rxParent;
		if (rx && rx->rxNext)		return rx->rxNext;	// Found an uncle
	}

	return NULL;										// Nobody left
}

API rogxml *rogxml_FindNextItem(rogxml *rx)
//! \brief Find the next item after the given element.
//!
//! This will be <u>ANY</u> element, including text.  The next element
//! is the first available in the following sequence:
//!   -# The first child of the element passed
//!   -# The next sibling to the element passed
//!   -# The next 'uncle' to the element passed
//! This last possibility means the sibling to the parent if there is one or
//! the next sibling to the grandparent etc.

//! The net result is that items will be returned in the order that they
//! would appear in an external file.
//! \return	NULL	The element passed was erroneous or the last in the tree
//! \return	rogxml*	The next element on from the last one
{
	if (!rx)				return rx;					// Passed a NULL
	if (rx->rxFirstChild)	return rx->rxFirstChild;	// Take first child
	if (rx->rxNext)			return rx->rxNext;			// Or next sibling
	if (rx->nType == ROGXML_T_ATTR) {					// Reached the end of a line of attributes
		rx=rx->rxParent;									// Back up to the containing element
		if (rx->rxFirstChild)	return rx->rxFirstChild;	// Take first child
		if (rx->rxNext)			return rx->rxNext;			// Or next sibling
	}
	// Ok, we need to find the nearest uncle...
	while (rx) {
		rx=rx->rxParent;
		if (rx && rx->rxNext)		return rx->rxNext;	// Found an uncle
	}

	return NULL;										// Nobody left
}

API rogxml *rogxml_FindNextElement(rogxml *rx)
//! \brief Find the next element after this one
//! Same as rogxml_FindNextItem() except that it only returns actual elements,
//! not text.
//! \return	NULL	- Passed element was error or named element not found
//! \return	rogxml*	- First matching element
{
	for (;;) {
		rx=rogxml_FindNextItem(rx);
		if (!rx || rx->nType == ROGXML_T_ELEM)
			return rx;
	}
}

API rogxml *rogxml_FindChild(rogxml *rx, const char *szName)
//! \brief Finds the named element amongst the immediate children of this one
//! Checks only the immediate children of this element.
//! \return	NULL	- Passed element was error or named element not found
//! \return	rogxml*	- First matching element
{
	if (rogxml_ErrorNo(rx) || !szName) return NULL;

	rx=rx->rxFirstChild;
	while (rx) {
		if (!strcmp(rx->szName, szName)) break;
		rx=rx->rxNext;
	}

	return rx;
}

API rogxml *rogxml_FindSelfOrSibling(rogxml *rx, const char *szName)
//! \brief Finds the named element amongst the siblings of this one
//! The search starts with the current node and continues
//! to the last one.  It <u>does not</u> wrap around to the first one
//! again when it finishes.
//! \return	NULL	- Passed element was error or named element not found
//! \return	rogxml*	- First matching element
{
	if (rogxml_ErrorNo(rx) || !szName) return NULL;

	while (rx) {
		if (!strcmp(rx->szName, szName)) break;
		rx=rx->rxNext;
	}

	return rx;
}

API rogxml *rogxml_FindSibling(rogxml *rx, const char *szName)
//! \brief Finds the named element amongst the siblings of this one
//! The search starts with the next sibling to this one and continues
//! to the last one.  It <u>does not</u> wrap around to the first one
//! again when it finishes.
//! \return	NULL	- Passed element was error or named element not found
//! \return	rogxml*	- First matching element
{
	return rx ? rogxml_FindSelfOrSibling(rx->rxNext, szName) : NULL;
}

API rogxml *rogxml_FindElement(rogxml *rx, const char *szName)
//! \brief Searches the tree from this point for the named element
//! This searches the tree starting at the element passed, <u>NOT</u>
//! the one after.  I.e., if you want to pick up all the elements in the
//! tree with a specific name, you'll need to call rogxml_FindNextElement() on
//! the returned value before passing it to this function.
//! The can be done with rogxml_FindElement(rogxml_FindNextElement(rx), "name"))
//! safely as this function will simply return with a NULL if one is passed.
//! \return	NULL	- Passed element was error or named element not found
//! \return	rogxml*	- First matching element
{
	if (rogxml_ErrorNo(rx)) return NULL;

	while (rx) {
		if (!strcmp(rx->szName, szName)) break;
		rx=rogxml_FindNextElement(rx);
	}

	return rx;
}

API int rogxml_GetElementDepth(rogxml *rx)
//! \brief Returns how deep this element is down the tree
//! \return 0 - rx is invalid or an error
//! \return 0... - Depth where 0 indicates the root element
{
	int nCount=0;

	if (rogxml_ErrorNo(rx)) return 0;

	rogxml_FindRoot(rx);					// Make sure there's a root node

	while (rx) {
		nCount++;
		rx=rx->rxParent;
	}

	return nCount-1;						// Don't count the root node
}

API const char *rogxml_GetElementText(rogxml *rx)
//! \brief Returns the plain text held within the element.
//! The text will be returned only if it is simple, i.e. it doesn't contain
//! any child elements.
//! For example, &lt;title>Magical Mystery Tour&lt;/title> will return the
//! contained text but &lt;title>An &lt;u>underlined&lt;/u> word&lt;/title> will return
//! NULL.
//! \return	char*	The text in the element
//! \return	NULL	Element passed was erroneous or doesn't contain simple text
{
	if (rx && rx->nType == ROGXML_T_ELEM) {		// Normal element
		if (rx->rxFirstChild && rx->rxFirstChild->rxNext == NULL) {	// 1 kid
			rx=rx->rxFirstChild;
			if (rx->nType == ROGXML_T_TEXT)		// That only child is text
				return rx->szValue;				// Return it!
		}
	}
	return NULL;
}

API rogxml *rogxml_FindFirstAttribute(rogxml *rx)
{
	return rx ? rx ->aAttrs : NULL;
}

API const char* rogxml_GetAttributeValue(rogxml *rx, const char *szName)
//! \brief Returns the value of a named attribute.
//! The value, less the quotes is returned.  If the attribute is not found,
//! NULL is returned to the difference between an omitted and blank attribute
//! is easy to distinguish.
//! \return	char*	Value of the attribute
//! \return NULL	rx is erroneous or doesn't contain the attribute
{
	if (rx) {
		rogxml *a=rx->aAttrs;
		while (a) {
			if (!strcmp(a->szName, szName))
				return a->szValue;
			a=a->rxNext;
		}
	}

	return NULL;
}

API rogxml *rogxml_FindFirstSibling(rogxml *rx)
// Gets my eldest brother
{
	if (rogxml_ErrorNo(rx)) return rx;

	switch (rx->nType) {
	case ROGXML_T_ATTR: return rx->rxParent->aAttrs;
	case ROGXML_T_NS:	return rx->rxParent->nsList;
	case ROGXML_T_TEXT:
	case ROGXML_T_ELEM: return rx->rxParent->rxFirstChild;
	default:			return NULL;
	}
}

API rogxml *rogxml_ReplaceElement(rogxml *rx, rogxml *rxNew)
// Replaces the element (and all children) with a new one.
// Don't call this with an attribute or namespace, it'll go wrong.
// Returns the old one so it can be deleted etc. if necessary.
{
	rogxml *rxParent;
	rogxml *rxElder;				// Siblings

	if (!rx) return rx;				// Replacing a NULL with something doesn't achieve much
	if (!rxNew) {					// Same as Unlink
		rogxml_Unlink(rx);
		return rx;
	}

	rxParent = rx->rxParent;
	if (!rxParent) return rx;		// If this is the root node, we have nothing to do.

	rxElder = rogxml_FindPrevSibling(rx);			// Find our older brother
	rogxml_Unlink(rx);								// Take us away

	if (rxElder)
		rogxml_LinkSibling(rxElder, rxNew);			// Add it to our brother if we have one
	else
		rogxml_LinkFirstChild(rxParent, rxNew);		// Otherwise, we're a first born

	return rx;										// Send the orphan back
}

API int rogxml_GetSiblingNumber(rogxml *rx)
// Tells me which sibling I am
// Feakily, this works with name spaces and attributes.
// If it isn't a namespace, attribute, normal element or text element, it
// returns 1.
{
	rogxml *rxTmp=rogxml_FindFirstSibling(rx);
	int nCount=1;

	while (rxTmp && rxTmp != rx) {
		nCount++;
		rxTmp=rxTmp->rxNext;
	}

	return nCount;
}

API int rogxml_GetNamedSiblingNumber(rogxml *rx)
// Tells me which sibling I am of the ones with my name
{
	rogxml *rxTmp=rogxml_FindFirstSibling(rx);
	int nCount=1;

	while (rxTmp && rxTmp != rx) {
		if (rxTmp->nsName == rx->nsName &&
				!strcmp(rxTmp->szName, rx->szName))
			nCount++;
		rxTmp=rxTmp->rxNext;
	}

	return nCount;
}

API int rogxml_GetSiblingCount(rogxml *rx)
// Tells me I'm a sibling out of how many kids
{
	rogxml *rxTmp=rogxml_FindFirstSibling(rx);
	int nCount=0;

	while (rxTmp) {
		nCount++;
		rxTmp=rxTmp->rxNext;
	}

	return nCount;
}

API int rogxml_GetNamedSiblingCount(rogxml *rx)
// Tells me how many of my siblings have my name (including me)
{
	rogxml *rxTmp=rogxml_FindFirstSibling(rx);
	int nCount=0;

	while (rxTmp) {
		if (rxTmp->nsName == rx->nsName &&
				!strcmp(rxTmp->szName, rx->szName))
			nCount++;
		rxTmp=rxTmp->rxNext;
	}

	return nCount;
}

API const char *rogxml_GetXPathH(rogxml *rx)
//! \brief Returns the full path of the element given.
//! This is, for example, '/COLLECTION/CD/TITLE'.  Note that passing this
//! back to rogxml_GetElement() <u>does not</u> gaurantee that the same
//! element will be returned as rogxml_GetElement() will return the first
//! match.
//! \return	char*	- The address of this element (MUST BE free()'d!!)
//! \return Strange but recognisable results - rx was NULL or erroneous
{
	const char *szParent;
	const char *szName;
	char *szResult;
	char buf[1000];				// V. surprised if this fails...
	int nSiblings;

	if (!rx) return strdup("{NULL}");
	if (rogxml_IsRoot(rx)) return strdup("/");

// If you have a parent and it's not the root node, fetch it
	if (rx->rxParent && !rogxml_IsRoot(rx->rxParent)) {
		szParent=rogxml_GetXPathH(rx->rxParent);
	} else {
		szParent=strdup("");
	}

	szName=rogxml_GetFullName(rx);
	nSiblings=rogxml_GetNamedSiblingCount(rx);
	if (nSiblings == 1) {
		sprintf(buf, "%s%s", rogxml_IsAttr(rx)?"@":"", szName);
	} else {
		int nSibling=rogxml_GetNamedSiblingNumber(rx);
		sprintf(buf, "%s[%d]", szName, nSibling);
	}
	szResult=(char*)malloc(strlen(szParent)+strlen(buf)+2);
	sprintf(szResult, "%s/%s", szParent, buf);
	szDelete(szParent);

	return szResult;
}

STATIC rogxitem *rogxitem_New(rogxml *rx)
{
	rogxitem *it=(rogxitem*)malloc(sizeof(rogxitem));

	it->rxElement = rx;
	it->iNext=NULL;
	it->iPrev=NULL;

	return it;
}

STATIC void rogxitem_Delete(rogxitem *it)
{
	if (it) {
		free(it);
	}
}

STATIC rogxlist *rogxlist_New()
{
	rogxlist *rxl = (rogxlist*)malloc(sizeof(rogxlist));

	rxl->nSize=0;
	rxl->iFirst=NULL;
	rxl->iLast=NULL;

	return rxl;
}

STATIC void rogxlist_Delete(rogxlist *rxl)
{
	if (rxl) {
		rogxitem *it=rxl->iFirst;

		while (it) {
			rogxitem *iNext=it->iNext;
			rogxitem_Delete(it);
			it=iNext;
		}
		free(rxl);
	}
}

STATIC void rogxlist_Remove(rogxlist *rxl, rogxitem *it)
// Removes an item from the list and deletes it
{
	if (rxl->iFirst == it) rxl->iFirst=it->iNext;
	if (rxl->iLast == it) rxl->iLast=it->iPrev;
	if (it->iNext) it->iNext->iPrev = it->iPrev;
	if (it->iPrev) it->iPrev->iNext = it->iNext;
	rogxitem_Delete(it);
}

STATIC rogxitem *rogxlist_FindElement(rogxlist *rxl, rogxml *rx)
// Finds an element in the list, returns a pointer to it or NULL
{
	rogxitem *it=rxl->iFirst;

	while (it) {
		if (it->rxElement == rx)
			break;
		it=it->iNext;
	}

	return it;
}

STATIC int rogxlist_AddItem(rogxlist *rxl, rogxitem *it)
// Adds an item to the list - this MUST be the last item so that any
//                                 ----
// synchronised array will stay in sync when two things are added
// in parallel.
{
	it->iNext=NULL;					// Nothing after this item
	if (rxl->iLast) {				// Something in list
		it->iPrev=rxl->iLast;		//   Before us is the previous last
		rxl->iLast->iNext=it;		//   We are after the previous last
	} else {						// New list
		it->iPrev=NULL;				//   Nothing before us
		rxl->iFirst=it;				//   We are first
	}
	rxl->iLast=it;					// New last is us
	rxl->nSize++;					// We have one more item

	return 1;
}

STATIC int rogxlist_AddElement(rogxlist *rxl, rogxml *rx)
{
	rogxitem *it=rogxitem_New(rx);
	return rogxlist_AddItem(rxl, it);
}

STATIC int rogxlist_AddElementUnique(rogxlist *rxl, rogxml *rx)
{
	rogxitem *it=rogxlist_FindElement(rxl, rx);
	if (it) return 0;

	return rogxlist_AddElement(rxl, rx);
}

STATIC rogxitem *rogxlist_GetItem(rogxlist *rxl, int nIndex)
// Returns an item from the list given its index
{
	rogxitem *it;

	if (nIndex < 1 || nIndex > rxl->nSize) return NULL;

	it=rxl->iFirst;
	while (--nIndex > 0) it=it->iNext;

	return it;
}

STATIC void rogxlist_RemoveAllBut(rogxlist *rxl, rogxitem *it)
// Removes all items except the one given from the list
// If 'it' is NULL or not in the list, removes all elements
{
	rogxitem *iRover=rxl->iFirst;
	int bFound = 0;

	while (iRover) {
		rogxitem *iNext=iRover->iNext;
		if (iRover == it) {
			bFound=1;
		} else {
			rogxitem_Delete(iRover);
		}
		iRover=iNext;
	}

	if (bFound) {
		it->iPrev=NULL;
		it->iNext=NULL;
		rxl->iFirst=it;
		rxl->iLast=it;
		rxl->nSize=1;
	} else {
		rxl->iFirst=NULL;
		rxl->iLast=NULL;
		rxl->nSize=0;
	}
}

// There is a problem in using XPaths where namespaces are involved.  If the author of the XML has a
// different idea of namespaces names, but uses the same URI, it's impossible to navigate.  E.g.
// <a:Alpha xmlns:a="fred">
//   <a:jim/>
// </a:Alpha>
// If you don't know that 'a' has been used to represent the 'fred' namespace, you can't find the
// 'jim' element by using '/a:Alpha/a:jim'.  Therefore, we need to tell the XPath code what URI we
// mean when we talk about the namespaces.  So, to find the above element, we can use:
// rogxpath_AddNamespace("b","fred");
// rogxpath_New(rx, "/b:Alpha/b:jim");

// Both of the following are like ARGV
STATIC char **_rogxpath_Namespace = NULL;
STATIC char **_rogxpath_URI = NULL;

API int rogxpath_FindNamespace(const char *szName)
// Returns the number of the namespace found (1..n) or 0 if not found
{
	char **ns=_rogxpath_Namespace;
	int nCount=1;

	if (!ns) return 0;

	while (*ns) {
		if (!strcmp(*ns, szName))
			return nCount;
		ns++;
		nCount++;
	}

	return 0;
}

API void rogxpath_AddNamespace(const char *szName, const char *szURI)
// Adds a namespace to rogxpath's knowledge.
// Adding a NULL szURI will create a namespace that explicitly matches no namespace.
// E.g. Searching for //fred but will find <a:fred> and <fred> but
// rogxpath_AddNamespace("blank",NULL) will mean that //blank:fred will only find <fred>
{
	int i=rogxpath_FindNamespace(szName);
	char **ns;
	int nCount;

	if (i) {												// Alread exists
		szDelete(_rogxpath_URI[i-1]);
		_rogxpath_URI[i-1]=szURI?strdup(szURI):NULL;
		return;
	}

	if (_rogxpath_Namespace) {								// No namespaces yet, create arrays
		_rogxpath_Namespace=NEW(char*,1);
		_rogxpath_Namespace[0]=NULL;
		_rogxpath_URI=NEW(char*,1);
		_rogxpath_URI[0]=NULL;
	}

	ns=_rogxpath_Namespace;
	while (*ns) ns++;
	nCount=ns-_rogxpath_Namespace;							// Number already recorded

	RENEW(_rogxpath_Namespace, char*, nCount+1);
	_rogxpath_Namespace[nCount]=strdup(szName);
	_rogxpath_Namespace[nCount+1]=NULL;

	RENEW(_rogxpath_URI, char*, nCount+1);
	_rogxpath_URI[nCount]=szURI ? strdup(szURI) : NULL;
	_rogxpath_URI[nCount+1]=NULL;

	return;
}

API void rogxpath_ForgetNamespaces()
{
	if (_rogxpath_Namespace) {
		int i=0;

		while (_rogxpath_Namespace[i]) {
			szDelete(_rogxpath_Namespace[i]);
			szDelete(_rogxpath_URI[i]);
		}
		free((char*)_rogxpath_Namespace);
		free((char*)_rogxpath_URI);
		_rogxpath_Namespace=NULL;
		_rogxpath_URI=NULL;
	}
}

STATIC rogxpath *rogxpath_NewList(rogxlist *rxl)
// Create a new XPath, initialised from the items in the list or
// empty if the list is NULL.
{
	rogxitem *it;
	rogxpath *rxp=(rogxpath*)malloc(sizeof(rogxpath));

	rxp->lList=rogxlist_New();
	rxp->nSize=0;
	if (rxl) {
		rxp->prxElement=(rogxml**)malloc(rxl->nSize*sizeof(rogxml*));
		for (it=rxl->iFirst;it;it=it->iNext) {
			if (rogxlist_AddElementUnique(rxp->lList, it->rxElement))
				rxp->prxElement[rxp->nSize++]=it->rxElement;
		}
	} else {
		rxp->prxElement=NULL;
	}

	return rxp;
}

API void rogxpath_Delete(rogxpath *rxp)
{
	if (rxp) {
		rogxlist_Delete(rxp->lList);
		if (rxp->prxElement) free(rxp->prxElement);
		free(rxp);
	}
}

API int rogxpath_GetCount(rogxpath *rxp)
{
	return rxp ? rxp->nSize : 0;
}

API rogxml *rogxpath_GetElement(rogxpath *rxp, int nElement)
{
	if (nElement < 1 || nElement > rogxpath_GetCount(rxp)) {
		return NULL;
	} else {
		return rxp->prxElement[nElement-1];
	}
}

API int MatchingElement(rogxml *rx, const char *szTest)
// Returns 1 if the element name matches the test, taking into consideration any namespace
// definitions that have been added.
{
	const char *szTestLocal;
	char *szTestNamespace = SplitName(szTest, &szTestLocal);

//printf("Matching '%s' against '%s' - ", rogxml_GetFullName(rx), szTest);
	if (strcmp(szTestLocal, rogxml_GetLocalName(rx))) {		// Localname doesn't match
		szDelete(szTestNamespace);
//printf("No\n");
		return 0;
	}

	if (!szTestNamespace) {				// No namespace in test
//printf("Yes\n");
		return 1;
	} else {							// Namespace part to match
		int i=rogxpath_FindNamespace(szTestNamespace);
		if (i) {						// A known namespace
			const char *szURI=rogxml_GetURI(rx);

			szDelete(szTestNamespace);

//printf("strcmp(%s,%s)=%d\n",szURI, _rogxpath_URI[i-1], strcmp(szURI, _rogxpath_URI[i-1]));
			return !strcmp(szURI, _rogxpath_URI[i-1]);
		} else {						// Referencing one we don't so use names not URI
			int nResult = !strcmp(rogxml_GetNamespace(rx), szTestNamespace);

			szDelete(szTestNamespace);

//printf("nResult = %d\n", nResult);
			return nResult;
		}
	}
}

STATIC int AddIfTest(rogxlist *l, rogxml *rx, const char *szTest)
{
	const char *chp;
	int nAdd=0;							// Make 1 to add the element

	if (!rx) return 0;					// Ignore nulls (no kids etc.)
	if (rx->nType == ROGXML_T_PI)
		return 0;						// Ignore processing instructions

	chp=strchr(szTest, '(');			// Test if function
	if (!chp &&							// Not a function
			(*szTest == '*' ||			// and Wildcard
//			!strcmp(szTest, rogxml_GetFullName(rx)))) {		// or matches
			MatchingElement(rx, szTest))) {		// or matches
		nAdd=1;
	} else if (chp) {
		char *szFn=rogxml_AddHeapTextLen(NULL, szTest, chp-szTest);
		if (!strcmp(szFn, "node")) {
			nAdd=1;
		} else if (!strcmp(szFn, "attribute")) {
			if (rogxml_IsAttr(rx))
				nAdd=1;
		} else if (!strcmp(szFn, "namespace")) {
			if (rogxml_IsNamespace(rx))
				nAdd=1;
		} else {
			rogxlist_AddElement(l, rogxml_NewError(ROGXML_E_BADPATHFN,"Path function '%s' not known", szFn));
		}

		free(szFn);
	} else {							// No match
		nAdd=0;
	}

	if (nAdd) rogxlist_AddElement(l, rx);

	return nAdd;
}

STATIC rogxlist *TakeOneStep(rogxlist *lOut, rogxml *rxSelf, const char *szAxis, const char *szTest, int nPos, int nSize)
{
	rogxml *rx;

	if (!*szAxis) {											// root
		rogxlist_AddElement(lOut, rogxml_FindRoot(rxSelf));
	} else if (!strcmp(szAxis, "child")) {					// CHILD
		rx=rxSelf->rxFirstChild;
		while (rx) {
			AddIfTest(lOut, rx, szTest);
			rx=rx->rxNext;
		}
	} else if (!strcmp(szAxis, "descendant")) {				// DESCENDANT
		rx=rxSelf->rxFirstChild;
		while (rx && rx != rxSelf) {
			AddIfTest(lOut, rx, szTest);
			rx=rogxml_FindNextItem(rx);
		}
	} else if (!strcmp(szAxis, "parent")) {
			AddIfTest(lOut, rxSelf->rxParent, szTest);
	} else if (!strcmp(szAxis, "ancestor")) {
		rx=rxSelf->rxParent;
		while (rx) {
			AddIfTest(lOut, rx, szTest);
			rx=rx->rxParent;
		}
	} else if (!strcmp(szAxis, "following-sibling")) {
		rx=rxSelf->rxNext;
		while (rx) {
			AddIfTest(lOut, rx, szTest);
			rx=rx->rxNext;
		}
	} else if (!strcmp(szAxis, "preceding-sibling")) {
		if (rxSelf->rxParent) {
			rogxml *rxTmp=rxSelf;
			rogxml *rxFirstChild = rxSelf->rxParent->rxFirstChild;
			for (;;) {
				rx=rxFirstChild;					// Start with first child
				if (rx == rxTmp) break;				// Found ourselves
				while (rx->rxNext != rxTmp)			// Find previous child
					rx=rx->rxNext;
				AddIfTest(lOut, rx, szTest);		// Add them in
				rxTmp=rx;							// Step back a child
			}
		}
	} else if (!strcmp(szAxis, "following")) {
		rx=rogxml_FindNextItem(rxSelf);
		while (rx) {
			AddIfTest(lOut, rx, szTest);
			rx=rogxml_FindNextItem(rx);
		}
	} else if (!strcmp(szAxis, "preceding")) {
		rx=rogxml_FindPrevItem(rxSelf);
		while (rx) {
			AddIfTest(lOut, rx, szTest);
			rx=rogxml_FindPrevItem(rx);
		}
	} else if (!strcmp(szAxis, "attribute")) {
		rx=rxSelf->aAttrs;
		while (rx) {
			AddIfTest(lOut, rx, szTest);
			rx=rx->rxNext;
		}
	} else if (!strcmp(szAxis, "namespace")) {
		rx=rxSelf->nsList;
		while (rx) {
			AddIfTest(lOut, rx, szTest);
			rx=rx->rxNext;
		}
	} else if (!strcmp(szAxis, "self")) {
		AddIfTest(lOut, rxSelf, szTest);
	} else if (!strcmp(szAxis, "descendant-or-self")) {
		TakeOneStep(lOut, rxSelf, "self", szTest, nPos, nSize);
		TakeOneStep(lOut, rxSelf, "descendant", szTest, nPos, nSize);
	} else if (!strcmp(szAxis, "ancestor-or-self")) {
		TakeOneStep(lOut, rxSelf, "self", szTest, nPos, nSize);
		TakeOneStep(lOut, rxSelf, "ancestor", szTest, nPos, nSize);
	} else {
		rogxlist_AddElement(lOut, rogxml_NewError(ROGXML_E_BADAXIS, "Unknown axis '%s'", szAxis));
	}

//rogxlist_Dump(lOut);
	return lOut;
}

STATIC void ApplyOnePredicate(rogxlist *lList, const char *szPredicate)
// Applies the single predicate to the list given.
{
	int nIndex=atol(szPredicate);

	if (nIndex) {
		rogxitem *it=rogxlist_GetItem(lList, nIndex);
		rogxlist_RemoveAllBut(lList, it);
	}
}

STATIC void ApplyPredicates(rogxlist *lList, const char *szPredicates)
// Currently, simply check for [2] etc.
{
	while (*szPredicates) {
		const char *chp=strchr(szPredicates, ']');
		if (chp) {
			int len=chp-szPredicates-1;			// Chars inside '[...]'
			char *szPred=(char*)malloc(len+1);
			memcpy(szPred, szPredicates+1, len);
			szPred[len]='\0';
			ApplyOnePredicate(lList, szPred);
			szPredicates=SkipSpace(chp+1);
		} else {
			break;						// No ']' - odd...
		}
	}
}

STATIC rogxlist *TakeStep(rogxlist *lIn, const char *szAxis, const char *szTest)
{
	rogxitem *it=lIn->iFirst;
	rogxlist *lOut=rogxlist_New();
	int nPos=1;
	int nSize=lIn->nSize;

	while (it) {
		TakeOneStep(lOut, it->rxElement, szAxis, szTest, nPos, nSize);
		it=it->iNext;
		nPos++;
	}

	return lOut;
}

STATIC int SplitStep(const char *szPath, char **pszAxis, char **pszTest, char **pszPredicates)
// Takes a string of the form axis::test[pred1][pred1]...
// Returns three pointers, one to the axis, one to the test and one to the
// predicates (or end of string).
// All the returned 'things' will need to be free()'d when done
// Returns	1	Ok
//			2	Selecting root (/), Axis, Test and Preds all blank
//			0	Error
{
	const char *chp;
	int len;
	szPath=SkipSpace(szPath);

	if (!*szPath) {								// Blank - must be doc root
		*pszAxis=strdup("");
		*pszTest=strdup("");
		*pszPredicates=strdup("");
		return 2;
	}

	if (!strcmp(szPath, "..")) {				// Special case for parent
		*pszAxis=strdup("parent");
		*pszTest=strdup("*");
		*pszPredicates=strdup("");
		return 1;
	}

	if (!strcmp(szPath, ".")) {					// Special case for self
		*pszAxis=strdup("self");
		*pszTest=strdup("node()");
		*pszPredicates=strdup("");
		return 1;
	}

	chp=strchr(szPath, ':');
	if (chp && chp[1] == ':') {					// Found our '::'
		len=chp-szPath;							// Length of axis
		*pszAxis=(char*)malloc(len+1);
		memcpy(*pszAxis, szPath, len);
		(*pszAxis)[len]='\0';
		szPath=chp+2;
	} else {
		if (*szPath=='@') {						// Attribute
			*pszAxis=strdup("attribute");
			szPath++;
		} else {
			*pszAxis=strdup("child");
		}
	}
	chp=strchr(szPath+1, '[');			// Look for first predicate
	if (!chp) chp=szPath+strlen(szPath);		// EOS if not there
	len=chp-szPath;
	*pszTest=(char*)malloc(len+1);
	memcpy(*pszTest, szPath, len);
	(*pszTest)[len]='\0';
	*pszPredicates=strdup(chp);					// Copy of last part

	return 1;
}

STATIC char *GetStep(char **pszPath)
// Gets the next step from the path given, pushing the path onto the next
// place.
// Returns	char*	Pointer to next path
//			NULL	No more paths to get
{
	char *szStep=(char *)SkipSpace(*pszPath);
	char *chp=strchr(szStep, '/');

	if (!*szStep) return NULL;		// Blank passed in

	if (chp) {				// Another path after this one
		*chp='\0';			// Terminate here
		*pszPath=chp+1;
	} else {
		*pszPath=szStep+strlen(szStep);
	}

	return szStep;
}

STATIC char *StrRepl(const char *szStr, const char *szSearch, const char *szReplace)
{
	const char *chp;
	char *szResult=NULL;

	while ((chp=strstr(szStr, szSearch))) {
		szResult=rogxml_AddHeapTextLen(szResult, szStr, chp-szStr);
		szResult=rogxml_AddHeapText(szResult, szReplace);
		szStr=chp+strlen(szSearch);
	}

	return rogxml_AddHeapText(szResult, szStr);
}

STATIC char *UnabbrevXPath(const char *szPath)
// Turns an abbreviated path into a full one
// Result is returned on the stack so must be free()'d later
{
	char *szResult;

	szResult=StrRepl(szPath, "//", "/descendant-or-self::node()/");

	return szResult;
}

STATIC rogxpath *rogxpath_NewXPathUnabbrev(rogxml *rx, char *szPath)
{
	rogxpath *rxp;
	rogxlist *rxl=rogxlist_New();

	if (rx) {
		rogxlist_AddElement(rxl, rx);		// Start with our current position
		for (;;) {
			char *szAxis;
			char *szTest;
			char *szPredicates;
			char *szStep;
			rogxlist *rxlTmp;				// Temp holding

			szStep=GetStep(&szPath);			// Get step text
			if (!szStep) break;					// No more steps, so drop out
			SplitStep(szStep, &szAxis, &szTest, &szPredicates);	// Split up
	//printf("Split '%s' to '%s::%s[%s]'\n", szStep, szAxis, szTest, szPredicates);
			rxlTmp=TakeStep(rxl, szAxis, szTest);	// Apply main step part
			rogxlist_Delete(rxl);					// Delete the old list
			ApplyPredicates(rxlTmp, szPredicates);	// Maybe prune list
			rxl=rxlTmp;								// Set for next time round

			free(szAxis);						// Clear out debris
			free(szTest);
			free(szPredicates);
		}
	}

	rxp=rogxpath_NewList(rxl);
	rogxlist_Delete(rxl);					// Forget the list

	return rxp;								// Return clean, path list
}

API rogxpath *rogxpath_New(rogxml *rx, const char *szPath)
{
	rogxpath *rxp;
	char *szTempPath;

	if (!rx || !szPath) {					// Just a plain, empty path wanted
		rxp = rogxpath_NewList(NULL);
	}

	szTempPath=UnabbrevXPath(szPath);

	rxp=rogxpath_NewXPathUnabbrev(rx, szTempPath);
	free(szTempPath);

	return rxp;
}

API int rogxpath_FindElement(rogxpath *rxp, rogxml *rx)
// Returns the index into the list where the element occurs.
// 0 if it doesn't
{
	int i;

	if (rxp) {
		for (i=0;i<rxp->nSize;i++)
			if (rxp->prxElement[i] == rx)
				return i+1;
	}

	return 0;
}

API int rogxpath_AddElement(rogxpath *rxp, rogxml *rx)
// Adds an element to an xpath
// Returns index of element if the element wasn't there
// already (and is therefore added), otherwise 0.
{
	if (rxp && rogxlist_AddElementUnique(rxp->lList, rx)) {
		if (rxp->prxElement) {
			rxp->prxElement=(rogxml**)realloc(rxp->prxElement,
											(rxp->nSize+1)*sizeof(rogxml*));
		} else {
			rxp->prxElement=(rogxml**)malloc((rxp->nSize+1)*sizeof(rogxml*));
		}
		rxp->prxElement[rxp->nSize++]=rx;
		return rxp->nSize;
	} else {
		return 0;
	}
}

API int rogxpath_DelElement(rogxpath *rxp, rogxml *rx)
// Removes an element from an xpath
// Returns old element number if the element was found (and removed),
// otherwise 0.
{
	rogxitem *it;

	if (!rxp) return 0;

	it=rogxlist_FindElement(rxp->lList, rx);
	if (it) {
		int i = rogxpath_FindElement(rxp, rx);
		rogxlist_Remove(rxp->lList, it);
		if (i < rxp->nSize)
			memmove(&rxp->prxElement[i-1], &rxp->prxElement[i],
							(rxp->nSize-i)*sizeof(*rxp->prxElement));
		rxp->nSize--;
		return i;
	} else {
		return 0;
	}
}

API rogxml *rogxml_FindByPath(rogxml *rx, const char *szAddress)
//! \brief uses the XPath given to find an element.
//! If none is found, we get a NULL back otherwise we get the
//! first matching element.
//! \return	rogxml*	Pointer first matching element
//! \return	NULL	No matches found or rx is erroneous
{
	rogxpath *rxp=rogxpath_New(rx, szAddress);
	rogxml *rxFound = NULL;

	if (rogxpath_GetCount(rxp) >= 1) {
		rxFound=rogxpath_GetElement(rxp, 1);
	}

	rogxpath_Delete(rxp);

	return rxFound;
}

STATIC void rogxitem_Dump(rogxitem *it)
{
	if (_rogxml_fpdump ==NULL) {
		_rogxml_fpdump = stdout;  
	}   

        if (!it) {
		fprintf(_rogxml_fpdump, "Item = NULL\n");
		return;
	}

	rogxml_DumpRaw(it->rxElement);
}

STATIC void rogxlist_Dump(rogxlist *rxl)
{
	rogxitem *it;
	int nPos=1;

        if (_rogxml_fpdump ==NULL) {
		_rogxml_fpdump = stdout;
	}

	if (!rxl) {
		fprintf(_rogxml_fpdump, "XList = NULL\n");
		return;
	}

	fprintf(_rogxml_fpdump, "Items = %d (in %p)\n", rxl->nSize, rxl);
	for (it=rxl->iFirst;it;it=it->iNext) {
		fprintf(_rogxml_fpdump, "%d: ", nPos++);
		rogxitem_Dump(it);
	}
}

API void rogxpath_Dump(rogxpath *rxp)
{
	if (_rogxml_fpdump ==NULL) {
		_rogxml_fpdump = stdout;
	}

	if (!rxp) {
		fprintf(_rogxml_fpdump, "XPath = NULL\n");
		return;
	}

	fprintf(_rogxml_fpdump, "List = %p\n", rxp->lList);
	rogxlist_Dump(rxp->lList);
}

API void rogxml_SetDump(FILE *fp)
{
	if (!fp) fp=stdout;
	_rogxml_fpdump = fp;
}

API rogxml *rogxml_AddChildCommentf(rogxml *rx, const char *szFmt, ...)
// As for AddChildComment only accepts a format string
{
	char buf[1024];

	va_list ap;
	va_start(ap, szFmt);
	vsnprintf(buf, sizeof(buf), szFmt, ap);
	va_end(ap);

	return rogxml_AddChildComment(rx, buf);
}

API rogxml *rogxml_AddSiblingCommentf(rogxml *rx, const char *szFmt, ...)
// As for AddSiblingComment only accepts a format string
{
	char buf[1024];

	va_list ap;
	va_start(ap, szFmt);
	vsnprintf(buf, sizeof(buf), szFmt, ap);
	va_end(ap);

	return rogxml_AddSiblingComment(rx, buf);
}

API rogxml *rogxml_SetAttrf(rogxml *rx, const char *szName, const char *szFmt, ...)
// As SetAttr only accepts a format string
{
	char buf[1024];

	va_list ap;
	va_start(ap, szFmt);
	vsnprintf(buf, sizeof(buf), szFmt, ap);
	va_end(ap);

	return rogxml_SetAttr(rx, szName, buf);
}

API rogxml *rogxml_AddTextf(rogxml *rx, const char *szFmt, ...)
// As for AddText only accepts a format string
{
	char buf[1024];

	va_list ap;
	va_start(ap, szFmt);
	vsnprintf(buf, sizeof(buf), szFmt, ap);
	va_end(ap);

	return rogxml_AddText(rx, buf);
}

API rogxml *rogxml_SetTextf(rogxml *rx, const char *szFmt, ...)
// As for SetText only accepts a format string
{
	char buf[1024];

	va_list ap;
	va_start(ap, szFmt);
	vsnprintf(buf, sizeof(buf), szFmt, ap);
	va_end(ap);

	return rogxml_SetText(rx, buf);
}

API rogxml *rogxml_AddTextChildf(rogxml *rx, const char *szName, const char *szFmt, ...)
// As for AddTextChild only accepts a format string
{
	char buf[1024];

	va_list ap;
	va_start(ap, szFmt);
	vsnprintf(buf, sizeof(buf), szFmt, ap);
	va_end(ap);

	return rogxml_AddTextChild(rx, szName, buf);
}

API void rogxml_WalkPrefix(rogxml *rx, const char *szPrefix, rogxmlWalkFn *fn)
// Walks the tree, picking out all the elements whose name starts with 'szPrefix' and
// calls 'fn' with each one.  If 'fn' returns non-NULL then the existing branch
// is replaced with the one returned, otherwise it is left.
// If szPrefix is NULL then all elements are passed.
{
	int nPrefixLen = szPrefix ? strlen(szPrefix) : 0;

	while (rx) {
		rogxml *rxReplace;

		if (nPrefixLen && !strncmp(szPrefix, rogxml_GetLocalName(rx), nPrefixLen)) {
			rxReplace = (*fn)(rx);
		} else {
			rxReplace = NULL;
		}

		if (rxReplace) {
			rogxml_ReplaceElement(rx, rxReplace);
			rx=rogxml_FindNextIfDeleted(rxReplace);		// Takes the next, skipping any children of rxReplace
		} else {
			rx=rogxml_FindNextElement(rx);
		}
	}
}

API rogxml *rogxml_AddCDataChild(rogxml *rx, const char *szName, const char *szText)
// Adds an element with a CDATA format child.
{
	rogxml *rxElement;
	rogxml *rxText;

	if (rogxml_ErrorNo(rx)) return NULL;

	rxElement = rogxml_AddChild(rx, szName);
	rxText=rogxml_AddText(rxElement, szText);
	rogxml_SetCData(rxText);

	return rxElement;
}

API rogxml *rogxml_AddCDataChildf(rogxml *rx, const char *szName, const char *szFmt, ...)
// As for AddTextChild only accepts a format string
{
	char buf[1024];

	va_list ap;
	va_start(ap, szFmt);
	vsnprintf(buf, sizeof(buf), szFmt, ap);
	va_end(ap);

	return rogxml_AddCDataChild(rx, szName, buf);
}

