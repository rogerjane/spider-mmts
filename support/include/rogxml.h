
#ifndef _ROGXML_H_
#define _ROGXML_H_

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

#include "rogxml.proto"

#endif
