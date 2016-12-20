

#ifndef __ROGCSV_H_
#define __ROGCSV_H_

#include <stdio.h>

typedef struct CSV {
	FILE	*fp;
	char	*szLine;				// Original line
	char	*szPtr;					// Pointer into line
	int		nLine;					// Current line number
} CSV;

typedef struct CSVXF {							// CSV Field
	struct CSVXF	*pNext;
	const char		*szName;
	const char		*szData;
} CSVXF;

typedef struct CSVX {
	CSV		*csv;								// CSV file
	CSVXF	*Field;								// Field information
	CSVXF	**Array;							// An array of the fields
	int		nNames;								// Count of the names in row 1
	int		nFields;							// Count of the fields this row
} CSVX;

#include "csv.proto"

#endif
