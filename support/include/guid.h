
#ifndef _GUID_H_
#define _GUID_H_

// XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX
typedef struct guid {
	unsigned long	time_low;
	unsigned short	time_mid;
	unsigned short	time_hi_and_version;
	unsigned char	clock_seq_hi_and_reserved;
	unsigned char	clock_seq_low;
	unsigned char	node[6];
} guid;

#include "guid.proto"

#endif
