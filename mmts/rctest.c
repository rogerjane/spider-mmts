#include <stdio.h>
#include <string.h>
#include <time.h>

#include "heapstrings.h"
#include "smap.h"
#include "rcache.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>

const char *cb_dns(const char *szKey, const char *szCurrentValue)
{
	rcache_SetStale(10, 60, 5);

	if (szCurrentValue && fork()) return szCurrentValue;

	static char buf[40];

	struct hostent *hp;
	hp=gethostbyname(szKey);
	if (!hp) return NULL;
	struct in_addr add=*(struct in_addr*)hp->h_addr_list[0];
	if (add.s_addr == INADDR_NONE) return NULL;

	snprintf(buf, sizeof(buf), "%s", (const char *)inet_ntoa(add));

	return buf;
}

SSMAP *cb_play(const char *szKey, SSMAP *szCurrentMap)
{
	static count=0;

fprintf(stderr, "cb_Play(\"%s\")\n", szKey);
	if (++count  >1) return NULL;
	SSMAP *map = ssmap_New();

	time_t now=time(NULL);
	struct tm *tm = localtime(&now);

	const char *szValue = hprintf(NULL, "Hello %s, it's %d:%02d:%02d on %d-%02d-%04d",
			szKey,
			tm->tm_hour, tm->tm_min, tm->tm_sec,
			tm->tm_mday, tm->tm_mon+1, tm->tm_year+1900);
	ssmap_Add(map, "value", szValue);
	szDelete(szValue);

	rcache_SetStale(10, 60, 5);

	return map;
}

void mynslookup(const char *szName)
{
	const char *result = rcache_GetValue("dns", szName);
	printf("DNS(%s) = '%s'\n", szName, result);
}

void main(int argc, char *argv[])
{
#if 1
	rcache_SetDir("/tmp/play");

	rcache_AddPoolStr("dns",cb_dns,NULL);
	mynslookup("google.co.uk");
	sleep(1);
	mynslookup("google.co.uk");
	sleep(1);
	mynslookup("google.co.uk");
	sleep(1);
	mynslookup("google.co.uk");
	exit(0);

	rcache_AddPoolMap("mypool", cb_play, NULL);
	fprintf(stderr, "Fetching value of 'hi'\n");

	const char *result = rcache_Get("mypool", "hi", "value");
	printf("Result 1 = '%s'\n", result);

	sleep(1);
	result = rcache_Get("mypool", "hi", "value");
	printf("Result 2 = '%s'\n", result);

	sleep(1);
	result = rcache_Get("mypool", "bert", "value");
	printf("Result 2 = '%s'\n", result);

	sleep(1);
	result = rcache_Get("mypool", "bert", "value");
	printf("Result 2 = '%s'\n", result);

	sleep(3);
	result = rcache_Get("mypool", "hi", "value");
	printf("Result 3 = '%s'\n", result);

#endif

#if 0					// rcache_Encode() and rcache_Decode() the command line arguments
	int i;

	for (i=1;i<argc;i++) {
		const char *szEncoded = rcache_Encode(argv[i]);
		char *szDecoded = strdup(argv[i]);
		rcache_Decode(szDecoded);

		printf("%d: '%s' -> encoded = '%s', decoded = '%s'\n", i, argv[i], szEncoded, szDecoded);
	}
#endif
}
