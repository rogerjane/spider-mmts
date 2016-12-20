#include "stdio.h"
#include "mtjson.h"
#include <time.h>
#include <mtsl3.h>
#include <sys/stat.h>
//#include <varargs.h>

void Log(const char *szFmt, ...)
{
	va_list ap;
	time_t now=time(NULL);
	struct tm *tm = gmtime(&now);

	printf("%02d-%02d-%02d %02d:%02d:%02d ",
			tm->tm_mday, tm->tm_mon+1, tm->tm_year % 100,
			tm->tm_hour, tm->tm_min, tm->tm_sec);

	va_start(ap, szFmt);
	vprintf(szFmt, ap);
	va_end(ap);
	printf("\n");
}

void main(int argc, const char *argv[])
{
	S3 *db = s3_Open("/tmp/play.db");
	S3LONG(db, updated);

	time_t now;
	time(&now);

	const char *filename = "/usr/mt/spider/rpc/something/good";
	int err;

	err = s3_Queryf(db, "CREATE TABLE api ("
			" id INTEGER PRIMARY KEY, "
			" api TEXT, "
			" filename TEXT, "
			" updated INTEGER, "
			" version TEXT, "
			" UNIQUE (filename, api), "
			" UNIQUE (id) "
			")");
	printf("%d: %s\n", err, s3_LastQuery(db));

	const char *files[] = {"/tmp/file1","/tmp/anotherfile","/tmp/andathirdfile","/tmp/little"};

	int i;
	for (i=0;i<4;i++) {
		struct stat st;
		const char *filename = files[i];

		if (!stat(filename, &st)) {
			err = s3_Queryf(db, "SELECT updated FROM api WHERE filename=%s", filename);
			printf("%d: %s - (%d, %d)\nQry: %s\n", err, s3_ErrorStr(db), updated, st.st_mtime, s3_LastQuery(db));

			if (st.st_mtime > updated) {
				if (updated) {
					err = s3_Queryf(db, "UPDATE api SET updated = %d WHERE filename = %s", now, filename);
					printf("%d: %s\nQry: %s\n", err, s3_ErrorStr(db), s3_LastQuery(db));
				} else {
					err = s3_Queryf(db, "INSERT INTO api (api,filename,updated,version) VALUES (%s,%s,%d,%s)", "api", filename, now, "1.00");
					printf("%d: %s\nQry: %s\n", err, s3_ErrorStr(db), s3_LastQuery(db));
				}
			}
		}
	}

	s3_Close(db);

}

void main2(int argc, const char *argv[])
{
	const char *input = argv[1];
	const char *next = input;

	JSON *json = json_Parse(&next);

	const char *output = json_Render(json);

	printf("In:   '%s'\n", input);
	printf("Out:  '%s'\n", output);
	printf("Next: '%s'\n", next);

	char buf[256];
	int i;
	for (i=0;i<128;i++)
		buf[i]=i;
	int pos = 0;
	buf[pos++]=0xf0;
	buf[pos++]=0x9f;
	buf[pos++]=0x96;
	buf[pos++]=0x95;

	JSON *j = json_NewArray();
	json_ArrayAddString(j, pos, buf);

	printf("Strg: '%s'\n", json_Render(j));
}
