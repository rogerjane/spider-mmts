
// 02-12-13 RJ 0.00 Convert between a SQLITE represntation of a .ini file and the real thing

// Usage:
// sl3ini -s fred.ini                          Convert fred.ini to fred.ini.sl3
// sl3ini -i fred.ini                          Convert fred.ini.sl3 to fred.ini
// sl3ini -d fred.ini section variable         Delete a value
// sl3ini -u fred.ini section variable value   Update a value
// sl3ini -g fred.ini [[section] variable]     Get (output) file, section, variable
// sl3ini -l fred.ini [section]                List variables

#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>

#include <heapstrings.h>

#include "mtsl3.h"

#define szMyName	"sl3ini"

void Usage(const char *msg)
{
	if (msg) {
		fprintf(stderr, "%s: %s\n", szMyName, msg);
	}

	fprintf(stderr, "Usage: %s -sidugl file [[[section] name] value]\n", szMyName);
	fprintf(stderr, " -s fred.ini                          Convert fred.ini to fred.ini.sl3\n");
	fprintf(stderr, " -i fred.ini                          Convert fred.ini.sl3 to fred.ini\n");
	fprintf(stderr, " -d fred.ini section variable         Delete a value\n");
	fprintf(stderr, " -u fred.ini section variable value   Update a value\n");
	fprintf(stderr, " -g fred.ini [[section] variable]     Get (output) file, section, variable\n");
	fprintf(stderr, " -l fred.ini [section]                List variables\n");

	exit(1);
}

void Fatal(const char *szFmt, ...)
{
	va_list ap;
	char buf[1000];

	va_start(ap, szFmt);
	vsnprintf(buf, sizeof(buf), szFmt, ap);
	va_end(ap);

	fprintf(stderr,"%s: %s\n",szMyName, buf);

	exit(99);
}

sl3Fatal(S3 *s3)
{
	Fatal("SQLITE Error %d: %s", s3_ErrorNo(s3), s3_ErrorStr(s3));
}

int sl3Error(S3 *s, int nError, const char *szError, const char *szQuery)
{
	fprintf(stderr, "Last query (%p): %s\n", s, szQuery);
	Fatal("SQL Error %d: %s", nError, szError);
}

void sl3Toini(const char *sl3File, const char *iniFile)
{
	S3 *s;
	FILE *ini;
	int nSection=0;

	if (access(sl3File, 4)) Fatal("I can't see sl3 file '%s'", sl3File);

	s = s3_MustOpen(sl3File);

	ini = strcmp(iniFile, "-") ? fopen(iniFile, "w") : stdout;
	if (!ini) Fatal("Can't create ini file '%s'", iniFile);

	S3STRING(s, section);
	S3STRING(s, name);
	S3STRING(s, value);

	s3it *it = s3_MustQuery(s, "SELECT DISTINCT section FROM ini");

	while (s3it_Next(it)) {
		if (nSection++) {
			fprintf(ini, "\n");
		} else if (ini != stdout) {
			struct tm *tm;
			time_t now = time(NULL);

			tm = localtime(&now);
			fprintf(ini, "# %s created by %s from %s\n", iniFile, szMyName, sl3File);
			fprintf(ini, "# Created at %02d-%02d-%04d %02d:%02d:%02d\n",
					tm->tm_mday, tm->tm_mon+1, tm->tm_year+1900,
					tm->tm_hour, tm->tm_min, tm->tm_sec);
			fprintf(ini, "\n");
		}
		fprintf(ini, "[%s]\n", section);

		s3it *it2 = s3_MustQueryf(s, "SELECT name,value FROM ini WHERE section=%s", section);

		while (s3it_Next(it2)) {
			fprintf(ini, "%s = %s\n", name, value);
		}
		s3it_Destroy(&it2);
	}
	s3it_Destroy(&it);
	s3_Destroy(&s);

	if (ini != stdout) fclose(ini);
}

void iniTosl3(const char *iniFile, const char *sl3File)
{
	S3 *s;
	const char *szSection = strdup("");

	if (!sl3File) sl3File=hprintf(NULL, "%s.sl3", iniFile);

	FILE *fpi=fopen(iniFile, "r");
	if (!fpi) Fatal("Could not read %s", iniFile);

	s = s3_Open(sl3File);
	if (!s) sl3Fatal(s);

	s3_MustDo(s, "CREATE TABLE IF NOT EXISTS ini ("
			"section TEXT, "
			"name TEXT, "
			"value TEXT, "
			"PRIMARY KEY (section,name)"
			")");

	char line[1000];

	while (fgets(line, sizeof(line), fpi)) {
		char *chp=strchr(line, '\n');
		if (chp) *chp='\0';

		char *l=line;
		while (isspace(*l)) l++;

		if (!*l || *l == '#' || *l == '=') continue;

		if (*l == '[') {
			chp=strchr(l,']');
			if (chp) {
				*chp='\0';
				szDelete(szSection);
				szSection=strdup(l+1);
			}
		} else {
			chp=strchr(l ,'=');
			if (chp) {
				char *trimmer=chp-1;
				while (trimmer > l && isspace(*trimmer))
					trimmer--;
				trimmer[1]='\0';

				chp++;
				while (isspace(*chp)) chp++;
				s3_MustDof(s, "REPLACE INTO ini (section,name,value) VALUES (%s,%s,%s)", szSection, l, chp);
			}
		}
	}

	fclose(fpi);
	s3_Destroy(&s);
}

void main(int argc, char *argv[])
{
	extern int optind;
	int sl3=0;
	int ini=0;
	int del=0;
	int update=0;
	int get=0;
	int list=0;
	int c;

	while((c=getopt(argc,argv,"sidugl"))!=-1){
		switch(c) {
			case 's': sl3=1; break;
			case 'i': ini=1; break;
			case 'd': del=1; break;
			case 'u': update=1; break;
			case 'g': get=1; break;
			case 'l': list=1; break;
		}
	}

	if (sl3 + ini + del + update + get + list != 1) {
		Usage("Exactly one of -s -i -d -u -g -l");
	}

	s3_OnError(NULL, sl3Error);

	argc-=optind;
	argv+=optind;
	if (sl3) {
		if (argc != 1 && argc != 2) Usage("Expect one or two arguments for -s");
		iniTosl3(argv[0], argv[1]);
	} else if (ini) {
		if (argc != 1 && argc != 2) Usage("Expect one or two arguments for -i");
		sl3Toini(argv[0], argv[1]);
	} else if (del) {
	} else if (update) {
	} else if (get) {
	} else if (list) {
	}
}
