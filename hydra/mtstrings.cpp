
#include <ctype.h>

#define STATIC static
#define API

API int strnicmp(const char* a, const char* b, int n)
{
	int diff = 0;

	if(n>0) {
		do {
			diff = toupper(*a) - toupper(*b);
			n--;
		} while (*a++ && *b++ && !diff && n);
	}

	return diff;
}

API int rtrim(char* lpsz)
{
	char* lpszStart = lpsz;
	char* lpszEnd;

	for(lpszEnd = lpsz; *lpsz; )
		if (*lpsz != ' ')
			lpszEnd = ++lpsz;
		else
			lpsz++;

	*lpszEnd = '\0';

	return (lpszEnd - lpszStart);
}

API int stricmp(const char* s1, const char* s2)
{
	int z;

	for(; *s1 != '\0'; s1++, s2++)
		if ((z = toupper(*s1) - toupper(*s2)) != 0)
			return z;

	return (toupper(*s1) - toupper(*s2));
}

API const char* stristr(const char* lpszText, const char* lpszSub)
{
	const char*	lpszA;
	const char*	lpszB;
	char	ch = toupper(*lpszSub);

	if (*lpszSub != '\0')
	{
		for(; *lpszText != '\0'; lpszText++)
		{
			if (toupper(*lpszText) == ch)
			{
				for(
						lpszA = lpszText + 1, lpszB = lpszSub + 1;
						(*lpszB != '\0') && (
								toupper(*lpszA) == toupper(*lpszB)
							);
					)
				{
					lpszA++;
					lpszB++;
				}

				if (*lpszB == '\0')
					return lpszText;

				if (*lpszA == '\0')
					break;
			}
		}
	}

	return (0);
}

API int strlwr(char* s)
{
	char* p;
	for(p=s; *p; p++) 
		if(isupper(*p)) *p = tolower(*p);
	return p-s;
}

API int strupr(char* s)
{
	char* p;
	for(p=s; *p; p++) 
		if(islower(*p))  *p = toupper(*p);
	return p-s;
}
