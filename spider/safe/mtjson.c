#if 0
./makeh $0
exit 0
#endif

#include "mtjson.h"
#define API
#define STATIC static

#include <stdio.h>						// For NULL and heapstrings needs it
#include <string.h>						// strdup()

#include <hbuf.h>
#include <heapstrings.h>
#include <mtmacro.h>					// For NEW

#if 0
// START HEADER

#include <stdarg.h>
#include <smap.h>

#define JSON_ERROR		0
#define JSON_INTEGER	1
#define JSON_FLOAT		2
#define JSON_NULL		3
#define JSON_BOOL		4
#define JSON_ARRAY		5
#define JSON_OBJECT		6
#define JSON_STRING		7

typedef struct JSON {
	struct JSON		*parent;
	unsigned char	type;
	union {
		long long integer;				// For JSON_INTEGER
		double number;					// For JSON_FLOAT
		unsigned char truth;			// For JSON_BOOL
		struct {						// For JSON_ARRAY
			int count;
			struct JSON **values;
		} array;
		SPMAP *object;					// For JSON_OBJECT
		struct {						// For JSON_STRING and JSON_ERROR
			int len;
			const char *string;
		} string;
	} data;
} JSON;

// END HEADER
#endif

STATIC char *SkipSpaces(const char *t)
{
	while (isspace(*t)) t++;

	return (char*)t;
}

API void json_Delete(JSON *j)
{
return ;
	if (j) {
		switch (j->type) {
		case JSON_ERROR:
		case JSON_STRING:
			free((char*)j->data.string.string);
			break;
		case JSON_ARRAY:
			{
				int i;

				for (i=0;i<j->data.array.count;j++) {
					json_Delete(j->data.array.values[i]);
				}
			}
			break;
		case JSON_OBJECT:
			{
				const char *name;
				JSON *value;

				spmap_Reset(j->data.object);
				while (spmap_GetNextEntry(j->data.object, &name, (void**)&value)) {
					json_Delete((JSON*)value);
				}
				spmap_Delete(j->data.object);
			}
			break;
		}
		free((char*)j);
	}
}

// Type ascertaining functions
API int json_Type(JSON *j)							{ return j ? j->type : JSON_ERROR; }
API int json_IsString(JSON *j)						{ return j && j->type == JSON_STRING; }
API int json_IsNumber(JSON *j)						{ return j && (j->type == JSON_INTEGER || j->type == JSON_FLOAT); }
API int json_IsInteger(JSON *j)						{ return j && j->type == JSON_INTEGER; }
API int json_IsFloat(JSON *j)						{ return j && j->type == JSON_FLOAT; }
API int json_IsArray(JSON *j)						{ return j && j->type == JSON_ARRAY; }
API int json_IsObject(JSON *j)						{ return j && j->type == JSON_OBJECT; }
API int json_IsNull(JSON *j)						{ return j && j->type == JSON_NULL; }
API int json_IsBool(JSON *j)						{ return j && j->type == JSON_BOOL; }

// Get as various types - the type should be known before you get it
API long long json_AsInteger(JSON *j)				{ return j ? j->data.integer : 0 ; }
API double json_AsFloat(JSON *j)
{
	return j ? (j->type == JSON_INTEGER ? j->data.integer : j->data.number) : 0.0;
}
API const char *json_AsString(JSON *j)				{ return j ? j->data.string.string : "NULL JSON"; }
API int json_AsBool(JSON *j)						{ return j ? j->data.truth : -1; }
API SPMAP *json_AsObject(JSON *j)					{ return j ? j->data.object : NULL; }

// Things that apply to arrays
API int json_ArrayCount(JSON *j)					{ return j ? j->data.array.count : 0; }
API JSON *json_ArrayAt(JSON *j, int n)				{ return j ? j->data.array.values[n] : NULL; }
API long long json_ArrayIntegerAt(JSON *j, int n)	{ return j ? json_AsInteger(json_ArrayAt(j, n)) : 0; }
API const char *json_ArrayStringAt(JSON *j, int n)	{ return j ? json_AsString(json_ArrayAt(j, n)) : 0; }
API SPMAP *json_ArrayObjectAt(JSON *j, int n)		{ return j ? json_AsObject(json_ArrayAt(j, n)) : 0; }
API JSON *json_ArrayTakeAt(JSON *j, int n)			// Removes an element from an array
{
	JSON *result = NULL;
	int count = j->data.array.count;

	if (j && n>= 0 && n < count) {
		JSON **values = j->data.array.values;

		result = values[n];
		if (n < count-1)
			memmove(values+n, values+n+1, sizeof(JSON*)*(count-1-n));
		count--;
		j->data.array.values = (JSON**)realloc(values, (sizeof(JSON*)*count));
		j->data.array.count = count;
	}

	return result;
}

// Things that apply to objects
API int json_ObjectCount(JSON *j)					{ return j ? spmap_Count(j->data.object) : 0; }
API JSON *json_ObjectValue(JSON *j, const char *name)
{
	return (j && name) ? (JSON*)spmap_GetValue(j->data.object, name) : NULL;
}
API JSON *json_ObjectAt(JSON *j, int n)				{ return j ? (JSON*)spmap_GetValueAtIndex(j->data.object, n) : NULL; }
API const char *json_ObjectNameAt(JSON *j, int n)	{ return j ? spmap_GetKeyAtIndex(j->data.object, n) : NULL; }


API const char *json_Error(JSON *j)
// Returns  NULL    There isn't an error
//          char*   Pointer to a static error message
{
	    if (j) {
			        return j->type == JSON_ERROR ? j->data.string.string : NULL;
					    } else {
							        return "NULL JSON";
									    }
}

API JSON *json_New(int type)
{
	JSON *j = NEW(JSON, 1);
	memset(j, sizeof(*j), 0);

	j->parent = NULL;
	j->type = type;

	return j;
}

API JSON *json_NewError(const char *string)
{
	JSON *j = json_New(JSON_STRING);
	if (!string) string = "NULL Error???";
	j->data.string.len = strlen(string);;
	j->data.string.string = strdup(string);

	return j;
}

API JSON *json_NewErrorf(const char *fmt, ...)
{
	char buf[1000];

	if (fmt) {
		va_list ap;

		va_start(ap, fmt);
		vsnprintf(buf, sizeof(buf), fmt, ap);
		va_end(ap);
	}

	return json_NewError(fmt ? buf : fmt);
}

API JSON *json_NewStringHeap(int len, const char *string)
// The string is on the heap and ownership is transferred to the JSON
{
	JSON *j = json_New(JSON_STRING);
	if (len == -1) len = string ? strlen(string) : 0;
	j->data.string.len = len;
	j->data.string.string = string;

	return j;
}

API JSON *json_NewString(int len, const char *string)	{ return json_NewStringHeap(len, string ? strdup(string) : NULL); }

API JSON *json_NewInteger(long long number)
{
	JSON *j = json_New(JSON_INTEGER);
	j->data.integer = number;

	return j;
}

API JSON *json_NewFloat(double number)
{
	JSON *j = json_New(JSON_INTEGER);
	j->data.number = number;

	return j;
}

API JSON *json_NewArray()
{
	JSON *j = json_New(JSON_ARRAY);
	j->data.array.count = 0;
	j->data.array.values = NULL;

	return j;
}

API JSON *json_NewObject()
{
	JSON *j = json_New(JSON_OBJECT);
	j->data.object = spmap_New();

	return j;
}

API JSON *json_NewBool(int truth)
{
	JSON *j = json_New(JSON_BOOL);
	j->data.truth = truth;

	return j;
}

API JSON *json_NewNull()
{
	return json_New(JSON_NULL);
}

API JSON *json_ArrayAdd(JSON *j, JSON *item)
{
	if (j && j->type == JSON_ARRAY) {
		int count = j->data.array.count;

		if (count) {
			RENEW(j->data.array.values, JSON*, count+1);
		} else {
			j->data.array.values = NEW(JSON*, 1);
		}
		j->data.array.values[count]=item;
		j->data.array.count = count+1;
	}

	return item;
}

API JSON *json_ObjectAdd(JSON *j, const char *name, JSON *item)
{
	if (j && j->type == JSON_OBJECT) {
		spmap_Add(j->data.object, name, item);
	}

	return item;
}

#if 0
// START HEADER
#define json_ArrayAddInteger(j, n)		json_ArrayAdd(j, json_NewInteger(n))
#define json_ArrayAddFloat(j, n)		json_ArrayAdd(j, json_NewFloat(n))
#define json_ArrayAddString(j, len, s)	json_ArrayAdd(j, json_NewString(len, s))

#define json_ObjectAddInteger(j, s, n)		json_ObjectAdd(j, s, json_NewInteger(n))
#define json_ObjectAddFloat(j, s, n)		json_ObjectAdd(j, s, json_NewFloat(n))
#define json_ObjectAddString(j, s, len, s2)	json_ObjectAdd(j, s, json_NewString(len, s2))
// END HEADER
#endif

STATIC const char *json_RenderString(const char *string)
// Returns a rendered string in the heap including surrounding " marks
{
	HBUF *h = hbuf_New();

	hbuf_AddChar(h, '"');

	char c;
	while ((c=*string++)) {
		if (c == '\\' || c == '"') {
			hbuf_AddChar(h, '\\');
			hbuf_AddChar(h, c);
		} else if (c < ' ' || c > '~') {
			hbuf_AddChar(h, '\\');
			hbuf_AddChar(h, c);
		} else {
			hbuf_AddChar(h, c);
		}
	}
	hbuf_AddChar(h, '"');
	hbuf_AddChar(h, '\0');

	const char *result = hbuf_ReleaseBuffer(h);
	hbuf_Delete(h);

	return result;
}

API const char *json_Render(JSON *json)
{
	HBUF *h = hbuf_New();

//	hbuf_AddHeap(h, -1, hprintf(NULL, "(%d)", json->type));
	switch (json->type) {
		case JSON_ERROR:
			hbuf_AddHeap(h, -1, hprintf(NULL, "(ERROR: %s)", json->data.string.string));
			break;
		case JSON_INTEGER:
			hbuf_AddHeap(h, -1, hprintf(NULL, "%Ld", json->data.integer));
			break;
		case JSON_FLOAT:
			hbuf_AddHeap(h, -1, hprintf(NULL, "%f", json->data.number));
			break;
		case JSON_NULL:
			hbuf_AddBuffer(h, 4, "null");
			break;
		case JSON_BOOL:
			hbuf_AddBuffer(h, -1, json->data.truth ? "true" : "false");
			break;
		case JSON_ARRAY:
			hbuf_AddChar(h, '[');

			int i;
			for (i=0;i<json->data.array.count;i++) {
				if (i) hbuf_AddChar(h, ',');
				hbuf_AddHeap(h, -1, json_Render(json->data.array.values[i]));
			}
			hbuf_AddChar(h, ']');
			break;
		case JSON_OBJECT:
			hbuf_AddChar(h, '{');
			int count = 0;

			spmap_Reset(json->data.object);
			const char *name;
			JSON *value;
			while (spmap_GetNextEntry(json->data.object, &name, (void**)&value)) {
				if (count++) hbuf_AddChar(h, ',');
				hbuf_AddHeap(h, -1, json_RenderString(name));
				hbuf_AddChar(h, ':');
				hbuf_AddHeap(h, -1, json_Render(value));
			}
			hbuf_AddChar(h, '}');
			break;
		case JSON_STRING:
			{
			hbuf_AddHeap(h, -1, json_RenderString(json->data.string.string));
			break;
			}
	}

	hbuf_AddChar(h, '\0');
	const char *result = hbuf_ReleaseBuffer(h);
	hbuf_Delete(h);

	return result;
}

STATIC const char *json_ParseString(const char **ptext, int *plen)
// Fetches a string on the heap, returning the length in *plen.
// Should be pointing at the initial ", will end pointing past the final "
// If *plen is -1 on return then the string returned is an error message
// NOT NULL SAFE on *ptext OR *plen.
{
	const char *result;

	const char *text=SkipSpaces(*ptext);
	if (*text++ != '"') {
		*plen=-1;
		return strdup("Expected leading \"");
	}

	HBUF *hbuf = hbuf_New();
	char c;
	const char *err = NULL;
	while ((c=*text++) && c != '"') {
		if (c != '\\') {
			hbuf_AddChar(hbuf, c);
		} else {
			c=*text++;
			if (!c) {
				err = strdup("Trailing \\ in string");
				break;
			}
			hbuf_AddChar(hbuf, c);
		}
	}

	if (err) {
		*plen = -1;
		hbuf_Delete(hbuf);
		return err;
	}

	hbuf_AddChar(hbuf, 0);
	*ptext = text;
	*plen = hbuf_GetLength(hbuf);
	result = hbuf_ReleaseBuffer(hbuf);

	hbuf_Delete(hbuf);

	return result;
}

API JSON *json_Parse(JSON *parent, const char **ptext)
{
	const char *text=SkipSpaces(*ptext);
	JSON *json = NULL;

	if (*text) {
		char c = *text;
		if (c == '"') {														// String
			int len;
			const char *string = json_ParseString(&text, &len);
			if (len == -1) {
				json = json_NewError(string);
			} else {
				json = json_NewStringHeap(-1, string);
			}
		} else if (c == '[') {												// Array
			json = json_NewArray();

			text++;
			c = ',';
			while (c == ',') {
				while (isspace(*text)) text++;

				if (*text != ']') {
					JSON *value = json_Parse(json, &text);

					json_ArrayAdd(json, value);

					while (isspace(*text)) text++;
				}
				c = *text++;
			}
			if (c != ']') {
				json_Delete(json);
				json = json_NewError("Expected ] at end of array");
			}
		} else if (c == '{') {												// Object
			json = json_NewObject();
			const char *err = NULL;

			text++;
			c = ',';

			while (c == ',') {
				while (isspace(*text)) text++;
				if (*text != '}') {
					int len;
					const char *name = json_ParseString(&text, &len);

					if (len == -1) {
						err = name;
						break;
					}

					while (isspace(*text)) text++;
					c = *text++;
					if (c != ':') {
						err = strdup("Expected : within object");
						break;
					}

					JSON *value = json_Parse(json, &text);
					if (value->type == JSON_ERROR) {
						err = value->data.string.string;
						free((char*)value);
						break;
					}

					json_ObjectAdd(json, name, value);

					while (isspace(*text)) text++;
				}
				c = *text++;
			}

			if (c != '}') {										// Don't over-write the error if we already have one!
				err = strdup("Expected } at end of object");
			}
			if (err) {
				json_Delete(json);
				json = json_NewError(err);
				szDelete(err);
			}
		} else if ((c >= '0' && c <= '9') || c == '-') {				// Number
			// Note that we try to make this an integer if we can, which means that number of decimal places is no more than
			// the exponent and the result is less than 2^53 (An IEEE double can be exactly represented as an int)
			double value = 0;
			int dp = 0;									// Number of decimal places
			int exp = 0;								// Effective exponent
			int mult = 1;

			if (c == '-') {
				c == *++text;
				mult = -1;
			}
			while (c >= '0' && c <= '9') {
				value = value * 10 + (c - '0');
				c = *++text;
			}
			if (c == '.') {
				c = *++text;
				int tmpdp = 0;

				double dec = 0.1;
				while (c >= '0' && c <= '9') {
					value += dec * (c-'0');
					dec /= 10;
					tmpdp++;
					if (c != '0') dp=tmpdp;									// Effectively ignores trailing zeroes
					c = *++text;
				}
			}
			if (c == 'e' || c == 'E') {
				int mult = 1;

				c = *++text;
				if (c == '-') {
					mult = -1;
					c = *++text;
				}
				while (c >= '0' && c <= '9') {
					exp = exp * 10 + (c - '0');
					c = *++text;
				}
				value *= pow(10.0, mult * exp);
			}
			if (dp <= exp && value < 0x20000000000000L) {		// 2^53 - we want to go that high
				json = json_NewInteger((long long)(value)*mult);
			} else {
				json = json_NewFloat(mult * value);
			}
		} else {															// true, false or null
			if (!strncmp(text, "true", 4)) {
				json = json_NewBool(1);
				text+=4;
			} else if (!strncmp(text, "false", 5)) {
				json = json_NewBool(0);
				text+=5;
			} else if (!strncmp(text, "null", 4)) {
				json = json_NewNull();
				text+=4;
			} else {
				json = json_NewErrorf("Invalid character '%c'", c);
			}
		}
		if (json->type == JSON_ERROR) {
			JSON *j = json_NewErrorf("%s at >>%.15s<<", json_AsString(json), text);
			json_Delete(json);
			json = j;
		}
	} // if (text)

	*ptext = text;
//const char *rendering = json_Render(json); Log("Rendering as: '%s'", rendering); szDelete(rendering);

	return json;
}

