#ifndef __MTJSON_H
#define __MTJSON_H


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


void json_Delete(JSON *j);
int json_Type(JSON *j);
int json_IsString(JSON *j);
int json_IsNumber(JSON *j);
int json_IsInteger(JSON *j);
int json_IsFloat(JSON *j);
int json_IsArray(JSON *j);
int json_IsObject(JSON *j);
int json_IsNull(JSON *j);
int json_IsBool(JSON *j);
long long json_AsInteger(JSON *j);
double json_AsFloat(JSON *j);
const char *json_AsString(JSON *j);
int json_AsBool(JSON *j);
SPMAP *json_AsObject(JSON *j);
int json_ArrayCount(JSON *j);
JSON *json_ArrayAt(JSON *j, int n);
long long json_ArrayIntegerAt(JSON *j, int n);
const char *json_ArrayStringAt(JSON *j, int n);
SPMAP *json_ArrayObjectAt(JSON *j, int n);
JSON *json_ArrayTakeAt(JSON *j, int n);// Removes an element from an array
int json_ObjectCount(JSON *j);
JSON *json_ObjectValue(JSON *j, const char *name);
JSON *json_ObjectAt(JSON *j, int n);
const char *json_ObjectNameAt(JSON *j, int n);
const char *json_Error(JSON *j);
JSON *json_New(int type);
JSON *json_NewError(const char *string);
JSON *json_NewErrorf(const char *fmt, ...);
JSON *json_NewStringHeap(int len, const char *string);
JSON *json_NewString(int len, const char *string);
JSON *json_NewInteger(long long number);
JSON *json_NewFloat(double number);
JSON *json_NewArray();
JSON *json_NewObject();
JSON *json_NewBool(int truth);
JSON *json_NewNull();
JSON *json_ArrayAdd(JSON *j, JSON *item);
JSON *json_ObjectAdd(JSON *j, const char *name, JSON *item);
#define json_ArrayAddInteger(j, n)		json_ArrayAdd(j, json_NewInteger(n))
#define json_ArrayAddFloat(j, n)		json_ArrayAdd(j, json_NewFloat(n))
#define json_ArrayAddString(j, len, s)	json_ArrayAdd(j, json_NewString(len, s))

#define json_ObjectAddInteger(j, s, n)		json_ObjectAdd(j, s, json_NewInteger(n))
#define json_ObjectAddFloat(j, s, n)		json_ObjectAdd(j, s, json_NewFloat(n))
#define json_ObjectAddString(j, s, len, s2)	json_ObjectAdd(j, s, json_NewString(len, s2))

const char *json_Render(JSON *json);
JSON *json_Parse(JSON *parent, const char **ptext);

#endif
