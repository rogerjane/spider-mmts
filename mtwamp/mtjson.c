#if 0
./makeh $0
exit 0
#endif

#include "mtjson.h"
#define API
#define STATIC static

#include <math.h>
#include <stdio.h>						// For NULL and heapstrings needs it
#include <stdlib.h>
#include <string.h>						// strdup()

#include <hbuf.h>
#include <heapstrings.h>
#include <mtmacro.h>					// For NEW

#if 0
// START HEADER

// NB. Limitations of JSON implementation
// Numbers are only gauranteed to be uniquely represented up to 53 bits
// Object names must not contain \u0000 characters and must be unique within an object

// Note that general strings within JSON can contain '\0' characters and so would normally travel around
// outside of these functions with an associated length (being the length in bytes).
// This is passed in and out of the json_*String*() functions.
// If you don't have '\0' in your strings, then there are equivalent json_*Stringz*() macros as a convenience.

// The external representation of an 'Object' is a SPMAP* so the functions that push objects into JSON*
// and return them, return them as SPMAP* types.

// The mtjson library is NULL safe but not re-entrant.

#include <stdarg.h>
#include <smap.h>

// Constants returned by json_Type()
#define JSON_ERROR		0
#define JSON_INTEGER	1
#define JSON_FLOAT		2
#define JSON_NULL		3
#define JSON_BOOL		4
#define JSON_ARRAY		5
#define JSON_OBJECT		6
#define JSON_STRING		7

typedef struct JSON {
	long signature;
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
	struct JSON *prev;					// To make a linked list of all JSONs for debug purposes
	struct JSON *next;
} JSON;

// Convenience functions for when you don't care about strings with '\0' in them.
#define json_AsStringz(j)						json_AsString(j, NULL)
#define json_ToStringz(j)						json_ToString(j, NULL)
#define json_ArrayStringzAt(j, n)				json_ArrayStringAt(j, n, NULL)
#define json_NewStringzHeap(string)				json_NewStringHeap(-1, string)
#define json_NewStringz(string)					json_NewString(-1, string)
#define json_ArrayAddStringz(j, string)			json_ArrayAddString(j, -1, string)
#define json_ArrayTakeStringzAt(j, n)			json_ArrayTakeStringAt(j, n, NULL)
#define json_ObjectAddStringz(j, name, string)	json_ObjectAddString(j, name, -1, string)
#define json_ObjectStringzCalled(j, name)		json_ObjectStringCalled(j, name, NULL)

// END HEADER
#endif

// The following section handles the allJson list of all JSON* structures.
// The rule is simply that if the JSON* structures does NOT have a parent then it'll be in the list.

#define	SIGN_JSON		0x4e4f534a		// JSON
#define SIGN_json		0x6e6f736a		// json

static void (*_Logger)(const char *fmt, va_list ap) = NULL;

static void Log(const char *fmt, ...)
{
	if (_Logger) {
		va_list ap;

		va_start(ap, fmt);
		(*_Logger)(fmt, ap);
		va_end(ap);
	}
}

API void json_Logger(void (*logger)(const char *, va_list))
// Sets a logging function to receive all debug output from the JSON library
{
	_Logger = logger;
}

#if 0									// Set to 0 to enable tracking of all JSON structures
API void json_LogAll()
{
	Log("Tracking of JSON structures disabled - see " __FILE__);
}
#define json_LinkIn(x)
#define json_LinkOut(x)

#else
static JSON *allJson = NULL;			// Head of a linked list of all JSON structures

API void json_LogAll()
// Calls the 'Log()' function for every allocated JSON structure
{
	JSON *json = allJson;

	int count=0;
	while (json) {
		count++;
		json = json->next;
	}

	Log("Dumping %d JSON structure%s starting from %p", allJson, count, count==1?"":"s");

	json = allJson;
	JSON *before = NULL;
	JSON *prevnext = allJson;
	while (json) {
		Log("JSON (%p [%p<->%p]): %s", json, json->prev, json->next, json_Render(json));
		before = json;
		json = json->next;
	}
}

STATIC void json_LinkIn(JSON *j)
// Adds the json passed to the linked list of all JSON*
{
//Log("LinkIn (%p:%.4s) %d p=%p, n=%p, a=%p: %s", j, &j->signature, j->type, j->prev, j->next, allJson, json_Render(j));
	if (allJson)
		allJson->prev = j;

	j->prev = NULL;
	j->next = allJson;
	allJson = j;
//Log("LinkIn (%p:%.4s) p=%p, n=%p, a=%p", j, &j->signature, j->prev, j->next, allJson);
//json_LogAll();
}

STATIC void json_LinkOut(JSON *j)
{
//Log("LinkOut(%p:%.4s) p=%p, n=%p, a=%p: %s", j, &j->signature, j->prev, j->next, allJson, json_Render(j));
	if (allJson == j)
		allJson = j->next;

	if (j->prev) {
		j->prev->next = j->next;
	}

	if (j->next) {
		j->next->prev = j->prev;
	}
	j->prev = NULL;
	j->next = NULL;
//Log("LinkOut(%p:%.4s) p=%p, n=%p, a=%p", j, &j->signature, j->prev, j->next, allJson);
//json_LogAll();
}
#endif

STATIC char *SkipSpaces(const char *t)
{
	if (t)
		while (isspace(*t)) t++;

	return (char*)t;
}

STATIC JSON *json_Release(JSON *json)
// Removes this structure from any containing structure
// The containing structure may be an array or an object
// Returns the now 'free as a bird' json structure
{
	if (json && json->parent) {
		JSON *parent = json->parent;

		switch (parent->type) {				// Must be array or object - if not then we don't do anything
		case JSON_ARRAY:
			{
				JSON *result = NULL;
				int count = parent->data.array.count;
				JSON **values = parent->data.array.values;
				int n;

				for (n = 0; n < count; n++) {
					if (values[n] == json) {			// Found our boy!
						count--;
						if (n < count)
							memmove(values+n, values+n+1, sizeof(JSON*)*(count-n));
						parent->data.array.values = (JSON**)realloc(values, (sizeof(JSON*)*count));
						parent->data.array.count = count;
						break;
					}
				}
			}
			break;
		case JSON_OBJECT:
			{
				const char *name = spmap_GetKey(parent->data.object, (void*)json);
				if (name)
					spmap_DeleteKey(parent->data.object, name);
			}
			break;
		}
		json->parent = NULL;

		json_LinkIn(json);
	}
}

API void json_Delete(JSON *json)
// Delete the JSON object, which will safely remove it from any containing JSON
{
	// The functionality in here is simply to remove the JSON from any container then walk through it, deleting
	// For non-array, non-object elements this means simply freeing any allocated data
	// Arrays and objects are discussed separately below.

	if (json) {
		if (json->signature != SIGN_JSON) {
			Log("Deleting something non-JSON at %p (%.4s)", json, &json->signature);
			int a=1;a--;int b=1/a;
		} else {
//Log("Deleting JSON %p (%d), son of %p: %s", json, json->type, json->parent, json_Render(json));
		}
		json_Release(json);											// first release it from any container

		switch (json->type) {
		case JSON_ERROR:											// Errors are really strings
		case JSON_STRING:
			if (json->data.string.string) {
				free((char*)json->data.string.string);
				json->data.string.string = NULL;
			}
			json->data.string.len = 0;
			break;
		case JSON_ARRAY:
			// Step through each element, deleting each one BUT nulling the parent pointer first so that 'release' isn't called,
			// which would remove them from the array itself and inefficiently put us in a mess.
			{
				if (json->data.array.values) {
					int i;

					for (i=0; i<json->data.array.count; i++) {
						JSON *j = json->data.array.values[i];

						j->parent = NULL;							// Stop it releasing and hence messing up this array
						json_Delete(j);								// Delete it
					}
					free((char*)json->data.array.values);
				}
				json->data.array.count = 0;
			}
			break;
		case JSON_OBJECT:
			// Iterate through the object, deleting each one BUT nulling the parent pointer first so that 'release' isn't called,
			// whcih would remove them from the MAP itself and put us in a mess.
			{
				// Smidgenly odd loop as doing it more simply messes up any attempt to dump the object during the _Delete()...
				while (spmap_Count(json->data.object)) {
					const char *name;
					JSON *j;

					spmap_Reset(json->data.object);
					spmap_GetNextEntry(json->data.object, &name, (void**)&j);
					json_Delete(j);
					spmap_DeleteKey(json->data.object, name);
				}
				spmap_Delete(json->data.object);
				json->data.object = NULL;
			}
			break;
		}

		json_LinkOut(json);

		json->signature = SIGN_json;		// json
		free((char*)json);
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

API int json_StringLength(JSON *j)
// Returns the length of the string in bytes.  Note that this may not be the same as characters for UTF-8 strings.
{
	return (j && j->type == JSON_STRING) ? j->data.string.len : 0;
}

// START HEADER
// Get as various types.  If the type you're trying to return isn't compatible with the JSON type then you'll get
// 0, NULL etc.  This also applies if the JSON* passed in is NULL.
// END HEADER

API long long json_AsInteger(JSON *j)
{
	long long result = 0;

	if (j) {
		if (j->type == JSON_INTEGER) {
			result = j->data.integer;
		} else if (j->type == JSON_FLOAT) {
			result = j->data.number;
		}
	}

	return result;
}

API double json_AsFloat(JSON *j)
{
	double result = 0.0;

	if (j) {
		if (j->type == JSON_INTEGER) {
			result = j->data.integer;
		} else if (j->type == JSON_FLOAT) {
			result = j->data.number;
		}
	}

	return result;
}

API const char *json_AsString(JSON *j, int *plen)
// Returns a pointer to the internally held string, which is always '\0' terminated.
// Note, however, that the string may contain '\0' characters so *plen gets the actual length in bytes (excluding the '\0')
// If the string is NULL, then *plen will get 0 and the returned string will be NULL.
// Note that this function also works to return the string associated with an error.
{
	const char *result = NULL;

	if (j && (j->type == JSON_STRING || j->type == JSON_ERROR)) {
		if (plen) *plen = j->data.string.len;
		result = j->data.string.string;
	}

	return result;
}

API int json_AsBool(JSON *j)
// Returns 1 or 0 depending on the 'truth' of the boolean
// NB. Returns -1 if the JSON* is not a bool, which will probably evaluate to 'true' but indicates you have something wrong...
{
	return json_IsBool(j) ? j->data.truth : -1;
}

API SPMAP *json_AsObject(JSON *j)
// Returns an SPMAP, which maps names onto JSON* objects.
// NB. Returns NULL if the JSON* passed is not an object.
{
	return json_IsObject(j) ? j->data.object : NULL;
}

// START HEADER
// The json_To* functoins convert the JSON* to various types, deleting the JSON object in the process.
// Note that this is the only proper way to deal with strings and objects as:
//
//		const char *myString = json_ToString(j); json_Delete(j);
//
// would actually deallocate the returned string.
// END HEADER

API const char *json_ToString(JSON *j, int *plen)
// Returns a string on the heap.
// NB. This also works for errors
{
	const char *result = NULL;

	if (j && (j->type == JSON_STRING || j->type == JSON_ERROR)) {
		if (plen) *plen = j->data.string.len;
		result = j->data.string.string;
		j->data.string.string = NULL;			// Make sure it doesn't get deleted when we delete the container
	}

	json_Delete(j);

	return result;
}

API long long json_ToInteger(JSON *j)
{
	long long result = json_AsInteger(j);
	json_Delete(j);
	return result;
}

API double json_ToFloat(JSON *j)
{
	double result = json_AsInteger(j);
	json_Delete(j);
	return result;
}

API int json_ToBool(JSON *j)
{
	int result = json_AsBool(j);
	json_Delete(j);

	return result;
}

API SPMAP *json_ToObject(JSON *j)
{
	SPMAP *result = NULL;

	if (j && j->type == JSON_OBJECT) {
		result = j->data.object;
		j->data.object = NULL;				// Make sure we don't delete the object we're returning
	}
	json_Delete(j);

	return result;
}

// START HEADER
// Array related functions
// END HEADER

API int json_ArrayCount(JSON *j)
// Returns the number of elements in a JSON array
{
	return j ? j->data.array.count : 0;
}

API JSON *json_ArrayAt(JSON *j, int n)
// Returns the object at index n of the JSON array.
// If n is outside of the array, or the JSON* isn't an array, returns NULL.
{
	JSON *result = NULL;

	if (j && j->type == JSON_ARRAY) {
		if (n >= 0 && n < j->data.array.count)
		   result = j->data.array.values[n];
	}

	return result;
}

API long long json_ArrayIntegerAt(JSON *j, int n)
// Returns the integer at position n of the JSON* array.
{
	return json_AsInteger(json_ArrayAt(j, n));
}

API const char *json_ArrayStringAt(JSON *j, int n, int *plen)
// Returns the string at position n of the JSON* array.
{
	return json_AsString(json_ArrayAt(j, n), plen);
}

API SPMAP *json_ArrayObjectAt(JSON *j, int n)
// Returns the object at position n of the JSON* array.
{
	return json_AsObject(json_ArrayAt(j, n));
}

API JSON *json_ArrayTakeAt(JSON *j, int n)
// Removes element the element at position n of the array.
// The element is returned as for json_ArrayAt() but it is also taken out of the array.
{
	JSON *result = NULL;

	if (j && j->type == JSON_ARRAY) {
		int count = j->data.array.count;

		if (n>= 0 && n < count) {
			JSON **values = j->data.array.values;

			result = values[n];
			result->parent = NULL;
			json_LinkIn(result);

			if (n < count-1)
				memmove(values+n, values+n+1, sizeof(JSON*)*(count-1-n));
			count--;
			j->data.array.values = (JSON**)realloc(values, (sizeof(JSON*)*count));
			j->data.array.count = count;
		}
	}

	return result;
}

API void json_ArrayDeleteAt(JSON *j, int n)
// Removes and deletes element n of the JSON* array.
{
	JSON *json = json_ArrayTakeAt(j, n);
	json_Delete(json);
}

API long long json_ArrayTakeIntegerAt(JSON *j, int n)
// Returns the integer at element n of the JSON* array, removing that element of the array
{
	JSON *element = json_ArrayTakeAt(j, n);
	return json_ToInteger(element);
}

API const char *json_ArrayTakeStringAt(JSON *j, int n, int *plen)
// Returns a string on the heap that is held at position n of the JSON* array
// Note that the element is removed from the array (and thrown away) even if it's not a string,
// in which case we'll return NULL here.
{
	// This is more tricky then might be imagined...

	const char *result = NULL;

	JSON *element = json_ArrayTakeAt(j, n);
	if (element && (element->type == JSON_STRING || element->type == JSON_ERROR)) {
		if (*plen) *plen = element->data.string.len;
		result = element->data.string.string;
		element->data.string.string = NULL;
	}
	json_Delete(element);

	return result;
}

API SPMAP *json_ArrayTakeObjectAt(JSON *j, int n)
// Returns the Object at the array element, removing it from the array.
// Note that the element is removed from the array (and thrown away) even if it's not an object,
// in which case we'll return NULL here.
{
	SPMAP *result = NULL;

	JSON *element = json_ArrayTakeAt(j, n);
	if (element && element->type == JSON_OBJECT) {
		result = element->data.object;
		element->data.object = NULL;
	}
	json_Delete(element);

	return result;
}

API JSON *json_ArrayAdd(JSON *j, JSON *item)
// Adds a json structure to an existing JSON array
{
	if (j && j->type == JSON_ARRAY) {
		json_Release(item);

		int count = j->data.array.count;

		if (count) {
			RENEW(j->data.array.values, JSON*, count+1);
		} else {
			j->data.array.values = NEW(JSON*, 1);
		}
		j->data.array.values[count]=item;
		j->data.array.count = count+1;
		item->parent = j;

		json_LinkOut(item);

		return item;
	}

	return NULL;
}

API JSON *json_ArraySetAt(JSON *j, int pos, JSON *item)
// pos is indexed from 0 and can be 0..count.  If pos==count, this becomes json_ArrayAdd()
// Any previous occupant of the array position is destroyed.
// Inserting the item already there should work OK.
{
	if (j && item && pos >= 0 && j->type == JSON_ARRAY) {
		int count = j->data.array.count;

		if (pos > count) return NULL;
		if (pos == count) return json_ArrayAdd(j, item);

		JSON *oldItem = j->data.array.values[pos];
		if (oldItem != item) {									// It would be bad if we're inserting the item already there
			json_Release(item);

			if (pos == j->data.array.count) {					// This can happen if the item is elsewhere in the SAME array
				return json_ArrayAdd(j, item);
			}

			if (oldItem) {										// Should never be NULL in fact
				oldItem->parent = NULL;							// Stop it from messing us up
				json_Delete(oldItem);
			}
			j->data.array.values[pos] = item;
			item->parent = j;
			json_LinkOut(item);
		}

		return item;
	}

	return NULL;
}

API JSON *json_ArraySetIntegerAt(JSON *j, int pos, long long value)
{
	return json_ArraySetAt(j, pos, json_NewInteger(value));
}

API JSON *json_ArrayAddInteger(JSON *j, long long n)
{
	return json_ArrayAdd(j, json_NewInteger(n));
}

API JSON *json_ArrayAddFloat(JSON *j, double n)
{
	return json_ArrayAdd(j, json_NewFloat(n));
}

API JSON *json_ArrayAddString(JSON *j, int len, const char *s)
{
	return json_ArrayAdd(j, json_NewString(len, s));
}

// START HEADER
// Object related functions
// END HEADER

API int json_ObjectCount(JSON *j)
{
	return (j && j->type == JSON_OBJECT) ? spmap_Count(j->data.object) : 0;
}

API JSON *json_ObjectElementAt(JSON *j, int n)
// The elements in an object can be accessed by number, remembering the is no ordering to the set of objects.
// Also, the ordering may change if any element is added or removed from the object so beware.
{
	return (j && j->type == JSON_OBJECT) ? (JSON*)spmap_GetValueAtIndex(j->data.object, n) : NULL;
}

API const char *json_ObjectNameAt(JSON *j, int n)
// Returns the name of the 'nth' element of the object.
// See the comments against json_ObjectElementAt()
{
	return (j && j->type == JSON_OBJECT) ? spmap_GetKeyAtIndex(j->data.object, n) : NULL;
}

API JSON *json_ObjectAdd(JSON *j, const char *name, JSON *item)
// Adds an item to an Object.
// If there was already an item with this name, it'll be deleted, unless it's this new item in which case nothing happens.
{
	if (j && j->type == JSON_OBJECT) {
		JSON *oldItem = spmap_GetValue(j->data.object, name);			// Can't have two items with the same name

		if (oldItem != item) {		// Things otherwise work out bad if we add an item to an object that it already belongs to...
			json_Delete(oldItem);

			json_Release(item);
			spmap_Add(j->data.object, name, item);
			item->parent = j;

			json_LinkOut(item);
		}
	}

	return item;
}

API JSON *json_ObjectAddInteger(JSON *j, const char *s, long long n)
{
	return json_ObjectAdd(j, s, json_NewInteger(n));
}

API JSON *json_ObjectAddFloat(JSON *j, const char *s, long long n)
{
	return json_ObjectAdd(j, s, json_NewFloat(n));
}

API JSON *json_ObjectAddString(JSON *j, const char *name, int len, const char *string)
{
	return json_ObjectAdd(j, name, json_NewString(len, string));
}

API JSON *json_ObjectAddBool(JSON *j, const char *name, int truth)
{
	return json_ObjectAdd(j, name, json_NewBool(truth));
}

API long long json_ObjectIntegerCalled(JSON *j, const char *name)
{
	// Ah, the bliss of having both json_ and spmap_ NULL safe!
	return json_AsInteger(spmap_GetValue(json_AsObject(j), name));
}

API double json_ObjectFloatCalled(JSON *j, const char *name)
{
	// Ah, the bliss of having both json_ and spmap_ NULL safe!
	return json_AsFloat(spmap_GetValue(json_AsObject(j), name));
}

API const char *json_ObjectStringCalled(JSON *j, const char *name, int *plen)
{
	// Ah, the bliss of having both json_ and spmap_ NULL safe!
	return json_AsString(spmap_GetValue(json_AsObject(j), name), plen);
}

API JSON *json_ObjectElementCalled(JSON *j, const char *name)
{
	// Ah, the bliss of having both json_ and spmap_ NULL safe!
	return spmap_GetValue(json_AsObject(j), name);
}

API int json_ObjectBoolCalled(JSON *j, const char *name)
// Returns the name of the 'nth' element of the object.
// See the comments against json_ObjectElementAt()
{
	return json_AsBool(spmap_GetValue(json_AsObject(j), name));
}

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
// Returns a new JSON structure with no content
{
	JSON *j = NEW(JSON, 1);
	memset(j, 0, sizeof(*j));

	j->signature = SIGN_JSON;		// JSON
	j->parent = NULL;
	j->type = type;

	j->prev = NULL;
	j->next = NULL;
	json_LinkIn(j);

	return j;
}

API JSON *json_NewError(const char *string)
// Returns a JSON structure as an error
{
	JSON *j = json_New(JSON_ERROR);
	if (!string) string = "NULL Error???";
	j->data.string.len = strlen(string);;
	j->data.string.string = strdup(string);

	return j;
}

API JSON *json_NewErrorf(const char *fmt, ...)
// As json_NewError() but with a format string
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
// len == -1 implies strlen(string)
{
	JSON *j = json_New(JSON_STRING);
	if (!string) len = 0;
	if (len == -1) len = strlen(string);
	j->data.string.len = len;
	j->data.string.string = string;

	return j;
}

API JSON *json_NewString(int len, const char *string)
// Returns a new JSON string taken from a static string.
// len == -1 implies strlen(string)
{
	if (string) {
		if (len == -1) len = strlen(string);

		char *buf = malloc(len+1);
		memcpy(buf, string, len+1);
		string = buf;
	}

	return json_NewStringHeap(len, string);
}

API JSON *json_NewInteger(long long number)
{
	JSON *j = json_New(JSON_INTEGER);
	j->data.integer = number;

	return j;
}

API JSON *json_NewFloat(double number)
{
	JSON *j = json_New(JSON_FLOAT);
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

API JSON *json_NewObjectWith(SPMAP *map)
// Returns a new JSON object with the content of the provided map or empty if map is NULL
// The returned object now owns the map
{
	JSON *j = json_New(JSON_OBJECT);
	j->data.object = map ? map : spmap_New();

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

API JSON *json_Copy(JSON *json)
// Returns a copy of the JSON* passed.
{
	JSON *j = json_New(json->type);

	switch (json->type) {
		case JSON_ERROR:
		case JSON_STRING:
		{
			int len = json->data.string.len;

			j->data.string.len = len;
			j->data.string.string = (char*)malloc(len);
			memcpy((void*)j->data.string.string, json->data.string.string, len);
		}
			break;
		case JSON_INTEGER:
			j->data.integer = json->data.integer;
			break;
		case JSON_FLOAT:
			j->data.number = json->data.number;
			break;
		case JSON_NULL:
			break;
		case JSON_BOOL:
			j->data.truth = json->data.truth;
			break;
		case JSON_ARRAY:
		{
			int i;

			j->data.array.count = json->data.array.count;
			j->data.array.values = (JSON **)malloc(json->data.array.count * sizeof(JSON*));
			for (i=0; i<json->data.array.count; i++) {
				j->data.array.values[i] = json_Copy(json->data.array.values[i]);
			}
		}
			break;
		case JSON_OBJECT:
		{
			j->data.object = spmap_New();
			const char *name;
			JSON *value;
			spmap_Reset(json->data.object);
			while (spmap_GetNextEntry(json->data.object, &name, (void**)&value)) {
				spmap_Add(j->data.object, name, json_Copy(value));
			}
		}
	}

	return j;
}

STATIC const char *Codepoint2Utf8(long code, int *plen)
// Returns a string that is the UTF-8 representation of the code point passed
{
	static char buf[5];
	int len=0;

	if (code <= 0x7f) {					// 1 byte
		len = 1;
		buf[0] = (char)code;
		buf[1] = '\0';
	} else if (code <= 0x7ff) {			// 2 bytes
		len = 2;
		buf[0] = 0xc0 | ((code >> 6) & 0x3f);
		buf[1] = 0x80 | (code & 0x3f);
		buf[2] = '\0';
	} else if (code <= 0xffff) {		// 3 bytes
		len = 3;
		buf[0] = 0xe0 | ((code >> 12) & 0x3f);
		buf[1] = 0x80 | ((code >> 6) & 0x3f);
		buf[2] = 0x80 | (code & 0x3f);
		buf[3] = '\0';
	} else if (code <= 0x1fffffL) {		// 4 bytes
		len = 4;
		buf[0] = 0xf0 | ((code >> 18) & 0x3f);
		buf[1] = 0x80 | ((code >> 12) & 0x3f);
		buf[2] = 0x80 | ((code >> 6) & 0x3f);
		buf[3] = 0x80 | (code & 0x3f);
		buf[4] = '\0';
	} else if (code <= 0x3ffffffL) {	// 5 bytes
		len = 5;
		buf[0] = 0xf8 | ((code >> 24) & 0x3f);
		buf[0] = 0x80 | ((code >> 18) & 0x3f);
		buf[1] = 0x80 | ((code >> 12) & 0x3f);
		buf[2] = 0x80 | ((code >> 6) & 0x3f);
		buf[3] = 0x80 | (code & 0x3f);
		buf[4] = '\0';
	}

	if (plen) *plen = len;

	return buf;
}

STATIC long Utf82Codepoint(const char **ptext)
// Returns a unicode codepoint from a single character that text points at.
// Returns		0...		Codepoint (*ptext incremented past last byte)
//				-1..-6		Error at byte 1..4 (*ptext unchanged)
{
	const unsigned char *text = *(const unsigned char **)ptext;

	int len;
	int mask;
	int i;

	unsigned char c = *text;
	if ((c & 0x80) == 0)			{ len = 1; mask=0x7f; }
	else if ((c & 0xe0) == 0xc0)	{ len = 2; mask=0x1f; }
	else if ((c & 0xf0) == 0xe0)	{ len = 3; mask=0x0f; }
	else if ((c & 0xf8) == 0xf0)	{ len = 4; mask=0x07; }
	else if ((c & 0xfc) == 0xf8)	{ len = 5; mask=0x03; }
	else if ((c & 0xfe) == 0xfc)	{ len = 6; mask=0x01; }
	else return -1;								// Illegal first byte

	for (i = 1; i < len; i++) {					// Continuation bytes must be 0b10xxxxxx
		if ((text[i] & 0xc0) != 0x80) {
			return -i-1;
		}
	}

	long codepoint = 0;
	for (i = 0; i < len; i++) {
		codepoint = (codepoint << 6) | (text[i] & mask);
		mask = 0x3f;
	}

	*ptext += len;

	return codepoint;
}

STATIC const char *json_RenderString(int len, const char *string)
// Returns a rendered string in the heap including surrounding " marks
{
	HBUF *h = hbuf_New();

	hbuf_AddChar(h, '"');

	if (len < 0) len = strlen(string);
	char c;

	const char *end = string+len;
	while (string < end) {
		c = *string;

//{int i;printf("Str = ");for (i=0;i<6;i++) { printf("%02x", (((unsigned char *)string)[i]));} printf("\n"); }
		if (!(c & 0x80)) {									// 7-bit characters
			if (c == '\\' || c == '"') {					// Plain chars that need escaping
				hbuf_AddChar(h, '\\');
				hbuf_AddChar(h, c);
			} else if (c & 0xe0) {							// Other plain characters
				hbuf_AddChar(h, c);
			} else {										// Control characters
				if (c == '\b') hbuf_AddBuffer(h, 2, "\\b");			// A few specials that have shorthand
				else if (c == '\f') hbuf_AddBuffer(h, 2, "\\f");
				else if (c == '\n') hbuf_AddBuffer(h, 2, "\\n");
				else if (c == '\r') hbuf_AddBuffer(h, 2, "\\r");
				else if (c == '\t') hbuf_AddBuffer(h, 2, "\\t");
				else {												// Other control characters (including \u0000)
					char buf[7];

					snprintf(buf, sizeof(buf), "\\u%04x", c);
					hbuf_AddBuffer(h, 6, buf);
				}
			}
			string++;
			len--;
		} else {											// Chars that are not 00-7f
			long codepoint = Utf82Codepoint(&string);

			if (codepoint >= 0) {							// Happily found our codepoint
				char buf[7];

				if (codepoint <= 0xffff) {
					sprintf(buf, "\\u%04lx", codepoint);
					hbuf_AddBuffer(h, 6, buf);
				} else {
					codepoint -= 0x10000;

					sprintf(buf, "\\u%04lx", 0xd800 + (codepoint >> 10));
					hbuf_AddBuffer(h, 6, buf);
					sprintf(buf, "\\u%04lx", 0xdc00 + (codepoint & 0x3ff));
					hbuf_AddBuffer(h, 6, buf);
				}
			} else {										// Illegal UTF-8...
				hbuf_AddBuffer(h, 6, "\\u????");			// Render it illegally
				break;										// And stop trying any further
			}
		}
	}

	// Here...
	// string = end				Normal and our string rendered ok
	// string < end				An illegal UTF-8 character was found
	// string > end				The last character was multi-byte and over-stretched the length

	hbuf_AddChar(h, '"');
	hbuf_AddChar(h, '\0');

	const char *result = hbuf_ReleaseBuffer(h);
	hbuf_Delete(h);

	return result;
}

static renderLevel = 0;					// Current indentation level

STATIC json_RenderIndent(HBUF *h, int increment)
// Indents (increment=1) or outdents (increment=-1) json into HBUF
{
	int i;

	renderLevel += increment;

	hbuf_AddChar(h, '\n');
	for (i=0;i<renderLevel;i++)
		hbuf_AddBuffer(h, 2, "  ");
}

API const char *json_RenderHeap(JSON *json, int pretty)
// Renders an entire JSON structure, returning a string on the heap.
// If 'pretty' then the result is being pretty printed (renderLevel will need to have been set externally)
// Passing in a NULL results in a blank string.
{
	HBUF *h = hbuf_New();
	int i;

	if (json) {
		switch (json->type) {
		case JSON_ERROR:
			hbuf_AddHeap(h, -1, hprintf(NULL, "(ERROR: %s)", json->data.string.string));
			break;
		case JSON_INTEGER:
			hbuf_AddHeap(h, -1, hprintf(NULL, "%Ld", json->data.integer));
			break;
		case JSON_FLOAT:
			hbuf_AddHeap(h, -1, hprintf(NULL, "%.16g", json->data.number));
			break;
		case JSON_NULL:
			hbuf_AddBuffer(h, 4, "null");
			break;
		case JSON_BOOL:
			hbuf_AddBuffer(h, -1, json->data.truth ? "true" : "false");
			break;
		case JSON_ARRAY:
			hbuf_AddChar(h, '[');
			if (json->data.array.count) {				// Something to render
				if (pretty) json_RenderIndent(h, 1);

				int i;
				for (i=0;i<json->data.array.count;i++) {
					if (i) {
						hbuf_AddChar(h, ',');
						if (pretty) json_RenderIndent(h, 0);
					}
					hbuf_AddHeap(h, -1, json_RenderHeap(json->data.array.values[i], pretty));
				}
				if (pretty) json_RenderIndent(h, -1);
			}
			hbuf_AddChar(h, ']');
			break;
		case JSON_OBJECT:
			hbuf_AddChar(h, '{');

			if (json->data.object && spmap_Count(json->data.object)) {		// Something to render
				if (pretty) json_RenderIndent(h, 1);
				int count = 0;

				spmap_Reset(json->data.object);
				const char *name;
				JSON *value;
				while (spmap_GetNextEntry(json->data.object, &name, (void**)&value)) {
					if (count++) {
						hbuf_AddChar(h, ',');
						if (pretty) json_RenderIndent(h, 0);
					}
					hbuf_AddHeap(h, -1, json_RenderString(-1, name));
					hbuf_AddChar(h, ':');
					if (pretty) hbuf_AddChar(h, ' ');
					hbuf_AddHeap(h, -1, json_RenderHeap(value, pretty));
				}

				if (pretty) json_RenderIndent(h, -1);
			}
			hbuf_AddChar(h, '}');
			break;
		case JSON_STRING:
			{
				hbuf_AddHeap(h, -1, json_RenderString(json->data.string.len, json->data.string.string));
				break;
			}
		}
	}

	hbuf_AddChar(h, '\0');
	const char *result = hbuf_ReleaseBuffer(h);
	hbuf_Delete(h);

	return result;
}


API const char *json_Render(JSON *json)
// As json_RenderHeap but the returned string is managed by this function (so don't go free()ing it!)
// If you want to de-allocate the memory, call this function with NULL.
{
	static const char *result = NULL;
	szDelete(result);

	result = json ? json_RenderHeap(json, 0) : NULL;

	return result;
}

API const char *json_RenderPretty(JSON *json)
// As json_Render but returns a 'prettyprinted' form with linefeeds and indents
{
	renderLevel = 0;

	static const char *result = NULL;
	szDelete(result);

	result = json ? json_RenderHeap(json, 1) : NULL;

	return result;
}

STATIC const char *json_ParseQuotedString(const char **ptext, int *plen)
// Fetches a string on the heap, returning the length in *plen.
// Should be pointing at the initial ", will end pointing just past the final "
// If *plen is -1 on return then the string returned is an error message, otherwise it's the length of the parsed string.
{
	if (!ptext) return NULL;

	const char *result;

	const char *text=SkipSpaces(*ptext);
	if (*text++ != '"') {
		if (plen) *plen=-1;
		return strdup("Expected leading \"");
	}

	HBUF *hbuf = hbuf_New();
	char c;
	const char *err = NULL;

	const char *surrogatePending = NULL;		// place in text of high surrogate when surrogate high received (next should be low)
	long surrogateHigh = 0;						// Codepoint of high part of surrogate 

	while ((c=*text) && c != '"' && !err) {
		const char *surrogateWasPending = surrogatePending;		// Watch for it now being handled
		const char *startText = text++;

		if (c != '\\') {
			hbuf_AddChar(hbuf, c);
		} else {
			c=*text++;

			switch (c) {
				case '\0':
					err = strdup("Trailing \\ in string");
					break;
				case '"': case '\\': case '/':	hbuf_AddChar(hbuf, c); break;			// \", \\ or \/ (dunno why \/ is valid!)
				case 'b':						hbuf_AddChar(hbuf, '\b'); break;		// Backspace
				case 'f':						hbuf_AddChar(hbuf, '\f'); break;		// Formfeed
				case 'n':						hbuf_AddChar(hbuf, '\n'); break;		// Linefeed
				case 'r':						hbuf_AddChar(hbuf, '\r'); break;		// Return
				case 't':						hbuf_AddChar(hbuf, '\t'); break;		// Tab
				case 'u':
				{
					int i;
					long codepoint = 0;

					for (i=0;i<4;i++) {
						c = *text++;
						int n = 0;

						if (c >= '0' && c <= '9') n = c-'0';
						else if (c >= 'a' && c <= 'f') n = c-'a'+10;
						else if (c >= 'A' && c <= 'F') n = c-'A'+10;
						else {
							err = hprintf(NULL, "Bad \\u escape (%.6s)", startText);
							break;
						}
						codepoint = (codepoint << 4) | n;
					}
					if (err)
						break;

					if (codepoint >= 0xd800 && codepoint <= 0xdbff) {					// High surrogate
						surrogateHigh = codepoint;
						surrogatePending = startText;
					} else {
						if (codepoint >= 0xdc00 && codepoint <= 0xdfff) {			// Low surrogate
							if (!surrogatePending) {
								err = hprintf(NULL, "Low surrogate received without high (%-.6s)", startText);
								break;
							}
							codepoint = 0x10000
								+ ((surrogateHigh & 0x3ff) << 10)
								+ (codepoint & 0x3ff);
							surrogatePending = NULL;
						}

						int len;
						const char *str = Codepoint2Utf8(codepoint, &len);
						hbuf_AddBuffer(hbuf, len, str);
					}
				}
					break;
				default:
					err = hprintf(NULL, "Unrecognised escape \\%c", c);
					break;
			}
		}

		if (surrogateWasPending && surrogatePending) {
			err = hprintf(NULL, "High surrogate not followed by low surrogate (%-.6s)", surrogatePending);
			break;
		}
	}

	if (!err && surrogatePending) {
		err = hprintf(NULL, "High surrogate at end of string (%-.6s)", surrogatePending);
	}

	if (err) {
		if (plen) *plen = -1;
		hbuf_Delete(hbuf);
		return err;
	}

	if (*text)						// Step over if we hit closing ", not if we hit end of string
		text++;
	hbuf_AddChar(hbuf, 0);
	*ptext = text;
	if (plen) *plen = hbuf_GetLength(hbuf)-1;
	result = hbuf_ReleaseBuffer(hbuf);

	hbuf_Delete(hbuf);

	return result;
}

API JSON *json_Parse(const char **ptext)
// Given a pointer to a string, will parse the string as JSON, returning a JSON structure.
// The string point will then be moved to just after the parsed string.
// Note this never returns NULL unless a NULL ptext is passed.
// If ptext points to a NULL, then a json_Error() will be returned.
{
	if (!ptext) return NULL;

	const char *text=SkipSpaces(*ptext);
	JSON *json = NULL;

	if (*text) {
		char c = *text;
		if (c == '"') {														// String
			int len;
			const char *string = json_ParseQuotedString(&text, &len);
			if (len == -1) {
				json = json_NewError(string);
			} else {
				json = json_NewStringHeap(len, string);						// +1 to include the \0 after it
			}
		} else if (c == '[') {												// Array
			json = json_NewArray();

			text++;
			c = ',';
			while (c == ',') {
				while (isspace(*text)) text++;

				if (*text != ']') {
					JSON *value = json_Parse(&text);

					if (json_Error(value)) {
						json_Delete(json);
						return value;
					}
					json_ArrayAdd(json, value);

					while (isspace(*text)) text++;
				}
				c = *text++;
			}
			if (c != ']') {
				json_Delete(json);
				json = json_NewError("Expected ] at end of array");
				text--;
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
					const char *name = json_ParseQuotedString(&text, &len);

					if (len == -1) {
						err = name;
						break;
					}

					while (isspace(*text)) text++;
					c = *text++;
					if (c != ':') {
						err = strdup("Expected : within object");
						text--;
						break;
					}

					JSON *value = json_Parse(&text);
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
				text--;
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
				} else if (c == '+') {
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
			JSON *j = json_NewErrorf("%s at >>%.15s<<", json_AsStringz(json), text);
			json_Delete(json);
			json = j;
		}
	} else { // if (text)
		json = json_NewError("NULL JSON");
	}

	*ptext = text;

	return json;
}

API JSON *json_ParseText(const char *text)
// Acts as for json_Parse but takes a pointer to a simple string, returning an error if there is anything other than
// whitespace after the end of the JSON text.
// Always test the result using json_Error(result) to spot problems.
// Never returns NULL.
{
	JSON *json = json_Parse(&text);
	if (!json_Error(json)) {
		text = SkipSpaces(text);
		if (*text) {
			json_Delete(json);
			json = json_NewErrorf("Characters after JSON text, starting at >>%.15s<<", text);
		}
	}

	return json;
}
