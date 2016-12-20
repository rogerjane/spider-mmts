#ifndef __MTJSON_H
#define __MTJSON_H


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


void json_Logger(void (*logger)(const char *, va_list));
// Sets a logging function to receive all debug output from the JSON library

void json_LogAll();
void json_LogAll();
// Calls the 'Log()' function for every allocated JSON structure

void json_Delete(JSON *json);
// Delete the JSON object, which will safely remove it from any containing JSON

int json_Type(JSON *j);
int json_IsString(JSON *j);
int json_IsNumber(JSON *j);
int json_IsInteger(JSON *j);
int json_IsFloat(JSON *j);
int json_IsArray(JSON *j);
int json_IsObject(JSON *j);
int json_IsNull(JSON *j);
int json_IsBool(JSON *j);
int json_StringLength(JSON *j);
// Returns the length of the string in bytes.  Note that this may not be the same as characters for UTF-8 strings.
// Get as various types.  If the type you're trying to return isn't compatible with the JSON type then you'll get
// 0, NULL etc.  This also applies if the JSON* passed in is NULL.


long long json_AsInteger(JSON *j);
double json_AsFloat(JSON *j);
const char *json_AsString(JSON *j, int *plen);
// Returns a pointer to the internally held string, which is always '\0' terminated.
// Note, however, that the string may contain '\0' characters so *plen gets the actual length in bytes (excluding the '\0')
// If the string is NULL, then *plen will get 0 and the returned string will be NULL.
// Note that this function also works to return the string associated with an error.

int json_AsBool(JSON *j);
// Returns 1 or 0 depending on the 'truth' of the boolean
// NB. Returns -1 if the JSON* is not a bool, which will probably evaluate to 'true' but indicates you have something wrong...

SPMAP *json_AsObject(JSON *j);
// Returns an SPMAP, which maps names onto JSON* objects.
// NB. Returns NULL if the JSON* passed is not an object.
// The json_To* functoins convert the JSON* to various types, deleting the JSON object in the process.
// Note that this is the only proper way to deal with strings and objects as:
//
//		const char *myString = json_ToString(j); json_Delete(j);
//
// would actually deallocate the returned string.


const char *json_ToString(JSON *j, int *plen);
// Returns a string on the heap.
// NB. This also works for errors

long long json_ToInteger(JSON *j);
double json_ToFloat(JSON *j);
int json_ToBool(JSON *j);
SPMAP *json_ToObject(JSON *j);
// Array related functions

int json_ArrayCount(JSON *j);
// Returns the number of elements in a JSON array

JSON *json_ArrayAt(JSON *j, int n);
// Returns the object at index n of the JSON array.
// If n is outside of the array, or the JSON* isn't an array, returns NULL.

long long json_ArrayIntegerAt(JSON *j, int n);
// Returns the integer at position n of the JSON* array.

const char *json_ArrayStringAt(JSON *j, int n, int *plen);
// Returns the string at position n of the JSON* array.

SPMAP *json_ArrayObjectAt(JSON *j, int n);
// Returns the object at position n of the JSON* array.

JSON *json_ArrayTakeAt(JSON *j, int n);
// Removes element the element at position n of the array.
// The element is returned as for json_ArrayAt() but it is also taken out of the array.

void json_ArrayDeleteAt(JSON *j, int n);
// Removes and deletes element n of the JSON* array.

long long json_ArrayTakeIntegerAt(JSON *j, int n);
// Returns the integer at element n of the JSON* array, removing that element of the array

const char *json_ArrayTakeStringAt(JSON *j, int n, int *plen);
// Returns a string on the heap that is held at position n of the JSON* array
// Note that the element is removed from the array (and thrown away) even if it's not a string,
// in which case we'll return NULL here.

SPMAP *json_ArrayTakeObjectAt(JSON *j, int n);
// Returns the Object at the array element, removing it from the array.
// Note that the element is removed from the array (and thrown away) even if it's not an object,
// in which case we'll return NULL here.

JSON *json_ArrayAdd(JSON *j, JSON *item);
// Adds a json structure to an existing JSON array

JSON *json_ArraySetAt(JSON *j, int pos, JSON *item);
// pos is indexed from 0 and can be 0..count.  If pos==count, this becomes json_ArrayAdd()
// Any previous occupant of the array position is destroyed.
// Inserting the item already there should work OK.

JSON *json_ArraySetIntegerAt(JSON *j, int pos, long long value);
JSON *json_ArrayAddInteger(JSON *j, long long n);
JSON *json_ArrayAddFloat(JSON *j, double n);
JSON *json_ArrayAddString(JSON *j, int len, const char *s);
// Object related functions

int json_ObjectCount(JSON *j);
JSON *json_ObjectElementAt(JSON *j, int n);
// The elements in an object can be accessed by number, remembering the is no ordering to the set of objects.
// Also, the ordering may change if any element is added or removed from the object so beware.

const char *json_ObjectNameAt(JSON *j, int n);
// Returns the name of the 'nth' element of the object.
// See the comments against json_ObjectElementAt()

JSON *json_ObjectAdd(JSON *j, const char *name, JSON *item);
// Adds an item to an Object.
// If there was already an item with this name, it'll be deleted, unless it's this new item in which case nothing happens.

JSON *json_ObjectAddInteger(JSON *j, const char *s, long long n);
JSON *json_ObjectAddFloat(JSON *j, const char *s, long long n);
JSON *json_ObjectAddString(JSON *j, const char *name, int len, const char *string);
JSON *json_ObjectAddBool(JSON *j, const char *name, int truth);
long long json_ObjectIntegerCalled(JSON *j, const char *name);
double json_ObjectFloatCalled(JSON *j, const char *name);
const char *json_ObjectStringCalled(JSON *j, const char *name, int *plen);
JSON *json_ObjectElementCalled(JSON *j, const char *name);
int json_ObjectBoolCalled(JSON *j, const char *name);
// Returns the name of the 'nth' element of the object.
// See the comments against json_ObjectElementAt()

const char *json_Error(JSON *j);
// Returns  NULL    There isn't an error
//          char*   Pointer to a static error message

JSON *json_New(int type);
// Returns a new JSON structure with no content

JSON *json_NewError(const char *string);
// Returns a JSON structure as an error

JSON *json_NewErrorf(const char *fmt, ...);
// As json_NewError() but with a format string

JSON *json_NewStringHeap(int len, const char *string);
// The string is on the heap and ownership is transferred to the JSON
// len == -1 implies strlen(string)

JSON *json_NewString(int len, const char *string);
// Returns a new JSON string taken from a static string.
// len == -1 implies strlen(string)

JSON *json_NewInteger(long long number);
JSON *json_NewFloat(double number);
JSON *json_NewArray();
JSON *json_NewObject();
JSON *json_NewObjectWith(SPMAP *map);
// Returns a new JSON object with the content of the provided map or empty if map is NULL
// The returned object now owns the map

JSON *json_NewBool(int truth);
JSON *json_NewNull();
JSON *json_Copy(JSON *json);
// Returns a copy of the JSON* passed.

const char *json_RenderHeap(JSON *json, int pretty);
// Renders an entire JSON structure, returning a string on the heap.
// If 'pretty' then the result is being pretty printed (renderLevel will need to have been set externally)
// Passing in a NULL results in a blank string.

const char *json_Render(JSON *json);
// As json_RenderHeap but the returned string is managed by this function (so don't go free()ing it!)
// If you want to de-allocate the memory, call this function with NULL.

const char *json_RenderPretty(JSON *json);
// As json_Render but returns a 'prettyprinted' form with linefeeds and indents

JSON *json_Parse(const char **ptext);
// Given a pointer to a string, will parse the string as JSON, returning a JSON structure.
// The string point will then be moved to just after the parsed string.
// Note this never returns NULL unless a NULL ptext is passed.
// If ptext points to a NULL, then a json_Error() will be returned.

JSON *json_ParseText(const char *text);
// Acts as for json_Parse but takes a pointer to a simple string, returning an error if there is anything other than
// whitespace after the end of the JSON text.
// Always test the result using json_Error(result) to spot problems.
// Never returns NULL.

#endif
