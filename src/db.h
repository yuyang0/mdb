#ifndef _DB_H_
#define _DB_H_

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <limits.h>

typedef long long mstime_t; /* millisecond time type. */

#include "stats.h"
#include "dict.h"

#define MDB_OK   0
#define MDB_ERR  1

typedef struct memoryDb {
	dict *dict; /* The keyspace for this DB */
	dict *expires; /* Timeout of keys with a timeout set */
	dict **slots;
}memoryDb;

#define ENCODING_RAW 0
#define ENCODING_INT 1

typedef struct value_s {
	unsigned encoding:4;
	unsigned ver:28;
	void *ptr;
} value_t;

value_t *createValue(unsigned encoding, void *p);
value_t *createValueFromStr(void *p, size_t len);
value_t *createValueFromLongLong(long long v);
void freeValue(value_t *val);
int getLongLongFromValue(value_t *val, long long *ret);

#endif
