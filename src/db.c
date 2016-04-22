#include "db.h"

value_t *createValue(unsigned encoding, void *p) {
	value_t *val = zmalloc(sizeof(*val));
	val->encoding = encoding;
	val->ver = 0;
	val->ptr = p;
	return val;
}

value_t *createValueFromLongLong(long long v) {
	value_t *val;
	if (v >= LONG_MIN && v <= LONG_MAX) {
		val = createValue(ENCODING_INT, NULL);
		val->ptr = (void*) ((long) v);
	} else {
		val = createValue(ENCODING_RAW, sdsfromlonglong(v));
	}
	return val;
}

value_t *createValueFromStr(void *s, size_t len) {
	long v;
	value_t *val;
	// first we try to encode the string as a long.
	if (len > 21 || !string2l(s, len, &v)) {
		val = createValue(ENCODING_RAW, sdsnewlen(s, len));
	} else {
		val = createValue(ENCODING_INT, NULL);
		val->ptr = (void*) ((long) v);
	}
	return val;
}

void freeValue(value_t *val) {
	if (val->encoding == ENCODING_RAW) {
		sdsfree(val->ptr);
	}
	zfree(val);
}

int getLongLongFromValue(value_t *val, long long *ret) {
	long long v;
	char *eptr;

	if (val == NULL) {
		v = 0;
	} else {
		if (val->encoding == ENCODING_INT) {
			v = (long) val->ptr;
		} else if (val->encoding == ENCODING_RAW) {
			errno = 0;
			v = strtoll(val->ptr, &eptr, 10);
			if (isspace(((char*) val->ptr)[0]) || eptr[0] != '\0'
			|| errno == ERANGE)
				return MDB_ERR;
		} else {
			panic("Unknown encoding");
		}
	}
	*ret = v;
	return MDB_OK;
}

value_t *toStringValue(value_t *val) {
	if (val->encoding == ENCODING_RAW)
		return val;
	long long v = (long) val->ptr;
	sds *p = sdsfromlonglong(v);
	if (p == NULL)
		return NULL;
	val->ptr = p;
	return val;
}

// try encode a string value as integer.
value_t *tryValueEncoding(value_t *val) {
	if (val->encoding == ENCODING_INT)
		return val;
	size_t len = sdslen(val->ptr);
	long v;
	if (len > 21 || !string2l(val->ptr, len, &v)) {
		if (len > 32 && sdsavail(val->ptr) > len / 10) {
			val->ptr = sdsRemoveFreeSpace(val->ptr);
		}
		return val;
	} else {
		val->encoding = ENCODING_INT;
		sdsfree(val->ptr);
		val->ptr = (void*) ((long) v);
	}
}

void incValueVersion(value_t *val) {
	val->ver++;
}

void setValueVersion(value_t *val, unsigned ver) {
	//TODO: check overflow
	val->ver = ver;
}

void resetValueVersion(value_t *val) {
	val->ver = 0;
}

size_t valueLen(value_t *val) {
	if (val->encoding == ENCODING_RAW)
		return 0;
	return sdslen(val->ptr);
}
/*-----------------------------------------------------------------------------
 * C-level DB API
 *----------------------------------------------------------------------------*/
unsigned int dictSdsHash(const void *key) {
	return dictGenHashFunction((unsigned char*) key, sdslen((char*) key));
}

int dictSdsKeyCompare(void *privdata, const void *key1, const void *key2) {
	int l1, l2;
	DICT_NOTUSED(privdata);

	l1 = sdslen((sds) key1);
	l2 = sdslen((sds) key2);
	if (l1 != l2)
		return 0;
	return memcmp(key1, key2, l1) == 0;
}

void dictSdsDestructor(void *privdata, void *val) {
	DICT_NOTUSED(privdata);

	sdsfree(val);
}

void dictValueDestructor(void *privdata, void *val) {
	DICT_NOTUSED(privdata);
	freeValue(val);
}

dictType dbDictType = {
		dictSdsHash, /* hash function */
		NULL, /* key dup */
		NULL, /* val dup */
		dictSdsKeyCompare, /* key compare */
		dictSdsDestructor, /* key destructor */
		dictValueDestructor /* val destructor */
};
/* Db->expires */
dictType keyptrDictType = {
		dictSdsHash, /* hash function */
		NULL, /* key dup */
		NULL, /* val dup */
		dictSdsKeyCompare, /* key compare */
		NULL, /* key destructor */
		NULL /* val destructor */
};

dictType hashSlotType = {
		dictSdsHash, /* hash function */
		NULL, /* key dup */
		NULL, /* val dup */
		dictSdsKeyCompare, /* key compare */
		NULL, /* key destructor */
		NULL /* val destructor */
};

memoryDb *memoryDbNew(int numSlots) {
	memoryDb *db = zmalloc(sizeof(*db));
	db->dict = dictCreate(dbDictType, NULL);
	db->expires = dictCreate(keyptrDictType, NULL);
	db->slots = zmalloc(numSlots * sizeof(dict*));
	int i;
	for (i = 0; i < numSlots; ++i) {
		db->slots[i] = dictCreate(hashSlotType, NULL);
	}
}

value_t *lookupKey(memoryDb *db, sds *key) {
	dictEntry *de = dictFind(db->dict, key);
	if (de) {
		value_t *val = dictGetVal(de);

		return val;
	} else {
		return NULL;
	}
}

value_t *lookupKeyRead(memoryDb *db, sds *key) {
	value_t *val;

	expireIfNeeded(db, key);
	val = lookupKey(db, key);
	if (val == NULL)
		stats.keyspace_misses++;
	else
		stats.keyspace_hits++;
	return val;
}

value_t *lookupKeyWrite(memoryDb *db, sds *key) {
	expireIfNeeded(db, key);
	return lookupKey(db, key);
}

/* Add the key to the DB. It's up to the caller to increment the reference
 * counter of the value if needed.
 *
 * The program is aborted if the key already exists. */
void dbAdd(memoryDb *db, sds *key, value_t *val) {
	sds copy = sdsdup(key);
	resetValueVersion(val);
	int retval = dictAdd(db->dict, copy, val);

	redisAssertWithInfo(NULL, key, retval == MDB_OK);
}

/* Overwrite an existing key with a new value. Incrementing the reference
 * count of the new value is up to the caller.
 * This function does not modify the expire time of the existing key.
 *
 * The program is aborted if the key was not already present. */
void dbOverwrite(memoryDb *db, sds *key, value_t *val) {
	struct dictEntry *de = dictFind(db->dict, key);

	redisAssertWithInfo(NULL, key, de != NULL);
	value_t *oldval = dictGetVal(de);
	setValueVersion(val, oldval->ver+1);
	dictReplace(db->dict, key, val);
}

/* High level Set operation. This function can be used in order to set
 * a key, whatever it was existing or not, to a new object.
 *
 * 1) The expire time of the key is reset (the key is made persistent). */
void setKey(memoryDb *db, sds *key, value_t *val) {
	if (lookupKeyWrite(db, key) == NULL) {
		dbAdd(db, key, val);
	} else {
		dbOverwrite(db, key, val);
	}
	removeExpire(db, key);
}

int dbExists(memoryDb *db, sds *key) {
	return dictFind(db->dict, key) != NULL;
}

/* Delete a key, value, and associated expiration entry if any, from the DB */
int dbDelete(memoryDb *db, sds *key) {
	/* Deleting an entry from the expires dict will not free the sds of
	 * the key, because it is shared with the main dictionary. */
	if (dictSize(db->expires) > 0)
		dictDelete(db->expires, key);
	if (dictDelete(db->dict, key) == DICT_OK) {
		return 1;
	} else {
		return 0;
	}
}

long long emptyDb(memoryDb *db, void (callback)(void*)) {
	int j;
	long long removed = 0;

	removed += dictSize(db.dict);
	dictEmpty(db.dict, callback);
	dictEmpty(db.expires, callback);
	return removed;
}

/*-----------------------------------------------------------------------------
 * Expires API
 *----------------------------------------------------------------------------*/

int removeExpire(memoryDb *db, sds *key) {
	/* An expire may only be removed if there is a corresponding entry in the
	 * main dict. Otherwise, the key will never be freed. */
	redisAssertWithInfo(NULL, key, dictFind(db->dict, key) != NULL);
	return dictDelete(db->expires, key) == DICT_OK;
}

void setExpire(memoryDb *db, sds *key, long long when) {
	dictEntry *kde, *de;

	/* Reuse the sds from the main dict in the expire dict */
	kde = dictFind(db->dict, key);
	redisAssertWithInfo(NULL, key, kde != NULL);
	de = dictReplaceRaw(db->expires, dictGetKey(kde));
	dictSetSignedIntegerVal(de, when);
}

/* Return the expire time of the specified key, or -1 if no expire
 * is associated with this key (i.e. the key is non volatile) */
long long getExpire(memoryDb *db, sds *key) {
	dictEntry *de;

	/* No expire? return ASAP */
	if (dictSize(db->expires) == 0 || (de = dictFind(db->expires, key)) == NULL)
		return -1;

	/* The entry was found in the expire dict, this means it should also
	 * be present in the main dict (safety check). */
	redisAssertWithInfo(NULL, key, dictFind(db->dict, key) != NULL);
	return dictGetSignedIntegerVal(de);
}

int expireIfNeeded(memoryDb *db, sds *key) {
	mstime_t when = getExpire(db, key);
	mstime_t now;

	if (when < 0)
		return 0; /* No expire for this key */

	/* Return when this key has not expired */
	if (now <= when)
		return 0;

	/* Delete the key */
	stats.expiredkeys++;

	return dbDelete(db, key);
}

