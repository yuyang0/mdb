#include "mdb.h"

static memoryDb *db = NULL;

static bool incrDecrCommand(memoryDb *db, sds *key, long long incr) {
	long long value, oldvalue;
	value_t *o, *new;

	o = lookupKeyWrite(db, key);
	if (getLongLongFromValue(o, &value) != MDB_OK)
		return false;

	oldvalue = value;
	if ((incr < 0 && oldvalue < 0 && incr < (LLONG_MIN - oldvalue))
			|| (incr > 0 && oldvalue > 0 && incr > (LLONG_MAX - oldvalue))) {
		return false;
	}
	value += incr;
	new = createValueFromLongLong(value);
	if (o)
		dbOverwrite(db, key, new);
	else
		dbAdd(db, key, new);

	return true;
}

bool initMdb(int numSlots) {
	if (db == NULL) return true;
	db = memoryDbNew(numSlots);
	if (db == NULL) return false;
	else return true;
}

value_t *get(const char *k) {
    sds *key = sdsnew(k);
    value_t *val = lookupKeyRead(db, key);
    if (!val) {
    	return NULL;
    } else {
    	return val;
    }
}

void gets() {

}

bool set(const char *k, const char *v, long expire) {
	sds *key = sdsnew(k);
	value_t *val = createValueFromStr(v, strlen(v));
	setKey(db, key, val);
	if (expire)
		setExpire(db, key, expire);
	return true;
}

bool add(const char *k, const char *v, long expire) {
	sds *key = sdsnew(k);
	value_t *val = createValueFromStr(v, strlen(v));
	if (lookupKeyWrite(db, key) != NULL) {
		return false;
	}
	setKey(db, key, val);
	if (expire)
		setExpire(db, key, expire);
	return true;
}

bool replace(const char *k, const char *v, long expire) {
	sds *key = sdsnew(k);
		value_t *val = createValueFromStr(v, strlen(v));
	if (lookupKeyWrite(db, key) == NULL) {
		return false;
	}
	setKey(db, key, val);
	if (expire)
		setExpire(db, key, expire);
	return true;
}

int append(const char *k, const char *suffix,) {
	size_t totlen;
	value_t *val;
	sds *key = sdsnew(k);

	val = lookupKeyWrite(db, key);
	if (val == NULL) {
		value_t *suffix_val = createValueFromStr(suffix, strlen(suffix));
		/* Create the key */
		dbAdd(db, key, suffix_val);
	} else {
		if (!toStringValue()) return false;
		/* "append" is an argument, so always an sds */
		totlen = stringObjectLen(val) + strlen(suffix);

		/* Append the value */
		val->ptr = sdscatlen(val->ptr, suffix, strlen(suffix));
		totlen = sdslen(val->ptr);
	}
	return totlen;
}

void prepend(const char *k, const char *prefix) {
	size_t totlen;
	value_t *val;
	sds *key = sdsnew(k);
	value_t *prefix_val = createValueFromStr(prefix, strlen(prefix));

	val = lookupKeyWrite(db, key);
	if (val == NULL) {
		/* Create the key */
		dbAdd(db, key, prefix_val);
		totlen = strlen(prefix);
	} else {

		/* "append" is an argument, so always an sds */
		append = c->argv[2];
		totlen = stringObjectLen(o) + sdslen(append->ptr);
		if (checkStringLength(c, totlen) != REDIS_OK)
			return;

		/* Prepend the value */
		o = dbUnshareStringValue(db, key, o);
		sds prefix_sds = sdsnew(prefix);
		sds tmp = o->ptr;
		o->ptr = sdscatsds(prefix_sds, tmp);
		sdsfree(tmp);
		totlen = sdslen(o->ptr);
	}
}

bool delete(const char *k) {
	sds *key = sdsnew(k);
	expireIfNeeded(db, key);
	if (dbDelete(db, key)) {
	    return true;
	} else {
		return false;
	}
}

void cas() {

}

bool incr(const char *k) {
	sds *key = sdsnew(k);
	bool ret = incrDecrCommand(db, key, 1);
	return ret;
}

bool decr(const char *k) {
	sds *key = sdsnew(k);
	bool ret = incrDecrCommand(db, key, -1);
	return ret;
}

void flush_all() {
    emptyDb(db, NULL);
}
