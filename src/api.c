#include "redis.h"
#include "api.h"

static bool incrDecrCommand(redisDb *db, robj *key, long long incr) {
	long long value, oldvalue;
	robj *o, *new;

	o = lookupKeyWrite(db, key);
	if (o != NULL && checkType(o, REDIS_STRING))
		return false;
	if (getLongLongFromObject(o, &value) != REDIS_OK)
		return false;

	oldvalue = value;
	if ((incr < 0 && oldvalue < 0 && incr < (LLONG_MIN - oldvalue))
			|| (incr > 0 && oldvalue > 0 && incr > (LLONG_MAX - oldvalue))) {
		return false;
	}
	value += incr;
	new = createStringObjectFromLongLong(value);
	if (o)
		dbOverwrite(db, key, new);
	else
		dbAdd(db, key, new);

	return true;
}

void get(const char *k) {
    robj *key = createStringObject(k, strlen(k));
    robj *o = lookupKeyRead(db, key);
    if (!o) {
    	return;
    } else {
    	return o;
    }
}

void gets() {

}

bool set(const char *k, const char *v, long expire) {
	robj *key = createStringObject(k, strlen(k));
	robj *val = createStringObject(v, strlen(v));
	setKey(db, key, val);
	if (expire)
		setExpire(db, key, expire);
	return true;
}

bool add(const char *k, const char *v, long expire) {
	robj *key = createStringObject(k, strlen(k));
	robj *val = createStringObject(v, strlen(v));
	if (lookupKeyWrite(db, key) != NULL) {
		return false;
	}
	setKey(db, key, val);
	if (expire)
		setExpire(db, key, expire);
	return true;
}

bool replace(const char *k, const char *v, long expire) {
	robj *key = createStringObject(k, strlen(k));
	robj *val = createStringObject(v, strlen(v));
	if (lookupKeyWrite(db, key) == NULL) {
		return false;
	}
	setKey(db, key, val);
	if (expire)
		setExpire(db, key, expire);
	return true;
}

void append(const char *k, const char *suffix,) {
	size_t totlen;
	robj *o, *append;
	robj *key = createStringObject(k, strlen(k));

	o = lookupKeyWrite(db, key);
	if (o == NULL) {
		robj *suffix_obj = createStringObject(suffix, strlen(suffix));
		/* Create the key */
		suffix_obj = tryObjectEncoding(suffix_obj);
		dbAdd(db, key, suffix_obj);
		incrRefCount(suffix_obj);
		totlen = stringObjectLen(suffix_obj);
	} else {
		/* Key exists, check type */
		if (checkType(o, REDIS_STRING))
			return;

		/* "append" is an argument, so always an sds */
		append = c->argv[2];
		totlen = stringObjectLen(o) + sdslen(append->ptr);
		if (checkStringLength(c, totlen) != REDIS_OK)
			return;

		/* Append the value */
		o = dbUnshareStringValue(db, key, o);
		o->ptr = sdscatlen(o->ptr, append->ptr, sdslen(append->ptr));
		totlen = sdslen(o->ptr);
	}
}

void prepend(const char *k, const char *prefix) {
	size_t totlen;
	robj *o, *append;
	robj *key = createStringObject(k, strlen(k));
	robj *prefix_obj = createStringObject(prefix, strlen(prefix));

	o = lookupKeyWrite(db, key);
	if (o == NULL) {
		/* Create the key */
		c->argv[2] = tryObjectEncoding(c->argv[2]);
		dbAdd(c->db, c->argv[1], c->argv[2]);
		incrRefCount(c->argv[2]);
		totlen = stringObjectLen(c->argv[2]);
	} else {
		/* Key exists, check type */
		if (checkType(o, REDIS_STRING))
			return;

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
	robj *key = createStringObject(k, strlen(k));
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
	robj *key = createStringObject(k, strlen(k));
	bool ret = incrDecrCommand(db, key, 1);
	return ret;
}

bool decr(const char *k) {
	robj *key = createStringObject(k, strlen(k));
	bool ret = incrDecrCommand(db, key, -1);
	return ret;
}

void flush_all() {
    emptyDb(NULL);
}
