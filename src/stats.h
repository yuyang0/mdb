#ifndef _STATS_H
#define _STATS_H
typedef struct {
	/* Fields used only for stats */
	time_t starttime; /* Server start time */
	long long numcommands; /* Number of processed commands */
	long long expiredkeys; /* Number of expired keys */
	long long evictedkeys; /* Number of evicted keys (maxmemory) */
	long long keyspace_hits; /* Number of successful lookups of keys */
	long long keyspace_misses; /* Number of failed lookups of keys */
	size_t peak_memory; /* Max used memory record */
} stats_t;

extern stats_t stats;
#endif
