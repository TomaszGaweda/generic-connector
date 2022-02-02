package com.github.tomaszgaweda.rocksdb;

import com.hazelcast.map.MapStore;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages open connection to databases.
 */
class RocksDatabaseContainer {

    /**
     * Map of db path -> db handler.
     */
    private static final Map<String, DatabaseWithRefs> CACHED_DATABASES = new ConcurrentHashMap<>();

    private record DatabaseWithRefs (RocksDatabase db, Set<MapStore<?, ?>> stores){}

    static synchronized RocksDatabase getRocksDb (String path, boolean autoCreate, MapStore<?, ?> mapStoreAsking) {
        DatabaseWithRefs dbWithRefCache = CACHED_DATABASES.computeIfAbsent(path,
                directory -> new DatabaseWithRefs(new RocksDatabase(directory, autoCreate), new HashSet<>()));
        dbWithRefCache.stores.add(mapStoreAsking);
        return dbWithRefCache.db;
    }

    static synchronized void dispose (String path, MapStore<?, ?> mapStoreAsking) {
        DatabaseWithRefs dbWithRefCache = CACHED_DATABASES.get(path);
        dbWithRefCache.stores.remove(mapStoreAsking);
        if (dbWithRefCache.stores.isEmpty()) {
            dbWithRefCache.db.close();
        }
    }
}
