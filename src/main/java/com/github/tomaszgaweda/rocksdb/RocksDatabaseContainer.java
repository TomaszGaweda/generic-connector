package com.github.tomaszgaweda.rocksdb;

import com.hazelcast.map.MapStore;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Manages open connection to databases.
 */
// impl note: this class is using synchronized, as getting RocksDB is not a very common operation (it should be done
// on startup). Therefore, I am using the simplest mechanism that can be used and not worry about performance here.
class RocksDatabaseContainer {

    /**
     * Map of db path -> db handler.
     */
    private static final Map<String, DatabaseWithRefs> CACHED_DATABASES = new HashMap<>();

    /**
     * @param db database handle.
     * @param stores mutable set of MapStores that uses given handle.
     */
    private record DatabaseWithRefs (RocksDatabase db, Set<MapStore<?, ?>> stores){}

    /**
     * Gets an instance of {@link  RocksDatabase} for given parameters and registers the usage.
     */
    static synchronized RocksDatabase getRocksDb (String path, boolean autoCreate, MapStore<?, ?> mapStoreAsking) {
        DatabaseWithRefs dbWithRefCache = CACHED_DATABASES.computeIfAbsent(path,
                directory -> new DatabaseWithRefs(new RocksDatabase(directory, autoCreate), new HashSet<>()));
        dbWithRefCache.stores.add(mapStoreAsking);
        return dbWithRefCache.db;
    }

    /**
     * Removes provided map store from the list of map stores using given database.
     *
     * If no other map store uses this database, it will be closed.
     */
    static synchronized void dispose (String path, MapStore<?, ?> mapStoreAsking) {
        DatabaseWithRefs dbWithRefCache = CACHED_DATABASES.get(path);
        dbWithRefCache.stores.remove(mapStoreAsking);
        if (dbWithRefCache.stores.isEmpty()) {
            dbWithRefCache.db.close();
        }
    }
}
