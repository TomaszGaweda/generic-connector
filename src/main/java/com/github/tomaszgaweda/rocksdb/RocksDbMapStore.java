package com.github.tomaszgaweda.rocksdb;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapLoaderLifecycleSupport;
import com.hazelcast.map.MapStore;

import java.io.File;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@linkplain MapStore} implementation for use with RocksDb.
 *
 * Can be used on any type of map.
 *
 * @param <K> type of key in the map
 * @param <V> type of values in the map
 */
public class RocksDbMapStore<K, V> implements MapStore<K, V>, MapLoaderLifecycleSupport {

    /**
     * Specifies the directory with RocksDB database.
     */
    public static final String DATABASE_PATH_PARAM = "rocksdb.datbase.path";

    /**
     * If true, connector will create database when it was not present in {@linkplain #DATABASE_PATH_PARAM}.
     */
    public static final String DATABASE_AUTOCREATION = "rocksdb.database.autocreate";

    /**
     * Default value of {@link #DATABASE_AUTOCREATION} parameter.
     */
    public static final String DATABASE_AUTOCREATION_DEFAULT = "true";

    /**
     * Map of db path -> db handler.
     */
    private static final Map<String, RocksDatabase> CACHED_DATABASES = new ConcurrentHashMap<>();

    private RocksDatabase rocksDatabase;

    @Override
    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
       this.rocksDatabase = databaseFor(properties);
    }

    /**
     * Returns new or existing {@link RocksDatabase} for given properties.
     * @return RocksDb database handler.
     */
    private static RocksDatabase databaseFor(Properties properties) {
        String dbPath = properties.getProperty(DATABASE_PATH_PARAM);
        boolean dbAutocreation = Boolean.getBoolean(properties.getProperty(DATABASE_AUTOCREATION, DATABASE_AUTOCREATION_DEFAULT));

        return CACHED_DATABASES.computeIfAbsent(dbPath, directory -> new RocksDatabase(directory, dbAutocreation));
    }

    @Override
    public void destroy() {

    }

    @Override
    public void store(K k, V v) {
        System.out.println(k + " " + v);
    }

    @Override
    public void storeAll(Map<K, V> map) {
        System.out.println(map);

    }

    @Override
    public void delete(K k) {

    }

    @Override
    public void deleteAll(Collection<K> collection) {

    }

    @Override
    public V load(K k) {
        return null;
    }

    @Override
    public Map<K, V> loadAll(Collection<K> collection) {
        return null;
    }

    @Override
    public Iterable<K> loadAllKeys() {
        return null;
    }
}
