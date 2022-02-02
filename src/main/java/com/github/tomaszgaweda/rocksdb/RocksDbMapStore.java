package com.github.tomaszgaweda.rocksdb;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapLoaderLifecycleSupport;
import com.hazelcast.map.MapStore;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.github.tomaszgaweda.rocksdb.RocksDatabaseContainer.dispose;
import static com.github.tomaszgaweda.rocksdb.RocksDatabaseContainer.getRocksDb;

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
    public static final String DATABASE_AUTOCREATION_PARAM = "rocksdb.database.autocreate";

    /**
     * Default value of {@link #DATABASE_AUTOCREATION_PARAM} parameter.
     */
    public static final String DATABASE_AUTOCREATION_DEFAULT = "true";

    /**
     * Name of the class that MapStore will handle as a key.
     */
    public static final String KEY_CLASS_PARAM = "rocksdb.mapstore.keyClass";

    /**
     * Name of the class that MapStore will handle as a value.
     */
    public static final String VALUE_CLASS_PARAM = "rocksdb.mapstore.valueClass";

    private RocksDatabase rocksDatabase;
    private Class<K> keyClass;
    private Class<V> valueClass;

    public RocksDbMapStore() {}

    // todo: add builder
    public RocksDbMapStore(File rocksDbDir, boolean autoCreate, Class<K> keyClass, Class<V> valueClass) {
        this.rocksDatabase = getRocksDb(rocksDbDir.getAbsolutePath(), autoCreate, this);
        this.keyClass = keyClass;
        this.valueClass = valueClass;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
        if (rocksDatabase != null) return; // already initialized in the constructor
       this.rocksDatabase = databaseFor(properties);
        try {
            this.keyClass = (Class<K>) Class.forName(properties.getProperty(KEY_CLASS_PARAM));
            this.valueClass = (Class<V>) Class.forName(properties.getProperty(VALUE_CLASS_PARAM));
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("error initializing RocksDbMapStore", e);
        }
    }

    /**
     * Returns new or existing {@link RocksDatabase} for given properties.
     * @return RocksDb database handler.
     */
    private RocksDatabase databaseFor(Properties properties) {
        String dbPath = properties.getProperty(DATABASE_PATH_PARAM);
        boolean dbAutocreation = Boolean.parseBoolean(properties.getProperty(DATABASE_AUTOCREATION_PARAM, DATABASE_AUTOCREATION_DEFAULT));

        return getRocksDb(dbPath, dbAutocreation, this);
    }

    @Override
    public void destroy() {
        dispose(rocksDatabase.getDirectory().getAbsolutePath(), this);
    }

    @Override
    public void store(K key, V value) {
        rocksDatabase.put(key, value);
    }

    @Override
    public void storeAll(Map<K, V> map) {
        for (Map.Entry<K, V> entry : map.entrySet()) {
            store(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void delete(K key) {
        rocksDatabase.delete(key);
    }

    @Override
    public void deleteAll(Collection<K> collection) {
        for (K key : collection) {
            delete(key);
        }
    }

    @Override
    public V load(K k) {
        return rocksDatabase.get(k, valueClass);
    }

    @Override
    public Map<K, V> loadAll(Collection<K> collection) {
        var resultMap = new HashMap<K, V>(collection.size());
        for (K key : collection) {
            resultMap.put(key, load(key));
        }
        return resultMap;
    }

    @Override
    public Iterable<K> loadAllKeys() {
        return null;
    }
}
