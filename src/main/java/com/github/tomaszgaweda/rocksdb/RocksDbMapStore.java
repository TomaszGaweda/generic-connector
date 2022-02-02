package com.github.tomaszgaweda.rocksdb;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapLoaderLifecycleSupport;
import com.hazelcast.map.MapStore;

import java.io.File;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import static com.github.tomaszgaweda.rocksdb.RocksDatabaseContainer.dispose;
import static com.github.tomaszgaweda.rocksdb.RocksDatabaseContainer.getRocksDb;
import static java.util.Collections.emptyList;

/**
 * {@linkplain MapStore} implementation for use with RocksDb.
 *
 * Can be used on any type of map.
 *
 * Instances of RocksDB will be reused, so if few maps want to reuse the same K-V store, then only one connection will
 * be kept in memory.
 *
 * User have two options to use this map store implementation in his code:
 * <ol>
 *     <li>Use {@linkplain com.hazelcast.config.MapStoreConfig#setClassName}
 *         and {@linkplain com.hazelcast.config.MapStoreConfig#setProperty} to provide all required arguments.
 *         Arguments are defined as static constants in the class.
 *
 *         Example usage:
 *         <pre>
 *             var mapConfig = new MapConfig("TestMap");
 *             var mapStoreConfig = new MapStoreConfig()
 *                 .setClassName(RocksDbMapStore.class.getName())
 *                 .setProperty(DATABASE_PATH_PARAM, "/path/to/rocksdb/database)
 *                 .setProperty(DATABASE_AUTOCREATION_PARAM, "true")
 *                 .setProperty(KEY_CLASS_PARAM, "java.lang.String")
 *                 .setProperty(VALUE_CLASS_PARAM, "java.lang.String")
 *                 .setEnabled(true)
 *                 .setWriteBatchSize(1)
 *                 .setWriteDelaySeconds(0);
 *
 *         mapConfig.setMapStoreConfig(mapStoreConfig);
 *         </pre>
 *         </li>
 *
 *         <li> Directly use constructor and pass the instance into
 *         {@link com.hazelcast.config.MapStoreConfig#setImplementation} method.
 *
 *         Example usage:
 *         <pre>
 *         var mapConfig = new MapConfig("TestMap");
 *         var mapStoreConfig = new MapStoreConfig()
 *                 .setImplementation(new RocksDbMapStore<>("/path/to/rocksdb/", true, String.class, String.class))
 *                 .setEnabled(true)
 *                 .setWriteBatchSize(1)
 *                 .setWriteDelaySeconds(0);
 *
 *         mapConfig.setMapStoreConfig(mapStoreConfig);
 *         </pre>
 *
 *         </li>
 * </ol>
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

    @SuppressWarnings("unused") // for indirect creation by Hazelcast
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
        return rocksDatabase.get(collection, valueClass);
    }

    @Override
    public Iterable<K> loadAllKeys() {
        return emptyList();
    }
}
