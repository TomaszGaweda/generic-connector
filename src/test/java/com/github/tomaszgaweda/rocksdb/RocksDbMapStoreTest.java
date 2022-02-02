package com.github.tomaszgaweda.rocksdb;


import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.map.IMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.nio.file.Path;

import static com.github.tomaszgaweda.rocksdb.RocksDbMapStore.*;
import static com.github.tomaszgaweda.rocksdb.SerializationUtils.fromBytes;
import static com.github.tomaszgaweda.rocksdb.SerializationUtils.toBytes;
import static org.assertj.core.api.Assertions.assertThat;

public class RocksDbMapStoreTest {

    static {
        RocksDB.loadLibrary();
    }

    @TempDir
    Path tempDbDir;

    @Test
    void creates_db_using_params() throws RocksDBException, InterruptedException {
        //given
        var config = new Config();
        var mapConfig = new MapConfig("TestMap");
        var mapStoreConfig = new MapStoreConfig()
                .setClassName(RocksDbMapStore.class.getName())
                .setProperty(DATABASE_PATH_PARAM, tempDbDir.toFile().getAbsolutePath())
                .setProperty(DATABASE_AUTOCREATION_PARAM, "true")
                .setProperty(VALUE_CLASS_PARAM, "java.lang.String")
                .setEnabled(true)
                .setWriteBatchSize(1)
                .setWriteDelaySeconds(0);

        mapConfig.setMapStoreConfig(mapStoreConfig);
        config.addMapConfig(mapConfig);
        var hazelcast = Hazelcast.newHazelcastInstance(config);

        // when
        IMap<String, String> testMap = hazelcast.getMap("TestMap");
        testMap.put("test", "hello");

        hazelcast.shutdown();

        Thread.sleep(1000); // wait until MapStores are disposed.

        // then
        try (var rocksDB = RocksDB.open(new Options().setCreateIfMissing(false), tempDbDir.toFile().getAbsolutePath())) {
            byte[] bytes = rocksDB.get(toBytes("test"));

            assertThat(bytes).isNotNull();
            String result = fromBytes(bytes, String.class);
            assertThat(result).isEqualTo("hello");
        }
    }

    @Test
    void creates_db_using_constructor() throws RocksDBException, InterruptedException {
        //given
        var config = new Config();
        var mapConfig = new MapConfig("TestMap");
        var mapStoreConfig = new MapStoreConfig()
                .setImplementation(new RocksDbMapStore<>(tempDbDir.toFile(), true, String.class))
                .setEnabled(true)
                .setWriteBatchSize(1)
                .setWriteDelaySeconds(0);

        mapConfig.setMapStoreConfig(mapStoreConfig);
        config.addMapConfig(mapConfig);
        var hazelcast = Hazelcast.newHazelcastInstance(config);

        // when
        IMap<String, String> testMap = hazelcast.getMap("TestMap");
        testMap.put("test2", "hello2");

        hazelcast.shutdown();

        Thread.sleep(1000); // wait until MapStores are disposed.

        // then
        try (var rocksDB = RocksDB.open(new Options().setCreateIfMissing(false), tempDbDir.toFile().getAbsolutePath())) {
            byte[] bytes = rocksDB.get(toBytes("test2"));

            assertThat(bytes).isNotNull();
            String result = fromBytes(bytes, String.class);
            assertThat(result).isEqualTo("hello2");
        }
    }

    @Test
    void reuses_exising_db() throws RocksDBException, InterruptedException {
        //given
        try (var rocksDB = RocksDB.open(new Options().setCreateIfMissing(true), tempDbDir.toFile().getAbsolutePath())) {
            rocksDB.put(toBytes("test3"), toBytes("hello3"));
        }

        var config = new Config();
        var mapConfig = new MapConfig("TestMap");
        var mapStoreConfig = new MapStoreConfig()
                .setImplementation(new RocksDbMapStore<>(tempDbDir.toFile(), false, String.class))
                .setEnabled(true)
                .setWriteBatchSize(1)
                .setWriteDelaySeconds(0);

        mapConfig.setMapStoreConfig(mapStoreConfig);
        config.addMapConfig(mapConfig);
        var hazelcast = Hazelcast.newHazelcastInstance(config);

        // when
        IMap<String, String> testMap = hazelcast.getMap("TestMap");
        testMap.put("test3_1", "hello3_1");
        String resultFromPreviousWrite = testMap.get("test3");

        hazelcast.shutdown();

        Thread.sleep(1000); // wait until MapStores are disposed.

        // then
        assertThat(resultFromPreviousWrite).isEqualTo("hello3");
        try (var rocksDB = RocksDB.open(new Options().setCreateIfMissing(false), tempDbDir.toFile().getAbsolutePath())) {
            byte[] first = rocksDB.get(toBytes("test3"));
            byte[] second = rocksDB.get(toBytes("test3_1"));

            assertThat(first)
                    .overridingErrorMessage("first value should not be removed")
                    .isNotNull();
            assertThat(second)
                    .overridingErrorMessage("second value should not be removed")
                    .isNotNull();
        }
    }

    @Test
    void multiple_map_access() throws RocksDBException, InterruptedException {
        //given
        final File rocksDbDir = tempDbDir.toFile();
        var config = new Config();
        var mapConfigMap1 = new MapConfig("TestMap1")
                .setMapStoreConfig(new MapStoreConfig()
                .setImplementation(new RocksDbMapStore<>(rocksDbDir, true, String.class))
                .setEnabled(true)
                .setWriteBatchSize(1)
                .setWriteDelaySeconds(0));
        var mapConfigMap2 = new MapConfig("TestMap2")
                .setMapStoreConfig(new MapStoreConfig()
                .setImplementation(new RocksDbMapStore<>(rocksDbDir, true, String.class))
                .setEnabled(true)
                .setWriteBatchSize(1)
                .setWriteDelaySeconds(0));
        var mapConfigMap3 = new MapConfig("TestMap3")
                .setMapStoreConfig(new MapStoreConfig()
                .setImplementation(new RocksDbMapStore<>(rocksDbDir, true, Integer.class))
                .setEnabled(true)
                .setWriteBatchSize(1)
                .setWriteDelaySeconds(0));

        config.addMapConfig(mapConfigMap1);
        config.addMapConfig(mapConfigMap2);
        config.addMapConfig(mapConfigMap3);
        var hazelcast = Hazelcast.newHazelcastInstance(config);

        // when
        IMap<String, String> testMap1 = hazelcast.getMap("TestMap1");
        IMap<String, String> testMap2 = hazelcast.getMap("TestMap2");
        IMap<String, Integer> testMap3 = hazelcast.getMap("TestMap3");
        testMap1.put("test-multi-1", "I am here!");
        testMap2.put("test-multi-2", "And here too!");
        testMap3.put("test-multi-3", 10);
        hazelcast.shutdown();

        Thread.sleep(1500); // wait until MapStores are disposed.

        // then
        try (var rocksDB = RocksDB.open(new Options().setCreateIfMissing(false), rocksDbDir.getAbsolutePath())) {
            byte[] first = rocksDB.get(toBytes("test-multi-1"));
            byte[] second = rocksDB.get(toBytes("test-multi-2"));
            byte[] third = rocksDB.get(toBytes("test-multi-3"));

            assertThat(first)
                    .overridingErrorMessage("first value should not be removed")
                    .isNotNull();
            assertThat(second)
                    .overridingErrorMessage("second value should not be removed")
                    .isNotNull();
            assertThat(third)
                    .overridingErrorMessage("third value should not be removed")
                    .isNotNull();
        }
    }

}
