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
                .setProperty(KEY_CLASS_PARAM, "java.lang.String")
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

        Thread.sleep(1000);

        // then
        try (var rocksDB = RocksDB.open(new Options().setCreateIfMissing(false), tempDbDir.toFile().getAbsolutePath())) {
            byte[] bytes = rocksDB.get(toBytes("test"));

            assertThat(bytes).isNotNull();
            String result = fromBytes(bytes, String.class);
            assertThat(result).isEqualTo("hello");
        }
    }

}
