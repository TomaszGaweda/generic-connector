package com.github.tomaszgaweda.rocksdb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.util.List;
import java.util.Map;

import static com.github.tomaszgaweda.rocksdb.SerializationUtils.fromBytes;
import static com.github.tomaszgaweda.rocksdb.SerializationUtils.toBytes;
import static org.assertj.core.api.Assertions.assertThat;

class RocksDatabaseTest {

    static {
        RocksDB.loadLibrary();
    }

    @TempDir
    File dbDir;

    @Test
    void put_all_writes_values() throws RocksDBException {
        // given
        String dbAbsoluteDir = dbDir.getAbsolutePath();
        var db = new RocksDatabase(dbAbsoluteDir, true);

        // when
        db.putAll(Map.of(
                "test1", "test1",
                "test2", "testTwoooo"
        ));
        db.close();

        // then
        try (var rocksDB = RocksDB.open(new Options().setCreateIfMissing(false), dbAbsoluteDir)) {
            assertThatValue(rocksDB, "test1", "test1");
            assertThatValue(rocksDB, "test2", "testTwoooo");
        }
    }

    @Test
    void put_writes_values() throws RocksDBException {
        // given
        String dbAbsoluteDir = dbDir.getAbsolutePath();
        var db = new RocksDatabase(dbAbsoluteDir, true);

        // when
        db.put("test1", "test1");
        db.put( "test2", "testTwoooo");
        db.close();

        // then
        try (var rocksDB = RocksDB.open(new Options().setCreateIfMissing(false), dbAbsoluteDir)) {
            assertThatValue(rocksDB, "test1", "test1");
            assertThatValue(rocksDB, "test2", "testTwoooo");
        }
    }

    @Test
    void put_delete_write_combo() throws RocksDBException {
        // given
        String dbAbsoluteDir = dbDir.getAbsolutePath();
        var db = new RocksDatabase(dbAbsoluteDir, true);

        // when
        db.put("test1", "test1");
        db.put("test2", "test2");
        db.delete("test2");
        db.putAll(Map.of(
                "test3", "test3",
                "test4", "test4"
        ));
        Map<String, String> valuesRead = db.get(List.of("test1", "test2", "test3", "test4"), String.class);
        db.close();

        // then
        assertThat(valuesRead).isEqualTo(Map.of(
           "test1", "test1",
           "test3", "test3",
           "test4", "test4"
        ));
        try (var rocksDB = RocksDB.open(new Options().setCreateIfMissing(false), dbAbsoluteDir)) {
            assertThatValue(rocksDB, "test1", "test1");
            assertThatValue(rocksDB, "test3", "test3");
            assertThatValue(rocksDB, "test4", "test4");
        }
    }

    @SuppressWarnings("unchecked")
    static <T> void assertThatValue(RocksDB rocksDB, Object key, T expected) throws RocksDBException {
        byte[] keyBytes = toBytes(key);
        byte[] valueBytes = rocksDB.get(keyBytes);
        assertThat(valueBytes).isNotNull();

        T value = fromBytes(valueBytes, (Class<T>) expected.getClass());
        assertThat(value).isEqualTo(expected);
    }

}