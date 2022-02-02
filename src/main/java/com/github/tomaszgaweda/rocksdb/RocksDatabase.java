package com.github.tomaszgaweda.rocksdb;

import com.hazelcast.map.MapStore;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.*;
import java.nio.file.Files;
import java.util.*;

import static com.github.tomaszgaweda.rocksdb.SerializationUtils.fromBytes;
import static com.github.tomaszgaweda.rocksdb.SerializationUtils.toBytes;

/**
 * Container for a single RocksDB database with methods for convenient usage of the database.
 *
 * By default, all exceptions will be rethrown.
 */
class RocksDatabase {

    private static final Logger log = LoggerFactory.getLogger(RocksDatabase.class);

    static {
        RocksDB.loadLibrary();
    }

    private final File directory;
    private final RocksDB db;
    private volatile boolean open;

    /**
     * Creates new instance and initializes connection to the RocksDB database.
     *
     * Should not be used directly; prefer using {@link  RocksDatabaseContainer#getRocksDb} method.
     */
    RocksDatabase(@Nonnull String dbDirectory, boolean autoCreate) {
        this.directory = new File(dbDirectory);

        if (!directory.exists() && !autoCreate) {
            throw new IllegalArgumentException("provided directory %s does not exist and auto creation was turned off".formatted(dbDirectory));
        }
        if (directory.exists() && !directory.isDirectory()) {
            throw new IllegalArgumentException("provided RocksDB directory %s is not a directory".formatted(dbDirectory));
        }

        final Options options = new Options();
        options.setCreateIfMissing(autoCreate);
        try {
            if (autoCreate) {
                Files.createDirectories(directory.getParentFile().toPath());
                Files.createDirectories(directory.getAbsoluteFile().toPath());
            }
            db = RocksDB.open(options, directory.getAbsolutePath());
            open = true;
        } catch (IOException | RocksDBException e) {
            throw new IllegalArgumentException("error initializing RocksDB connection", e);
        }

        log.info("connection to database {} is opened successfully", directory);
    }

    void put(@Nonnull Object key, @Nonnull Object value) {
        checkOpened();
        try {
            db.put(toBytes(key), toBytes(value));
        } catch (RocksDBException e) {
            throw new IllegalStateException("error when writing to RocksDB " + directory, e);
        }
    }

    void putAll(@Nonnull Map<?, ?> map) {
        checkOpened();
        WriteBatch batch = new WriteBatch();
        try {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                batch.put(toBytes(entry.getKey()), toBytes(entry.getValue()));
            }

            db.write(new WriteOptions(), batch);
        } catch (RocksDBException e) {
            throw new IllegalStateException("error when writing to RocksDB " + directory, e);
        }
    }

    <V> V get(@Nonnull Object key, @Nonnull Class<V> valueClass) {
        checkOpened();
        try {
            byte[] keyBytes = toBytes(key);
            byte[] bytesFromDb = db.get(keyBytes);
            return bytesFromDb == null ? null : fromBytes(bytesFromDb, valueClass);
        } catch (RocksDBException e) {
            throw new IllegalStateException("error when writing to RocksDB " + directory, e);
        }
    }

    <K, V> Map<K, V> get(@Nonnull Collection<K> keys, @Nonnull Class<V> valueClass) {
        checkOpened();
        try {
            List<byte[]> keysSerialized = keys.stream()
                    .map(SerializationUtils::toBytes)
                    .toList();

            List<byte[]> resultList = db.multiGetAsList(keysSerialized);

            Iterator<K> keyIterator;
            Iterator<byte[]> resultIterator;
            Map<K, V> resultMap = new HashMap<>(resultList.size());
            for (keyIterator = keys.iterator(), resultIterator = resultList.iterator();
                 keyIterator.hasNext();) {
                K key = keyIterator.next();
                byte[] result = resultIterator.next();
                if (result != null) {
                    resultMap.put(key, fromBytes(result, valueClass));
                }
            }
            return resultMap;
        } catch (RocksDBException e) {
            throw new IllegalStateException("error when writing to RocksDB " + directory, e);
        }
    }

    void delete (@Nonnull Object key) {
        checkOpened();
        try {
            db.delete(toBytes(key));
        } catch (RocksDBException e) {
            throw new IllegalStateException("error when writing to RocksDB " + directory, e);
        }
    }

    /**
     * Closes the database instance.
     * Usage of this {@linkplain RocksDatabase} is not possible after connection is closed.
     */
    public void close() {
        log.info("closing connection to database " + directory);
        open = false;
        db.close();
    }

    private void checkOpened() {
        if (!open) {
            throw new IllegalStateException("cannot perform actions on already closed instance of RocksDB");
        }
    }

    /**
     * Returns directory in which RocksDB is located.
     */
    public File getDirectory() {
        return directory;
    }
}
