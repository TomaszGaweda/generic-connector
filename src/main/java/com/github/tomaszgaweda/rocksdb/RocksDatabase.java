package com.github.tomaszgaweda.rocksdb;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;

/**
 * Container for a single RocksDB database.
 */
class RocksDatabase {

    private static final Logger log = LoggerFactory.getLogger(RocksDatabase.class);

    static {
        RocksDB.loadLibrary();
    }

    private final File directory;
    private final boolean autoCreate;
    private final RocksDB db;

    RocksDatabase(String dbDirectory, boolean autoCreate) {
        this.directory = new File(dbDirectory);
        this.autoCreate = autoCreate;

        if (!directory.exists() && !autoCreate) {
            throw new IllegalArgumentException("provided directory %s does not exist and autocreation was turned off".formatted(dbDirectory));
        }
        if (directory.exists() && !directory.isDirectory()) {
            throw new IllegalArgumentException("provided directory %s is not a directory".formatted(dbDirectory));
        }

        final Options options = new Options();
        options.setCreateIfMissing(autoCreate);
        try {
            if (autoCreate) {
                Files.createDirectories(directory.getParentFile().toPath());
                Files.createDirectories(directory.getAbsoluteFile().toPath());
            }
            db = RocksDB.open(options, directory.getAbsolutePath());
        } catch (IOException | RocksDBException e) {
            throw new IllegalArgumentException("error initializing RocksDB", e);
        }
    }

    void put(Object key, Object value) {
        try {
            db.put(toBytes(key), toBytes(value));
        } catch (RocksDBException e) {
            throw new IllegalStateException("error when writing to rocksdb " + directory, e);
        }
    }

    <V> V get(Object key, Class<V> valueClass) {
        try {
            return fromBytes(db.get(toBytes(key)), valueClass);
        } catch (RocksDBException e) {
            throw new IllegalStateException("error when writing to rocksdb " + directory, e);
        }
    }

    void delete (Object key) {
        try {
            db.delete(toBytes(key));
        } catch (RocksDBException e) {
            throw new IllegalStateException("error when writing to rocksdb " + directory, e);
        }
    }
    /**
     * Note: this should be done using SerializationService; this is a shortcut to avoid "hacking" Hazelcast.
     */
    @SuppressWarnings("unchecked")
    private <V> V fromBytes(byte[] bytes, Class<V> valueClass) {
        try (var ois = new ObjectInputStream(new ByteArrayInputStream(bytes))){
            return (V) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new IllegalStateException("error when writing to rocksdb " + directory, e);
        }
    }

    /**
     * Note: this should be done using SerializationService; this is a shortcut to avoid "hacking" Hazelcast.
     */
    private byte[] toBytes(Object value) {
        var baos = new ByteArrayOutputStream();

        try (var oos = new ObjectOutputStream(baos)){
            oos.writeObject(value);
            oos.flush();
        } catch (IOException e) {
            throw new IllegalStateException("error when writing to rocksdb " + directory, e);
        }
        return baos.toByteArray();
    }

}
