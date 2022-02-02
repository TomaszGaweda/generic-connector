package com.github.tomaszgaweda.rocksdb;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.*;
import java.nio.file.Files;

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
    private final boolean autoCreate;
    private final RocksDB db;
    private volatile boolean open;

    RocksDatabase(@Nonnull String dbDirectory, boolean autoCreate) {
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
            open = true;
        } catch (IOException | RocksDBException e) {
            throw new IllegalArgumentException("error initializing RocksDB", e);
        }

        log.info("connection to database {} is opened successfully", directory);
    }

    void put(@Nonnull Object key, @Nonnull Object value) {
        checkOpened();
        try {
            db.put(toBytes(key), toBytes(value));
        } catch (RocksDBException e) {
            throw new IllegalStateException("error when writing to rocksdb " + directory, e);
        }
    }

    <V> V get(@Nonnull Object key, @Nonnull Class<V> valueClass) {
        checkOpened();
        try {
            byte[] keyBytes = toBytes(key);
            byte[] bytesFromDb = db.get(keyBytes);
            return bytesFromDb == null ? null : fromBytes(bytesFromDb, valueClass);
        } catch (RocksDBException e) {
            throw new IllegalStateException("error when writing to rocksdb " + directory, e);
        }
    }

    void delete (@Nonnull Object key) {
        checkOpened();
        try {
            db.delete(toBytes(key));
        } catch (RocksDBException e) {
            throw new IllegalStateException("error when writing to rocksdb " + directory, e);
        }
    }

    /**
     * Closes the database instance.
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

    public boolean isAutoCreate() {
        return autoCreate;
    }
}
