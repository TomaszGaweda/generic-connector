package com.github.tomaszgaweda.rocksdb;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
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

}
