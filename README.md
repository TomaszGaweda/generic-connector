Simple RocksDB MapStore for Hazelcast
=====================================

This project is a simple RocksDB implementation of Hazelcast's MapStore.
It's a sandbox project; not intended to use in production (at least, not yet).

Allows multiple maps to access the same RocksDB database. Caches connections in order to use as little resources as possible.

Usage examples
--------------

Add following dependency (once it's published):

```xml
    <dependency>
        <groupId>com.github.tomaszgaweda</groupId>
        <artifactId>generic-connector</artifactId>
        <version>0.1</version>
    </dependency>
```

Then you have two options to configure your MapStore:

1) Use MapStoreConfig#setClassName with setProperty.
2) Use MapStoreConfig#setImplementation.

Requirements:
- Java 17
- Hazelcast 5.0