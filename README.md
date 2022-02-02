Simple RocksDB MapStore for Hazelcast
=====================================

This project is a simple RocksDB implementation of Hazelcast's MapStore.
It's a sandbox project; not intended to use in production (at least, not yet).

Allows multiple maps to access the same RocksDB database. Store caches connections in order to use as little resources as possible
when you have multiple Hazelcast instances within one JVM.

Note that the RocksDB directory must be created on every single node on which ```IMap``` with ```RocksDbMapStore```
is used. The values are not merged by default between nodes - this is a known design flaw for now. If you restart your
cluster and will have different number of machines used (e.g. some machine was not used anymore), you may lose your data.

Usage examples
--------------

Add following dependency (once it's published):

```xml
    <dependency>
        <groupId>com.github.tomaszgaweda</groupId>
        <artifactId>rocksdb-hazelcast-connector</artifactId>
        <version>0.1</version>
    </dependency>
```

Main implementation method is ```com.github.tomaszgaweda.rocksdb.RocksDbMapStore```.

Then you have two options to configure your MapStore:

1) Use MapStoreConfig#setClassName with setProperty.
2) Use MapStoreConfig#setImplementation.

Both usages were described in more details in the RocksDbMapStore class' JavaDoc.

For future maintainers
----------------------

For the future, most of the tests should be done without Hazelcast instance spawning; those are much faster. However,
it's good to confirm all risky behaviours, for example I've tested:
- simple cases of initializing MapStore.
- if usage from multiple map stores will work (no deadlock, no write issue).
- the auto creation parameter is correctly passed and that lib won't override existing db.



Requirements:
- Java 17
- Hazelcast 5.0