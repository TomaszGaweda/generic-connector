package com.github.tomaszgaweda.rocksdb;

import javax.annotation.Nonnull;
import java.io.*;

/**
 * Serialization handling class, simple replacement for SerializationService from Hazelcast that
 * uses only standard Java serialization.
 */
class SerializationUtils {

    /**
     * Note: this should be done using SerializationService; this is a shortcut to avoid "hacking" Hazelcast.
     */
    @SuppressWarnings("unchecked")
    static <V> V fromBytes(@Nonnull byte[] bytes, @Nonnull Class<V> valueClass) {
        try (var ois = new ObjectInputStream(new ByteArrayInputStream(bytes))){
            return (V) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new IllegalStateException("error when reading " + valueClass, e);
        }
    }

    /**
     * Note: this should be done using SerializationService; this is a shortcut to avoid "hacking" Hazelcast.
     */
    static byte[] toBytes(@Nonnull Object value) {
        var baos = new ByteArrayOutputStream();

        try (var oos = new ObjectOutputStream(baos)){
            oos.writeObject(value);
            oos.flush();
        } catch (IOException e) {
            throw new IllegalStateException("error when writing instance of " + value.getClass(), e);
        }
        return baos.toByteArray();
    }
}
