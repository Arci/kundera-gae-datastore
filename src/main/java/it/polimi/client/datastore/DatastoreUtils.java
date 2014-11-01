package it.polimi.client.datastore;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.metadata.model.EntityMetadata;

import java.io.*;

/**
 * @author Fabio Arcidiacono.
 */
public class DatastoreUtils {

    public static Entity createDatastoreEntity(EntityMetadata entityMetadata, Object id) {
        Class idClazz = entityMetadata.getIdAttribute().getJavaType();
        if (!(idClazz.equals(String.class) || idClazz.equals(Long.class))) {
            throw new KunderaException("Id attribute must be either of type " + String.class + " or " + Long.class);
        }
        return createDatastoreEntity(entityMetadata.getTableName(), id);
    }

    public static Entity createDatastoreEntity(String tableName, Object id) {
        if (id instanceof String) {
            return new Entity(tableName, (String) id);
        } else if (id instanceof Long) {
            return new Entity(tableName, (Long) id);
        } else {
            return createDatastoreEntity(tableName);
        }
    }

    public static Entity createDatastoreEntity(String tableName) {
        return new Entity(tableName);
    }

    public static Key createKey(String tableName, Object id) {
        if (id instanceof Long) {
            return KeyFactory.createKey(tableName, (Long) id);
        }
        return KeyFactory.createKey(tableName, (String) id);
    }

    public static Blob serialize(Object obj) throws IOException {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        ObjectOutputStream o = new ObjectOutputStream(b);
        o.writeObject(obj);
        return new Blob(b.toByteArray());
    }

    public static Object deserialize(Blob blob) throws IOException, ClassNotFoundException {
        byte[] bytes = blob.getBytes();
        ByteArrayInputStream b = new ByteArrayInputStream(bytes);
        ObjectInputStream o = new ObjectInputStream(b);
        return o.readObject();
    }
}
