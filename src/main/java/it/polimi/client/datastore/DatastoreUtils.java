package it.polimi.client.datastore;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.metadata.model.EntityMetadata;

import java.io.*;

/**
 * Utils method for common operation with Datastore api.
 *
 * @author Fabio Arcidiacono.
 */
public class DatastoreUtils {

    private DatastoreUtils() {
    }

    /**
     * Generate a datastore {@link com.google.appengine.api.datastore.Entity} from
     * {@link com.impetus.kundera.metadata.model.EntityMetadata}.
     *
     * @param entityMetadata metadata from Kundera ({@link com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata}).
     * @param id             entity id.
     *
     * @return a fresh new datastore {@link com.google.appengine.api.datastore.Entity}
     *
     * @throws com.impetus.kundera.KunderaException if id is not either {@link String} or {@link Long}.
     * @see com.google.appengine.api.datastore.Entity
     */
    public static Entity createDatastoreEntity(EntityMetadata entityMetadata, Object id) {
        Class idClazz = entityMetadata.getIdAttribute().getJavaType();
        if (!(idClazz.equals(String.class) || idClazz.equals(Long.class))) {
            throw new KunderaException("Id attribute must be either of type " + String.class + " or " + Long.class);
        }
        return createDatastoreEntity(entityMetadata.getTableName(), id);
    }

    /**
     * Generate a datastore {@link com.google.appengine.api.datastore.Entity}.
     *
     * @param kind kind of the entity.
     * @param id   entity id.
     *
     * @return a fresh new datastore {@link com.google.appengine.api.datastore.Entity}.
     *
     * @throws com.impetus.kundera.KunderaException if id is not either {@link String} or {@link Long}.
     * @see com.google.appengine.api.datastore.Entity
     */
    public static Entity createDatastoreEntity(String kind, Object id) {
        if (id instanceof String) {
            return new Entity(kind, (String) id);
        } else if (id instanceof Long) {
            return new Entity(kind, (Long) id);
        }
        throw new KunderaException("Id attribute must be either of type " + String.class + " or " + Long.class);
    }

    /**
     * Generate a datastore {@link com.google.appengine.api.datastore.Entity}, letting
     * datastore to generate id for it.
     *
     * @param kind kind of the entity.
     *
     * @return a fresh new datastore {@link com.google.appengine.api.datastore.Entity}.
     *
     * @see com.google.appengine.api.datastore.Entity
     */
    public static Entity createDatastoreEntity(String kind) {
        return new Entity(kind);
    }

    /**
     * Create a datastore {@link com.google.appengine.api.datastore.Key} from
     * entity kind and id.
     * <p/>
     * id is checked to be {@link Long}, otherwise is silently casted to {@link String}.<></>
     *
     * @param kind kind of the entity.
     * @param id   id of the entity.
     *
     * @return the {@link com.google.appengine.api.datastore.Key}  for the given pair kind-id.
     *
     * @see com.google.appengine.api.datastore.Key
     */
    public static Key createKey(String kind, Object id) {
        if (id instanceof Long) {
            return KeyFactory.createKey(kind, (Long) id);
        }
        return KeyFactory.createKey(kind, (String) id);
    }

    /**
     * Serialize an object to datastore {@link com.google.appengine.api.datastore.Blob}.
     *
     * @param obj object to be serialized.
     *
     * @return an instance of {@link com.google.appengine.api.datastore.Blob} of the given object.
     *
     * @throws IOException
     * @see com.google.appengine.api.datastore.Blob
     */
    public static Blob serialize(Object obj) throws IOException {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        ObjectOutputStream o = new ObjectOutputStream(b);
        o.writeObject(obj);
        return new Blob(b.toByteArray());
    }

    /**
     * Deserialize a datastore {@link com.google.appengine.api.datastore.Blob}.
     *
     * @param blob a datastore {@link com.google.appengine.api.datastore.Blob}.
     *
     * @return the deserialized object.
     *
     * @throws IOException
     * @throws ClassNotFoundException
     * @see com.google.appengine.api.datastore.Blob
     */
    public static Object deserialize(Blob blob) throws IOException, ClassNotFoundException {
        byte[] bytes = blob.getBytes();
        ByteArrayInputStream b = new ByteArrayInputStream(bytes);
        ObjectInputStream o = new ObjectInputStream(b);
        return o.readObject();
    }
}
