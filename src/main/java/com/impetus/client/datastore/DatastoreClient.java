package com.impetus.client.datastore;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Entity;
import com.impetus.client.datastore.query.DatastoreQuery;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.generator.AutoGenerator;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.ClientMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.property.PropertyAccessorHelper;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;
import java.lang.reflect.Field;
import java.util.*;

/**
 * @author Fabio Arcidiacono.
 *         <p>The gateway to CRUD operations on database, except for queries.</p>
 */
public class DatastoreClient extends ClientBase implements Client<DatastoreQuery>, AutoGenerator {

    private static final Logger logger = LoggerFactory.getLogger(DatastoreClient.class);
    private EntityReader reader;
    private DatastoreService datastore;
    private Random random;
    private int batchSize;

    protected DatastoreClient(final KunderaMetadata kunderaMetadata, Map<String, Object> properties,
                              String persistenceUnit, final ClientMetadata clientMetadata, IndexManager indexManager,
                              EntityReader reader, final DatastoreService datastore) {
        super(kunderaMetadata, properties, persistenceUnit);
        this.reader = reader;
        this.datastore = datastore;
        this.indexManager = indexManager;
        this.clientMetadata = clientMetadata;
        this.random = new Random();
        setBatchSize(persistenceUnit, this.externalProperties);
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public void close() {
        this.indexManager.flush();
        this.reader = null;
        externalProperties = null;
    }

    @Override
    public EntityReader getReader() {
        return reader;
    }

    @Override
    public Class<DatastoreQuery> getQueryImplementor() {
        return DatastoreQuery.class;
    }

    /*
     * Persist operations
     */

    @Override
    protected void onPersist(EntityMetadata entityMetadata, Object entity, Object id, List<RelationHolder> rlHolders) {

        MetamodelImpl metamodel = KunderaMetadataManager.getMetamodel(kunderaMetadata,
                entityMetadata.getPersistenceUnit());

        System.out.println("entityMetadata = [" + entityMetadata + "], entity = [" + entity + "], id = [" + id + "], rlHolders = [" + rlHolders + "]");

        EntityType entityType = metamodel.entity(entityMetadata.getEntityClazz());

        // TODO decide
        // this way datastore generate id
        // entity must have an id annotated with @Id
        // and must have a value different from null
        // Entity gaeEntity = new Entity(entityType.getName());

        // is also possible to use the one generated
        // need to define a Long id annotated with @Id
        // and with @GeneratedValue()
        // apparently is also setted back to the entity
        Entity gaeEntity = new Entity(entityType.getName(), (Long) id);

        System.out.println(entityMetadata.getIdAttribute());

        handleAttributes(gaeEntity, entity, metamodel, entityMetadata, entityType.getAttributes());
        handleRelations(gaeEntity, rlHolders);
        handleDiscriminatorColumn(gaeEntity, entityType);

        datastore.put(gaeEntity);
    }

    @Override
    public Object generate() {
        return random.nextLong();
    }

    private void handleAttributes(Entity gaeEntity, Object entity, MetamodelImpl metamodel, EntityMetadata metadata, Set<Attribute> attributes) {
        for (Attribute attribute : attributes) {
            // by pass id attribute, use datastore one
            if (!attribute.equals(metadata.getIdAttribute())) {
                // by pass association.   TODO verify this
                if (!attribute.isAssociation()) {
                    if (metamodel.isEmbeddable(((AbstractAttribute) attribute).getBindableJavaType())) {
                        processEmbeddableAttribute(entity, metamodel, metadata, attribute);
                    } else {
                        Field field = (Field) attribute.getJavaMember();
                        Object valueObj = PropertyAccessorHelper.getObject(entity, field);

                        if (valueObj != null) {
                            gaeEntity.setProperty(attribute.getName(), valueObj);
                        }
                    }
                }
            }
        }
    }

    private void processEmbeddableAttribute(Object entity, MetamodelImpl metamodel, EntityMetadata metadata, Attribute attribute) {
        // TODO Auto-generated method stub
    }

    private void handleRelations(Entity gaeEntity, List<RelationHolder> rlHolders) {
        if (rlHolders != null && !rlHolders.isEmpty()) {
            for (RelationHolder rh : rlHolders) {
                String relationName = rh.getRelationName();
                // TODO maybe need a key or something like parent, not valueObj
                Object valueObj = rh.getRelationValue();

                if (!StringUtils.isEmpty(relationName) && valueObj != null) {
                    gaeEntity.setProperty(relationName, valueObj);
                }
            }
        }
    }

    private void handleDiscriminatorColumn(Entity gaeEntity, EntityType entityType) {
        String discrColumn = ((AbstractManagedType) entityType).getDiscriminatorColumn();
        String discrValue = ((AbstractManagedType) entityType).getDiscriminatorValue();

        if (discrColumn != null && discrValue != null) {
            gaeEntity.setProperty(discrColumn, discrValue);
        }
    }

    @Override
    public void persistJoinTable(JoinTableData joinTableData) {
        // TODO Auto-generated method stub
        throw new NotImplementedException();
    }

    /*
     * Find operations
     */

    /**
     * This is called by Kundera when find method is invoked on Entity Manager. This method is responsible
     * for fetching data from underlying database for given entity class and primary key.
     */
    @Override
    public Object find(Class entityClass, Object key) {
        System.out.println("DatastoreClient.find");
        // TODO Auto-generated method stub
        throw new NotImplementedException();
        // return entity;
    }

    @Override
    public <E> List<E> findAll(Class<E> entityClass, String[] columnsToSelect, Object... keys) {
        System.out.println("DatastoreClient.findAll");
        List results = new ArrayList();
        for (Object key : keys) {
            Object object = find(entityClass, key);
            if (object != null) {
                results.add(object);
            }
        }
        System.out.println(this.getClass().getCanonicalName() + results.toString());
        return results;
    }

    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> embeddedColumnMap) {
        System.out.println("DatastoreClient.find");
        // TODO Auto-generated method stub
        throw new NotImplementedException();
        // return null;
    }

    @Override
    public Object[] findIdsByColumn(String schemaName, String tableName, String pKeyName, String columnName, Object columnValue, Class entityClazz) {
        System.out.println("DatastoreClient.findIdsByColumn");
        // TODO Auto-generated method stub
        throw new NotImplementedException();
        // return null;
    }

    @Override
    public List<Object> findByRelation(String colName, Object colValue, Class entityClazz) {
        System.out.println("DatastoreClient.findByRelation");
        // TODO Auto-generated method stub
        throw new NotImplementedException();
        //return null;
    }

    /*
     * Delete operations
     */

    /**
     * This is called by Kundera when a remove method is invoked on entity manager.
     * This is responsible for removing entity object from underlying database.
     */
    @Override
    public void delete(Object entity, Object pKey) {
        System.out.println("DatastoreClient.delete");
        // TODO Auto-generated method stub
        throw new NotImplementedException();
    }

    @Override
    public void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue) {
        System.out.println("DatastoreClient.deleteByColumn");
        // TODO Auto-generated method stub
        throw new NotImplementedException();
    }

    /*
     * Get operation
     */

    @Override
    public <E> List<E> getColumnsById(String schemaName, String tableName, String pKeyColumnName, String columnName, Object pKeyColumnValue, Class columnJavaType) {
        System.out.println("DatastoreClient.getColumnsById");
        // TODO Auto-generated method stub
        throw new NotImplementedException();
        // return null;
    }

    private void setBatchSize(String persistenceUnit, Map<String, Object> puProperties) {
        String batch_Size = null;
        if (puProperties != null) {
            batch_Size = puProperties != null ? (String) puProperties.get(PersistenceProperties.KUNDERA_BATCH_SIZE)
                    : null;
            if (batch_Size != null) {
                setBatchSize(Integer.valueOf(batch_Size));
            }
        } else if (batch_Size == null) {
            PersistenceUnitMetadata puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(kunderaMetadata,
                    persistenceUnit);
            setBatchSize(puMetadata.getBatchSize());
        }
    }
}
