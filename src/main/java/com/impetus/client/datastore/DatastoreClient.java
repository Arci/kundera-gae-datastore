package com.impetus.client.datastore;

import com.google.appengine.api.datastore.*;
import com.impetus.client.datastore.query.DatastoreQuery;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.client.EnhanceEntity;
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
    private int batchSize;

    protected DatastoreClient(final KunderaMetadata kunderaMetadata, Map<String, Object> properties,
                              String persistenceUnit, final ClientMetadata clientMetadata, IndexManager indexManager,
                              EntityReader reader, final DatastoreService datastore) {
        super(kunderaMetadata, properties, persistenceUnit);
        this.reader = reader;
        this.datastore = datastore;
        this.indexManager = indexManager;
        this.clientMetadata = clientMetadata;
        setBatchSize(persistenceUnit, this.externalProperties);
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    private void setBatchSize(String persistenceUnit, Map<String, Object> puProperties) {
        String batch_Size;
        if (puProperties != null) {
            batch_Size = (String) puProperties.get(PersistenceProperties.KUNDERA_BATCH_SIZE);
            if (batch_Size != null) {
                setBatchSize(Integer.valueOf(batch_Size));
            }
        } else {
            PersistenceUnitMetadata puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(kunderaMetadata,
                    persistenceUnit);
            setBatchSize(puMetadata.getBatchSize());
        }
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

    /*---------------------------------------------------------------------------------*/
    /*----------------------------- PERSIST OPERATIONS -------------------------------*/

    @Override
    protected void onPersist(EntityMetadata entityMetadata, Object entity, Object id, List<RelationHolder> rlHolders) {
        System.out.println("DatastoreClient.onPersist");
        System.out.println("entityMetadata = [" + entityMetadata + "], entity = [" + entity + "], id = [" + id + "], rlHolders = [" + rlHolders + "]");

        MetamodelImpl metamodel = KunderaMetadataManager.getMetamodel(kunderaMetadata,
                entityMetadata.getPersistenceUnit());
        EntityType entityType = metamodel.entity(entityMetadata.getEntityClazz());

        Entity gaeEntity = new Entity(entityType.getName(), stringify(id));

        handleAttributes(gaeEntity, entity, metamodel, entityMetadata, entityType.getAttributes());
        handleRelations(gaeEntity, rlHolders);
        handleDiscriminatorColumn(gaeEntity, entityType);

        datastore.put(gaeEntity);
    }

    private String stringify(Object id) {
        // datastore ids can be Long or String
        return (String) id;
    }

    @Override
    public Object generate() {
        /*
         * use random UUID instead of datastore generated
         * since here is not available entityClass.
         *
         * If the entityClass is available, a Key can be generated as follow
         *
         * KeyRange keyRange = datastore.allocateIds(entityType.getName(), 1L);
         * Key key = keyRange.getStart();
         *
         */
        return UUID.randomUUID();
    }

    private void handleAttributes(Entity gaeEntity, Object entity, MetamodelImpl metamodel, EntityMetadata metadata, Set<Attribute> attributes) {
        for (Attribute attribute : attributes) {
            //by pass id attribute, is already stored by datastore as part of the key
            if (!attribute.equals(metadata.getIdAttribute())) {
                // by pass association. TODO verify this
                if (!attribute.isAssociation()) {
                    if (metamodel.isEmbeddable(((AbstractAttribute) attribute).getBindableJavaType())) {
                        processEmbeddableAttribute(gaeEntity, entity, attribute, metamodel, metadata);
                    } else {
                        processAttribute(gaeEntity, entity, attribute);
                    }
                }
            }
        }
    }

    private void processEmbeddableAttribute(Entity gaeEntity, Object entity, Attribute attribute, MetamodelImpl metamodel, EntityMetadata metadata) {
        // TODO Auto-generated method stub
    }

    private void processAttribute(Entity gaeEntity, Object entity, Attribute attribute) {
        Field field = (Field) attribute.getJavaMember();
        Object valueObj = PropertyAccessorHelper.getObject(entity, field);

        if (valueObj != null) {
            gaeEntity.setProperty(attribute.getName(), valueObj);
        }
    }

    private void handleRelations(Entity gaeEntity, List<RelationHolder> rlHolders) {
        if (rlHolders != null && !rlHolders.isEmpty()) {
            for (RelationHolder rh : rlHolders) {
                String relationName = rh.getRelationName();
                // TODO maybe need a key or something like parent, not valueObj (which is the referenced id)
                Object valueObj = rh.getRelationValue();

                System.out.println("\nRelationValue:[ \n\t " + valueObj + "\n]\n");

                System.out.println("\nRelationVia:[ \n\t " + rh.getRelationVia() + "\n]\n");

                // TODO decide
                // to retrieve the related object need to know
                // the kind anf the id, maybe save as relation value
                // kind.id so can be splitted by "."
                //

                // il problema è avere il kind, posso recuperarlo da Key.getKind()
                // ma bisognerebbe riuscire a usare le key come id nelle entity
                // come fa l'implementazione della jpa di google

                // try {
                //     String kind = rh.getRelationValue().toString().split(".")[0];
                //     String embeddedId = rh.getRelationValue().toString().split(".")[1];
                //     Key key = KeyFactory.createKey(kind, embeddedId);
                //     Entity entity = datastore.get(key);
                //     EmbeddedEntity embeddedEntity = new EmbeddedEntity();
                //     embeddedEntity.setPropertiesFrom(entity);
                //     gaeEntity.setProperty(relationName, embeddedEntity);
                // } catch (EntityNotFoundException e) {
                //     e.printStackTrace();
                // }

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
        System.out.println("DatastoreClient.persistJoinTable");
        System.out.println("joinTableData = [" + joinTableData + "]");
        // TODO Auto-generated method stub
        throw new NotImplementedException();
    }

    /*---------------------------------------------------------------------------------*/
    /*------------------------------ FIND OPERATIONS ----------------------------------*/

    /*
     * is not called if entity is managed
     * is called for un-managed ones
     */
    @Override
    public Object find(Class entityClass, Object id) {
        System.out.println("DatastoreClient.find");
        System.out.println("entityClass = [" + entityClass + "], id = [" + id + "]");
        try {
            Key key = KeyFactory.createKey(entityClass.getSimpleName(), stringify(id));
            Entity gaeEntity = datastore.get(key);
            return initializeEntity(gaeEntity, entityClass);
        } catch (EntityNotFoundException e) {
            // No entity found
            return null;
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

    private Object initializeEntity(Entity gaeEntity, Class entityClass) throws IllegalAccessException, InstantiationException {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);
        String idColumnName = ((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName();

        // use reflection to instantiate the entity class
        Object entity = entityMetadata.getEntityClazz().newInstance();
        // use reflection to set entity fields
        for (Field field : entityClass.getDeclaredFields()) {
            if (field.getName().equals(idColumnName)) {
                field.setAccessible(true);
                field.set(entity, gaeEntity.getKey().getName());
            } else if (gaeEntity.getProperties().containsKey(field.getName())) {
                Object fieldValue = gaeEntity.getProperties().get(field.getName());
                field.setAccessible(true);
                field.set(entity, fieldValue);
            }
        }

        return new EnhanceEntity(entity, gaeEntity.getKey().getName(), null);
    }

    @Override
    public <E> List<E> findAll(Class<E> entityClass, String[] columnsToSelect, Object... keys) {
        System.out.println("DatastoreClient.findAll");
        System.out.println("entityClass = [" + entityClass + "], columnsToSelect = [" + columnsToSelect + "], keys = [" + keys + "]");
        List results = new ArrayList();
        for (Object key : keys) {
            Object object = this.find(entityClass, key);
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
        System.out.println("entityClass = [" + entityClass + "], embeddedColumnMap = [" + embeddedColumnMap + "]");
        // TODO Auto-generated method stub
        throw new NotImplementedException();
        // return null;
    }

    @Override
    public Object[] findIdsByColumn(String schemaName, String tableName, String pKeyName, String columnName, Object columnValue, Class entityClazz) {
        System.out.println("DatastoreClient.findIdsByColumn");
        System.out.println("schemaName = [" + schemaName + "], tableName = [" + tableName + "], pKeyName = [" + pKeyName + "], columnName = [" + columnName + "], columnValue = [" + columnValue + "], entityClazz = [" + entityClazz + "]");
        // TODO Auto-generated method stub
        throw new NotImplementedException();
        // return null;
    }

    @Override
    public List<Object> findByRelation(String colName, Object colValue, Class entityClazz) {
        System.out.println("DatastoreClient.findByRelation");
        System.out.println("colName = [" + colName + "], colValue = [" + colValue + "], entityClazz = [" + entityClazz + "]");
        // TODO Auto-generated method stub
        throw new NotImplementedException();
        //return null;
    }

    /*---------------------------------------------------------------------------------*/
    /*----------------------------- DELETE OPERATIONS ----------------------------------*/

    @Override
    public void delete(Object entity, Object pKey) {
        System.out.println("DatastoreClient.delete");
        System.out.println("entity = [" + entity + "], pKey = [" + pKey + "]");
        Key key = KeyFactory.createKey(entity.getClass().getSimpleName(), stringify(pKey));
        datastore.delete(key);
    }

    @Override
    public void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue) {
        System.out.println("DatastoreClient.deleteByColumn");
        System.out.println("schemaName = [" + schemaName + "], tableName = [" + tableName + "], columnName = [" + columnName + "], columnValue = [" + columnValue + "]");
        // TODO Auto-generated method stub
        throw new NotImplementedException();
    }

    /*---------------------------------------------------------------------------------*/
    /*------------------------------ GET OPERATIONS ----------------------------------*/

    @Override
    public <E> List<E> getColumnsById(String schemaName, String tableName, String pKeyColumnName, String columnName, Object pKeyColumnValue, Class columnJavaType) {
        System.out.println("DatastoreClient.getColumnsById");
        System.out.println("schemaName = [" + schemaName + "], tableName = [" + tableName + "], pKeyColumnName = [" + pKeyColumnName + "], columnName = [" + columnName + "], pKeyColumnValue = [" + pKeyColumnValue + "], columnJavaType = [" + columnJavaType + "]");
        // TODO Auto-generated method stub
        throw new NotImplementedException();
        // return null;
    }
}
