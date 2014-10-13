package it.polimi.client.datastore;

import com.google.appengine.api.datastore.*;
import com.impetus.kundera.KunderaException;
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
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.property.PropertyAccessorHelper;
import it.polimi.client.datastore.query.DatastoreQuery;
import org.apache.commons.lang.NotImplementedException;
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

    private Map<Key, Class> oneToManyPrefetch = new HashMap<Key, Class>();

    protected DatastoreClient(final KunderaMetadata kunderaMetadata, Map<String, Object> properties,
                              String persistenceUnit, final ClientMetadata clientMetadata, IndexManager indexManager,
                              EntityReader reader, final DatastoreService datastore) {
        super(kunderaMetadata, properties, persistenceUnit);
        this.reader = reader;
        this.datastore = datastore;
        this.indexManager = indexManager;
        this.clientMetadata = clientMetadata;
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
        return UUID.randomUUID().toString();
    }

    @Override
    protected void onPersist(EntityMetadata entityMetadata, Object entity, Object id, List<RelationHolder> rlHolders) {
        System.out.println("DatastoreClient.onPersist");
        System.out.println("entityMetadata = [" + entityMetadata + "], entity = [" + entity + "], id = [" + id + "], rlHolders = [" + rlHolders + "]\n");

        MetamodelImpl metamodel = KunderaMetadataManager.getMetamodel(kunderaMetadata,
                entityMetadata.getPersistenceUnit());
        EntityType entityType = metamodel.entity(entityMetadata.getEntityClazz());
        Entity gaeEntity = new Entity(entityMetadata.getTableName(), (String) id);

        handleAttributes(gaeEntity, entity, metamodel, entityMetadata, entityType.getAttributes());
        handleRelations(gaeEntity, entity, entityMetadata, rlHolders);
        /* discriminator column is used for JPA inheritance */
        handleDiscriminatorColumn(gaeEntity, entityType);

        System.out.println("\n" + gaeEntity + "\n");

        datastore.put(gaeEntity);
    }

    private void handleAttributes(Entity gaeEntity, Object entity, MetamodelImpl metamodel, EntityMetadata metadata, Set<Attribute> attributes) {
        for (Attribute attribute : attributes) {
            /*
             * ID attribute will be saved (is redundant since is also stored within the Key).
             * By pass associations (i.e. relations) that are handled in handleRelations()
             */
            if (!attribute.isAssociation()) {
                if (metamodel.isEmbeddable(((AbstractAttribute) attribute).getBindableJavaType())) {
                    processEmbeddableAttribute(gaeEntity, entity, attribute, metamodel, metadata);
                } else {
                    processAttribute(gaeEntity, entity, attribute);
                }
            }
        }
    }

    private void processAttribute(Entity gaeEntity, Object entity, Attribute attribute) {
        Field field = (Field) attribute.getJavaMember();
        Object valueObj = PropertyAccessorHelper.getObject(entity, field);
        String jpaColumnName = ((AbstractAttribute) attribute).getJPAColumnName();

        if (valueObj != null) {
            System.out.println("field = [" + field.getName() + "], jpaColumnName = [" + jpaColumnName + "], valueObj = [" + valueObj + "]");
            gaeEntity.setProperty(jpaColumnName, valueObj);
        }
    }

    private void processEmbeddableAttribute(Entity gaeEntity, Object entity, Attribute attribute, MetamodelImpl metamodel, EntityMetadata metadata) {
        System.out.println("DatastoreClient.processEmbeddableAttribute");
        System.out.println("gaeEntity = [" + gaeEntity + "], entity = [" + entity + "], attribute = [" + attribute + "], metamodel = [" + metamodel + "], metadata = [" + metadata + "]");

        // TODO fill embeddedEntity properties
        // EmbeddedEntity embeddedEntity = new EmbeddedEntity();
        // embeddedEntity.setProperty(...);
        // gaeEntity.setProperty("contactInfo", embeddedEntity);
    }

    private void handleRelations(Entity gaeEntity, Object entity, EntityMetadata entityMetadata, List<RelationHolder> rlHolders) {
        if (rlHolders != null && !rlHolders.isEmpty()) {
            for (RelationHolder rh : rlHolders) {
                String jpaColumnName = rh.getRelationName();
                String fieldName = entityMetadata.getFieldName(jpaColumnName);
                String targetId = (String) rh.getRelationValue();

                if (jpaColumnName != null && targetId != null) {
                    System.out.println("field = [" + fieldName + "], jpaColumnName = [" + jpaColumnName + "], targetId = [" + targetId + "]");
                    gaeEntity.setProperty(jpaColumnName, targetId);
                }
            }
        }
    }

    private void handleDiscriminatorColumn(Entity gaeEntity, EntityType entityType) {
        String discrColumn = ((AbstractManagedType) entityType).getDiscriminatorColumn();
        String discrValue = ((AbstractManagedType) entityType).getDiscriminatorValue();

        if (discrColumn != null && discrValue != null) {
            System.out.println("discrColumn = [" + discrColumn + "], discrValue = [" + discrValue + "]");
            gaeEntity.setProperty(discrColumn, discrValue);
        }
    }

    /*
     * persist join table for ManyToMany
     *
     * for example:
     *  -----------------------------------------------------------------------
     *  |                    EMPLOYEE_PROJECT (joinTableName)                  |
     *  -----------------------------------------------------------------------
     *  | EMPLOYEE_ID (joinColumnName)  |  PROJECT_ID (inverseJoinColumnName)  |
     *  -----------------------------------------------------------------------
     *  |          id  (owner)          |             id (child)               |
     *  -----------------------------------------------------------------------
     */
    @Override
    public void persistJoinTable(JoinTableData joinTableData) {
        System.out.println("DatastoreClient.persistJoinTable");

        String joinTableName = joinTableData.getJoinTableName();
        String joinColumnName = joinTableData.getJoinColumnName();
        String inverseJoinColumnName = joinTableData.getInverseJoinColumnName();
        Map<Object, Set<Object>> joinTableRecords = joinTableData.getJoinTableRecords();

        for (Object owner : joinTableRecords.keySet()) {
            Set<Object> children = joinTableRecords.get(owner);

            for (Object child : children) {
                /* let datastore generate ID for the entity */
                Entity gaeEntity = new Entity(joinTableName);
                gaeEntity.setProperty(joinColumnName, owner);
                gaeEntity.setProperty(inverseJoinColumnName, child);
                datastore.put(gaeEntity);

                System.out.println(gaeEntity);
            }
        }
    }

    /*---------------------------------------------------------------------------------*/
    /*------------------------------ FIND OPERATIONS ----------------------------------*/

    /*
     * it's called to find detached entities
     */
    @Override
    public Object find(Class entityClass, Object id) {
        System.out.println("DatastoreClient.find");
        System.out.println("entityClass = [" + entityClass.getSimpleName() + "], id = [" + id + "]");

        try {
            EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);
            Entity gaeEntity = get(entityMetadata.getTableName(), id);
            if (gaeEntity == null) {
                // case not found
                return null;
            }
            System.out.println(gaeEntity);
            return initializeEntity(gaeEntity, entityClass);
        } catch (InstantiationException e) {
            e.printStackTrace();
            throw new KunderaException(e.getMessage());
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            throw new KunderaException(e.getMessage());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new KunderaException(e.getMessage());
        }
    }

    private Entity get(String kind, Object id) {
        try {
            Key key = KeyFactory.createKey(kind, (String) id);
            return datastore.get(key);
        } catch (EntityNotFoundException e) {
            System.out.println("\tNot found {kind = [" + kind + "], id = [" + id + "]}");
            return null;
        }
    }

    private Object initializeEntity(Entity gaeEntity, Class entityClass) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        System.out.println("DatastoreClient.initializeEntity");

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);
        MetamodelImpl metamodel = KunderaMetadataManager.getMetamodel(kunderaMetadata,
                entityMetadata.getPersistenceUnit());
        EntityType entityType = metamodel.entity(entityMetadata.getEntityClazz());

        Map<String, Object> relationMap = new HashMap<String, Object>();
        Object entity = entityMetadata.getEntityClazz().newInstance();

        Set<Attribute> attributes = entityType.getAttributes();
        for (Attribute attribute : attributes) {
            if (!attribute.isAssociation()) {
                initializeAttribute(gaeEntity, entity, attribute);
            } else {
                initializeRelation(gaeEntity, attribute, relationMap);
            }
        }
        System.out.println(entity + "\n\n");

        return new EnhanceEntity(entity, gaeEntity.getKey().getName(), relationMap.isEmpty() ? null : relationMap);
    }

    private void initializeAttribute(Entity gaeEntity, Object entity, Attribute attribute) {
        String jpaColumnName = ((AbstractAttribute) attribute).getJPAColumnName();
        Object fieldValue = gaeEntity.getProperties().get(jpaColumnName);
        System.out.println("jpaColumnName = [" + jpaColumnName + "], fieldValue = [" + fieldValue + "]");

        if (jpaColumnName != null && fieldValue != null) {
            PropertyAccessorHelper.set(entity, (Field) attribute.getJavaMember(), fieldValue);
        }
    }

    private void initializeRelation(Entity gaeEntity, Attribute attribute, Map<String, Object> relationMap) {
        String jpaColumnName = ((AbstractAttribute) attribute).getJPAColumnName();
        Object fieldValue = gaeEntity.getProperties().get(jpaColumnName);
        System.out.println("jpaColumnName = [" + jpaColumnName + "], fieldValue = [" + fieldValue + "]");

        if (jpaColumnName != null && fieldValue != null) {
            relationMap.put(jpaColumnName, fieldValue);
        }
    }

    @Override
    public <E> List<E> findAll(Class<E> entityClass, String[] columnsToSelect, Object... keys) {
        System.out.println("DatastoreClient.findAll");
        System.out.println("entityClass = [" + entityClass + "], columnsToSelect = [" + columnsToSelect + "], keys = [" + keys + "]");

        // TODO review this
        // List results = new ArrayList();
        // for (Object key : keys) {
        //     Object object = this.find(entityClass, key);
        //     if (object != null) {
        //         results.add(object);
        //     }
        // }
        // return results;
        throw new NotImplementedException();
    }

    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> embeddedColumnMap) {
        System.out.println("DatastoreClient.find");
        System.out.println("entityClass = [" + entityClass + "], embeddedColumnMap = [" + embeddedColumnMap + "]");

        // TODO Auto-generated method stub
        throw new NotImplementedException();
        // return null;
    }

    /*
     * used to retrieve relation for OneToMany
     * (ManyToOne inverse relation)
     */
    @Override
    public List<Object> findByRelation(String colName, Object colValue, Class entityClazz) {
        System.out.println("DatastoreClient.findByRelation");
        System.out.println("colName = [" + colName + "], colValue = [" + colValue + "], entityClazz = [" + entityClazz + "]");

        Query q = new Query(entityClazz.getSimpleName())
                .setFilter(new Query.FilterPredicate(colName,
                        Query.FilterOperator.EQUAL,
                        colValue)).setKeysOnly();
        System.out.println("\n" + q + "\n");

        List<Object> results = new ArrayList<Object>();
        List<Entity> entities = datastore.prepare(q).asList(FetchOptions.Builder.withDefaults());
        if (!entities.isEmpty()) {
            for (Entity entity : entities) {
                results.add(find(entityClazz, entity.getKey().getName()));
            }
        }
        return results;
    }

    /*
     * used to retrieve owner-side relation for ManyToMany
     *
     * for example:
     *      select PROJECT_ID (columnName) from EMPLOYEE_PROJECT (tableName)
     *      where EMPLOYEE_ID (pKeyColumnName) equals (pKeyColumnValue)
     *
     */
    @Override
    public <E> List<E> getColumnsById(String schemaName, String tableName, String pKeyColumnName, String columnName, Object pKeyColumnValue, Class columnJavaType) {
        System.out.println("DatastoreClient.getColumnsById");
        System.out.println("schemaName = [" + schemaName + "], tableName = [" + tableName + "], pKeyColumnName = [" + pKeyColumnName + "], columnName = [" + columnName + "], pKeyColumnValue = [" + pKeyColumnValue + "], columnJavaType = [" + columnJavaType + "]");

        Query q = new Query(tableName)
                .setFilter(new Query.FilterPredicate(pKeyColumnName,
                        Query.FilterOperator.EQUAL,
                        pKeyColumnValue));
        System.out.println("\n" + q + "\n");

        List<E> results = new ArrayList<E>();
        List<Entity> entities = datastore.prepare(q).asList(FetchOptions.Builder.withDefaults());
        System.out.println(columnName + " for " + pKeyColumnName + "[" + pKeyColumnValue + "]:");
        if (!entities.isEmpty()) {
            for (Entity entity : entities) {
                System.out.println("\t" + entity.getProperty(columnName));
                results.add((E) entity.getProperty(columnName));
            }
        }
        System.out.println();
        return results;
    }

    /*
     * used to retrieve target-side relation for ManyToMany
     *
     * for example:
     *      select EMPLOYEE_ID (pKeyName) from EMPLOYEE_PROJECT (tableName)
     *      where PROJECT_ID (columnName) equals (columnValue)
     *
     */
    @Override
    public Object[] findIdsByColumn(String schemaName, String tableName, String pKeyName, String columnName, Object columnValue, Class entityClazz) {
        System.out.println("DatastoreClient.findIdsByColumn");
        System.out.println("schemaName = [" + schemaName + "], tableName = [" + tableName + "], pKeyName = [" + pKeyName + "], columnName = [" + columnName + "], columnValue = [" + columnValue + "], entityClazz = [" + entityClazz + "]");

        Query q = new Query(tableName)
                .setFilter(new Query.FilterPredicate(columnName,
                        Query.FilterOperator.EQUAL,
                        columnValue));
        System.out.println("\n" + q + "\n");

        List<Object> results = new ArrayList<Object>();
        List<Entity> entities = datastore.prepare(q).asList(FetchOptions.Builder.withDefaults());
        System.out.println(pKeyName + " for " + columnName + "[" + columnValue + "]:");
        if (!entities.isEmpty()) {
            for (Entity entity : entities) {
                System.out.println("\t" + entity.getProperty(pKeyName));
                results.add(entity.getProperty(pKeyName));
            }
        }
        System.out.println();
        return results.toArray();
    }

    /*---------------------------------------------------------------------------------*/
    /*----------------------------- DELETE OPERATIONS ----------------------------------*/

    @Override
    public void delete(Object entity, Object pKey) {
        System.out.println("DatastoreClient.delete");
        System.out.println("entity = [" + entity + "], pKey = [" + pKey + "]");

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entity.getClass());
        Key key = KeyFactory.createKey(entityMetadata.getTableName(), (String) pKey);
        datastore.delete(key);

        System.out.println();
    }

    @Override
    public void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue) {
        System.out.println("DatastoreClient.deleteByColumn");
        System.out.println("schemaName = [" + schemaName + "], tableName = [" + tableName + "], columnName = [" + columnName + "], columnValue = [" + columnValue + "]");

        // TODO Auto-generated method stub
        throw new NotImplementedException();
    }
}
