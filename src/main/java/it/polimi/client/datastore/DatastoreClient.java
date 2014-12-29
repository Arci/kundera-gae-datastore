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
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.property.accessor.EnumAccessor;
import it.polimi.client.datastore.query.DatastoreQuery;
import it.polimi.client.datastore.query.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;

/**
 * The gateway to CRUD operations on database, except for queries.
 *
 * @author Fabio Arcidiacono.
 * @see com.impetus.kundera.client.ClientBase
 * @see com.impetus.kundera.client.Client
 * @see it.polimi.client.datastore.query.DatastoreQuery
 * @see com.impetus.kundera.generator.AutoGenerator
 */
public class DatastoreClient extends ClientBase implements Client<DatastoreQuery>, AutoGenerator {

    private EntityReader reader;
    private DatastoreService datastore;
    private static final Logger logger;

    static {
        logger = LoggerFactory.getLogger(DatastoreClient.class);
    }

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
        this.datastore = null;
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

    @Override
    public Object generate() {
        /*
         * use random UUID instead of datastore generated
         * since here is not available the class of the entity to be persisted.
         */
        return UUID.randomUUID().toString();
    }

    /*---------------------------------------------------------------------------------*/
    /*----------------------------- PERSIST OPERATIONS --------------------------------*/
    /*---------------------------------------------------------------------------------*/

    @Override
    protected void onPersist(EntityMetadata entityMetadata, Object entity, Object id, List<RelationHolder> rlHolders) {
        logger.debug("entityMetadata = [" + entityMetadata + "], entity = [" + entity + "], id = [" + id + "], rlHolders = [" + rlHolders + "]");

        MetamodelImpl metamodel = KunderaMetadataManager.getMetamodel(kunderaMetadata,
                entityMetadata.getPersistenceUnit());
        EntityType entityType = metamodel.entity(entityMetadata.getEntityClazz());

        Entity gaeEntity = DatastoreUtils.createDatastoreEntity(entityMetadata, id);

        handleAttributes(gaeEntity, entity, metamodel, entityMetadata, entityType.getAttributes());
        handleRelations(gaeEntity, entityMetadata, rlHolders);
        /* discriminator column is used for JPA inheritance */
        handleDiscriminatorColumn(gaeEntity, entityType);

        datastore.put(gaeEntity);
        logger.info(gaeEntity.toString());
    }

    private void handleAttributes(Entity gaeEntity, Object entity, MetamodelImpl metamodel, EntityMetadata entityMetadata, Set<Attribute> attributes) {
        String idAttribute = ((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName();
        for (Attribute attribute : attributes) {
            // By pass ID attribute, is redundant since is also stored within the Key.
            // By pass associations (i.e. relations) that are handled in handleRelations()
            if (!attribute.isAssociation() && !((AbstractAttribute) attribute).getJPAColumnName().equals(idAttribute)) {
                if (metamodel.isEmbeddable(((AbstractAttribute) attribute).getBindableJavaType())) {
                    processEmbeddableAttribute(gaeEntity, entity, attribute, metamodel);
                } else {
                    processAttribute(gaeEntity, entity, attribute);
                }
            }
        }
    }

    private void processAttribute(PropertyContainer gaeEntity, Object entity, Attribute attribute) {
        Field field = (Field) attribute.getJavaMember();
        Object valueObj = PropertyAccessorHelper.getObject(entity, field);
        String jpaColumnName = ((AbstractAttribute) attribute).getJPAColumnName();

        if (valueObj instanceof Collection<?> || valueObj instanceof Map<?, ?>) {
            try {
                logger.debug("field = [" + field.getName() + "], objectType = [" + valueObj.getClass().getName() + "]");
                valueObj = DatastoreUtils.serialize(valueObj);
            } catch (IOException e) {
                throw new KunderaException("Some errors occurred while serializing the object: ", e);
            }
        } else if (((Field) attribute.getJavaMember()).getType().isEnum()) {
            valueObj = valueObj.toString();
        }
        if (valueObj != null) {
            logger.debug("field = [" + field.getName() + "], jpaColumnName = [" + jpaColumnName + "], valueObj = [" + valueObj + "]");
            gaeEntity.setProperty(jpaColumnName, valueObj);
        }
    }

    private void processEmbeddableAttribute(Entity gaeEntity, Object entity, Attribute attribute, MetamodelImpl metamodel) {
        Field field = (Field) attribute.getJavaMember();
        String jpaColumnName = ((AbstractAttribute) attribute).getJPAColumnName();
        Object embeddedObj = PropertyAccessorHelper.getObject(entity, field);
        logger.debug("field = [" + field.getName() + "], jpaColumnName = [" + jpaColumnName + "], embeddedObj = [" + embeddedObj + "]");

        EmbeddedEntity embeddedEntity = new EmbeddedEntity();
        EmbeddableType embeddable = metamodel.embeddable(((AbstractAttribute) attribute).getBindableJavaType());
        Set<Attribute> embeddedAttributes = embeddable.getAttributes();
        for (Attribute embeddedAttribute : embeddedAttributes) {
            processAttribute(embeddedEntity, embeddedObj, embeddedAttribute);
        }
        gaeEntity.setProperty(jpaColumnName, embeddedEntity);
    }

    private void handleRelations(Entity gaeEntity, EntityMetadata entityMetadata, List<RelationHolder> rlHolders) {
        if (rlHolders != null && !rlHolders.isEmpty()) {
            for (RelationHolder rh : rlHolders) {
                String jpaColumnName = rh.getRelationName();
                String fieldName = entityMetadata.getFieldName(jpaColumnName);
                Relation relation = entityMetadata.getRelation(fieldName);
                Object targetId = rh.getRelationValue();

                if (relation != null && jpaColumnName != null && targetId != null) {
                    Key targetKey = DatastoreUtils.createKey(relation.getTargetEntity().getSimpleName(), targetId);
                    logger.debug("field = [" + fieldName + "], jpaColumnName = [" + jpaColumnName + "], targetKey = [" + targetKey + "]");
                    gaeEntity.setProperty(jpaColumnName, targetKey);
                }
            }
        }
    }

    private void handleDiscriminatorColumn(Entity gaeEntity, EntityType entityType) {
        String discriminatorColumn = ((AbstractManagedType) entityType).getDiscriminatorColumn();
        String discriminatorValue = ((AbstractManagedType) entityType).getDiscriminatorValue();

        if (discriminatorColumn != null && discriminatorValue != null) {
            logger.debug("discriminatorColumn = [" + discriminatorColumn + "], discriminatorValue = [" + discriminatorValue + "]");
            gaeEntity.setProperty(discriminatorColumn, discriminatorValue);
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
     *  |          id (owner)           |             id (child)               |
     *  -----------------------------------------------------------------------
     */
    @Override
    public void persistJoinTable(JoinTableData joinTableData) {
        String joinTableName = joinTableData.getJoinTableName();
        String joinColumnName = joinTableData.getJoinColumnName();
        String inverseJoinColumnName = joinTableData.getInverseJoinColumnName();
        Map<Object, Set<Object>> joinTableRecords = joinTableData.getJoinTableRecords();

        for (Object owner : joinTableRecords.keySet()) {
            Set<Object> children = joinTableRecords.get(owner);
            for (Object child : children) {
                /* let datastore generate ID for the entity */
                Entity gaeEntity = DatastoreUtils.createDatastoreEntity(joinTableName);
                gaeEntity.setProperty(joinColumnName, owner);
                gaeEntity.setProperty(inverseJoinColumnName, child);
                datastore.put(gaeEntity);
                logger.info(gaeEntity.toString());
            }
        }
    }

    /*---------------------------------------------------------------------------------*/
    /*------------------------------ FIND OPERATIONS ----------------------------------*/
    /*---------------------------------------------------------------------------------*/

    /*
     * it's called to find detached entities
     */
    @Override
    public Object find(Class entityClass, Object id) {
        logger.debug("entityClass = [" + entityClass.getSimpleName() + "], id = [" + id + "]");

        try {
            EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);
            Entity gaeEntity = get(entityMetadata.getTableName(), id);
            if (gaeEntity == null) {
                /* case not found */
                return null;
            }
            logger.info(gaeEntity.toString());
            return initializeEntity(gaeEntity, entityClass);
        } catch (InstantiationException | IllegalAccessException e) {
            throw new KunderaException(e);
        }
    }

    private Entity get(String kind, Object id) {
        try {
            Key key;
            if (id instanceof Key) {
                /* case id is field retrieved from datastore */
                key = (Key) id;
            } else {
                key = DatastoreUtils.createKey(kind, id);
            }
            return datastore.get(key);
        } catch (EntityNotFoundException e) {
            logger.info("Not found {kind = [" + kind + "], id = [" + id + "]}");
            return null;
        }
    }

    private EnhanceEntity initializeEntity(Entity gaeEntity, Class entityClass) throws IllegalAccessException, InstantiationException {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);
        MetamodelImpl metamodel = KunderaMetadataManager.getMetamodel(kunderaMetadata,
                entityMetadata.getPersistenceUnit());
        EntityType entityType = metamodel.entity(entityMetadata.getEntityClazz());

        Map<String, Object> relationMap = new HashMap<>();
        Object entity = entityMetadata.getEntityClazz().newInstance();

        initializeID(gaeEntity, entityMetadata, entity);
        Set<Attribute> attributes = entityType.getAttributes();
        for (Attribute attribute : attributes) {
            if (!attribute.isAssociation()) {
                if (metamodel.isEmbeddable(((AbstractAttribute) attribute).getBindableJavaType())) {
                    initializeEmbeddedAttribute(gaeEntity, entity, attribute, metamodel);
                } else {
                    initializeAttribute(gaeEntity, entity, attribute);
                }
            } else {
                initializeRelation(gaeEntity, attribute, relationMap);
            }
        }
        logger.info(entity.toString());

        return new EnhanceEntity(entity, gaeEntity.getKey().getName(), relationMap.isEmpty() ? null : relationMap);
    }

    private void initializeID(Entity gaeEntity, EntityMetadata entityMetadata, Object entity) {
        String jpaColumnName = ((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName();
        Object id = gaeEntity.getKey().getName();
        if (id == null) {
            /* case datastore generated long */
            id = gaeEntity.getKey().getId();
        }
        logger.debug("jpaColumnName = [" + jpaColumnName + "], fieldValue = [" + id + "]");
        PropertyAccessorHelper.setId(entity, entityMetadata, id);
    }

    /* (non-Javadoc)
     *
     * PropertyContainer is used to handle both EmbeddedEntity and Entity.
     *
     * @see com.google.appengine.api.datastore.PropertyContainer
     * @see com.google.appengine.api.datastore.EmbeddedEntity
     * @see com.google.appengine.api.datastore.Entity
     */
    private void initializeAttribute(PropertyContainer gaeEntity, Object entity, Attribute attribute) {
        String jpaColumnName = ((AbstractAttribute) attribute).getJPAColumnName();
        Object fieldValue = gaeEntity.getProperties().get(jpaColumnName);

        if (fieldValue instanceof Blob) {
            try {
                /* deserialize reconstruct also the original object class */
                fieldValue = DatastoreUtils.deserialize((Blob) fieldValue);
            } catch (ClassNotFoundException | IOException e) {
                throw new KunderaException("Some errors occurred while deserializing the object: ", e);
            }
        } else if (((Field) attribute.getJavaMember()).getType().isEnum()) {
            EnumAccessor accessor = new EnumAccessor();
            fieldValue = accessor.fromString(((AbstractAttribute) attribute).getBindableJavaType(), fieldValue.toString());
        }
        if (jpaColumnName != null && fieldValue != null) {
            logger.debug("jpaColumnName = [" + jpaColumnName + "], fieldValue = [" + fieldValue + "]");
            PropertyAccessorHelper.set(entity, (Field) attribute.getJavaMember(), fieldValue);
        }
    }

    private void initializeEmbeddedAttribute(Entity gaeEntity, Object entity, Attribute attribute, MetamodelImpl metamodel) throws IllegalAccessException, InstantiationException {
        String jpaColumnName = ((AbstractAttribute) attribute).getJPAColumnName();
        EmbeddedEntity embeddedEntity = (EmbeddedEntity) gaeEntity.getProperties().get(jpaColumnName);

        if (jpaColumnName != null && embeddedEntity != null) {
            logger.debug("jpaColumnName = [" + jpaColumnName + "], embeddedEntity = [" + embeddedEntity + "]");

            EmbeddableType embeddable = metamodel.embeddable(((AbstractAttribute) attribute).getBindableJavaType());
            Object embeddedObj = embeddable.getJavaType().newInstance();
            Set<Attribute> embeddedAttributes = embeddable.getAttributes();
            for (Attribute embeddedAttribute : embeddedAttributes) {
                initializeAttribute(embeddedEntity, embeddedObj, embeddedAttribute);
            }
            PropertyAccessorHelper.set(entity, (Field) attribute.getJavaMember(), embeddedObj);
        }
    }

    private void initializeRelation(Entity gaeEntity, Attribute attribute, Map<String, Object> relationMap) {
        String jpaColumnName = ((AbstractAttribute) attribute).getJPAColumnName();
        Object fieldValue = gaeEntity.getProperties().get(jpaColumnName);
        logger.debug("jpaColumnName = [" + jpaColumnName + "], fieldValue = [" + fieldValue + "]");

        if (jpaColumnName != null && fieldValue != null) {
            relationMap.put(jpaColumnName, fieldValue);
        }
    }

    /*
     * Implicitly it gets invoked, when kundera.indexer.class or lucene.home.dir is configured.
     * Means to use custom indexer for secondary indexes.
     * This method can also be very helpful to find rows for all primary keys! as with
     * em.getDelegate() you can get a handle of client object and can simply invoke findAll().
     */
    @Override
    public <E> List<E> findAll(Class<E> entityClass, String[] columnsToSelect, Object... keys) {
        logger.debug("entityClass = [" + entityClass + "], columnsToSelect = [" + Arrays.toString(columnsToSelect) + "], keys = [" + Arrays.toString(keys) + "]");

        List results = new ArrayList();
        for (Object key : keys) {
            Object object = this.find(entityClass, key);
            if (object != null) {
                results.add(object);
            }
        }
        return results;
    }

    /*
     * It can be ignored, It was in place to purely support Cassandra's super columns.
     */
    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> embeddedColumnMap) {
        throw new UnsupportedOperationException("Not supported in " + this.getClass().getSimpleName());
    }

    /*
     * used to retrieve relation for OneToMany (ManyToOne inverse relation),
     * is supposed to retrieve the initialized objects.
     *
     * for example:
     *      select * from EmployeeMTObis (entityClass)
     *      where DEPARTMENT_ID (colName) equals (colValue)
     */
    @Override
    public List<Object> findByRelation(String colName, Object colValue, Class entityClass) {
        logger.debug("colName = [" + colName + "], colValue = [" + colValue + "], entityClazz = [" + entityClass + "]");

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);
        String fieldName = entityMetadata.getFieldName(colName);
        Relation relation = entityMetadata.getRelation(fieldName);
        Key targetKey = DatastoreUtils.createKey(relation.getTargetEntity().getSimpleName(), colValue);

        Query query = generateRelationQuery(entityClass.getSimpleName(), colName, targetKey);
        query.setKeysOnly();

        List<Object> results = new ArrayList<>();
        List<Entity> entities = getQueryResults(query);
        if (!entities.isEmpty()) {
            for (Entity entity : entities) {
                results.add(find(entityClass, entity.getKey()));
            }
        }
        return results;
    }

    /*
     * used to retrieve owner-side relation for ManyToMany,
     * is supposed to retrieve the objects id.
     *
     * for example:
     *      select PROJECT_ID (columnName) from EMPLOYEE_PROJECT (tableName)
     *      where EMPLOYEE_ID (pKeyColumnName) equals (pKeyColumnValue)
     *
     */
    @Override
    public <E> List<E> getColumnsById(String schemaName, String tableName, String pKeyColumnName, String columnName, Object pKeyColumnValue, Class columnJavaType) {
        logger.debug("schemaName = [" + schemaName + "], tableName = [" + tableName + "], pKeyColumnName = [" + pKeyColumnName + "], columnName = [" + columnName + "], pKeyColumnValue = [" + pKeyColumnValue + "], columnJavaType = [" + columnJavaType + "]");

        Query query = generateRelationQuery(tableName, pKeyColumnName, pKeyColumnValue);

        List<E> results = new ArrayList<>();
        List<Entity> entities = getQueryResults(query);
        logger.debug(columnName + " for " + pKeyColumnName + "[" + pKeyColumnValue + "]:");
        if (!entities.isEmpty()) {
            for (Entity entity : entities) {
                logger.debug("\t" + entity.getProperty(columnName));
                results.add((E) entity.getProperty(columnName));
            }
        }
        return results;
    }

    /*
     * used to retrieve target-side relation for ManyToMany,
     * is supposed to retrieve the objects id.
     *
     * for example:
     *      select EMPLOYEE_ID (pKeyName) from EMPLOYEE_PROJECT (tableName)
     *      where PROJECT_ID (columnName) equals (columnValue)
     *
     */
    @Override
    public Object[] findIdsByColumn(String schemaName, String tableName, String pKeyName, String columnName, Object columnValue, Class entityClazz) {
        logger.debug("schemaName = [" + schemaName + "], tableName = [" + tableName + "], pKeyName = [" + pKeyName + "], columnName = [" + columnName + "], columnValue = [" + columnValue + "], entityClazz = [" + entityClazz + "]");

        Query query = generateRelationQuery(tableName, columnName, columnValue);

        List<Object> results = new ArrayList<>();
        List<Entity> entities = getQueryResults(query);
        logger.debug(pKeyName + " for " + columnName + "[" + columnValue + "]:");
        if (!entities.isEmpty()) {
            for (Entity entity : entities) {
                logger.debug("\t" + entity.getProperty(pKeyName));
                results.add(entity.getProperty(pKeyName));
            }
        }
        return results.toArray();
    }

    /*---------------------------------------------------------------------------------*/
    /*----------------------------- DELETE OPERATIONS ---------------------------------*/
    /*---------------------------------------------------------------------------------*/

    @Override
    public void delete(Object entity, Object pKey) {
        logger.debug("entity = [" + entity + "], pKey = [" + pKey + "]");

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entity.getClass());
        Key key = DatastoreUtils.createKey(entityMetadata.getTableName(), pKey);
        datastore.delete(key);
    }

    /*
     * used to delete relation for ManyToMany
     *
     * for example:
     *      delete from EMPLOYEE_PROJECT (tableName)
     *      where EMPLOYEE_ID (columnName) equals (columnValue)
     *
     */
    @Override
    public void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue) {
        logger.debug("schemaName = [" + schemaName + "], tableName = [" + tableName + "], columnName = [" + columnName + "], columnValue = [" + columnValue + "]");

        Query query = generateRelationQuery(tableName, columnName, columnValue);
        query.setKeysOnly();

        List<Entity> entities = getQueryResults(query);
        if (!entities.isEmpty()) {
            for (Entity entity : entities) {
                datastore.delete(entity.getKey());
            }
        }
    }

    /*---------------------------------------------------------------------------------*/
    /*-------------------------------- QUERY UTILS ------------------------------------*/
    /*---------------------------------------------------------------------------------*/

    private Query generateRelationQuery(String tableName, String columnName, Object columnValue) {
        Query query = new Query(tableName)
                .setFilter(new Query.FilterPredicate(columnName,
                        Query.FilterOperator.EQUAL,
                        columnValue));
        logger.debug(query.toString());
        return query;
    }

    public List<Object> executeQuery(QueryBuilder builder) {
        logger.info(builder.getQuery().toString());

        List<Object> results = new ArrayList<>();
        List<Entity> entities = getQueryResults(builder.getQuery(), builder.getLimit());
        for (Entity entity : entities) {
            logger.debug(entity.toString());
            try {
                EnhanceEntity ee = initializeEntity(entity, builder.getEntityClass());
                if (!builder.isProjectionQuery()) {
                    if (!builder.holdRelationships()) {
                        /* comes from DatastoreQuery.populateEntities */
                        results.add(ee.getEntity());
                    } else {
                        /* comes from DatastoreQuery.recursivelyPopulateEntities */
                        results.add(ee);
                    }
                } else {
                    for (String column : builder.getProjections()) {
                        if (entity.hasProperty(column)) {
                            results.add(entity.getProperty(column));
                        } else {
                            throw new KunderaException("Entity " + entity.getKind() + " does not contains property " + column + "");
                        }
                    }
                }
            } catch (InstantiationException | IllegalAccessException e) {
                throw new KunderaException(e);
            }
        }
        return results;
    }

    private List<Entity> getQueryResults(Query query) {
        return datastore.prepare(query).asList(FetchOptions.Builder.withDefaults());
    }

    private List<Entity> getQueryResults(Query query, int limit) {
        logger.info("set query result limit to: " + limit);
        return datastore.prepare(query).asList(FetchOptions.Builder.withLimit(limit));
    }
}
