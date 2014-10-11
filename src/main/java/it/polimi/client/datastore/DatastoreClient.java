package it.polimi.client.datastore;

import com.google.appengine.api.datastore.*;
import it.polimi.client.datastore.query.DatastoreQuery;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.generator.AutoGenerator;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.*;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.property.PropertyAccessorHelper;
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

        Entity gaeEntity = new Entity(entityType.getName(), (String) id);

        handleAttributes(gaeEntity, entity, metamodel, entityMetadata, entityType.getAttributes());
        handleRelations(gaeEntity, entityMetadata, rlHolders);
        handleDiscriminatorColumn(gaeEntity, entityType);

        datastore.put(gaeEntity);
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
        return UUID.randomUUID().toString();
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

    private void processAttribute(Entity gaeEntity, Object entity, Attribute attribute) {
        Field field = (Field) attribute.getJavaMember();
        Object valueObj = PropertyAccessorHelper.getObject(entity, field);

        if (valueObj != null) {
            gaeEntity.setProperty(attribute.getName(), valueObj);
        }
    }

    private void processEmbeddableAttribute(Entity gaeEntity, Object entity, Attribute attribute, MetamodelImpl metamodel, EntityMetadata metadata) {
        // TODO Auto-generated method stub
        throw new NotImplementedException();
    }

    private void handleRelations(Entity gaeEntity, EntityMetadata entityMetadata, List<RelationHolder> rlHolders) {
        if (rlHolders != null && !rlHolders.isEmpty()) {
            for (RelationHolder rh : rlHolders) {

                Relation relation = entityMetadata.getRelation(rh.getRelationName());

                System.out.println("\nRelation:[ \n\t"
                        + "relationType: " + relation.getType() + "\n\t"
                        + "mappedBy: " + relation.getMappedBy() + "\n\t"
                        + "biDirectionalField: " + relation.getBiDirectionalField() + "\n\t"
                        + "cascade: " + relation.getCascades() + "\n\t"
                        + "fetchType: " + relation.getFetchType() + "\n\t"
                        + "mapKeyJoinClass: " + relation.getMapKeyJoinClass() + "\n\t"
                        + "property: " + relation.getProperty() + "\n\t"
                        + "propertyType: " + relation.getPropertyType() + "\n\t"
                        + "isBiDirectional: " + relation.isBiDirectional() + "\n\t"
                        + "isCollection: " + relation.isCollection() + "\n\t"
                        + "isOptional: " + relation.isOptional() + "\n\t"
                        + "isRelatedViaJoinTable: " + relation.isRelatedViaJoinTable() + "\n\t"
                        + "isUnary: " + relation.isUnary() + "\n\t"
                        + "joinTableMetadata: " + relation.getJoinTableMetadata() + "\n\t"
                        + "joinColumnName: " + relation.getJoinColumnName(kunderaMetadata) + "\n\t"
                        + "targetEntity: " + relation.getTargetEntity().getCanonicalName() + "\n]");
                System.out.println("RelationHolder:[ \n\t"
                        + "relationName: " + rh.getRelationName() + "\n\t"
                        + "relationVia: " + rh.getRelationVia() + "\n\t"
                        + "relationValue: " + rh.getRelationValue() + "\n]\n");

                // TODO if fully classified class name use .getCanonicalName()
                String targetClass = relation.getTargetEntity().getSimpleName();
                String targetId = (String) rh.getRelationValue();
                Key targetKey = KeyFactory.createKey(targetClass, targetId);
                gaeEntity.setProperty(rh.getRelationName(), targetKey);
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
            Entity gaeEntity = get(entityClass, id);
            System.out.println("gaeEntity:[ \n\t"
                    + "kind: " + gaeEntity.getKind() + "\n\t"
                    + "name: " + gaeEntity.getKey().getName() + "\n]\n");
            return initializeEntity(gaeEntity, entityClass);
        } catch (InstantiationException e) {
            e.printStackTrace();
            throw new KunderaException();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            throw new KunderaException();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new KunderaException();
        }
    }

    private Entity get(Class clazz, Object id) {
        System.out.println("DatastoreClient.get");
        System.out.println("clazz = [" + clazz + "], id = [" + id + "]");
        try {
            // TODO if fully classified class name use .getCanonicalName()
            Key key = KeyFactory.createKey(clazz.getSimpleName(), (String) id);
            return datastore.get(key);
        } catch (EntityNotFoundException e) {
            return null;
        }
    }

    private Object initializeEntity(Entity gaeEntity, Class entityClass) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        System.out.println("DatastoreClient.initializeEntity");
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);
        String idColumnName = ((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName();
        String entityId = gaeEntity.getKey().getName();
        // use reflection to instantiate the entity class
        Object entity = entityMetadata.getEntityClazz().newInstance();
        // use reflection to set entity fields
        Map<String, Object> relationMap = new HashMap<String, Object>();
        for (Field field : entityClass.getDeclaredFields()) {
            if (field.getName().equals(idColumnName)) {
                // id
                System.out.println("Fill id: " + field.getName());
                setFiled(field, entity, entityId);
            } else if (entityMetadata.getRelation(field.getName()) != null) {
                // relations
                Relation relation = entityMetadata.getRelation(field.getName());
                System.out.println("Fill relation: " + field.getName());
                System.out.println("\tRelation:[ \n\t\t"
                        + "relationType: " + relation.getType() + "\n\t\t"
                        + "mappedBy: " + relation.getMappedBy() + "\n\t\t"
                        + "joinTableMetadata: " + relation.getJoinTableMetadata() + "\n\t\t"
                        + "joinColumnName: " + relation.getJoinColumnName(kunderaMetadata) + "\n\t\t"
                        + "targetEntity: " + relation.getTargetEntity().getCanonicalName() + "\n\t]");
                Key relKey = (Key) gaeEntity.getProperties().get(field.getName());
                if (relKey != null) {
                    System.out.println("\tKey:[ \n\t\t"
                            + "kind: " + relKey.getKind() + "\n\t\t"
                            + "name: " + relKey.getName() + "\n\t]");
                    relationMap.put(field.getName(), relKey.getName());
                } else {
                    System.out.println("\tNo rel key, inverse relation?");

                    String targetClass = relation.getTargetEntity().getSimpleName();
                    System.out.println("\ttargetClass = " + targetClass);
                    String targetField = relation.getMappedBy();
                    System.out.println("\ttargetField = " + targetField);
                    Key key = gaeEntity.getKey();
                    System.out.println("\tsubjectKey = " + key);
                    Query q = new Query(targetClass)
                            .setFilter(new Query.FilterPredicate(targetField,
                                    Query.FilterOperator.EQUAL,
                                    key)).setKeysOnly();
                    List<Entity> results = datastore.prepare(q).asList(FetchOptions.Builder.withDefaults());
                    if (relation.getType().equals(Relation.ForeignKey.ONE_TO_ONE)) {
                        System.out.println("\tONE TO ONE -> results");
                        System.out.println("\tresults.isEmpty()? " + results.isEmpty());
                        System.out.println("\tresultKey:[ \n\t\t"
                                + "kind: " + results.get(0).getKey().getKind() + "\n\t\t"
                                + "name: " + results.get(0).getKey().getName() + "\n\t]");

                        relationMap.put(field.getName(), results.get(0).getKey().getName());
                    } else if (relation.getType().equals(Relation.ForeignKey.ONE_TO_MANY)) {
                        System.out.println("\tONE TO MANY");
                        for (Entity e : results) {
                            oneToManyPrefetch.put(e.getKey(), relation.getTargetEntity());
                            System.out.println("\tresultKey:[ \n\t\t"
                                    + "kind: " + e.getKey().getKind() + "\n\t\t"
                                    + "name: " + e.getKey().getName() + "\n\t]");
                        }
                        System.out.println("\toneToManyPrefetch.isEmpty()? " + oneToManyPrefetch.isEmpty());
                    }
                }
            } else if (gaeEntity.getProperties().containsKey(field.getName())) {
                //other fields
                System.out.println("Fill field: " + field.getName());
                Object fieldValue = gaeEntity.getProperties().get(field.getName());
                setFiled(field, entity, fieldValue);
            }
        }
        System.out.println();

        return new EnhanceEntity(entity, entityId, relationMap.isEmpty() ? null : relationMap);
    }

    private void setFiled(Field field, Object entity, Object value) throws IllegalAccessException {
        field.setAccessible(true);
        field.set(entity, value);
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
        throw new NotImplementedException();
        // return null;
    }

    @Override
    public List<Object> findByRelation(String colName, Object colValue, Class entityClazz) {
        System.out.println("DatastoreClient.findByRelation");
        System.out.println("colName = [" + colName + "], colValue = [" + colValue + "], entityClazz = [" + entityClazz + "]");

        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClazz);
        Entity entity = null;
        Class targetClass = null;
        String targetField = null;
        for (Relation relation : metadata.getRelations()) {
            targetClass =  relation.getTargetEntity();
            targetField = relation.getProperty().getName();
            entity = get(targetClass, colValue);
            if (entity != null) {
                System.out.println("id [" + colValue + "] is of [" + entity.getKind() + "]");
                break;
            }
        }
        if(entity != null) {
            System.out.println(targetField);
            Query q = new Query(entityClazz.getSimpleName())
                    .setFilter(new Query.FilterPredicate(targetField,
                            Query.FilterOperator.EQUAL,
                            entity.getKey()));
            System.out.println(q);
            List<Entity> gaeResults = datastore.prepare(q).asList(FetchOptions.Builder.withDefaults());
            System.out.println("Query done");
            List<Object> results = new ArrayList<Object>();
            for (Entity e : gaeResults) {
                System.out.println("result: " + e);
                try {
                    results.add(initializeEntity(e, entityClazz));
                } catch (IllegalAccessException e1) {
                    e1.printStackTrace();
                } catch (InstantiationException e1) {
                    e1.printStackTrace();
                } catch (ClassNotFoundException e1) {
                    e1.printStackTrace();
                }
            }
            return results;
        }
        return null;

//        if (!oneToManyPrefetch.isEmpty()) {
//            List<Object> results = new ArrayList<Object>();
//            for (Key key : oneToManyPrefetch.keySet()) {
//                Class targetClass = oneToManyPrefetch.get(key);
//                results.add(find(targetClass, key.getName()));
//            }
//            return results;
//        } else {
//            System.out.println("oneToManyPrefetch is empty");
//            return null;
//        }
    }

    /*---------------------------------------------------------------------------------*/
    /*----------------------------- DELETE OPERATIONS ----------------------------------*/

    @Override
    public void delete(Object entity, Object pKey) {
        System.out.println("DatastoreClient.delete");
        System.out.println("entity = [" + entity + "], pKey = [" + pKey + "]");
        Key key = KeyFactory.createKey(entity.getClass().getSimpleName(), (String) pKey);
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
