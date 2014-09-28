package com.impetus.client.datastore;

import com.google.appengine.api.datastore.DatastoreService;
import com.impetus.client.datastore.query.DatastoreQuery;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.ClientMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.metamodel.EntityType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Fabio Arcidiacono.
 *         <p>The gateway to CRUD operations on database, except for queries.<p/>
 */
public class DatastoreClient extends ClientBase implements Client<DatastoreQuery> {

    private static final Logger logger = LoggerFactory.getLogger(DatastoreClient.class);
    private EntityReader reader;
    private DatastoreService datastore;
    private int batchSize;

    protected DatastoreClient(final KunderaMetadata kunderaMetadata, Map<String, Object> properties,
                              String persistenceUnit, final ClientMetadata clientMetadata, IndexManager indexManager,
                              EntityReader reader, final DatastoreService datastore) {
        super(kunderaMetadata, properties, persistenceUnit);
        System.out.println("DatastoreClient.DatastoreClient");
        this.reader = reader;
        this.datastore = datastore;
        this.indexManager = indexManager;
        this.clientMetadata = clientMetadata;
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

    // Persist operation

    /**
     * A Node object would be available in this method that holds data variable referring to your entity
     * object. This method is responsible for writing node data to underlying database.
     */
    @Override
    protected void onPersist(EntityMetadata entityMetadata, Object entity, Object id, List<RelationHolder> rlHolders) {

        MetamodelImpl metamodel = KunderaMetadataManager.getMetamodel(kunderaMetadata,
                entityMetadata.getPersistenceUnit());

        EntityType entityType = metamodel.entity(entityMetadata.getEntityClazz());
        System.out.println("[DatastoreClient.onPersist] Persisting entity " + entity + " into " + entityMetadata.getSchema() + "." + entityMetadata.getTableName()
                + " with id " + id);

        // Set<Attribute> attributes = entityType.getAttributes();
        //
        // // attribute handling TODO extract method
        // for (Attribute attribute : attributes){
        //     // by pass association. TODO understand this isAssociation()
        //     if (!attribute.isAssociation()){
        //
        //     }
        // }
        //
        // // Iterate over relations  TODO extract method
        // if (rlHolders != null && !rlHolders.isEmpty()) {
        //     for (RelationHolder rh : rlHolders) {
        //         String relationName = rh.getRelationName();
        //         Object valueObj = rh.getRelationValue();
        //
        //         if (!StringUtils.isEmpty(relationName) && valueObj != null) {
        //
        //         }
        //     }
        // }
        //
        //
        // //descriminator TODO extract method
        // String discrColumn = ((AbstractManagedType) entityType).getDiscriminatorColumn();
        // String discrValue = ((AbstractManagedType) entityType).getDiscriminatorValue();
        //
        // if (discrColumn != null && discrValue != null){
        //
        // }

        throw new NotImplementedException("");
    }

    @Override
    public void persistJoinTable(JoinTableData joinTableData) {
        // TODO Auto-generated method stub
        throw new NotImplementedException("");
    }

    // Find operations

    /**
     * This is called by Kundera when find method is invoked on Entity Manager. This method is responsible
     * for fetching data from underlying database for given entity class and primary key.
     */
    @Override
    public Object find(Class entityClass, Object key) {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);
        Object entity = null;
        // TODO Auto-generated method stub
        throw new NotImplementedException("find(Class entityClass, Object key)");
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
        // TODO Auto-generated method stub
        throw new NotImplementedException("find ... embeddedColumnMap");
        // return null;
    }

    @Override
    public Object[] findIdsByColumn(String schemaName, String tableName, String pKeyName, String columnName, Object columnValue, Class entityClazz) {
        // TODO Auto-generated method stub
        throw new NotImplementedException("findIdsByColumn");
        // return null;
    }

    @Override
    public List<Object> findByRelation(String colName, Object colValue, Class entityClazz) {
        // TODO Auto-generated method stub
        throw new NotImplementedException("findByRelation");
        //return null;
    }

    // Delete operations

    /**
     * This is called by Kundera when a remove method is invoked on entity manager.
     * This is responsible for removing entity object from underlying database.
     */
    @Override
    public void delete(Object entity, Object pKey) {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entity.getClass());
        // TODO Auto-generated method stub
        throw new NotImplementedException("delete");
    }

    @Override
    public void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue) {
        // TODO Auto-generated method stub
        throw new NotImplementedException("deleteByColumn");
    }

    //Get operation

    @Override
    public <E> List<E> getColumnsById(String schemaName, String tableName, String pKeyColumnName, String columnName, Object pKeyColumnValue, Class columnJavaType) {
        // TODO Auto-generated method stub
        throw new NotImplementedException("getColumnsById");
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
