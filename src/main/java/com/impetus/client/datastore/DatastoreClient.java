package com.impetus.client.datastore;

import com.google.appengine.api.datastore.DatastoreService;
import com.impetus.client.datastore.query.DatastoreQuery;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.model.ClientMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author Fabio Arcidiacono.
 */
public class DatastoreClient extends ClientBase implements Client<DatastoreQuery> {

    private static final Logger logger = LoggerFactory.getLogger(DatastoreClient.class);
    private EntityReader reader;
    private DatastoreService datastore;
    private ClientMetadata clientMetadata;  //seems that is never used also in other clients

    protected DatastoreClient(final KunderaMetadata kunderaMetadata, Map<String, Object> properties,
                              String persistenceUnit, final ClientMetadata clientMetadata, IndexManager indexManager,
                              EntityReader reader, final DatastoreService datastore) {
        super(kunderaMetadata, properties, persistenceUnit);
        this.reader = reader;
        this.datastore = datastore;
        this.indexManager = indexManager;
        this.clientMetadata = clientMetadata;
        // TODO check bash size? (some other dbs do that)
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
        // TODO Auto-generated method stub
    }

    @Override
    public void persistJoinTable(JoinTableData joinTableData) {
        // TODO Auto-generated method stub
    }

    // Find operations

    /**
     * This is called by Kundera when find method is invoked on Entity Manager. This method is responsible
     * for fetching data from underlying database for given entity class and primary key.
     */
    @Override
    public Object find(Class entityClass, Object key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <E> List<E> findAll(Class<E> entityClass, String[] columnsToSelect, Object... keys) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> embeddedColumnMap) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object[] findIdsByColumn(String schemaName, String tableName, String pKeyName, String columnName, Object columnValue, Class entityClazz) {
        // TODO Auto-generated method stub
        return new Object[0];
    }

    @Override
    public List<Object> findByRelation(String colName, Object colValue, Class entityClazz) {
        // TODO Auto-generated method stub
        return null;
    }

    // Delete operations

    /**
     * This is called by Kundera when a remove method is invoked on entity manager.
     * This is responsible for removing entity object from underlying database.
     */
    @Override
    public void delete(Object entity, Object pKey) {
        // TODO Auto-generated method stub
    }

    @Override
    public void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue) {
        // TODO Auto-generated method stub
    }

    //Get operation
    @Override
    public <E> List<E> getColumnsById(String schemaName, String tableName, String pKeyColumnName, String columnName, Object pKeyColumnValue, Class columnJavaType) {
        // TODO Auto-generated method stub
        return null;
    }
}
