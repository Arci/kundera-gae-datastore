package com.impetus.client.datastore.query;

import com.impetus.client.datastore.DatastoreEntityReader;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.QueryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

/**
 * @author Fabio Arcidiacono.
 *
 * used by Kundera to run JPA queries by invoking appropriate methods in Entity Readers.
 *
 */
public class DatastoreQuery extends QueryImpl {

    private static Logger logger = LoggerFactory.getLogger(DatastoreQuery.class);
    private DatastoreEntityReader reader;

    /**
     * Instantiates a new query impl.
     *
     * @param kunderaQuery
     * @param persistenceDelegator the persistence delegator
     * @param kunderaMetadata
     */
    public DatastoreQuery(KunderaQuery kunderaQuery, PersistenceDelegator persistenceDelegator, EntityManagerFactoryImpl.KunderaMetadata kunderaMetadata) {
        super(kunderaQuery, persistenceDelegator, kunderaMetadata);
    }

    /**
     * This method would be called by Kundera to populate entities while it doesn't hold any relationships.
     */
    @Override
    protected List<Object> populateEntities(EntityMetadata m, Client client) {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * This method would be called by Kundera to populate entities while it holds relationships.
     */
    @Override
    protected List<Object> recursivelyPopulateEntities(EntityMetadata m, Client client) {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * Initialize and return your entity reader here.
     */
    @Override
    protected EntityReader getReader() {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * This method is called by Kundera when executeUpdate method is invoked on query instance that
     * represents update/ delete query. Your responsibility would be to call appropriate methods of client.
     */
    @Override
    protected int onExecuteUpdate() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

    @Override
    public Iterator iterate() {
        // TODO Auto-generated method stub
        return null;
    }
}
