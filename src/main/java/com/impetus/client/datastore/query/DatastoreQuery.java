package com.impetus.client.datastore.query;

import com.impetus.client.datastore.DatastoreEntityReader;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.QueryImpl;
import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Queue;

/**
 * @author Fabio Arcidiacono.
 *         <p>Used by Kundera to run JPA queries by invoking appropriate methods in Entity Readers.</p>
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
        logger.info("query constructor");
    }

    /**
     * This method would be called by Kundera to populate entities while it doesn't hold any relationships.
     */
    @Override
    protected List<Object> populateEntities(EntityMetadata m, Client client) {
        // TODO Auto-generated method stub
        throw new NotImplementedException("populateEntities");
        // return null;
    }

    /**
     * This method would be called by Kundera to populate entities while it holds relationships.
     */
    @Override
    protected List<Object> recursivelyPopulateEntities(EntityMetadata m, Client client) {
        // TODO Auto-generated method stub
        throw new NotImplementedException("recursivelyPopulateEntities");
        // return null;
    }

    @Override
    protected EntityReader getReader() {
        logger.info("requested entity reader");
        return new DatastoreEntityReader(kunderaQuery, kunderaMetadata);
    }

    /**
     * This method is called by Kundera when executeUpdate method is invoked on query instance that
     * represents update/ delete query. Your responsibility would be to call appropriate methods of client.
     */
    @Override
    protected int onExecuteUpdate() {
        // TODO Auto-generated method stub
        throw new NotImplementedException("onExecuteUpdate");
        // return 0;
    }

    @Override
    public void close() {
        // TODO seems no one use this
        throw new NotImplementedException("close");
    }

    @Override
    public Iterator iterate() {
        // TODO If planning to build scrolling/pagination, then have a look at ResultIterator implementation
        //return new ResultIterator(...)
        throw new NotImplementedException("iterate");
        // return null;
    }

}
