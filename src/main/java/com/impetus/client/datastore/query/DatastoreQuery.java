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
     * @param kunderaQuery         the kundera query object
     * @param persistenceDelegator the persistence delegator
     * @param kunderaMetadata      kundera metadata
     */
    public DatastoreQuery(KunderaQuery kunderaQuery, PersistenceDelegator persistenceDelegator, EntityManagerFactoryImpl.KunderaMetadata kunderaMetadata) {
        super(kunderaQuery, persistenceDelegator, kunderaMetadata);
    }

    /**
     * This method would be called by Kundera to populate entities while it doesn't hold any relationships.
     */
    @Override
    protected List<Object> populateEntities(EntityMetadata m, Client client) {
        System.out.println("DatastoreQuery.populateEntities");
        // TODO Auto-generated method stub
        throw new NotImplementedException();
        // return null;
    }

    /**
     * This method would be called by Kundera to populate entities while it holds relationships.
     */
    @Override
    protected List<Object> recursivelyPopulateEntities(EntityMetadata m, Client client) {
        System.out.println("DatastoreQuery.recursivelyPopulateEntities");
        // TODO Auto-generated method stub
        throw new NotImplementedException();
        // return null;
    }

    @Override
    protected EntityReader getReader() {
        System.out.println("DatastoreQuery.getReader");
        return new DatastoreEntityReader(kunderaQuery, kunderaMetadata);
    }

    /**
     * This method is called by Kundera when executeUpdate method is invoked on query instance that
     * represents update/ delete query. Your responsibility would be to call appropriate methods of client.
     */
    @Override
    protected int onExecuteUpdate() {
        System.out.println("DatastoreQuery.onExecuteUpdate");
        // TODO Auto-generated method stub
        throw new NotImplementedException();
        // return 0;
    }

    @Override
    public void close() {
        System.out.println("DatastoreQuery.close");
        // TODO seems no one use this
    }

    @Override
    public Iterator iterate() {
        System.out.println("DatastoreQuery.iterate");
        // TODO If planning to build scrolling/pagination, then have a look at ResultIterator implementation
        //return new ResultIterator(...)
        throw new NotImplementedException();
        // return null;
    }
}
