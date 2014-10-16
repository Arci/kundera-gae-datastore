package it.polimi.client.datastore.query;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.QueryImpl;
import it.polimi.client.datastore.DatastoreEntityReader;
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

     /*
     * TODO
     *
     * verificare se Client.findAll e Client.find(...., embeddedColumnMap)
     * vengono chiamate da qui o da superclasse (QueryImpl)
     *
     */

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

    @Override
    protected EntityReader getReader() {
        return new DatastoreEntityReader(kunderaQuery, kunderaMetadata);
    }

    @Override
    public void close() {
        /* do nothing, nothing to close */
    }

    /**
     * This method would be called by Kundera to populate entities while it doesn't hold any relationships.
     */
    @Override
    protected List<Object> populateEntities(EntityMetadata m, Client client) {
        System.out.println("DatastoreQuery.populateEntities");
        System.out.println("m = [" + m + "], client = [" + client + "]");
        System.out.println("kunderaQuery = [" + this.kunderaQuery + "]");

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
        System.out.println("m = [" + m + "], client = [" + client + "]");
        System.out.println("kunderaQuery = [" + this.kunderaQuery + "]");

        //return setRelationEntities(queryResults, client, m);
        // TODO Auto-generated method stub
        throw new NotImplementedException();
        // return null;
    }

    /**
     * This method is called by Kundera when executeUpdate method is invoked on query instance that
     * represents update/ delete query. Your responsibility would be to call appropriate methods of client.
     */
    /*
     * used for update and delete queries
     */
    @Override
    protected int onExecuteUpdate() {
        System.out.println("DatastoreQuery.onExecuteUpdate");
        System.out.println("kunderaQuery = [" + this.kunderaQuery + "]");

        /*
         * TODO decide
         *
         * delete all and re-insert
         * return onUpdateDeleteEvent();
         *
         * or
         * super.onDeleteOrUpdate(queryResults);
         */
        throw new NotImplementedException();
        // return 0;
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
