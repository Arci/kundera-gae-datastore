package it.polimi.client.datastore.query;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.QueryImpl;
import it.polimi.client.datastore.DatastoreClient;
import it.polimi.client.datastore.DatastoreEntityReader;
import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.metamodel.EntityType;
import java.util.Iterator;
import java.util.List;

/**
 * @author Fabio Arcidiacono.
 *         <p>Used by Kundera to run JPA queries by invoking appropriate methods in Entity Readers.</p>
 */
public class DatastoreQuery extends QueryImpl {

    private static Logger logger = LoggerFactory.getLogger(DatastoreQuery.class);

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

    /*
     * called by Kundera to populate entities without any relationships.
     */
    @Override
    protected List<Object> populateEntities(EntityMetadata m, Client client) {
        System.out.println("DatastoreQuery.populateEntities");
        printQuery();

        QueryBuilder builder = translateQuery(this.kunderaQuery, false);
        return ((DatastoreClient) client).executeQuery(builder);
    }

    /*
     * called by Kundera to populate entities while they holds relationships.
     */
    @Override
    protected List<Object> recursivelyPopulateEntities(EntityMetadata m, Client client) {
        System.out.println("DatastoreQuery.recursivelyPopulateEntities");
        printQuery();

        QueryBuilder builder = translateQuery(this.kunderaQuery, true);
        List<Object> queryResults = ((DatastoreClient) client).executeQuery(builder);
        return setRelationEntities(queryResults, client, m);
    }

    /*
     * used for update and delete queries, called on method executeUpdate()
     */
    @Override
    protected int onExecuteUpdate() {
        System.out.println("DatastoreQuery.onExecuteUpdate");
        printQuery();

        return onUpdateDeleteEvent();
    }

    private QueryBuilder translateQuery(KunderaQuery kunderaQuery, boolean holdRelationships) {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, kunderaQuery.getEntityClass());
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                entityMetadata.getPersistenceUnit());
        EntityType entityType = metaModel.entity(entityMetadata.getEntityClazz());

        QueryBuilder builder = new QueryBuilder(entityMetadata, entityType, holdRelationships);

        builder.setFrom(kunderaQuery.getEntityClass())
                .addProjections(super.getColumns(kunderaQuery.getResult(), entityMetadata))
                .addFilters(kunderaQuery.getFilterClauseQueue())
                .addOrderings(kunderaQuery.getOrdering());
        return builder;
    }

    @Override
    public Iterator iterate() {
        System.out.println("DatastoreQuery.iterate");

        // TODO If planning to build scrolling/pagination, then have a look at ResultIterator implementation
        //return new ResultIterator(...)
        throw new NotImplementedException();
        // return null;
    }

    @Override
    protected List findUsingLucene(EntityMetadata entityMetadata, Client client) {
        throw new UnsupportedOperationException("findUsingLucene is currently unsupported for this client");
    }

    private void printQuery() {
        System.out.println("kunderaQuery = [\n\t" +
                "from = " + this.kunderaQuery.getFrom() + "\n\t" +
                "entityClass = " + this.kunderaQuery.getEntityClass() + "\n\t" +
                "entityAlias = " + this.kunderaQuery.getEntityAlias() + "\n\t" +
                "filter = " + this.kunderaQuery.getFilter() + "\n\t" +
                "ordering = " + orderingString() + "\n\t" +
                "parameters = " + this.kunderaQuery.getParameters() + "\n\t" +
                "isNative = " + this.kunderaQuery.isNative() + "\n\t" +
                "isDeleteUpdate = " + this.kunderaQuery.isDeleteUpdate() + "\n\t" +
                "getResults = " + resultString() + "\n\t" +
                "columnsToSelect = " + columnsString() + "\n\t" +
                "updateQueue = " + updateClauseQueueString() + "\n\t" +
                "filterQueue = " + this.kunderaQuery.getFilterClauseQueue() + "\n]\n");
    }

    private String resultString() {
        String results = "[";
        if (this.kunderaQuery.getResult() != null) {
            for (String res : this.kunderaQuery.getResult()) {
                results += "\n\t\t" + res;
            }
            results += "\n\t";
        }
        results += "]";
        return results;
    }

    private String columnsString() {
        String results = "[";
        if (this.kunderaQuery.getResult() != null) {
            EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, kunderaQuery.getEntityClass());
            String[] columns = super.getColumns(this.kunderaQuery.getResult(), entityMetadata);
            if (columns.length != 0) {
                for (String res : columns) {
                    results += "\n\t\t" + res;
                }
                results += "\n\t";
            } else {
                results += "*";
            }
        }
        results += "]";
        return results;
    }

    private String updateClauseQueueString() {
        String results = "[";
        Iterator<KunderaQuery.UpdateClause> clauseIterator = this.kunderaQuery.getUpdateClauseQueue().iterator();
        while (clauseIterator.hasNext()) {
            KunderaQuery.UpdateClause updateClause = clauseIterator.next();
            results += "UpdateClause [";
            results += "property=" + updateClause.getProperty();
            results += ", value=" + updateClause.getValue();
            if (clauseIterator.hasNext()) {
                results += "], ";
            } else {
                results += "]";
            }
        }
        results += "]";
        return results;
    }

    private String orderingString() {
        String results = "[";
        if (this.kunderaQuery.getOrdering() != null) {
            Iterator<KunderaQuery.SortOrdering> orderingIterator = this.kunderaQuery.getOrdering().iterator();
            while (orderingIterator.hasNext()) {
                KunderaQuery.SortOrdering sortOrdering = orderingIterator.next();
                results += "SortOrdering [";
                results += "property=" + sortOrdering.getColumnName();
                results += ", value=" + sortOrdering.getOrder();
                if (orderingIterator.hasNext()) {
                    results += "], ";
                } else {
                    results += "]";
                }
            }
            results += "]";
            return results;
        }
        return "[]";
    }
}
