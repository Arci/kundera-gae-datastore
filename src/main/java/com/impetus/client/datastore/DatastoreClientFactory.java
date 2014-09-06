package com.impetus.client.datastore;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.impetus.client.datastore.config.DatastorePropertyReader;
import com.impetus.client.datastore.schemamanager.DatastoreSchemaManager;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.persistence.EntityReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author Fabio Arcidiacono.
 */
public class DatastoreClientFactory extends GenericClientFactory {

    private static Logger logger = LoggerFactory.getLogger(DatastoreClientFactory.class);
    private EntityReader reader;
    private SchemaManager schemaManager;
    private DatastoreService datastore;

    /**
     * This is called by Kundera when you create entity manager factory.
     * Your responsibility is to initialize entity reader, schema manager and any other instance variable
     * this class might hold.
     */
    @Override
    public void initialize(Map<String, Object> puProperties) {
        reader = new DatastoreEntityReader(kunderaMetadata);
        initializePropertyReader();
        setExternalProperties(puProperties);
    }

    /**
     * This method is called after initialize by Kundera and your responsibility would be to create
     * a pool (or connection) provided by your Java driver and return it.
     */
    @Override
    protected Object createPoolOrConnection() {
        logger.info("Getting reference to datastore");
        datastore = DatastoreServiceFactory.getDatastoreService();
        return datastore;
    }

    /**
     * Kundera calls this method to get instance of client. You should initialize client and return it here.
     */
    @Override
    protected Client instantiateClient(String persistenceUnit) {
        return new DatastoreClient(kunderaMetadata, externalProperties, persistenceUnit, clientMetadata,
                indexManager, reader, datastore);
    }

    /**
     * It would return true or false depending upon how you want your client to be.
     */
    @Override
    public boolean isThreadSafe() {
        return false;
    }

    /**
     * Finally, when entity manager factory is closed, Kundera calls this method to free up resources
     * (e.g. closing connection pool)
     */
    @Override
    public void destroy() {
        indexManager.close();
        if (schemaManager != null) {
            schemaManager.dropSchema();
        }
        datastore = null;
        externalProperties = null;
        schemaManager = null;
    }

    @Override
    protected void initializeLoadBalancer(String loadBalancingPolicyName) {
        throw new UnsupportedOperationException("Load balancing feature is not supported in "
                + this.getClass().getSimpleName());
    }

    @Override
    public SchemaManager getSchemaManager(Map<String, Object> puProperties) {
        if (schemaManager == null) {
            initializePropertyReader();
            setExternalProperties(puProperties);
            schemaManager = new DatastoreSchemaManager(DatastoreClientFactory.class.getName(), puProperties, kunderaMetadata);
        }
        return schemaManager;
    }

    private void initializePropertyReader() {
        if (propertyReader == null) {
            propertyReader = new DatastorePropertyReader(externalProperties, kunderaMetadata.getApplicationMetadata()
                    .getPersistenceUnitMetadata(getPersistenceUnit()));
            propertyReader.read(getPersistenceUnit());
        }
    }
}
