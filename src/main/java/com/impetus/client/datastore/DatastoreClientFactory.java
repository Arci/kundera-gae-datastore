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
 *
 * Used by Kundera to instantiate the Client.
 *
 */
public class DatastoreClientFactory extends GenericClientFactory {

    private static Logger logger = LoggerFactory.getLogger(DatastoreClientFactory.class);
    private EntityReader reader;
    private SchemaManager schemaManager;
    private DatastoreService datastore;

    @Override
    public void initialize(Map<String, Object> puProperties) {
        reader = new DatastoreEntityReader(kunderaMetadata);
        initializePropertyReader();
        setExternalProperties(puProperties);
    }


    @Override
    protected Object createPoolOrConnection() {
        logger.info("Getting reference to datastore");
        datastore = DatastoreServiceFactory.getDatastoreService();
        return datastore;
    }


    @Override
    protected Client instantiateClient(String persistenceUnit) {
        logger.info("Instantiate new DatastoreClient");
        return new DatastoreClient(kunderaMetadata, externalProperties, persistenceUnit, clientMetadata,
                indexManager, reader, datastore);
    }

    @Override
    public boolean isThreadSafe() {
        return false;
    }


    @Override
    public void destroy() {
        logger.info("Destroying");
        indexManager.close();
        if (schemaManager != null) {
            schemaManager.dropSchema();
        }
        datastore = null;
        externalProperties = null;
        schemaManager = null;
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

    @Override
    protected void initializeLoadBalancer(String loadBalancingPolicyName) {
        throw new UnsupportedOperationException("Load balancing feature is not supported in "
                + this.getClass().getSimpleName());
    }
}
