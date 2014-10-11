package it.polimi.client.datastore;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import it.polimi.client.datastore.config.DatastorePropertyReader;
import it.polimi.client.datastore.schemamanager.DatastoreSchemaManager;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.persistence.EntityReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author Fabio Arcidiacono.
 *         <p>Used by Kundera to instantiate the Client.</p>
 */
public class DatastoreClientFactory extends GenericClientFactory {

    private static Logger logger = LoggerFactory.getLogger(DatastoreClientFactory.class);
    private EntityReader reader;
    private SchemaManager schemaManager;
    private DatastoreService datastore;

    @Override
    public void initialize(Map<String, Object> puProperties) {
        reader = new DatastoreEntityReader(kunderaMetadata);
        initializePropertyReader(); // TODO this is needed? maybe not, is related to external properties?
        setExternalProperties(puProperties);
    }

    @Override
    protected Object createPoolOrConnection() {
        /**
         * TODO manage external properties? probably not but need for specific properties
         * see https://github.com/impetus-opensource/Kundera/wiki/Data-store-Specific-Configuration
         * and https://github.com/impetus-opensource/Kundera/wiki/Common-Configuration\
         */
        // PersistenceUnitMetadata persistenceUnitMetadata = kunderaMetadata.getApplicationMetadata()
        //        .getPersistenceUnitMetadata(getPersistenceUnit());
        // Properties props = persistenceUnitMetadata.getProperties();
        // String keyspace = null;
        // String poolSize = null;
        // if(externalProperties != null) {
        //     /* keyspace is property "kundera.keyspace" in persistence */
        //     keyspace = (String) externalProperties.get(PersistenceProperties.KUNDERA_KEYSPACE);
        //     poolSize = (String) externalProperties.get(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_ACTIVE);
        // }
        // if(keyspace == null) {
        //      keyspace = (String) props.get(PersistenceProperties.KUNDERA_KEYSPACE);
        // }
        // if (poolSize == null) {
        //     poolSize = props.getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_ACTIVE);
        // }
        //
        // DatastoreServiceConfig config = withReadPolicy(new ReadPolicy(ReadPolicy.Consistency.EVENTUAL));
        // datastore = DatastoreServiceFactory.getDatastoreService(config);
        datastore = DatastoreServiceFactory.getDatastoreService();
        return datastore;
    }

    @Override
    protected Client instantiateClient(String persistenceUnit) {
        return new DatastoreClient(kunderaMetadata, externalProperties, persistenceUnit, clientMetadata,
                indexManager, reader, datastore);
    }

    @Override
    public boolean isThreadSafe() {
        return false;
    }

    @Override
    public void destroy() {
        if (indexManager != null) {
            indexManager.close();
        }
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
            schemaManager = new DatastoreSchemaManager(this.getClass().getName(), puProperties, kunderaMetadata);
        }
        return schemaManager;
    }

    @Override
    protected void initializeLoadBalancer(String loadBalancingPolicyName) {
        throw new UnsupportedOperationException("Load balancing feature is not supported in "
                + this.getClass().getSimpleName());
    }

    private void initializePropertyReader() {
        if (propertyReader == null) {
            propertyReader = new DatastorePropertyReader(externalProperties, kunderaMetadata.getApplicationMetadata()
                    .getPersistenceUnitMetadata(getPersistenceUnit()));
            propertyReader.read(getPersistenceUnit());
        }
    }
}
