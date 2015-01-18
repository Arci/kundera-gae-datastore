package it.polimi.kundera.client.datastore;

import com.google.appengine.api.datastore.*;
import com.google.appengine.tools.remoteapi.RemoteApiInstaller;
import com.google.appengine.tools.remoteapi.RemoteApiOptions;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.configure.ClientProperties;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.loader.ClientLoaderException;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.persistence.EntityReader;
import it.polimi.kundera.client.datastore.config.DatastoreConstants;
import it.polimi.kundera.client.datastore.config.DatastorePropertyReader;
import it.polimi.kundera.client.datastore.config.DatastorePropertyReader.DatastoreSchemaMetadata;
import it.polimi.kundera.client.datastore.schemamanager.DatastoreSchemaManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Used by Kundera to instantiate the Client.
 *
 * @author Fabio Arcidiacono.
 * @see com.impetus.kundera.loader.GenericClientFactory
 */
public class DatastoreClientFactory extends GenericClientFactory {

    private static Logger logger = LoggerFactory.getLogger(DatastoreClientFactory.class);
    private EntityReader reader;
    private SchemaManager schemaManager;
    private RemoteApiInstaller installer;
    private DatastoreService datastore;

    @Override
    public void initialize(Map<String, Object> puProperties) {
        installer = null;
        datastore = null;
        reader = new DatastoreEntityReader(kunderaMetadata);
        initializePropertyReader();
        setExternalProperties(puProperties);
    }

    @Override
    protected Object createPoolOrConnection() {
        String pu = getPersistenceUnit();
        PersistenceUnitMetadata puMetadata = kunderaMetadata.getApplicationMetadata().getPersistenceUnitMetadata(pu);
        Properties properties = puMetadata.getProperties();
        String nodes = null;
        String port = null;
        String username = null;
        String password = null;
        if (externalProperties != null) {
            nodes = (String) externalProperties.get(PersistenceProperties.KUNDERA_NODES);
            port = (String) externalProperties.get(PersistenceProperties.KUNDERA_PORT);
            username = (String) externalProperties.get(PersistenceProperties.KUNDERA_USERNAME);
            password = (String) externalProperties.get(PersistenceProperties.KUNDERA_PASSWORD);
        }
        if (nodes == null) {
            nodes = (String) properties.get(PersistenceProperties.KUNDERA_NODES);
        }
        if (port == null) {
            port = (String) properties.get(PersistenceProperties.KUNDERA_PORT);
        }
        if (username == null) {
            username = (String) properties.get(PersistenceProperties.KUNDERA_USERNAME);
        }
        if (password == null) {
            password = (String) properties.get(PersistenceProperties.KUNDERA_PASSWORD);
        }

        if (nodes != null && username != null && password != null) {
            initializeConnection(nodes, port, username, password);
        }
        DatastoreServiceConfig config = buildConfiguration();
        datastore = DatastoreServiceFactory.getDatastoreService(config);

        return datastore;
    }

    @Override
    protected Client instantiateClient(String persistenceUnit) {
        return new DatastoreClient(kunderaMetadata, externalProperties, persistenceUnit, clientMetadata, indexManager, reader, datastore);
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
        if (installer != null) {
            logger.debug("Uninstall remote API connection");
            installer.uninstall();
        }
        datastore = null;
        schemaManager = null;
        externalProperties = null;
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
        throw new UnsupportedOperationException("Load balancing feature is not supported in " + this.getClass().getSimpleName());
    }

    private void initializePropertyReader() {
        if (propertyReader == null) {
            propertyReader = new DatastorePropertyReader(externalProperties,
                    kunderaMetadata.getApplicationMetadata().getPersistenceUnitMetadata(getPersistenceUnit()));
            propertyReader.read(getPersistenceUnit());
        }
    }

    private void initializeConnection(String nodes, String port, String username, String password) {
        logger.debug("Trying to connect using remote API");
        int connectionPort = DatastoreConstants.DEFAULT_PORT;
        if (port != null) {
            try {
                connectionPort = Integer.parseInt(port);
            } catch (NumberFormatException e) {
                throw new ClientLoaderException("Invalid port " + port + ": ", e);
            }
        }
        try {
            RemoteApiOptions options = new RemoteApiOptions().server(nodes, connectionPort).credentials(username, password);
            this.installer = new RemoteApiInstaller();
            this.installer.install(options);
            logger.info("Connected to Datastore at " + nodes + ":" + connectionPort);
        } catch (IOException e) {
            throw new ClientLoaderException("Unable to connect to Datastore at " + nodes + ":" + connectionPort + ": ", e);
        }
    }

    private DatastoreServiceConfig buildConfiguration() {
        Properties properties = getClientSpecificProperties();
        DatastoreServiceConfig config = DatastoreServiceConfig.Builder.withDefaults();
        if (properties == null) {
            return config;
        }

        logger.info("Initialize datastore with:");
        ReadPolicy readPolicy = parseReadPolicy(properties);
        Double deadline = parseDeadline(properties);
        ImplicitTransactionManagementPolicy transactionPolicy = parseTransactionPolicy(properties);
        try {
            if (readPolicy != null) {
                logger.info("\tread policy [" + readPolicy.getConsistency() + "]");
                config.readPolicy(readPolicy);
            }
            if (deadline != null) {
                logger.info("\tdeadline [" + deadline + "]");
                config.deadline(deadline);
            }
            if (transactionPolicy != null) {
                logger.info("\ttransaction policy [" + transactionPolicy.name() + "]");
                config.implicitTransactionManagementPolicy(transactionPolicy);
            }
        } catch (IllegalArgumentException e) {
            throw new ClientLoaderException("Some error occurred creating Datastore configuration: ", e);
        }
        return config;
    }

    private Double parseDeadline(Properties properties) {
        String deadline = (String) properties.get(DatastoreConstants.DEADLINE);
        if (deadline != null && !deadline.isEmpty()) {
            try {
                return Double.parseDouble(deadline);
            } catch (NumberFormatException e) {
                throw new ClientLoaderException("Invalid read deadline " + deadline + ": ", e);
            }
        }
        return null;
    }

    private ReadPolicy parseReadPolicy(Properties properties) {
        String readPolicy = (String) properties.get(DatastoreConstants.READ_POLICY);
        if (readPolicy != null && !readPolicy.isEmpty()) {
            try {
                ReadPolicy.Consistency consistencyType = ReadPolicy.Consistency.valueOf(readPolicy.toUpperCase());
                return new ReadPolicy(consistencyType);
            } catch (IllegalArgumentException | NullPointerException e) {
                throw new ClientLoaderException("Invalid read policy " + readPolicy + ": ", e);
            }
        }
        return null;
    }

    private ImplicitTransactionManagementPolicy parseTransactionPolicy(Properties properties) {
        String transactionPolicy = (String) properties.get(DatastoreConstants.TRANSACTION_POLICY);
        if (transactionPolicy != null && !transactionPolicy.isEmpty()) {
            try {
                return ImplicitTransactionManagementPolicy.valueOf(transactionPolicy.toUpperCase());
            } catch (IllegalArgumentException | NullPointerException e) {
                throw new ClientLoaderException("Invalid transaction policy " + transactionPolicy + ": ", e);
            }
        }
        return null;
    }

    private Properties getClientSpecificProperties() {
        DatastoreSchemaMetadata metadata = DatastorePropertyReader.dsm;
        ClientProperties clientProperties = metadata != null ? metadata.getClientProperties() : null;
        if (clientProperties != null) {
            ClientProperties.DataStore dataStore = metadata.getDataStore();
            if (dataStore != null && dataStore.getConnection() != null) {
                return dataStore.getConnection().getProperties();
            }
        }
        return null;
    }
}
