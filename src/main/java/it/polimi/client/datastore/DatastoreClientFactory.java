package it.polimi.client.datastore;

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
import it.polimi.client.datastore.config.DatastoreConstants;
import it.polimi.client.datastore.config.DatastorePropertyReader;
import it.polimi.client.datastore.config.DatastorePropertyReader.DatastoreSchemaMetadata;
import it.polimi.client.datastore.schemamanager.DatastoreSchemaManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        PersistenceUnitMetadata persistenceUnitMetadata = kunderaMetadata.getApplicationMetadata()
                .getPersistenceUnitMetadata(getPersistenceUnit());
        Properties properties = persistenceUnitMetadata.getProperties();
        // String keyspace = null;
        String nodes = null;
        String port = null;
        String username = null;
        String password = null;
        if (externalProperties != null) {
            // keyspace = (String) externalProperties.get(PersistenceProperties.KUNDERA_KEYSPACE);
            nodes = (String) externalProperties.get(PersistenceProperties.KUNDERA_NODES);
            port = (String) externalProperties.get(PersistenceProperties.KUNDERA_PORT);
            username = (String) externalProperties.get(PersistenceProperties.KUNDERA_USERNAME);
            password = (String) externalProperties.get(PersistenceProperties.KUNDERA_PASSWORD);
        }
        // if (keyspace == null) {
        //     keyspace = (String) props.get(PersistenceProperties.KUNDERA_KEYSPACE);
        // }
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
        if (installer != null) {
            System.out.println("\nUninstall remote API connection\n");
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

    private void initializeConnection(String nodes, String port, String username, String password) {
        System.out.println("Trying to connect using remote API");
        int connection_port = DatastoreConstants.DEFAULT_PORT;
        if (port != null) {
            try {
                connection_port = Integer.parseInt(port);
            } catch (NumberFormatException nfe) {
                throw new ClientLoaderException("Invalid port [" + port + "]");
            }
        }
        try {
            RemoteApiOptions options = new RemoteApiOptions()
                    .server(nodes, connection_port)
                    .credentials(username, password);
            this.installer = new RemoteApiInstaller();
            this.installer.install(options);
            System.out.println("Connected to Datastore at " + nodes + ":" + connection_port);
        } catch (Exception e) {
            System.out.println("Unable to connect to Datastore at " + nodes + ":" + connection_port + "; Caused by:" + e.getMessage());
            throw new ClientLoaderException(e);
        }
    }

    private DatastoreServiceConfig buildConfiguration() {
        Properties properties = getClientSpecificProperties();
        DatastoreServiceConfig config = DatastoreServiceConfig.Builder.withDefaults();
        if (properties != null) {
            System.out.println("Initialize datastore with:");
            ReadPolicy readPolicy = parseReadPolicy(properties);
            Double deadline = ReadDeadline(properties);
            ImplicitTransactionManagementPolicy transactionPolicy = parseTransactionPolicy(properties);
            try {
                if (readPolicy != null) {
                    config = DatastoreServiceConfig.Builder.withReadPolicy(readPolicy);
                    System.out.println("\tread policy [" + readPolicy.getConsistency() + "]");
                    if (deadline != null) {
                        config.deadline(deadline);
                        System.out.println("\tdeadline [" + deadline + "]");
                    }
                    if (transactionPolicy != null) {
                        config.implicitTransactionManagementPolicy(transactionPolicy);
                        System.out.println("\ttransaction policy [" + transactionPolicy.name() + "]");
                    }
                } else if (deadline != null) {
                    config = DatastoreServiceConfig.Builder.withDeadline(deadline);
                    System.out.println("\tdeadline [" + deadline + "]");
                    if (transactionPolicy != null) {
                        config.implicitTransactionManagementPolicy(transactionPolicy);
                        System.out.println("\ttransaction policy [" + transactionPolicy.name() + "]");
                    }
                } else if (transactionPolicy != null) {
                    config = DatastoreServiceConfig.Builder.withImplicitTransactionManagementPolicy(transactionPolicy);
                    System.out.println("\ttransaction policy [" + transactionPolicy.name() + "]");
                }
            } catch (Exception e) {
                System.out.println("Some error occurred creating Datastore configuration; Caused by:" + e.getMessage());
            }
        }
        return config;
    }

    private Double ReadDeadline(Properties properties) {
        String deadline = (String) properties.get(DatastoreConstants.DEADLINE);
        if (deadline != null && !deadline.isEmpty()) {
            try {
                return Double.parseDouble(deadline);
            } catch (NumberFormatException nfe) {
                throw new ClientLoaderException("invalid read deadline [" + deadline + "]");
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
            } catch (Exception e) {
                throw new ClientLoaderException("Invalid read policy [" + readPolicy + "]");
            }
        }
        return null;
    }

    private ImplicitTransactionManagementPolicy parseTransactionPolicy(Properties properties) {
        String transactionPolicy = (String) properties.get(DatastoreConstants.TRANSACTION_POLICY);
        if (transactionPolicy != null && !transactionPolicy.isEmpty()) {
            try {
                return ImplicitTransactionManagementPolicy.valueOf(transactionPolicy.toUpperCase());
            } catch (Exception e) {
                throw new ClientLoaderException("Invalid transaction policy [" + transactionPolicy + "]");
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
