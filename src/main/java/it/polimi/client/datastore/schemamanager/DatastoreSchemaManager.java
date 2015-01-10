package it.polimi.client.datastore.schemamanager;

import com.google.appengine.api.datastore.*;
import com.google.appengine.tools.remoteapi.RemoteApiInstaller;
import com.google.appengine.tools.remoteapi.RemoteApiOptions;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.configure.schema.TableInfo;
import com.impetus.kundera.configure.schema.api.AbstractSchemaManager;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.loader.ClientLoaderException;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Provide support for automatic schema generation through "kundera_ddl_auto_prepare" property in persistence.xml.
 *
 * @author Fabio Arcidiacono.
 * @see com.impetus.kundera.configure.schema.api.AbstractSchemaManager
 * @see com.impetus.kundera.configure.schema.api.SchemaManager
 */
public class DatastoreSchemaManager extends AbstractSchemaManager implements SchemaManager {

    private static final Logger logger = LoggerFactory.getLogger(DatastoreSchemaManager.class);
    private RemoteApiInstaller installer;
    private DatastoreService datastore;

    public DatastoreSchemaManager(String clientFactory, Map<String, Object> externalProperties, EntityManagerFactoryImpl.KunderaMetadata kunderaMetadata) {
        super(clientFactory, externalProperties, kunderaMetadata);
    }

    /*
     * Need re-implementation because AbstractSchemaManager.exportSchema()
     * do unsafe split on hostName so if it is null a NullPointerException is thrown and
     * no DDL can be done over local Datastore instance
     */
    @Override
    public void exportSchema(final String persistenceUnit, List<TableInfo> schemas) {
        this.puMetadata = kunderaMetadata.getApplicationMetadata().getPersistenceUnitMetadata(persistenceUnit);
        String hostName = null;
        if (externalProperties != null) {
            hostName = (String) externalProperties.get(PersistenceProperties.KUNDERA_NODES);
            this.port = (String) externalProperties.get(PersistenceProperties.KUNDERA_PORT);
            this.userName = (String) externalProperties.get(PersistenceProperties.KUNDERA_USERNAME);
            this.password = (String) externalProperties.get(PersistenceProperties.KUNDERA_PASSWORD);
            this.operation = (String) externalProperties.get(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE);
        }
        if (hostName == null) {
            hostName = this.puMetadata.getProperty(PersistenceProperties.KUNDERA_NODES);
        }
        if (this.port == null) {
            this.port = this.puMetadata.getProperty(PersistenceProperties.KUNDERA_PORT);
        }
        if (this.userName == null) {
            this.userName = this.puMetadata.getProperty(PersistenceProperties.KUNDERA_USERNAME);
        }
        if (this.password == null) {
            this.password = this.puMetadata.getProperty(PersistenceProperties.KUNDERA_PASSWORD);
        }
        if (this.operation == null) {
            this.operation = this.puMetadata.getProperty(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE);
        }
        if (hostName != null) {
            this.hosts = new String[]{hostName.trim()};
        }
        if (this.operation != null && initiateClient()) {
            this.tableInfos = schemas;
            handleOperations(schemas);
        }
    }

    /*
     * same as super.handleOperations() but cannot use since is private
     */
    private void handleOperations(List<TableInfo> tablesInfo) {
        SchemaOperationType operationType = SchemaOperationType.getInstance(operation);

        switch (operationType) {
            case createdrop:
                create_drop(tablesInfo);
                break;
            case create:
                create(tablesInfo);
                break;
            case update:
                update(tablesInfo);
                break;
            case validate:
                validate(tablesInfo);
                break;
        }
    }

    @Override
    protected boolean initiateClient() {
        if (hosts != null && userName != null && password != null) {
            try {
                RemoteApiOptions options = new RemoteApiOptions()
                        .server(hosts[0], Integer.valueOf(port))
                        .credentials(userName, password);
                this.installer = new RemoteApiInstaller();
                this.installer.install(options);
                logger.info("Connected to Datastore at " + hosts[0] + ":" + port);
            } catch (IOException e) {
                throw new ClientLoaderException("Unable to connect to Datastore at " + hosts[0] + ":" + port + ": ", e);
            }
        } else {
            logger.info("Get reference from local Datastore");
        }
        this.datastore = DatastoreServiceFactory.getDatastoreService();
        return true;
    }

    /*
     * validates schema tables based on entity definition. Throws SchemaGenerationException if validation fails.
     */
    @Override
    protected void validate(List<TableInfo> tablesInfo) {
        /*
         * cannot validate since query over not existent Kinds will return empty,
         * does not throws exceptions.
         */
        throw new UnsupportedOperationException("DDL validate is unsupported for Datastore");
    }

    /*
     * updates schema tables based on entity definition.
     */
    @Override
    protected void update(List<TableInfo> tableInfo) {
        throw new UnsupportedOperationException("DDL update is unsupported for Datastore");
    }

    /*
     * drops (if exists) schema and then creates schema tables based on entity definitions.
     */
    @Override
    protected void create(List<TableInfo> tableInfo) {
        /* no need to create schema, tables are created when first entity is persisted */
        dropSchema();
        uninstall();
    }

    /*
     * drops (if exists) schema, creates schema tables based on entity definitions.
     */
    @Override
    protected void create_drop(List<TableInfo> tableInfo) {
        create(tableInfo);
        uninstall();
    }

    /*
     * Method required to drop auto create schema, in case
     * of schema operation as {create-drop}.
     */
    @Override
    public void dropSchema() {
        Query query = new Query().setKeysOnly();
        for (Entity entity : datastore.prepare(query).asList(FetchOptions.Builder.withDefaults())) {
            logger.debug("\tdrop kind= [" + entity.getKind() + "], key =[" + entity.getKey() + "]");
            datastore.delete(entity.getKey());
        }
        logger.info("Schema dropped");
    }

    @Override
    public boolean validateEntity(Class clazz) {
        return true;
    }

    private void uninstall() {
        if (installer != null) {
            installer.uninstall();
        }
    }
}
