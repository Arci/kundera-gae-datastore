package com.impetus.client.datastore.config;

import com.impetus.kundera.configure.AbstractPropertyReader;
import com.impetus.kundera.configure.ClientProperties;
import com.impetus.kundera.configure.ClientProperties.DataStore;
import com.impetus.kundera.configure.PropertyReader;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author Fabio Arcidiacono.
 */
public class DatastorePropertyReader extends AbstractPropertyReader implements PropertyReader {

    private static Logger logger = LoggerFactory.getLogger(DatastorePropertyReader.class);
    private final DatastoreSchemaMetadata dsmd;

    public DatastorePropertyReader(Map externalProperties, PersistenceUnitMetadata puMetadata) {
        super(externalProperties, puMetadata);
        dsmd = new DatastoreSchemaMetadata();
    }

    public void onXml(ClientProperties cp) {
        if (cp != null) {
            dsmd.setClientProperties(cp);
        }
    }

    private class DatastoreSchemaMetadata {

        // TODO verify this name (comes from persistence.xml ?)
        private static final String GAE_DATASTORE = "datastore";
        private ClientProperties clientProperties;

        private DatastoreSchemaMetadata() {

        }

        public ClientProperties getClientProperties() {
            return clientProperties;
        }

        private void setClientProperties(ClientProperties clientProperties) {
            this.clientProperties = clientProperties;
        }

        // TODO is this needed?
        public DataStore getDataStore() {
            if (getClientProperties() != null && getClientProperties().getDatastores() != null) {
                for (DataStore dataStore : getClientProperties().getDatastores()) {
                    if (dataStore.getName() != null && dataStore.getName().trim().equalsIgnoreCase(GAE_DATASTORE)) {
                        return dataStore;
                    }
                }
            }
            return null;
        }
    }
}
