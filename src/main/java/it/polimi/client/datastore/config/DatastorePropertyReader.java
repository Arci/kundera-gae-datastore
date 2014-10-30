package it.polimi.client.datastore.config;

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
    public static DatastoreSchemaMetadata dsm;

    public DatastorePropertyReader(Map externalProperties, PersistenceUnitMetadata puMetadata) {
        super(externalProperties, puMetadata);
        dsm = new DatastoreSchemaMetadata();
    }

    public void onXml(ClientProperties cp) {
        if (cp != null) {
            dsm.setClientProperties(cp);
        }
    }

    public class DatastoreSchemaMetadata {

        private ClientProperties clientProperties;

        private DatastoreSchemaMetadata() {
        }

        public ClientProperties getClientProperties() {
            return clientProperties;
        }

        private void setClientProperties(ClientProperties clientProperties) {
            this.clientProperties = clientProperties;
        }

        public DataStore getDataStore() {
            if (getClientProperties() != null && getClientProperties().getDatastores() != null) {
                for (DataStore dataStore : getClientProperties().getDatastores()) {
                    if (dataStore.getName() != null && dataStore.getName().trim().equalsIgnoreCase("datastore")) {
                        return dataStore;
                    }
                }
            }
            return null;
        }
    }
}
