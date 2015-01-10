package it.polimi.client.datastore.config;

import com.impetus.kundera.configure.AbstractPropertyReader;
import com.impetus.kundera.configure.ClientProperties;
import com.impetus.kundera.configure.ClientProperties.DataStore;
import com.impetus.kundera.configure.PropertyReader;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

import java.util.Map;

/**
 * Reads datastore specific property from external file specified
 * through "kundera.client.property" property in persistence.xml.
 *
 * @author Fabio Arcidiacono.
 * @see com.impetus.kundera.configure.AbstractPropertyReader
 * @see com.impetus.kundera.configure.PropertyReader
 */
public class DatastorePropertyReader extends AbstractPropertyReader implements PropertyReader {

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
                    if (dataStore.getName() != null && "datastore".equalsIgnoreCase(dataStore.getName().trim())) {
                        return dataStore;
                    }
                }
            }
            return null;
        }
    }
}
