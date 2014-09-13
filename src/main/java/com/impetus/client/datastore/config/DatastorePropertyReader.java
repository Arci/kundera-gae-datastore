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

        private ClientProperties clientProperties;

        private DatastoreSchemaMetadata() {

        }

        public ClientProperties getClientProperties() {
            return clientProperties;
        }

        private void setClientProperties(ClientProperties clientProperties) {
            this.clientProperties = clientProperties;
        }

    }
}
