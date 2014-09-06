package com.impetus.client.datastore;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.AbstractEntityReader;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author Fabio Arcidiacono.
 */
public class DatastoreEntityReader extends AbstractEntityReader implements EntityReader {

    private static Logger logger = LoggerFactory.getLogger(DatastoreEntityReader.class);

    public DatastoreEntityReader(final KunderaMetadata kunderaMetadata) {
        super(kunderaMetadata);
        // TODO
    }

    /**
     * This is used by Query implementor to populate relationship entities into their parent entity.
     */
    @Override
    public List<EnhanceEntity> populateRelation(EntityMetadata m, Client client, int maxResults) {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * This is used by Query implementor to find entities by their ID.
     */
    @Override
    public EnhanceEntity findById(Object primaryKey, EntityMetadata m, Client client) {
        // TODO Auto-generated method stub
        return null;
    }
}
