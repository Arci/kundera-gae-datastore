package com.impetus.client.datastore;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.AbstractEntityReader;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.query.KunderaQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author Fabio Arcidiacono.
 *
 * Used by Kundera to translate the queries into correct client method calls.
 *
 */
public class DatastoreEntityReader extends AbstractEntityReader implements EntityReader {

    private static Logger logger = LoggerFactory.getLogger(DatastoreEntityReader.class);

    public DatastoreEntityReader(final KunderaMetadata kunderaMetadata) {
        super(kunderaMetadata);
    }

    public DatastoreEntityReader(KunderaQuery kunderaQuery, final KunderaMetadata kunderaMetadata) {
        super(kunderaMetadata);
        this.kunderaQuery = kunderaQuery;
    }

    /**
     * This is used by Query implementor to populate relationship entities into their parent entity.
     */
    @Override
    public List<EnhanceEntity> populateRelation(EntityMetadata m, Client client, int maxResults) {
        // TODO understand more deeply (all implementatios act like this)
        throw new UnsupportedOperationException("Method supported not required for Datastore");
    }

    @Override
    public EnhanceEntity findById(Object primaryKey, EntityMetadata m, Client client) {
        return super.findById(primaryKey, m, client);
    }
}
