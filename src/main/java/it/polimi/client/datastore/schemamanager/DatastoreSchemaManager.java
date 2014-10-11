package it.polimi.client.datastore.schemamanager;

import com.impetus.kundera.configure.schema.TableInfo;
import com.impetus.kundera.configure.schema.api.AbstractSchemaManager;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;

import java.util.List;
import java.util.Map;

/**
 * @author Fabio Arcidiacono.
 */
public class DatastoreSchemaManager extends AbstractSchemaManager implements SchemaManager {

    /**
     * Initialise with configured client factory.
     *
     * @param clientFactory      specific client factory.
     * @param externalProperties external properties
     * @param kunderaMetadata    kundera metadata
     */
    public DatastoreSchemaManager(String clientFactory, Map<String, Object> externalProperties, EntityManagerFactoryImpl.KunderaMetadata kunderaMetadata) {
        super(clientFactory, externalProperties, kunderaMetadata);
        // TODO
    }

    /**
     * Exports schema according to configured schema operation e.g.
     * {create,create-drop,update,validate}
     */
    @Override
    public void exportSchema(final String persistenceUnit, List<TableInfo> schemas) {
        // TODO Auto-generated method stub
    }

    @Override
    protected boolean initiateClient() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    protected void validate(List<TableInfo> tableInfos) {
        // TODO Auto-generated method stub
    }

    @Override
    protected void update(List<TableInfo> tableInfos) {
        // TODO Auto-generated method stub
    }

    @Override
    protected void create(List<TableInfo> tableInfos) {
        // TODO Auto-generated method stub
    }

    @Override
    protected void create_drop(List<TableInfo> tableInfos) {
        // TODO Auto-generated method stub
    }

    /**
     * Method required to drop auto create schema,in case of schema operation as
     * {create-drop},
     */
    @Override
    public void dropSchema() {
        // TODO Auto-generated method stub
    }

    @Override
    public boolean validateEntity(Class clazz) {
        // TODO Auto-generated method stub
        return false;
    }
}
