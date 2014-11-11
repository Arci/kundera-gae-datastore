package it.polimi.client.datastore.config;

/**
 * Constants defining properties that can be specified in
 * datastore specific property file.
 *
 * @author Fabio Arcidiacono.
 */
public class DatastoreConstants {

    public static final int DEFAULT_PORT = 443;

    public final static String ROOT_NAME = "datastore.fictitious";

    public static final String READ_POLICY = "datastore.policy.read";
    public static final String TRANSACTION_POLICY = "datastore.policy.transaction";
    public static final String DEADLINE = "datastore.deadline";
}
