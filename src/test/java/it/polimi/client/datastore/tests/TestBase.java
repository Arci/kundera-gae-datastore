package it.polimi.client.datastore.tests;

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

/**
 * @author Fabio Arcidiacono.
 */
public abstract class TestBase {

    /**
     * gae testing tools
     */
    private LocalDatastoreServiceTestConfig datastoreConfig = new LocalDatastoreServiceTestConfig();
    private final LocalServiceTestHelper helper = new LocalServiceTestHelper(datastoreConfig);

    /**
     * JPA stuff
     */
    private static final String PERSISTENCE_UNIT = "pu";
    private EntityManagerFactory emf;
    protected EntityManager em;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() {
        helper.setUp();
        emf = Persistence.createEntityManagerFactory(PERSISTENCE_UNIT);
        if (em != null && em.isOpen()) {
            em.close();
        }
        em = emf.createEntityManager();
    }

    @After
    public void tearDown() {
        em.close();
        emf.close();
        helper.tearDown();
    }

    /*---------------------------------------------------------------------------------*/
    /*----------------------- PRINT UTILS, for debug purposes -------------------------*/

    protected void clear() {
        em.clear();
        print("clear entity manager");
    }

    protected void print(String message) {
        String delimiter = "-------------------------------------------------------";
        String spacing = message.length() < 10 ? "\t\t\t\t\t\t" : message.length() < 20 ? "\t\t\t\t\t" : "\t\t\t\t";
        System.out.println(delimiter + "\n" + spacing + message.toUpperCase() + "\n" + delimiter);
    }
}
