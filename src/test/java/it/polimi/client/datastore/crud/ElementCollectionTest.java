package it.polimi.client.datastore.crud;

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import it.polimi.client.datastore.entities.AddressCollection;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

/**
 * @author Fabio Arcidiacono.
 */
public class ElementCollectionTest {

    private LocalDatastoreServiceTestConfig datastoreConfig = new LocalDatastoreServiceTestConfig();
    private final LocalServiceTestHelper helper = new LocalServiceTestHelper(datastoreConfig);
    private static final String PERSISTENCE_UNIT = "pu";
    private EntityManagerFactory emf;
    private EntityManager em;

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

    @Test
    public void testElementCollection() {
        print("create");
        AddressCollection address = new AddressCollection();
        address.setStreets("Street 1", "Street 2", "Street 3");
        em.persist(address);
        Assert.assertNotNull(address.getId());

        String adrId = address.getId();
        clear();

        print("read");
        AddressCollection foundAddress = em.find(AddressCollection.class, adrId);
        Assert.assertNotNull(foundAddress);
        Assert.assertNotNull(foundAddress.getStreets());
        Assert.assertEquals(adrId, foundAddress.getId());
        Assert.assertFalse(foundAddress.getStreets().isEmpty());
        Assert.assertFalse(foundAddress.getStreets().size() > 3);
        print("access streets");
        int counter = 3;
        for (String street : foundAddress.getStreets()) {
            System.out.println(street);
            if (street.equals("Street 1") || street.equals("Street 2") || street.equals("Street 3")) {
                counter--;
            }
        }
        Assert.assertEquals(0, counter);

        print("update");
        foundAddress.setStreets("Street 4", "Street 5", "Street 6");
        em.merge(foundAddress);

        clear();

        foundAddress = em.find(AddressCollection.class, adrId);
        Assert.assertNotNull(foundAddress);
        Assert.assertNotNull(foundAddress.getStreets());
        Assert.assertEquals(adrId, foundAddress.getId());
        Assert.assertFalse(foundAddress.getStreets().isEmpty());
        Assert.assertFalse(foundAddress.getStreets().size() > 3);
        print("access streets");
        counter = 3;
        for (String street : foundAddress.getStreets()) {
            System.out.println(street);
            if (street.equals("Street 4") || street.equals("Street 5") || street.equals("Street 6")) {
                counter--;
            }
        }
        Assert.assertEquals(0, counter);

        print("delete");
        em.remove(foundAddress);
        foundAddress = em.find(AddressCollection.class, adrId);
        Assert.assertNull(foundAddress);
    }

    private void clear() {
        em.clear();
        print("clear entity manager");
    }

    private void print(String message) {
        String delimiter = "-------------------------------------------------------";
        String spacing = message.length() < 10 ? "\t\t\t\t\t\t" : message.length() < 20 ? "\t\t\t\t\t" : "\t\t\t\t";
        System.out.println(delimiter + "\n" + spacing + message.toUpperCase() + "\n" + delimiter);
    }
}
