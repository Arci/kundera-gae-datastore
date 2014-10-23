package it.polimi.client.datastore.crud;

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import it.polimi.client.datastore.entities.PhoneEnum;
import it.polimi.client.datastore.entities.PhoneType;
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
public class EnumeratedTest {

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
    public void testEnum() {
        print("create");
        PhoneEnum phone = new PhoneEnum();
        phone.setNumber(123L);
        phone.setType(PhoneType.HOME);
        em.persist(phone);
        Assert.assertNotNull(phone.getId());

        String phnId = phone.getId();
        clear();

        print("read");
        PhoneEnum foundPhone = em.find(PhoneEnum.class, phnId);
        Assert.assertNotNull(foundPhone);
        Assert.assertEquals(phnId, foundPhone.getId());
        Assert.assertEquals((Long) 123L, foundPhone.getNumber());
        Assert.assertEquals(PhoneType.HOME, foundPhone.getType());

        print("update");
        foundPhone.setType(PhoneType.MOBILE);
        em.merge(foundPhone);

        clear();

        foundPhone = em.find(PhoneEnum.class, phnId);
        Assert.assertNotNull(foundPhone);
        Assert.assertEquals(phnId, foundPhone.getId());
        Assert.assertEquals((Long) 123L, foundPhone.getNumber());
        Assert.assertEquals(PhoneType.MOBILE, foundPhone.getType());

        print("delete");
        em.remove(foundPhone);
        foundPhone = em.find(PhoneEnum.class, phnId);
        Assert.assertNull(foundPhone);
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
