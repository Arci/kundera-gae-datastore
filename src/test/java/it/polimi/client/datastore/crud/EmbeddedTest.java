package it.polimi.client.datastore.crud;

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import it.polimi.client.datastore.entities.Address;
import it.polimi.client.datastore.entities.EmployeeEmbedded;
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
public class EmbeddedTest {

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
    public void testEmbedded() {
        print("create");
        EmployeeEmbedded employee = new EmployeeEmbedded();
        employee.setName("Fabio");
        employee.setSalary(123L);
        Address address = new Address("Via Cadore 12");
        employee.setAddress(address);
        em.persist(employee);
        Assert.assertNotNull(employee.getId());

        String empId = employee.getId();
        clear();

        print("read");
        EmployeeEmbedded foundEmployee = em.find(EmployeeEmbedded.class, empId);
        Assert.assertNotNull(foundEmployee);
        Assert.assertNotNull(foundEmployee.getAddress());
        Assert.assertEquals(empId, foundEmployee.getId());
        Assert.assertEquals("Fabio", foundEmployee.getName());
        Assert.assertEquals((Long) 123L, foundEmployee.getSalary());
        Assert.assertTrue(foundEmployee.getAddress() instanceof Address);
        Assert.assertEquals("Via Cadore 12", foundEmployee.getAddress().getStreet());

        print("update");
        foundEmployee.getAddress().setStreet("Piazza Leonardo Da Vinci 32");
        em.merge(foundEmployee);

        clear();

        foundEmployee = em.find(EmployeeEmbedded.class, empId);
        Assert.assertNotNull(foundEmployee);
        Assert.assertNotNull(foundEmployee.getAddress());
        Assert.assertEquals(empId, foundEmployee.getId());
        Assert.assertEquals("Fabio", foundEmployee.getName());
        Assert.assertEquals((Long) 123L, foundEmployee.getSalary());
        Assert.assertTrue(foundEmployee.getAddress() instanceof Address);
        Assert.assertEquals("Piazza Leonardo Da Vinci 32", foundEmployee.getAddress().getStreet());

        print("delete");
        em.remove(foundEmployee);
        foundEmployee = em.find(EmployeeEmbedded.class, empId);
        Assert.assertNull(foundEmployee);
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
