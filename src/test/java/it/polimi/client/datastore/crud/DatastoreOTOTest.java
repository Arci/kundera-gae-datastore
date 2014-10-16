package it.polimi.client.datastore.crud;

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import it.polimi.client.datastore.entities.EmployeeOTO;
import it.polimi.client.datastore.entities.Phone;
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
public class DatastoreOTOTest {

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
    public void testCRUD() {
        print("create");
        Phone phone = new Phone();
        phone.setNumber(123456789L);
        em.persist(phone);
        Assert.assertNotNull(phone.getId());

        EmployeeOTO employee = new EmployeeOTO();
        employee.setName("Fabio");
        employee.setSalary(123L);
        employee.setPhone(phone);
        em.persist(employee);
        Assert.assertNotNull(employee.getId());

        String empId = employee.getId();
        String phnId = phone.getId();
        clear();

        print("read");
        EmployeeOTO foundEmployee = em.find(EmployeeOTO.class, empId);
        Assert.assertNotNull(foundEmployee);
        Assert.assertNotNull(foundEmployee.getPhone());
        Assert.assertEquals(empId, foundEmployee.getId());
        Assert.assertEquals("Fabio", foundEmployee.getName());
        Assert.assertEquals((Long) 123L, foundEmployee.getSalary());
        Assert.assertEquals(phnId, foundEmployee.getPhone().getId());
        Assert.assertEquals((Long) 123456789L, foundEmployee.getPhone().getNumber());

        print("update");
        foundEmployee.setName("Pippo");
        foundEmployee.setSalary(456L);
        foundEmployee.getPhone().setNumber(987654321L);
        em.merge(foundEmployee);

        clear();

        foundEmployee = em.find(EmployeeOTO.class, empId);
        Assert.assertNotNull(foundEmployee);
        Assert.assertNotNull(foundEmployee.getPhone());
        Assert.assertEquals(empId, foundEmployee.getId());
        Assert.assertEquals("Pippo", foundEmployee.getName());
        Assert.assertEquals((Long) 456L, foundEmployee.getSalary());
        Assert.assertEquals(phnId, foundEmployee.getPhone().getId());
        Assert.assertEquals((Long) 987654321L, foundEmployee.getPhone().getNumber());

        print("delete");
        em.remove(foundEmployee);
        foundEmployee = em.find(EmployeeOTO.class, empId);
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
