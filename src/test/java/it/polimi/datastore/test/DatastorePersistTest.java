package it.polimi.datastore.test;

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import it.polimi.datastore.test.model.Employee;
import it.polimi.datastore.test.model.EmployeeOTO;
import it.polimi.datastore.test.model.Phone;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

/**
 * @author Fabio Arcidiacono.
 */
public class DatastorePersistTest {

    private final LocalServiceTestHelper helper =
            new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig());
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

    @Test
    public void simplePerist() {
        Employee employee = new Employee();
        employee.setName("Fabio");
        employee.setSalary(123L);
        Assert.assertNull(employee.getId());
        em.persist(employee);
        em.flush();

        Phone phone = new Phone();
        phone.setNumber(123456789L);
        Assert.assertNull(phone.getId());
        em.persist(phone);
        em.flush();
        Assert.assertNotNull(phone.getId());
    }

    @Test
    public void persistOTO() {
        EmployeeOTO employeeOTO = new EmployeeOTO();
        employeeOTO.setName("Fabio");
        employeeOTO.setSalary(123L);
        em.persist(employeeOTO);

        Phone phone = new Phone();
        phone.setNumber(123456789L);
        em.persist(phone);

        employeeOTO.setPhone(phone);
        em.flush();

        Assert.assertTrue(employeeOTO.getPhone() != null);
        Assert.assertEquals(phone.getId(), employeeOTO.getPhone().getId());
        Assert.assertEquals(phone.getNumber(), employeeOTO.getPhone().getNumber());

        String empID = employeeOTO.getId();
        em.clear();
        em.find(EmployeeOTO.class, empID);

        Assert.assertEquals(employeeOTO.getName(), "Fabio");
        Assert.assertTrue(employeeOTO.getSalary().equals(123L));
        Assert.assertTrue(employeeOTO.getPhone() != null);
        Assert.assertEquals(phone.getId(), employeeOTO.getPhone().getId());
        Assert.assertEquals(phone.getNumber(), employeeOTO.getPhone().getNumber());

    }

    @After
    public void tearDown() {
        em.close();
        emf.close();
        helper.tearDown();
    }
}
