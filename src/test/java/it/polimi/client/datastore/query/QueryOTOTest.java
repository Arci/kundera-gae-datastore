package it.polimi.client.datastore.query;

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
import javax.persistence.Query;
import java.util.List;

/**
 * @author Fabio Arcidiacono.
 */
public class QueryOTOTest {

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
    public void testQuery() {
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

        print("select all");
        Query query = em.createQuery("SELECT e FROM EmployeeOTO e");
        List<EmployeeOTO> allEmployees = query.getResultList();
        int toCheck = 1;
        for (EmployeeOTO emp : allEmployees) {
            Assert.assertNotNull(emp.getId());
            Assert.assertNotNull(emp.getPhone());
            if (emp.getId().equals(empId)) {
                toCheck--;
                Assert.assertEquals(empId, emp.getId());
                Assert.assertEquals("Fabio", emp.getName());
                Assert.assertEquals((Long) 123L, emp.getSalary());
                Assert.assertEquals(phnId, emp.getPhone().getId());
                Assert.assertEquals((Long) 123456789L, emp.getPhone().getNumber());
            }
        }
        Assert.assertEquals(0, toCheck);

        clear();

        print("select by inner filed");
        query = em.createQuery("SELECT e FROM EmployeeOTO e WHERE e.phone = :pid");
        EmployeeOTO foundEmployee = (EmployeeOTO) query.setParameter("pid", phnId).getSingleResult();
        Assert.assertNotNull(foundEmployee.getId());
        Assert.assertNotNull(foundEmployee.getPhone());
        Assert.assertEquals(empId, foundEmployee.getId());
        Assert.assertEquals("Fabio", foundEmployee.getName());
        Assert.assertEquals((Long) 123L, foundEmployee.getSalary());
        Assert.assertEquals(phnId, foundEmployee.getPhone().getId());
        Assert.assertEquals((Long) 123456789L, foundEmployee.getPhone().getNumber());
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
