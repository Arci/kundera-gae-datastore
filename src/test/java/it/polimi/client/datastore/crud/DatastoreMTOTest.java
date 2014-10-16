package it.polimi.client.datastore.crud;

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import it.polimi.client.datastore.entities.Department;
import it.polimi.client.datastore.entities.EmployeeMTO;
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
public class DatastoreMTOTest {

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
        Department department = new Department();
        department.setName("Computer Science");
        em.persist(department);
        Assert.assertNotNull(department.getId());

        EmployeeMTO employee1 = new EmployeeMTO();
        employee1.setName("Fabio");
        employee1.setSalary(123L);
        employee1.setDepartment(department);
        em.persist(employee1);
        Assert.assertNotNull(employee1.getId());

        EmployeeMTO employee2 = new EmployeeMTO();
        employee2.setName("Crizia");
        employee2.setSalary(456L);
        employee2.setDepartment(department);
        em.persist(employee2);
        Assert.assertNotNull(employee2.getId());

        String emp1Id = employee1.getId();
        String emp2Id = employee2.getId();
        String depId = department.getId();
        clear();

        print("read");
        print("employee 1");
        EmployeeMTO foundEmployee1 = em.find(EmployeeMTO.class, emp1Id);
        Assert.assertNotNull(foundEmployee1);
        Assert.assertNotNull(foundEmployee1.getDepartment());
        Assert.assertEquals(emp1Id, foundEmployee1.getId());
        Assert.assertEquals("Fabio", foundEmployee1.getName());
        Assert.assertEquals((Long) 123L, foundEmployee1.getSalary());
        Assert.assertEquals(depId, foundEmployee1.getDepartment().getId());
        Assert.assertEquals("Computer Science", foundEmployee1.getDepartment().getName());

        print("employee 2");
        EmployeeMTO foundEmployee2 = em.find(EmployeeMTO.class, emp2Id);
        Assert.assertNotNull(foundEmployee2);
        Assert.assertNotNull(foundEmployee2.getDepartment());
        Assert.assertEquals(emp2Id, foundEmployee2.getId());
        Assert.assertEquals("Crizia", foundEmployee2.getName());
        Assert.assertEquals((Long) 456L, foundEmployee2.getSalary());
        Assert.assertEquals(depId, foundEmployee2.getDepartment().getId());
        Assert.assertEquals("Computer Science", foundEmployee2.getDepartment().getName());

        print("update");
        foundEmployee1.setName("Pippo");
        foundEmployee1.setSalary(456L);
        foundEmployee2.setName("Minnie");
        foundEmployee2.setSalary(789L);
        em.merge(foundEmployee1);
        em.merge(foundEmployee2);

        clear();

        print("employee 1");
        foundEmployee1 = em.find(EmployeeMTO.class, emp1Id);
        Assert.assertNotNull(foundEmployee1);
        Assert.assertNotNull(foundEmployee1.getDepartment());
        Assert.assertEquals(emp1Id, foundEmployee1.getId());
        Assert.assertEquals("Pippo", foundEmployee1.getName());
        Assert.assertEquals((Long) 456L, foundEmployee1.getSalary());
        Assert.assertEquals(depId, foundEmployee1.getDepartment().getId());
        Assert.assertEquals("Computer Science", foundEmployee1.getDepartment().getName());

        print("employee 2");
        foundEmployee2 = em.find(EmployeeMTO.class, emp2Id);
        Assert.assertNotNull(foundEmployee2);
        Assert.assertNotNull(foundEmployee2.getDepartment());
        Assert.assertEquals(emp2Id, foundEmployee2.getId());
        Assert.assertEquals("Minnie", foundEmployee2.getName());
        Assert.assertEquals((Long) 789L, foundEmployee2.getSalary());
        Assert.assertEquals(depId, foundEmployee2.getDepartment().getId());
        Assert.assertEquals("Computer Science", foundEmployee2.getDepartment().getName());

        print("delete");
        em.remove(foundEmployee1);
        em.remove(foundEmployee2);
        foundEmployee1 = em.find(EmployeeMTO.class, emp1Id);
        foundEmployee2 = em.find(EmployeeMTO.class, emp2Id);
        Assert.assertNull(foundEmployee1);
        Assert.assertNull(foundEmployee2);
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
