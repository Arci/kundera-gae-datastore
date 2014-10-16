package it.polimi.client.datastore.crud;

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import it.polimi.client.datastore.entities.DepartmentOTM;
import it.polimi.client.datastore.entities.EmployeeMTObis;
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
public class DatastoreOTMTest {

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
        DepartmentOTM department = new DepartmentOTM();
        department.setName("Computer Science");
        em.persist(department);
        Assert.assertNotNull(department.getId());

        EmployeeMTObis employee1 = new EmployeeMTObis();
        employee1.setName("Fabio");
        employee1.setSalary(123L);
        employee1.setDepartment(department);
        em.persist(employee1);
        Assert.assertNotNull(employee1.getId());

        EmployeeMTObis employee2 = new EmployeeMTObis();
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
        DepartmentOTM foundDepartment = em.find(DepartmentOTM.class, depId);
        Assert.assertNotNull(foundDepartment);
        print("access employees");
        int counter = 2;
        for (EmployeeMTObis emp : foundDepartment.getEmployees()) {
            Assert.assertNotNull(emp);
            Assert.assertEquals(depId, emp.getDepartment().getId());
            Assert.assertEquals("Computer Science", emp.getDepartment().getName());
            if (emp.getId().equals(emp1Id)) {
                counter--;
                Assert.assertEquals(emp1Id, emp.getId());
                Assert.assertEquals("Fabio", emp.getName());
                Assert.assertEquals((Long) 123L, emp.getSalary());
            } else if (emp.getId().equals(emp2Id)) {
                counter--;
                Assert.assertEquals(emp2Id, emp.getId());
                Assert.assertEquals("Crizia", emp.getName());
                Assert.assertEquals((Long) 456L, emp.getSalary());
            }
        }
        Assert.assertEquals(0, counter);

        print("update");
        foundDepartment.setName("Software Engineering");
        em.merge(foundDepartment);

        clear();

        foundDepartment = em.find(DepartmentOTM.class, depId);
        Assert.assertNotNull(foundDepartment);
        print("access employees");
        counter = 2;
        for (EmployeeMTObis emp : foundDepartment.getEmployees()) {
            Assert.assertNotNull(emp);
            Assert.assertEquals(depId, emp.getDepartment().getId());
            Assert.assertEquals("Software Engineering", emp.getDepartment().getName());
            if (emp.getId().equals(emp1Id)) {
                counter--;
                Assert.assertEquals(emp1Id, emp.getId());
                Assert.assertEquals("Fabio", emp.getName());
                Assert.assertEquals((Long) 123L, emp.getSalary());
            } else if (emp.getId().equals(emp2Id)) {
                counter--;
                Assert.assertEquals(emp2Id, emp.getId());
                Assert.assertEquals("Crizia", emp.getName());
                Assert.assertEquals((Long) 456L, emp.getSalary());
            }
        }
        Assert.assertEquals(0, counter);

        print("delete");
        em.remove(foundDepartment);
        foundDepartment = em.find(DepartmentOTM.class, emp1Id);
        Assert.assertNull(foundDepartment);
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
