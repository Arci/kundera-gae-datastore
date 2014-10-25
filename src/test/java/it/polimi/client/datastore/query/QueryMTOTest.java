package it.polimi.client.datastore.query;

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import it.polimi.client.datastore.entities.Department;
import it.polimi.client.datastore.entities.EmployeeMTO;
import it.polimi.client.datastore.entities.EmployeeOTO;
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
public class QueryMTOTest {

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

        print("select all");
        Query query = em.createQuery("SELECT e FROM EmployeeMTO e");
        List<EmployeeMTO> allEmployees = query.getResultList();
        int toCheck = 2;
        for (EmployeeMTO emp : allEmployees) {
            Assert.assertNotNull(emp.getId());
            Assert.assertNotNull(emp.getDepartment());
            if (emp.getId().equals(emp1Id)) {
                toCheck--;
                Assert.assertEquals("Fabio", emp.getName());
                Assert.assertEquals((Long) 123L, emp.getSalary());
                Assert.assertEquals(depId, emp.getDepartment().getId());
                Assert.assertEquals("Computer Science", emp.getDepartment().getName());
            } else if (emp.getId().equals(emp2Id)) {
                toCheck--;
                Assert.assertEquals("Crizia", emp.getName());
                Assert.assertEquals((Long) 456L, emp.getSalary());
                Assert.assertEquals(depId, emp.getDepartment().getId());
                Assert.assertEquals("Computer Science", emp.getDepartment().getName());
            }
        }
        Assert.assertEquals(0, toCheck);

        clear();

        print("select by inner filed");
        query = em.createQuery("SELECT e FROM EmployeeMTO e WHERE e.department = :did AND e.name = :n");
        EmployeeMTO foundEmployee = (EmployeeMTO) query.setParameter("did", depId).setParameter("n", "Fabio").getSingleResult();
        Assert.assertNotNull(foundEmployee.getId());
        Assert.assertNotNull(foundEmployee.getDepartment());
        Assert.assertEquals("Fabio", foundEmployee.getName());
        Assert.assertEquals((Long) 123L, foundEmployee.getSalary());
        Assert.assertEquals(depId, foundEmployee.getDepartment().getId());
        Assert.assertEquals("Computer Science", foundEmployee.getDepartment().getName());
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
