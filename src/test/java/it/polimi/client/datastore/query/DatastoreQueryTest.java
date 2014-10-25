package it.polimi.client.datastore.query;

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import it.polimi.client.datastore.entities.*;
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
public class DatastoreQueryTest {

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
    public void testSelectQuery() {
        print("create");
        Employee employee1 = new Employee();
        employee1.setName("Fabio");
        employee1.setSalary(123L);
        em.persist(employee1);

        Employee employee2 = new Employee();
        employee2.setName("Crizia");
        employee2.setSalary(456L);
        em.persist(employee2);

        String emp1Id = employee1.getId();
        String emp2Id = employee2.getId();
        clear();

        print("select all");
        Query query = em.createQuery("SELECT e FROM Employee e");
        List<Employee> allEmployees = query.getResultList();
        Assert.assertNotNull(allEmployees);
        Assert.assertEquals(2, allEmployees.size());
        int toCheck = 2;
        for (Employee emp : allEmployees) {
            Assert.assertNotNull(emp.getId());
            if (emp.getId().equals(emp1Id)) {
                toCheck--;
                Assert.assertEquals("Fabio", emp.getName());
                Assert.assertEquals((Long) 123L, emp.getSalary());
            } else if (emp.getId().equals(emp2Id)) {
                toCheck--;
                Assert.assertEquals("Crizia", emp.getName());
                Assert.assertEquals((Long) 456L, emp.getSalary());
            }
        }
        Assert.assertEquals(0, toCheck);

        clear();

        print("where clause");
        query = em.createQuery("SELECT e FROM Employee e WHERE e.id = :id");
        Employee foundEmployee = (Employee) query.setParameter("id", emp1Id).getSingleResult();
        Assert.assertNotNull(foundEmployee);
        Assert.assertEquals(emp1Id, foundEmployee.getId());
        Assert.assertEquals("Fabio", foundEmployee.getName());
        Assert.assertEquals((Long) 123L, foundEmployee.getSalary());

        clear();

        print("complex where clause");
        query = em.createQuery("SELECT e FROM Employee e WHERE e.name = :n AND e.salary = :s");
        foundEmployee = (Employee) query.setParameter("n", "Crizia").setParameter("s", 456L).getSingleResult();
        Assert.assertNotNull(foundEmployee);
        Assert.assertEquals(emp2Id, foundEmployee.getId());
        Assert.assertEquals("Crizia", foundEmployee.getName());
        Assert.assertEquals((Long) 456L, foundEmployee.getSalary());

        // TODO query over embedded, element collections and enums ?

        clear();

        print("order by clause");
        query = em.createQuery("SELECT e FROM Employee e ORDER BY e.name");
        allEmployees = query.getResultList();
        Assert.assertEquals(2, allEmployees.size());
        Assert.assertTrue(allEmployees.get(0).getName().equals("Crizia"));
        Assert.assertTrue(allEmployees.get(1).getName().equals("Fabio"));
        query = em.createQuery("SELECT e FROM Employee e ORDER BY e.salary DESC");
        allEmployees = query.getResultList();
        Assert.assertEquals(2, allEmployees.size());
        Assert.assertTrue(allEmployees.get(0).getSalary().equals(456L));
        Assert.assertTrue(allEmployees.get(1).getSalary().equals(123L));
    }

    //@Test
    public void testUpdateDeleteQuery() {
        print("create");
        Employee employee = new Employee();
        employee.setName("Fabio");
        employee.setSalary(123L);
        em.persist(employee);

        String emp1Id = employee.getId();
        clear();

        print("update");
        Query query = em.createQuery("UPDATE Employee SET salary = :s WHERE name = :n");
        int updated = query.setParameter("s", 789L).setParameter("n", "Pippo").executeUpdate();
        Assert.assertEquals(1, updated);

        query = em.createQuery("SELECT e FROM Employee e WHERE e.id = :id");
        Employee foundEmployee = (Employee) query.setParameter("id", emp1Id).getSingleResult();
        Assert.assertNotNull(foundEmployee);
        Assert.assertEquals(emp1Id, foundEmployee.getName());
        Assert.assertEquals("Pippo", foundEmployee.getName());
        Assert.assertEquals((Long) 789L, foundEmployee.getSalary());

        print("delete");
        query = em.createQuery("DELETE FROM Employee e WHERE e.name = :n");
        int deleted = query.setParameter("n", "Fabio").executeUpdate();
        Assert.assertEquals(1, deleted);

        query = em.createQuery("SELECT e FROM Employee e");
        List<Employee> allEmployees = query.getResultList();
        Assert.assertNotNull(allEmployees);
        Assert.assertTrue(allEmployees.isEmpty());
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
