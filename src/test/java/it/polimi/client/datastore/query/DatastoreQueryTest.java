package it.polimi.client.datastore.query;

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import it.polimi.client.datastore.entities.Employee;
import it.polimi.client.datastore.entities.PhoneEnum;
import it.polimi.client.datastore.entities.PhoneType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.persistence.*;
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

        PhoneEnum phone = new PhoneEnum();
        phone.setNumber(123L);
        phone.setType(PhoneType.HOME);
        em.persist(phone);

        String phnId = phone.getId();
        String emp1Id = employee1.getId();
        String emp2Id = employee2.getId();
        clear();

        print("select all");
        TypedQuery<Employee> query = em.createQuery("SELECT e FROM Employee e", Employee.class);
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

        print("select property");
        Query projection = em.createQuery("SELECT e.name, e.salary FROM Employee e WHERE e.id = :id");
        List results = projection.setParameter("id", emp1Id).getResultList();
        Assert.assertTrue(results.size() == 2);
        for (Object property : results) {
            Assert.assertTrue(property.equals("Fabio") || property.equals(123L));
        }

        clear();

        print("where clause");
        query = em.createQuery("SELECT e FROM Employee e WHERE e.id = :id", Employee.class);
        Employee foundEmployee = query.setParameter("id", emp1Id).getSingleResult();
        Assert.assertNotNull(foundEmployee);
        Assert.assertEquals(emp1Id, foundEmployee.getId());
        Assert.assertEquals("Fabio", foundEmployee.getName());
        Assert.assertEquals((Long) 123L, foundEmployee.getSalary());

        clear();

        print("complex where clause");
        query = em.createQuery("SELECT e FROM Employee e WHERE e.name = :n AND e.salary = :s", Employee.class);
        foundEmployee = query.setParameter("n", "Crizia").setParameter("s", 456L).getSingleResult();
        Assert.assertNotNull(foundEmployee);
        Assert.assertEquals(emp2Id, foundEmployee.getId());
        Assert.assertEquals("Crizia", foundEmployee.getName());
        Assert.assertEquals((Long) 456L, foundEmployee.getSalary());

        clear();

        print("where over enumerated");
        TypedQuery<PhoneEnum> enumQuery = em.createQuery("SELECT p FROM PhoneEnum p WHERE p.type = :type", PhoneEnum.class);
        PhoneEnum foundPhone = enumQuery.setParameter("type", PhoneType.HOME.toString()).getSingleResult();
        Assert.assertNotNull(foundPhone);
        Assert.assertEquals(phnId, foundPhone.getId());
        Assert.assertEquals((Long) 123L, foundPhone.getNumber());
        Assert.assertEquals(PhoneType.HOME, foundPhone.getType());

        clear();

        print("order by clause");
        query = em.createQuery("SELECT e FROM Employee e ORDER BY e.name", Employee.class);
        allEmployees = query.getResultList();
        Assert.assertEquals(2, allEmployees.size());
        Assert.assertTrue(allEmployees.get(0).getName().equals("Crizia"));
        Assert.assertTrue(allEmployees.get(1).getName().equals("Fabio"));
        query = em.createQuery("SELECT e FROM Employee e ORDER BY e.salary DESC", Employee.class);
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
        TypedQuery<Employee> query = em.createQuery("UPDATE Employee SET salary = :s WHERE name = :n", Employee.class);
        int updated = query.setParameter("s", 789L).setParameter("n", "Pippo").executeUpdate();
        Assert.assertEquals(1, updated);

        query = em.createQuery("SELECT e FROM Employee e WHERE e.id = :id", Employee.class);
        Employee foundEmployee = query.setParameter("id", emp1Id).getSingleResult();
        Assert.assertNotNull(foundEmployee);
        Assert.assertEquals(emp1Id, foundEmployee.getName());
        Assert.assertEquals("Pippo", foundEmployee.getName());
        Assert.assertEquals((Long) 789L, foundEmployee.getSalary());

        print("delete");
        query = em.createQuery("DELETE FROM Employee e WHERE e.name = :n", Employee.class);
        int deleted = query.setParameter("n", "Fabio").executeUpdate();
        Assert.assertEquals(1, deleted);

        query = em.createQuery("SELECT e FROM Employee e", Employee.class);
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
