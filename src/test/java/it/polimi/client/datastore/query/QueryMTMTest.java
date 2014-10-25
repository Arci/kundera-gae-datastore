package it.polimi.client.datastore.query;

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import it.polimi.client.datastore.entities.EmployeeMTM;
import it.polimi.client.datastore.entities.ProjectMTM;
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
public class QueryMTMTest {

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
        ProjectMTM project1 = new ProjectMTM();
        project1.setName("Project 1");

        ProjectMTM project2 = new ProjectMTM();
        project2.setName("Project 2");

        ProjectMTM project3 = new ProjectMTM();
        project3.setName("Project 3");

        EmployeeMTM employee1 = new EmployeeMTM();
        employee1.setName("Fabio");
        employee1.setSalary(123L);
        employee1.addProjects(project1, project2);
        em.persist(employee1);

        EmployeeMTM employee2 = new EmployeeMTM();
        employee2.setName("Crizia");
        employee2.setSalary(456L);
        employee2.addProjects(project2, project3);
        em.persist(employee2);

        String prj1Id = project1.getId();
        String prj2Id = project2.getId();
        String prj3Id = project3.getId();
        String emp1Id = employee1.getId();
        String emp2Id = employee2.getId();
        clear();

        print("select all");
        Query query = em.createQuery("SELECT e FROM EmployeeMTM e");
        List<EmployeeMTM> allEmployees = query.getResultList();
        int empToCheck = 2;
        int emp1projToCheck = 2;
        int emp2projToCheck = 2;
        for (EmployeeMTM emp : allEmployees) {
            Assert.assertNotNull(emp.getId());
            if (emp.getId().equals(emp1Id)) {
                empToCheck--;
                Assert.assertEquals("Fabio", emp.getName());
                Assert.assertEquals((Long) 123L, emp.getSalary());
                for (ProjectMTM proj : emp.getProjects()) {
                    if (proj.getId().equals(prj1Id)) {
                        emp1projToCheck--;
                        Assert.assertEquals(prj1Id, proj.getId());
                        Assert.assertEquals("Project 1", proj.getName());
                    } else if (proj.getId().equals(prj2Id)) {
                        emp1projToCheck--;
                        Assert.assertEquals(prj2Id, proj.getId());
                        Assert.assertEquals("Project 2", proj.getName());
                    }
                }
            } else if (emp.getId().equals(emp2Id)) {
                empToCheck--;
                Assert.assertEquals("Crizia", emp.getName());
                Assert.assertEquals((Long) 456L, emp.getSalary());
                for (ProjectMTM proj : emp.getProjects()) {
                    if (proj.getId().equals(prj2Id)) {
                        emp2projToCheck--;
                        Assert.assertEquals(prj2Id, proj.getId());
                        Assert.assertEquals("Project 2", proj.getName());
                    } else if (proj.getId().equals(prj3Id)) {
                        emp2projToCheck--;
                        Assert.assertEquals(prj3Id, proj.getId());
                        Assert.assertEquals("Project 3", proj.getName());
                    }
                }
            }
        }
        Assert.assertEquals(0, empToCheck);
        Assert.assertEquals(0, emp1projToCheck);
        Assert.assertEquals(0, emp2projToCheck);

        /*
         * NOTE: cannot directly query over join table
         * EMPLOYEE_PROJECT since is not a class in JPA model
         */
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
