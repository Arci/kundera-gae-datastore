package it.polimi.client.datastore.crud;

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

/**
 * @author Fabio Arcidiacono.
 */
public class DatastoreMTMTest {

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

        print("read");
        print("employee 1");
        EmployeeMTM foundEmployee1 = em.find(EmployeeMTM.class, emp1Id);
        Assert.assertNotNull(foundEmployee1);
        Assert.assertEquals(emp1Id, foundEmployee1.getId());
        Assert.assertEquals("Fabio", foundEmployee1.getName());
        Assert.assertEquals((Long) 123L, foundEmployee1.getSalary());
        print("access projects");
        int projectCount = 0;
        int project1Employees = 0;
        int project2Employees = 0;
        for (ProjectMTM proj : foundEmployee1.getProjects()) {
            if (proj.getId().equals(prj1Id)) {
                projectCount++;
                Assert.assertEquals(prj1Id, proj.getId());
                Assert.assertEquals("Project 1", proj.getName());
                print("access employees project 1");
                for (EmployeeMTM emp : proj.getEmployees()) {
                    System.out.println(emp);
                    if (emp.getId().equals(emp1Id)) {
                        project1Employees++;
                        Assert.assertEquals(emp1Id, emp.getId());
                        Assert.assertEquals("Fabio", emp.getName());
                        Assert.assertEquals((Long) 123L, emp.getSalary());
                    }
                }
            } else if (proj.getId().equals(prj2Id)) {
                projectCount++;
                Assert.assertEquals(prj2Id, proj.getId());
                Assert.assertEquals("Project 2", proj.getName());
                print("access employees project 2");
                for (EmployeeMTM emp : proj.getEmployees()) {
                    System.out.println(emp);
                    if (emp.getId().equals(emp1Id)) {
                        project2Employees++;
                        Assert.assertEquals(emp1Id, emp.getId());
                        Assert.assertEquals("Fabio", emp.getName());
                        Assert.assertEquals((Long) 123L, emp.getSalary());
                    } else if (emp.getId().equals(emp2Id)) {
                        project2Employees++;
                        Assert.assertEquals(emp2Id, emp.getId());
                        Assert.assertEquals("Crizia", emp.getName());
                        Assert.assertEquals((Long) 456L, emp.getSalary());
                    }
                }
            }
        }
        Assert.assertEquals(2, projectCount);
        Assert.assertEquals(1, project1Employees);
        Assert.assertEquals(2, project2Employees);

        print("employee 2");
        EmployeeMTM foundEmployee2 = em.find(EmployeeMTM.class, emp2Id);
        Assert.assertNotNull(foundEmployee2);
        Assert.assertEquals(emp2Id, foundEmployee2.getId());
        Assert.assertEquals("Crizia", foundEmployee2.getName());
        Assert.assertEquals((Long) 456L, foundEmployee2.getSalary());
        print("access projects");
        projectCount = 0;
        project2Employees = 0;
        int project3Employees = 0;
        for (ProjectMTM proj : foundEmployee2.getProjects()) {
            if (proj.getId().equals(prj2Id)) {
                projectCount++;
                Assert.assertEquals(prj2Id, proj.getId());
                Assert.assertEquals("Project 2", proj.getName());
                print("access employees project 2");
                for (EmployeeMTM emp : proj.getEmployees()) {
                    System.out.println(emp);
                    if (emp.getId().equals(emp1Id)) {
                        project2Employees++;
                        Assert.assertEquals(emp1Id, emp.getId());
                        Assert.assertEquals("Fabio", emp.getName());
                        Assert.assertEquals((Long) 123L, emp.getSalary());
                    } else if (emp.getId().equals(emp2Id)) {
                        project2Employees++;
                        Assert.assertEquals(emp2Id, emp.getId());
                        Assert.assertEquals("Crizia", emp.getName());
                        Assert.assertEquals((Long) 456L, emp.getSalary());
                    }
                }
            } else if (proj.getId().equals(prj3Id)) {
                projectCount++;
                Assert.assertEquals(prj3Id, proj.getId());
                Assert.assertEquals("Project 3", proj.getName());
                print("access employees project 3");
                for (EmployeeMTM emp : proj.getEmployees()) {
                    System.out.println(emp);
                    if (emp.getId().equals(emp2Id)) {
                        project3Employees++;
                        Assert.assertEquals(emp2Id, emp.getId());
                        Assert.assertEquals("Crizia", emp.getName());
                        Assert.assertEquals((Long) 456L, emp.getSalary());
                    }
                }
            }
        }
        Assert.assertEquals(2, projectCount);
        Assert.assertEquals(1, project3Employees);
        Assert.assertEquals(2, project2Employees);

        print("update");
        project1.setName("Project 11");
        project2.setName("Project 22");
        project3.setName("Project 33");
        em.merge(project1);
        em.merge(project2);
        em.merge(project3);

        clear();

        print("employee 1");
        foundEmployee1 = em.find(EmployeeMTM.class, emp1Id);
        Assert.assertNotNull(foundEmployee1);
        Assert.assertEquals(emp1Id, foundEmployee1.getId());
        Assert.assertEquals("Fabio", foundEmployee1.getName());
        Assert.assertEquals((Long) 123L, foundEmployee1.getSalary());
        print("access projects");
        projectCount = 0;
        project1Employees = 0;
        project2Employees = 0;
        for (ProjectMTM proj : foundEmployee1.getProjects()) {
            if (proj.getId().equals(prj1Id)) {
                projectCount++;
                Assert.assertEquals(prj1Id, proj.getId());
                Assert.assertEquals("Project 11", proj.getName());
                print("access employees project 11");
                for (EmployeeMTM emp : proj.getEmployees()) {
                    System.out.println(emp);
                    if (emp.getId().equals(emp1Id)) {
                        project1Employees++;
                        Assert.assertEquals(emp1Id, emp.getId());
                        Assert.assertEquals("Fabio", emp.getName());
                        Assert.assertEquals((Long) 123L, emp.getSalary());
                    }
                }
            } else if (proj.getId().equals(prj2Id)) {
                projectCount++;
                Assert.assertEquals(prj2Id, proj.getId());
                Assert.assertEquals("Project 22", proj.getName());
                print("access employees project 22");
                for (EmployeeMTM emp : proj.getEmployees()) {
                    System.out.println(emp);
                    if (emp.getId().equals(emp1Id)) {
                        project2Employees++;
                        Assert.assertEquals(emp1Id, emp.getId());
                        Assert.assertEquals("Fabio", emp.getName());
                        Assert.assertEquals((Long) 123L, emp.getSalary());
                    } else if (emp.getId().equals(emp2Id)) {
                        project2Employees++;
                        Assert.assertEquals(emp2Id, emp.getId());
                        Assert.assertEquals("Crizia", emp.getName());
                        Assert.assertEquals((Long) 456L, emp.getSalary());
                    }
                }
            }
        }
        Assert.assertEquals(2, projectCount);
        Assert.assertEquals(1, project1Employees);
        Assert.assertEquals(2, project2Employees);

        print("employee 2");
        foundEmployee2 = em.find(EmployeeMTM.class, emp2Id);
        Assert.assertNotNull(foundEmployee2);
        Assert.assertEquals(emp2Id, foundEmployee2.getId());
        Assert.assertEquals("Crizia", foundEmployee2.getName());
        Assert.assertEquals((Long) 456L, foundEmployee2.getSalary());
        print("access projects");
        projectCount = 0;
        project2Employees = 0;
        project3Employees = 0;
        for (ProjectMTM proj : foundEmployee2.getProjects()) {
            if (proj.getId().equals(prj2Id)) {
                projectCount++;
                Assert.assertEquals(prj2Id, proj.getId());
                Assert.assertEquals("Project 22", proj.getName());
                print("access employees project 22");
                for (EmployeeMTM emp : proj.getEmployees()) {
                    System.out.println(emp);
                    if (emp.getId().equals(emp1Id)) {
                        project2Employees++;
                        Assert.assertEquals(emp1Id, emp.getId());
                        Assert.assertEquals("Fabio", emp.getName());
                        Assert.assertEquals((Long) 123L, emp.getSalary());
                    } else if (emp.getId().equals(emp2Id)) {
                        project2Employees++;
                        Assert.assertEquals(emp2Id, emp.getId());
                        Assert.assertEquals("Crizia", emp.getName());
                        Assert.assertEquals((Long) 456L, emp.getSalary());
                    }
                }
            } else if (proj.getId().equals(prj3Id)) {
                projectCount++;
                Assert.assertEquals(prj3Id, proj.getId());
                Assert.assertEquals("Project 33", proj.getName());
                print("access employees project 33");
                for (EmployeeMTM emp : proj.getEmployees()) {
                    System.out.println(emp);
                    if (emp.getId().equals(emp2Id)) {
                        project3Employees++;
                        Assert.assertEquals(emp2Id, emp.getId());
                        Assert.assertEquals("Crizia", emp.getName());
                        Assert.assertEquals((Long) 456L, emp.getSalary());
                    }
                }
            }
        }
        Assert.assertEquals(2, projectCount);
        Assert.assertEquals(1, project3Employees);
        Assert.assertEquals(2, project2Employees);

        print("delete");
        em.remove(foundEmployee1);
        foundEmployee1 = em.find(EmployeeMTM.class, emp1Id);
        Assert.assertNull(foundEmployee1);
        project1 = em.find(ProjectMTM.class, prj1Id);
        Assert.assertNull(project1);
        project2 = em.find(ProjectMTM.class, prj2Id);
        Assert.assertNull(project2);

        foundEmployee2 = em.find(EmployeeMTM.class, emp2Id);
        Assert.assertNotNull(foundEmployee2);

        project3 = em.find(ProjectMTM.class, prj3Id);
        Assert.assertNotNull(project3);
    }

    @Test
    public void testQuery() {
        // TODO
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
