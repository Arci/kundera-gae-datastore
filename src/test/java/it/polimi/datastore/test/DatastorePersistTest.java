package it.polimi.datastore.test;

import com.google.appengine.api.datastore.dev.LocalDatastoreService;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import it.polimi.datastore.test.model.*;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import java.io.File;

/**
 * @author Fabio Arcidiacono.
 */
public class DatastorePersistTest {

    private File dbContents = new File("datastore/testDB/WEB-INF/appengine-generated/local_db.bin");
    private LocalDatastoreServiceTestConfig datastoreConfig = new LocalDatastoreServiceTestConfig()
            .setBackingStoreLocation(dbContents.getAbsolutePath())
            .setNoStorage(false);
    private final LocalServiceTestHelper helper = new LocalServiceTestHelper(datastoreConfig);
    private static final String PERSISTENCE_UNIT = "pu";
    private EntityManagerFactory emf;
    private EntityManager em;

    @Before
    public void setUp() {
        helper.setUp();
        LocalDatastoreService dsService = (LocalDatastoreService) LocalServiceTestHelper.getLocalService(LocalDatastoreService.PACKAGE);
        dsService.setNoStorage(false);
        emf = Persistence.createEntityManagerFactory(PERSISTENCE_UNIT);
        if (em != null && em.isOpen()) {
            em.close();
        }
        em = emf.createEntityManager();
        System.out.println();
    }

    @After
    public void tearDown() {
        em.close();
        emf.close();
        helper.tearDown();
    }

    @Test
    public void simplePersist() {
        Employee employee = new Employee();
        employee.setName("Fabio");
        employee.setSalary(123L);
        Assert.assertNull(employee.getId());
        em.persist(employee);
        em.flush();

        Assert.assertNotNull(employee.getId());
        String empID = employee.getId();
        em.clear();
        Employee retrievedEmployee = em.find(Employee.class, empID);
        Assert.assertTrue(retrievedEmployee.getName().equals("Fabio"));
        Assert.assertTrue(retrievedEmployee.getSalary().equals(123L));

        Phone phone = new Phone();
        phone.setNumber(123456789L);
        Assert.assertNull(phone.getId());
        em.persist(phone);
        em.flush();

        Assert.assertNotNull(phone.getId());
        String phoneID = phone.getId();
        em.clear();
        Phone retrievedPhone = em.find(Phone.class, phoneID);
        Assert.assertTrue(retrievedPhone.getNumber().equals(123456789L));
    }

    @Test
    public void persistOTO() {
        EmployeeOTO employeeOTO = new EmployeeOTO();
        employeeOTO.setName("Fabio");
        employeeOTO.setSalary(123L);
        Phone phone = new Phone();
        phone.setNumber(123456789L);
        employeeOTO.setPhone(phone);
        em.persist(phone);
        em.persist(employeeOTO);
        em.flush();

        Assert.assertTrue(employeeOTO.getPhone() != null);
        Assert.assertEquals(phone.getId(), employeeOTO.getPhone().getId());
        Assert.assertEquals(phone.getNumber(), employeeOTO.getPhone().getNumber());

        String empID = employeeOTO.getId();
        em.clear();
        EmployeeOTO retrieved = em.find(EmployeeOTO.class, empID);

        Assert.assertEquals(retrieved.getName(), "Fabio");
        Assert.assertTrue(retrieved.getSalary().equals(123L));
        Assert.assertTrue(retrieved.getPhone() != null);
        Assert.assertEquals(phone.getId(), retrieved.getPhone().getId());
        Assert.assertEquals(phone.getNumber(), retrieved.getPhone().getNumber());

    }

    @Test
    public void persistMTO() {
        EmployeeMTO employeeMTO = new EmployeeMTO();
        employeeMTO.setName("Fabio");
        employeeMTO.setSalary(123L);
        Department department = new Department();
        department.setName("Computer Science");
        employeeMTO.setDepartment(department);
        em.persist(department);
        em.persist(employeeMTO);
        em.flush();

        Assert.assertTrue(employeeMTO.getDepartment() != null);
        Assert.assertEquals(department.getId(), employeeMTO.getDepartment().getId());
        Assert.assertEquals(department.getName(), employeeMTO.getDepartment().getName());

        String empID = employeeMTO.getId();
        em.clear();
        EmployeeMTO retrieved = em.find(EmployeeMTO.class, empID);

        Assert.assertEquals(retrieved.getName(), "Fabio");
        Assert.assertTrue(retrieved.getSalary().equals(123L));
        Assert.assertTrue(retrieved.getDepartment() != null);
        Assert.assertEquals(department.getId(), retrieved.getDepartment().getId());
        Assert.assertEquals(department.getName(), retrieved.getDepartment().getName());

    }
}
