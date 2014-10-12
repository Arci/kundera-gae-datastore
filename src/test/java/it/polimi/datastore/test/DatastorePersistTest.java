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
import java.util.ArrayList;

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

    //@Test
    public void simplePersist() {
        Employee employee = new Employee();
        employee.setName("Fabio");
        employee.setSalary(123L);

        Phone phone = new Phone();
        phone.setNumber(123456789L);

        // ids are null before persist
        Assert.assertNull(employee.getId());
        Assert.assertNull(phone.getId());

        em.persist(phone);
        em.persist(employee);
        em.flush();

        // ids are populated after persist
        Assert.assertNotNull(employee.getId());
        Assert.assertNotNull(phone.getId());

        /*
         * force detach entities and retrieve them again
         */
        String empID = employee.getId();
        String phoneID = phone.getId();
        em.clear();
        System.out.println("-----------------------------------------\n\t\tCLEAR ENTITY MANAGER\n-----------------------------------------\n");
        Employee retrievedEmployee = em.find(Employee.class, empID);
        Phone retrievedPhone = em.find(Phone.class, phoneID);

        // employee is retrieved successfully
        Assert.assertTrue(retrievedEmployee.getName().equals("Fabio"));
        Assert.assertTrue(retrievedEmployee.getSalary().equals(123L));
        // phone is retrieved successfully
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

        // employee "Fabio" have a phone
        Assert.assertNotNull(employeeOTO.getPhone());
        // employee "Fabio" have the right entity as phone
        Assert.assertEquals(phone.getId(), employeeOTO.getPhone().getId());
        Assert.assertEquals(phone.getNumber(), employeeOTO.getPhone().getNumber());

        /*
         * force detach entities and retrieve them again
         */
        String empID = employeeOTO.getId();
        em.clear();
        System.out.println("-----------------------------------------\n\t\tCLEAR ENTITY MANAGER\n-----------------------------------------\n");
        EmployeeOTO retrieved = em.find(EmployeeOTO.class, empID);

        // employee is retrieved successfully
        Assert.assertEquals(retrieved.getName(), "Fabio");
        Assert.assertTrue(retrieved.getSalary().equals(123L));
        // employee "Fabio" have a phone
        Assert.assertNotNull(retrieved.getPhone());
        // employee "Fabio" have the right entity as phone
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

        // employee "Fabio" have a department
        Assert.assertNotNull(employeeMTO.getDepartment());
        // employee "Fabio" have "Computer Science" as department
        Assert.assertEquals(department.getId(), employeeMTO.getDepartment().getId());
        Assert.assertEquals(department.getName(), employeeMTO.getDepartment().getName());

        /*
         * force detach entities and retrieve them again
         */
        String empID = employeeMTO.getId();
        em.clear();
        System.out.println("-----------------------------------------\n\t\tCLEAR ENTITY MANAGER\n-----------------------------------------\n");
        EmployeeMTO retrieved = em.find(EmployeeMTO.class, empID);

        // employee is retrieved successfully
        Assert.assertEquals(retrieved.getName(), "Fabio");
        Assert.assertTrue(retrieved.getSalary().equals(123L));
        // employee "Fabio" have a department
        Assert.assertNotNull(retrieved.getDepartment());
        // employee "Fabio" have "Computer Science" as department
        Assert.assertEquals(department.getId(), retrieved.getDepartment().getId());
        Assert.assertEquals(department.getName(), retrieved.getDepartment().getName());
    }

    @Test
    public void persistOTM() {
        EmployeeMTObis employeeMTObis1 = new EmployeeMTObis();
        employeeMTObis1.setName("Fabio");
        employeeMTObis1.setSalary(123L);

        EmployeeMTObis employeeMTObis2 = new EmployeeMTObis();
        employeeMTObis2.setName("Crizia");
        employeeMTObis2.setSalary(456L);

        DepartmentOTM departmentOTM = new DepartmentOTM();
        departmentOTM.setName("Computer Science");
        departmentOTM.setEmployees(new ArrayList<EmployeeMTObis>());
        departmentOTM.addEmployee(employeeMTObis1);
        departmentOTM.addEmployee(employeeMTObis2);

        employeeMTObis1.setDepartment(departmentOTM);
        employeeMTObis2.setDepartment(departmentOTM);

        em.persist(departmentOTM);
        em.persist(employeeMTObis1);
        em.persist(employeeMTObis2);
        em.flush();

        // employee "Fabio" have a department
        Assert.assertNotNull(employeeMTObis1.getDepartment());
        // employee "Fabio" have "Computer Science" as department
        Assert.assertEquals(departmentOTM.getId(), employeeMTObis1.getDepartment().getId());
        Assert.assertEquals(departmentOTM.getName(), employeeMTObis1.getDepartment().getName());
        // employee "Crizia" have a department
        Assert.assertNotNull(employeeMTObis2.getDepartment());
        // employee "Crizia" have "Computer Science" as department
        Assert.assertEquals(departmentOTM.getId(), employeeMTObis2.getDepartment().getId());
        Assert.assertEquals(departmentOTM.getName(), employeeMTObis2.getDepartment().getName());
        // department has two employees
        Assert.assertNotNull(departmentOTM.getEmployees());
        Assert.assertEquals(2, departmentOTM.getEmployees().size());
        // department employees are "Fabio" and "Crizia"
        Assert.assertEquals(departmentOTM.getEmployees().get(0).getId(), employeeMTObis1.getId());
        Assert.assertEquals(departmentOTM.getEmployees().get(0).getName(), employeeMTObis1.getName());
        Assert.assertEquals(departmentOTM.getEmployees().get(0).getSalary(), employeeMTObis1.getSalary());
        Assert.assertEquals(departmentOTM.getEmployees().get(1).getId(), employeeMTObis2.getId());
        Assert.assertEquals(departmentOTM.getEmployees().get(1).getName(), employeeMTObis2.getName());
        Assert.assertEquals(departmentOTM.getEmployees().get(1).getSalary(), employeeMTObis2.getSalary());

        /*
         * force detach entities and retrieve them again
         */
        String emp1ID = employeeMTObis1.getId();
        String emp2ID = employeeMTObis2.getId();
        String depID = departmentOTM.getId();
        em.clear();
        System.out.println("-----------------------------------------\n\t\tCLEAR ENTITY MANAGER\n-----------------------------------------\n");
        EmployeeMTObis postEmp1 = em.find(EmployeeMTObis.class, emp1ID);
        EmployeeMTObis postEmp2 = em.find(EmployeeMTObis.class, emp2ID);
        DepartmentOTM postDep = em.find(DepartmentOTM.class, depID);

        // retrieved "Computer Science" has employee
        Assert.assertNotNull(postDep.getEmployees());
        System.out.println("----------------------------------------- dep employees -----------------------------------------");
        for (EmployeeMTObis emp : postDep.getEmployees()) {
            // "Computer Science" employees are either "Fabio" or "Crizia"
            Assert.assertTrue(emp.getId().equals(postEmp1.getId()) || emp.getId().equals(postEmp2.getId()));
            // employees have "Computer Science" as department
            Assert.assertEquals(emp.getDepartment().getId(), postDep.getId());
            Assert.assertEquals(emp.getDepartment().getName(), postDep.getName());
        }
        System.out.println("----------------------------------------------------------------------------------");
    }
}
