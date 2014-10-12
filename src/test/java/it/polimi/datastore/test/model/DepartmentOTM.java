package it.polimi.datastore.test.model;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import javax.persistence.*;
import java.util.List;

@ToString(exclude = "employees")
@NoArgsConstructor
@Entity
@Table(name = "DepartmentOTM", schema = "gae-test@pu")
public class DepartmentOTM {

    @Getter
    @Setter
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "DEPARTMENT_ID")
    private String id;

    @Getter
    @Setter
    @Column(name = "NAME")
    private String name;

    /* a department employs many employees */
    @Getter
    @Setter
    @OneToMany(mappedBy = "department")
    private List<EmployeeMTObis> employees;

    public void addEmployee(EmployeeMTObis employee) {
        this.employees.add(employee);
    }
}
