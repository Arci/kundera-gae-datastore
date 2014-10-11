package it.polimi.datastore.test.model;

import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.util.Set;

@Data
@NoArgsConstructor
@Entity
@Table(name = "DepartmentOTM", schema = "gae-test@pu")
public class DepartmentOTM {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "DEPARTMENT_ID")
    private String id;

    @Column(name = "NAME")
    private String name;

    /* a department employs many employees */
    @OneToMany
    @JoinColumn(name = "EMPLOYEE_ID") //  TODO or DEPARTMENT_ID?
    private Set<Employee> employees;
}
