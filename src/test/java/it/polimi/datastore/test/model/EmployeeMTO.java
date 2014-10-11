package it.polimi.datastore.test.model;

import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

@Data
@NoArgsConstructor
@Entity
@Table(name = "EmployeeMTO", schema = "gae-test@pu")
public class EmployeeMTO {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "EMPLOYEE_ID")
    private String id;

    @Column(name = "NAME")
    private String name;

    @Column(name = "SALARY")
    private Long salary;

    /* many employees work in one department */
    @ManyToOne
    @JoinColumn(name = "DEPARTMENT_ID")
    private Department department;
}
