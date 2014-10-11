package it.polimi.datastore.test.model;

import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.util.Set;

@Data
@NoArgsConstructor
@Entity
@Table(name = "EmployeeMTM", schema = "gae-test@pu")
public class EmployeeMTM {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "EMPLOYEE_ID")
    private String id;

    @Column(name = "NAME")
    private String name;

    @Column(name = "SALARY")
    private Long salary;

    @ManyToMany
    @JoinTable(name = "EMPLOYEE_PROJECT",
            joinColumns = {@JoinColumn(name = "EMPLOYEE_ID")},
            inverseJoinColumns = {@JoinColumn(name = "PROJECT_ID")})
    private Set<Project> projects;
}
