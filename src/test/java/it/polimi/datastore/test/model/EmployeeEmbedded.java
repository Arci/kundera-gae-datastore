package it.polimi.datastore.test.model;

import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

@Data
@NoArgsConstructor
@Entity
@Table(name = "EmployeeEmbedded", schema = "gae-test@pu")
public class EmployeeEmbedded {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "EMPLOYEE_ID")
    private String id;

    @Column(name = "NAME")
    private String name;

    @Column(name = "SALARY")
    private Long salary;

    @Embedded
    private Address embeddable;
}
