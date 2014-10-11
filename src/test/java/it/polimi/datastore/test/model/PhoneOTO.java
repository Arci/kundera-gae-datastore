package it.polimi.datastore.test.model;

import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

@Data
@NoArgsConstructor
@Entity
@Table(name = "PhoneOTO", schema = "gae-test@pu")
public class PhoneOTO {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "PHONE_ID")
    private String id;

    @Column(name = "NUMBER")
    private Long number;

    /* bidirectional one to one */
    @OneToOne
    @JoinColumn(name = "EMPLOYEE_ID")   //  TODO or PHONE_ID??
    private Employee employee;
}
