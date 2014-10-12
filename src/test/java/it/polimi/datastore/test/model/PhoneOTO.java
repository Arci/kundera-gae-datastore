package it.polimi.datastore.test.model;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import javax.persistence.*;

@ToString(exclude = "employee")
@NoArgsConstructor
@Entity
@Table(name = "PhoneOTO", schema = "gae-test@pu")
public class PhoneOTO {

    @Getter
    @Setter
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "PHONE_ID")
    private String id;

    @Getter
    @Setter
    @Column(name = "NUMBER")
    private Long number;

    /* bidirectional one to one */
    @Getter
    @Setter
    @OneToOne(mappedBy = "phone")
    //@OneToOne @JoinColumn(name = "EMPLOYEE_ID") TODO or PHONE_ID??
    private Employee employee;
}
