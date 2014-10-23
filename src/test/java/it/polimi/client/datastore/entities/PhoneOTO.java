package it.polimi.client.datastore.entities;

import lombok.*;

import javax.persistence.*;

@Data
@ToString(exclude = "employee")
@EqualsAndHashCode(exclude = "employee")
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
    @OneToOne(mappedBy = "phone")
    //@OneToOne @JoinColumn(name = "EMPLOYEE_ID") TODO or PHONE_ID??
    private Employee employee;
}
