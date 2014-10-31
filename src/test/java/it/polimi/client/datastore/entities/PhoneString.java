package it.polimi.client.datastore.entities;

import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

@Data
@NoArgsConstructor
@Entity
@Table(name = "PhoneString", schema = "gae-test@pu")
public class PhoneString {

    @Id
    @Column(name = "PHONE_ID")
    private String id;

    @Column(name = "NUMBER")
    private Long number;
}
