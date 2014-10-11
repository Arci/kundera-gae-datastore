package it.polimi.datastore.test.model;

import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.util.Set;

@Data
@NoArgsConstructor
@Entity
@Table(name = "ProjectMTM", schema = "gae-test@pu")
public class ProjectMTM {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "PROJECT_ID")
    private String id;

    @Column(name = "NAME")
    private String name;

    @ManyToMany(mappedBy = "projects")
    private Set<Employee> employees;
}
