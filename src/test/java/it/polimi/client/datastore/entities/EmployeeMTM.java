package it.polimi.client.datastore.entities;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import javax.persistence.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@ToString(exclude = "projects")
@NoArgsConstructor
@Entity
@Table(name = "EmployeeMTM", schema = "gae-test@pu")
public class EmployeeMTM {

    @Getter
    @Setter
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "EMPLOYEE_ID")
    private String id;

    @Getter
    @Setter
    @Column(name = "NAME")
    private String name;

    @Getter
    @Setter
    @Column(name = "SALARY")
    private Long salary;

    @Getter
    @Setter
    @ManyToMany(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    @JoinTable(name = "EMPLOYEE_PROJECT",
            joinColumns = {@JoinColumn(name = "EMPLOYEE_ID")},
            inverseJoinColumns = {@JoinColumn(name = "PROJECT_ID")})
    private List<ProjectMTM> projects;

    public void addProjects(ProjectMTM... projects) {
        if (this.projects == null) {
            this.projects = new ArrayList<ProjectMTM>();
        }
        Collections.addAll(this.projects, projects);
    }
}
