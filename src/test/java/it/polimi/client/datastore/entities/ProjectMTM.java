package it.polimi.client.datastore.entities;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import javax.persistence.*;
import java.util.List;

@ToString(exclude = "employees")
@NoArgsConstructor
@Entity
@Table(name = "ProjectMTM", schema = "gae-test@pu")
public class ProjectMTM {

    @Getter
    @Setter
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "PROJECT_ID")
    private String id;

    @Getter
    @Setter
    @Column(name = "NAME")
    private String name;

    @Getter
    @Setter
    @ManyToMany(mappedBy = "projects")
    private List<EmployeeMTM> employees;

    public void addEmployees(EmployeeMTM... employees) {
        for (EmployeeMTM e : employees) {
            this.employees.add(e);
        }
    }
}
