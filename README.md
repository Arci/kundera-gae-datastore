
#kundera-gae-datastore

Kundera client module to support Google App Engine Datastore.

Kundera is _a JPA 2.0 compliant Object-Datastore Mapping Library for NoSQL Datastores_ and is available [here](https://github.com/impetus-opensource/Kundera).

For complete documentation see [Kundera Wiki](https://github.com/impetus-opensource/Kundera/wiki).

##Supported Features
The following feature are supported by this extension:

- JPA relationships are supported as Kundera supports them
- ` @GeneratedValue` only with strategy `GenerationType.AUTO`.
- `@ElementCollection` java `Collection` or `Map` are supported as types and are serialized when persisted into datastore.
- `@Embedded` embedded entities are natively supported by datastore so are stored using datastore `EmbeddedEntity`.
- `@Enumerated` java `Enum` types are supported and stored as strings.

For each feature see the relative [JUnit test](https://github.com/Arci/kundera-azure-table/tree/master/src/test/java/it/polimi/kundera/client/azuretable/tests) for usage examples.

__Note:__ java `List` are natively supported by datastore but only for primitive types, for uniformity each map or collection is serialized.

##ID and Consistency
In GAE Datastore, consistency (__strong__ vs __eventual__) is managed through ancestor paths.

Was not possible for this release to automatically guess the ancestor path through JPA relationships so is not possible for the user to manage entity consistency, each entity is stored in a separated entity group identified by its Kind (the name of the JPA table associated to the entity).

IDs can be specified by the user or automatically generated, there are three possibilities:

- `@Id` annotation on a `String` type field
- `@Id` annotation on a `Long` type field
- `@Id` annotation on a `long` type field

For each case the ID can be user specified before the persist operation but in case of ID auto-generated the field must be of type `String` and the generated ID will be a string representation of a random java `UUID`.

##Query support
JPQL queries are supported as Kundera supports them, the operator supported is resumed in the following table:

| JPA-QL Clause | Azure Table |
|:-------------:|:-----------:|
| SELECT        | &#10004;    |
| UPDATE        | &#10004;    |
| DELETE        | &#10004;    |
| ORDER BY      | &#10004;    |
| AND           | &#10004;    |
| OR            | &#10004;    |
| BETWEEN       | &#10004;    |
| LIKE          | X           |
| IN            | &#10004;    |
| =             | &#10004;    |
| >             | &#10004;    |
| <             | &#10004;    |
| >=            | &#10004;    |
| <=            | &#10004;    |

Examples in use of queries can be found in the [JUnit test](https://github.com/Arci/kundera-gae-datastore/blob/master/src/test/java/it/polimi/kundera/client/datastore/tests/DatastoreQueryTest.java).

More details on the operator supported by Datastore can be found in the [official documentation](https://cloud.google.com/appengine/docs/java/datastore/queries).

##Configuration

####persistence.xml
The configuration is done in the persistence.xml file, two configuration are possible:

1. use the datastore instance within the appengine application
2. use a remote datastore instance through remote API

the properties to be specified inside the`<properties>` tag for the first case are:

- `kundera.client.lookup.class` __required__, `it.polimi.kundera.client.datastore.DatastoreClientFactory`
- `kundera.ddl.auto.prepare` _optional_, possible values are:
  - `create` which creates the schema (if not already exists)
  - `create-drop` which drop the schema (if exists) and creates it
- `kundera.client.property` _optional_, the name of the xml file containing the datastore specific properties.

in addition to the previous properties and in case of remote API, those properties are also necessary:

- `kundera.nodes` __required__, url of the appengine application on which the datastore is located
- `kundera.port` _optional_ default: 443, port used to connect to datastore
- `kundera.username` __required__, username of an admin on the remote server
- `kundera.password` __required__, password of an admin on the remote server

To test against local appengine runtime emulator the configuration is as follow:
```
<property name="kundera.nodes" value="localhost"/>
<property name="kundera.port" value="8888"/>
<property name="kundera.username" value="username"/>
<property name="kundera.password""/>
```
the value for `kundera.password` does not matter.

####Datastore specific properties
A file with client specific properties can be created and placed inside the classpath, you need to specify its name in the persistence.xml file.

the skeleton of the file is the following:

```
<?xml version="1.0" encoding="UTF-8"?>
<clientProperties>
    <datastores>
        <dataStore>
            <name>datastore</name>
            <connection>
                <properties>
                    <!-- list of properties -->
                    <property name="" value=""></property>
                </properties>
            </connection>
        </dataStore>
    </datastores>
</clientProperties>
```
for more information see [kundera datastore specific properties](https://github.com/impetus-opensource/Kundera/wiki/Data-store-Specific-Configuration).

The available properties are:

- `datastore.policy.read` [eventual|strong] _default: strong_, set the read policy.
- `datastore.deadline` _optional_, RPCs deadline in seconds.
- `datastore.policy.transaction` [auto|none] _default: none_, define if use implicit transaction.
