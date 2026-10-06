package br.com.zenon;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionFactory {

    public ConnectionFactory() {}

    public static Connection getConnection() {
        try {
            return DriverManager.getConnection("jdbc:mysql://localhost:3307/zenon_frauds", "root", "senha123");
        } catch (SQLException e) {
            throw new RuntimeException("Erro ao conectar com o Banco de Dados: ",e);
        }
    }

    /*
    * Configured authentication: Managed by agent

This code implements a simple **Factory Pattern** designed to establish and provide a connection to a MySQL database using the Java Database Connectivity (JDBC) API in [`ConnectionFactory`](file:///home/alexandre/Projetos-Pratica-UNIPDS/zenon-fraud-detector/src/main/java/br/com/zenon/ConnectionFactory.java).

---

### Code Breakdown

#### 1. Constructor
```java
public ConnectionFactory() {}
```
* **Purpose**: Default public constructor.
* **Note**: Because [`getConnection()`](file:///home/alexandre/Projetos-Pratica-UNIPDS/zenon-fraud-detector/src/main/java/br/com/zenon/ConnectionFactory.java) is `static`, this class functions as a utility/factory class. In standard Java practices, constructors for static utility classes are often made `private` to prevent unnecessary instantiation.

---

#### 2. Factory Method
```java
public static Connection getConnection()
```
* **Return Type**: Returns a `java.sql.Connection` object representing an active session with the database.
* **`static` Keyword**: Allows consumers to call `ConnectionFactory.getConnection()` directly without creating an instance of [`ConnectionFactory`](file:///home/alexandre/Projetos-Pratica-UNIPDS/zenon-fraud-detector/src/main/java/br/com/zenon/ConnectionFactory.java).

---

#### 3. Database Connection Details
```java
DriverManager.getConnection("jdbc:mysql://localhost:3307/zenon_frauds", "root", "senha123");
```
Uses `java.sql.DriverManager` to locate an appropriate JDBC driver and open a connection:
* **JDBC URL**: `"jdbc:mysql://localhost:3307/zenon_frauds"`
  * `jdbc:mysql://`: Connects via MySQL driver protocol.
  * `localhost`: Target database server host.
  * `3307`: Port number (commonly used when MySQL runs in a Docker container to prevent collision with the default `3306` port).
  * `zenon_frauds`: Target database/schema name.
* **Username**: `"root"`
* **Password**: `"senha123"`

---

#### 4. Exception Handling
```java
} catch (SQLException e) {
    throw new RuntimeException("Erro ao conectar com o Banco de Dados: ", e);
}
```
* **Catching `java.sql.SQLException`**: Catches checked database errors (e.g., incorrect credentials, unreachable server, missing database, or wrong port).
* **Exception Wrapping**: Re-throws the error as an unchecked `java.lang.RuntimeException`, attaching the original exception `e` as the cause to preserve the stack trace.
* **Benefit**: Callers do not need to explicitly handle or declare `throws SQLException` in their method signatures.

---

### Best Practice Considerations
* **Credentials & Configuration**: Database credentials and URLs are currently hardcoded; moving them to environment variables or an `application.properties` configuration file prevents sensitive data leaks.
* **Connection Pooling**: `DriverManager.getConnection(...)` opens a new physical connection per call, which is expensive. For production services, a connection pool library such as **HikariCP** is recommended.
    * */
}
