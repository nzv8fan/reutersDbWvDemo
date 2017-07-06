# reutersDbWvDemo
Demo code for creating WordVector models through connecting DeepLearning4J with Postgres on the Reuters-21578 dataset

The purpose of this code is to primarily show how DeepLearning4J can train a WordVector model from data contained in a common RDBMS database rather than flat files or a distributed database system.  

Additionally, the code shows a custom pre-processor and custom iterators over the database ResultSet object.

### If you don't have text in a database then create some:

1. Download the SGM files: 
```./downloadReutersSGM.sh```

2. Setup the Database:
  * Copy the code from ```setupDbTable.sql``` into your database server
  * Optional: Change the pom.xml file if you are using something other than postgres as your database

3. Build the project:
```mvn package```

4. Extract the SGM files into the DB - this creates 21,788 rows in the table

```java -cp target/reutersDbWvDemo-1.0-SNAPSHOT.jar au.edu.unsw.cse.ExtractSGM 'jdbc:postgresql://server.domain:port/database?user=username&password=password' reuters21578sgm/ <jdbcClassName>```

### Construct the Word Vector model from the DB

```java -cp target/reutersDbWvDemo-1.0-SNAPSHOT.jar au.edu.unsw.cse.BuildWordVectorsFromDatabase 'jdbc:postgresql://server.domain:port/database?user=username&password=password' <iterations> <layer size> <output filename> <sql query> <sql column name> <jdbcClassName>```

### Test the Word Vector Model

```java -cp target/reutersDbWvDemo-1.0-SNAPSHOT.jar au.edu.unsw.cse.WordVectorChecker```

