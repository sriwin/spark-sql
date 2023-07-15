# spark-sql

## Widgets Command

- Note 
  - import the dbutils package if you are creating the scala notebook
    - import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

### Create Widgets Command

```sql
CREATE WIDGET TEXT DATABASE_NAME DEFAULT "health_db";
CREATE WIDGET TEXT TABLE_NAME DEFAULT "health_records";
```
### Remove All Widgets Command

  

```
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
dbutils.widgets.removeAll()
```

### Define Widgets Command
- Use the below command to create a widget with default value
  
```
dbutils.widgets.text("db_schema","health_db")
```

### Access Widgets Command

```
dbSchema = dbutils.widgets.get("db_schema")
```


## Create Database Command

```sql
CREATE WIDGET TEXT DATABASE_NAME DEFAULT "health_db";

DROP DATABASE IF EXISTS $DATABASE_NAME CASCADE;

CREATE DATABASE IF NOT EXISTS $DATABASE_NAME LOCATION "abfss://adlaxyx.xyz.net/xyz/xyz";
```

## Describe Command

```sql
describe health_db.health_records;
describe extended health_db.health_records;
describe formatted health_db.health_records;
```
