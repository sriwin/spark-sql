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


CREATE DATABASE IF NOT EXISTS $DATABASE_NAME LOCATION "abfss://adlaxyx.xyz.net/xyz/xyz";
```

## Drop Database Command
```sql
DROP DATABASE IF EXISTS $DATABASE_NAME CASCADE;
```

## Create Tables Command

### Delta Table 

```sql
CREATE TABLE IF NOT EXISTS $DBSCHEMA.a2b_logs 
(
env  					STRING, 
app  					STRING, 
service_name 	STRING,
log_date     	STRING, 
log_level   	STRING, 
message      	STRING
)
USING delta PARTITIONED BY (app) 
LOCATION "abfss://adlaxyx.xyz.net/health/app/health_logs/*/*/*";
```

### External Table
- 
```sql
CREATE EXTERNAL TABLE health_db.app_logs
(
env  					STRING, 
app  					STRING, 
service_name 	STRING,
log_date     	STRING, 
log_level   	STRING, 
message      	STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
LOCATION "abfss://adlaxyx.xyz.net/health/app/health_logs/*/*/*";
```

### Create Table From Other Env

```sql
Create or replace table ###target-table###
DEEP CLONE ###source-table####
LOCATION abfss://cntnr@strg.dfs.core.windows.net/env-folder/###target-table###
```
### Refresh Table

```
REFRESH TABLE db_schema.table_name
```

## Create-View

```
val viewQry="""CREATE OR REPLACE TEMPORARY VIEW """+viewName + """ USING org.apache.spark.sql.parquet OPTIONS (path '""" +_viewpath + """')"""
or
CREATE OR REPLACE TEMPORARY VIEW VIEWNAME USING org.apache.spark.sql.parquet OPTIONS (path '""" +_viewpath + """')
```

## Rename Table
```sql
ALTER TABLE health_db.source_table RENAME TO health_db.target
```

## Optimize Table
```
OPTIMIZE health_db.source_table
```
## Describe Command

```sql
describe health_db.health_records;
describe extended health_db.health_records;
describe formatted health_db.health_records;
```

## Delta Properties

```
ALTER TABLE health_db.health_records; SET TBLPROPERTIES ('delta.isolationLevel' = 'Serializable')
ALTER TABLE health_db.health_records; SET TBLPROPERTIES ('delta.isolationLevel' = 'WriteSerializable')
ALTER TABLE health_db.health_records; SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true')
spark.sql("set spark.databricks.delta.autoCompact.enabled = true")
```

## Remove Double Quote
```
val removeDoubleQuotes = udf( (x:String) => s.replace("\"","'"))
```
## Merge - Using SQL

```
spark.sql(s"""
     |MERGE INTO $targetTableName
     |USING $updatesTableName
     |ON $targetTableName.active IN (1,0) AND $targetTableName.id = $updatesTableName.id
     |WHEN MATCHED THEN
     |  UPDATE SET $targetTableName.ts = $updatesTableName.ts
     |WHEN NOT MATCHED THEN
     |  INSERT (id, active, ts) VALUES ($updatesTableName.id, $updatesTableName.active, $updatesTableName.ts)
 """.stripMargin)
```

## Merge - Using Scala

```
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.{add_months, col, current_timestamp, date_format, lit, monotonically_increasing_id}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable
import scala.util.Random

val tableName = dbSchema + "." + "tableName"
val upsertCondition = "target.col1 = source.col1" +
	" and target.col2 =source.col2" +
	" and target.col3 =source.col3" +
	" and target.col4 =source.col4" +
	" and target.col5 =source.col5" +
	" and target.col6 =source.col6" +
	" and target.col7 =source.col7"


val deltaTable = DeltaTable.forName(spark, tableName)
deltaTable.as("target")
	.merge(df.alias("source"), upsertCondition)
	.whenMatched
	.updateExpr(Map("col8" -> "source.col8",
		"col09" -> "source.col09",
		"col10" -> "source.col10",
		"col11" -> "source.col11",
		"col12" -> "source.col12"
	))
	.whenNotMatched
	.insertAll()
	.execute()
```

## JSON Query

```sql
SELECT cast(a.record_id as bigint) as record_id,
	  Get_json_object(payload,'$.parentObj.child01.field01') AS field01
FROM   db_schema.table_name
WHERE  a.record_id = b.record_id
```
