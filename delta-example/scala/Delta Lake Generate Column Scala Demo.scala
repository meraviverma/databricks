// Databricks notebook source
import org.apache.spark.sql.types._
import io.delta.tables._
DeltaTable.create(spark)
          .tableName("employee")
          .addColumn("Id", IntegerType)
          .addColumn("Name", StringType)
          .addColumn("Department", StringType)
          .addColumn("Salary", DoubleType)
          .addColumn("Doj", TimestampType)
         .addColumn(
                     DeltaTable.columnBuilder("joined_date")
                       .dataType(DateType)
                       .generatedAlwaysAs("CAST(Doj AS DATE)")
                       .build() )
         .addColumn("Date_Updated", DateType).execute()
spark.read.table("employee").printSchema()

// COMMAND ----------

display(spark.catalog.listTables("default"))



// COMMAND ----------

import org.apache.spark.sql.types._
val schema = new StructType().add("Id",IntegerType).add("Name",StringType).add("Department",StringType).add("Salary",DoubleType)
.add("Doj",TimestampType).add("Date_Updated",DateType)
val df = spark.read.schema(schema).csv("/FileStore/tables/sample_employee_data.txt")
df.show()
df.write.format("delta").mode("overwrite").saveAsTable("default.employee")


// COMMAND ----------

display(spark.read.table("employee"))



// COMMAND ----------

import org.apache.spark.sql.types._
import io.delta.tables._

DeltaTable.create(spark)
  .tableName("default.people10mscala")
  .addColumn("id", IntegerType)
  .addColumn("firstName", StringType)
  .addColumn("middleName", StringType)
  .addColumn("lastName", StringType)
  .addColumn("gender", StringType)
  .addColumn("birthDate", TimestampType)
  .addColumn(
    DeltaTable.columnBuilder("dateOfBirth")
     .dataType(DateType)
     .generatedAlwaysAs("CAST(birthDate AS DATE)")
     .build())
  .addColumn("ssn", StringType)
  .addColumn("salary", IntegerType)
  .execute()

// COMMAND ----------

display(spark.read.table("people10mscala"))

// COMMAND ----------

spark.sql("select * from people10mscala").show()

// COMMAND ----------


