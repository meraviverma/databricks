# Databricks notebook source
# Given below #Jira ticket data and holiday data:
# | ticket_id | create_date | resolved_date |
# | 1 | 2022-08-01 | 2022-08-03 |
# | 2 | 2022-08-01 | 2022-08-12 |
# | 3 | 2022-08-01 | 2022-08-16 |

# | holiday_date |
# | 2022-08-11 |
# | 2022-08-15 |

# ❓Find out the total working days between ticket creation and resolution dates.

# Notes: Exclude weekend i.e saturday and sunday

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType

# COMMAND ----------

my_schema=StructType(
    [
        StructField("ticket_id",IntegerType(),True),
        StructField("create_date",DateType(),True),
        StructField("resolved_date",DateType(),True)
    ]
)

# COMMAND ----------

mydata=[(1,datetime.strptime('2022-08-01','%Y-%m-%d'),datetime.strptime('2022-08-03','%Y-%m-%d')),
        (2,datetime.strptime('2022-08-01','%Y-%m-%d'),datetime.strptime('2022-08-12','%Y-%m-%d')),
        (3,datetime.strptime('2022-08-01','%Y-%m-%d'),datetime.strptime('2022-08-16','%Y-%m-%d'))]

# COMMAND ----------

tickets=spark.createDataFrame(data=mydata,schema=my_schema)

# COMMAND ----------

display(tickets)

# COMMAND ----------

holiday_schema=StructType([
    StructField("holiday_date",DateType(),True),
    StructField("reason",StringType(),True)
])

# COMMAND ----------

holiday_data=[(datetime.strptime('2022-08-11','%Y-%m-%d'),"Rakhi"),(datetime.strptime('2022-08-15','%Y-%m-%d'),"Independence Day")]

# COMMAND ----------

holidays=spark.createDataFrame(data=holiday_data,schema=holiday_schema)

# COMMAND ----------

display(holidays)

# COMMAND ----------

display(tickets)

# COMMAND ----------

from pyspark.sql.functions import datediff,date_part,lit,floor,col,count

# COMMAND ----------

#Between one week difference there will be two weekends so we are doing minus with 2*difference in days
tickets.select(tickets.create_date,
               tickets.resolved_date,
               datediff(tickets.resolved_date,tickets.create_date).alias("actual_days"),
               date_part(lit('week'),tickets.create_date).alias("create_date_week"),
               date_part(lit('week'),tickets.resolved_date).alias("resolved_date_week"),
               floor((datediff(tickets.resolved_date,tickets.create_date)/7)).alias("week_difference"),
               (datediff(tickets.resolved_date,tickets.create_date) - 2*(floor((datediff(tickets.resolved_date,tickets.create_date)/7)))).alias("excluded_weekend_days")).show()

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

joined_data=tickets.join(holidays,(tickets.create_date < holidays.holiday_date) & (holidays.holiday_date < tickets.resolved_date),"LEFT")
#joined_data.withColumn("count_holiday_day",count("holiday_date").over(Window.partitionBy(*["ticket_id","create_date","resolved_date"]))).select(col("count_holiday_day")).show()
#joined_data.select(count("holiday_date").over(Window.partitionBy(*["ticket_id","create_date","resolved_date"])).alias("count_holiday_day")).show()
joined_data=joined_data.groupBy("ticket_id","create_date","resolved_date").agg(count('holiday_date').alias("count_holiday_day"))

# COMMAND ----------

display(joined_data)

# COMMAND ----------

joined_data.select(joined_data.ticket_id,
                   joined_data.create_date,
               joined_data.resolved_date,
               datediff(joined_data.resolved_date,joined_data.create_date).alias("actual_days"),
               ((datediff(tickets.resolved_date,tickets.create_date) - 2*(floor((datediff(tickets.resolved_date,tickets.create_date)/7)))) - col("count_holiday_day")).alias("actual_ticket_close_day")).show()

# COMMAND ----------


