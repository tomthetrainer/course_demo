# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # NOTEBOOK GOALS
# MAGIC
# MAGIC The main goal of this demo is to give you an opportunity to explore some key settings for streaming operations
# MAGIC
# MAGIC Note that this notebook can just be a starting point, you can run this demo pretty much anywhere and use it, or add to it to explore the behavior of key settings like
# MAGIC * output mode
# MAGIC * watermarking
# MAGIC
# MAGIC You can also explore the spark ui, streaming tab, or individual jobs for spark performance issues such as:
# MAGIC * Skew
# MAGIC * Shuffle
# MAGIC * Spill
# MAGIC * Garbage Collection
# MAGIC
# MAGIC When run against a multi-node cluster you can also check the metrics view to see if we are driver-bound or worker-bound
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # View the state store manager
# MAGIC
# MAGIC A streaming query may be stateful or stateless. It depends on the operation being performed. 
# MAGIC
# MAGIC Joins and aggregates typical require state management.
# MAGIC
# MAGIC
# MAGIC
# MAGIC It is a complicated topic, but in general the  use of  watermarks, or filters such as "where date - interval 1 day < now()" can help.
# MAGIC
# MAGIC Here is a blog post that discusses the basics.
# MAGIC
# MAGIC https://dataninjago.com/2022/07/16/spark-structured-streaming-deep-dive-6-stateful-operations/
# MAGIC
# MAGIC ### State Store Manager
# MAGIC
# MAGIC There is the default, and then there is an optimized manager that uses RocksDB
# MAGIC
# MAGIC In the following cells we will view the default, and then set to RocksDB

# COMMAND ----------

DefaultStateManager = spark.conf.get("spark.sql.streaming.stateStore.providerClass")

print(f"Default State Manager is {DefaultStateManager}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Set State Manager to RocksDB
# MAGIC
# MAGIC

# COMMAND ----------

spark.conf.set(
  "spark.sql.streaming.stateStore.providerClass",
  "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")

# COMMAND ----------

print(spark.conf.get("spark.sql.streaming.stateStore.providerClass"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Connect to the Database created by the table generator notebook

# COMMAND ----------

#####
# Extract the username, append a name to it
# And clean out special characters
#####
username = spark.sql("select current_user()").collect()[0][0]
#print(username)
database_name = f"{username}_intro_to_stream_monitoring"
#print(database_name)
database_name = (database_name.replace("+", "_").replace("@", "_").replace(".", "_"))
print(database_name)


# COMMAND ----------

spark.sql(f"use {database_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Verify the tables created by the table generator are present

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select count(*) from login_events;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select * from input_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from late_login_events;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) from late_error_events;

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from input_table limit 10;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Create a streaming dataframe that requires no state management

# COMMAND ----------

streaming_df = (spark.readStream.format("delta").table("input_table"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Use noop write to generate an output stream that requires no state management

# COMMAND ----------


simple_stream = streaming_df.writeStream.format("noop").queryName("NoTriggerIntervalNoState").start()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Expand the Streaming Dashboard
# MAGIC
# MAGIC
# MAGIC In this instance we are not approaching any limits, the incoming data is not large, but practice reviewing the metrics presented in the dashboard.
# MAGIC
# MAGIC Note Input and Processing rate
# MAGIC
# MAGIC Note Batch Duration
# MAGIC
# MAGIC Go to the spark UI for your cluster and choose the streaming tab, and review the metrics provided there 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Use Explain to view execution plan

# COMMAND ----------

simple_stream.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # View the dashboard
# MAGIC
# MAGIC Note Input and Processing rate
# MAGIC
# MAGIC Note Batch Duration
# MAGIC
# MAGIC Go to the spark UI for your cluster and choose the streaming tab, and review the metrics provided there

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Stop the Stream

# COMMAND ----------

simple_stream.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Add a trigger interval
# MAGIC
# MAGIC Using the proper sized trigger interval can be important to manage costs
# MAGIC
# MAGIC Trigger interval will also affect the details of the spark job that is generated per trigger
# MAGIC
# MAGIC Viewing the spark tasks in the spark ui for this example, and comparing it to the previous example might be informative

# COMMAND ----------

#streaming_df = (spark.readStream..format("delta").table("input_table"))
stream_with_trigger_interval = streaming_df.writeStream.trigger(processingTime='5 seconds').format("noop").queryName("TriggerInterval_5_sec").start()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # View the dashboard
# MAGIC
# MAGIC Note Input and Processing rate
# MAGIC
# MAGIC Note Batch Duration
# MAGIC
# MAGIC Go to the spark UI for your cluster and choose the streaming tab
# MAGIC
# MAGIC The table we are streaming from is updated every 5 seconds in a loop.
# MAGIC
# MAGIC A trigger interval that has fewer lookups for data that return with "no change detected" will be more efficient. 

# COMMAND ----------

stream_with_trigger_interval.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Create a windowed aggregation
# MAGIC
# MAGIC And use noop write to show a write that requires state management.
# MAGIC
# MAGIC By defining a windowed aggregation we force the streaming api to manage state, each time window may get events that arrive late, in this case we are reading from a source that is consistently on time, but that is not always the case. 
# MAGIC
# MAGIC We have tables available from the table generator notebook that have late arriving data. 
# MAGIC
# MAGIC We will demonstrate reading from those tables, and using watermarking to give spark the ability to ignore late arriving data. More or less the ability to "close" windows of our windowed aggregation
# MAGIC
# MAGIC We will also switch from output mode complete, to output mode append.
# MAGIC
# MAGIC Output mode append for a windowed aggregation will require that your streams are watermarked.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # First a basic windowed aggregation

# COMMAND ----------

from pyspark.sql.functions import window
windowed_df = (streaming_df.
               groupBy(window(streaming_df.event_time, "1 minute"),streaming_df.id)
               .count() 
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # clean it up a bit, 
# MAGIC
# MAGIC The window column is a struct, containing start, and end. 
# MAGIC
# MAGIC By Extracting the start, it makes it more sortable when displayed. 

# COMMAND ----------

final_df = (windowed_df.select("window.start", "id", "count"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Verify our state store is using RocksDB
# MAGIC
# MAGIC It is not likely that this is affecting performance for the small amount of data we are processing here. 
# MAGIC
# MAGIC But you could switch back to default and compare if you would like.
# MAGIC
# MAGIC See the default value printed in the beginning of the notebook.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SET spark.sql.streaming.stateStore.providerClass

# COMMAND ----------

windowed_stream = final_df.writeStream.outputMode("complete").format("noop").queryName("OutputMode_Complete").start()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC # What is different about the status monitor for the windowed aggregation?
# MAGIC
# MAGIC Hint look for the following <pre>***Mouse Over To View Hidden Text***</pre>
# MAGIC
# MAGIC <span style="color: white;">
# MAGIC </br>
# MAGIC There is a third graph showing aggregation state
# MAGIC </br>
# MAGIC
# MAGIC </span>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # What is aggregation state?
# MAGIC
# MAGIC In order to complete the windowed aggregate spark must track all timestamps received in case it gets another record for that window
# MAGIC
# MAGIC If aggregation state continues to grow as it tracks more records, at some point memory requirements will use all available memory.
# MAGIC
# MAGIC A useful check at this point would be to view the ganglia metrics.
# MAGIC
# MAGIC Do you see any process that is not a counter, that is using more resources with each batch? 
# MAGIC
# MAGIC Over time, if that continues at some point the driver will run out of memory.
# MAGIC
# MAGIC Also look at the heat map (only useful on a multi node cluster), you may see that the driver is under load, and a different instance type might be appropriate.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Scheduler Pools
# MAGIC
# MAGIC Structured Streaming makes use of fair scheduler pools to fairly allocate slots on the cluster.
# MAGIC
# MAGIC It can be tricky to get to this, because the pools are torn down after each job finishes.
# MAGIC
# MAGIC So if you see, service not available, that is expected.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Shuffle Partitions
# MAGIC
# MAGIC For the windowed aggregation a shuffle will be used to reorganize each tasks data so that the group by completed in the following stage
# MAGIC
# MAGIC Use the spark ui to see a stage with 200 shuffle partitions, and view the timeline and the task details.
# MAGIC
# MAGIC Note that many partitions are empty. 
# MAGIC
# MAGIC This is not an efficient use of resources. 
# MAGIC
# MAGIC Change spark.sql.shuffle.partitions to 4 for a single node cluster, 8 for a 2 node cluster. 

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "8")

# COMMAND ----------

windowed_stream.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Adding a watermark
# MAGIC
# MAGIC To best demo the watermark functionality we should start with a stream<->stream join against the two tables.
# MAGIC
# MAGIC One of the tables having late arriving data
# MAGIC
# MAGIC

# COMMAND ----------

on_time_stream = (spark.readStream.format("delta").table("error_events"))
late_stream = (spark.readStream.format("delta").table("late_error_events"))
union_stream = on_time_stream.unionAll(late_stream)

# COMMAND ----------

from pyspark.sql.functions import window
windowed_df = (union_stream.
               withWatermark("event_time", "10 minutes")
               .groupBy(window(union_stream.event_time, "1 minute"),union_stream.id)
               .count() 
               )

# COMMAND ----------

windowed_stream = windowed_df.writeStream.outputMode("complete").format("noop").start()

# COMMAND ----------

windowed_stream.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Switch the output mode to append, and note the change in aggregation state
# MAGIC
# MAGIC
# MAGIC Note that output mode "append" vs output mode "complete" have different requirements.
# MAGIC
# MAGIC Writing the windowed_stream that worked fine in output mode complete, will not work in output mode append
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Running the cell below will return an error

# COMMAND ----------

# windowed_stream_append = windowed_df.writeStream.outputMode("append").format("noop").start()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Fix the error by watermarking the union dataframe, and then write as append

# COMMAND ----------

on_time_stream_ww = (spark.readStream.format("delta").table("error_events"))
late_stream_ww = (spark.readStream.format("delta").table("late_error_events"))
union_stream_ww = on_time_stream_ww.withWatermark("event_time", "10 minutes").unionAll(late_stream_ww)

# COMMAND ----------

windowed_df_ww = (union_stream_ww.
               withWatermark("event_time", "10 minutes")
               .groupBy(window(union_stream_ww.event_time, "1 minute"),union_stream_ww.id)
               .count() 
               )

# COMMAND ----------

windowed_stream_ww = windowed_df_ww.writeStream.outputMode("append").format("noop").start()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # View the spark ui
# MAGIC
# MAGIC
# MAGIC Note the watermark information provided there
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Other stream tools
# MAGIC
# MAGIC View the output of
# MAGIC
# MAGIC explain

# COMMAND ----------

windowed_stream_ww.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Explore the other options available
# MAGIC
# MAGIC ```type windowed_stream_ww.<tab>```
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# windowed_stream_ww.

# COMMAND ----------

windowed_stream_ww.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Summary
# MAGIC
# MAGIC A streaming job is a collection of spark jobs
# MAGIC
# MAGIC Usual spark perfomance issues such as, partition size, spill, small files, shuffle are also important considerations for streaming and will have a great effect on performance
# MAGIC
# MAGIC
# MAGIC Streaming is also a special case. 
# MAGIC
# MAGIC Issues such as trigger interval, watermarking, stream-stream vs stream-static joins will affect performance
# MAGIC
# MAGIC Make sure to note on long running streams if some aspect, say state management, or total size when output mode is complete continue to increase. Any process that runs forever and continues to take up more resource will at some point create an issue. 
# MAGIC
# MAGIC Using different instance types for driver/workers may be needed in streaming. 
