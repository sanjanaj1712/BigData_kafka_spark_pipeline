import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, when, format_number, date_format, coalesce, min
from pyspark.sql.types import TimestampType

TEAM_NUMBER="23"
CPU_THRESHOLD=76.44
MEM_THRESHOLD=80.07

current_directory=os.getcwd()
cpu_data_path=f"file://{current_directory}/cpu_data.csv"
mem_data_path=f"file://{current_directory}/mem_data.csv"

spark=SparkSession.builder.appName(f"Team{TEAM_NUMBER}_CPU_MEM_Final").getOrCreate()

cpu_df = spark.read.csv(cpu_data_path, header=True, inferSchema=True).withColumn("timestamp", col("ts").cast(TimestampType()))
mem_df = spark.read.csv(mem_data_path, header=True, inferSchema=True).withColumn("timestamp", col("ts").cast(TimestampType()))

min_ts_cpu = cpu_df.agg(min("timestamp")).collect()[0][0]
min_ts_mem = mem_df.agg(min("timestamp")).collect()[0][0]

if min_ts_cpu < min_ts_mem:
    first_event_time = min_ts_cpu
else:
    first_event_time = min_ts_mem

windowed_cpu = cpu_df.groupBy("server_id", window("timestamp", "30 seconds", "10 seconds")).agg(avg("cpu_pct").alias("avg_cpu"))
windowed_mem = mem_df.groupBy("server_id", window("timestamp", "30 seconds", "10 seconds")).agg(avg("mem_pct").alias("avg_mem"))
combined_window_df = windowed_cpu.join(windowed_mem, ["server_id", "window"], "full_outer")

alerts_df=combined_window_df.withColumn("avg_cpu_safe", coalesce(col("avg_cpu"), col("avg_cpu") * 0))
alerts_df=alerts_df.withColumn("avg_mem_safe", coalesce(col("avg_mem"), col("avg_mem") * 0))

alerts_df=alerts_df.withColumn("alert",
    when((col("avg_cpu_safe")>CPU_THRESHOLD) & (col("avg_mem_safe")>MEM_THRESHOLD), "High CPU + Memory stress")
    .when((col("avg_cpu_safe")>CPU_THRESHOLD), "CPU spike suspected")
    .when((col("avg_mem_safe")>MEM_THRESHOLD), "Memory saturation suspected")
    .otherwise(None)
)

corrected_df=alerts_df.filter(col("window.start") >= first_event_time)

final_df=corrected_df.select(
    "server_id",
    date_format(col("window.start"), "HH:mm:ss").alias("window_start"),
    date_format(col("window.end"), "HH:mm:ss").alias("window_end"),
    format_number("avg_cpu", 2).alias("avg_cpu"),
    format_number("avg_mem", 2).alias("avg_mem"),
    "alert"
).orderBy("server_id","window_start")

output_filename=f"team_{TEAM_NUMBER}_CPU_MEM_submission"
final_df.coalesce(1).write.csv(output_filename,header=True,mode="overwrite")

print(f"Submission file created with {final_df.count()} rows.")
spark.stop()
