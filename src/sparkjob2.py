import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, max, when, format_string, date_format, coalesce, min, lit
from pyspark.sql.types import TimestampType

TEAM_NUMBER="23"
NET_IN_THRESHOLD=3205.23
DISK_IO_THRESHOLD=1534.99

current_directory=os.getcwd()
net_data_path=f"file://{current_directory}/net_data.csv"
disk_data_path=f"file://{current_directory}/disk_data.csv"

spark=SparkSession.builder.appName(f"Team{TEAM_NUMBER}_NET_DISK_Final_Corrected").getOrCreate()

net_df=spark.read.csv(net_data_path, header=True, inferSchema=True).withColumn("timestamp", col("ts").cast(TimestampType()))
disk_df=spark.read.csv(disk_data_path, header=True, inferSchema=True).withColumn("timestamp", col("ts").cast(TimestampType()))

min_ts_net=net_df.agg(min("timestamp")).collect()[0][0]
min_ts_disk=disk_df.agg(min("timestamp")).collect()[0][0]

if min_ts_net<min_ts_disk:
    first_event_time=min_ts_net
else:
    first_event_time=min_ts_disk

windowed_net=net_df.groupBy("server_id", window("timestamp", "30 seconds", "10 seconds")).agg(max("net_in").alias("max_net_in"))
windowed_disk=disk_df.groupBy("server_id", window("timestamp", "30 seconds", "10 seconds")).agg(max("disk_io").alias("max_disk_io"))
combined_window_df=windowed_net.join(windowed_disk, ["server_id", "window"], "full_outer")

alerts_df=combined_window_df.withColumn("max_net_in_safe", coalesce(col("max_net_in"), lit(0.0)))
alerts_df=alerts_df.withColumn("max_disk_io_safe", coalesce(col("max_disk_io"), lit(0.0)))

alerts_df=alerts_df.withColumn("alert",
    when((col("max_net_in_safe")>NET_IN_THRESHOLD) & (col("max_disk_io_safe")>DISK_IO_THRESHOLD), "Network flood + Disk thrash suspected")
    .when((col("max_net_in_safe") >NET_IN_THRESHOLD), "Possible DDoS")
    .when((col("max_disk_io_safe")>DISK_IO_THRESHOLD), "Disk thrash suspected")
    .otherwise(None)
)

corrected_df=alerts_df.filter(col("window.start") >= first_event_time)

final_output_df = corrected_df \
    .withColumn("window_start_fmt", date_format(col("window.start"), "HH:mm:ss")) \
    .withColumn("window_end_fmt", date_format(col("window.end"), "HH:mm:ss")) \
    .withColumn("max_net_in_fmt", format_string("%.2f", col("max_net_in"))) \
    .withColumn("max_disk_io_fmt", format_string("%.2f", col("max_disk_io"))) \
    .select(
        col("server_id"),
        col("window_start_fmt").alias("window_start"),
        col("window_end_fmt").alias("window_end"),
        col("max_net_in_fmt").alias("max_net_in"),
        col("max_disk_io_fmt").alias("max_disk_io"),
        col("alert")
    ).orderBy("server_id", "window_start")

output_filename = f"team_{TEAM_NUMBER}_NET_DISK_submission"
final_output_df.coalesce(1).write.csv(output_filename, header=True, mode="overwrite")

print(f"Submission file created with {final_output_df.count()} rows.")
spark.stop()
