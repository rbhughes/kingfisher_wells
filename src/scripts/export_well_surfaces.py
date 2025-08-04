from pyspark.sql import functions as F

export_path = f"/Volumes/geodata/staging/export/"

df = spark.table("geodata.gold.well_surface_locations")

df = (
    df.withColumn("geom_ENV", F.expr("ST_AsText(geom_ENV)"))
    .withColumn("geom_OCC", F.expr("ST_AsText(geom_OCC)"))
    .withColumn("geom_SP", F.expr("ST_AsText(geom_SP)"))
)

df.write.mode("overwrite").parquet(export_path)
