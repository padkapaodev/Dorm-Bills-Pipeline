from delta.tables import DeltaTable
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# merge funtions for each table

def merge_into_table(df, keys, target_table):
    merge_condition = " and ".join([f"t.{k} = s.{k}" for k in keys])
    dt = DeltaTable.forName(spark, f"project_catalog.{target_table}")

    (
        dt.alias("t")
        .merge(df.alias("s"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


# table management tools for each layer

def delete_bronze():
    spark.sql(" drop table if exists project_catalog.bronze_layer ")


def init_bronze():
    (
        spark.sql(""" create or replace table project_catalog.bronze_layer (
        bill_id string,
        room_id int, 
        bill_date date, 
        elec_meter decimal(10,2), 
        water_meter decimal(10,2), 
        note string, 
        created_at timestamp, 
        _processing_time timestamp)""")
    )

def reset_bronze():
    delete_bronze()
    init_bronze()

def delete_silver():
    spark.sql(" drop table if exists project_catalog.silver_layer ")


def init_silver():
    (
        spark.sql(""" create or replace table project_catalog.silver_layer (
        bill_id string,
        room_id int,
        bill_date date,
        elec_unit_used numeric (10,2),
        water_unit_used numeric (10,2),
        rent numeric (10,2),
        elec_charge numeric (10,2),
        water_charge numeric (10,2),
        total_charge numeric (10,2))""")
    )

def reset_silver():
    delete_silver()
    init_silver()

def delete_gold():
    spark.sql(" drop table if exists project_catalog.gold_layer ")


def init_gold():
    (
        spark.sql(""" create or replace table project_catalog.gold_layer (
        bill_id string,
        room_id int,
        bill_date date,
        elec_unit_used numeric (10,2),
        water_unit_used numeric (10,2),
        rent numeric (10,2),
        elec_charge numeric (10,2),
        water_charge numeric (10,2),
        total_charge numeric (10,2))""")
    )

def reset_gold():
    delete_gold()
    init_gold()


def delete_sum_revenue():
    spark.sql(" drop table if exists project_catalog.sum_revenue ")

def init_sum_revenue():
    (
        spark.sql(""" create or replace table project_catalog.sum_revenue(
        bill_date date,
        monthly_revenue numeric (10,2),
        avg_revenue_per_room numeric (10,2)
    )""")
    )

def reset_sum_revenue():
    delete_sum_revenue()
    init_sum_revenue()
