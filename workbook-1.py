# Databricks notebook source
# MAGIC %md 
# MAGIC # Import

# COMMAND ----------

import json
from os.path import expanduser
from pyspark.sql import SparkSession
from os import environ, getenv
from time import strftime, gmtime
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Row
from pyspark.sql import Row
from pyspark.sql.functions import rand, expr
from pyspark.sql.types import StringType, TimestampType, StructType, StructField, BooleanType, DecimalType, DateType

# COMMAND ----------

# MAGIC %md 
# MAGIC # Initiate the dataframe

# COMMAND ----------

base_manual_file_input_schema = StructType()
base_manual_file_input_schema.add("adjustment_id", StringType(), True)
base_manual_file_input_schema.add("adjustment_type", StringType(), True)
base_manual_file_input_schema.add("adjustment_start_date", StringType(), True)
base_manual_file_input_schema.add("adjustment_end_date", StringType(), True)
base_manual_file_input_schema.add("adjusted_by", StringType(), True)
base_manual_file_input_schema.add("adjustment_reason", StringType(), True)
base_manual_file_input_schema.add("date_key", StringType(), True)
base_manual_file_input_schema.add("invoice_line_id", StringType(), True)
base_manual_file_input_schema.add("legal_entity_key_hash", StringType(), True)
base_manual_file_input_schema.add("account_number", StringType(), True)
base_manual_file_input_schema.add("contract_number", StringType(), True)
base_manual_file_input_schema.add("job_id", StringType(), True)
base_manual_file_input_schema.add("order_item_number", StringType(), True)
base_manual_file_input_schema.add("order_number", StringType(), True)
base_manual_file_input_schema.add("product_code", StringType(), True)
base_manual_file_input_schema.add("recognition_type", StringType(), True)
base_manual_file_input_schema.add("usage_number", StringType(), True)
base_manual_file_input_schema.add("commissionable_revenue_local_currency", StringType(), True)
base_manual_file_input_schema.add("made_foc", StringType(), True)
base_manual_file_input_schema.add("contract_owner_name", StringType(), True)
base_manual_file_input_schema.add("all_other_fields", StringType(), True)
base_manual_file_input_schema.add("new_column_1", StringType(), True)
base_manual_file_input_schema.add("ad_type", StringType(), True)
base_manual_file_input_schema.add("contract_group", StringType(), True)
base_manual_file_input_schema.add("contract_rejection_status", StringType(), True)
base_manual_file_input_schema.add("contract_type", StringType(), True)
base_manual_file_input_schema.add("invoice_number", StringType(), True)
base_manual_file_input_schema.add("is_autoinclusion", StringType(), True)
base_manual_file_input_schema.add("is_chargeable_revenue", StringType(), True)
base_manual_file_input_schema.add("is_free_of_charge", StringType(), True)
base_manual_file_input_schema.add("legacy_contract_id", StringType(), True)
base_manual_file_input_schema.add("pricing_group_name", StringType(), True)
base_manual_file_input_schema.add("product_group", StringType(), True)
base_manual_file_input_schema.add("product_name", StringType(), True)
base_manual_file_input_schema.add("product_sub_group", StringType(), True)
base_manual_file_input_schema.add("purchase_type", StringType(), True)
base_manual_file_input_schema.add("sf_order_item_type_key", StringType(), True)
base_manual_file_input_schema.add("ss_free_flag", StringType(), True)
base_manual_file_input_schema.add("transaction_currency_code", StringType(), True)
base_manual_file_input_schema.add("ad_type_at_qtr_end", StringType(), True)
base_manual_file_input_schema.add("is_foc_qtr_end", StringType(), True)
base_manual_file_input_schema.add("ctrl_source_system", StringType(), True)
base_manual_file_input_schema.add("ctrl_load_date", StringType(), True)

base_manual_file_input_data = [
    Row(
        adjustment_id="1",
        adjustment_type="Add/Overwrite",
        adjustment_start_date="20230125",
        adjustment_end_date="20240125",
        adjusted_by="SomeOne",
        adjustment_reason="This is a sample adjustment reason",
        date_key="20230126",
        invoice_line_id="(Can be null)",
        legal_entity_key_hash="key_hash",
        account_number="1234567",
        contract_number="9876542",
        job_id="(Can be null)",
        order_item_number="11223344",
        order_number="1234",
        product_code="PROD-CODE",
        recognition_type="Job Ad Consumption",
        usage_number="(Can be null)",
        commissionable_revenue_local_currency="99",
        made_foc="FALSE",
        contract_owner_name="Matt Damon",
        all_other_fields="Sample Adjustment",
        new_column_1=None,
        ad_type="sample",
        contract_group="group_test",
        contract_rejection_status="NONE",
        contract_type="TEST",
        invoice_number="11223344",
        is_autoinclusion="FALSE",
        is_chargeable_revenue="TRUE",
        is_free_of_charge="FALSE",
        legacy_contract_id="11114444",
        pricing_group_name="pricing_group_test",
        product_group="product_group_test",
        product_name="test",
        product_sub_group="test",
        purchase_type="test",
        sf_order_item_type_key="test",
        ss_free_flag="TRUE",
        transaction_currency_code="test",
        ad_type_at_qtr_end="test",
        is_foc_qtr_end="TRUE",
        ctrl_source_system="GBL",
        ctrl_load_date="27:37.4",
    )
]

base_manual_file_df = spark.createDataFrame(schema=base_manual_file_input_schema, data=base_manual_file_input_data)

base_manual_file_df.printSchema()

# COMMAND ----------

base_manual_file_df.show(n=1, vertical=True, truncate=False)

# COMMAND ----------


