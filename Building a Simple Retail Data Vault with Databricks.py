# 1. Define Paths and File Names:
data_path = "/mnt/data/retail"
source_file = "sales.csv"

#sample csv records:
customer_id, product_id,store_id,transaction_date,sale_amount,loyalty_tier
1001,        254,       3,        2024-02-15,      12.99,      Bronze
1001,        302,       3,        2024-02-15,      24.50,      Bronze
1002,        117,       1,        2024-02-17,      39.99,      Silver
           
# customer_id: Unique identifier for the customer.
# product_id: Unique identifier for the product.
# store_id: Unique identifier for the store.
# transaction_date: Date of the sale.
# sale_amount: Total amount of the sale.
# loyalty_tier: Customer's loyalty program tier (Bronze, Silver, Gold).

#2. Load Data from CSV:
# Read the CSV file into a Spark DataFrame
sales_df = spark.read.csv(f"{data_path}/{source_file}", header=True, inferSchema=True)
# Preview the data
sales_df.show(5)

#3. Create Hub (Customer Dimension):
# Select relevant columns for the customer hub
customer_hub_df = sales_df.select("customer_id", "customer_name", "loyalty_tier")

# Create a unique identifier (e.g., monotonically increasing ID)
from pyspark.sql.functions import monotonically_increasing_id

customer_hub_df = customer_hub_df.withColumn(
    "customer_sk", monotonically_increasing_id()
)
# Define the schema for the hub table
customer_hub_schema = customer_hub_df.schema
# Write the hub data to Delta table format
customer_hub_df.write.format("delta").saveAsTable("retail_hub_customer")


# Select relevant columns for the sales fact table
sales_fact_df = sales_df.select(
    "customer_id", "product_id", "store_id", "transaction_date", "sale_amount"
)

#4. Create Satellite (Sales Fact):
# Link the sales fact to the customer hub using the foreign key
sales_fact_df = sales_fact_df.join(
    spark.table("retail_hub_customer"), on="customer_id", how="left"
)

# Define the schema for the fact table
sales_fact_schema = sales_fact_df.schema

# Write the fact data to Delta table format
sales_fact_df.write.format("delta").saveAsTable("retail_sat_sales")


