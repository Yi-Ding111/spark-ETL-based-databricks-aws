# Databricks notebook source
#from pyspark.sql import SparkSession

#spark=SparkSession.builder.appName('transformation').getOrCreate()



# COMMAND ----------

spark

# COMMAND ----------

# MAGIC %fs
# MAGIC ls 

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt"

# COMMAND ----------

'''
#read IAM user kay value pair for connecting aws account

file_location="dbfs:/FileStore/tables/aws_keys/aws_key.csv"
file_type="csv"
first_row_header="true"
delimiter=','

AWS_IAM_df=spark.read.format(file_type) \
  .option("header", first_row_header) \
  .option("sep", delimiter) \
  .load(file_location)




#collect key value pair strings
IAM_ID=AWS_IAM_df.select("IAM_ID").collect()[0].IAM_ID
IAM_SECRET=AWS_IAM_df.select('IAM_SECRET').collect()[0].IAM_SECRET
#print(IAM_ID)
#print(IAM_SECRET)

#encode secret
IAM_SECRET_encode=urllib.parse.quote(IAM_SECRET,"")
#print(IAM_SECRET_encode)




  
aws_s3_bucket="databricks-yiding"
mount_name="/mnt/databricks-yiding"
s3_location="s3n://{0}:{1}@{2}".format(IAM_ID,IAM_SECRET_encode,aws_s3_bucket)
dbutils.fs.mount(s3_location,mount_name)
#dbutils.fs.rm('dbfs:/mnt/databricks-yiding/',True)


'''

# COMMAND ----------


#mount s3 buckt on dbfs
from pyspark.sql.functions import *
import urllib

file_location="dbfs:/FileStore/tables/aws_keys/aws_key.csv"
file_type="csv"
first_row_header="true"
delimiter=','

AWS_IAM_df=spark.read.format(file_type) \
  .option("header", first_row_header) \
  .option("sep", delimiter) \
  .load(file_location)


#collect key value pair strings
IAM_ID=AWS_IAM_df.select("IAM_ID").collect()[0].IAM_ID
IAM_SECRET=AWS_IAM_df.select('IAM_SECRET').collect()[0].IAM_SECRET
#print(IAM_ID)
#print(IAM_SECRET)

#encode secret
IAM_SECRET_encode=urllib.parse.quote(IAM_SECRET,"")
#print(IAM_SECRET_encode)


aws_s3_bucket='imba-yiding'
mount_name='/mnt/imba-yiding'
s3_location='s3n://{0}:{1}@{2}'.format(IAM_ID,IAM_SECRET_encode,aws_s3_bucket)
dbutils.fs.mount(s3_location,mount_name)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt"

# COMMAND ----------

# MAGIC %fs
# MAGIC ls '/mnt/imba-yiding/data/'

# COMMAND ----------

#read data from mounted s3 into dataframe
order_df=spark.read.option('header','true').csv('/mnt/imba-yiding/data/orders/',inferSchema=True)
product_df=spark.read.option('header','true').csv('/mnt/imba-yiding/data/products/',inferSchema=True)
order_product_df=spark.read.option('header','true').csv('/mnt/imba-yiding/data/order_products/',inferSchema=True)
department_df=spark.read.option('header','true').csv('/mnt/imba-yiding/data/departments/',inferSchema=True)
aisles_df=spark.read.option('header','true').csv('/mnt/imba-yiding/data/aisles/',inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #PREVIEW DATA

# COMMAND ----------

order_df.show()

# COMMAND ----------

order_df.printSchema()

# COMMAND ----------

order_df.columns

# COMMAND ----------

order_product_df.show()

# COMMAND ----------

order_product_df.columns

# COMMAND ----------

aisles_df.show(5)

# COMMAND ----------

product_df.show(5)

# COMMAND ----------

department_df.show(5)

# COMMAND ----------

#create temp view to do sql query
order_df.createOrReplaceTempView('order_table')
order_product_df.createOrReplaceTempView('order_product_table')
department_df.createOrReplaceTempView('department_table')
product_df.createOrReplaceTempView('product_table')
aisles_df.createOrReplaceTempView('aisles_table')

# COMMAND ----------

# MAGIC %md
# MAGIC #Transformation

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ot.*, pt.product_id, pt.add_to_cart_order, pt.reordered
# MAGIC FROM order_table ot
# MAGIC JOIN order_product_table pt
# MAGIC ON ot.order_id = pt.order_id
# MAGIC WHERE ot.eval_set = 'prior'

# COMMAND ----------

#collect necessary data together
order_products_prior=spark.sql("""
SELECT ot.*, pt.product_id, pt.add_to_cart_order, pt.reordered
FROM order_table ot
JOIN order_product_table pt
ON ot.order_id = pt.order_id
WHERE ot.eval_set = 'prior'
""")
order_products_prior.show()

# COMMAND ----------

user_features_1=spark.sql("""
SELECT user_id,
Max(order_number) AS user_orders,
Sum(days_since_prior_order) AS user_period,
Avg(days_since_prior_order) AS user_mean_days_since_prior
FROM order_table 
GROUP BY user_id 
""")

# COMMAND ----------

#calculate the total number of products, total number of distinct products, and user reorder ratio
#(number of reordered = 1 divided by number of order_number > 1)

order_products_prior.createOrReplaceTempView('order_products_prior_table')

user_features_2=spark.sql("""
SELECT user_id,Count(*) AS user_total_products, Count(DISTINCT product_id) AS user_distinct_products,
Sum(CASE WHEN reordered = 1 THEN 1 ELSE 0 END) / Cast(Sum(CASE WHEN order_number > 1 THEN 1 ELSE 0 END) AS DOUBLE) AS user_reorder_ratio
FROM order_products_prior_table 
GROUP BY user_id
""")

# COMMAND ----------

#for each user and product, calculate the total number of orders, minimum order_number, maximum order_number and average add_to_cart_order.

up_features=spark.sql("""
select user_id, product_id, count(*) AS up_orders, Min(order_number) AS up_first_order, 
Max(order_number) as up_last_order, Avg(add_to_cart_order) As up_average_cart_position
from order_products_prior_table
GROUP BY user_id,product_id
""")

# COMMAND ----------

#calculate the sequence of product purchase for each user
#for each product, calculate the count, sum of reordered, count of product_seq_time = 1 and count of product_seq_time = 2.

prd_features=spark.sql("""
SELECT product_id,Count(*) AS prod_orders,Sum(reordered) AS prod_reorders,
Sum(CASE WHEN product_seq_time = 1 THEN 1 ELSE 0 END) AS prod_first_orders, Sum(CASE WHEN product_seq_time = 2 THEN 1 ELSE 0 END) AS prod_second_orders
FROM (
    SELECT *, Rank()OVER (partition BY user_id, product_id ORDER BY order_number) AS product_seq_time
    FROM order_products_prior_table
    )
GROUP BY product_id
""")

# COMMAND ----------

#persist some RDD dataframe for mutiple use

from pyspark import StorageLevel
prd_features.persist(StorageLevel.MEMORY_AND_DISK)
user_features_1.persist(StorageLevel.MEMORY_AND_DISK)
user_features_2.persist(StorageLevel.MEMORY_AND_DISK)
up_features.persist(StorageLevel.MEMORY_AND_DISK)

# COMMAND ----------

# MAGIC %md
# MAGIC #join all dataframes together to get the final transformed data

# COMMAND ----------

user_features_1.show(5)
user_features_2.show(5)

# COMMAND ----------

#join user1 and user2 with key user_id, inner join

users = user_features_1.join(user_features_2,'user_id', 'inner')
users.show(5)

# COMMAND ----------

#join up_features prd_features and users together

final_df=(up_features.join(users,'user_id','inner')).join(prd_features,'product_id','inner')
final_df.show(5)

# COMMAND ----------

final_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #put final data to data lake curated stage

# COMMAND ----------

final_df.repartition(1).write.format('csv') \
.option('header','true') \
.save('/mnt/imba-yiding/curated/version1')

# COMMAND ----------

# release memory
prd_features.unpersist()
user_features_1.unpersist()
user_features_2.unpersist()
up_features.unpersist()


# COMMAND ----------


