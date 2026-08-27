#!/usr/bin/env python
# coding: utf-8

# In[3]:


from pyspark.sql import functions as f
from pyspark.sql import SparkSession


# In[4]:


spark = (
    SparkSession.builder
    .master("spark://spark-master:7077")
    .appName("palu-jaya-data-transformation-remainder")
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider")
    .config("spark.hadoop.fs.s3a.endpoint.region", "ap-southeast-1")\
    .config("spark.cores.max", "8")
    .getOrCreate()
)


# In[5]:


from datetime import datetime
now = datetime.now().strftime("%Y-%m-%d")

s3_path_remainder = f"s3a://palu-jaya-data-storage-910162731301-ap-southeast-1-an/raw/raw-remainder/data-remainder-{now}.parquet"


# In[6]:


df = spark.read.parquet(s3_path_remainder)


# In[ ]:


# In[8]:


bronze_remainder = df.select(f.col("NAMA").alias("nama"), f.col("NO TELP").alias("no_telp"), f.col("TIPE MOTOR").alias("motor"), f.col("NOPOL").alias("nopol"), f.col("SERVICE TERAKHIR").alias("servis_terakhir"), f.col("KM TERAKHIR").alias("km_terakhir"), f.col("NAMA MEKANIK").alias("mekanik"))


# In[ ]:


# In[10]:


silver_remainder = bronze_remainder.dropna(subset=["no_telp"]).dropDuplicates(subset=["nama", "nopol"])


# In[ ]:



# In[ ]:


# silver_remainder.printSchema()


# In[13]:


silver_remainder = silver_remainder.select([f.upper(f.col(x)).alias(x) for x in bronze_remainder.columns])


# In[ ]:


# In[ ]:


silver_remainder = silver_remainder.withColumn("no_telp", f.when(f.col("no_telp").contains("/"), f.regexp_replace(f.col("no_telp"), r"^.*?/", "")).otherwise(f.col("no_telp")))


# In[16]:


silver_remainder_invalid = silver_remainder.filter((f.length("no_telp") > 14) | (f.length("no_telp" ) < 10) | (f.col("no_telp").startswith("08") == False) & (f.col("no_telp").startswith("8") == False))


# In[ ]:



# In[18]:


silver_remainder_valid = silver_remainder.join(other=silver_remainder_invalid, on=[silver_remainder_invalid["nopol"] == silver_remainder["nopol"]],  how="left_outer").select(silver_remainder["*"])


# In[ ]:



# In[ ]:




# In[21]:


silver_remainder_valid = silver_remainder_valid.withColumn("no_telp", f.when(f.col("no_telp").startswith("8"), f.concat(f.lit("0"), f.col("no_telp"))).otherwise(f.col("no_telp")))


# In[ ]:




# In[23]:


mekanik_duplicates_mapping = {
    "andiva fais" : "ANDIVA",
    "AHMAD DANIL AINUL" : "AHMAD DANIL"
}

for key, correct in mekanik_duplicates_mapping.items():
    silver_remainder_valid = silver_remainder_valid.withColumn("mekanik", f.when(f.col("mekanik") == key, correct).otherwise(f.col("mekanik")))


# In[24]:


from pyspark.sql.types import StringType, IntegerType, DateType

cast_type_mapping = {
    "nama" : StringType(),
    "no_telp" : StringType(),
    "motor" : StringType(),
    "nopol" : StringType(),
    "servis_terakhir" : DateType(),
    "km_terakhir" : IntegerType(),
    "mekanik" : StringType()
}

silver_remainder_valid = silver_remainder_valid.select([f.col(key).cast(types) for key, types in cast_type_mapping.items()])


# In[ ]:




# In[26]:


gold_layer = silver_remainder_valid


# In[29]:


store_path = f"s3a://palu-jaya-data-storage-910162731301-ap-southeast-1-an/gold/gold-remainder/gold-remainder-{now}"
gold_layer.coalesce(1).write.parquet(store_path, mode="overwrite")

spark.stop()