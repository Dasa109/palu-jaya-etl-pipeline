#!/usr/bin/env python
# coding: utf-8

# In[163]:


from pyspark.sql import functions as f
from pyspark.sql import SparkSession


# In[164]:


spark = (SparkSession.builder\
        .master("spark://spark-master:7077")\
        .appName("palu-jaya-transformation-njb")\
        .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")\
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider")\
        .config("spark.cores.max", "8")\
        .getOrCreate())


# In[165]:


from datetime import datetime
now = datetime.now().strftime("%Y-%m-%d")

s3_path = f"s3a://palu-jaya-data-storage-910162731301-ap-southeast-1-an/raw/raw-njb/data-njb-{now}.parquet"


# In[166]:


raw_df = spark.read.parquet(s3_path)


# In[167]:


new_column_name = ["no", "nota", "tanggal", "no_pkb", "jasa_bengkel", "tipe_bayar", "jumlah"
                   , "disc_%", "disc_rp", "total"]

old_columns = raw_df.columns
for x in range(len(new_column_name)):
    raw_df = raw_df.withColumnRenamed(old_columns[x], new_column_name[x])


# In[168]:


bronze_df = raw_df.dropna()


# In[169]:


bronze_df = bronze_df.withColumn("jasa_bengkel", f.regexp_replace("jasa_bengkel", r"^PAKET\b", ""))


# In[170]:


bronze_df = bronze_df.withColumn("jasa_bengkel", f.when(f.col("jasa_bengkel").contains("KPB"), 
                                                        f.substring(f.trim(f.col("jasa_bengkel")), 1, 4))
                                 .otherwise(f.col("jasa_bengkel")))


# In[171]:


fix_word_mapping = {
    "STEL" : "SETEL",
    "METIC" : "MATIC",
    "METIK" : "MATIC",
    "CU" : "CUB",
    "JASA COLTER" : "KOLTER",
    "GANTI BAN LUAR MELAKANG MATIC DAN CUB" : "PASANG BAN LUAR BELAKANG",
    "RIM" : "REM",
    "HEAVY REPAIR LENGKAP" : "HEAVY REPAIR"
}

for key, correct in fix_word_mapping.items():
    bronze_df = bronze_df.withColumn("jasa_bengkel", f.regexp_replace("jasa_bengkel", rf"\b{key}\b", correct))


# In[172]:


silver_df = bronze_df.drop("no").drop("tipe_bayar")


# In[173]:


silver_df = silver_df.withColumn("tanggal", f.regexp_replace("tanggal", r"/", "-")).withColumn("tanggal", f.concat(f.col("tanggal"), f.lit("-"), f.regexp_extract(f.col("nota"), r'20\d{2}', 0)))


# In[174]:


silver_df = silver_df.withColumn("tanggal", f.to_date(f.col("tanggal"), "dd-MM-yyyy"))


# In[175]:


cols = ["nota", "no_pkb", "jasa_bengkel"]

for col in cols:
    silver_df = silver_df.withColumn(col, f.upper(col))


# In[176]:


for col in cols:
    silver_df = silver_df.withColumn(col, f.trim(col))


# In[177]:


from pyspark.sql.types import StringType, DateType, LongType, IntegerType

dtype_mapping = {
    "nota" : StringType(),
    "tanggal" : DateType(),
    "no_pkb" : StringType(),
    "jasa_bengkel" : StringType(),
    "jumlah" : LongType(),
    "disc_%" : IntegerType(),
    "disc_rp" : IntegerType(),
    "total" : LongType()
}

for key, type in dtype_mapping.items():
    silver_df = silver_df.withColumn(key, f.col(key).cast(type))


# In[ ]:


store_path = f"s3a://palu-jaya-data-storage-910162731301-ap-southeast-1-an/gold/gold-njb-data/gold-njb-data-{now}"

silver_df.coalesce(1).write.parquet(store_path, mode="overwrite")

