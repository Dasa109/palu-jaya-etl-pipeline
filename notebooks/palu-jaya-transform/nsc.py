#!/usr/bin/env python
# coding: utf-8

# In[262]:


from pyspark.sql import functions as f
from pyspark.sql import SparkSession 


# In[263]:


spark = (SparkSession.builder\
        .master("spark://spark-master:7077")\
        .appName("palu-jaya-transformation-nsc")\
        .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")\
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider")\
        .config("spark.cores.max", "8")\
        .getOrCreate())


# In[ ]:


from datetime import datetime

now = datetime.now().strftime("%Y-%m-%d")

nsc_s3_path = f"s3a://palu-jaya-data-storage-910162731301-ap-southeast-1-an/raw/raw-nsc/data-nsc-{now}.parquet"


# In[ ]:


raw_df = spark.read.parquet(nsc_s3_path)


# In[266]:


new_column_name = ["no", "nota", "tanggal", "no_pkb_or_cust", "kode_part", "suku_cadang", "tipe_bayar", "qty", "harga", "disc_%", "disc_rp", "total"]
old_columns = raw_df.columns
for x in range(len(old_columns)):
    raw_df = raw_df.withColumnRenamed(old_columns[x], new_column_name[x])


# In[268]:


bronze_df = raw_df.withColumn("tanggal", f.concat(f.regexp_replace(f.col("tanggal"), r'/', "-"), f.lit("-"), f.regexp_extract(f.col("nota"), r'20\d{2}', 0)))


# In[ ]:


import pyspark.pandas as ps
proper_sparepart_name_path = "/opt/public/proper_sparepart_name.xlsx"

pd_df = ps.read_excel(proper_sparepart_name_path)
proper_sparepart_name_df = pd_df.to_spark()
proper_sparepart_name_df = proper_sparepart_name_df.withColumn("Kode", f.regexp_replace(f.col("Kode"), r'-', ''))


# In[271]:


bronze_df = bronze_df.join(proper_sparepart_name_df, on=[bronze_df["kode_part"] == proper_sparepart_name_df["Kode"]], how="left_outer")\
         .withColumn("suku_cadang", f.when(f.isnull(proper_sparepart_name_df["Nama"]), bronze_df["suku_cadang"])\
                    .otherwise(proper_sparepart_name_df["Nama"]))\
         .select(bronze_df.columns)


# In[273]:


sparepart_names = [
    "BRAKE SHOE",
    "BELT DRIVE",
    "PIECE SET SLIDE",
    "OIL SEAL",
    "ROLLER WEIGHT",
    "ELEMENT COMP",
    "CYLINDER SET MASTER",
    "PAD SET",
    "BEARING BALL",
    "BATTERY",
    "BULB",
    "CABLE COMP",
    "SPARK PLUG",
    "OIL SHOCK",
    "GASKET",
    "WEIGHT SET CLUTCH",
    "TIRE REAR",
    "TIRE FRONT",
    "SWITCH UNIT",
    "OLI",
    "SCOOTER GEAR OIL",
    "BRAKE FLUID",
    "CVT GREASE",
    "COOLANT",
]


# In[274]:


from pyspark.sql.functions import pandas_udf, PandasUDFType
from thefuzz import process, fuzz
from pyspark.sql.types import ArrayType, StringType, IntegerType
import pandas as pd

@pandas_udf("match string, score int")
def sparepart_grouping(data: pd.Series) -> pd.DataFrame:
    def match(x):
        if x is None:
            return ("None", 0)
        res = process.extractOne(x, sparepart_names, scorer=fuzz.partial_ratio)
        return res
    res_match = data.apply(match)
    return pd.DataFrame(res_match.tolist(), columns=["match", "score"])


# In[275]:


silver_df = bronze_df.withColumn("match", sparepart_grouping(f.col("suku_cadang")))\
         .withColumn("sparepart_group", f.when(f.col("match.score") > 75, f.col("match.match"))\
         .otherwise("OTHERS"))\
         .drop("match")\
         .drop("tipe_bayar")


# In[277]:


oil_mapping = ["MPX",'SPX']

for oil in oil_mapping:
    silver_df = silver_df.withColumn("sparepart_group", f.when(f.col("suku_cadang").contains(oil), "OIL").otherwise(f.col("sparepart_group")))


# In[281]:


silver_df = silver_df.dropna()


# In[283]:


silver_df =silver_df.withColumn("tanggal", f.to_date(f.col("tanggal"), "d-MM-yyyy"))


# In[284]:


from pyspark.sql.types import StringType, IntegerType, LongType, DateType

dtypes_mapping = {
    "no" : IntegerType(),
    "nota" : StringType(),
    "tanggal" : DateType(),
    "no_pkb_or_cust" : StringType(),
    "kode_part" : StringType(),
    "suku_cadang" : StringType(),
    "qty" : IntegerType(),
    "harga" : LongType(),
    "disc_%" :IntegerType(),
    "disc_rp" : IntegerType(),
    "total" : LongType(),
    "sparepart_group" : StringType()
}


# In[285]:


for key, type in dtypes_mapping.items():
    silver_df = silver_df.withColumn(key, f.col(key).cast(type))


# In[287]:


cols_str = ["nota", "no_pkb_or_cust", "kode_part", "suku_cadang", "sparepart_group"]


for col in cols_str:
    silver_df = silver_df.withColumn(col, f.trim(col).alias(col))


# In[289]:


for col in cols_str:
    silver_df = silver_df.withColumn(col, f.upper(col).alias(col))


# In[291]:


silver_df = silver_df.drop("no")


# In[292]:


gold_cust_sparepart_df = silver_df.filter(f.col("no_pkb_or_cust").contains("PKB") == False)
gold_cust_pkb_df = silver_df.filter(f.col("no_pkb_or_cust").contains("PKB") == True)


# In[294]:


s3_write_pkb_path = f"s3a://palu-jaya-data-storage-910162731301-ap-southeast-1-an/gold/gold-nsc-pkb/gold-nsc-pkb-{now}"
s3_write_cust_path = f"s3a://palu-jaya-data-storage-910162731301-ap-southeast-1-an/gold/gold-nsc-cust/gold-nsc-cust-{now}"


# In[295]:


gold_cust_pkb_df.coalesce(1).write\
    .parquet(s3_write_pkb_path, mode="overwrite")


# In[296]:


gold_cust_sparepart_df.coalesce(1).write\
    .parquet(s3_write_cust_path, mode="overwrite")

spark.stop()