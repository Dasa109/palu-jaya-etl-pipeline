#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
import boto3
from datetime import datetime


# In[2]:


# now = datetime.now().strftime("%Y-%m-%d")
now = datetime.now().strftime("%Y-%m-%d")
file_type = "parquet"
s3_path = f"s3a://palu-jaya-data-storage-910162731301-ap-southeast-1-an/raw/raw-konsumen/data-konsumen-{now}.{file_type}"
# print(s3_path)


# In[3]:


spark = (
    SparkSession.builder
    .master("spark://spark-master:7077")
    .appName("palu-jaya-data-transformation-pkb")
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider")
    .config("spark.hadoop.fs.s3a.endpoint.region", "ap-southeast-1")\
    .config("spark.cores.max", "8")
    .getOrCreate()
)


# In[242]:


from pyspark.sql.types import StructType, StructField, StringType
df = spark.read.format("parquet")\
    .load(s3_path)


# In[ ]:



# In[244]:


df_bronze = df.select(
                    f.col("No PKB Service Terakhir").alias("no_pkb"),
                    f.col("Nama").alias("nama"),
                    f.col("Alamat").alias("alamat"),
                    f.col("Kelurahan").alias("kelurahan"),
                    f.col("Kecamatan").alias("kecamatan"),
                    f.col("No HP1").alias("no_hp"),
                    f.col("Market Name").alias("motor"),
                    f.col("Nopol").alias("nopol"),
                    f.col("Tgl Service Terakhir").alias("tgl_servis_terakhir"),
                    f.col("Biaya Jasa").alias("biaya_jasa"),
                    f.col("Biaya Part (Non Oli)").alias("biaya_part_non_oli"),
                    f.col("Biaya Part (Oli)").alias("biaya_part_oli"),
                    f.col("Total").alias("total"),
                    f.col("KM Service Terakhir").alias("km")
                )


# In[ ]:



# In[246]:


s3_pkb_rupiah_path = f"s3a://palu-jaya-data-storage-910162731301-ap-southeast-1-an/raw/raw-pkb-rupiah/data-pkb-rupiah-{now}.{file_type}"

pkb_rupiah_df = spark.read.parquet(s3_pkb_rupiah_path)


# In[ ]:



# In[248]:


df_silver=df_bronze.join(pkb_rupiah_df, on=[pkb_rupiah_df["No PKB"] == df_bronze["no_pkb"]], how="inner").select([df_bronze[x] for x in df_bronze.columns] + [pkb_rupiah_df["Nama"].alias("mekanik")])


# In[ ]:




# In[250]:


mekanik_duplicates_mapping = {
    "andiva fais" : "ANDIVA",
    "AHMAD DANIL AINUL" : "AHMAD DANIL"
}

for key, correct in mekanik_duplicates_mapping.items():
    df_silver = df_silver.withColumn("mekanik", f.when(f.col("mekanik") == key, correct).otherwise(f.col("mekanik")))


# In[ ]:


# df_silver.printSchema()


# In[252]:


df_silver = df_silver.withColumn("tgl_servis_terakhir", f.to_date(f.col("tgl_servis_terakhir"), "MM-dd-yyyy"))


# In[ ]:




# In[254]:


honda_motorcycles = [
    "BeAT",
    "BeAT Street",
    "Genio",
    "Scoopy",
    "Spacy",
    "Vario 110",
    "Vario 125",
    "Vario 150",
    "Vario 160",
    "Stylo 160",
    "PCX 150",
    "PCX 160",
    "ADV 150",
    "ADV 160",
    "Forza 250",
    "Forza 350",
    "Forza 750",
    "Revo Fit",
    "Revo X",
    "Revo Absolute",
    "Supra X 125",
    "Supra Fit",
    "Supra XX",
    "Blade 110",
    "Blade 125",
    "Supra GTR 150",
    "Sonic 150R",
    "CS1",
    "Super Cub C125",
    "CT125",
    "ST125 Dax",
    "CB150 Verza",
    "Verza 150",
    "CB150R StreetFire",
    "CB150X",
    "Tiger 2000",
    "Tiger Revo",
    "Megapro Primus",
    "Megapro FI",
    "CBR150R",
    "CBR250R",
    "CBR250RR",
    "CRF150L",
    "CRF250L",
    "CRF250 Rally",
    "Rebel 500",
    "Rebel 1100",
    "Monkey 125",
    "CB500X",
    "NX500",
    "Gold Wing 1800",
    "CB500F",
    "CB650R",
    "CBR500R",
    "CBR650R",
]


# In[255]:


from thefuzz import fuzz, process
from pyspark.sql.functions import udf


#token_set_ratio

@udf(returnType=StringType())
def string_matching_honda_motor(motor):
    res = process.extractOne(motor, honda_motorcycles, scorer=fuzz.partial_ratio)
    return res[0]


# In[256]:


df_silver = df_silver.withColumn("motor_group", string_matching_honda_motor(df_silver["motor"]))


# In[ ]:


# df_silver.printSchema()
#089606452121 dimas


# In[258]:


from pyspark.sql.types import DateType, IntegerType, LongType
data_type_mapping = {
    "no_pkb": StringType(),
    "nama": StringType(),
    "alamat": StringType(),
    "kelurahan": StringType(),
    "kecamatan": StringType(),
    "no_hp": LongType(),
    "motor": StringType(),
    "nopol": StringType(),
    "tgl_servis_terakhir": DateType(),
    "biaya_jasa": IntegerType(),
    "biaya_part_non_oli": IntegerType(),
    "biaya_part_oli": IntegerType(),
    "total": IntegerType(),
    "km": IntegerType(),
    "mekanik": StringType(),
    "motor_group": StringType(),
}

for col, types in data_type_mapping.items():
    df_silver = df_silver.withColumn(col, f.col(col).cast(types))
df_silver = df_silver.withColumn("no_hp", df_silver["no_hp"].cast(StringType()))


# In[ ]:




# In[ ]:



# In[261]:


df_silver = df_silver.withColumn("no_hp", f.when(f.col("no_hp").startswith("8"), f.concat(f.lit("0"), f.col("no_hp"))).otherwise(f.col("no_hp")))


# In[262]:


columns_to_trim = ["motor_group", "mekanik", "nopol", "motor", "no_hp", 'kecamatan', 'kelurahan', 'alamat', 'nama', 'no_pkb']
for column in columns_to_trim:
    df_silver = df_silver.withColumn(column, f.trim(f.col(column)))


# In[263]:


df_silver = df_silver.select([f.upper(x).alias(x) for x in df_silver.columns])


# In[264]:


cols = df_silver.columns
cols = cols[:6] + [cols[-1]] + cols[6:-1]
cols


# In[ ]:


df_gold = df_silver.select(cols)


# In[267]:


store_path = f"s3a://palu-jaya-data-storage-910162731301-ap-southeast-1-an/gold/gold-pkb-data/gold-pkb-data-{now}"

df_gold = df_gold.coalesce(1)
df_gold.write.parquet(store_path, mode="overwrite")

spark.stop()