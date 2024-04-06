from pyspark.sql.functions import split, explode, col, udf
from pyspark.sql.types import *
container_name = "own_key_container_name"
storage_account_name = "own_storage_account_name"
storage_account_access_key = "own_key"
spark.conf.set("fs.azure.account.key." + storage_account_name +".blob.core.windows.net",storage_account_access_key)
moviesLocation = "wasbs://" + container_name +"@" + storage_account_name +".blob.core.windows.net/movies.csv"
# Get ratings and movies data
movies = spark.read.format("csv") \
  .option("inferSchema", "true") \
  .option("header", "true") \
  .load(moviesLocation)
display(movies)