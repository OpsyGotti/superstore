from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType


mount_point = "/mnt/superstore_cluster"

if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
    dbutils.fs.unmount(mount_point)
else:
    print(f"{mount_point} is not mounted.")


configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "<<APPLICATION(CLIENT_ID>>",
"fs.azure.account.oauth2.client.secret": '<<SECRETKEY>>',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<<TENANT_ID>>/oauth2/token"}


dbutils.fs.mount(
source = "abfss://<<CONTAINERNAME>>@<<STORAGEACCOUNTNAME>>.dfs.core.windows.net", # container@storageacc
mount_point = "/mnt/superstore", # choose name you want "/mnt/xyz"
extra_configs = configs)

%fs
ls "/mnt/superstore"

bronze = spark.read.format("csv").option("header", "true").load("/mnt/superstore/rawdata/bronze")
bronze.show()