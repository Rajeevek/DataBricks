# Databricks notebook source
# Python code to mount and access Azure Data Lake Storage Gen2 Account to Azure Databricks with Service Principal and OAuth
# Author: Dhyanendra Singh Rathore

# Define the variables used for creating connection strings
adlsAccountName = "rajeevdatalakestorage"
adlsContainerName = "taxisource"
mountPoint = "/mnt/csvFiles"

# Application (Client) ID
applicationId = dbutils.secrets.get(scope="NewYorkTaxiKeyVault",key="ApplicationID")

# Application (Client) Secret Key
authenticationKey = dbutils.secrets.get(scope="NewYorkTaxiKeyVault",key="ClientSecret")

# Directory (Tenant) ID
tenandId = dbutils.secrets.get(scope="NewYorkTaxiKeyVault",key="TenantID")

endpoint = "https://login.microsoftonline.com/" + tenandId + "/oauth2/token"
source = "abfss://" + adlsContainerName + "@" + adlsAccountName + ".dfs.core.windows.net/" 

# Connecting using Service Principal secrets and OAuth
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": applicationId,
           "fs.azure.account.oauth2.client.secret": authenticationKey,
           "fs.azure.account.oauth2.client.endpoint": endpoint}

# Mounting ADLS Storage to DBFS
# Mount only if the directory is not already mounted
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  dbutils.fs.mount(
    source = source,
    mount_point = mountPoint,
    extra_configs = configs)

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.conf.set("fs.azure.account.auth.type.rajeevdatalakestorage.dfs.core.windows.net", "OAuth")
# MAGIC spark.conf.set("fs.azure.account.oauth.provider.type.rajeevdatalakestorage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.id.rajeevdatalakestorage.dfs.core.windows.net", "ApplicationID")
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.secret.rajeevdatalakestorage.dfs.core.windows.net", dbutils.secrets.get(scope="NewYorkTaxiKeyVault",key="ClientSecret"))
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.endpoint.rajeevdatalakestorage.dfs.core.windows.net", "https://login.microsoftonline.com/TenantID/oauth2/token")

# COMMAND ----------

#Get the file list mounted
%fs
ls "/mnt/csvFiles"

# COMMAND ----------

#Read the content of the files
df = spark.read.text("/mnt/csvFiles")
df = spark.read.text("dbfs:/mnt/csvFiles/....")

# COMMAND ----------

# Unmount only if directory is mounted
if any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  dbutils.fs.unmount(mountPoint)
