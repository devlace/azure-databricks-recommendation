# Databricks notebook source
# MAGIC %md
# MAGIC # Mount Blob Storage

# COMMAND ----------

# MAGIC %md
# MAGIC ### Attach to Spark cluster

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.{YOUR STORAGE ACCOUNT NAME}.blob.core.windows.net",
  "{YOUR STORAGE ACCOUNT ACCESS KEY}")

# COMMAND ----------

# MAGIC %md
# MAGIC <br>
# MAGIC <br>
# MAGIC <br>
# MAGIC <br>
# MAGIC <br>
# MAGIC <br>
# MAGIC <br>
# MAGIC <br>
# MAGIC <br>
# MAGIC <br>
# MAGIC <br>
# MAGIC <br>
# MAGIC <br>
# MAGIC <br>
# MAGIC <br>
# MAGIC <br>
# MAGIC <br>
# MAGIC <br>
# MAGIC <br>
# MAGIC <br>
# MAGIC <br>
# MAGIC <br>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount with DBFS

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://databricks@{YOUR STORAGE ACCOUNT NAME}.blob.core.windows.net",
  mount_point = "{MOUNT_PATH}",
  extra_configs = {"fs.azure.account.key.{YOUR STORAGE ACCOUNT NAME}.blob.core.windows.net": "{YOUR STORAGE ACCOUNT ACCESS KEY}"})

# COMMAND ----------

# MAGIC %md
# MAGIC <br>
# MAGIC <br>
# MAGIC <br>
# MAGIC <br>
# MAGIC <br>
# MAGIC <br>
# MAGIC <br>
# MAGIC <br>
# MAGIC <br>
# MAGIC <br>
# MAGIC <br>
# MAGIC <br>
# MAGIC <br>
# MAGIC <br>
# MAGIC <br>
# MAGIC <br>
# MAGIC <br>
# MAGIC <br>
# MAGIC <br>
# MAGIC <br>
# MAGIC <br>
# MAGIC <br>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount with DBFS using Secrets

# COMMAND ----------

# Set mount path
storage_mount_path = "/mnt/blob_storage"

# Unmount if existing
for mp in dbutils.fs.mounts():
  if mp.mountPoint == storage_mount_path:
    dbutils.fs.unmount(storage_mount_path)

# Refresh mounts
dbutils.fs.refreshMounts()

# COMMAND ----------

# Retrieve storage credentials
storage_account = dbutils.preview.secret.get(scope = "storage_scope", key = "storage_account")
storage_key = dbutils.preview.secret.get(scope = "storage_scope", key = "storage_key")

# Mount
dbutils.fs.mount(
  source = "wasbs://databricks@" + storage_account + ".blob.core.windows.net",
  mount_point = storage_mount_path,
  extra_configs = {"fs.azure.account.key." + storage_account + ".blob.core.windows.net": storage_key})