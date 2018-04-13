# Databricks notebook source
# Retrieve storage credentials
storage_account = dbutils.preview.secret.get(scope = "storage_scope", key = "storage_account")
storage_key = dbutils.preview.secret.get(scope = "storage_scope", key = "storage_key")

# Set mount path
storage_mount_path = "/mnt/blob_storage"

# Unmount if existing
for mp in dbutils.fs.mounts():
  if mp.mountPoint == storage_mount_path:
    dbutils.fs.unmount(storage_mount_path)

# Refresh mounts
dbutils.fs.refreshMounts()

# COMMAND ----------

# Mount
dbutils.fs.mount(
  source = "wasbs://databricks@" + storage_account + ".blob.core.windows.net",
  mount_point = storage_mount_path,
  extra_configs = {"fs.azure.account.key." + storage_account + ".blob.core.windows.net": storage_key})

# Refresh mounts
dbutils.fs.refreshMounts()