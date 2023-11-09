# Databricks notebook source
try:
    # Si la commande ls ne lève pas d'exception, le point de montage existe
    dbutils.fs.ls("/mnt/mntgrp2")
    print("Le point de montage mntgrp2 existe déjà.")
    
except Exception as e:
    print("Le point de montage mntgrp2 n'existe pas. Création en cours...")
    # Créer le point de montage s'il n'existe pas
    dbutils.fs.mount(
        source="wasbs://grp2cnt@grp2sto.blob.core.windows.net",
        mount_point="/mnt/mntgrp2",
        extra_configs={"fs.azure.account.key.grp2sto.blob.core.windows.net":dbutils.secrets.get(scope="grp2scope", key="grp2secret")}
    )
    print("Le point de montage mntgrp2 a été créé.")

# COMMAND ----------

df_test = spark.read.option("header", "true").csv("/mnt/mntgrp2/test.csv")
df_train = spark.read.option("header", "true").csv("/mnt/mntgrp2/train.csv")
df_val= spark.read.option("header", "true").csv("/mnt/mntgrp2/val.csv")
display(df_test)
display(df_train )
display(df_val)

# COMMAND ----------

format = "parquet"
df_test.write.mode("ignore").format(format).saveAsTable("test_prex")
df_train.write.mode("ignore").format(format).saveAsTable("train_prex")
df_val.write.mode("ignore").format(format).saveAsTable("val_prex")


# COMMAND ----------

# Lecture de la table
test = spark.sql("SELECT * FROM test_prex")

# Affichage du DataFrame
display(test)
