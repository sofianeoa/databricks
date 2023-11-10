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

try:
    # Vérifie si le point de montage existe
    dbutils.fs.ls("/mnt/mntgrp2")
    print("Le point de montage mntgrp2 existe déjà. Suppression en cours...")
    
    # Supprime le point de montage existant
    dbutils.fs.unmount("/mnt/mntgrp2")
    print("Le point de montage mntgrp2 a été supprimé.")

except Exception as e:
    print("Le point de montage mntgrp2 n'existe pas ou a été supprimé.")

# Crée le point de montage (fonctionne dans les deux cas: s'il existait déjà ou non)
dbutils.fs.mount(
    source="wasbs://grp2cnt@grp2sto.blob.core.windows.net",
    mount_point="/mnt/mntgrp2",
    extra_configs={"fs.azure.account.key.grp2sto.blob.core.windows.net": dbutils.secrets.get(scope="grp2scope", key="grp2secret")}
)
print("Le point de montage mntgrp2 a été créé ou recréé.")


# COMMAND ----------

df_train = spark.read.option("header", "true").option("inferSchema", "true").csv("/mnt/mntgrp2/train.csv")
display(df_train)


# COMMAND ----------


df_val= spark.read.option("header", "true").csv("/mnt/mntgrp2/val.csv")

display(df_val)

# COMMAND ----------

df_test = spark.read.option("header", "true").option("inferSchema", "true").csv("/mnt/mntgrp2/test.csv")

display(df_test)


# COMMAND ----------

# Liste des noms de table et des DataFrames correspondants
tables_et_dfs = {
    "test_prex": df_test,
    "train_prex": df_train,
    "val_prex": df_val
}

format = "parquet"

# Parcourir chaque paire table-DataFrame
for table, df in tables_et_dfs.items():
    # Vérifier si la table existe
    if spark.catalog.tableExists(table):
        # Supprimer la table si elle existe
        spark.sql(f"DROP TABLE IF EXISTS {table}")

    # Créer la nouvelle table à partir du DataFrame
    df.write.mode("overwrite").format(format).saveAsTable(table)


# COMMAND ----------

tables = spark.catalog.listTables()

# Afficher les noms des tables
for table in tables:
    print(table.name)




# COMMAND ----------



# Lister toutes les tables dans la base de données
tables = spark.catalog.listTables()

# Supprimer chaque table
for table in tables:
    spark.sql(f"DROP TABLE {table.name}")
