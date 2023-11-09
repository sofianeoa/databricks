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

files = dbutils.fs.ls("/mnt/mntgrp2")
for file in files:
    # Vérifiez si le fichier est un fichier CSV par exemple
    if file.name.endswith('.csv'):
        # Lecture du fichier CSV dans un DataFrame Spark
        df = spark.read.option("header", "true").csv(file.path)

        # Enregistrement du DataFrame en tant que table
        df.write.table(f"dbfs:/mnt/mntgrp2/{file.name[:-4]}_tbl")

        # Affichage du DataFrame
        display(df)

        # Changement des noms des colonnes
        new_cols = []
        for col in df.columns:
            new_col = col.replaceAll("[ ,;{}()\n\t=]+", "")
            new_cols.append(new_col)

        # Mise à jour des noms des colonnes
        df.columns = new_cols

        # Affichage du DataFrame avec les nouveaux noms de colonnes
        display(df)

# COMMAND ----------

from pyspark.sql import SparkSession


file_list = dbutils.fs.ls("/mnt/mntgrp2")

for file in file_list:
    if file.name.endswith('.csv'):
        # Lecture du fichier CSV dans un DataFrame Spark
        df = spark.read.option("header", "true").csv(file.path)

        # Déduire le nom de la table à partir du nom du fichier
        table_name = file.name.replace('.csv', '')

        # Enregistrer le DataFrame en tant que table
        df.write.format("parquet").saveAsTable(table_name)

spark.stop()

# COMMAND ----------

file_list = dbutils.fs.ls("/mnt/mntgrp2")

for file in file_list:
    if file.name.endswith('.csv'):
        # Déduire le nom de la table à partir du nom du fichier
        table_name = file.name.replace('.csv', '')

        # Supprimer la table si elle existe
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")

        # Lecture du fichier CSV dans un DataFrame Spark
        df = spark.read.option("header", "true").csv(file.path)

        # Enregistrer le DataFrame en tant que table
        df.write.format("parquet").saveAsTable(table_name)

tables = spark.sql("SHOW TABLES")
tables.show()

spark.stop()
