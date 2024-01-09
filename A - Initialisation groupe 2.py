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

spark.sql("CREATE DATABASE IF NOT EXISTS db_grp_2")
spark.catalog.setCurrentDatabase("db_grp_2")

# COMMAND ----------

nom_base_de_donnees = spark.catalog.currentDatabase()
# Afficher le nom de la base de données
print(nom_base_de_donnees)

# COMMAND ----------

df_train = spark.read.option("header", "true").option("inferSchema", "true").csv("/mnt/mntgrp2/train.csv")

from pyspark.sql.functions import col
# Fonction pour nettoyer les noms de colonnes
def clean_column_name(column_name):
    # Supprimer ou remplacer les caractères spéciaux
    cleaned_name = column_name.replace("n°", "_no_").replace("N°", "_no_").replace("      ", "")  # Remplacer 'n°' par '_no_'
    cleaned_name = cleaned_name.replace(":", "_").replace("'", "_").replace(" ", "_")
    cleaned_name = cleaned_name.replace("(", "_").replace(")", "_")
    return cleaned_name

# Renommer les colonnes
new_column_names = [clean_column_name(c) for c in df_train.columns]
df_train = df_train.toDF(*new_column_names)

# Afficher les noms de colonnes modifiés
df_train.show()


# COMMAND ----------

from pyspark.sql.functions import col

# DataFrame original
df_test = spark.read.option("header", "true").option("inferSchema", "true").csv("/mnt/mntgrp2/train.csv")

# Fonction pour nettoyer les noms de colonnes
def clean_column_name(column_name):
    # Remplacer les caractères spéciaux et les espaces par des underscores
    cleaned_name = column_name.replace("n°", "_no_").replace("N°", "_no_")  # Remplacer 'n°' par '_no_'
    cleaned_name = cleaned_name.replace(":", "_").replace("'", "_").replace(" ", "_")
    cleaned_name = cleaned_name.replace("(", "_").replace(")", "_")
    return cleaned_name

# Appliquer la fonction de nettoyage à chaque nom de colonne
new_column_names = [clean_column_name(c) for c in df_test.columns]

# Renommer les colonnes du DataFrame
df_test = df_test.toDF(*new_column_names)

# Afficher les noms de colonnes modifiés
df_test.show()


# COMMAND ----------

df_test

# COMMAND ----------

# Liste des noms de table et des DataFrames correspondants
tables_et_dfs = {
    "test_prex": df_test,
    "train_prex": df_train,
}

# Parcourir chaque paire table-DataFrame
for table, df in tables_et_dfs.items():
    print(table)
    # Vérifier si la table existe
    if spark.catalog.tableExists(table):
        # Supprimer la table si elle existe
        spark.sql(f"DROP TABLE {table}")
    # Créer la nouvelle table à partir du DataFrame
    df.write.mode("overwrite").saveAsTable(table)


# COMMAND ----------

tables = spark.catalog.listTables()


# Afficher les noms des tables
for table in tables:
    print(table.name)
# Supprimer chaque table
#for table in tables:
  #  spark.sql(f"DROP TABLE {table.name}")
