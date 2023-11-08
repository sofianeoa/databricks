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

import pandas as pd
import sqlite3
import re

# Fonction pour nettoyer les noms de colonnes
def clean_column_names(column_name):
    # Utiliser une expression régulière pour remplacer les caractères non valides
    return re.sub(r'[ ,;{}()\n\t=]+', '_', column_name)

# Nom de la base de données SQLite
db_name = 'my_dataset.db'

# Créer ou se connecter à la base de données SQLite
conn = sqlite3.connect(db_name)

# Liste des fichiers CSV
csv_files = ['/mnt/mntgrp2/test.csv', '/mnt/mntgrp2/train.csv', '/mnt/mntgrp2/val.csv']


# COMMAND ----------

# Parcourir la liste des fichiers CSV
for file_path in csv_files:
    # Lire le fichier CSV dans un DataFrame pandas
    df = pd.read_csv(file_path)
    
    # Nettoyer les noms des colonnes
    df.rename(columns=clean_column_names, inplace=True)
    
    # Déduire le nom de la table à partir du nom du fichier
    table_name = re.sub(r'.csv$', '', file_path.split('/')[-1])
    
    # Enregistrer le DataFrame dans la base de données SQLite comme table
    df.to_sql(table_name, conn, if_exists='replace', index=False)

# Fermer la connexion à la base de données
conn.close()

# COMMAND ----------

files = dbutils.fs.ls("/mnt/mntgrp2")
for file in files:
    print(file.name, file.size)
file_list = dbutils.fs.ls("/mnt/mntgrp2")


for file in file_list:
    # Vérifiez si le fichier est un fichier CSV par exemple
    if file.name.endswith('.csv'):
        # Lecture du fichier CSV dans un DataFrame Spark
        df = spark.read.option("header", "true").csv(file.path)

        display(df)
