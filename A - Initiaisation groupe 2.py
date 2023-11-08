# Databricks notebook source
import pandas as pd




# COMMAND ----------

# Spécifiez le chemin du fichier sur le point de montage
file_path = "/dbfs/mnt/mntgrp2/train.csv"

# Lire le fichier CSV dans un DataFrame pandas
data = pd.read_csv(file_path)

# Afficher les premières lignes du DataFrame
print(data.head())
