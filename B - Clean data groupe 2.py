# Databricks notebook source
import pandas as pd
from pyspark.sql import functions as F


# COMMAND ----------

spark.sql(f"USE db_grp_2")

data = spark.sql("SELECT * FROM train_prex")
data_val = spark.sql("SELECT * FROM test_prex")

# COMMAND ----------


correspondance = {'très bonne':4, 'insuffisante':1, 'bonne':3, 'moyenne':2, 0:0, 'appartement':1, 'maison':2, 'immeuble':3, 'A':6, 'B':5,
                'C':4, 'D':3, 'E':2, 'F':1, 'G':0, 'Électricité':1, 'Bois – Bûches':2, 'GPL':3, 'Gaz naturel':4,'Fioul domestique':5,
                'Réseau de Chauffage urbain':6,'Bois – Granulés (pellets) ou briquettes':7,"Électricité d'origine renouvelable utilisée dans le bâtiment":8,
                'Bois – Plaquettes d’industrie':9, 'Bois – Plaquettes forestières':10,'Charbon':11, 'Propane':12, 'Butane':13, 'Réseau de Froid Urbain':14}

colonnes_a_transformer = ['Qualité_isolation_plancher_bas', 'Qualité_isolation_enveloppe', 'Qualité_isolation_menuiseries',
                         'Qualité_isolation_murs', 'Qualité_isolation_plancher_haut_comble_aménagé','Qualité_isolation_plancher_haut_comble_perdu',
                         'Qualité_isolation_plancher_haut_toit_terrase', 'Type_bâtiment', 'Etiquette_GES', 'Etiquette_DPE']

colonnes_a_transformer_val = ['Qualité_isolation_plancher_bas', 'Qualité_isolation_enveloppe', 'Qualité_isolation_menuiseries',
                         'Qualité_isolation_murs', 'Qualité_isolation_plancher_haut_comble_aménagé','Qualité_isolation_plancher_haut_comble_perdu',
                         'Qualité_isolation_plancher_haut_toit_terrase', 'Type_bâtiment', 'Etiquette_GES']




# COMMAND ----------

data = data.na.fill(0)
data_val = data_val.na.fill(0)


# COMMAND ----------


for col_name in colonnes_a_transformer:
    for key, value in correspondance.items():
        data = data.withColumn(col_name, F.when(F.col(col_name) == key, value).otherwise(F.col(col_name)))

for col_name in colonnes_a_transformer_val:
    for key, value in correspondance.items():
        data_val = data_val.withColumn(col_name, F.when(F.col(col_name) == key, value).otherwise(F.col(col_name)))


# COMMAND ----------

# Liste des colonnes à supprimer de data
columns_to_drop_data = [
    '_c0', '_no_DPE', 'Configuration_installation_chauffage__no_2', 'Type_générateur_froid', 
    'Type_émetteur_installation_chauffage__no_2', 'Classe_altitude', 'Code_postal__brut_', 
    'Type_générateur__no_1_installation__no_2', 'Nom__commune__Brut_', "Cage_d_escalier", 'Code_INSEE__BAN_', 
    'Description_générateur_chauffage__no_2_installation__no_2', '_no__département__BAN_', 'Surface_totale_capteurs_photovoltaïque', 
    'Facteur_couverture_solaire_saisi', 'Facteur_couverture_solaire', 'Type_énergie__no_3', 
    'Qualité_isolation_plancher_haut_toit_terrase', 'Qualité_isolation_murs', 'Qualité_isolation_plancher_haut_comble_perdu', 
    'Hauteur_sous-plafond', 'Qualité_isolation_plancher_haut_comble_aménagé', 'Qualité_isolation_menuiseries', 
    'Surface_habitable_immeuble', 'Surface_habitable_logement', 'Type_bâtiment', 'Conso_5_usages/m²_é_finale', 
    'Conso_chauffage_dépensier_installation_chauffage__no_1', 'Surface_habitable_desservie_par_installation_ECS', 
    'Coût_chauffage_énergie__no_2', 'Emission_GES_chauffage_énergie__no_2', 'Code_postal__BAN_', 'Année_construction'
]

# Supprimer les colonnes indésirables de data
data = data.drop(*columns_to_drop_data)

# Liste des colonnes à supprimer de data_val
columns_to_drop_data_val = ['_c0', 'Configuration_installation_chauffage__no_2', 'Type_générateur_froid', 'Type_émetteur_installation_chauffage__no_2',
                          'Classe_altitude', 'Code_postal__brut_', 'Type_générateur__no_1_installation__no_2', 'Nom__commune__Brut_',
                          "Cage_d_escalier", 'Code_INSEE__BAN_', 'Description_générateur_chauffage__no_2_installation__no_2', '_no__département__BAN_',
                          'Surface_totale_capteurs_photovoltaïque', 'Facteur_couverture_solaire_saisi', 'Facteur_couverture_solaire',
                        'Type_énergie__no_3', 'Qualité_isolation_plancher_haut_toit_terrase', 'Qualité_isolation_murs','Qualité_isolation_plancher_haut_comble_perdu',
                        'Hauteur_sous-plafond', 'Qualité_isolation_plancher_haut_comble_aménagé', 'Qualité_isolation_menuiseries', 'Surface_habitable_immeuble', 'Surface_habitable_logement',
                        'Type_bâtiment', 'Conso_5_usages/m²_é_finale', 'Conso_chauffage_dépensier_installation_chauffage__no_1', 'Surface_habitable_desservie_par_installation_ECS',
                        'Coût_chauffage_énergie__no_2', 'Emission_GES_chauffage_énergie__no_2', 'Code_postal__BAN_', 'Année_construction'
]

# Supprimer les colonnes indésirables de data_val
data_val = data_val.drop(*columns_to_drop_data_val)

# COMMAND ----------

# Renommer les colonnes dans le DataFrame 'data'
data_6 = data.withColumnRenamed("Emission_GES_éclairage", "emission_ges_eclairage") \
           .withColumnRenamed("Conso_5_usages_é_finale_énergie__no_2", "conso_final_energie") \
           .withColumnRenamed("Etiquette_GES", "etiquette_ges") \
           .withColumnRenamed("Conso_5_usages_é_finale", "conso_final") \
           .withColumnRenamed("Etiquette_DPE", "etiquette_dpe") \
           .withColumnRenamed("Qualité_isolation_enveloppe", "qualite_isolation_enveloppe") \
           .withColumnRenamed("Qualité_isolation_plancher_bas", "qualite_isolation_plancher_bas")

# Renommer les colonnes dans le DataFrame 'data_val'
data_val_6 = data_val.withColumnRenamed("Emission_GES_éclairage", "emission_ges_eclairage") \
                   .withColumnRenamed("Conso_5_usages_é_finale_énergie__no_2", "conso_final_energie") \
                   .withColumnRenamed("Etiquette_GES", "etiquette_ges") \
                   .withColumnRenamed("Conso_5_usages_é_finale", "conso_final") \
                   .withColumnRenamed("Etiquette_DPE", "etiquette_dpe") \
                   .withColumnRenamed("Qualité_isolation_enveloppe", "qualite_isolation_enveloppe") \
                   .withColumnRenamed("Qualité_isolation_plancher_bas", "qualite_isolation_plancher_bas")

# COMMAND ----------



tables_et_dfs = {
    "data_prod": data_6,
    "data_val_prod": data_val_6
}

# Parcourir chaque paire table-DataFrame
for table, df in tables_et_dfs.items():
    print(f"Création de la table {table}")
    # Supprimer la table si elle existe
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    
    # Enregistrer la table dans la base de données spécifiée en spécifiant l'emplacement des partitions Parquet
    df.write.mode("overwrite").saveAsTable(table)

    print(f"La table {table} a été créé")

# COMMAND ----------

# # Chemin du répertoire de partitions Parquet à supprimer
# chemin_partitions_parquet = "dbfs:/user/hive/warehouse/db_grp_2/prod/data_prod.parquet"

# # Supprimer le répertoire de partitions Parquet et son contenu de manière récursive
# dbutils.fs.rm(chemin_partitions_parquet, recurse=True)

# # Vérifier que le répertoire de partitions Parquet a été supprimé
# if not dbutils.fs.ls(chemin_partitions_parquet):
#     print(f"Le répertoire de partitions Parquet {chemin_partitions_parquet} a été supprimé avec succès.")
# else:
#     print(f"La suppression du répertoire de partitions Parquet {chemin_partitions_parquet} a échoué.")


# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE data_prod

# COMMAND ----------

# Lecture de la table
test = spark.sql("SELECT * FROM data_prod")
test_val = spark.sql("SELECT * FROM data_val_prod")


# COMMAND ----------

tables = spark.catalog.listTables()

# Afficher les noms des tables
for table in tables:
    print(table.name)
# Supprimer chaque table
#for table in tables:
  #  spark.sql(f"DROP TABLE {table.name}")

# COMMAND ----------

test = spark.read.table("db_grp_2.data_prod")
test_val = spark.read.table("db_grp_2.data_val_prod")

# COMMAND ----------

test.display()
test_val.display()

# COMMAND ----------


