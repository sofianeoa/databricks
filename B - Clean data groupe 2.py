# Databricks notebook source
import pandas as pd
from pyspark.sql import functions as F


# COMMAND ----------

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

data = data.fillna(0)
data_val = data_val.fillna(0)


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
    'Unnamed: 0', 'N°DPE', 'Configuration_installation_chauffage_n°2', 'Type_générateur_froid', 
    'Type_émetteur_installation_chauffage_n°2', 'Classe_altitude', 'Code_postal_(brut)', 
    'Type_générateur_n°1_installation_n°2', 'Nom__commune_(Brut)', "Cage_d'escalier", 'Code_INSEE_(BAN)', 
    'Description_générateur_chauffage_n°2_installation_n°2', 'N°_département_(BAN)', 'Surface_totale_capteurs_photovoltaïque', 
    'Facteur_couverture_solaire_saisi', 'Facteur_couverture_solaire', 'Type_énergie_n°3', 
    'Qualité_isolation_plancher_haut_toit_terrase', 'Qualité_isolation_murs', 'Qualité_isolation_plancher_haut_comble_perdu', 
    'Hauteur_sous-plafond', 'Qualité_isolation_plancher_haut_comble_aménagé', 'Qualité_isolation_menuiseries', 
    'Surface_habitable_immeuble', 'Surface_habitable_logement', 'Type_bâtiment', 'Conso_5_usages/m²_é_finale', 
    'Conso_chauffage_dépensier_installation_chauffage_n°1', 'Surface_habitable_desservie_par_installation_ECS', 
    'Coût_chauffage_énergie_n°2', 'Emission_GES_chauffage_énergie_n°2', 'Code_postal_(BAN)', 'Année_construction'
]

# Supprimer les colonnes indésirables de data
data = data.drop(*columns_to_drop_data)

# Liste des colonnes à supprimer de data_val
columns_to_drop_data_val = ['Configuration_installation_chauffage_n°2', 'Type_générateur_froid', 'Type_émetteur_installation_chauffage_n°2',
                          'Classe_altitude', 'Code_postal_(brut)', 'Type_générateur_n°1_installation_n°2', 'Nom__commune_(Brut)',
                          "Cage_d'escalier", 'Code_INSEE_(BAN)', 'Description_générateur_chauffage_n°2_installation_n°2', 'N°_département_(BAN)',
                          'Surface_totale_capteurs_photovoltaïque', 'Facteur_couverture_solaire_saisi', 'Facteur_couverture_solaire',
                        'Type_énergie_n°3', 'Qualité_isolation_plancher_haut_toit_terrase', 'Qualité_isolation_murs','Qualité_isolation_plancher_haut_comble_perdu',
                        'Hauteur_sous-plafond', 'Qualité_isolation_plancher_haut_comble_aménagé', 'Qualité_isolation_menuiseries', 'Surface_habitable_immeuble', 'Surface_habitable_logement',
                        'Type_bâtiment', 'Conso_5_usages/m²_é_finale', 'Conso_chauffage_dépensier_installation_chauffage_n°1', 'Surface_habitable_desservie_par_installation_ECS',
                        'Coût_chauffage_énergie_n°2', 'Emission_GES_chauffage_énergie_n°2', 'Code_postal_(BAN)', 'Année_construction'
]

# Supprimer les colonnes indésirables de data_val
data_val = data_val.drop(*columns_to_drop_data_val)

# COMMAND ----------

data_val.columns

# COMMAND ----------

tables_et_dfs = {
    "data_prod": data,
    "data_val_prod": data_val
}

format = "parquet"
# Parcourir chaque paire table-DataFrame
for table, df in tables_et_dfs.items():
    print(table)
    # Vérifier si la table existe
    if spark.catalog.tableExists(table):
        # Supprimer la table si elle existe
        spark.sql(f"DROP TABLE IF EXISTS {table}")
    # Créer la nouvelle table à partir du DataFrame
    df.write.mode("overwrite").format(format).saveAsTable(table)


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
