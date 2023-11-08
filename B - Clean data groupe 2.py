# Databricks notebook source
import pandas as pd
import tensorflow as tf
import matplotlib.pyplot as plt

# COMMAND ----------

dataset_test = pd.read_csv('/dbfs/mnt/mntgrp2/test.csv',index_col=0)
dataset_train = pd.read_csv('/dbfs/mnt/mntgrp2/train.csv',index_col=0)
dataset_validation = pd.read_csv('/dbfs/mnt/mntgrp2/val.csv',index_col=0)


# COMMAND ----------

dataset_test.head()

# COMMAND ----------

print('test --------------')
dataset_test.isna().sum()
print('train--------------')
dataset_train.isnull().sum()
print('val--------------')
dataset_validation.isnull().sum()

# COMMAND ----------

X_train = dataset_train.drop(columns=["N°DPE", "Cage_d'escalier","Nom__commune_(Brut)","Code_INSEE_(BAN)", "N°_département_(BAN)","Configuration_installation_chauffage_n°2","Facteur_couverture_solaire_saisi","Type_générateur_froid","Type_émetteur_installation_chauffage_n°2","Surface_totale_capteurs_photovoltaïque","Type_énergie_n°3","Type_générateur_n°1_installation_n°2","Description_générateur_chauffage_n°2_installation_n°2","Facteur_couverture_solaire","Qualité_isolation_plancher_haut_comble_aménagé","Qualité_isolation_plancher_haut_toit_terrase","Surface_habitable_immeuble", "Qualité_isolation_plancher_haut_comble_perdu", "Qualité_isolation_plancher_bas", "Qualité_isolation_murs","Qualité_isolation_menuiseries", "Qualité_isolation_enveloppe",	"Conso_5_usages_é_finale","Conso_5_usages/m²_é_finale", "Emission_GES_chauffage_énergie_n°2", "Emission_GES_éclairage", "Surface_habitable_desservie_par_installation_ECS" ])


# COMMAND ----------

X_train.head()


# COMMAND ----------

X_test = dataset_test.drop(columns=["N°DPE", "Cage_d'escalier","Nom__commune_(Brut)","Code_INSEE_(BAN)", "N°_département_(BAN)","Configuration_installation_chauffage_n°2","Facteur_couverture_solaire_saisi","Type_générateur_froid","Type_émetteur_installation_chauffage_n°2","Surface_totale_capteurs_photovoltaïque","Type_énergie_n°3","Type_générateur_n°1_installation_n°2","Description_générateur_chauffage_n°2_installation_n°2","Facteur_couverture_solaire","Qualité_isolation_plancher_haut_comble_aménagé","Qualité_isolation_plancher_haut_toit_terrase","Surface_habitable_immeuble"])
X_val = dataset_validation.drop(columns=["Cage_d'escalier","Nom__commune_(Brut)","Code_INSEE_(BAN)", "N°_département_(BAN)","Configuration_installation_chauffage_n°2","Facteur_couverture_solaire_saisi","Type_générateur_froid","Type_émetteur_installation_chauffage_n°2","Surface_totale_capteurs_photovoltaïque","Type_énergie_n°3","Type_générateur_n°1_installation_n°2","Description_générateur_chauffage_n°2_installation_n°2","Facteur_couverture_solaire","Qualité_isolation_plancher_haut_comble_aménagé","Qualité_isolation_plancher_haut_toit_terrase","Surface_habitable_immeuble"])
