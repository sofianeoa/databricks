# Databricks notebook source
import pandas as pd
import xgboost as xgb
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics import accuracy_score, classification_report

# COMMAND ----------

#lecture data via Mount 
file_path = "/dbfs/mnt/mntgrp2/train.csv"
data = pd.read_csv(file_path)

# COMMAND ----------

correspondance = {  'très bonne':4, 'insuffisante':1, 'bonne':3, 'moyenne':2, 0:0,
                    'appartement':1, 'maison':2, 'immeuble':3, 
                    'A':6, 'B':5,'C':4, 'D':3, 'E':2, 'F':1, 'G':0,
                    'Électricité':1, 'Bois – Bûches':2, 'GPL':3, 'Gaz naturel':4,'Fioul domestique':5,'Réseau de Chauffage urbain':6,'Bois – Granulés (pellets) ou briquettes':7,"Électricité d'origine renouvelable utilisée dans le bâtiment":8, 'Bois – Plaquettes d’industrie':9, 'Bois – Plaquettes forestières':10,'Charbon':11, 'Propane':12, 'Butane':13, 'Réseau de Froid Urbain':14
                }

# COMMAND ----------

colonnes_a_transformer = [  'Qualité_isolation_plancher_bas',
                            'Qualité_isolation_enveloppe',
                            'Qualité_isolation_menuiseries',
                            'Qualité_isolation_murs',
                            'Qualité_isolation_plancher_haut_comble_aménagé',
                            'Qualité_isolation_plancher_haut_comble_perdu',
                            'Qualité_isolation_plancher_haut_toit_terrase',
                            'Type_bâtiment',
                            'Etiquette_GES',
                            'Type_énergie_n°3',
                            'Etiquette_DPE'
                            ]

# COMMAND ----------

data = data.fillna(0)

# COMMAND ----------

data[colonnes_a_transformer] = data[colonnes_a_transformer].replace(correspondance)

# COMMAND ----------

data = data.drop(columns= [ 'Unnamed: 0',
                            'N°DPE',
                            'Configuration_installation_chauffage_n°2',
                            'Type_générateur_froid',
                            'Type_émetteur_installation_chauffage_n°2',
                            'Classe_altitude',
                            'Code_postal_(brut)',
                            'Type_générateur_n°1_installation_n°2',
                            'Nom__commune_(Brut)',
                            "Cage_d'escalier",
                            'Code_INSEE_(BAN)',
                            'Description_générateur_chauffage_n°2_installation_n°2',
                            'N°_département_(BAN)'
                            ])
                            

# COMMAND ----------

# Séparer les caractéristiques (X) de la cible (y)
X = data.drop(columns=['Etiquette_DPE'])  
y = data['Etiquette_DPE']
