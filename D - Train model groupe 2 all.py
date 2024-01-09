# Databricks notebook source
pip install xgboost

# COMMAND ----------

import pandas as pd
import xgboost as xgb
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics import accuracy_score, classification_report
from pyspark.sql.functions import col

# COMMAND ----------

data = spark.read.table("db_grp_2.data_prod_all")
data_val = spark.read.table("db_grp_2.data_val_prod_all")

# COMMAND ----------



# COMMAND ----------

# Convertir la colonne 'etiquette_dpe' en entier
data = data.withColumn('etiquette_dpe', col('etiquette_dpe').cast('int'))
data = data.withColumn("etiquette_ges",col("etiquette_ges").cast("int"))
data = data.withColumn("qualite_isolation_enveloppe",col("qualite_isolation_enveloppe").cast("int"))
data = data.withColumn("qualite_isolation_plancher_bas",col("qualite_isolation_plancher_bas").cast("int"))

data = data.na.fill(0)

data_pd = data.toPandas()

X = data_pd.drop(columns=['etiquette_dpe'])

y = data_pd['etiquette_dpe']


# COMMAND ----------

data_pd

# COMMAND ----------

len(X.columns)

# COMMAND ----------

# Définir la proportion de données pour le test
proportion_test = 0.2

# Séparer les données en ensembles d'apprentissage et de test
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=proportion_test, random_state=42)



# COMMAND ----------

# Créez un modèle XGBoost
model = xgb.XGBClassifier(objective='multi:softmax', num_class=7)

# Entraînez le modèle sur l'ensemble d'apprentissage
model.fit(X, y)

# COMMAND ----------

# Faites des prédictions sur l'ensemble de test
y_pred = model.predict(X_test)

# Mesurez la performance du modèle
accuracy = accuracy_score(y_test, y_pred)
print(f"Précision : {accuracy:.2f}")
