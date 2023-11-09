# Databricks notebook source
import pandas as pd
import xgboost as xgb
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics import accuracy_score, classification_report
from pyspark.sql.functions import col

# COMMAND ----------

# Lecture de la table
data = spark.sql("SELECT * FROM data_prod")
data_val = spark.sql("SELECT * FROM data_val_prod")

# Liste de toutes les colonnes de type string
string_columns = [item[0] for item in data.dtypes if item[1].startswith('string')]
string_columns_val = [item[0] for item in data_val.dtypes if item[1].startswith('string')]

# Conversion de chaque colonne string en float
for column in string_columns:
    data = data.withColumn(column, col(column).cast('float'))

for column in string_columns_val:
    data_val = data_val.withColumn(column, col(column).cast('int'))

# Vérifiez les types des colonnes après conversion
data.printSchema()
data_val.printSchema()


# COMMAND ----------

# Convertir la colonne 'etiquette_dpe' en entier
data = data.withColumn('etiquette_dpe', col('etiquette_dpe').cast('int'))

X = data.drop('etiquette_dpe','Unnamed: 0','_c0')

y = data['etiquette_dpe']



# COMMAND ----------

len(X.columns)

# COMMAND ----------

# Définir la proportion de données pour le test
proportion_test = 0.2

# Séparer les données en ensembles d'apprentissage et de test
X_train, X_test = data.randomSplit([1 - proportion_test, proportion_test], seed=42)


# COMMAND ----------

import xgboost as xgb

# Initialiser le modèle
model = xgb.XGBClassifier(objective='multi:softmax', num_class=7)

# Entraîner le modèle
model.fit(X_train, y_train)

# Évaluer le modèle
predictions = model.predict(X_test)
accuracy = accuracy_score(y_test, predictions)

print(f"Précision du modèle : {accuracy}")

