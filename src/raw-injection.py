# Databricks notebook source
import requests
import json
from datetime import datetime

#1. chamada da API
url = "https://pokeapi.co/api/v2/pokemon?offset=20&limit=1400"
response = requests.get(url)

#2 captura dos dados no formato json
data = response.json()

#3 monta o nome do arquivo
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
filename = f"pokemon_list_{timestamp}.json"

#4 define o caminho dentro do volume do databricks
volume_path = f"/Volumes/raw/pokemon/pokemon_raw/pokemon_list/"

#5 define o caminho completo do arquivo
full_path = f"{volume_path}{filename}"

#5 grava o arquivo no Databricks 
with open(full_path, "w") as file:
    json.dump(data, file)


# COMMAND ----------

dbutils.fs.ls("/Volumes/raw/pokemon/pokemon_raw/pokemon_list/")

