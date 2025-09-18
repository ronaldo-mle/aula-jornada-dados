# Databricks notebook source
# 1. le os arquivos JSON do volume
df_raw = spark.read.json("/Volumes/raw/pokemon/pokemon_raw/pokemon_list/")

# 2. Remove as linhas identicas ()
df_no_full_duplicates = df_raw.distinct()

# 3. Grava na tabela bronze Delta do Unity Catalog
df_no_full_duplicates.write.format("delta").mode("append").saveAsTable("bronze.pokemon.tbl_pokemon_list")


