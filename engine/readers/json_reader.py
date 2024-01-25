""" 
JSONReader
Effectue une lecture sur un fichier JSON
"""
from pyspark.sql import SparkSession
from typing import Dict, Any, Optional
from model_base import BaseStep
import logging
import pandas as pd
from utils.pandas import toSpark


class JSONReaderStep(BaseStep):
    type = "JSONReader"
    options: Dict[str, Any] = {
        "filepath": str
    }


class JSONReader:
    def __init__(self, filepath):
        self.filepath = filepath

    def run(self):
        spark = SparkSession.builder.appName("SirenSpark").getOrCreate()
        try:
            pandas_df = pd.read_json(self.filepath)

            # Transformation vers spark
            df = toSpark(pandas_df, [])

            # Déduction des types de colonne
            column_types = {col: str(dtype) for col, dtype in df.dtypes}
            return df, column_types, "success"
        except Exception as e:
            logging.error(
                f"Error reading to JSON file {e}")
            return False, False, 'error'
