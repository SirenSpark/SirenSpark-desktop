"""
Reprojector
Reprojette une des colonnes du DataFrame
"""
from model_base import BaseStep
from typing import Dict, Any, Optional

from utils.pandas import toPandas, toSpark
from pyspark.sql.functions import expr
# from pyproj import Proj, CRS

import pyproj

class ReprojectorStep(BaseStep):
    type = "Reprojector"
    options: Dict[str, Any] = {
        "column_name": str,
        "source_srid": str,
        "new_srid": str
    }


class Reprojector:
    def __init__(self, df, types, properties, column_name='geom', source_srid='4326', new_srid='4326'):
        self.df = df
        self.types = types
        self.properties = properties
        self.column_name = column_name
        self.source_srid = source_srid
        self.new_srid = new_srid

    def run(self):

        # Reproject column
        reprojected_df = self.df.withColumn(self.column_name,
                                            expr(f"ST_AsEWKB(ST_Transform(ST_GeomFromWKB({self.column_name}), 'EPSG:{self.source_srid}', 'EPSG:{self.new_srid}'))"))
        
        # Set column SRID
        self.types[self.column_name]['srid'] = f"{self.new_srid}"

        return reprojected_df, self.types, 'success'
