""" 
Attribute creator
Supprime un ou plusieurs attributs du df
Fonctionne soit avec attributes_to_remove soit avec attributes_to_keep
"""
from typing import Dict, Any, Optional
from model_base import BaseStep


class AttributeRemoverStep(BaseStep):
    type = "AttributeRemover"
    options: Dict[str, Any] = {
        "attributes_to_remove": Optional[str],  # Attributs à supprimer séparés par un |
        "attributes_to_keep": Optional[str],  # Attributs à conserver séparés par un |
    }


class AttributeRemover:
    def __init__(self, df, attributes_to_remove=None, attributes_to_keep=None, types=None, properties=None):
        self.attributes_to_remove = attributes_to_remove
        self.attributes_to_keep = attributes_to_keep
        self.df = df
        self.types = types
        self.properties = properties

    def run(self):
        cols = list(self.df.columns)
        if self.attributes_to_remove:
            for attr in self.attributes_to_remove.split('|'):
                if attr in cols:
                    self.df = self.df.drop(attr)
                    del self.types[attr]
                else:
                    print(str('WARNING : attribute not present in DataFrame :' + attr))
        elif self.attributes_to_keep:
            attrs = self.attributes_to_keep.split('|')
            for col in cols:
                if col not in attrs:
                    self.df = self.df.drop(col)
                    del self.types[col]

        return self.df, self.types, 'success'
