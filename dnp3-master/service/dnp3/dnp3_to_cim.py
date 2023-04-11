import json
import pandas as pd
import numpy as np

class CIMMapping():
    """ This creates dnp3 input and output points for incoming CIM messages  and model dictionary file respectively."""

    def __init__(self, conversion_dict="conversion_dict.json", model_line_dict="model_line_dict.json"):
        with open(conversion_dict) as f:
            conversion_dict = json.load(f)
        self.conversion_dict = conversion_dict

        with open(model_line_dict) as f:
            model_line_dict = json.load(f)
        self.model_line_dict = model_line_dict

if __name__ == '__main__':
    CIMMapping()
