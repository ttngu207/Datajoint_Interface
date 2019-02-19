import re
import os
import sys
from datetime import datetime

import numpy as np
import scipy.io as sio
import datajoint as dj

schema = dj.schema(dj.config.get('database.prefix', '') + 'reference')


@schema
class AnimalSource(dj.Lookup):
    definition = """
    animal_source: varchar(32)      # source of the animal, Jax, Charles River etc.
    """
    contents = zip(['Jackson', 'Charles River', 'Guoping Feng', 'Homemade', 'Unknown'])


@schema
class Strain(dj.Lookup):
    definition = """ 
    strain: varchar(24)
    """
    contents = zip(['C57', 'Ai35D', 'VGAT-ChR2-EYFP', 'Ai32', 'GAD2-Cre', 'PV-Cre', 'Unknown'])


@schema
class Laboratory(dj.Lookup):
    definition = """ 
    lab_name: varchar(24)  # name of lab
    ---
    lab_description=null: varchar(128)  
    """
    contents = [['WangLab', 'WangLab']]


@schema
class InjectionType(dj.Lookup):
    definition = """ 
    injection_type: varchar(30)  # # (Str) what kind of tracer/injection
    """
    contents = zip(['flourescent'])
