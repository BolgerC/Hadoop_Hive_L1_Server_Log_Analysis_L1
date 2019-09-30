# -*- coding: utf-8 -*-
"""
Created on Fri Sep 27 11:26:03 2019

@author: CA20072599
"""

# Here we import the input and output path from config.yaml
import yaml

with open("..\config.yaml", 'r') as stream:
    yaml_data = yaml.safe_load(stream)
    
# Here we create the readers for the IP dbs
import geoip2.database

city_reader = geoip2.database.Reader(yaml_data['input_path']+'GeoLite2-City.mmdb')
country_reader = geoip2.database.Reader(yaml_data['input_path']+'/path/to/GeoLite2-Country.mmdb')

