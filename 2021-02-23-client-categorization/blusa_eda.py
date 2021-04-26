
"""

"""



import os
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from math import isnan

import io


# file_product = '/var/lib/lookiero/stock/stock_tool/productos_preprocessed.csv.gz'
path_save = '/home/darya/Documents/Reports/2021-02-23-client-categorization'
# file_modelo = '/home/darya/Documents/Reports/2021-02-23-client-categorization/Variedad 2021 - Por refe.csv'



features_to_include = ['application', 'basic', 'composition', 'cut', 'detail', 'fabric', 'fabric_adj', 'finishing',
                      'fit', 'message', 'neck', 'neckline', 'print', 'sizing', 'sleeve', 'sleeve_long', 'style',
                      'top_type', 'weather']

features_to_include = ['application', 'basic', 'composition', 'cut', 'fabric', 'neck', 'neckline', 'print', 'sleeve',
                       'sleeve_long', 'style']

features_to_remove = ['sleeve_long_cm']

# aa = df_productos_family[list(set([s for s in df_productos_family.columns for f in ['style'] if s.startswith(f)]))]


family = 'blusa_camisa'

family_product_file = os.path.join(path_save, 'Features_' + family + '.csv')

df_productos_family = pd.read_csv(family_product_file, index_col=0).rename(columns={'grupo': 'modelo'})

list_modelo_fam = df_productos_family['modelo'].unique().tolist()

# Include just features of interes
# features_to_analyze = [s for s in df_productos_family.columns for f in features_to_include if f in s]

features_to_analyze = list(set([s for s in df_productos_family.columns for f in features_to_include if s.startswith(f)]))

features_to_analyze = [ele for ele in features_to_analyze if ele not in features_to_remove]

df_productos_family = df_productos_family[['modelo'] + features_to_analyze]

df_feature_gr = df_productos_family.groupby(features_to_analyze)['modelo'].count().reset_index()

df_feature_gr = df_productos_family.groupby(['style_minimal', 'fabric_crepe'])['modelo'].count().reset_index()



a = list(set([s for s in df_productos_family.columns for f in features_to_include if s.startswith(f)]))

aa = df_productos_family.drop_duplicates(subset=features_to_analyze)
df['Group'] = df.groupby(['gw_mac', 'mac']).cumcount()


import networkx as nx
G = nx.Graph()
df_test = df_productos_family.iloc[0:6][['modelo', 'application_work', 'sleeve_cuffed_sleeve', 'fabric_artificial', 'print_floral']]


G = nx.from_pandas_dataframe(edges, 'source', 'target', 'weight')