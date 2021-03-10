
import os
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

path_save = '/home/darya/Documents/Reports/2021-02-23-client-categorization'

file_client_preprocessed = '/var/lib/lookiero/stock/stock_tool/clientas_preprocessed.csv.gz'
file_client = '/var/lib/lookiero/stock/stock_tool/clientas.csv.gz'

file_client_aux = '/home/darya/Documents/Reports/2021-02-23-client-categorization/clientas_20210225.csv.gz'

file_demanda_quarter = '/home/darya/Documents/Reports/2021-02-23-client-categorization/df_demanda_purchased_quarter_2020_1.csv.gz'

df_demanda_quarter = pd.read_csv(file_demanda_quarter, index_col=0)



user_id_list = list(set(df_demanda_quarter['user_id']))

# skiprows=range(1, 10000), nrows=100

# df_clienta_raw = pd.read_csv(file_client, skiprows=range(1, 10000), nrows=100)
# df_clienta_raw = pd.read_csv(file_client_preprocessed, skiprows=range(1, 10000), nrows=100)
#
# df_clienta_aux_raw = pd.read_csv(file_client_aux, skiprows=range(1, 10000), nrows=100)




# load clients
column_clienta_list = ['user_id', 'dedicacion',
                       'aventurera', 'country',
                       'fit_abajo', 'fit_arriba',
                       'is_mother',
                       'talla_copa', 'talla_sujetador',
                       # 'n_estilos',
                       'style_boho', 'style_casual', 'style_classical', 'style_minimal', 'style_night', 'style_street',
                       # 'estilo_clienta',
                       'look_salir', 'look_trabajo',
                       'age_segment', 'height_segment', 'weight_segment',
                       'talla_arriba', 'talla_abajo']

column_clienta_aux = ['user_id', 'silueta',
                      'destacar_escote', 'destacar_brazo', 'destacar_gluteo', 'destacar_cintura', 'destacar_pierna',
                      'disimular_escote', 'disimular_brazo', 'disimular_gluteo', 'disimular_cintura',
                      'disimular_pierna',
                      'centrarEnRopa_trabajo', 'centrarEnRopa_tiempolibre', 'centrarEnRopa_noche']

# skiprows=range(1, 10000), nrows=100

query_text = 'user_id in @user_id_list'
df_clienta_raw = pd.read_csv(file_client_preprocessed, usecols=column_clienta_list).query(query_text)


# more quiz data
df_clienta_aux = pd.read_csv(file_client_aux, usecols=column_clienta_aux).query(query_text)

# df_clienta_raw = pd.read_csv(file_client, usecols=column_clienta_list, skiprows=range(1, 100000), nrows=100)
df_clienta = pd.merge(df_clienta_raw, df_clienta_aux, on='user_id', how='outer')

df_clienta.loc[(df_clienta['height_segment'] < 140) | (df_clienta['height_segment'] > 180), 'height_segment'] = np.nan

df_clienta.loc[(df_clienta['weight_segment'] < 40) | (df_clienta['weight_segment'] > 120), 'weight_segment'] = np.nan

df_clienta.loc[(df_clienta['age_segment'] < 20), 'age_segment'] = np.nan




# TODO: silueta
silueta_map = {1: 'triangulo',
               2: 'oval',
               3: 'triangulo_invertido',
               4: 'rectangulo',
               5: 'reloj_arena',
               6: 'diamante'}

df_clienta['silueta'] = df_clienta['silueta'].map(silueta_map)





# TODO: dummyes despues del merge
df_quarter = pd.merge(df_demanda_quarter, df_clienta, on='user_id', how='left')

colums_dummy = ['aventurera', 'country', 'dedicacion', 'fit_abajo',
                'fit_arriba', 'talla_copa', 'talla_sujetador',
                'look_salir', 'look_trabajo',
                'age_segment', 'height_segment', 'weight_segment', 'talla_arriba',
                'talla_abajo', 'silueta', 'centrarEnRopa_trabajo',
                'centrarEnRopa_tiempolibre', 'centrarEnRopa_noche',
                'price_range', 'n_cajas_misma_clienta', 'n_cajas_12_meses',
                'box_number']

column_bool = ['is_mother', 'style_boho', 'style_casual', 'style_classical', 'style_minimal',
               'style_night', 'style_street', 'destacar_escote', 'destacar_brazo', 'destacar_gluteo',
               'destacar_cintura', 'destacar_pierna', 'disimular_escote',
               'disimular_brazo', 'disimular_gluteo', 'disimular_cintura',
               'disimular_pierna',
               'client_is_big_size', 'is_first_box']

columns_drop = ['family_desc', 'user_id', 'box_id', 'reference', 'date_ps_done',
                'date_cancelled', 'brand', 'purchased', 'rej_fit', 'rej_price', 'rej_size', 'rej_style',
                'rej_quality', 'date_last_box', 'time_since_last_box', 'AOVn', 'date_ps_done_datetime',
                'date_ps_done_quarter',
                'date_ps_done_quarter_start', 'date_ps_done_quarter_end',
                'date_ps_done_year']

df_quarter = df_quarter.drop(columns=columns_drop)
df_dummy = pd.get_dummies(df_quarter, columns=colums_dummy)
df_dummy = df_dummy.fillna(0).astype(int)


################################################################################################################
# Apriori
from mlxtend.frequent_patterns import apriori, association_rules

# Define a function to compute Zhang's metric
def zhang(antecedent, consequent):
    # Compute the support of each book
    supportA = antecedent.mean()
    supportC = consequent.mean()

    # Compute the support of both books
    supportAC = np.logical_and(antecedent, consequent).mean()

    # Complete the expressions for the numerator and denominator
    numerator = supportAC - supportA * supportC
    denominator = max(supportAC * (1 - supportA), supportA * (supportC - supportAC))

    # Return Zhang's metric
    return numerator / denominator

def zhangs_rule(rules):
    PAB = rules['support'].copy()
    PA = rules['antecedent support'].copy()
    PB = rules['consequent support'].copy()
    numerator = PAB - PA*PB
    denominator = np.max((PAB*(1-PA).values, PA*(PB-PAB).values), axis = 0)
    return numerator / denominator

frequent_itemsets = apriori(df_dummy.iloc[0:10000, :], min_support=0.006, max_len=2, use_colnames=True).sort_values(by='support', ascending=False)
frequent_itemsets1 = apriori(df_dummy.iloc[0:20000, :], min_support=0.006, max_len=2, use_colnames=True).sort_values(by='support', ascending=False)


rules = association_rules(frequent_itemsets, metric = "support", min_threshold = 0.0015)


rules['zhang'] = zhangs_rule(rules)
