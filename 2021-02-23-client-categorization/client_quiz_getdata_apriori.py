

import os
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
##################################################################################################################
# Demanda

file_demanda = '/var/lib/lookiero/stock/stock_tool/demanda_preprocessed.csv.gz'
path_save = '/home/darya/Documents/Reports/2021-02-23-client-categorization'


file_client_preprocessed = '/var/lib/lookiero/stock/stock_tool/clientas_preprocessed.csv.gz'
file_client = '/var/lib/lookiero/stock/stock_tool/clientas.csv.gz'

file_client_aux = '/home/darya/Documents/Reports/2021-02-23-client-categorization/clientas_20210225.csv.gz'

######################################################################
# parametros
fecha_start = '2021-01-01'

fecha_end = '2021-02-28'


column_demanda_list = ['family_desc', 'user_id', 'box_id', 'reference', 'date_ps_done',
                       'date_cancelled',
                       'country',
                       'client_is_big_size', 'is_first_box', 'price_range',
                       'brand',
                       'purchased',
                       'rej_fit', 'rej_price', 'rej_size', 'rej_style', 'rej_quality',
                       'n_cajas_misma_clienta', 'n_cajas_12_meses', 'box_number',
                       'date_last_box', 'time_since_last_box',
                       'AOVn']


# load
query_text = 'date_ps_done >= @fecha_start & date_ps_done <= @fecha_end ' # & (country == "es" | country == "fr")

df_demanda_raw = pd.read_csv(file_demanda, usecols=column_demanda_list).query(query_text)  # , skiprows=range(1, 100000), nrows=100
df_demanda_raw = df_demanda_raw[(~df_demanda_raw['purchased'].isna()) & (df_demanda_raw['date_cancelled'].isna())]

#######################################################################
# clientas



country_list = df_demanda_raw['country'].dropna().unique().tolist()
for country_name in country_list:
    print(country_name)
    df_demanda_country = df_demanda_raw[df_demanda_raw['country'] == country_name]






    # df_demanda_quarter = pd.read_csv(file_demanda_quarter, index_col=0)



    user_id_list = list(set(df_demanda_country['user_id']))






    # load clients
    column_clienta_list = ['user_id', 'dedicacion',
                           'aventurera',
                           # 'country',
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

    df_clienta = pd.merge(df_clienta_raw, df_clienta_aux, on='user_id', how='outer')

    df_clienta.loc[(df_clienta['height_segment'] < 140) | (df_clienta['height_segment'] > 180), 'height_segment'] = np.nan

    df_clienta.loc[(df_clienta['weight_segment'] < 40) | (df_clienta['weight_segment'] > 120), 'weight_segment'] = np.nan

    df_clienta.loc[(df_clienta['age_segment'] < 20), 'age_segment'] = np.nan




    # silueta
    silueta_map = {1: 'triangulo',
                   2: 'oval',
                   3: 'triangulo_invertido',
                   4: 'rectangulo',
                   5: 'reloj_arena',
                   6: 'diamante'}

    df_clienta['silueta'] = df_clienta['silueta'].map(silueta_map)






    df_demanda_clienta = pd.merge(df_demanda_country, df_clienta, on='user_id', how='left')

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
                    'rej_quality', 'date_last_box', 'time_since_last_box', 'AOVn']

    df_demanda_clienta = df_demanda_clienta.drop(columns=columns_drop)
    df_dummy = pd.get_dummies(df_demanda_clienta, columns=colums_dummy, dummy_na=True)
    df_dummy = df_dummy.fillna(0).astype(int)



    ######################################################################
    # save different countries

    df_dummy.to_csv(os.path.join(path_save, 'df_demanda_clienta_dummy_' + country_name + '_' + str(fecha_start) + '_' +
                                                   str(fecha_end) + '.csv.gz'))


# col_nan = sign_headers = pd.Series(i for i in df_dummy.columns if i.find('nan') >= 0)
# col_all = df_dummy.columns.to_series()
#
# col_all.reset_index(drop=True).to_csv(os.path.join(path_save, 'df_opciones_caracteristicas_clientas.csv'))
