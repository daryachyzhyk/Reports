"""
Necesitamos sacar datos de junio 2020 a ahora de:
Envios y KR por familia y por rango de precio de clienta (30-60, 60-100, >100)
Poner el precio medio tanto de envios como de KR. Precio medio de KR es de purchased solo.
Es para poder analizar los rangos de precio de familias que tenemos en el modulo de Amy de precio

Rango de precio 1 - es (30-60)
"""


import pandas as pd
import numpy as np
import os



file_demanda = '/var/lib/lookiero/stock/stock_tool/demanda_preprocessed.csv.gz'

file_clienta = '/var/lib/lookiero/stock/stock_tool/clientas_preprocessed.csv.gz'


query_demanda = 'date_ps_done >= "2020-06-01" & date_ps_done <= "2021-05-31" '

df_demanda = pd.read_csv(file_demanda,
                           usecols=['date_ps_done', 'country', 'family_desc', 'user_id', 'box_id',
                                    'reference', 'date_co', 'purchased', 'precio_bruto', 'precio_neto']).query(query_demanda)

df_demanda = df_demanda[(~df_demanda['date_co'].isna()) & (df_demanda['date_co'] != '')]

# df_demanda2 = df_demanda[(df_demanda['date_co'].isna()) & (df_demanda['date_co'] == '')]


list_clienta = list(set(df_demanda['user_id']))




query_clienta = 'user_id in @list_clienta'

df_clienta = pd.read_csv(file_clienta,
                           usecols=['user_id', 'price_range', 'age_segment', 'email']).query(query_clienta).rename(columns={'price_range': 'clienta_price_range'})



df = pd.merge(df_demanda, df_clienta, on='user_id', how='left')

df['precio_bruto_purchased'] = df['precio_bruto'] * df['purchased']
df['precio_bruto_purchased'] = df['precio_bruto_purchased'].replace(0, np.NaN)

df_gr = df.groupby(['family_desc', 'clienta_price_range']).agg({'date_co': 'count',
                                                        'purchased': 'sum',
                                                        'precio_bruto': 'mean',
                                                                'precio_bruto_purchased': 'mean'}).reset_index().rename(columns={'date_co': 'envios', 'precio_bruto': 'precio_bruto_envios'})

df_gr['KR'] = df_gr['purchased'] / df_gr['envios']

df_gr = df_gr[['family_desc', 'clienta_price_range', 'envios', 'purchased', 'KR', 'precio_bruto_envios', 'precio_bruto_purchased']]

# df['precio_bruto_purchased1'] = df['precio_bruto_purchased']
#

df_gr.to_excel('/home/darya/Documents/projects/Reports/2021-06-07-family-client-price-kr/Familia-precio-clienta-envios-KR-jun2020-may2021.xlsx', index=False)
