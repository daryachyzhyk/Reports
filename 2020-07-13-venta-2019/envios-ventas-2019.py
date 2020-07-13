'''Envios y ventas de 2019 por mes.
Columnas:
- mes
- familia
- talla
- envios
- enviod checkout
- venta

'''



import pandas as pd

import seaborn as sns
import numpy as np


demanda_file = ('/var/lib/lookiero/stock/stock_tool/demanda_preprocessed.csv')


# 29.06.2020 y 05.06.2020
query_text = 'date_ps_done >= "2019-01-01" & date_ps_done <= "2019-12-31" '

df_demanda_all = pd.read_csv(demanda_file,
                           usecols=['date_ps_done', 'country', 'family_desc', 'user_id', 'box_id',
                                    'reference', 'size', 'date_co', 'purchased', 'paid']
                           ).query(query_text)

# df_demanda_all['date_ps_done'] = pd.to_datetime(df_demanda_all['date_ps_done'])
#
# df_demanda_all['date_ps_done1'] = df_demanda_all['date_ps_done'].apply(lambda x: x.strftime('%Y-%m'))
#
# df_demanda_all['date_ps_done1'] = pd.DatetimeIndex(df_demanda_all['date_ps_done']).month

df_demanda_all['date_month'] = pd.to_datetime(df_demanda_all['date_ps_done']).dt.to_period('M')

df_demanda_all = df_demanda_all.dropna(subset=['family_desc', 'size'])

df = df_demanda_all.groupby(['date_month', 'family_desc', 'size']).agg({'date_ps_done': 'count',
                                                                        'date_co': 'count',
                                                                        'purchased': 'sum'}).reset_index()

df = df.rename(columns={'date_ps_done': 'envios',
                        'date_co': 'chechouts'})

df[['envios', 'chechouts', 'purchased']] = df[['envios', 'chechouts', 'purchased']].astype(int)
df.to_csv('/home/darya/Documents/Reports/2020-07-13-venta-2019/report-venta-checkouts-purchased-2019.csv')