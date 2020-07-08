'''
Calculate number of items with and without sales for specific week
'''
import pandas as pd

import seaborn as sns
import numpy as np


demanda_file = ('/var/lib/lookiero/stock/stock_tool/demanda_preprocessed.csv')


# 29.06.2020 y 05.06.2020
query_text = 'date_ps_done >= "2020-06-29" & date_ps_done <= "2020-07-05" '

df_demanda_all = pd.read_csv(demanda_file,
                           usecols=['date_ps_done', 'rebaja', 'country', 'family_desc', 'user_id', 'box_id',
                                    'reference', 'family', 'precio_bruto', 'precio_neto', 'date_co']
                           ).query(query_text)

# ['family_desc', 'user_id', 'box_id', 'reference', 'date_ps_done',
#        'rejection_quality', 'rejection_style', 'rejection_size',
#        'rejection_price', 'rejection_fit', 'atemporal', 'box_discountAmount',
#        'brand', 'client_is_big_size', 'country', 'date_cancelled', 'date_co',
#        'date_created', 'date_delivered', 'date_dispatched', 'date_erp_ok',
#        'date_in_return', 'date_prepared', 'date_terminated', 'family',
#        'is_first_box', 'is_printed', 'paid', 'precio_bruto', 'precio_neto',
#        'price_range', 'ps_id', 'purchased', 'size', 'temporada_AWSS_caja',
#        'temporada_AWSS_prenda', 'temporada_caja', 'temporada_prenda', 'color',
#        'modelo', 'rebaja', 'exclusivo', 'rej_fit', 'rej_price', 'rej_size',
#        'rej_style', 'rej_quality', 'box_is_ok', 'n_cajas_misma_clienta',
#        'n_cajas_12_meses', 'box_number', 'date_last_box',
#        'time_since_last_box', 'precio_catalogo', 'size_ord', 'size_num',
#        'comp_ABRIGO', 'comp_BLUSA', 'comp_BOLSO', 'comp_BUFANDA',
#        'comp_CAMISETA', 'comp_CARDIGAN', 'comp_CHAQUETA', 'comp_DENIM',
#        'comp_FALDA', 'comp_FULAR', 'comp_GORRO', 'comp_JERSEY',
#        'comp_JUMPSUIT', 'comp_PANTALON', 'comp_PARKA', 'comp_SHORT',
#        'comp_SUDADERA', 'comp_TOP', 'comp_TRENCH', 'comp_VESTIDO',
#        'composicion', 'talla_arriba', 'talla_abajo', 'parte_arriba_abajo',
#        'size_client']

# df_demanda_all['rebaja_si'] = df_demanda_all['rebaja']
# df_demanda_all['rebaja_no'] = df_demanda_all['rebaja']



df_demanda_all.loc[df_demanda_all['rebaja'] > 0, 'rebaja_si'] = 1
df_demanda_all.loc[df_demanda_all['rebaja'] == 0, 'rebaja_no'] = 1

#df_demanda_all_nan = df_demanda_all.fillna(0)

df_rebaja = df_demanda_all.groupby(['country']).agg({'rebaja_si': 'sum', 'rebaja_no': 'sum'}).reset_index()


# chechout
df_demanda_all['co_binary'] = df_demanda_all['date_co'].where(df_demanda_all['date_co'].isnull(), 1).fillna(0).astype(int)
df_demanda_co = df_demanda_all[df_demanda_all['co_binary'] == 1]

df_demanda_co_rebaja = df_demanda_co.groupby(['country']).agg({'rebaja_si': 'sum', 'rebaja_no': 'sum'}).reset_index()




#
# rebajas_recientes_envios_pais <-
#     demanda[!is.na(precio_bruto) & !is.na(precio_neto),
#             .(
#               total_sin_rebaja_uds = round(sum(precio_bruto == precio_neto)),
#               total_con_rebaja_uds = round(sum(precio_bruto > precio_neto)),
#               total_sin_rebaja_eur = round(sum(precio_neto[precio_bruto == precio_neto])),
#               total_con_rebaja_eur = round(sum(precio_neto[precio_bruto > precio_neto])),
#               proporcion_rebajas_uds = sum(precio_bruto > precio_neto) / .N,
#               proporcion_rebajas_eur = sum(precio_neto[precio_bruto > precio_neto]) / sum(precio_neto),
#               rebaja_media = (sum(precio_bruto[precio_bruto > precio_neto]) - sum(precio_neto[precio_bruto > precio_neto])) / sum(precio_bruto[precio_bruto > precio_neto]),
#               uds_sin_rebaja = sum(rebaja==0),
#               uds_con_rebaja = sum(rebaja!=0),
#               uds_rebaja_10 = sum(rebaja==0.10),
#               uds_rebaja_15 = sum(rebaja==0.15),
#               uds_rebaja_20 = sum(rebaja==0.20),
#               uds_rebaja_30 = sum(rebaja==0.30),
#               uds_rebaja_50 = sum(rebaja==0.50)
#             ),.(
#               country,date_ps_done)][order(country,-date_ps_done)][Sys.Date() - date_ps_done <= 30]


