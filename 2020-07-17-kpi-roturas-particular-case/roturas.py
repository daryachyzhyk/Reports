
import os
import glob
import pandas as pd
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt

from datetime import datetime, timedelta

####################################################################################################################
# indicar parametros

fecha_stock_actual_start_str = '2020-07-13'
fecha_stock_actual_end_str = '2020-07-19'

fecha_compra = '2020-06-17'


######
# fechas
fecha_stock_actual_start = datetime.strptime(fecha_stock_actual_start_str, '%Y-%m-%d')
fecha_stock_actual_end = datetime.strptime(fecha_stock_actual_end_str, '%Y-%m-%d')

delta_fecha_stock_actual = fecha_stock_actual_end - fecha_stock_actual_start
#####################################################################################################################
# path
# stock_fecha = fecha_stock_actual.replace("-", "")
stock_path = ('/var/lib/lookiero/stock/snapshots')

# stock_actual_file = []
df_stock_all = pd.DataFrame([])

for i in range(delta_fecha_stock_actual.days + 1):
    day = fecha_stock_actual_start + timedelta(days=i)
    print(day)

    stock_fecha = day.strftime('%Y%m%d')

    # list_dates =

    # stock_fecha = datetime.datetime.strptime(fecha_compra, '%Y-%m-%d').strftime('%Y%m%d')

    stock_file = sorted(glob.glob(os.path.join(stock_path, stock_fecha + '*')))[0]

    print(stock_file)




    query_venta_text = 'real_stock > 0 and active > 0'

    df_stock_day = pd.read_csv(stock_file,
                               usecols=['reference', 'family', 'real_stock', 'active']
                               ).query(query_venta_text)
    df_stock_day['date'] = day

    df_stock_all = df_stock_all.append(df_stock_day)


# pedodis recibidos
pedidos_recibidos_fecha = datetime.datetime.strptime(fecha_compra, '%Y-%m-%d').strftime('%d%m%y')
pedidos_recibidos_file = os.path.join('/var/lib/lookiero/stock/Pedidos_recibidos', '')


pedidos_recibidos = ('')




# # recomendacion de stuart
# stuart_output_file1 = ('/var/lib/lookiero/stock/stock_tool/stuart/20200511/stuart_output.csv')
# stuart_output_file2 = ('/var/lib/lookiero/stock/stock_tool/stuart/20200525/stuart_output.csv')
#
# # compra realizada
# compra_actual_file = ('/home/darya/Documents/stuart/data/kpi/kpi-20200511/compra-referencias-11-25-05-2020.csv')
# compra_anterior_file = ('/var/lib/lookiero/stock/stock_tool/stuart/pedidos.csv')
#
# # venta
# venta_file = ('/var/lib/lookiero/stock/stock_tool/demanda_preprocessed.csv')
#
# precios_compra_file = ('/home/darya/Documents/stuart/data/kpi/precio_compra.csv')

path_results = ('/home/darya/Documents/Reports/2020-07-17-kpi-roturas-particular-case')



#######################################################################################################################
# load data

# stock actual

query_venta_text = 'real_stock > 0 and active > 0'

df_stock_all = pd.read_csv(stock_file,
                           usecols=['reference', 'family', 'real_stock', 'active']
                           ).query(query_venta_text)


####################################
# promedio de stock actual




# stuart recommendation
df_stuart_all1 = pd.read_csv(stuart_output_file1, usecols=['family_desc', 'size', 'recomendacion', 'size_ord'])
df_stuart_all2 = pd.read_csv(stuart_output_file2, usecols=['family_desc', 'size', 'recomendacion', 'size_ord'])
df_stuart_all1 = df_stuart_all1.rename(columns={'recomendacion': 'recomendacion1'})
df_stuart_all2 = df_stuart_all2.rename(columns={'recomendacion': 'recomendacion2'})

df_stuart = pd.merge(df_stuart_all1, df_stuart_all2, on=['family_desc', 'size', 'size_ord'])

df_stuart['recomendacion'] = df_stuart['recomendacion1'] + df_stuart['recomendacion2']

df_stuart['recomendacion'] = df_stuart['recomendacion'].round(0)