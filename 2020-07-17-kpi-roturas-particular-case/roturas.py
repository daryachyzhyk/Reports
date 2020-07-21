
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

familias_interes = ['BLUSA', 'CAMISETA', 'FALDA', 'JUMPSUIT', 'SHORT', 'TOP', 'VESTIDO']
#####################################################################################################################
# path
# stock_fecha = fecha_stock_actual.replace("-", "")
stock_path = ('/var/lib/lookiero/stock/snapshots')

productos_file = ('/var/lib/lookiero/stock/stock_tool/productos_preprocessed.csv.gz')

stock_proyeccion_file = ('/var/lib/lookiero/stock/stock_tool/stuart/20200616/proyeccion_stock_todos.csv.gz')

stuart_file = ()


path_results = ('/home/darya/Documents/Reports/2020-07-17-kpi-roturas-particular-case')


######
# fechas
fecha_stock_actual_start = datetime.strptime(fecha_stock_actual_start_str, '%Y-%m-%d')
fecha_stock_actual_end = datetime.strptime(fecha_stock_actual_end_str, '%Y-%m-%d')

delta_fecha_stock_actual = fecha_stock_actual_end - fecha_stock_actual_start

########################################
# stock actual
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




    query_stock_text = 'real_stock > 0 and active > 0'

    df_stock_day = pd.read_csv(stock_file,
                               usecols=['reference', 'family', 'real_stock', 'active']
                               ).query(query_stock_text)
    df_stock_day['date'] = day

    df_stock_all = df_stock_all.append(df_stock_day)



#######################################################
# info de cada prenda
list_reference_stock = df_stock_all["reference"].to_list()
query_product_text = 'reference in @list_reference_stock'


df_productos = pd.read_csv(productos_file,
                           usecols=['reference', 'family_desc', 'size', 'clima'] # , 'clima_grupo'
                           ).query(query_product_text)

#########################################################
# stock actual añadir familia, talla, clima

df_stock_reference = pd.merge(df_stock_all, df_productos, on='reference', how='left')

df_stock_familia_talla = df_stock_reference.groupby(['family_desc', 'size']).agg({'real_stock': 'sum',
                                                                                  'date': 'last'}).reset_index()

df_stock_familia_clima = df_stock_reference.groupby(['family_desc', 'clima']).agg({'real_stock': 'sum',
                                                                                   'date': 'last'}).reset_index()

df_stock_familia_talla['stock_mean'] = (df_stock_familia_talla['real_stock'] / 7)

df_stock_familia_clima['stock_mean'] = (df_stock_familia_clima['real_stock'] / 7)


# df_stock_familia_talla['stock_mean'] = (df_stock_familia_talla['real_stock'] / 7).round(0)
# df_stock_familia_talla['stock_mean1'] = (df_stock_familia_talla['real_stock'] / 7).apply(np.ceil)


################################################################3
# proyeccion_stock de Stuart

df_proyeccion_all = pd.read_csv(stock_proyeccion_file)


df_proyeccion_week = df_proyeccion_all[df_proyeccion_all['week'] == fecha_stock_actual_start_str]

df_proyeccion_familia_talla = df_proyeccion_week[df_proyeccion_all['caracteristica'] == 'tallas']

df_proyeccion_familia_clima = df_proyeccion_week[df_proyeccion_all['caracteristica'] == 'clima_valor']

df_proyeccion_familia_talla['size'] = df_proyeccion_familia_talla['clase'].str.split('-').str[1]

df_proyeccion_familia_talla = df_proyeccion_familia_talla.rename(columns={'clase': 'size_ord'})
df_proyeccion_familia_clima = df_proyeccion_familia_clima.rename(columns={'clase': 'clima'})
###############################################################
# Stock Stuart vs real
# TODO: check name of the projected stock
df_stock_real_proyeccion_familia_talla = pd.merge(df_stock_familia_talla[['family_desc', 'size', 'stock_mean']],
                                                  df_proyeccion_familia_talla[['family_desc', 'size', 'posicion', 'size_ord']],
                                                  on=['family_desc', 'size'])

#######################################
# plot familia talla


df_plot_proyeccion_real_ft = df_stock_real_proyeccion_familia_talla[df_stock_real_proyeccion_familia_talla['family_desc'].isin(familias_interes)]

df_plot_proyeccion_real_ft_melt = pd.melt(df_plot_proyeccion_real_ft,
                                                             id_vars=['family_desc', 'size', 'size_ord'],
                                                             value_vars=['stock_mean', 'posicion'],
                                                             var_name='proyeccion/real',
                                                             value_name='stock')

df_plot_proyeccion_real_ft_melt = df_plot_proyeccion_real_ft_melt.sort_values(by=['family_desc', 'size_ord'])



sns.set(font_scale=1.5)
g = sns.catplot(data=df_plot_proyeccion_real_ft_melt,
                x="size",
                y="stock",
                hue='proyeccion/real',
                col="family_desc",
                #col_wrap=7,
                kind="bar",
                aspect=0.8,
                palette='muted',
                ci=None,
                sharey=False,
                sharex=False,
                legend=True
                )

for ax in g.axes.ravel():
    # ax.axhline(0, color="k", clip_on=False)
    ax.set_xticklabels(ax.get_xticklabels(), fontsize=14, rotation=90)
#
# ax.text(10.5, 0.85, 'GOAL')
#
(g.set_axis_labels("", "Unidades").set_titles("{col_name}"))
#
# plt.legend(loc=(2, 0))  # bbox_to_anchor=(1.5, 0),
# plt.tight_layout()
g.fig.suptitle('Stock real vs Proyeccion de Stuart, familia - talla')
g.fig.subplots_adjust(top=0.85)

g.savefig(os.path.join(path_results, "plot_stock_real_proyected_familia_talla.png"))



###############################################################################################################
# Stuart vs Compra

########################################################33

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