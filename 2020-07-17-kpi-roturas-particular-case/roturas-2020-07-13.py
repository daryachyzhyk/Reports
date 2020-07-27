
import os
import glob
import pandas as pd
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt

import datetime
# from datetime import datetime, timedelta, date

####################################################################################################################
# indicar parametros

fecha_stock_actual_start_str = '2020-07-13'
fecha_stock_actual_end_str = '2020-07-19'

fecha_compra = '2020-06-17'

fecha_pedido_start = '2020-06-17'
fecha_pedido_end = '2020-06-30'


familias_interes = ['BLUSA', 'CAMISETA', 'FALDA', 'JUMPSUIT', 'SHORT', 'TOP', 'VESTIDO']

dic_clima = {0.: 'cold',
             0.5: 'cold_soft_cold',
             1.: 'soft_cold',
             1.5: 'soft_cold_soft_warm',
             2.: 'soft_warm',
             2.5: 'soft_warm_warm',
             3.:  'warm'}

dic_clima = {0.: '0_invierno',
             0.5: '0.5_invierno',
             1.: '1_invierno',
             1.5: '1.5_entretiempo',
             2.: '2_verano',
             2.5: '2.5_verano',
             3.:  '3_verano'}

dic_clima = {0.: '0_inv',
             0.5: '0.5_inv',
             1.: '1_inv',
             1.5: '1.5_ent',
             2.: '2_ver',
             2.5: '2.5_ver',
             3.:  '3_ver'}
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
fecha_stock_actual_start = datetime.datetime.strptime(fecha_stock_actual_start_str, '%Y-%m-%d')
fecha_stock_actual_end = datetime.datetime.strptime(fecha_stock_actual_end_str, '%Y-%m-%d')

delta_fecha_stock_actual = fecha_stock_actual_end - fecha_stock_actual_start

########################################
# stock actual
# stock_actual_file = []
df_stock_all = pd.DataFrame([])

for i in range(delta_fecha_stock_actual.days + 1):
    day = fecha_stock_actual_start + datetime.timedelta(days=i)
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


df_stock_all = df_stock_all.rename(columns={'real_stock': 'stock_real_week'})
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



df_stock_familia_talla = df_stock_reference.groupby(['family_desc', 'size']).agg({'stock_real_week': 'sum',
                                                                                  'date': 'last'}).reset_index()

df_stock_familia_clima = df_stock_reference.groupby(['family_desc', 'clima']).agg({'stock_real_week': 'sum',
                                                                                   'date': 'last'}).reset_index()

df_stock_familia_clima['clima_desc'] = df_stock_familia_clima['clima'].replace(dic_clima)
df_stock_familia_clima['clima'] = df_stock_familia_clima['clima'].astype(str)

df_stock_familia_talla['stock_real'] = (df_stock_familia_talla['stock_real_week'] / 7)

df_stock_familia_clima['stock_real'] = (df_stock_familia_clima['stock_real_week'] / 7)



# df_stock_familia_talla['stock_mean'] = (df_stock_familia_talla['real_stock'] / 7).round(0)
# df_stock_familia_talla['stock_mean1'] = (df_stock_familia_talla['real_stock'] / 7).apply(np.ceil)


################################################################3
# proyeccion_stock de Stuart

df_proyeccion_all = pd.read_csv(stock_proyeccion_file)


df_proyeccion_week = df_proyeccion_all[df_proyeccion_all['week'] == fecha_stock_actual_start_str]

df_proyeccion_familia_talla = df_proyeccion_week[df_proyeccion_all['caracteristica'] == 'tallas']

df_proyeccion_familia_clima = df_proyeccion_week[df_proyeccion_all['caracteristica'] == 'clima_valor']

df_proyeccion_familia_talla['size'] = df_proyeccion_familia_talla['clase'].str.split('-').str[1]

df_proyeccion_familia_talla = df_proyeccion_familia_talla.rename(columns={'clase': 'size_ord',
                                                                          'posicion': 'stock_proyeccion'})
df_proyeccion_familia_clima = df_proyeccion_familia_clima.rename(columns={'clase': 'clima',
                                                                          'posicion': 'stock_proyeccion'})
###############################################################
# Stock Stuart vs real

df_stock_real_proyeccion_familia_talla = pd.merge(df_stock_familia_talla[['family_desc', 'size', 'stock_real']],
                                                  df_proyeccion_familia_talla[['family_desc', 'size', 'stock_proyeccion', 'size_ord']],
                                                  on=['family_desc', 'size'])

# df_stock_real_proyeccion_familia_talla = df_stock_real_proyeccion_familia_talla.rename(columns={'stock_mean': 'stock_real'})



# TODO: rename clima values
# clima
df_stock_real_proyeccion_familia_clima = pd.merge(df_stock_familia_clima[['family_desc', 'clima', 'stock_real', 'clima_desc']],
                                                  df_proyeccion_familia_clima[['family_desc', 'clima', 'stock_proyeccion']],
                                                  on=['family_desc', 'clima'])

# df_stock_real_proyeccion_familia_clima = df_stock_real_proyeccion_familia_clima.rename(columns={'stock_mean': 'stock_real'})

#######################################
# plot familia talla


df_plot_proyeccion_real_ft = df_stock_real_proyeccion_familia_talla[df_stock_real_proyeccion_familia_talla['family_desc'].isin(familias_interes)]

df_plot_proyeccion_real_ft_melt = pd.melt(df_plot_proyeccion_real_ft,
                                                             id_vars=['family_desc', 'size', 'size_ord'],
                                                             value_vars=['stock_real', 'stock_proyeccion'],
                                                             var_name='stock_type',
                                                             value_name='stock')

df_plot_proyeccion_real_ft_melt = df_plot_proyeccion_real_ft_melt.sort_values(by=['family_desc', 'size_ord'])



sns.set(font_scale=1.5)
g = sns.catplot(data=df_plot_proyeccion_real_ft_melt,
                x="size",
                y="stock",
                hue='stock_type',
                col="family_desc",
                col_wrap=3,
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

g.fig.subplots_adjust(top=0.92)

g.savefig(os.path.join(path_results, "plot_stock_real_proyected_familia_talla.png"))

###############################################
# plot familia - clima

df_plot_proyeccion_real_fc = df_stock_real_proyeccion_familia_clima[df_stock_real_proyeccion_familia_clima['family_desc'].isin(familias_interes)]

df_plot_proyeccion_real_fc_melt = pd.melt(df_plot_proyeccion_real_fc,
                                                             id_vars=['family_desc', 'clima', 'clima_desc'],
                                                             value_vars=['stock_real', 'stock_proyeccion'],
                                                             var_name='stock_type',
                                                             value_name='stock')

# df_plot_proyeccion_real_ft_melt = df_plot_proyeccion_real_ft_melt.sort_values(by=['family_desc', 'size_ord'])



sns.set(font_scale=1.5)
g = sns.catplot(data=df_plot_proyeccion_real_fc_melt,
                x="clima_desc",
                y="stock",
                hue='stock_type',
                col="family_desc",
                col_wrap=3,
                kind="bar",
                aspect=0.8,
                palette='muted',
                ci=None,
                sharey=False,
                sharex=False,
                legend=False
                )

for ax in g.axes.ravel():
    # ax.axhline(0, color="k", clip_on=False)
    ax.set_xticklabels(ax.get_xticklabels(), fontsize=14, rotation=0) # 90
#
# ax.text(10.5, 0.85, 'GOAL')
#
(g.set_axis_labels("", "Unidades").set_titles("{col_name}"))
#
plt.legend(loc=(2, 0))  # bbox_to_anchor=(1.5, 0),
# plt.tight_layout()
g.fig.suptitle('Stock real vs Proyeccion de Stuart, familia - clima')
g.fig.subplots_adjust(top=0.92)

g.savefig(os.path.join(path_results, "plot_stock_real_proyected_familia_clima.png"))



###############################################################################################################
# Stuart vs Compra

########################################################33



# recomendacion de stuart
stuart_output_file = ('/var/lib/lookiero/stock/stock_tool/stuart/20200616/stuart_output_todos.csv.gz')


# compra realizada
compra_actual_file = ('/home/darya/Documents/stuart/data/kpi/Compra-2020-06-17.xlsx')
compra_anterior_file = ('/var/lib/lookiero/stock/stock_tool/stuart/pedidos.csv.gz')

# venta
venta_file = ('/var/lib/lookiero/stock/stock_tool/demanda_preprocessed.csv.gz')

# # precios_compra_file = ('/home/darya/Documents/stuart/data/kpi/precio_compra.csv')
#
# path_results = ('/home/darya/Documents/Reports/2020-07-17-kpi-roturas-particular-case')
#
##
# # pedodis recibidos
# pedidos_recibidos_fecha = datetime.datetime.strptime(fecha_compra, '%Y-%m-%d').strftime('%d%m%y')
# pedidos_recibidos_file = os.path.join('/var/lib/lookiero/stock/Pedidos_recibidos', '')
#
#
# pedidos_recibidos = ('')



##########################################################################################3
# stuart recommendation


df_stuart_all = pd.read_csv(stuart_output_file)

# talla
df_stuart_familia_talla = df_stuart_all[(df_stuart_all['caracteristica'] == 'tallas') & (df_stuart_all['family_desc'].isin(familias_interes))]
df_stuart_familia_talla['size'] = df_stuart_familia_talla['clase'].str.split('-').str[1]
df_stuart_familia_talla = df_stuart_familia_talla.rename(columns={'clase': 'size_ord'})


# clima
df_stuart_familia_clima = df_stuart_all[(df_stuart_all['caracteristica'] == 'clima_valor') & (df_stuart_all['family_desc'].isin(familias_interes))]

df_stuart_familia_clima = df_stuart_familia_clima.rename(columns={'clase': 'clima'})

df_stuart_familia_clima['clima_desc'] = df_stuart_familia_clima['clima'].replace(dic_clima)
df_stuart_familia_clima['clima'] = df_stuart_familia_clima['clima'].astype(str)


##############################################################################################################
# compra real


df_compra_actual_all = pd.read_excel(compra_actual_file)




df_compra_anterior_all = pd.read_csv(compra_anterior_file, encoding="ISO-8859-1")

# rename and clean the data of the real compra

df_compra_actual = df_compra_actual_all[['Artículo', 'Artículo -> Grupo -> Temporada',
                                         'Artículo -> Grupo -> Familia -> Nombre',
                                         'Artículo -> Talla',
                                         'Cantidad pedida',
                                         'Cantidad Entregada',
                                         'Cantidad pendiente',
                                         'Ultima recepción -> Compra -> Fecha',
                                         'Fecha prevista de entrega',
                                         'Artículo -> Precio de Venta (con Iva)',
                                         'Pedidos -> Pedido']]

df_compra_actual = df_compra_actual.rename(columns={'Artículo': 'reference',
                                                    'Artículo -> Grupo -> Temporada': 'temporada',
                                                    'Artículo -> Grupo -> Familia -> Nombre': 'family_desc',
                                                    'Artículo -> Talla': 'size',
                                                    'Cantidad pedida': 'cantidad_pedida',
                                                    'Cantidad Entregada': 'cantidad_entregada',
                                                    'Cantidad pendiente': 'cantidad_pendiente',
                                                    'Ultima recepción -> Compra -> Fecha': 'fecha_recepcion',
                                                    'Fecha prevista de entrega': 'fecha_prevista_entrega',
                                                    'Artículo -> Precio de Venta (con Iva)': 'precio_venta_coniva',
                                                    'Pedidos -> Pedido': 'numero_pedido'})

df_compra_actual.loc[df_compra_actual['family_desc'] == 'DENIM JEANS', 'family_desc'] = 'DENIM'

# TODO : añadir clima

# info de cada prenda
list_reference_compra = df_compra_actual["reference"].to_list()
query_product_text = 'reference in @list_reference_compra'


df_productos_compra = pd.read_csv(productos_file,
                           usecols=['reference', 'clima'] # , 'clima_grupo'
                           ).query(query_product_text)



df_compra_actual_ftc = pd.merge(df_compra_actual,
                                df_productos_compra,
                                on=['reference'],
                                how='left')

df_compra_actual_ft = df_compra_actual_ftc.groupby(['family_desc', 'size']).agg({'cantidad_pedida': 'sum',
                                                                                 # 'cantidad_entregada': 'sum',
                                                                                 # 'cantidad_pendiente': 'sum'
                                                                                 }).reset_index()
df_compra_actual_fc = df_compra_actual_ftc.groupby(['family_desc', 'clima']).agg({'cantidad_pedida': 'sum'}).reset_index()


df_compra_actual_familia_talla = df_compra_actual_ft[df_compra_actual_ft['family_desc'].isin(familias_interes)]

df_compra_actual_familia_clima = df_compra_actual_fc[df_compra_actual_fc['family_desc'].isin(familias_interes)]


# talla
df_stuart_compra_actual_ft = pd.merge(df_stuart_familia_talla[['family_desc', 'size', 'size_ord', 'recomendacion']],
                                      df_compra_actual_familia_talla,
                                      on=['family_desc', 'size'],
                                      how='left')

df_stuart_compra_actual_ft['cantidad_pedida'] = df_stuart_compra_actual_ft['cantidad_pedida'].fillna(0)

df_stuart_compra_actual_ft_plot = df_stuart_compra_actual_ft.melt(id_vars=['family_desc', 'size', 'size_ord'],
                                                                  value_vars=['recomendacion', 'cantidad_pedida'],
                                                                  var_name='stuart_compra', value_name='cantidad')

# clima
df_compra_actual_familia_clima['clima'] = df_compra_actual_familia_clima['clima'].astype(str)

df_stuart_compra_actual_fc = pd.merge(df_stuart_familia_clima[['family_desc', 'clima', 'clima_desc', 'recomendacion']],
                                      df_compra_actual_familia_clima,
                                      on=['family_desc', 'clima'],
                                      how='left')

df_stuart_compra_actual_fc['cantidad_pedida'] = df_stuart_compra_actual_fc['cantidad_pedida'].fillna(0)

df_stuart_compra_actual_fc_plot = df_stuart_compra_actual_fc.melt(id_vars=['family_desc', 'clima', 'clima_desc'],
                                                                  value_vars=['recomendacion', 'cantidad_pedida'],
                                                                  var_name='stuart_compra', value_name='cantidad')



# talla

sns.set(font_scale=1.5)
g = sns.catplot(data=df_stuart_compra_actual_ft_plot,
                x="size",
                y="cantidad",
                hue='stuart_compra',
                col="family_desc",
                col_wrap=3,
                kind="bar",
                aspect=0.8,
                palette='muted',
                ci=None,
                sharey=False,
                sharex=False,
                legend=False
                )

for ax in g.axes.ravel():
    # ax.axhline(0, color="k", clip_on=False)
    ax.set_xticklabels(ax.get_xticklabels(), fontsize=14, rotation=90)
#
# ax.text(10.5, 0.85, 'GOAL')
#
(g.set_axis_labels("", "Unidades").set_titles("{col_name}"))
#
plt.legend(loc=(2, 0))  # bbox_to_anchor=(1.5, 0),
# plt.tight_layout()
g.fig.suptitle('Stuart recomendacion vs compra real, familia - talla')

g.fig.subplots_adjust(top=0.92)

g.savefig(os.path.join(path_results, "plot_stuart_compra_familia_talla.png"))


# clima

sns.set(font_scale=1.5)
g = sns.catplot(data=df_stuart_compra_actual_fc_plot,
                x="clima_desc",
                y="cantidad",
                hue='stuart_compra',
                col="family_desc",
                col_wrap=3,
                kind="bar",
                aspect=0.8,
                palette='muted',
                ci=None,
                sharey=False,
                sharex=False,
                legend=False
                )

for ax in g.axes.ravel():
    # ax.axhline(0, color="k", clip_on=False)
    ax.set_xticklabels(ax.get_xticklabels(), fontsize=14, rotation=90)
#
# ax.text(10.5, 0.85, 'GOAL')
#
(g.set_axis_labels("", "Unidades").set_titles("{col_name}"))
#
plt.legend(loc=(2, 0))  # bbox_to_anchor=(1.5, 0),
# plt.tight_layout()
g.fig.suptitle('Stuart recomendacion vs compra real, familia - clima')

g.fig.subplots_adjust(top=0.92)

g.savefig(os.path.join(path_results, "plot_stuart_compra_familia_clima.png"))

#################################################################3
# Pendientes




def get_current_season(date_):
    if isinstance(fecha_stock_actual_start, datetime.datetime):
        date_fisrt_season = datetime.datetime(2016, 1, 1)

        # delta_month = (date_.year - date_fisrt_season.year) * 12 + date_.month - date_fisrt_season.month

        delta_season = (date_.year - date_fisrt_season.year) * 2
        if date_.month <= 6:
            season = delta_season + 1
        else:
            season = delta_season + 2
    else:
        print('Shoud be datetime')
        season = np.nan()
    return season

season_actual = get_current_season(fecha_stock_actual_start)


# campos fichero PENDIENTES.txt
# "reference","pendiente","date","family","family_desc","color","temporada","size","brand","precio_compra","precio_compra_iva","precio_compra_libras","precio_compra_libras_iva","NA"
# date es fecha prevista de recibir
# campos fichero PEDIDOS_RECIBIDOS.txt
# "date","reference","pedidos","family","family_desc","date2","brand","precio_compra","precio_compra_iva","precio_compra_libras","precio_compra_libras_iva","NA"
# date es fecha de recepción

# datetime.

# pendientes_file = ('/var/lib/lookiero/stock/stock_tool/stuart2/20200702/pedidos.csv.gz')

pendientes_folder = ('/var/lib/lookiero/stock/Pendiente_llegar')

pendientes_fecha_start = fecha_stock_actual_start.strftime('%d%m%Y')

pendientes_file = os.path.join(pendientes_folder, 'PENDIENTES_' + pendientes_fecha_start + '.txt')
df_pendientes_raw = pd.read_csv(pendientes_file, sep=";", header=None, error_bad_lines=False, encoding="ISO-8859-1")
df_pendientes_raw = df_pendientes_raw.drop(df_pendientes_raw.columns[-1], axis=1)

df_pendientes_raw.columns = ["reference", "pendiente", "date", "family", "family_desc", "color", "temporada", "size",
                             "brand", "precio_compra", "precio_compra_iva", "precio_compra_libras",
                             "precio_compra_libras_iva"]
# print(df.Geek_ID.str.split('_').str[1].tolist())
# aa = df_pendientes_raw['reference'].str.extract('(?:(.*\d))?(?:([a-zA-Z]+))?')
df_pendientes_raw['season'] = df_pendientes_raw['reference'].str.extract('(^[0-9]+)')

df_pendientes_raw['season'] = df_pendientes_raw['season'].fillna('0')
df_pendientes_raw['season'] = df_pendientes_raw['season'].astype(int)
# (?:.+?(?=C))(C[0-9]+)(?:[0-9]{2}$|[^0-9]+$|X4XL+$)
# df_pendientes_raw['season'] = df_pendientes_raw['reference'].str.extract('(?:(.*\d))?(?:([a-zA-Z]+))?')



df_pendientes_all = df_pendientes_raw[df_pendientes_raw['season'] >= season_actual - 1]





# df_pendientes_raw = pd.read_csv(pendientes_file, encoding="ISO-8859-1")


df_pendientes_all = df_pendientes_raw[df_pendientes_raw['reference'].isin(list_reference_compra)]





