"""
DENIM

VARIABLES PRINCIPALES		Campos

Tipo		Pitillo (Tipo Pitillo y Skinny)	Recto	Otros (Todos los demás)
Color		Azul oscuro (C22)	Azul (C3, C17, C26)	Negro (C2)	Gris	Otros
Precio		Precio bajo (Hasta 34)	Precio medio (35-59)	Precio alto (60+)
Largura		Corto	Medio	Largo

VARIABLES SECUNDARIAS		Campos

Acabado		Roto	Desgastado	Deshilachado

SIEMPRE TIENE QUE HABER:
Pitillo azul precio medio largo medio
Pitillo negro Precio medio largo medio
Pitillo gris precio medio largo medio

PARA AÑADIR VARIEDAD NECESITAMOS:
Diferentes tipos que no sean pitillo
Diferente precio, sobre todos alto
Larguras cortas y largas


"""

# 1 - stock de Denim
# 2 - stock de denim el dia de 14 de abril
# 3 - title de Denim


import pandas as pd
import numpy as np
import itertools
import os
import plotly
import plotly.express as px
plotly.io.renderers.default = "browser"

file_modelos_features = '/home/darya/Documents/Reports/2021-02-23-client-categorization/Features_value_denim_jeans.xlsx'
file_product = '/var/lib/lookiero/stock/stock_tool/productos_preprocessed.csv.gz'

path_save = '/home/darya/Documents/Reports/2021-02-23-client-categorization'

df_modelos_raw = pd.read_excel(file_modelos_features, index_col=0)

list_modelo = list(set(df_modelos_raw['modelo']))
query_text = 'modelo in @list_modelo'
df_product = pd.read_csv(file_product, usecols=['modelo', 'precio_catalogo', 'color']).query(query_text)
df_product = df_product.drop_duplicates(subset=['modelo', 'color'])


df_modelos = pd.merge(df_modelos_raw, df_product, on='modelo', how='left')
# df_modelos = df_modelos_raw.copy()

# Tipo		Pitillo (Tipo Pitillo y Skinny)	Recto	Otros (Todos los demás)


df_modelos.loc[df_modelos['down_part_type'] == 'skinny', 'down_part_type'] = 'cigarette'

df_modelos.loc[(df_modelos['down_part_type'] != 'cigarette') &
               (df_modelos['down_part_type'] != 'straight'), 'down_part_type'] = 'type_other'

# Color		Azul oscuro (C22)	Azul (C3, C17, C26)	Negro (C2)	Gris (C8, C20, C31)	Blanco (C1, C15, C34, C53)	Otros
dic_color = {'azul_oscuro': ['C22'],
             'azul': ['C3', 'C17', 'C26'],
             'negro': ['C2'],
             'gris': ['C8', 'C20', 'C31'],
             'blanco': ['C1', 'C15', 'C34', 'C53']}

for key, val in dic_color.items():
    print(key, val)
    df_modelos.loc[df_modelos['color'].isin(val), 'color_categ'] = key

list_color = list((dic_color.keys())) + ['color_other']
df_modelos.loc[~df_modelos['color_categ'].isin(list_color), 'color_categ'] = 'color_other'

# Precio		Precio bajo (Hasta 34)	Precio medio (35-59)	Precio alto (60+)


bins_precio = [0, 35, 60, np.inf]
price_names = ['precio_bajo', 'precio_medio', 'precio_alto']
df_modelos['precio'] = pd.cut(df_modelos['precio_catalogo'], bins_precio, labels=price_names)

# Largura		Corto (94 cm y menos)	Medio	Largo (104 cm en adelante)

bins_long = [0, 94, 104, np.inf]
long_list = ['largura_corto', 'largura_medio', 'largura_largo']
df_modelos['largura'] = pd.cut(df_modelos['long_cm'], bins_long, labels=long_list)

# df_modelos.loc[~df_modelos['long_cm'].isin(lenght_list), 'long_cm'] = 'length_other'

dic_category = {'down_part_type': ['cigarette', 'straight', 'type_other'],
                'color_categ': list_color,
                'precio': price_names,
                'largura': long_list}

keys, values = zip(*dic_category.items())
permutations_dic = [dict(zip(keys, v)) for v in itertools.product(*values)]

# df_permutation = pd.DataFrame().from_dict(permutations_dicts)
df_categ_all = pd.DataFrame([])
categ_num = []
i = 0
for dic_categ in permutations_dic:
    print(dic_categ)
    categ_num.append(i)
    mask_categ = pd.DataFrame([df_modelos[key] == val for key, val in dic_categ.items()]).T.all(axis=1)

    df_categ = df_modelos[mask_categ]
    df_categ['categoria_desc'] = str(dic_categ)
    df_categ['categoria_num'] = i

    df_categ_all = df_categ_all.append(df_categ)
    i = i + 1
    print()




df_categ_all['modelo_num'] = df_categ_all['modelo']

df_categ_gr = df_categ_all.groupby(['categoria_num',
                                    'categoria_desc']).agg({'down_part_type': 'last',
                                                            'color_categ': 'last',
                                                            'precio': 'last',
                                                            'largura': 'last',
                                                            'modelo_num': 'count',
                                                            # 'noos': 'last',
                                                            # 'especial': 'last',
                                                            'modelo': lambda x: list(set(x)),
                                                            'imagen_prenda': 'last'}).reset_index()

df_categ_gr['modelo_pct'] = df_categ_gr['modelo_num'] * 100 / df_categ_gr['modelo_num'].sum()
# df_categ_gr.to_excel(os.path.join(path_save, 'denim_categorias.xlsx'))

# largira

# fig = px.sunburst(data_frame=df_categ_gr, path=['largura', 'down_part_type', 'color_categ', 'precio'],
#                   values='modelo_num')
# fig.show()

# import plotly as px
#######################################################################################################################
# plot diferentes secuencias del stock historico
def plot_sunburst(df, path_list, value, title_text, path_save, name_save):
    fig = px.sunburst(data_frame=df,
                      path=path_list,
                      values=value,
                      branchvalues="total")

    fig.update_layout(
        title={
            'text': title_text,
            'y': 0.9,
            'x': 0.13,
            'xanchor': 'center',
            'yanchor': 'top'})
    fig.update_traces(textinfo="label+percent root + value")
    fig['layout'].update(margin=dict(l=0, r=0, b=0, t=0))

    # fig.show()

    fig.write_html(os.path.join(path_save, name_save + '.html'))
    fig.write_image(os.path.join(path_save, name_save + '.png'))

# Del centro al exterior:
# Tipo-precio-color-largura
# Largura-tipo-precio-color
# Precio-Tipo-Largura-Color
# Color-tipo-precio-largura

plot_order_list = [['down_part_type', 'precio', 'color_categ', 'largura'],
                   ['largura', 'down_part_type', 'precio', 'color_categ'],
                   ['precio', 'down_part_type', 'largura', 'color_categ'],
                   ['color_categ', 'down_part_type', 'precio', 'largura']]

plot_value = 'modelo_num'
family_desc = 'DENIM'
careg_object_text = 'stock_historico'

for order_list in plot_order_list:


    plot_title_text = careg_object_text.replace('_', ' ') + ' ' + family_desc
    order_list_save = '_'.join(order_list)
    print('Plotting sunburst for ' + family_desc + ' for order ' + order_list_save)
    plot_name_save = 'sunburst_categor_' + family_desc + '_' + careg_object_text + '_' + order_list_save
    plot_sunburst(df_categ_gr, order_list, plot_value, plot_title_text, path_save, plot_name_save)


#######################################################################################################################
# Missing categorias en stock historico



def get_df_categ(categ_list_of_dic, path_save=None, file_name=None):
    i = 0
    categ_num = []
    categ_desc = []
    for dic_categ in categ_list_of_dic:
        print(dic_categ)
        categ_num.append(i)
        categ_desc.append(str(dic_categ))
        i = i + 1

    df_categ_desc = pd.DataFrame({'categoria_num': categ_num, 'categoria_desc': categ_desc})

    if path_save:
        df_categ_desc.to_excel(os.path.join(path_save, file_name), index=False)
    return df_categ_desc


# categorias faltan
def get_missing_categ(df_categ_desc, df_categ_gr, path_save=None, file_name=None):


    # list_stock_hist_categ_mis = df_categ_desc[~df_categ_desc['categ_desc'].isin(df_categ_gr['categoria_desc'])]

    df_missing_categ = pd.merge(df_categ_desc, df_categ_gr[['categoria_num', 'categoria_desc']],
                             on=['categoria_num', 'categoria_desc'],
                             how='left', indicator=True)

    df_missing_categ = df_missing_categ[df_missing_categ['_merge'] == 'left_only']
    df_missing_categ = df_missing_categ.drop(columns=['_merge'])
    if path_save:
        df_missing_categ.to_excel(os.path.join(path_save, file_name), index=False)
    return df_missing_categ


family_desc_value = 'DENIM'
careg_object_text = 'stock_historico'
name_save_all_categories = family_desc_value + '_categorias.xlsx'
df_categ_desc = get_df_categ(permutations_dic, path_save=path_save, file_name=name_save_all_categories)


name_save_missing_categories_stock = family_desc_value + '_categorias_que_faltan_' + careg_object_text + '.xlsx'
df_missing_categ = get_missing_categ(df_categ_desc, df_categ_gr, path_save=path_save, file_name=name_save_missing_categories_stock)




#######################################################################################################################
# categorizacion por temporada


stock_file = ('/var/lib/lookiero/stock/stock_tool/stock.csv.gz')
productos_file = ('/var/lib/lookiero/stock/stock_tool/productos_preprocessed.csv.gz')
venta_file = ('/var/lib/lookiero/stock/stock_tool/demanda_preprocessed.csv.gz')

# Dates as parameters

date_start_str = '2020-01-01'
date_end_str = '2020-06-30'

date_start_str_save = date_start_str.replace('-', '')
date_end_str_save = date_end_str.replace('-', '')

print('Extracting data for: ' + date_start_str + ' - ' + date_end_str)

# All 'envios' for selected dates, reference level. Note: there is no 'temporada' at this level

print('Load envios')

query_envios_text = 'date_ps_done >= @date_start_str and date_ps_done <= @date_end_str and family_desc == @family_desc_value'
df_envios_raw = pd.read_csv(venta_file,
                            usecols=['reference', 'modelo', 'family_desc', 'size', 'color', 'date_ps_done',
                                     'purchased', 'country', 'date_terminated', 'date_co']
                            ).query(query_envios_text)

list_date_ps_done = list(set(df_envios_raw['date_ps_done']))

df_reference_modelo_unq = df_envios_raw[['reference', 'modelo', 'color']].drop_duplicates()  # , 'color'

modelo_list = df_reference_modelo_unq['modelo'].to_list()
reference_list = df_reference_modelo_unq['reference'].to_list()

print('Extract stock')

# load data in chunks and from each chunk extract reference and dates of interest
df_stock_raw = pd.DataFrame([])

i = 0
df_stock_reader = pd.read_csv(stock_file, iterator=True, chunksize=100000)
for df_chunk in df_stock_reader:
    print(i, ' chunk')
    df_stock_raw = df_stock_raw.append(df_chunk[(df_chunk['reference'].isin(reference_list)) &
                                                (df_chunk['date'].isin(list_date_ps_done)) &
                                                (df_chunk['active'] == 1)])
    i = i + 1

df_stock_raw[df_stock_raw['real_stock'] < 0] = 0


# add modelo
df_stock_raw = pd.merge(df_stock_raw, df_reference_modelo_unq, on=['reference'], how='left')
df_stock_periodo_modelo = df_stock_raw.groupby(['modelo', 'color']).agg({'real_stock': 'sum'}).reset_index() # 'date',


df_stock_periodo_modelo['real_stock'] = df_stock_periodo_modelo['real_stock'].fillna(0)




#  envios, purchased

print('Extract envios and purchased')

df_envios = df_envios_raw[['reference', 'modelo', 'color', 'purchased', 'date_co']]

df_envios['envios'] = 1
df_envios['envios_co'] = df_envios['date_co']

df_envios['purchased'] = df_envios['purchased'].fillna(0)

df_envios_periodo_modelo = df_envios.groupby(['modelo', 'color']).agg({'purchased': 'sum',
                                                                         'envios': 'sum',
                                                                         'envios_co': 'count'}).reset_index()


df_periodo = pd.merge(df_stock_periodo_modelo, df_envios_periodo_modelo,
                  on=['modelo', 'color'])

df_periodo_feature = pd.merge(df_periodo,
                  df_categ_all[['modelo', 'color', 'categoria_num',
                                'largura', 'down_part_type', 'color_categ', 'precio', 'categoria_desc', 'precio_catalogo']],
                  on=['modelo', 'color'], how='left')



df_periodo_feature['modelo_num'] = df_periodo_feature['modelo']

df_periodo_categ = df_periodo_feature.groupby(['categoria_num',
                                    'categoria_desc']).agg({'down_part_type': 'last',
                                                            'color_categ': 'last',
                                                            'precio': 'last',
                                                            'largura': 'last',
                                                            'modelo_num': 'count',
                                                            'modelo': lambda x: list(set(x)),
                                                            'purchased': 'sum',
                                                            'envios': 'sum',
                                                            'envios_co': 'sum',
                                                            'precio_catalogo': 'mean'}).reset_index()


df_periodo_categ['purchased_por_precio_medio'] = df_periodo_categ['purchased'] * df_periodo_categ['precio_catalogo']

# missing categories

careg_object_text = 'periodo_' + date_start_str_save + '_' + date_end_str_save


name_save_missing_categories_periodo = family_desc_value + '_categorias_que_faltan_' + careg_object_text + '.xlsx'

df_missing_categ_periodo = get_missing_categ(df_categ_desc, df_periodo_categ, path_save=path_save, file_name=name_save_missing_categories_periodo)

# plot


##############################################3

# NOOS
# SIEMPRE TIENE QUE HABER:
# Pitillo azul precio medio largo medio
# Pitillo negro Precio medio largo medio
# Pitillo gris precio medio largo medio

#
# df_categ_all.loc[(df_modelos['down_part_type'] == 'cigarette') & (df_modelos['color_categ'].isin(['azul', 'negro', 'gris'])) &
#                      (df_modelos['precio'] == 'precio_medio') & (df_modelos['largura'] == 'largura_medio'), 'noos'] = 1
#
# df_categ_all['noos'] = df_categ_all['noos'].fillna(0)

# ESPECIAL
# PARA AÑADIR VARIEDAD NECESITAMOS:
# Diferentes tipos que no sean pitillo
# Diferente precio, sobre todos alto
# Larguras cortas y largas
#
# df_categ_all.loc[(df_modelos['down_part_type'] != 'cigarette') &
#                  (df_modelos['largura'].isin(['largura_corto', 'largura_largo'])), 'especial'] = 1
#
# df_categ_all['especial'] = df_categ_all['especial'].fillna(0)

#######################################################################################################################
#
# fig = px.sunburst(data_frame=df_categ_gr,
#                           path=['largura', 'down_part_type', 'color_categ', 'precio'],
#                           values='modelo_num',
#                           branchvalues="total")
#
# fig.update_layout(
#     title={
#         'text': "Stock historico DENIM",
#         'y': 0.9,
#         'x': 0.13,
#         'xanchor': 'center',
#         'yanchor': 'top'})
# fig.update_traces(textinfo="label+percent root + value")
# fig['layout'].update(margin=dict(l=0, r=0, b=0, t=0))
#
# # fig.show()
#
# fig.write_html(os.path.join(path_save, 'denim_categ_stock_sunburst.html'))
# fig.write_image(os.path.join(path_save, 'denim_categ_stock_sunburst.png'))

############
# con imagenes de pantalones

fig = px.sunburst(data_frame=df_categ_gr,
                          path=['largura', 'down_part_type', 'color_categ', 'precio'],
                          values='modelo_num',
                          branchvalues="total")

fig.add_layout_image(
    dict(
        source="https://s3-eu-west-1.amazonaws.com/catalogo.labs/M1005/M1005C9.jpg",
        x=0.93,
        y=0.55,
    ))
# fig.add_layout_image(dict(
#         source="https://raw.githubusercontent.com/michaelbabyn/plot_data/master/naphthalene.png",
#         x=0.9,
#         y=0.3,
#         ))

fig.update_layout_images(dict(
    xref="paper",
    yref="paper",
    sizex=0.25,
    sizey=0.25,
    xanchor="right",
    yanchor="bottom", layer='below'
))
fig.update_traces(textinfo="label+percent root + value")
fig['layout'].update(margin=dict(l=0, r=0, b=0, t=0))

# fig.show()

# fig.write_html(os.path.join(path_save, 'denim_categ_stock_sunburst.html'))
fig.write_image(os.path.join(path_save, 'denim_categ_stock_sunburst_imagen_pantalon.png'))


#################################################################################################################
# stock enero-abril 2021


stock_file = ('/var/lib/lookiero/stock/stock_tool/stock.csv.gz')
productos_file = ('/var/lib/lookiero/stock/stock_tool/productos_preprocessed.csv.gz')
venta_file = ('/var/lib/lookiero/stock/stock_tool/demanda_preprocessed.csv.gz')

# Dates as parameters

date_start_str = '2021-01-01'
date_end_str = '2021-01-31'

family_desc_value = 'DENIM'

print('Extracting data for: ' + date_start_str + ' - ' + date_end_str)

# All 'envios' for selected dates, reference level. Note: there is no 'temporada' at this level

print('Load envios')

query_envios_text = 'date_ps_done >= @date_start_str and date_ps_done <= @date_end_str and family_desc == @family_desc_value'
df_envios_raw = pd.read_csv(venta_file,
                            usecols=['reference', 'modelo', 'family_desc', 'size', 'color', 'date_ps_done',
                                     'purchased', 'country', 'date_terminated', 'date_co']
                            ).query(query_envios_text)

list_date_ps_done = list(set(df_envios_raw['date_ps_done']))

df_reference_modelo_unq = df_envios_raw[['reference', 'modelo', 'color']].drop_duplicates()  # , 'color'

modelo_list = df_reference_modelo_unq['modelo'].to_list()
reference_list = df_reference_modelo_unq['reference'].to_list()

print('Extract stock')

# load data in chunks and from each chunk extract reference and dates of interest
df_stock_raw = pd.DataFrame([])

i = 0
df_stock_reader = pd.read_csv(stock_file, iterator=True, chunksize=100000)
for df_chunk in df_stock_reader:
    print(i, ' chunk')
    df_stock_raw = df_stock_raw.append(df_chunk[(df_chunk['reference'].isin(reference_list)) &
                                                (df_chunk['date'].isin(list_date_ps_done)) &
                                                (df_chunk['active'] == 1)])
    i = i + 1

df_stock_raw[df_stock_raw['real_stock'] < 0] = 0

# add modelo
df_stock_raw = pd.merge(df_stock_raw, df_reference_modelo_unq, on=['reference'], how='left')
df_stock_mes_modelo = df_stock_raw.groupby(['modelo', 'color']).agg({'real_stock': 'sum'}).reset_index() # 'date',


df_stock_mes_modelo['real_stock'] = df_stock_mes_modelo['real_stock'].fillna(0)




#  envios, purchased

print('Extract envios and purchased')

df_envios = df_envios_raw[['reference', 'modelo', 'color', 'purchased', 'date_co']]

df_envios['envios'] = 1
df_envios['envios_co'] = df_envios['date_co']

df_envios['purchased'] = df_envios['purchased'].fillna(0)

df_envios_mes_modelo = df_envios.groupby(['modelo', 'color']).agg({'purchased': 'sum',
                                                                         'envios': 'sum',
                                                                         'envios_co': 'count'}).reset_index()


df_mes = pd.merge(df_stock_mes_modelo, df_envios_mes_modelo,
                  on=['modelo', 'color'])

df_mes_feature = pd.merge(df_mes,
                  df_categ_all[['modelo', 'color', 'categoria_num',
                                'largura', 'down_part_type', 'color_categ', 'precio', 'categoria_desc', 'precio_catalogo']],
                  on=['modelo', 'color'], how='left')



df_mes_feature['modelo_num'] = df_mes_feature['modelo']

df_mes_categ = df_mes_feature.groupby(['categoria_num',
                                    'categoria_desc']).agg({'down_part_type': 'last',
                                                            'color_categ': 'last',
                                                            'precio': 'last',
                                                            'largura': 'last',
                                                            'modelo_num': 'count',
                                                            'modelo': lambda x: list(set(x)),
                                                            'purchased': 'sum',
                                                            'envios': 'sum',
                                                            'envios_co': 'sum',
                                                            'precio_catalogo': 'mean'}).reset_index()


df_mes_categ['purchased_por_precio_medio'] = df_mes_categ['purchased'] * df_mes_categ['precio_catalogo']
# df_mes_categ['acierto'] = df_mes_categ['purchased'] * 100 / df_mes_categ['envios_co']
# df_mes_categ['acierto'] = df_mes_categ['acierto'].round(0)


fig = px.sunburst(data_frame=df_mes_categ,
                          path=['largura', 'down_part_type', 'color_categ', 'precio'],
                          values='modelo_num',
                          branchvalues="total")

fig.update_layout(title={'text': "Stock " + family_desc_value + ' <br> de ' + str(date_start_str) + ' - <br> ' + str(date_end_str),
                         'y': 0.9,
                         'x': 0.13,
                         'xanchor': 'center',
                         'yanchor': 'top'})

fig.update_traces(textinfo="label+percent root + value")
fig['layout'].update(margin=dict(l=0, r=0, b=0, t=0))

# fig.show()

# fig.write_html(os.path.join(path_save, 'denim_categ_stock_sunburst.html'))
fig.write_image(os.path.join(path_save, 'denim_categ_stock_sunburst_' + str(date_start_str) + '.png'))


# plot Envios


fig = px.sunburst(data_frame=df_mes_categ,
                          path=['largura', 'down_part_type', 'color_categ', 'precio'],
                          values='envios',
                          branchvalues="total",
                  title="Envios")


fig.update_layout(title={'text': "Envios " + family_desc_value + ' <br> de ' + str(date_start_str) + ' - <br> ' + str(date_end_str),
                         'y': 0.9,
                         'x': 0.13,
                         'xanchor': 'center',
                         'yanchor': 'top'})

fig.update_traces(textinfo="label+percent root + value")
fig['layout'].update(margin=dict(l=0, r=0, b=0, t=0))

# fig.show()
# name_save = 'sunburst_denim_categ_envios_sunburst_' + str(date_start_str)
fig.write_html(os.path.join(path_save, 'denim_categ_stock_sunburst.html'))
fig.write_image(os.path.join(path_save, 'denim_categ_envios_sunburst_' + str(date_start_str) + '.png'))



# plot purchased


fig = px.sunburst(data_frame=df_mes_categ,
                          path=['largura', 'down_part_type', 'color_categ', 'precio'],
                          values='purchased',
                          branchvalues="total")

fig.update_layout(title={'text': "Purchased " + family_desc_value + ' <br> de ' + str(date_start_str) + ' - <br> ' + str(date_end_str),
                         'y': 0.9,
                         'x': 0.13,
                         'xanchor': 'center',
                         'yanchor': 'top'})

fig.update_traces(textinfo="label+percent root + value")
fig['layout'].update(margin=dict(l=0, r=0, b=0, t=0))

# fig.show()

fig.write_html(os.path.join(path_save, 'denim_categ_stock_sunburst.html'))
fig.write_image(os.path.join(path_save, 'denim_categ_purchased_sunburst_' + str(date_start_str) + '.png'))


####purchased_por_precio_medio


fig = px.sunburst(data_frame=df_mes_categ,
                          path=['largura', 'down_part_type', 'color_categ', 'precio'],
                          values='purchased_por_precio_medio',
                          branchvalues="total")

fig.update_layout(title={'text': "Purchased * precio medio <br> " + family_desc_value + ' <br> de ' + str(date_start_str) + ' - <br> ' + str(date_end_str),
                         'y': 0.9,
                         'x': 0.14,
                         'xanchor': 'center',
                         'yanchor': 'top'})

fig.update_traces(textinfo="label+percent root + value")
fig['layout'].update(margin=dict(l=0, r=0, b=0, t=0))

# fig.show()

# fig.write_html(os.path.join(path_save, 'denim_categ_stock_sunburst.html'))
fig.write_image(os.path.join(path_save, 'denim_categ_purchased_precio_sunburst_' + str(date_start_str) + '.png'))



###########################
# categorias faltan
i = 0
categ_num = []
categ_desc = []
for dic_categ in permutations_dic:
    print(dic_categ)
    categ_num.append(i)
    categ_desc.append(str(dic_categ))
    i = i+1

df_categ_desc = pd.DataFrame({'categ_num': categ_num, 'categ_desc': categ_desc})


df_categ_gr

list_stock_hist_categ_mis = df_categ_desc[~df_categ_desc['categ_desc'].isin(df_categ_gr['categoria_desc'])]


#################################
import plotly.express as px
df = px.data.gapminder().query("year == 2007")

link_ref = '<a href="files/file1.html">{}</a>'
df['country'] = df['country'].apply(lambda item: link_ref.format(item))
fig = px.treemap(df, path=[ 'continent', 'country'], values='pop',
                  color='lifeExp', hover_data=['iso_alpha'])

###############################################################################################################

# df_categ_gr['link'] = 'href="https://s3-eu-west-1.amazonaws.com/catalogo.labs/C1123/C1123C9.jpg"'

df_categ_gr['link'] = '<img src="https://s3-eu-west-1.amazonaws.com/catalogo.labs/C1123/C1123C9.jpg" />'

# =IMAGE("https://s3-eu-west-1.amazonaws.com/catalogo.labs/C1123/C1123C9.jpg")

fig = px.sunburst(data_frame=df_categ_gr,
                          path=['down_part_type', 'link'],
                          values='modelo_num',
                          branchvalues="total")


fig.update_traces(textinfo="label+ value")
fig['layout'].update(margin=dict(l=0, r=0, b=0, t=0))


fig.write_image(os.path.join(path_save, 'test.png'))


import plotly.express as px
df = px.data.gapminder().query("year == 2007")

link_ref = '<a href="files/file1.html">{}</a>'
df['country'] = df['country'].apply(lambda item: link_ref.format(item))
fig = px.treemap(df, path=[ 'continent', 'country'], values='pop',
                  color='lifeExp', hover_data=['iso_alpha'])

#####################
df = pd.DataFrame({
    'link': [
        'https://twitter.com/CertSG/status/1286557929198563328',
        'https://twitter.com/osiseguridad/status/1286565901568016384'
    ]
})

def convert(row):
    #print(row)
    return '<a href="{}">{}</a>'.format(row['link'],  row.name)

df['link'] = df.apply(convert, axis=1)

print(df)

# Display it with `plotly`

import plotly.figure_factory as ff

fig = ff.create_table(df)
fig.show()
fig.write_image(os.path.join(path_save, 'test.png'))
######################################################################################################################
# Separar por NOOS
# df_categ_gr.loc[df_categ_gr['noos'] == 1, 'noos_vs_especial'] = 'noos'
# df_categ_gr.loc[df_categ_gr['especial'] == 1, 'noos_vs_especial'] = 'especial'
#
# df_categ_gr['noos_vs_especial'] = df_categ_gr['noos_vs_especial'].fillna('regular')


# coff = np.asarray(coff) * C
# import plotly.express as px

px.io.renderers.default = "browser"

fig = px.express.sunburst(data_frame=df_categ_gr, path=['down_part_type', 'color_categ', 'largura', 'precio'],
                          values='modelo_num')
fig.show()

fig = px.express.sunburst(data_frame=df_categ_gr, path=['down_part_type', 'color_categ', 'largura', 'precio'],
                          values='modelo_num')
fig.show()

fig = px.express.sunburst(data_frame=df_categ_gr,
                          path=['noos_vs_especial', 'down_part_type', 'color_categ', 'largura', 'precio'],
                          values='modelo_num')
fig.show()

fig.write_html(os.path.join(path_save, 'denim_categ_stock_sunburst.html'))

# largira

fig = px.express.sunburst(data_frame=df_categ_gr,
                          path=['largura', 'noos_vs_especial', 'down_part_type', 'color_categ', 'precio'],
                          values='modelo_num')
fig.show()

fig.write_html(os.path.join(path_save, 'denim_categ_stock_sunburst_largura.html'))

# save
df_categ_gr.to_excel(os.path.join(path_save, 'denim_categorias_noos.xlsx'))



df = pd.DataFrame({
    'link': [
        'https://twitter.com/CertSG/status/1286557929198563328',
        'https://twitter.com/osiseguridad/status/1286565901568016384'
    ]
})

def convert(row):
    #print(row)
    return '<a href="{}">{}</a>'.format(row['link'],  row.name)

for row in df.iterrows():
    print(row['link'])
# ![Philadelphia's Magic Gardens. This place was so cool!](/assets/images/philly-magic-gardens.jpg "Philadelphia's Magic Gardens")

df['link'] = df.apply(convert, axis=1)

# df['link'] = '[![A click on the image links to its own URL.](https://www.gravatar.com/avatar/dd5a7ef1476fb01998a215b1642dfd07
# "A click on the image links to its own URL.")](https://www.gravatar.com/avatar/dd5a7ef1476fb01998a215b1642dfd07)'

# df['link'] = '[![alt text](https://s3-eu-west-1.amazonaws.com/catalogo.labs/C1123/C1123C9.jpg)]'

df['link'] = '<img href="https://s3-eu-west-1.amazonaws.com/catalogo.labs/C1123/C1123C9.jpg" />'

print(df)

# Display it with `plotly`

import plotly.figure_factory as ff

fig = ff.create_table(df)
fig.show()


df_categ_gr['link'] = '<img src="https://s3-eu-west-1.amazonaws.com/catalogo.labs/C1123/C1123C9.jpg" />'

df['link'] = '![imagen](https://s3-eu-west-1.amazonaws.com/catalogo.labs/C1123/C1123C9.jpg)'