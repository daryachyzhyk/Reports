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

# df_categ_possib = df_categ_desc.copy()
# df_categ_stock = df_categ_gr.copy()
# df_categ_periodo = df_periodo_categ.copy()



# categorias faltan
def get_missing_categ(df_categ_possib, df_categ_stock, df_categ_periodo=None, path_save=None, file_name=None):


    # list_stock_hist_categ_mis = df_categ_desc[~df_categ_desc['categ_desc'].isin(df_categ_gr['categoria_desc'])]

    df_missing_hist = pd.merge(df_categ_possib, df_categ_stock[['categoria_num', 'categoria_desc']],
                             on=['categoria_num', 'categoria_desc'],
                             how='left', indicator=True)

    df_missing_hist = df_missing_hist[df_missing_hist['_merge'] == 'left_only']
    # df_missing_categ = df_missing_categ.rename(columns={'_merge': 'missing_stock_history'})
    df_missing_hist['categ_missing_history'] = 1
    df_missing_hist = df_missing_hist.drop(columns=['_merge'])

    if df_categ_periodo is None:
        df_missing_categ = df_missing_hist.copy()
    else:
        df_missing_periodo = pd.merge(df_categ_possib, df_categ_periodo[['categoria_num', 'categoria_desc']],
                                    on=['categoria_num', 'categoria_desc'],
                                    how='left', indicator=True)
        df_missing_periodo = df_missing_periodo[df_missing_periodo['_merge'] == 'left_only']

        df_missing_periodo['categ_missing_season'] = 1
        df_missing_periodo = df_missing_periodo.drop(columns=['_merge'])
        df_missing_categ = pd.merge(df_missing_hist, df_missing_periodo, on=['categoria_num', 'categoria_desc'],
                                    how='outer')

    df_missing_categ['categ_missing_history'] = df_missing_categ['categ_missing_history'].fillna(0)
    # df_periodo_categ


    if path_save:
        if not os.path.exists(path_save):
            os.mkdir(path_save)
        df_missing_categ.to_excel(os.path.join(path_save, file_name), index=False)
    return df_missing_categ


family_desc_value = 'DENIM'
careg_object_text = 'stock_historico'
name_save_all_categories = family_desc_value + '_categorias.xlsx'
df_categ_desc = get_df_categ(permutations_dic, path_save=path_save, file_name=name_save_all_categories)


name_save_missing_categories_stock = family_desc_value + '_categorias_que_faltan_' + careg_object_text + '.xlsx'
df_missing_categ = get_missing_categ(df_categ_desc, df_categ_gr, df_categ_periodo=None, path_save=path_save, file_name=name_save_missing_categories_stock)




#######################################################################################################################
# categorizacion por temporada


stock_file = ('/var/lib/lookiero/stock/stock_tool/stock.csv.gz')
productos_file = ('/var/lib/lookiero/stock/stock_tool/productos_preprocessed.csv.gz')
venta_file = ('/var/lib/lookiero/stock/stock_tool/demanda_preprocessed.csv.gz')

# Dates as parameters
def get_season_dates(season_number):
    lk_start_year = 2016
    # season_number = 9
    if (season_number % 2) == 0:
        season_start = '-07-01'
        season_end = '-12-31'
    else:
        season_start = '-01-01'
        season_end = '-06-30'

    date_start_str = str(lk_start_year + np.int(np.ceil(season_number / 2)) - 1) + season_start
    date_end_str = str(lk_start_year + np.int(np.ceil(season_number / 2)) - 1) + season_end

    # TODO add test
    return date_start_str, date_end_str

for season_number in list(range(1, 12)):
    date_start_str, date_end_str = get_season_dates(season_number)
    print(str(season_number) + ': ' + date_start_str + ' ' + str(date_end_str))



for season_number in list(range(1, 12)): # list(range(1, 12))  [4, 9, 10, 11]
    date_start_str, date_end_str = get_season_dates(season_number)


    # date_start_str = '2020-01-01'
    # date_end_str = '2020-06-30'

    date_start_str_save = date_start_str.replace('-', '')
    date_end_str_save = date_end_str.replace('-', '')

    print('Extracting data for: season ' + str(season_number) + ' ' + date_start_str + ' - ' + date_end_str)

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
        # print(i, ' chunk')
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

    # revenue: envíos * acierto * precio medio
    df_periodo_categ['revenue'] = df_periodo_categ['purchased'] * df_periodo_categ['precio_catalogo']

    # missing categories
    # TODO: missing distinguish

    # careg_object_text = 'periodo_' + date_start_str_save + '_' + date_end_str_save

    careg_object_text = 'temporada_' + str(season_number)
    name_save_missing_categories_periodo = family_desc_value + '_categorias_que_faltan_' + careg_object_text + '.xlsx'
    path_save_periodo = os.path.join(path_save, family_desc_value + '_categorias_' + careg_object_text)

    # df_categ_desc, df_categ_gr, df_categ_periodo = None
    df_missing_categ_periodo = get_missing_categ(df_categ_desc, df_categ_gr, df_periodo_categ, path_save=path_save_periodo,
                                                 file_name=name_save_missing_categories_periodo)

    # df_missing_categ_periodo = get_missing_categ(df_categ_gr, df_periodo_categ, path_save=path_save_periodo,
    #                                              file_name=name_save_missing_categories_periodo)

    # df_missing_categ_periodo = get_missing_categ(df_categ_desc, df_periodo_categ, path_save=path_save_periodo,
    #                                              file_name=name_save_missing_categories_periodo)


    # name_save_missing_categories_periodo = family_desc_value + '_categorias_que_faltan1_' + careg_object_text + '.xlsx'




    # plot





    value_list = ['modelo_num', 'purchased', 'envios', 'revenue'] # , 'purchased_por_precio_medio'
    # careg_object_text = 'stock_' + date_start_str_save + '_' + date_end_str_save

    for plot_value in value_list:
        print(plot_value)
        for order_list in plot_order_list:

            plot_title_text = family_desc_value + '<br>' + careg_object_text.replace('_', ' ') + '<br>' + plot_value.replace('_', ' ')
            order_list_save = '_'.join(order_list)
            print('Plotting sunburst for ' + family_desc_value + ' for order ' + order_list_save)
            plot_name_save = 'sunburst_categor_' + family_desc_value + '_' + careg_object_text + '_' + plot_value + '_' + order_list_save
            plot_sunburst(df_periodo_categ, order_list, plot_value, plot_title_text, path_save_periodo, plot_name_save)

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



######################################################################################################################
# Wide Eyes


file_we = '/var/lib/lookiero/stock/stock_tool/wide_eyes_20210504.csv.gz'


df_we = pd.read_csv(file_we)

# WE ejemplo

df_we_test = pd.DataFrame({"product_variant_id": "e5806967-5e81-4a52-abfb-9c52f216e57d", "distance_vector": {"e5806967-5e81-4a52-abfb-9c52f216e57d": 0, "6ed1e576-2ee2-4711-924f-3a03956c3d15": 0.07318007946014404, "7aa694b6-779b-4894-a1a0-97e890b90be4": 0.0994342565536499, "19a476cd-0516-4de8-a3ba-b50879e94276": 0.10089927911758423, "0c1db538-65c4-4d69-8c93-dd2b208e0b43": 0.10904598236083984, "505e3c2c-3bc4-47ed-9ebe-764e9c3faa51": 0.1154322624206543, "5b9acbe9-e8c4-4de6-b036-28180e2dce99": 0.12051546573638916, "7f862373-32ac-47ec-9e9b-93054719aa6c": 0.12643033266067505, "909a5618-fca9-4d23-996c-ed07ab2b90c1": 0.13084381818771362, "d8a5cba4-7044-4a7e-b05c-ce6ab7482929": 0.13084381818771362, "ecd9e061-9b5b-4698-aff5-67a73432ce06": 0.13248717784881592, "22a80c27-d4cd-4747-b772-038d935ad61b": 0.13362640142440796, "237faf2b-015f-42c8-b9ee-f955da9158d9": 0.13687443733215332, "90a3d48c-74d4-4069-9371-ca7e96008e84": 0.14010578393936157, "d708a454-b090-4241-8c78-85d28b6e27af": 0.14084315299987793, "ff35c68f-e32a-4e20-8ca3-1c29e34a1bff": 0.14368534088134766, "45ff4cbc-adae-40f8-9484-7e376e08ab85": 0.1453608274459839, "3f575b1b-0986-4c95-ba06-1e4b7ae60b9e": 0.14585870504379272, "2ec1089a-9ebc-4090-86e0-ffe3233eb08c": 0.14607441425323486, "4b799b67-269e-4b01-a27c-eede368243ee": 0.14616739749908447, "444e066f-533e-4d5a-9de5-8cecc6e33bac": 0.1472773551940918, "fc7fdc73-58c4-464f-a183-267bb6191b0f": 0.14758121967315674, "38416bf6-730d-49f2-ac35-fb5f8fb7dc78": 0.14868903160095215, "fa8dc096-d349-4293-a33d-deacdf83c18e": 0.14923936128616333, "347dfaa2-47ba-4913-b797-76ba7a760020": 0.15039020776748657, "c11db38b-3e5f-4c3a-9f6b-1d39d6ffdb55": 0.1509099006652832, "c6d86f9f-d991-4568-b02c-f0fd283ff643": 0.15116196870803833, "92ce6ebe-714d-4ba7-89c0-ea463f7c7e96": 0.15126138925552368, "1219e3e2-3d22-4f7e-84bb-8601e6a3ef2d": 0.15157252550125122, "11c5cf82-d83d-475d-8ee1-caab6ca2bf27": 0.15179651975631714, "5bf9f0be-9fd0-44f8-b40d-4cd5a0ba3d25": 0.15202653408050537, "d8381a44-faa7-4399-9427-967e00803f06": 0.15331977605819702, "1bc4989d-a7c5-423b-b5ef-7d0479c5e9ce": 0.15338557958602905, "2f570086-7497-4b1f-84ea-4566fd3921ef": 0.1537257432937622, "a95b1013-544b-4c7a-91a3-75404d63232d": 0.15498435497283936, "1a937429-f517-4410-984d-6cfb419f012e": 0.15543591976165771, "7ccb433d-d9cf-4221-886b-7758c7487e65": 0.15589392185211182, "5174491a-7929-430a-a73a-827f21011d89": 0.15596812963485718}}).reset_index()


df_we_test = df_we_test.rename(columns={'index': 'product_id_j', 'product_variant_id': 'product_id_i'})
df_id_modelo = pd.read_csv('/home/darya/Documents/Reports/2021-05-04-WideEyes/comments_data_tab.csv', error_bad_lines=False, sep=';')
df_id_modelo = df_id_modelo.rename(columns={'id': 'product_id_j'})
df_modelo_dist = pd.merge(df_we_test, df_id_modelo,
                          on='product_id_j', how='left')

def get_image_url(modelo, color):
    image_url = '=IMAGE("https://s3-eu-west-1.amazonaws.com/catalogo.labs/' + modelo + '/' + modelo + color +'.jpg")'
    return image_url

df_modelo_dist['product_image_j'] = df_modelo_dist.apply(lambda x: get_image_url(x['_group_id'], x['color']), axis=1)

image_i = df_modelo_dist[df_modelo_dist['product_id_j'] == df_modelo_dist['product_id_i'][0]]['product_image_j']

df_modelo_dist['product_image_i'] = image_i.iloc[0]


df_save = df_modelo_dist[['product_image_i', 'product_image_j', 'distance_vector']].sort_values('distance_vector')

df_save.to_excel('/home/darya/Documents/Reports/2021-05-04-WideEyes/we_dress_similarity_test.xlsx')

df['col_3'] = df.apply(lambda x: f(x.col_1, x.col_2), axis=1)


writer = pd.ExcelWriter('output.xlsx')
s.to_excel(writer, 'Sheet1', header=False, index=False)
writer.save()




