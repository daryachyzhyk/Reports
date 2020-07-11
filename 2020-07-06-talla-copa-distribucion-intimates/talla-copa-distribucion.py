'''
Actualizar la distrubución de tallas para la compra de intimates
Hay que calcular el % de clientas en cada talla / copa de sujetador,
teniendo en cuenta también el número de cajas pedidas por cada clienta,
de forma que si las clientas por ejemplo con talla 85B, piden más cajas, que lo tenga en cuenta

y lo mismo para talla de abajo

Los ultimos: 7 y 12 meses
'''

import pickle
import os
import pandas as pd
import numpy as np
import seaborn as sns
import datetime


#######################################################################################################################
# Intro

# repo_file = '/var/lib/lookiero/repo.obj'
repo_file = '/var/lib/lookiero/repo_v2.5.0.obj'
repo = pickle.load(open(repo_file, 'rb'))
path_results = ('/home/darya/Documents/Reports/2020-07-06-report-size-sujetador')

# indicate the period of interestin MONTHS, e.i last 7 or 12 months and END period
period_in_months = 7

date_end = datetime.datetime.strptime('2020-07-01', '%Y-%m-%d')

########
n_months = datetime.timedelta(period_in_months*365/12)


date_start = date_end - n_months




#
# def get_talla_sujetador(value):
#
#     value = int(value)
#
#     if value == 1:
#         return 80
#     elif value == 2:
#         return 85
#     elif value == 3:
#         return 90
#     elif value == 4:
#         return 95
#     elif value == 5:
#         return 100
#     elif value == 6:
#         return 105
#     elif value == 7:
#         return 110
#     elif value == 8:
#         return 115
#     elif value == 9:
#         return 120
#     else:
#         return None


# Clientas registradas

list_cl_id_registradas = []
list_talla_registradas = []
list_copa_registradas = []
list_date_registradas = []

list_box_id = []
list_box_date = []

list_bottom_size = []
list_top_size = []


#
# cl = repo.clients[328009]
# cl.profile.get_quiz_var('copa')
# copa_key = '57'
# talla_key = '21' # 'tallasSujetador'


for id in repo.clients:
    print(id)
    
    #cl_year = repo.clients[id].date_created.year
    # if (cl_year > 2016) and (repo.clients[id].profile is not None):
    if (repo.clients[id].profile is not None) & (len(repo.clients[id].boxes) > 0):


        for box in repo.clients[id].get_normal_boxes():
            if box.date_created > date_start:


                list_box_id.append(box.id)
                list_box_date.append(box.date_created)
        
                # if (copa_key in repo.clients[id].profile.quiz.keys()) and (talla_key in repo.clients[id].profile.quiz.keys()):
                list_cl_id_registradas.append(id)

                # repo global
                list_talla_registradas.append(repo.clients[id].profile.get_quiz_var('tallaSujetador'))
                list_copa_registradas.append(repo.clients[id].profile.get_quiz_var('copa'))

                list_bottom_size.append(repo.clients[id].profile.get_bottom_size())
                list_top_size.append(repo.clients[id].profile.get_top_size())

                # cl.profile.get_bottom_size()
                # list_talla_registradas.append(repo.clients[id].profile.quiz['19'])
                # list_copa_registradas.append(repo.clients[id].profile.quiz['57'])

                #list_date_registradas.append(repo.clients[id].b.date_created)


# repo.clients[id].get_normal_boxes()

df_cl = pd.DataFrame({'id': list_cl_id_registradas,
                      'box_id': list_box_id,
                      'box_date': list_box_date,
                      'tallaSujetador': list_talla_registradas,
                      'copa': list_copa_registradas,
                      'bottom_size': list_bottom_size,
                      'top_size': list_top_size})


df_copa_sujetador = df_cl.groupby(['copa', 'tallaSujetador']).agg({'box_id': 'count',
                                                                   'id': pd.Series.nunique}).reset_index()

df_copa_top_size = df_cl.groupby(['copa', 'top_size']).agg({'box_id': 'count',
                                                                   'id': pd.Series.nunique}).reset_index()

df_copa_bottom_size = df_cl.groupby(['copa', 'bottom_size']).agg({'box_id': 'count',
                                                                   'id': pd.Series.nunique}).reset_index()


# df_copa_sujetador['proportion'] = df_copa_sujetador['box_id'] / df_copa_sujetador['id']

# df_copa_sujetador['pct'] = df_copa_sujetador['proportion'] / df_copa_sujetador['proportion'].sum() * 100

df_copa_sujetador['pct_clientas'] = df_copa_sujetador['box_id'] / df_copa_sujetador['id'].sum() * 100
df_copa_top_size['pct_clientas'] = df_copa_top_size['box_id'] / df_copa_top_size['id'].sum() * 100
df_copa_bottom_size['pct_clientas'] = df_copa_bottom_size['box_id'] / df_copa_sujetador['id'].sum() * 100



# df_copa_sujetador_matrix = df_copa_sujetador.pivot(index='copa', columns='tallaSujetador', values='pct').reset_index()


df_copa_sujetador_matrix = df_copa_sujetador.pivot(index='copa',
                                                            columns='tallaSujetador',
                                                            values='pct_clientas').reset_index()


df_copa_top_size_matrix = df_copa_top_size.pivot(index='copa',
                                                            columns='top_size',
                                                            values='pct_clientas').reset_index()

df_copa_bottom_size_matrix = df_copa_bottom_size.pivot(index='copa',
                                                            columns='bottom_size',
                                                            values='pct_clientas').reset_index()



#
#
# df_all_registradas = pd.DataFrame({'date': list_date_registradas,
#                                    'id': list_cl_id_registradas,
#                                    'tallaSujetador': list_talla_registradas,
#                                    'copa':list_copa_registradas})
#
#
# print(df_all_registradas.date.min())
#
# df_all_registradas['copa'].replace('', np.nan, inplace=True)
# df_all_registradas = df_all_registradas.dropna()
#
# df_registared_count = df_all_registradas.groupby(['copa', 'tallaSujetador']).size().reset_index(name="count")
#
# df_talla_copa_percent_registradas = df_registared_count.pivot_table('count', ['copa'], 'tallaSujetador') * 100 / df_registared_count['count'].sum()
#
#
# df_talla_copa_percent_registradas.to_excel(os.path.join(path_results, 'Percentage_copa_talla_clientas_registradas_20018_2019.xlsx'))
#
# # sns.set_style("whitegrid")
# sns_plot = sns.heatmap(df_talla_copa_percent_registradas, cmap='YlOrRd', annot=True)
# fig = sns_plot.get_figure()
# fig.savefig(os.path.join(path_results, 'Percentage_copa_talla_clientas_registradas_20018_2019.png'))
#
#
#
# # clientas que han comprado
#
#
# list_cl_id_pedido = []
# list_talla_pedido = []
# list_copa_pedido = []
# list_date_pedido = []
# list_boxes_pedido = []
#
# for id in repo.clients:
#
#     cl_year = repo.clients[id].date_created.year
#     if (cl_year > 2016) and (repo.clients[id].profile is not None) and (len(repo.clients[id].boxes) > 0):
#
#         list_cl_id_pedido.append(id)
#         list_talla_pedido.append(repo.clients[id].profile.get_quiz_var('tallaSujetador'))
#         list_copa_pedido.append(repo.clients[id].profile.get_quiz_var('copa'))
#         list_date_pedido.append(repo.clients[id].date_created)
#         num_boxes = 0
#         for box_id in repo.clients[id].boxes:
#             if repo.clients[id].boxes[box_id].checkout is True:
#                 num_boxes = num_boxes + 1
#         list_boxes_pedido.append(num_boxes)
#
#
#
# df_all_pedido = pd.DataFrame({'date': list_date_pedido,
#                                    'id': list_cl_id_pedido,
#                                    'tallaSujetador': list_talla_pedido,
#                                    'copa':list_copa_pedido,
#                               'boxes': list_boxes_pedido})
#
#
#
# df_all_pedido['copa'].replace('', np.nan, inplace=True)
# df_all_pedido = df_all_pedido.dropna()
#
# print(df_all_pedido.shape)
#
# # drop boxes without checkout
# df_all_pedido = df_all_pedido[(df_all_pedido[['boxes']] != 0).all(axis=1)]
# print(df_all_pedido.shape)
#
#
#
# # df_all_pedido = df_all_pedido.drop()
#
# df_pedido_repeated = df_all_pedido.loc[df_all_pedido.index.repeat(df_all_pedido['boxes'])].reset_index(drop=True)
#
#
# df_pedido_count = df_pedido_repeated.groupby(['copa', 'tallaSujetador']).size().reset_index(name="count")
#
# df_talla_copa_percent_pedido = df_pedido_count.pivot_table('count', ['copa'], 'tallaSujetador') * 100 / df_pedido_count['count'].sum()
#
#
# df_talla_copa_percent_pedido.to_excel(os.path.join(path_results, 'Percentage_copa_talla_clientas_pedido_contando_boxes.xlsx'))
#
# # sns.set_style("whitegrid")
# sns_plot = sns.heatmap(df_talla_copa_percent_pedido, cmap='YlOrRd', annot=True)
# fig = sns_plot.get_figure()
# fig.savefig(os.path.join(path_results, 'Percentage_copa_talla_clientas_pedido_contando_boxes.png'))
#
#
# # han comprado algo
#
#
# list_cl_id_comprado = []
# list_talla_comprado = []
# list_copa_comprado = []
# list_date_comprado = []
# list_boxes_comprado = []
#
# for id in repo.clients:
#
#     cl_year = repo.clients[id].date_created.year
#     if (cl_year > 2016) and (repo.clients[id].profile is not None) and (len(repo.clients[id].boxes) > 0):
#
#         list_cl_id_comprado.append(id)
#         list_talla_comprado.append(repo.clients[id].profile.get_quiz_var('tallaSujetador'))
#         list_copa_comprado.append(repo.clients[id].profile.get_quiz_var('copa'))
#         list_date_comprado.append(repo.clients[id].date_created)
#         num_boxes = 0
#         for box_id in repo.clients[id].boxes:
#             if (repo.clients[id].boxes[box_id].checkout is True) and (repo.clients[id].boxes[box_id]) and (len(repo.clients[id].boxes[box_id].products_purchased) > 0):
#                 num_boxes = num_boxes + 1
#         list_boxes_comprado.append(num_boxes)
#
#
# len(repo.clients[id].boxes[box_id].products_purchased)
#
#
# df_all_comprado = pd.DataFrame({'date': list_date_comprado,
#                               'id': list_cl_id_comprado,
#                               'tallaSujetador': list_talla_comprado,
#                               'copa': list_copa_comprado,
#                               'boxes': list_boxes_comprado})
#
# df_all_comprado['copa'].replace('', np.nan, inplace=True)
# df_all_comprado = df_all_comprado.dropna()
#
# # drop boxes without checkout
# df_all_comprado = df_all_comprado[(df_all_comprado[['boxes']] != 0).all(axis=1)]
# print(df_all_comprado.shape)
#
#
#
#
# df_comprado_repeated = df_all_comprado.loc[df_all_comprado.index.repeat(df_all_comprado['boxes'])].reset_index(drop=True)
#
# df_comprado_count = df_comprado_repeated.groupby(['copa', 'tallaSujetador']).size().reset_index(name="count")
#
# df_talla_copa_percent_comprado = df_comprado_count.pivot_table('count', ['copa'], 'tallaSujetador') * 100 / df_comprado_count[
#     'count'].sum()
#
# df_talla_copa_percent_comprado.to_excel(
#     os.path.join(path_results, 'Percentage_copa_talla_clientas_comprado_contando_boxes.xlsx'))
#
# # sns.set_style("whitegrid")
# sns_plot = sns.heatmap(df_talla_copa_percent_comprado, cmap='YlOrRd', annot=True)
# # plt.subplots_adjust(bottom=0.4, top=0.94)
# fig = sns_plot.get_figure()
# fig.savefig(os.path.join(path_results, 'Percentage_copa_talla_clientas_comprado_contando_boxes.png'))
#
# print(df_all_registradas.shape)
# print(df_all_pedido.shape)
# print(df_all_comprado.shape)