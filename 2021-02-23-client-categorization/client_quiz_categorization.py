


import os
import pandas as pd

##################################################################################################################
# Demanda

file_demanda = '/var/lib/lookiero/stock/stock_tool/demanda_preprocessed.csv.gz'
path_save = '/home/darya/Documents/Reports/2021-02-23-client-categorization'

column_demanda_list = ['family_desc', 'user_id', 'box_id', 'reference', 'date_ps_done',
                       'date_cancelled',
                       # 'country',
                       'client_is_big_size', 'is_first_box', 'price_range',
                       'brand',
                       'purchased',
                       'rej_fit', 'rej_price', 'rej_size', 'rej_style', 'rej_quality',
                       'n_cajas_misma_clienta', 'n_cajas_12_meses', 'box_number',
                       'date_last_box', 'time_since_last_box',
                       'AOVn']


# load
df_demanda_raw = pd.read_csv(file_demanda, usecols=column_demanda_list)  # , skiprows=range(1, 100000), nrows=100

df_demanda_raw = df_demanda_raw[(~df_demanda_raw['purchased'].isna()) & (df_demanda_raw['date_cancelled'].isna())]

# df_demanda_raw['date_ps_done_datetime'] = pd.to_datetime(df.date)
df_demanda_raw['date_ps_done_datetime'] = pd.to_datetime(df_demanda_raw['date_ps_done'])
df_demanda_raw["date_ps_done_quarter"] = pd.to_datetime(df_demanda_raw['date_ps_done']).dt.quarter
df_demanda_raw["date_ps_done_quarter_start"] = pd.to_datetime(df_demanda_raw['date_ps_done']).dt.to_period(
    "Q").dt.start_time
df_demanda_raw["date_ps_done_quarter_end"] = pd.to_datetime(df_demanda_raw['date_ps_done']).dt.to_period(
    "Q").dt.end_time

df_demanda_raw["date_ps_done_year"] = df_demanda_raw['date_ps_done_datetime'].dt.year



df_quarter = df_demanda_raw.drop_duplicates(subset=['date_ps_done_quarter_start', 'date_ps_done_quarter_end'])

df_quarter = df_quarter.sort_values(by='date_ps_done_quarter_start')

for index, row in df_quarter.iloc[16:, :].iterrows():
    # for q_start, q_end in quarter_start_list[-2]:

    df_demanda_quarter = df_demanda_raw[(df_demanda_raw['date_ps_done_datetime'] >= row['date_ps_done_quarter_start']) &
                                        (df_demanda_raw['date_ps_done_datetime'] <= row['date_ps_done_quarter_end'])]



    df_demanda_quarter.to_csv(os.path.join(path_save, 'df_demanda_quarter_' + str(row['date_ps_done_year']) + '_' +
                                           str(row['date_ps_done_quarter']) + '.csv.gz'))


##################################################################################################################
# Clienta

import os
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

path_save = '/home/darya/Documents/Reports/2021-02-23-client-categorization'

file_client_preprocessed = '/var/lib/lookiero/stock/stock_tool/clientas_preprocessed.csv.gz'

file_client_aux = '/home/darya/Documents/Reports/2021-02-23-client-categorization/clientas_20210225.csv.gz'

file_demanda_quarter = '/home/darya/Documents/Reports/2021-02-23-client-categorization/df_demanda_quarter_2020_1.csv.gz'

df_demanda_quarter = pd.read_csv(file_demanda_quarter, index_col=0)

df_demanda_quarter_purchased = df_demanda_quarter[df_demanda_quarter['purchased'] == 1]
df_demanda_quarter_purchased.to_csv(os.path.join(path_save, 'df_demanda_purchased_quarter_2020_1.csv.gz'))


# user_id_list = df_demanda_quarter['user_id'].unique().to_list()
user_id_list = list(set(df_demanda_quarter['user_id']))
# load clients
column_clienta_list = ['user_id', 'dedicacion',
                       # 'age', 'height', 'weight',
                       'aventurera', 'country',
                       # 'dedicacion', # nans
                       'fit_abajo', 'fit_arriba',
                       'is_mother',
                       # 'lookSalir', 'lookTrabajar',
                       # 'price_range',
                       # 'talla_abajo_num', 'talla_arriba_num',
                       'talla_copa', 'talla_sujetador',
                       'n_estilos',
                       # 'style_boho', 'style_casual', 'style_classical', 'style_minimal', 'style_night', 'style_street',
                       'estilo_clienta',
                       'look_salir', 'look_trabajo',
                       # 'is_petite', 'is_big_size',
                       'age_segment', 'height_segment', 'weight_segment',
                       'talla_arriba', 'talla_abajo']

column_clienta_aux = ['user_id', 'silueta',
                      'destacar_escote', 'destacar_brazo', 'destacar_gluteo', 'destacar_cintura', 'destacar_pierna',
                      'disimular_escote', 'disimular_brazo', 'disimular_gluteo', 'disimular_cintura',
                      'disimular_pierna',

                      'centrarEnRopa_trabajo', 'centrarEnRopa_tiempolibre', 'centrarEnRopa_noche'

                      # 'evitarRopa_vestidos', 'evitarRopa_camisas',
                      # 'evitarRopa_camisetas', 'evitarRopa_pantalones', 'evitarRopa_faldas',
                      # 'evitarRopa_prendaexterior', 'evitarRopa_bolsos', 'evitarRopa_fulares',
                      ]

# skiprows=range(1, 10000), nrows=100

query_text = 'user_id in @user_id_list'
df_clienta_raw = pd.read_csv(file_client_preprocessed, usecols=column_clienta_list).query(query_text)


# more quiz data
df_clienta_aux = pd.read_csv(file_client_aux, usecols=column_clienta_aux).query(query_text)

# df_clienta_raw = pd.read_csv(file_client, usecols=column_clienta_list, skiprows=range(1, 100000), nrows=100)
df_clienta = pd.merge(df_clienta_raw, df_clienta_aux, on='user_id', how='outer')

df_clienta.loc[(df_clienta['height_segment'] < 140) | (df_clienta['height_segment'] > 180), 'height_segment'] = np.nan

df_clienta.loc[(df_clienta['weight_segment'] < 40) | (df_clienta['weight_segment'] > 120), 'weight_segment'] = np.nan

df_clienta.loc[(df_clienta['age_segment'] < 20), 'age_segment'] = np.nan


# TODO: destacar

df_destacar_dummy = df_clienta[['user_id', 'destacar_escote', 'destacar_brazo', 'destacar_gluteo', 'destacar_cintura',
                                'destacar_pierna']]

s_destacar_dummy = df_destacar_dummy.where(df_destacar_dummy.eq(True)).stack().reset_index(level=1)['level_1']
s_destacar_dummy = s_destacar_dummy.str.replace('destacar_', '')

s_destacar = s_destacar_dummy.groupby([s_destacar_dummy.index]).transform(
    lambda x: '-'.join(x)).reset_index().drop_duplicates('index').set_index('index')

df_clienta['destacar'] = s_destacar

# TODO: disimular
df_disimular_dummy = df_clienta[['user_id', 'disimular_escote', 'disimular_brazo', 'disimular_gluteo',
                                 'disimular_cintura', 'disimular_pierna']]

s_disimular_dummy = df_disimular_dummy.where(df_disimular_dummy.eq(True)).stack().reset_index(level=1)['level_1']
s_disimular_dummy = s_disimular_dummy.str.replace('disimular_', '')

s_disimular = s_disimular_dummy.groupby([s_disimular_dummy.index]).transform(
    lambda x: '-'.join(x)).reset_index().drop_duplicates('index').set_index('index')

df_clienta['disimular'] = s_disimular

# TODO: silueta
silueta_map = {1: 'triangulo',
               2: 'oval',
               3: 'triangulo_invertido',
               4: 'rectangulo',
               5: 'reloj_arena',
               6: 'diamante'}

df_clienta['silueta'] = df_clienta['silueta'].map(silueta_map)

df_clienta = df_clienta.drop(columns=['destacar_escote', 'destacar_brazo', 'destacar_gluteo', 'destacar_cintura',
                                'destacar_pierna', 'disimular_escote', 'disimular_brazo', 'disimular_gluteo',
                                 'disimular_cintura', 'disimular_pierna'])


df_quarter = pd.merge(df_demanda_quarter, df_clienta, on='user_id', how='left')

df_quarter.to_csv(os.path.join(path_save, 'df_demanda_quarter_clienta.csv.gz'))

characteristics = ['estilo_clienta', 'look_salir', 'look_trabajo',
                   'age_segment', 'height_segment', 'weight_segment',
                   'talla_arriba', 'talla_abajo',
                   'silueta',
                   'centrarEnRopa_trabajo', 'centrarEnRopa_tiempolibre', 'centrarEnRopa_noche',
                   'destacar', 'disimular',
                   'family_desc',
                   'client_is_big_size',
                   'is_first_box', 'price_range',
                   'brand',
                   # 'purchased',
                   # 'rej_fit', 'rej_price', 'rej_size', 'rej_style', 'rej_quality',
                   'n_cajas_misma_clienta', 'n_cajas_12_meses',
                   # 'box_number', 'date_last_box', 'time_since_last_box', 'AOVn',
                   'aventurera', 'country', 'dedicacion',
                   'fit_abajo', 'fit_arriba', 'is_mother', 'talla_copa', 'talla_sujetador', 'n_estilos'
                   ]



# category_rej = ['rej_fit', 'rej_price', 'rej_size', 'rej_style', 'rej_quality']

df_gr_cat = pd.DataFrame([])

for category_name in characteristics:
    # category_name = 'estilo_clienta'
    print(category_name)

    df_gr = df_quarter.groupby([category_name], dropna=False)['purchased'].agg(['count', 'sum']).reset_index()
    df_gr['KR'] = df_gr['sum'] / df_gr['count']

    df_gr['pct_category'] = df_gr['count'] * 100 / df_quarter.shape[0]
    df_gr['KR_pct'] = df_gr['KR'] * df_gr['pct_category']
    df_gr = df_gr.sort_values(by=['KR', 'count'], ascending=False)


    df_gr['category'] = category_name
    df_gr = df_gr.rename(columns={category_name: 'category_option',
                                    'count': 'envios',
                                    'sum': 'purchased'})

    df_gr_cat = df_gr_cat.append(df_gr, ignore_index=False)



df_cat_all = df_gr_cat[df_gr_cat['pct_category'] > 0.1]

# df_cat_all = df_gr_cat[['category', 'category_option', 'KR', 'rej_fit', 'rej_price', 'rej_size', 'rej_style',
#                         'rej_quality', 'envios', 'purchased', 'pct_category', 'KR_pct']]

df_cat_all.to_csv(os.path.join(path_save, 'KR_client_characteristic.csv'))

for category_name in characteristics:
    df_plot = df_cat_all[df_cat_all['category'] == category_name]
    df_plot = df_plot.dropna(subset=['category_option'])
    df_plot = df_plot.sort_values(by='KR', ascending=False).reset_index()
    # plot
    if category_name == 'brand':
        fig, ax = plt.subplots(figsize=(17, 5))
    else:
        fig, ax = plt.subplots()
    # sns.boxplot(data=df_plot, x='category_option', y='KR')
    plot_order = df_plot.groupby('category_option')['KR'].sum().sort_values(ascending=False).index.values


    sns.barplot(data=df_plot, x='category_option', y='KR', label="KR", ax=ax, order=plot_order)  # color="olive", alpha=0.5,
    # sns.barplot(data=df_plot, x='category_option', y='envios', label="KR")  # color="olive", alpha=0.5,
    ax2 = plt.twinx()
    sns.lineplot(data=df_plot['envios'], marker='o', ax=ax2, sort=False, color='firebrick', linewidth=2.5) #, sort=False

    ax.set_title('' + str(category_name))
    ax.set_xticklabels(ax.get_xticklabels(), fontsize=14, rotation=90)
    ax.set(xlabel='', ylabel='')
    ax.set_ylabel('KR', fontsize=16)
    ax2.set_ylabel('Envios', fontsize=16, color='firebrick')
    plt.legend()
    fig.tight_layout()

    plt.savefig(os.path.join(path_save, 'KR_client_category ' + str(category_name) + '.png'))

sns.barplot(data = df, x='x_var', y='y_var_2', alpha=0.5, ax=ax2)

#########
# plot

category_plot_1 = ['estilo_clienta', 'look_salir', 'look_trabajo', 'dedicacion_str']
category_plot_2 = ['age_segment', 'height_segment', 'weight_segment', 'aventurera']

category_plot_3 = ['is_first_box', 'n_cajas_misma_clienta', 'n_cajas_12_meses', 'box_number']
category_plot_4 = ['price_range_x', 'is_mother', 'brand', 'country']
category_plot_5 = ['fit_abajo', 'fit_arriba', 'talla_arriba', 'talla_abajo']
category_plot_6 = ['talla_copa', 'talla_sujetador', 'n_estilos', 'family_desc', ]

for list_i in [category_plot_1, category_plot_2, category_plot_3, category_plot_4, category_plot_5, category_plot_6]:
    fig, ax = plt.subplots(1, 4, figsize=(12, 5), sharey=True, sharex=False)
    i = 0
    j = 0
    for cat in list_i:
        df_cat = df_gr_cat[df_gr_cat['category'] == cat]

        sns.barplot(data=df_cat, x='category_option', y='KR', label="KR", ax=ax[j])  # color="olive", alpha=0.5,

        # sns.barplot(data=df_cat, x='category_option', y='pct_category', label="Percentage",
        #             facecolor=(1, 1, 1, 0), edgecolor="tomato", linewidth=2.5, ax=ax[j])

        # sns.barplot(data=df_shop, x='size_desc', y='q_adapt_norm', label="Modification",
        #             facecolor=(1, 1, 1, 0), edgecolor="teal", linewidth=1.5, ax=ax[i]) # , alpha=0.5

        ax[j].set_title('' + str(cat))
        ax[j].set_xticklabels(ax[j].get_xticklabels(), fontsize=14, rotation=90)
        ax[j].set(xlabel='', ylabel='')
        plt.legend()
        fig.tight_layout()
        j = j + 1

    plt.savefig(os.path.join(path_save, 'KR_client_category ' + str(list_i) + '.png'))

# df_dg_rej_pct = df_quarter.groupby(['estilo_clienta'], dropna=False)[cathegory_rej].agg(['count']).groupby(level=0).apply(lambda x: 100 * x / float(x.count()))


# filtrar
# purchased != NA
# date_cancelled == ‘’
# KR = sum(purchased == 1) / N

# 29.06.2020 y 05.06.2020
# query_text = 'date_ps_done >= "2019-01-01" & date_ps_done <= "2019-12-31" '
#
# df_demanda_all = pd.read_csv(demanda_file,
#                            usecols=['date_ps_done', 'country', 'family_desc', 'user_id', 'box_id',
#                                     'reference', 'size', 'date_co', 'purchased', 'paid']
#                            ).query(query_text)
#
# 'is_first_box', 'n_cajas_misma_clienta', 'n_cajas_12_mese', 'box_number'



# dedicacion
#
# dedicacion_map = {1: 'Comercio',
#                   2: 'Sanidad',
#                   3: 'Educación',
#                   4: 'Administrativa',
#                   5: 'Ejecutiva',
#                   6: 'Ama de casa',
#                   7: 'Industria',
#                   8: 'Agricultura',
#                   9: 'Serv_domest',
#                   10: 'Estudiante',
#                   11: 'Tecnología',
#                   12: 'Hostelería',
#                   13: 'Adm_publicas',
#                   14: 'Otros'}
# df_clienta['dedicacion_str'] = df_clienta['dedicacion'].map(dedicacion_map)

# category = ['estilo_clienta', 'look_salir', 'look_trabajo',
#             'age_segment', 'height_segment', 'weight_segment',
#             'dedicacion_str',
#             'aventurera',
#             'price_range_x', 'brand',
#
#             'is_first_box', 'n_cajas_misma_clienta', 'n_cajas_12_meses', 'box_number',
#             # 'date_last_box', 'time_since_last_box',
#             # 'AOVn',
#             'fit_abajo', 'fit_arriba',
#             'is_mother',
#             # 'client_is_big_size',
#             # 'talla_abajo_num', 'talla_arriba_num',
#             # 'is_big_size', 'is_petite',
#             'talla_arriba', 'talla_abajo',
#             'talla_copa', 'talla_sujetador',
#             'n_estilos',
#             'family_desc', 'country',
#             # 'style_boho' 'style_casual', 'style_classical', 'style_minimal', 'style_night', 'style_street',
#             ]



# for category_name in caracteristics:
#     # category_name = 'estilo_clienta'
#     print(category_name)
#
#     df_gr = df_quarter.groupby([category_name], dropna=False)['purchased'].agg(['count', 'sum']).reset_index()
#     df_gr['KR'] = df_gr['sum'] / df_gr['count']
#
#     df_gr['pct_category'] = df_gr['count'] * 100 / df_quarter.shape[0]
#     df_gr['KR_pct'] = df_gr['KR'] * df_gr['pct_category']
#     # df_gr = df_gr.sort_values(by=['KR_pct'], ascending=False)
#
#     # rejection
#
#     df_gr_rej = df_quarter.groupby([category_name], dropna=False)[category_rej].agg(['count'])
#
#     df_gr_rej_pct = df_gr_rej / df_quarter.shape[0] * 100
#     df_gr_rej_pct = df_gr_rej_pct.reset_index()
#
#     df_gr_rej_pct.columns = df_gr_rej_pct.columns.droplevel(1)
#
#     df_cat = pd.merge(df_gr,  # [[category_name, 'KR']]
#                       df_gr_rej_pct).sort_values(by=['KR'], ascending=False)
#     df_cat['category'] = category_name
#     df_cat = df_cat.rename(columns={category_name: 'category_option',
#                                     'count': 'envios',
#                                     'sum': 'purchased'})
#
#     df_gr_cat = df_gr_cat.append(df_cat, ignore_index=False)
