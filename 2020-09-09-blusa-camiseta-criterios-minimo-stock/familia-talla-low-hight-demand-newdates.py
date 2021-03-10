'''Two families: BLUSA and CAMISETA do not follow the rule of the minimum of stock that we establish on the level family-size. They have a good level of stock but the PS feedback is bad.

Investigate these families using another variables such as: color, print, basic, style, fabric, etc.

Use particular dates.

Compare % of sent units to available in stock.

pd.set_option('display.max_columns', None)

# TODO future: cuando hacemos el promedio de toda las variables para familia talla,
# podemos ponderar por importancia de variables, por ejemplo hacer un PCA o random Forest para predecir un indicador de negocio (roturas)
'''


import os
import pandas as pd
import numpy as np
import datetime
from joblib import Parallel, delayed, parallel_backend


def opt_sum(opt, df_opt_dummy):
    df_opt = pd.DataFrame([])
    df_opt['opt_demanda'] = df_opt_dummy[opt] * df_opt_dummy['demanda']
    df_opt['opt_stock_actual'] = df_opt_dummy[opt] * df_opt_dummy['stock_actual']
    df_opt['demanda'] = df_opt_dummy['demanda']
    df_opt['stock_actual'] = df_opt_dummy['stock_actual']
    opt_sum = df_opt.sum()
    return opt_sum


def var_loop(var, df_fam_sz_var):

    if ~df_fam_sz_var[var].isnull().all():
        # dummies

        df_dummy = pd.get_dummies(df_fam_sz_var[var], columns=var)
        var_group_aux = ['date', 'family_desc', 'size', 'demanda', 'real_stock', 'stock_actual']
        df_opt_dummy = pd.concat([df_fam_sz_var[var_group_aux], df_dummy], axis=1)
        var_opt_list = df_dummy.columns.to_list()

        # for each option of the variable calculate distr_abs and distr_relativa

        # option
        with parallel_backend('threading', n_jobs=6):
            opt_sum_paral = Parallel()(
                delayed(opt_sum)(opt, df_opt_dummy) for opt in var_opt_list)

        df_gr = pd.DataFrame(opt_sum_paral)
        df_gr['pct_demanda'] = df_gr['opt_demanda'] / df_gr['demanda']

        # porcentaje de demanda de opcion de demanda de variable
        df_gr['pct_stock'] = df_gr['opt_stock_actual'] / df_gr['stock_actual']

        df_gr['pct_demanda_stock_actual'] = df_gr['opt_demanda'] / df_gr['opt_stock_actual']

        # df_gr['distr_relative'] = np.where((df_gr['pct_demanda'] == 0) | (df_gr['pct_demanda'] < df_gr['pct_stock']), 0, 1)

        df_gr['distr_abs'] = np.where((df_gr['demanda'] == 0) | (df_gr['pct_demanda_stock_actual'] < 1), 0, 1)

        date_family_size_var_valor_tuple = (var,
                                             # (df_gr['distr_relative'] * df_gr['pct_demanda']).sum(),
                                             (df_gr['distr_abs'] * df_gr['pct_demanda']).sum())

    else:
        date_family_size_var_valor_tuple = (var,
                                             # np.nan,
                                             np.nan)
    return date_family_size_var_valor_tuple


def get_var_distr_relat_abs(date_family_size, df):
    # date_family_size = date_family_size_list[0]


    dt = date_family_size[0]
    family = date_family_size[1]
    sz = date_family_size[2]
    # family = 'BLUSA'
    # size = 'XL'
    print(dt)
    print(family)
    print(sz)
    df_fam_sz_var = df[(df['date'] == dt) & (df['family_desc'] == family) & (df['size'] == sz)]



    with parallel_backend('threading', n_jobs=6):
        date_family_size_var_valor_list = Parallel()(
            delayed(var_loop)(var, df_fam_sz_var) for var in var_list_cat)

    df_date_family_size_var_valor = pd.DataFrame(date_family_size_var_valor_list, columns=['var',
                                                                                           # 'distr_relative',
                                                                                           'distr_abs'])
    df_date_family_size_var_valor['date'] = dt
    df_date_family_size_var_valor['family_desc'] = family
    df_date_family_size_var_valor['size'] = sz
    df_date_family_size_var_valor = df_date_family_size_var_valor.dropna()

    return df_date_family_size_var_valor


######################################################################################################################
# path

file_demanda = ('/var/lib/lookiero/stock/stock_tool/demanda_preprocessed.csv.gz')
file_product = ('/var/lib/lookiero/stock/stock_tool/productos_preprocessed.csv.gz')
file_stock = ('/var/lib/lookiero/stock/stock_tool/stock.csv.gz')

path_results = ('/home/darya/Documents/Reports/2020-09-09-blusa-camiseta-criterios-minimo-stock')

file_feedback = ('/home/darya/Documents/Reports/2020-09-09-blusa-camiseta-criterios-minimo-stock/Stock_Situacion.csv')

######################################################################################################################
# feedback
df_feedback = pd.read_csv(file_feedback, usecols=['Familia', 'Talla', 'Fecha'])

df_feedback = df_feedback.rename(columns={'Familia': 'family_desc',
                                          'Talla': 'size',
                                          'Fecha': 'date'})

family_list = list(set(df_feedback['family_desc']))


date_start_str = df_feedback['date'].iloc[0]

date_end_str = datetime.date.today().strftime('%Y-%m-%d')

date_list = [d.strftime('%Y-%m-%d') for d in pd.bdate_range(date_start_str, date_end_str)]

# [d.strftime('%Y%m%d') for d in pandas.date_range('20130226','20130302')]
# date_list2 = df_feedback['date'].unique().tolist() #list(set(df_feedback['date']))


######################################################################################################################
# demanda

query_demanda_text = 'date_ps_done in @date_list and family_desc in @family_list'

df_demanda_raw = pd.read_csv(file_demanda,
                             usecols=['reference', 'date_ps_done', 'family_desc']).query(query_demanda_text)

reference_list = list(set(df_demanda_raw['reference'].to_list()))

df_demanda = df_demanda_raw.groupby(['date_ps_done', 'reference']).size().reset_index(name='demanda')

df_demanda = df_demanda.rename(columns={'date_ps_done': 'date'})

######################################################################################################################
# stock

query_stock_text = 'date in @date_list and reference in @reference_list'

df_stock_raw = pd.read_csv(file_stock).query(query_stock_text)

# visible
df_stock = df_stock_raw.drop(df_stock_raw[(df_stock_raw['es'] == 0) & (df_stock_raw['fr'] == 0) &
                                          (df_stock_raw['gb'] == 0) & (df_stock_raw['pt'] == 0) &
                                          (df_stock_raw['be'] == 0) & (df_stock_raw['lu'] == 0) &
                                          (df_stock_raw['it'] == 0)].index)

df_demanda_stock = pd.merge(df_demanda,
                            df_stock[['date', 'real_stock', 'reference']],
                            on=['date', 'reference'],
                            how='outer')

df_demanda_stock['real_stock'] = df_demanda_stock['real_stock'].fillna(0)
df_demanda_stock['demanda'] = df_demanda_stock['demanda'].fillna(0)

######################################################################################################################
# add description of the product

var_list_aux = ['reference', 'family_desc', 'size']

# TODO add 'composicion' , corte, grosor, ligero -> all fields are Nan
var_list_cat = ['clima',
                # 'aventurera',
                'basico',
                'estilo_producto',
                'fit',
                'uso',
                # 'pattern',
                'has_pattern',
                'composicion',
                'origen',
                'color_group',
                'color_category',
                'price_range_product',
                # 'tejido',
                'acabado',
                # TODO: añadir premium
                #'premium',
                # 'corte',
                # 'grosor',
                # 'ligero'
                ]

# var_list_opt = []

# var_list = var_list_aux + var_list_cat + var_list_opt
var_list = var_list_aux + var_list_cat

query_product_text = 'reference in @reference_list'

df_product_raw = pd.read_csv(file_product, usecols=var_list).query(query_product_text)

df_product_raw = df_product_raw.drop_duplicates('reference', keep='last')

######################################################################################################################

df = pd.merge(df_demanda_stock,
              df_product_raw,
              on=['reference'],
              how='outer')


df = df[df['family_desc'].isin(family_list)]

# stock actual

df['stock_actual'] = df['real_stock']
df.loc[df['stock_actual'] < df['demanda'], 'stock_actual'] = df['demanda']

date_family_size_list = list(zip(df_feedback['date'], df_feedback['family_desc'], df_feedback['size']))

#################################3
# test
# date_family_size_list = list(zip(['2020-07-24', '2020-07-24'], ['VESTIDO', 'VESTIDO'], ['M', 'XXXL']))

# result
# Out[14]:
#          date family_desc  size  mean_weight_relative  mean_weight_abs      stock_nok
# 0  2020-07-24     VESTIDO     M              0.472393         0.032209        0
# 1  2020-07-24     VESTIDO  XXXL              0.722222         0.506944        1
#

#### test end #########

######################################################################################################################
# run

with parallel_backend('threading', n_jobs=6):
    date_family_size_var_valor_list = Parallel()(
        delayed(get_var_distr_relat_abs)(date_family_size, df) for date_family_size in date_family_size_list)

df_indicators = pd.concat(date_family_size_var_valor_list)

# wothout distr_relative


df_indicators_gr = df_indicators.groupby(['date', 'family_desc', 'size']).agg({'distr_abs': 'mean'}).reset_index()


# with distr_relative
# df_indicators_label = pd.merge(df_indicators, df_feedback, on=['date', 'family_desc', 'size'])
#
# df_indicators_label_gr = df_indicators_label.groupby(['date', 'family_desc', 'size']).agg({'distr_relative': 'mean',
#                                                                                            'distr_abs': 'mean',
#                                                                                            'stock_nok': 'last'}).reset_index()
# df_indicators_label_gr['distr_mean'] = (df_indicators_label_gr['distr_relative'] + df_indicators_label_gr['distr_abs']) / 2


# save
date_save = str(df.date.max())
df_indicators.to_csv(os.path.join(path_results, 'date_family_size_var_mean_weight_relat_abs_psfeedback_' + date_save + '.csv'), index=False)

df_indicators_gr.to_csv(os.path.join(path_results, 'date_family_size_mean_var_mean_weight_relat_abs_psfeedback_' + date_save + '.csv'), index=False)




