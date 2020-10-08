'''Two families: BLUSA and CAMISETA do not follow the rule of the minimum of stock that we establish on the level family-size. They have a good level of stock but the PS feedback is bad.

Investigate these families using another variables such as: color, print, basic, style, fabric, etc.

Use particular dates.

Compare % of sent units to available in stock.

'''


import os
import pandas as pd
import numpy as np
import pickle

######################################################################################################################
# path

file_demanda = ('/var/lib/lookiero/stock/stock_tool/demanda_preprocessed.csv.gz')
file_product = ('/var/lib/lookiero/stock/stock_tool/productos_preprocessed.csv.gz')
file_stock = ('/var/lib/lookiero/stock/stock_tool/stock.csv.gz')

path_results = ('/home/darya/Documents/Reports/2020-09-09-blusa-camiseta-criterios-minimo-stock')

file_feedback = ('/home/darya/Documents/Reports/2020-09-09-blusa-camiseta-criterios-minimo-stock/Stock_Situacion.csv')

#######################################
# feedback
df_feedback = pd.read_csv(file_feedback, usecols=['Familia', 'Talla', 'Fecha', 'Stock NOK'])

df_feedback = df_feedback.rename(columns={'Familia': 'family_desc',
                                          'Talla': 'size',
                                          'Fecha': 'date',
                                          'Stock NOK': 'stock_nok'})

df_feedback['stock_nok'] = df_feedback['stock_nok'].fillna(0)

df_feedback.loc[df_feedback['stock_nok'] != 0, 'stock_nok'] = 1

family_list = list(set(df_feedback['family_desc']))

date_list = list(set(df_feedback['date']))


#######################################
# demanda

query_demanda_text = 'date_ps_done in @date_list and family_desc in @family_list'

df_demanda_raw = pd.read_csv(file_demanda, usecols=['reference', 'date_ps_done', 'family_desc']).query(query_demanda_text)
reference_list = list(set(df_demanda_raw['reference'].to_list()))

df_demanda = df_demanda_raw.groupby(['date_ps_done', 'reference']).size().reset_index(name='demanda')

df_demanda = df_demanda.rename(columns={'date_ps_done': 'date'})

###################################
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


######################################
# add description of the product
# product

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
                #'composicion', ??? Nan
                'origen',
                'color_group',
                'color_category',
                'price_range_product',
                # 'tejido',
                'acabado',
                'premium'
                ]


var_list_opt = []

var_list = var_list_aux + var_list_cat + var_list_opt



query_product_text = 'reference in @reference_list'

df_product_raw = pd.read_csv(file_product, usecols=var_list).query(query_product_text)


df_product_raw = df_product_raw.drop_duplicates('reference', keep='last')

################################

df = pd.merge(df_demanda_stock,
              df_product_raw,
              on=['reference'],
              how='outer')


df = df[df['family_desc'].isin(family_list)]




# dummies




df = pd.get_dummies(df, columns=var_list_cat)



df['stock_actual'] = df['real_stock']
df.loc[df['stock_actual'] < df['demanda'], 'stock_actual'] = df['demanda']





var_group_aux = ['date', 'demanda', 'real_stock', 'family_desc', 'size', 'stock_actual']
var_group = list(set(df.columns.to_list()) - set(var_group_aux) - set(['reference']))

df_var_pct = pd.DataFrame(columns=['date', 'family_desc', 'size'])
df_var_pct_col = pd.DataFrame([])

##########################################
# test


df_test = df.copy()

df = df_test.copy()
df = df[(df['family_desc'] == 'VESTIDO') & (df['date'] == '2020-07-24') & (df['size']=='XXXL')]

#pd.set_option('display.max_columns', None)
df = df[['family_desc', 'size', 'demanda', 'real_stock', 'stock_actual', 'fit_entallado', 'fit_recto', 'fit_holgado', 'fit_oversize', 'date']]
var_group = ['fit_entallado', 'fit_recto', 'fit_holgado', 'fit_oversize']

####### end test

for col in var_group:
    print(col)
    columns = var_group_aux + [col]
    df_var = df[columns].copy()


    df_var[col + '_demanda'] = df_var[col] * df_var['demanda']
    df_var[col + '_stock_real'] = df_var[col] * df_var['real_stock']
    df_var[col + '_stock_actual'] = df_var[col] * df_var['stock_actual']

    df_gr = df_var.groupby(['date', 'family_desc', 'size']).sum().reset_index()
    # stock real, stock appeared in snapshots
    # stock actual, in case when number of items in snapshots is less then in stock_real,

    # porcentaje de demanda de opcion de demanda de variable
    df_gr[col + '_pct_demanda_demanda'] = df_gr[col + '_demanda'] / df_gr['demanda']

    # porcentaje de demanda de opcion de demanda de variable
    df_gr[col + '_pct_stock_stock'] = df_gr[col + '_stock_actual'] / df_gr['stock_actual']

    # percentage of variable option (option "holgado" of variable "fit") shipped of all real stock (snapshot),
    # could be more then 100%
    df_gr[col + '_pct_demanda_stock_real'] = df_gr[col + '_demanda'] / df_gr[col + '_stock_real']

    # percentage of variable option (option "holgado" of variable "fit") shipped of all actual stock (snapshot),
    # could be 100% maximum
    df_gr[col + '_pct_demanda_stock_actual'] = df_gr[col + '_demanda'] / df_gr[col + '_stock_actual']

    # percentage of option (option 'holgado') of variable ('fit') stock, could be 100% maximum
    df_gr[col + '_pct_varstock'] = df_gr[col + '_stock_actual'] / df_gr['stock_actual']

    # percentage of option (option 'holgado') shipped of variable ('fit') stock

    df_gr[col + '_pct_demanda_weight'] = df_gr[col + '_pct_demanda_stock_actual'] * df_gr[col + '_pct_varstock']

    # TODO: distr relativa, absoluta
    # distr relativa	distr abs
    # =if (OR(demanda=0, demanda de stock actual > stock actual), 0, 1)
    df_gr[col + '_distr_relativa'] = np.where((df_gr[col + '_pct_demanda_demanda'] == 0) |
                                              (df_gr[col + '_pct_demanda_demanda'] < df_gr[col + '_pct_stock_stock']),
                                              0, 1)

    df_gr[col + '_distr_abs'] = np.where((df_gr[col + '_demanda'] == 0) |
                                              (df_gr[col + '_pct_demanda_stock_actual'] < 1),
                                              0, 1)



    df_gr = df_gr.fillna(0)

    df_gr = df_gr.replace(np.inf, 1.0)

    # TODO: añadir nuevas columnas
    df_var_pct = df_var_pct.merge(df_gr[['date', 'family_desc', 'size',
                                         col + '_stock_actual',
                                         col + '_pct_demanda_demanda',
                                         col + '_pct_stock_stock',
                                         col + '_pct_demanda_stock_real',
                                         col + '_pct_demanda_stock_actual',
                                         col + '_pct_varstock',
                                         col + '_pct_demanda_weight',
                                         col + '_distr_relativa',
                                         col + '_distr_abs']],
                                  on=['date', 'family_desc', 'size'],
                                  how='outer')

    # save as column

    df_temp = df_var_pct[['date', 'family_desc', 'size']]
    df_temp['varoption_pct_demanda_stock_real'] = df_var_pct[col + '_pct_demanda_stock_real']
    df_temp['varoption_pct_demanda_stock_actual'] = df_var_pct[col + '_pct_demanda_stock_actual']

    df_temp['varoption_pct_varstock'] = df_var_pct[col + '_pct_varstock']
    df_temp['varoption_pct_demanda_weight'] = df_var_pct[col + '_pct_demanda_weight']

    df_temp['varoption'] = col
    df_var_pct_col = df_var_pct_col.append(df_temp)

# TODO df_var_pct_col change inf to 10, nan to 0

df_var_pct = df_var_pct.fillna(0)
df_var_pct_col = df_var_pct_col.fillna(0)

df_var_pct_col = df_var_pct_col.replace(np.inf, 1.0)

df_var_pct_ps = df_var_pct.merge(df_feedback,
                                 on=['date', 'family_desc', 'size'],
                                 how='outer')

df_var_pct_col_ps = df_var_pct_col.merge(df_feedback,
                                 on=['date', 'family_desc', 'size'],
                                 how='outer')

df_var_pct_ps = df_var_pct_ps.fillna(0)
df_var_pct_ps = df_var_pct_ps.replace(np.inf, 1)

# TODO merge df_var_pct_col with PS labels
##################################################################################################################
# save

# df_var_pct_ps.to_csv(os.path.join(path_results, 'date_family_size_var_pct_psfeedback.csv'), index=False)
# df_var_pct_col_ps.to_csv(os.path.join(path_results, 'date_family_size_var_pct_col_psfeedback.csv'), index=False)
#
# aa = df_var_pct_ps.groupby(['family_desc']).agg({'stock_nok': 'mean'})
#
# with open(os.path.join(path_results, 'var_list.txt'), "wb") as fp:  # Pickling
#     pickle.dump(var_group, fp)






#
#
# test = df[(df['family_desc']=='VESTIDO') & (df['date']=='2020-07-24') & (df['size']=='XXXL')]
#
# var_list_aux = ['reference', 'family_desc', 'size']
# var_group = set(df.columns.to_list()) - set(['date', 'reference', 'demanda', 'real_stock', 'family_desc', 'size', 'stock_actual'])
#
#
#
#
# # # eliminate good dates for CAMISETA and good date for BLUSA
# #
# # df = df.drop(df[(df['family_desc'] == 'CAMISETA') & (~df['date'].isin(date_list_camiseta))].index)
# #
# # df = df.drop(df[(df['family_desc'] == 'BLUSA') & (~df['date'].isin(date_list_blusa))].index)
#
#
# df_return = pd.DataFrame([])
# df_threshold = pd.DataFrame([])
#
# var_dummies = set(df.columns.to_list()) - set(['date', 'reference', 'demanda', 'real_stock', 'family_desc', 'size'])
#
# ###############################################
# ###############################################
#
# for var_name in var_list[2:]:
#     print(var_name)
#     # var_name = 'aventurera'
#
#     df_var = df.groupby(['date', 'family_desc', var_name]).agg({'demanda': 'sum',
#                                                                  'real_stock': 'sum'}).reset_index()
#
#     df_var['demanda_pct'] = df_var['demanda'] / df_var['real_stock'] * 100
#
#
#     df_var['demanda_pct_w'] = df_var['demanda_pct'] / df_var['real_stock']
#
#     df_var.loc[(df_var['demanda'] == 0) & (df_var['real_stock'] != 0), 'demanda_pct'] = 0
#     df_var.loc[(df_var['demanda'] != 0) & (df_var['real_stock'] == 0), 'demanda_pct'] = 1
#
#     threshold_min = 20
#
#     threshold_max = 80
#
#     threshols_days = 0.3
#
#
#     df_var_thr = df_var[(df_var['demanda_pct'] < threshold_min) | (df_var['demanda_pct'] > threshold_max)]
#     df_var_thr['var_name'] = var_name
#     df_var_thr = df_var_thr.rename(columns={var_name: 'var_option'})
#     df_threshold = df_threshold.append(df_var_thr)
#
#
#     df_var_thr_min = df_var[df_var['demanda_pct'] < threshold_min]
#
#     df_var_thr_max = df_var[df_var['demanda_pct'] > threshold_max]
#
#
#
#
#
#
#     df_var_thr_min['n'] = 1
#     df_var_thr_max['n'] = 1
#     n_div = len(df['date'].unique())
#
#     df_var_thr_min_fam = df_var_thr_min.groupby(['family_desc', var_name]).agg({'n': 'sum',
#                                                                                 'demanda_pct': 'mean'}).reset_index()
#     df_var_thr_min_fam['n'] = df_var_thr_min_fam['n'] / n_div
#
#     df_var_thr_max_fam = df_var_thr_max.groupby(['family_desc', var_name]).agg({'n': 'sum',
#                                                                                 'demanda_pct': 'mean'}).reset_index()
#     df_var_thr_max_fam['n'] = df_var_thr_max_fam['n'] / n_div
#
#     df_var_demand_low = df_var_thr_min_fam[df_var_thr_min_fam['n'] >= threshols_days]
#
#
#     df_var_demand_high = df_var_thr_max_fam[df_var_thr_max_fam['n'] >= threshols_days]
#
#     df_var_demand_low['var_type'] = var_name
#     df_var_demand_low['problem_type'] = 'demand_low'
#
#     df_var_demand_high['var_type'] = var_name
#     df_var_demand_high['problem_type'] = 'demand_high'
#
#     df_var_demand_low = df_var_demand_low.rename(columns={var_name: 'var_name'})
#     df_var_demand_high = df_var_demand_high.rename(columns={var_name: 'var_name'})
#
#     df_return = df_return.append(df_var_demand_low)
#     df_return = df_return.append(df_var_demand_high)
#
#     # df_return = df_return.rename(columns={var_name: 'var_name'})
#
#     # save

    # df_return.to_csv(os.path.join(path_results, 'blusa_camiseta_low_hight_demand_pct.csv'))
    # df_threshold.to_csv(os.path.join(path_results, 'blusa_camiseta_threshold_pct_pct.csv'))



################ END ################







#####
# brand

# df_brand = df.groupby(['date', 'family_desc', 'brand']).agg({'demanda': 'sum',
#                                                              'real_stock': 'sum'}).reset_index()
#
# df_brand['demanda_pct'] = df_brand['demanda'] / df_brand['real_stock'] * 100
#
#
# df_brand['demanda_pct_w'] = df_brand['demanda_pct'] / df_brand['real_stock']
#
# df_brand.loc[(df_brand['demanda'] == 0) & (df_brand['real_stock'] != 0), 'demanda_pct'] = 0
# df_brand.loc[(df_brand['demanda'] != 0) & (df_brand['real_stock'] == 0), 'demanda_pct'] = 1
#
# threshold_min = 20
#
# threshold_max = 80
#
#
# df_brand_thr = df_brand[(df_brand['demanda_pct'] < threshold_min) | (df_brand['demanda_pct'] > threshold_max)]
#
#
#
# df_brand_thr_min = df_brand[df_brand['demanda_pct'] < threshold_min]
#
# df_brand_thr_max = df_brand[df_brand['demanda_pct'] > threshold_max]
#
#
#
#
#
#
# df_brand_thr_min['n'] = 1
# df_brand_thr_max['n'] = 1
# n_div = len(df['date'].unique())
#
# df_brand_thr_min_fam = df_brand_thr_min.groupby(['family_desc', 'brand']).agg({'n': 'sum'}).reset_index()
# df_brand_thr_min_fam['n'] = df_brand_thr_min_fam['n'] / n_div
#
# df_brand_thr_max_fam = df_brand_thr_max.groupby(['family_desc', 'brand']).agg({'n': 'sum'}).reset_index()
# df_brand_thr_max_fam['n'] = df_brand_thr_max_fam['n'] / n_div
#
# df_brand_demand_low = df_brand_thr_min_fam[df_brand_thr_min_fam['n'] >= 0.5]
#
#
# df_brand_demand_high = df_brand_thr_max_fam[df_brand_thr_max_fam['n'] >= 0.5]
#
# df_brand_demand_low['var_type'] = 'brand'
# df_brand_demand_low['problem_type'] = 'demand_low'
#
# df_brand_demand_high['var_type'] = 'brand'
# df_brand_demand_high['problem_type'] = 'demand_high'
#
#
#
#
# df_return = df_return.append(df_brand_demand_low)
# df_return = df_return.append(df_brand_demand_high)
#
# df_return = df_return.rename(columns={'brand': 'var_name'})


