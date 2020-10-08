'''
Example fit

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

var_list = ['reference', 'family_desc', 'size',
            'brand',
            'aventurera', 'basico', 'composicion',
            # 'estilo_boho',	'estilo_casual', 'estilo_clasico', 'estilo_minimal', 'estilo_noche', 'estilo_street',
            'fit', 'pattern', 'tejido',
            # 'uso_administrativa', 'uso_eventos', 'uso_noche', 'uso_tiempo_libre', 'uso_working_girl',
            'color', 'color_group', 'color_category',
            'price_range_product',
            # 'has_pattern', 'basico_pattern',
            'clima', 'uso',
            'estilo_producto',
            'acabado']

var_list_aux = ['reference', 'family_desc', 'size']

var_list_cat = ['clima',
                # 'aventurera',
                'basico',
                'estilo_producto',
                'fit',
                'uso',
                # 'size',
                # 'pattern',
                'has_pattern',
                #'composicion', ??? Nan
                'origen',
                'color_group',
                'color_category', 'price_range_product',
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
df['size_cat'] = df['size']
var_list_cat.append('size_cat')


df = pd.get_dummies(df, columns=var_list_cat) # , prefix='', prefix_sep=''

df['stock_actual'] = df['real_stock']
df.loc[df['stock_actual'] < df['demanda'], 'stock_actual'] = df['demanda']




var_group_aux = ['date', 'demanda', 'real_stock', 'family_desc', 'size', 'stock_actual']
var_group = list(set(df.columns.to_list()) - set(var_group_aux) - set(['reference']))




# cols = ['fit_entallado', 'fit_recto', 'fit_holgado', 'fit_oversize']
# test2 = test[cols].mul(test['demanda'], axis=0).add_suffix('_demanda')
# test3 = test[cols].mul(test['stock_actual'], axis=0).add_suffix('_stock_actual')
# test_var = pd.concat([test[var_group_aux], test2], axis=1)
# test_var = pd.concat([test_var, test3], axis=1)


df_var_pct = pd.DataFrame(columns=['date', 'family_desc', 'size'])
df_var_pct_col = pd.DataFrame([])

########################################################################################
# example

# test = df[(df['family_desc'] == 'VESTIDO') & (df['date'] == '2020-07-24') & (df['size'] == 'XXXL')]
# test
df_test = df.copy()

df = df_test.copy()
df = df[(df['family_desc'] == 'VESTIDO') & (df['date'] == '2020-07-24') & (df['size']=='XXXL')]

pd.set_option('display.max_columns', None)
df = df[['family_desc', 'size', 'demanda', 'real_stock', 'stock_actual', 'fit_entallado', 'fit_recto', 'fit_holgado', 'fit_oversize', 'date']]

# df.to_excel(os.path.join(path_results, 'example_df_inicial.xlsx'), index=False)


for col in ['fit_entallado', 'fit_recto', 'fit_holgado', 'fit_oversize']:
    print(col)
    columns = var_group_aux + [col]
    df_var = df[columns].copy()


    df_var[col + '_demanda'] = df_var[col] * df_var['demanda']
    df_var[col + '_stock_real'] = df_var[col] * df_var['real_stock']
    df_var[col + '_stock_actual'] = df_var[col] * df_var['stock_actual']

    df_gr = df_var.groupby(['date', 'family_desc', 'size']).sum().reset_index()
    # stock real, stock appeared in snapshots
    # stock actual, in case when number of items in snapshots is less then in stock_real,

    # percentage of variable option (option "holgado" of variable "fit") shipped of all real stock (snapshot),
    # could be more then 100%
    df_gr[col + '_shipped_stock_real_pct'] = df_gr[col + '_demanda'] / df_gr[col + '_stock_real']

    # percentage of variable option (option "holgado" of variable "fit") shipped of all actual stock (snapshot),
    # could be 100% maximum
    df_gr[col + '_shipped_stock_actual_pct'] = df_gr[col + '_demanda'] / df_gr[col + '_stock_actual']

    # percentage of option (option 'holgado') of variable ('fit') stock, could be 100% maximum
    df_gr[col + '_varstock_pct'] = df_gr[col + '_stock_actual'] / df_gr['stock_actual']

    # percentage of option (option 'holgado') shipped of variable ('fit') stock



    df_gr[col + '_shipped_weight_pct'] = df_gr[col + '_shipped_stock_actual_pct'] * df_gr[col + '_varstock_pct']

    df_gr = df_gr.fillna(0)

    df_gr = df_gr.replace(np.inf, 10.0)

    df_var_pct = df_var_pct.merge(df_gr[['date', 'family_desc', 'size',
                                         col + '_stock_actual',
                                         col + '_shipped_stock_real_pct',
                                         col + '_shipped_stock_actual_pct',
                                         col + '_varstock_pct',
                                         col + '_shipped_weight_pct']],
                                  on=['date', 'family_desc', 'size'],
                                  how='outer')

    # TODO: save as column

    df_temp = df_var_pct[['date', 'family_desc', 'size']]
    df_temp['varoption_shipped_stock_real_pct'] = df_var_pct[col + '_shipped_stock_real_pct']
    df_temp['varoption_shipped_stock_actual_pct'] = df_var_pct[col + '_shipped_stock_actual_pct']

    df_temp['varoption_varstock_pct'] = df_var_pct[col + '_varstock_pct']
    df_temp['varoption_shipped_weight_pct'] = df_var_pct[col + '_shipped_weight_pct']

    df_temp['varoption'] = col
    df_var_pct_col = df_var_pct_col.append(df_temp)

# TODO df_var_pct_col change inf to 10, nan to 0

df_var_pct = df_var_pct.fillna(0)
df_var_pct_col = df_var_pct_col.fillna(0)

df_var_pct_col = df_var_pct_col.replace(np.inf, 10.0)

df_var_pct_ps = df_var_pct.merge(df_feedback,
                                 on=['date', 'family_desc', 'size'],
                                 how='outer')

df_var_pct_col_ps = df_var_pct_col.merge(df_feedback,
                                 on=['date', 'family_desc', 'size'],
                                 how='outer')

df_var_pct_ps = df_var_pct_ps.fillna(0)
df_var_pct_ps = df_var_pct_ps.replace(np.inf, 1)



# # save
# df_var_pct_ps.to_csv(os.path.join(path_results, 'date_family_size_var_pct_psfeedback.csv'), index=False)
# df_var_pct_col_ps.to_csv(os.path.join(path_results, 'date_family_size_var_pct_col_psfeedback.csv'), index=False)
#
# aa = df_var_pct_ps.groupby(['family_desc']).agg({'stock_nok': 'mean'})
#
# with open(os.path.join(path_results, 'var_list.txt'), "wb") as fp:  # Pickling
#     pickle.dump(var_group, fp)



