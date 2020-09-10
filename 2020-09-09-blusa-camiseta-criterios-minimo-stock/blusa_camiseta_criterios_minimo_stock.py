'''Two families: BLUSA and CAMISETA do not follow the rule of the minimum of stock that we establish on the level family-size. They have a good level of stock but the PS feedback is bad.

Investigate these families using another variables such as: color, print, basic, style, fabric, etc.

Use particular dates.

Compare % of sent units to available in stock.

'''


import pandas as pd


# path

file_demanda = ('/var/lib/lookiero/stock/stock_tool/demanda_preprocessed.csv.gz')

file_product = ('/var/lib/lookiero/stock/stock_tool/productos_preprocessed.csv.gz')
file_stock = ('/var/lib/lookiero/stock/stock_tool/stock.csv.gz')


family_list = ['BLUSA', 'CAMISETA']

# 'reference', 'category'
date_list = ['2020-07-24', '2020-07-27', '2020-07-28', '2020-07-29', '2020-07-30', '2020-07-31', '2020-08-03',
             '2020-08-04', '2020-08-05', '2020-08-06', '2020-08-10', '2020-08-11', '2020-08-12', '2020-08-13',
             '2020-08-14', '2020-08-17', '2020-08-18', '2020-08-19', '2020-08-20', '2020-08-21']

#######################################
# demanda

query_demanda_text = 'date_ps_done in @date_list and family_desc in @family_list'

df_demanda_raw = pd.read_csv(file_demanda, usecols=['reference', 'date_ps_done', 'family_desc']).query(query_demanda_text)
reference_list = list(set(df_demanda_raw['reference'].to_list()))

# df_demanda_raw['family_desc'].value_counts(dropna=True)



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



var_list = ['reference', 'family_desc',
            'brand', 'color', 'aventurera', 'basico', 'composicion',
            # 'estilo_boho',	'estilo_casual', 'estilo_clasico', 'estilo_minimal', 'estilo_noche', 'estilo_street',
            'fit', 'pattern', 'tejido',
            # 'uso_administrativa', 'uso_eventos', 'uso_noche', 'uso_tiempo_libre', 'uso_working_girl',
            'color_group', 'color_category',
            'price_range_product',
            # 'has_pattern', 'basico_pattern',
            'clima', 'uso',
            'estilo_producto',
            'acabado']



query_product_text = 'reference in @reference_list'

df_product_raw = pd.read_csv(file_product, usecols=var_list).query(query_product_text)


df_product_raw = df_product_raw.drop_duplicates('reference', keep='last')

################################

df = pd.merge(df_demanda_stock,
              df_product_raw,
              on=['reference'],
              how='outer')


df = df[df['family_desc'].isin(family_list)]

df_brand = df.groupby(['date', 'family_desc', 'brand']).agg({'demanda': 'sum',
                                                             'real_stock': 'sum'}).reset_index()

df_brand['demanda_pct'] = df_brand['demanda'] / df_brand['real_stock'] * 100


df_brand['demanda_pct_w'] = df_brand['demanda_pct'] / df_brand['real_stock']

df_brand.loc[(df_brand['demanda'] == 0) & (df_brand['real_stock'] != 0), 'demanda_pct'] = 0
df_brand.loc[(df_brand['demanda'] != 0) & (df_brand['real_stock'] == 0), 'demanda_pct'] = 1

threshold_min = 20

threshold_max = 80


df_brand_thr = df_brand[(df_brand['demanda_pct'] < threshold_min) | (df_brand['demanda_pct'] > threshold_max)]



df_brand_thr_min = df_brand[df_brand['demanda_pct'] < threshold_min]

df_brand_thr_max = df_brand[df_brand['demanda_pct'] > threshold_max]






df_brand_thr_min['n'] = 1
df_brand_thr_max['n'] = 1
n_div = len(df['date'].unique())

df_brand_thr_min_fam = df_brand_thr_min.groupby(['family_desc', 'brand']).agg({'n': 'sum'}).reset_index()
df_brand_thr_min_fam['n'] = df_brand_thr_min_fam['n'] / n_div

df_brand_thr_max_fam = df_brand_thr_max.groupby(['family_desc', 'brand']).agg({'n': 'sum'}).reset_index()
df_brand_thr_max_fam['n'] = df_brand_thr_max_fam['n'] / n_div

df_brand_demand_low = df_brand_thr_min_fam[df_brand_thr_min_fam['n'] >= 0.5]


df_brand_demand_high = df_brand_thr_max_fam[df_brand_thr_max_fam['n'] >= 0.5]

df_brand_demand_low['var_type'] = 'brand'
df_brand_demand_low['problem_type'] = 'demand_low'

df_brand_demand_high['var_type'] = 'brand'
df_brand_demand_high['problem_type'] = 'demand_high'


df_return = pd.DataFrame([])

df_return = df_return.append(df_brand_demand_low)
df_return = df_return.append(df_brand_demand_high)

df_return = df_return.rename(columns={'brand': 'var_name'})



