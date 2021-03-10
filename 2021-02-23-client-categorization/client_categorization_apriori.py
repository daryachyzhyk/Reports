import os
import pandas as pd
import numpy as np
from mlxtend.frequent_patterns import apriori, association_rules, fpgrowth
import seaborn as sns
import matplotlib.pyplot as plt

path_save = '/home/darya/Documents/Reports/2021-02-23-client-categorization'

file_data = '/home/darya/Documents/Reports/2021-02-23-client-categorization/df_demanda_clienta_dummy_es_2021-01-01_2021-02-28.csv.gz'

df_dummy = pd.read_csv(file_data, index_col=0)


################################################################################################################
# Apriori


def zhangs_rule(rules):
    PAB = rules['support'].copy()
    PA = rules['antecedent support'].copy()
    PB = rules['consequent support'].copy()
    numerator = PAB - PA * PB
    denominator = np.max((PAB * (1 - PA).values, PA * (PB - PAB).values), axis=0)
    return numerator / denominator


columns_all = df_dummy.columns.to_series()

columns_drop = ['country_es', 'country_nan',
                'talla_arriba_X4XL', 'talla_arriba_XXL', 'talla_arriba_XXXL',
                'talla_abajo_X4XL', 'talla_abajo_XXL', 'talla_abajo_XXXL',
                'price_range_nan',
                'n_cajas_misma_clienta_15.0', 'n_cajas_misma_clienta_16.0', 'n_cajas_misma_clienta_17.0',
                'n_cajas_misma_clienta_18.0', 'n_cajas_misma_clienta_19.0', 'n_cajas_misma_clienta_20.0',
                'n_cajas_misma_clienta_21.0', 'n_cajas_misma_clienta_22.0', 'n_cajas_misma_clienta_23.0',
                'n_cajas_misma_clienta_24.0',
                'n_cajas_misma_clienta_25.0', 'n_cajas_misma_clienta_26.0', 'n_cajas_misma_clienta_27.0',
                'n_cajas_misma_clienta_28.0', 'n_cajas_misma_clienta_29.0', 'n_cajas_misma_clienta_30.0',
                'n_cajas_misma_clienta_31.0', 'n_cajas_misma_clienta_32.0', 'n_cajas_misma_clienta_33.0',
                'n_cajas_misma_clienta_34.0',
                'n_cajas_misma_clienta_35.0', 'n_cajas_misma_clienta_36.0', 'n_cajas_misma_clienta_37.0',
                'n_cajas_misma_clienta_38.0', 'n_cajas_misma_clienta_39.0', 'n_cajas_misma_clienta_40.0',
                'n_cajas_misma_clienta_41.0', 'n_cajas_misma_clienta_42.0', 'n_cajas_misma_clienta_43.0',
                'n_cajas_misma_clienta_44.0',
                'n_cajas_misma_clienta_45.0', 'n_cajas_misma_clienta_46.0',
                'n_cajas_misma_clienta_48.0',  'n_cajas_misma_clienta_50.0',
                'n_cajas_misma_clienta_52.0', 'n_cajas_misma_clienta_53.0',
                'n_cajas_misma_clienta_55.0',
                'n_cajas_12_meses_13.0', 'n_cajas_12_meses_14.0', 'n_cajas_12_meses_15.0', 'n_cajas_12_meses_13.0',
                'n_cajas_12_meses_17.0', 'n_cajas_12_meses_19.0', 'n_cajas_12_meses_20.0',
                'n_cajas_12_meses_21.0', 'n_cajas_12_meses_nan',
                'box_number_1.0', 'box_number_2.0', 'box_number_3.0', 'box_number_4.0', 'box_number_5.0',
                'box_number_6.0', 'box_number_7.0', 'box_number_8.0', 'box_number_9.0', 'box_number_10.0',
                'box_number_11.0', 'box_number_12.0', 'box_number_13.0', 'box_number_14.0', 'box_number_15.0',
                'box_number_16.0', 'box_number_17.0', 'box_number_18.0', 'box_number_19.0', 'box_number_20.0',
                'box_number_21.0', 'box_number_22.0', 'box_number_23.0', 'box_number_24.0', 'box_number_25.0',
                'box_number_26.0', 'box_number_27.0', 'box_number_28.0', 'box_number_29.0', 'box_number_30.0',
                'box_number_31.0', 'box_number_32.0', 'box_number_33.0', 'box_number_34.0', 'box_number_35.0',
                'box_number_36.0', 'box_number_37.0', 'box_number_38.0', 'box_number_39.0', 'box_number_40.0',
                'box_number_41.0', 'box_number_42.0', 'box_number_43.0', 'box_number_44.0', 'box_number_45.0',
                'box_number_46.0', 'box_number_47.0', 'box_number_48.0', 'box_number_50.0',
                'box_number_51.0', 'box_number_52.0', 'box_number_53.0', 'box_number_54.0', 'box_number_55.0',
                'box_number_nan'
                ]
df_dummy_drop = df_dummy.drop(columns=columns_drop)

frequent_itemsets = apriori(df_dummy_drop.iloc[0:30000, :], min_support=0.006, max_len=2, use_colnames=True).sort_values(by='support', ascending=False)


frequent_fpgrowth = fpgrowth(df_dummy_drop.iloc[0:10000, :], min_support=0.006, use_colnames=True).sort_values(by='support', ascending=False)

# frequent_itemsets = apriori(df_dummy.iloc[0:10000, :], min_support=0.006, max_len=2, use_colnames=True).sort_values(by='support', ascending=False)
# frequent_itemsets1 = apriori(df_dummy.iloc[0:20000, :], min_support=0.006, max_len=2, use_colnames=True).sort_values(by='support', ascending=False)


rules = association_rules(frequent_itemsets, metric="support", min_threshold=0.0015)

rules['zhang'] = zhangs_rule(rules)

rules_zhang_pos = rules[rules['zhang'] > 0.1].sort_values(by='zhang', ascending=False)

rules_zhang_neg = rules[rules['zhang'] < -0.1].sort_values(by='zhang', ascending=True)