"""
Script
"""


import pandas as pd
import os
import pickle
import numpy as np
from itertools import product

from sklearn.metrics import accuracy_score, f1_score, roc_auc_score, precision_score, recall_score, confusion_matrix

##################################################################################################
# path

path = ('/home/darya/Documents/Reports/2020-09-09-blusa-camiseta-criterios-minimo-stock')
path_save = ('/home/darya/Documents/Reports/2020-09-09-blusa-camiseta-criterios-minimo-stock/pct')
path_results = ('/home/darya/Documents/Reports/2020-09-09-blusa-camiseta-criterios-minimo-stock')


##################################################################################################
# params
df_raw = pd.read_csv(os.path.join(path, 'date_family_size_mean_var_mean_weight_relat_abs_psfeedback.csv'))
df_raw['size'] = df_raw['size'].str.upper()
# df_raw.columns = sorted(df_raw.columns)


df_raw['stock_nok'].value_counts()
# 0.0    3130
# 1.0     2091

# 0    2318
# 1    1590

print(df_raw.groupby(['family_desc']).agg({'stock_nok': 'mean'}).sort_values(by='stock_nok'))


list_family_drop = ['PARKA', 'DENIM']

df_raw = df_raw[~df_raw['family_desc'].isin(list_family_drop)]
print(df_raw['stock_nok'].value_counts(normalize=True))

# 0.0    2535
# 1.0    2028
#
# 0    0.534305
# 1    0.465695

######################################################################################################################
# Accuracy
# accuracy_score(df_raw['stock_nok'], df_raw['distr_abs'])



#############################################################################################
# Accuracy per variables and threshold

# with open(os.path.join(path_results, 'var_list.txt'), "rb") as fp:  # Unpickling
#
#     var_group = pickle.load(fp)

var_group = ['distr_relative', 'distr_abs', 'distr_mean']


df_raw_col = df_raw[['date', 'family_desc', 'size', 'stock_nok']]

y_true = df_raw['stock_nok']



accuracy_col = []
f1_score_col = []
roc_col = []
precision_col = []
recall_col = []

col_thr_list = []

for col in var_group:
    print(col)
    # df_raw_col = df_raw_col.join(df_raw[col])

    list_thresholds = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
    for threshold in list_thresholds:
        print(threshold)

        df_col = np.where(df_raw[col] > threshold, 1, 0)
        # df_aux = pd.DataFrame(df_aux, columns=df_thr.columns, index=df_thr.index)
        # df_aux = df_aux.fillna(0)
        # df_iqual = df_aux.eq(df['stock_nok'], axis=0)
        # TODO add sklearn metrics
        # accuracy_col = []
        # f1_score_col = []
        #
        # roc_col = []
        # precision_col = []
        # recall_col = []

        # for col in df_aux.columns:
        accuracy_col.append(accuracy_score(y_true, df_col))
        f1_score_col.append(f1_score(y_true, df_col))

        roc_col.append(roc_auc_score(y_true, df_col))
        precision_col.append(precision_score(y_true, df_col, zero_division=0))
        recall_col.append(recall_score(y_true, df_col))

        col_thr_list.append((col, threshold))

df = pd.DataFrame(col_thr_list, columns=['indicatot', 'threshold'])
df['accuracy'] = accuracy_col
df['f1_score'] = f1_score_col
df['ric'] = roc_col
df['precision'] = precision_col
df['recall'] = recall_col


df.to_csv(os.path.join(path_results, 'accuracy_precision_variedad.csv'), index=False)




