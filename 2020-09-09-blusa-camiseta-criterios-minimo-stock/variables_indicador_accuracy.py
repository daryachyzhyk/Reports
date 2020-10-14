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
df_raw['stock_nok'].value_counts(normalize=True)

# 0.0    2535
# 1.0    2028
#
# 0    0.534305
# 1    0.465695

######################################################################################################################
# Accuracy
accuracy_score(df_raw['stock_nok'], df_raw['distr_abs'])



accuracy_col.append(accuracy_score(y_true, df_aux[col]))
            f1_score_col.append(f1_score(y_true, df_aux[col]))

            roc_col.append(roc_auc_score(y_true, df_aux[col]))
            precision_col.append(precision_score(y_true, df_aux[col], zero_division=0))
            recall_col.append(recall_score(y_true, df_aux[col]))

#############################################################################################
# load data

with open(os.path.join(path_results, 'var_list.txt'), "rb") as fp:  # Unpickling

    var_group = pickle.load(fp)


# TODO remove reference
# var_group = list(set(var_group) - set(['reference', 'date', 'family_desc', 'size', 'stock_nok']))



df_raw_col = df_raw[['date', 'family_desc', 'size', 'stock_nok']]

var_list = []
for col in var_group:
    df_raw_col = df_raw_col.join(df_raw[col + '_shipped_weight_pct'])
    var_list.append(col + '_shipped_weight_pct')




df = df_raw_col.copy()


# df = df[~df['stock_nok'].isnull()]
#
# df = df.fillna(0)

df_threshold = df[var_list]



family_list = list(df['family_desc'].unique())
size_list = list(df['size'].unique())

# TODO: xs to XS
df_best_pct_precision = pd.DataFrame([])
# family_size_list = list(product(df['family_desc'], df['size']))

family_size_list = list(zip(df['family_desc'], df['size']))

for family_size in family_size_list:
    # for sz in size_list:
    family = family_size[0]
    sz = family_size[1]
    print(family)
    print(sz)
    df_thr = df_threshold[(df['family_desc'] == family) & (df['size'] == sz)]

    y_true = df[(df['family_desc'] == family) & (df['size'] == sz)]['stock_nok']

    accuracy_list = []
    f1_score_list = []

    roc_list = []
    precision_list = []
    recall_list = []



    i = -1
    # list_thresholds = [0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
    list_thresholds = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
    for threshold in list_thresholds:
        print(threshold)
        i = i + 1
        df_aux = np.where(df_thr > threshold, 1, 0)
        df_aux = pd.DataFrame(df_aux, columns=df_thr.columns, index=df_thr.index)
        # df_aux = df_aux.fillna(0)
        # df_iqual = df_aux.eq(df['stock_nok'], axis=0)
        # TODO add sklearn metrics
        accuracy_col = []
        f1_score_col = []

        roc_col = []
        precision_col = []
        recall_col = []

        for col in df_aux.columns:
            accuracy_col.append(accuracy_score(y_true, df_aux[col]))
            f1_score_col.append(f1_score(y_true, df_aux[col]))

            roc_col.append(roc_auc_score(y_true, df_aux[col]))
            precision_col.append(precision_score(y_true, df_aux[col], zero_division=0))
            recall_col.append(recall_score(y_true, df_aux[col]))



        accuracy_list.append(accuracy_col)
        f1_score_list.append(f1_score_col)

        roc_list.append(roc_col)
        precision_list.append(precision_col)
        recall_list.append(recall_col)

    df_accuracy = pd.DataFrame(accuracy_list, columns=sorted(df_thr.columns), index=list_thresholds)
    df_f1_score = pd.DataFrame(f1_score_list, columns=sorted(df_thr.columns), index=list_thresholds)

    df_roc = pd.DataFrame(roc_list, columns=sorted(df_thr.columns), index=list_thresholds)
    df_precision = pd.DataFrame(precision_list, columns=sorted(df_thr.columns), index=list_thresholds)
    df_recall = pd.DataFrame(recall_list, columns=sorted(df_thr.columns), index=list_thresholds)

    best_pct_precision = df_precision.idxmax()
    print(best_pct_precision.shape)
    df_best_pct_precision = df_best_pct_precision.append(best_pct_precision, ignore_index=True)

# confusion_matrix(df['stock_nok'], df_aux[col])





