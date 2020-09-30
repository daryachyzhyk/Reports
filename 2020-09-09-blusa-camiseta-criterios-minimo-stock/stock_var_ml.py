


import pandas as pd
import os

import numpy as np

from collections import Counter

from sklearn.model_selection import train_test_split
from sklearn import metrics
from sklearn.tree import (DecisionTreeClassifier, tree, export_graphviz, export_text)
from sklearn.externals.six import StringIO

import pydotplus

# which pct is more userfull
path = ('/home/darya/Documents/Reports/2020-09-09-blusa-camiseta-criterios-minimo-stock')
path_save = ('/home/darya/Documents/Reports/2020-09-09-blusa-camiseta-criterios-minimo-stock/pct')

df_raw = pd.read_csv(os.path.join(path, 'date_family_size_var_pct_col_psfeedback.csv'))

# df_raw = pd.read_csv(os.path.join(path, 'date_family_size_var_pct_psfeedback.csv'))


if 'varoption' in df_raw.columns:
    df_raw = df_raw.drop(columns=['varoption'])

df_raw = df_raw[~df_raw['stock_nok'].isnull()]

df_raw = df_raw.fillna(0)
y = df_raw['stock_nok']

X = df_raw.drop(columns=['date', 'family_desc', 'size', 'stock_nok'])


x_train, x_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=1)

print('Training and test samples per class {}'.format(Counter(y)))
# print('Validation dataset samples per class {}'.format(Counter(y_val['fraud'])))


# Parameters

max_depth_list = [4, 5, 6, 7]

#######################################################################################################################
# Models
for max_depth in max_depth_list:

    clf = DecisionTreeClassifier(max_depth=max_depth)  # , criterion="entropy"
    clf = clf.fit(x_train, y_train)

    y_pred = clf.predict(x_test)
    # Validation
    # y_pred_val = clf.predict(X_val)

    df_metrics_test_validation = pd.DataFrame({'F1': [metrics.f1_score(y_test, y_pred)],
                                               'Cohen Kappa': [metrics.cohen_kappa_score(y_test, y_pred)],
                                               'Precision': [metrics.precision_score(y_test, y_pred)],
                                               'Recall': [metrics.recall_score(y_test, y_pred)],
                                               'ROC AUC': [metrics.roc_auc_score(y_test, y_pred)],
                                               'Accuracy': [metrics.accuracy_score(y_test, y_pred)],
                                               'Confusion matrix': [metrics.confusion_matrix(y_test, y_pred)]},
                                              index=['test'])

    metrics_file_name = 'stock_nok_DecisionTree_graph_maxdepth_' + str(max_depth) + '_metrix_test.csv'
    df_metrics_test_validation.to_csv(os.path.join(path, metrics_file_name))

    #######################################################################################################################
    # plot the tree

    feature_cols = x_train.columns

    dot_data = StringIO()
    export_graphviz(clf,
                    out_file=dot_data,
                    filled=True,
                    rounded=True,
                    special_characters=False,
                    node_ids=True,
                    feature_names=feature_cols,
                    class_names=['ok', 'nok'],
                    leaves_parallel=False,
                    proportion=False)
    graph = pydotplus.graph_from_dot_data(dot_data.getvalue())

    name_graph = 'stock_nok_DecisionTree_graph_maxdepth_' + str(max_depth)
    name_graph_png = name_graph + '.png'
    name_graph_pdf = name_graph + '.pdf'
    name_graph_jpg = name_graph + '.jpg'

    graph.write_png(os.path.join(path_save, name_graph_png))
    graph.write_jpg(os.path.join(path_save, name_graph_jpg))
    graph.write_pdf(os.path.join(path_save, name_graph_pdf))

    #######################################################################################################################
    # Extract graph as text

    r = export_text(clf, feature_names=x_train.columns.to_list(), show_weights=True)

    # print(r)

    #######################################################################################################################
    # Feature importance

    feat_importance = clf.tree_.compute_feature_importances().round(2)

    dict_importance = dict(zip(x_test.columns.to_list(), feat_importance))

    df_important = pd.DataFrame.from_dict(dict_importance, orient='index', columns=['Importance'])
    df_important = df_important.sort_values(by='Importance', ascending=False)

    importance_file_name = 'stock_nok_DecisionTree_graph_maxdepth_' + str(
        max_depth) + '_feature_importance' + '.csv'
    df_important.to_csv(os.path.join(path_save, importance_file_name))

    plot_importance = df_important.plot(kind='bar', figsize=(10, 5), fontsize=14)

    importance_file_name_plot = 'stock_nok_DecisionTree_graph_maxdepth_' + str(
        max_depth) + '_feature_importance' + '.png'
    fig = plot_importance.get_figure()
    fig.savefig(os.path.join(path_save, importance_file_name_plot), bbox_inches='tight')

    #######################################################################################################################
    # Extract rules for class 1

    n_nodes = clf.tree_.node_count
    children_left = clf.tree_.children_left
    children_right = clf.tree_.children_right
    feature = clf.tree_.feature
    threshold = clf.tree_.threshold

    # probability
    samples = clf.tree_.n_node_samples
    class1_positives = clf.tree_.value[:, 0, 1]
    probs = (class1_positives / samples).tolist()


    def find_path(node_numb, path, x):
        path.append(node_numb)
        if node_numb == x:
            return True
        left = False
        right = False
        if (children_left[node_numb] != -1):
            left = find_path(children_left[node_numb], path, x)
        if (children_right[node_numb] != -1):
            right = find_path(children_right[node_numb], path, x)
        if left or right:
            return True
        path.remove(node_numb)
        return False


    def get_rule(path, column_names):
        mask = ''
        for index, node in enumerate(path):
            # We check if we are not in the leaf
            if index != len(path) - 1:
                # Do we go under or over the threshold ?
                if (children_left[node] == path[index + 1]):
                    mask += "'{}' <= {} \t ".format(column_names[feature[node]], round(threshold[node], 1))
                else:
                    mask += "'{}' > {} \t ".format(column_names[feature[node]], round(threshold[node], 1))
        # We insert the & at the right places
        mask = mask.replace("\t", "&", mask.count("\t") - 1)
        mask = mask.replace("\t", "")
        return mask


    # Leaves

    leave_id = clf.apply(x_test)
    paths = {}
    for leaf in np.unique(leave_id):
        path_leaf = []
        find_path(0, path_leaf, leaf)
        paths[leaf] = np.unique(np.sort(path_leaf))

    rules = {}

    for key in paths:
        rules[key] = [get_rule(paths[key], x_test.columns), round(probs[key], 2),
                      clf.tree_.value[key][0].round(0), clf.classes_[np.argmax(clf.tree_.value[key])]]

    df_rules = pd.DataFrame.from_dict(rules, orient='index', columns=['rule', 'probability', 'samples_class', 'class'])

    df_rules_class1 = df_rules[df_rules['class'] == 1]

    name_all_rules = 'stock_nok_DecisionTree_maxdepth_' + str(max_depth) + '_rules_class_all' + '.csv'
    name_c1_rules = 'stock_nok_DecisionTree_maxdepth_' + str(max_depth) + '_rules_class_nok' + '.csv'

    df_rules.to_csv(os.path.join(path_save, name_all_rules))
    df_rules_class1.to_csv(os.path.join(path_save, name_c1_rules))




