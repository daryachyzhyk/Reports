
import os
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

import datetime

from sklearn.preprocessing import MinMaxScaler
import pickle as pkl

from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import xgboost as xgb





path_load = '/home/darya/Documents/Reports/2021-02-18-basico'
file_product = ('/var/lib/lookiero/stock/stock_tool/productos_preprocessed.csv.gz')
path_save = '/home/darya/Documents/Reports/2021-02-18-basico'

df_raw = pd.DataFrame([])

for code in ['ES', 'FR', 'GB', 'IT', 'PT']:

    file_basico = os.path.join(path_load, code + '.xlsx')


    df_code = pd.read_excel(file_basico)
    df_code['country'] = code

    df_raw = df_raw.append(df_code)



df_raw[['modelo', 'color']] = df_raw['groupcolor'].str.split("_",expand=True)


df = df_raw[['modelo', 'color', 'es_basico?', 'country']].rename(columns={'es_basico?': 'basico'})

df['basico'] = df['basico'].replace(to_replace=['si', 'yes', 'no'], value=[1, 1, 0])



#######################################################################
# extract variables

# family_desc	modelo	brand	size	color	reference	tipo	corte	fit	tallaje	pattern	aventurera	basico	gomaCintura	pinza	tiro	largoCM	largoTipo	volumenCadera	composicion	tejido	tejido_adj_a	tejido_adj_b	tejido_adj_c	tejido_adj_d	tejido_adj_e	tejido_adj_f	contornoCaderaCm	contornoCinturaCm	estilo_casual	estilo_clasico	estilo_boho	estilo_minimal	estilo_street	estilo_noche	uso_tiempo_libre	uso_noche	uso_eventos	uso_working_girl	uso_administrativa	climate_warm_weather	climate_cold_weather	climate_soft_weather	climate_soft_cold_weather	climate_soft_warm_weather	alertas	bajo	capucha	contornoPechoCm	cuello	escote	escoteEspalda	forro	grosor	hombreras	ligero	manga	mangaLargo	mangaLargoCm	mensaje	model_fitting	model_size	solapa	tamano	volumenPecho	precio_catalogo	precio_catalogo_gb	premium	origen	category	category_5	categorySF	categoria	parte_arriba_abajo	is_repo	color_desc	color_group	color_category	price_range_product	imagen_prenda	size_num	size_adj	size_recalc	informado_por	clima	clima_grupo	family	has_pattern	pattern_stripes_dot_mini	pattern_plaid	pattern_animal	pattern_other	basico_pattern	uso	estilo_producto	cierre_botones	cierre_cremallera	cierre_cremallerainvisible	cierre_corchetes	cierre_lazo	cierre_nudo	cierre_velcro	cierre_acordonado	cierre_goma	cierre_automatico	cierre_cinturon	cierre_bragueta	cierre_sincierre	cierre	acabado_roto	acabado_tornasolado	acabado_agradable	acabado_tablas	acabado_canale	acabado_charol	acabado_arrugado	acabado_impermeable	acabado_desgastado	acabado_encerado	acabado_brillante	acabado_transparente	acabado_acolchado	acabado_drapeado	acabado_plisado	acabado_parches	acabado_jaspeado	acabado_metalizado	acabado_calado	acabado	detalle_bordado	detalle_pedreria	detalle_volantes	detalle_pailletes	detalle_encaje	detalle_lazos	detalle_parches	detalle_flecos	detalle_abalorios	detalle_bies	detalle_cinturon	detalle_bolsillo	detalle_crochet	detalle_tachuelas	detalle_pasamaneria	detalle_borlas	detalle_greca	detalle_troquelado	detalle_nudo	detalle_otro	detalle	tipo_bag	tipo_bandana	tipo_bandolera	tipo_bucket	tipo_bufanda	tipo_capazo	tipo_clutch	tipo_cruzado	tipo_cuello	tipo_de_hombro	tipo_de_mano	tipo_de_viaje	tipo_elastico	tipo_estola	tipo_fular	tipo_hebilla	tipo_mochila	tipo_otro	tipo_playero	tipo_rinonera	tipo_saco	tipo_satchel	tipo_shopper	tipo_sobre	tipo_tote	tipo_trenzado	tipo2

modelo_list = df['modelo'].tolist()
var_list_aux = ['modelo', 'color']

var_list_cat = ['family_desc',
                'tipo',
                'fit',
                'pattern',
                'has_pattern',
                'acabado',
                'aventurera', #
                'estilo_producto',
                'uso',
                # 'composicion', #
                # 'tejido', #
                'origen',
                'color_group',
                'color_category',
                'price_range_product',

                # 'premium', #
                # 'corte', #
                # 'grosor', #
                # 'ligero', #
                # 'clima', #
                'cuello',
                'escote',
                'is_repo',
                # 'family',
                'pattern_stripes_dot_mini',
                'pattern_plaid',
                'pattern_animal',
                'pattern_other',
                'basico_pattern',
                'uso',
                'estilo_producto',
                'detalle',
                # 'is_first_box' from demanda

                ]

# var_list_opt = []

# var_list = var_list_aux + var_list_cat + var_list_opt
var_list = var_list_aux + var_list_cat

query_product_text = 'modelo in @modelo_list'

df_product_raw = pd.read_csv(file_product, usecols=var_list).query(query_product_text)

# df_product_raw = df_product_raw.drop_duplicates('reference', keep='last')

df_product_basico = pd.merge(df, df_product_raw, on=['modelo', 'color'], how='left')

df_product_basico = df_product_basico.drop_duplicates(['modelo', 'color'], keep='last')

# plots
# fig, ax = plt.subplots(1, 1, figsize=(12, 14), sharex=True)


# fig, ax = plt.subplots(figsize=(8, 5))
# sns.countplot(x='pattern', hue='basico', data=df_product_basico)
# ax.set_xticklabels(ax.get_xticklabels(), fontsize=14, rotation=90)
# fig.tight_layout()
var_list_cat = var_list_cat + ['country']
for var in var_list_cat:
    fig, ax = plt.subplots(figsize=(8, 5))
    sns.countplot(x=var, hue='basico', data=df_product_basico)
    ax.set_xticklabels(ax.get_xticklabels(), fontsize=14, rotation=90)
    ax.set_title(str(var))
    fig.tight_layout()


    plt.legend()
    plt.savefig(os.path.join(path_save, 'Countplot_' + str(var) + '.png'))



# ['modelo', 'color', 'basico', 'country', 'family_desc', 'tipo', 'fit',
#        'pattern', 'aventurera', 'cuello', 'escote', 'origen', 'is_repo',
#        'color_group', 'color_category', 'price_range_product', 'family',
#        'has_pattern', 'pattern_stripes_dot_mini', 'pattern_plaid',
#        'pattern_animal', 'pattern_other', 'basico_pattern', 'uso',
#        'estilo_producto', 'acabado', 'detalle'],

####################################################################################################################
# preprocessing

# dummies

aa = df_product_basico[var_list_cat].str.replace("-", "_")
df_dummy = pd.get_dummies(df_product_basico[var_list_cat])

df_id_label_dummy = pd.concat([df_product_basico[['modelo', 'color', 'basico']], df_dummy], axis=1)


###################################################################################################
# XGBoost


X_data = df_dummy
y_data = df_product_basico['basico']

X = X_data.to_numpy()
y = y_data.to_numpy()

# X = df_dummy
# y = df_product_basico['basico']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
print(X_train.shape)
print(y_train.shape)

################################################################################################
# model

def build_and_fit_xgb_model(X_train, y_train, X_test, y_test, n_depth, subsample, n_estimators, scale_pos_weight):
    xgb_model = xgb.XGBClassifier(max_depth=n_depth,
                                  objective='binary:logistic',  # error evaluation for multiclass training
                                  # num_class=2,
                                  subsample=subsample,
                                  # randomly selected fraction of training samples that will be used to train each tree.
                                  n_estimators=n_estimators,
                                  scale_pos_weight=scale_pos_weight
                                  )
    eval_set = [(X_train, y_train), (X_test, y_test)]
    history = xgb_model.fit(X_train, y_train, eval_metric="auc", eval_set=eval_set, verbose=True)
    return xgb_model, history



trained_xgb_model, xgb_history = build_and_fit_xgb_model(X_train, y_train, X_test, y_test, n_depth=5, subsample=0.8, n_estimators=300, scale_pos_weight=3)


# make predictions for test data
y_pred = trained_xgb_model.predict(X_test)
# determine the total accuracy
accuracy = accuracy_score(y_test, y_pred)
print("Accuracy: %.2f%%" % (accuracy * 100.0))

# create_confusion_matrix(y_pred, y_test)






# feature importance


# fig, ax = plt.subplots()
trained_xgb_model.get_booster().feature_names = df_dummy.columns.tolist()
xgb.plot_importance(trained_xgb_model.get_booster(), max_num_features=30)


# sorted_idx = np.argsort(trained_xgb_model.feature_importances_)[::-1]


df_feature_importance = pd.DataFrame([])
df_feature_importance['feature'] = df_dummy.columns #trained_xgb_model.get_booster().feature_names
df_feature_importance['importance'] = trained_xgb_model.feature_importances_



feature_importance = trained_xgb_model.feature_importances_

print(map(float, feature_importance))


# plot
plt.bar(range(len(feature_importance)), feature_importance)
plt.show()

# xgb.plot_importance(feature_importance)

sorted_idx = np.argsort(trained_xgb_model.feature_importances_)[::-1]
fig, ax = plt.subplots()
for index in sorted_idx:
    print([df_dummy.columns[index], trained_xgb_model.feature_importances_[index]])

xgb.plot_importance(trained_xgb_model, max_num_features = 15)



# pyplot.show()

# plt.savefig(os.path.join(path_save_test, 'plot_test_feature_importance'))

sorted_idx = trained_xgb_model.feature_importances_.argsort()
fig, ax = plt.subplots()
plt.barh(df_dummy.columns[sorted_idx], trained_xgb_model.feature_importances_[sorted_idx])
plt.xlabel("Xgboost Feature Importance")


fig, ax = plt.subplots()
feature_important = trained_xgb_model.get_booster().get_score(importance_type='weight')
keys = list(feature_important.keys())
values = list(feature_important.values())

df_plot = pd.DataFrame(data=values, index=keys, columns=["score"]).sort_values(by = "score", ascending=False)
df_plot.plot(kind='barh')