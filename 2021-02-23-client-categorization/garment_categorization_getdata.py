
import os
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from math import isnan

import io
import requests
##################################################################################################################
# Demanda

file_modelo = '/home/darya/Documents/Reports/2021-02-23-client-categorization/Variedad 2021 - Por refe.csv'
file_product = '/var/lib/lookiero/stock/stock_tool/productos_preprocessed.csv.gz'
path_save = '/home/darya/Documents/Reports/2021-02-23-client-categorization'

file_feature = '/home/darya/Documents/projects/styling-basics-data/feature.csv'
file_feature_value = '/home/darya/Documents/projects/styling-basics-data/feature_value.csv'
file_feature_value_family = '/home/darya/Documents/projects/styling-basics-data/feature_value_family.csv'
file_family = '/home/darya/Documents/projects/styling-basics-data/family.csv'


# TODO: link features and family

df_feature = pd.read_csv(file_feature).rename(columns={'id': 'feature_id', 'name': 'name_feature'})

df_feature_value = pd.read_csv(file_feature_value).rename(columns={'id': 'feature_value_id'})
df_feature_value_family = pd.read_csv(file_feature_value_family)
df_family = pd.read_csv(file_family).rename(columns={'id': 'family_id', 'name': 'family_desc'})

df_family_feature_value_id = pd.merge(df_family, df_feature_value_family, on='family_id')

df_family_feature_value_id_feature_id = pd.merge(df_family_feature_value_id, df_feature_value, on='feature_value_id')

df_family_feature_name = pd.merge(df_family_feature_value_id_feature_id, df_feature, on='feature_id')

df_family_feature = df_family_feature_name.groupby(['family_desc', 'name_feature']).agg({'value': lambda x: list(x)}).reset_index()

df_family_feature.to_csv(os.path.join(path_save, 'familia_caracteristica_valor.xlsx'))


#####################################################################################
df_productos_raw = pd.read_csv(file_product)
df_productos = df_productos_raw.fillna('sin_clase')
list_col_to_lowcase = list(set(df_productos.columns) - set(['modelo', 'family_desc', 'imagen_prenda']))
df_productos[list_col_to_lowcase] = df_productos[list_col_to_lowcase].applymap(lambda s: s.lower() if type(s) == str else s) # & (s.name in ['modelo', 'family_desc'])

df_modelos_raw = pd.read_csv(file_modelo)
df_modelos_raw = df_modelos_raw.rename(columns={'Unnamed: 0': 'categoria'})


df_modelos_raw = df_modelos_raw.iloc[0:6, :]

model_categories = df_modelos_raw.iloc[:, 0]
# column_list = ['brand', 'color', 'aventurera', ]

df_all_category = pd.DataFrame([])

for modelo_cat in model_categories[1:2]:
    # 0         Blusa lisa escote pico
    # 1                 Camisa vaquera
    # 2             Camisa lisa vestir
    # 3               Camisa estampada
    # 4             Blusa/camisa rayas
    # 5    Blusa miniprint escote pico

    # modelo_cat = 'Blusa miniprint escote pico'
    print(modelo_cat)
    list_modelos = df_modelos_raw[df_modelos_raw['categoria'] == modelo_cat].iloc[:, 1:].values.tolist()[0]
    print(list_modelos)
    # if modelo_cat == 'Camisa vaquera':
    #     list_modelos.append('K381')

    df_modelo_producto = df_productos[(df_productos['modelo'].isin(list_modelos))]
    df_modelo_producto = df_modelo_producto.drop_duplicates(subset=['modelo'])

    simila_caracteristic = {}
    not_simila_caracteristic = {}
    column_analyze = df_modelo_producto.columns.tolist()
    column_analyze.remove('size')


    for col in column_analyze:
        freq = df_modelo_producto[col].value_counts(normalize=True, ascending=False, dropna=False)
        # print(freq)
        if (freq.max() >= 0.98):
            # simila_caracteristic.append(freq.idxmax())
            simila_caracteristic[col] = [freq.idxmax()]
        else:
            not_simila_caracteristic[col] = [freq.idxmax()]

    if modelo_cat == 'Blusa lisa escote pico':
        # TODO: check color denim
        charact_apriori = {'cuello': ['sin_clase', 'mao', 'solapa', 'mandarinacollar']}
        for col in charact_apriori.keys():
            print(col)
            simila_caracteristic[col] = charact_apriori[col]
            not_simila_caracteristic.pop(col, None)

    elif modelo_cat == 'Camisa vaquera':
        # TODO: check color denim
        charact_apriori = {'tejido': ['sarga', 'vaquero']}
        for col in charact_apriori.keys():
            print(col)
            simila_caracteristic[col] = charact_apriori[col]
            not_simila_caracteristic.pop(col, None)

        # simila_caracteristic2 = {k: v for k, v in simila_caracteristic.items() if pd.Series(v).notna().all()}
        # {k: [elem for elem in v if elem is not np.nan] for k, v in simila_caracteristic.items()}
        # simila_caracteristic2 = {k: v for k, v in simila_caracteristic.items() if not isnan(v)}

    simila_caracteristic_list = list(simila_caracteristic)
    not_simila_caracteristic_list = list(not_simila_caracteristic)
    not_simila_caracteristic_list.remove('modelo')


    df_modelo_similar = df_productos[np.logical_and.reduce([df_productos[k].isin(v) for k, v in simila_caracteristic.items()])]

    df_modelo_similar = df_modelo_similar.drop_duplicates(subset='modelo')
    if modelo_cat == 'Camisa vaquera':

        list_modelo_drop = ['S1127', 'S1720', 'S2189', 'S2791']
        df_modelo_similar = df_modelo_similar[~df_modelo_similar['modelo'].isin(list_modelo_drop)]
    # Drop manually wrong informed by FL
    df_modelo_similar['variety_category'] = str(modelo_cat)
    df_all_category = df_all_category.append(df_modelo_similar)
    df_modelo_similar_save = df_modelo_similar[['modelo', 'imagen_prenda'] + list(simila_caracteristic) + not_simila_caracteristic_list]


    # df_modelo_similar_save.to_csv(os.path.join(path_save, 'Modelos_similares_' + str(modelo_cat.replace('/', '_')) + '.csv'))
    # df_modelo_similar_save.to_csv(os.path.join(path_save, 'Modelos_similares_' + str(modelo_cat.replace('/', '_')) + '.xlsx'))


##############################################################################################################
# blusa vaquera

df_test_vaq = df_productos[df_productos['modelo'].isin(['S1132', 'S1531', 'S1543', 'S2108', 'S2205', 'S2252',
                                                        'S2265', 'S2355', 'K381'])].drop_duplicates(subset='modelo')

df_test_vaq = df_test_vaq[df_modelo_similar_save.columns]
df_test_vaq.to_csv(os.path.join(path_save, 'BLUSA_vaquera_cat and not.xlsx'))


family_name = 'BLUSA'
df_vaquero = df_productos[(df_productos['family_desc'] == family_name)  & (df_productos['tejido'].isin(['sarga', 'vaquero']))].drop_duplicates(subset=['modelo'])

df_vaquero.to_csv(os.path.join(path_save, 'BLUSA_vaquera_tejido_sarga_vaquero.xlsx'))

##################################################################################################################
# df_rayas
family_name = 'BLUSA'
df_rayas = df_productos[(df_productos['family_desc'] == family_name) & (df_productos['pattern'].str.contains('|'.join(['pattern', 'rayas', 'modelo', 'mini', 'print', 'raya'])))].drop_duplicates(subset=['modelo'])

df_rayas.to_csv(os.path.join(path_save, 'BLUSA_pattern_rayas.xlsx'))

######################################################################################################################3
# TODO encontrar los modelos que estan en diferentes categorias




df_modelo_dif_categ = df_all_category[df_all_category.duplicated(keep=False)]


###################################################################################################################

# TODO: encontrar modelos que no estan en ninguna categoria

family_name = 'BLUSA'
list_modelo_category = df_all_category['modelo'].tolist()

df_productos_not_category = df_productos[(df_productos['family_desc'] == family_name) & ~(df_productos['modelo'].isin(list_modelo_category))].drop_duplicates(subset=['modelo'])

df_productos_not_category.to_csv(os.path.join(path_save, family_name + '_modelos_no_categorizadas.xlsx'))



####################################################################################################################
# Todas caracteristicas
columnas_prod = df_productos.columns - ['modelo', 'imagen_prenda']

dic_prod_uniq = {}
for col in df_productos.columns:
    list_prod = df_productos[col].unique().astype(str).tolist()
    if 'sin_clase' in list_prod:
        list_prod.remove('sin_clase')
    dic_prod_uniq[col] = list_prod[0:2]

columnas_prod = pd.DataFrame.from_dict(dic_prod_uniq, orient='index')
columnas_prod.to_csv(os.path.join(path_save, 'Todas_caracteristicas_productos.xlsx'))

####################################################################################################################

col_pattern = df_modelo_producto.columns[df_modelo_producto.columns.str.contains('|'.join(['pattern', 'rayas', 'modelo', 'mini', 'print', 'raya']))]
aa = df_modelo_producto[col_pattern]

df_test = df_productos[df_productos['modelo'].isin(['S916', 'S1712', 'S2657'])].drop_duplicates(subset='modelo')
aa = df_test[col_pattern]

aa = df_modelo_producto[col_pattern]
# drop columns same value
df_test_diff = df_test[df_test.columns[df_test.nunique() > 1]]

S916

# col_pattern = [col for col in df_modelo_producto.columns if ['pattern', 'stripe', 'modelo'] in col]
# has_pattern


df_modelo_producto.to_csv(os.path.join(path_save, 'Modelos_train_' + str(modelo_cat) + '.csv'))
df_modelo_producto.to_csv(os.path.join(path_save, 'Modelos_train_' + str(modelo_cat) + '.xlsx'))
S1127

df2 = df.filter(regex='spike')

df_test = df_productos_raw[df_productos['modelo'].isin(['S2725', 'S1159'])].drop_duplicates(subset='modelo')



df_modelo_similar = df_productos.loc[np.all(df_productos[list(simila_caracteristic)] == pd.Series(simila_caracteristic), axis=1)]


aa = [df_productos[k].isin(v) for k, v in simila_caracteristic.items()]

df_modelo_similar2 = df_productos.loc[np.all(df_productos[list(simila_caracteristic2)] == pd.Series(simila_caracteristic2), axis=1)][['modelo'] + list(simila_caracteristic2)]


df_modelo_similar2 = df_modelo_similar2.drop_duplicates(subset='modelo')


df_modelo_similar2_image = pd.merge(df_modelo_similar2,
                                    df_productos[['modelo', 'imagen_prenda']],
                                    on='modelo',
                                    how='left')
df_modelo_similar2_image = df_modelo_similar2_image.drop_duplicates(subset='modelo')

df_modelo_similar2_image.to_csv(os.path.join(path_save, 'Modelos_similares_' + str(modelo_cat) + '.csv'))





similar_modelo = df_modelo_similar['modelo'].unique().tolist()
similar_modelo2 = pd.DataFrame(df_modelo_similar2['modelo'].unique())



df_test = df_productos_raw[df_productos_raw['modelo'].isin(['S1548', 'S1969'])].drop_duplicates(subset='modelo')

# df_test_dr = df_test.T.drop_duplicates().T
df_test_dr = df_test[[i for i in df_test if len(set(df_test[i]))>1]]


df = df.loc[:,~df.columns.duplicated()]

df_test = df_productos_raw[df_productos_raw['modelo'].isin(['S1548', 'C1074', 'S1576'])][['modelo'] + list(simila_caracteristic2)].drop_duplicates(subset='modelo')

df_test = df_productos_raw[df_productos_raw['modelo'].isin(['S2277', 'S1576', 'S2768'])][['modelo'] + list(simila_caracteristic)].drop_duplicates(subset='modelo')

# query = ' and '.join([f'{k} == {repr(v)}' for k, v in simila_caracteristic.items()])
# # query = ' and '.join(['{} == {}'.format(k, repr(v)) for k, v in m.items()])
# df_modelo_similar = df_productos_raw.query(query)


S1969

# df_modelo_similar = df_productos.loc[df_productos[simila_caracteristic.keys()].isin(simila_caracteristic.values()).all(axis=1), :]
# df_modelo_similar = df_productos.loc[df_productos[list(simila_caracteristic.keys())].isin(list(simila_caracteristic.values())).all(axis=1), :]

# df_modelo_similar = df_productos.loc[(df_productos[list(simila_caracteristic)] == pd.Series(simila_caracteristic)).any(axis=1)]


# df_modelo_similar = df_productos.loc[np.all(df_productos[list(simila_caracteristic)] == pd.Series(simila_caracteristic), axis=1)]

#
# aa = df_modelo_producto.filter(regex='pattern')


url_feauture = 'https://github.com/lookiero/styling-basics-data/blob/master/feature.csv'
# url_feauture_content = requests.get(url_feauture).content
df_feature = pd.read_csv(url_feauture) # , error_bad_lines=False

# # TODO: test productos
#
# file_product_test = '/var/lib/lookiero/stock/stock_tool/productos.csv.gz'
# df_productos_raw_test = pd.read_csv(file_product_test)
#
# df_test_vaq2 = df_productos_raw_test[df_productos_raw_test['modelo'].isin(['S1132', 'S1531', 'S1543', 'S2108', 'S2205', 'S2252',
#                                                         'S2265', 'S2355', 'K381'])].drop_duplicates(subset='modelo')
#
# df_test_vaq2 = df_test_vaq2[df_modelo_similar_save.columns]