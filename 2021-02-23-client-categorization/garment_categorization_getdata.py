
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

# file_modelo = '/home/darya/Documents/Reports/2021-02-23-client-categorization/data_modelo_feature_value.csv'


file_product = '/var/lib/lookiero/stock/stock_tool/productos_preprocessed.csv.gz'
path_save = '/home/darya/Documents/Reports/2021-02-23-client-categorization'

###### features
file_feature = '/home/darya/Documents/projects/styling-basics-data/feature.csv'
file_feature_value = '/home/darya/Documents/projects/styling-basics-data/feature_value.csv'
file_feature_value_family = '/home/darya/Documents/projects/styling-basics-data/feature_value_family.csv'
file_family = '/home/darya/Documents/projects/styling-basics-data/family.csv'

###################################################################################################################
# link features and family

df_feature = pd.read_csv(file_feature).rename(columns={'id': 'feature_id', 'name': 'name_feature'})

df_feature_value = pd.read_csv(file_feature_value).rename(columns={'id': 'feature_value_id'})
df_feature_value_family = pd.read_csv(file_feature_value_family)
df_family = pd.read_csv(file_family).rename(columns={'id': 'family_id', 'name': 'family_desc'})

df_family_feature_value_id = pd.merge(df_family, df_feature_value_family, on='family_id')

df_family_feature_value_id_feature_id = pd.merge(df_family_feature_value_id, df_feature_value, on='feature_value_id')

df_family_feature_name = pd.merge(df_family_feature_value_id_feature_id, df_feature, on='feature_id')

df_family_feature = df_family_feature_name.groupby(['family_desc', 'name_feature']).agg({'value': lambda x: list(x)}).reset_index()

df_family_feature.to_csv(os.path.join(path_save, 'familia_caracteristica_valor.xlsx'))

#############
# features sin pasar a dummies


# file_catalog = '/home/darya/Documents/Reports/2021-02-23-client-categorization/All_groups_features_Catalog_100321.csv'
file_catalog = '/home/darya/Documents/Reports/2021-02-23-client-categorization/data_modelo_feature_value.csv'
file_demanda = ('/var/lib/lookiero/stock/stock_tool/demanda_preprocessed.csv.gz')


df_catalog_raw = pd.read_csv(file_catalog, usecols=['grupo', 'brand_name', 'brand_origin', 'family_name',
                                                             'family_category', 'feature_name', 'feature_multiple',
                                                             'feature_value'
                                                             # , 'description', 'family_letter', 'family_number',
                                                             # 'feature_type', 'feature_unit',
                                                             ])
# , sep=';'

df_catalog_raw.loc[df_catalog_raw['feature_name'] == 'skirt_type', 'feature_multiple'] = 't'


list_family = df_catalog_raw['family_name'].unique().tolist()

for family in list_family: # ['BLUSA_CAMISA']: list_family ['blusa_camisa']
    df_catalog_fam = df_catalog_raw[df_catalog_raw['family_name'] == family]

    # df_catalog_fam_gr = df_catalog_fam.groupby


    df_catalog_uni = df_catalog_fam[df_catalog_fam['feature_multiple'] == False]
    df_catalog_mul = df_catalog_fam[df_catalog_fam['feature_multiple'] == True]

    # df_catalog_mul['feature_name_value'] = df_catalog_mul['feature_name'] + '_' + df_catalog_mul['feature_value']


    # df_feature_uni = df_catalog_uni.pivot(index='grupo', columns="feature_name", values="feature_value").reset_index()
    # df_feature_mul = df_catalog_mul.pivot(columns="feature_name_value", values="grupo").reset_index()

    pivot_table = df_catalog_fam.pivot_table(index='grupo', columns="feature_name", values="feature_value",
                                             aggfunc = lambda x: ', '.join(str(v) for v in x)).reset_index().rename(columns={'grupo': 'modelo'})

    list_modelo_fam = list(set(pivot_table['modelo']))
    query_text = 'modelo in @list_modelo_fam'
    df_images = pd.read_csv(file_product, usecols=['modelo', 'imagen_prenda', 'is_repo', 'color_desc', 'color_group', 'tipo']).query(query_text)
    df_images = df_images.drop_duplicates(subset='modelo')

    # df_product_servidor = pd.read_csv(file_product, usecols=['modelo', 'imagen_prenda'])

    df_productos_feature = pd.merge(pivot_table, df_images, on=['modelo'], how='left')

    df_productos_feature.to_excel(os.path.join(path_save, 'Features_value_' + family + '.xlsx'))


    # porcentaje de valores de cada feature

    list_features = df_productos_feature.columns.to_list()
    list_features.remove('modelo')
    list_features.remove('imagen_prenda')
    # TODO: añadir color
    # añadir KR
    modelo_list = df_productos_feature['modelo'].to_list()
    query_demanda_text = 'modelo in @modelo_list'

    df_demanda_raw = pd.read_csv(file_demanda, usecols=['modelo', 'purchased']).query(query_demanda_text)

    df_demanda = df_demanda_raw.groupby(['modelo'], dropna=False).agg({'purchased': ['sum', 'count']})
    df_demanda.columns = df_demanda.columns.droplevel()
    df_demanda = df_demanda.reset_index()
    df_demanda = df_demanda.rename(columns={'sum': 'purchased', 'count': 'envios'})


    df_modelo_feature_demanda = pd.merge(df_productos_feature, df_demanda, on='modelo', how='left')

    df_feature_value_count = pd.DataFrame([])
    for feature in list_features:

        df_feature_count = df_productos_feature[feature].value_counts(normalize=True, dropna=False).rename_axis('feature_value').reset_index(name='counts_pct')

        df_feature_demanda = df_modelo_feature_demanda.groupby([feature], dropna=False).agg({'purchased': 'sum',
                                                                               'envios': 'sum'}).reset_index().rename(columns={feature: 'feature_value'})

        df_feature_count['feature_name'] = feature

        df_feature_count_demanda = pd.merge(df_feature_count, df_feature_demanda, on=['feature_value'], how='left')
        df_feature_count_demanda['KR'] = df_feature_count_demanda['purchased'] / df_feature_count_demanda['envios']
        df_feature_value_count = df_feature_value_count.append(df_feature_count_demanda)

    df_feature_value_count = df_feature_value_count[['feature_name', 'feature_value', 'counts_pct', 'KR']]

    df_feature_value_count['counts_pct'] = df_feature_value_count['counts_pct'] *100
    aa = df_feature_value_count[(df_feature_value_count['counts_pct'] > 0.2) & (df_feature_value_count['KR'] < 0.3)]


    df_feature_value_count = df_feature_value_count.fillna('sin_clase')
    df_feature_value_count.to_excel(os.path.join(path_save, 'Features_value_count_pct_KR_' + family + '.xlsx'))


    df_repo = df_modelo_feature_demanda[df_modelo_feature_demanda['is_repo'] == True]

#############################
# product de catalogo

# file_catalog = '/home/darya/Documents/Reports/2021-02-23-client-categorization/All_groups_features_Catalog_100321.csv'
file_catalog = '/home/darya/Documents/Reports/2021-02-23-client-categorization/data_modelo_feature_value.csv'

df_catalog_raw = pd.read_csv(file_catalog, usecols=['grupo', 'brand_name', 'brand_origin', 'family_name',
                                                             'family_category', 'feature_name', 'feature_multiple',
                                                             'feature_value'
                                                             # , 'description', 'family_letter', 'family_number',
                                                             # 'feature_type', 'feature_unit',
                                                             ])
# , sep=';'

df_catalog_raw.loc[df_catalog_raw['feature_name'] == 'skirt_type', 'feature_multiple'] = 't'


list_family = df_catalog_raw['family_name'].unique().tolist()

for family in list_family: # ['BLUSA_CAMISA']:
    df_catalog_fam = df_catalog_raw[df_catalog_raw['family_name'] == family]

    df_catalog_uni = df_catalog_fam[df_catalog_fam['feature_multiple'] == False]
    df_catalog_mul = df_catalog_fam[df_catalog_fam['feature_multiple'] == True]

    df_catalog_mul['feature_name_value'] = df_catalog_mul['feature_name'] + '_' + df_catalog_mul['feature_value']


    df_feature_uni = df_catalog_uni.pivot(index='grupo', columns="feature_name", values="feature_value").reset_index()
    df_feature_mul = df_catalog_mul.pivot(columns="feature_name_value", values="grupo").reset_index()

    # df_feature_mul = df_catalog_mul.set_index('grupo').to_frame('feature_name_value');
    # pd.get_dummies(x, prefix='g', columns=['genre']).groupby(level=0).sum()


    df_feature_mul = pd.get_dummies(df_catalog_mul.set_index('grupo'), columns=['feature_name_value'], prefix='', prefix_sep='').groupby(level=0).sum().reset_index()
    # df_feature_mul['grupo'] = df_catalog_mul['grupo']

    df_grupo_feature = pd.merge(df_feature_uni, df_feature_mul, on='grupo', how='outer')

    df_grupo_feature = pd.merge(df_grupo_feature,
                                df_catalog_fam[['grupo', 'brand_name', 'brand_origin', 'family_name']].drop_duplicates(),
                                on='grupo', how='left')

    df_grupo_feature.to_csv(os.path.join(path_save, 'Features_' + family + '.csv'))



#
# # aa = df_catalog_raw[df_catalog_raw['grupo'] == 'S400']
# aa = df_catalog_raw[df_catalog_raw['grupo'].isin(['S400', 'S1307'])]
# aa_mul = aa[aa['feature_multiple'] == 't']
#
# aa_un = aa[aa['feature_multiple'] == 'f']
#
# df_un = aa_un.pivot(index='grupo', columns="feature_name", values="feature_value")
# bb = aa.pivot(index='grupo', columns="feature_name", values="feature_value")
#
# dd = aa.groupby(['grupo', 'feature_name'])['feature_value'].apply(lambda x: list(x)).reset_index()
#
# # dd = aa.groupby(['grupo','feature_name'])['feature_value'].sum().unstack(-1)# you can using `mean` instead of `sum`


#####################################################################################


import os
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from math import isnan

import io


file_product = '/var/lib/lookiero/stock/stock_tool/productos_preprocessed.csv.gz'
path_save = '/home/darya/Documents/Reports/2021-02-23-client-categorization'
file_modelo = '/home/darya/Documents/Reports/2021-02-23-client-categorization/Variedad 2021 - Por refe.csv'

list_family = ['BLUSA_CAMISA', 'VESTIDO', 'TOP', 'CHAQUETA', 'PANTALON', 'CAMISETA', 'FALDA', 'CARDIGAN', 'SUDADERA',
               'DENIM_JEANS', 'BUFANDA', 'JERSEY', 'ABRIGO', 'SHORT', 'PARKA', 'TRENCH', 'BOLSO', 'JUMPSUIT',
               'FULAR', 'BOTINES', 'SNEAKERS', 'BOTAS']
list_family = ['denim_jeans', 'blusa_camisa', 'vestido', 'top', 'chaqueta', 'pantalon', 'camiseta', 'falda', 'cardigan',
               'sudadera', 'sandalias', 'bufanda', 'jersey', 'sneakers', 'abrigo', 'bolso', 'short', 'parka', 'trench',
               'jumpsuit', 'fular', 'botines', 'zapatos', 'botas']

features_to_include = ['application', 'basic', 'composition', 'cut', 'detail', 'fabric', 'fabric_adj', 'finishing',
                      'fit', 'message', 'neck', 'neckline', 'print', 'sizing', 'sleeve', 'sleeve_long', 'style',
                      'top_type', 'weather']


family = 'blusa_camisa'

family_product_file = os.path.join(path_save, 'Features_' + family + '.csv')

df_productos_family = pd.read_csv(family_product_file, index_col=0).rename(columns={'grupo': 'modelo'})

list_modelo_fam = df_productos_family['modelo'].unique().tolist()

# Include just features of interes
# features_to_analyze = [s for s in df_productos_family.columns for f in features_to_include if f in s]

features_to_analyze = list(set([s for s in df_productos_family.columns for f in features_to_include if s.startswith(f)]))

df_productos_family = df_productos_family[['modelo'] + features_to_analyze]
# total 294 features, after filtering 241

aa = pd.DataFrame(np.count_nonzero(df_productos_family, axis=0), columns=df_productos_family.columns)

df_feature_count = pd.DataFrame(df_productos_family.fillna(0).astype(bool).sum(axis=0), columns=['count']).reset_index().rename(columns={'index': 'feature_name'})

df_feature_count['pct_count'] = df_feature_count['count'] * 100 / df_productos_family.shape[0]


# load images
#
query_text = 'modelo in @list_modelo_fam'
df_images = pd.read_csv(file_product, usecols=['modelo', 'imagen_prenda']).query(query_text)
df_images = df_images.drop_duplicates(subset='modelo')

# df_product_servidor = pd.read_csv(file_product, usecols=['modelo', 'imagen_prenda'])

df_productos_family = pd.merge(df_productos_family, df_images, on=['modelo'], how='left')

df_productos_family = df_productos_family.fillna('sin_clase')

# df_productos = df_productos_raw.fillna('sin_clase')
# list_col_to_lowcase = list(set(df_productos.columns) - set(['modelo', 'family_desc', 'imagen_prenda']))
# df_productos[list_col_to_lowcase] = df_productos[list_col_to_lowcase].applymap(lambda s: s.lower() if type(s) == str else s) # & (s.name in ['modelo', 'family_desc'])

# ejemplos de modelos para cada categoria
df_modelos_raw = pd.read_csv(file_modelo)
df_modelos_raw = df_modelos_raw.rename(columns={'Unnamed: 0': 'categoria'})

df_modelos_raw = df_modelos_raw.iloc[0:6, :]
model_categories = df_modelos_raw.iloc[:, 0]

# list_test = df_modelos_raw.iloc[:, 1:].values.tolist()

# df_test = pd.DataFrame({'modelo': [item for sublist in list_test for item in sublist]})
# # df_test['modelo'] = df_test['modelo'].dropna()
# df_test['modelo_en_datos'] = df_test['modelo'].isin(list_modelo_fam)
#
# aa = df_test[df_test['modelo_en_datos'] == 0]



df_all_category = pd.DataFrame([])

for modelo_cat in model_categories:
    # 0         Blusa lisa escote pico
    # 1                 Camisa vaquera
    # 2             Camisa lisa vestir
    # 3               Camisa estampada
    # 4             Blusa/camisa rayas
    # 5    Blusa miniprint escote pico

    # modelo_cat = 'Camisa vaquera'
    print(modelo_cat)
    list_modelos = df_modelos_raw[df_modelos_raw['categoria'] == modelo_cat].iloc[:, 1:].values.tolist()[0]
    print(list_modelos)
    # if modelo_cat == 'Camisa vaquera':
    #     list_modelos.append('K381')

    df_modelo_producto = df_productos_family[(df_productos_family['modelo'].isin(list_modelos))]
    df_modelo_producto = df_modelo_producto.drop_duplicates(subset=['modelo'])


    simila_caracteristic = {}
    not_simila_caracteristic = {}
    column_analyze = df_modelo_producto.columns.tolist()
    # column_analyze.remove('size')


    for col in column_analyze:
        # print(col)
        freq = df_modelo_producto[col].value_counts(normalize=True, ascending=False, dropna=False)
        # print(freq)
        if (freq.max() >= 0.98):
            # simila_caracteristic.append(freq.idxmax())
            simila_caracteristic[col] = [freq.idxmax()]
            # simila_caracteristic[col] = freq.idxmax()
        else:
            not_simila_caracteristic[col] = [freq.idxmax()]

    # TODO: check if its still neccessary
    # if modelo_cat == 'Blusa lisa escote pico':
    #     # check color denim
    #     charact_apriori = {'cuello': ['sin_clase', 'mao', 'solapa', 'mandarinacollar']}
    #     for col in charact_apriori.keys():
    #         print(col)
    #         simila_caracteristic[col] = charact_apriori[col]
    #         not_simila_caracteristic.pop(col, None)
    #

    # if modelo_cat == 'Camisa vaquera':
    #     features_pocket = [s for s in simila_caracteristic_list if "pocket" in s]
    #     features_finishing = [s for s in simila_caracteristic_list if "finishing" in s]
    #
    #     # check color denim
    #     # charact_apriori_sim = {'tejido': ['sarga', 'vaquero']}
    #     charact_apriori_sim = {}
    #     # charact_apriori_nosim = ['detail_pocket', 'detail_pocket_flap', 'finishing_frayed']
    #     charact_apriori_nosim = ['sizing', 'alerts_breastfeeding'] + features_pocket + features_finishing
    #     for col in charact_apriori_sim.keys():
    #         print(col)
    #         simila_caracteristic[col] = charact_apriori_sim[col]
    #         not_simila_caracteristic.pop(col, None)
    #     for col in charact_apriori_nosim:
    #         simila_caracteristic.pop(col, None)
    #         not_simila_caracteristic[col] = 1

    #
    #     # simila_caracteristic2 = {k: v for k, v in simila_caracteristic.items() if pd.Series(v).notna().all()}
    #     # {k: [elem for elem in v if elem is not np.nan] for k, v in simila_caracteristic.items()}
    #     # simila_caracteristic2 = {k: v for k, v in simila_caracteristic.items() if not isnan(v)}

    simila_caracteristic_list = list(simila_caracteristic)
    not_simila_caracteristic_list = list(not_simila_caracteristic)
    not_simila_caracteristic_list.remove('modelo')
    not_simila_caracteristic_list.remove('imagen_prenda')

    # TODO nan to sin clase!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    df_modelo_similar = df_productos_family[np.logical_and.reduce([df_productos_family[k].isin(v) for k, v in simila_caracteristic.items()])]

    df_modelo_similar = df_modelo_similar.drop_duplicates(subset='modelo')

    # df_test = df_modelo_producto[np.logical_and.reduce([df_modelo_producto[k].isin(v) for k, v in simila_caracteristic.items()])]
    # df_test = df_modelo_producto.loc[(df_modelo_producto[list(simila_caracteristic)] == pd.Series(simila_caracteristic)).all(axis=0)]
    #
    # df_test = df_modelo_producto.loc[(df_modelo_producto[simila_caracteristic.keys()] == simila_caracteristic.values()).all(axis=1), :]


    # df_modelo_producto.loc[df_modelo_producto[simila_caracteristic.keys()].isin(simila_caracteristic.values()).all(axis=1), :]
    # df_test = df_modelo_producto[simila_caracteristic]

    ## Drop manually wrong informed by FL

    # if modelo_cat == 'Camisa vaquera':
    #
    #     list_modelo_drop = ['S1127', 'S1720', 'S2189', 'S2791']
    #     df_modelo_similar = df_modelo_similar[~df_modelo_similar['modelo'].isin(list_modelo_drop)]

    df_modelo_similar['variety_category'] = str(modelo_cat)
    df_all_category = df_all_category.append(df_modelo_similar)
    df_modelo_similar_save = df_modelo_similar[['modelo', 'imagen_prenda'] + list(simila_caracteristic) + not_simila_caracteristic_list]


    # df_modelo_similar_save.to_csv(os.path.join(path_save, 'Modelos_similares_' + str(modelo_cat.replace('/', '_')) + '.csv'))
    df_modelo_similar_save.to_csv(os.path.join(path_save, 'Modelos_similares_' + str(modelo_cat.replace('/', '_')) + '_selacted_features.xlsx'))

##########################
# tejido
matching_feature = [s for s in simila_caracteristic_list if "fabric" in s]

matching_feature = [s for s in simila_caracteristic_list if "composition" in s]
matching_feature = [s for s in simila_caracteristic_list if "pocket" in s]
matching_feature = [s for s in not_simila_caracteristic_list if "perce" in s]

aa = [s for s in df_productos_family.columns if "back" in s]

print(df_productos_family.filter(like='percentage').columns)
##############################################################################################################
# blusa vaquera
#
# list_model_viejo = [S1132
# S1531
# S1543
# S2108
# S2205
# S2252
# S2265
# S2355
# S2406
# S2653
# S2800
# S2859
# S2918
# S3003
# S3184
# S3302
# S400]

df_test_vaq = df_productos_family[df_productos_family['modelo'].isin(['S1132', 'S1531', 'S1543', 'S2108', 'S2205', 'S2252',
                                                        'S2265', 'S2355', 'K381', 'S1102', 'S1157'])].drop_duplicates(subset='modelo')
df_test_vaq_sim  = df_test_vaq[['modelo'] + simila_caracteristic_list]

df_test_vaq_dif = df_test_vaq_sim.loc[:, ~(df_test_vaq_sim == df_test_vaq_sim.iloc[0]).all()]

feature_pocket = [s for s in simila_caracteristic_list if "pocket" in s]
df_test_vaq_pocket = df_test_vaq[['modelo'] + feature_pocket]


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

df_productos_not_category = df_productos_family[~(df_productos_family['modelo'].isin(list_modelo_category))].drop_duplicates(subset=['modelo'])

df_productos_not_category.to_csv(os.path.join(path_save, family_name + '_modelos_no_categorizadas.xlsx'))


#############

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


#################################################################################################
############## categorizacion, version 1

# df_productos_raw = pd.read_csv(file_product)
# df_productos = df_productos_raw.fillna('sin_clase')
# list_col_to_lowcase = list(set(df_productos.columns) - set(['modelo', 'family_desc', 'imagen_prenda']))
# df_productos[list_col_to_lowcase] = df_productos[list_col_to_lowcase].applymap(lambda s: s.lower() if type(s) == str else s) # & (s.name in ['modelo', 'family_desc'])
#
# df_modelos_raw = pd.read_csv(file_modelo)
# df_modelos_raw = df_modelos_raw.rename(columns={'Unnamed: 0': 'categoria'})
#
#
# df_modelos_raw = df_modelos_raw.iloc[0:6, :]
#
# model_categories = df_modelos_raw.iloc[:, 0]
# # column_list = ['brand', 'color', 'aventurera', ]
#
# df_all_category = pd.DataFrame([])
#
# for modelo_cat in model_categories[1:2]:
#     # 0         Blusa lisa escote pico
#     # 1                 Camisa vaquera
#     # 2             Camisa lisa vestir
#     # 3               Camisa estampada
#     # 4             Blusa/camisa rayas
#     # 5    Blusa miniprint escote pico
#
#     # modelo_cat = 'Blusa miniprint escote pico'
#     print(modelo_cat)
#     list_modelos = df_modelos_raw[df_modelos_raw['categoria'] == modelo_cat].iloc[:, 1:].values.tolist()[0]
#     print(list_modelos)
#     # if modelo_cat == 'Camisa vaquera':
#     #     list_modelos.append('K381')
#
#     df_modelo_producto = df_productos[(df_productos['modelo'].isin(list_modelos))]
#     df_modelo_producto = df_modelo_producto.drop_duplicates(subset=['modelo'])
#
#     simila_caracteristic = {}
#     not_simila_caracteristic = {}
#     column_analyze = df_modelo_producto.columns.tolist()
#     column_analyze.remove('size')
#
#
#     for col in column_analyze:
#         freq = df_modelo_producto[col].value_counts(normalize=True, ascending=False, dropna=False)
#         # print(freq)
#         if (freq.max() >= 0.98):
#             # simila_caracteristic.append(freq.idxmax())
#             simila_caracteristic[col] = [freq.idxmax()]
#         else:
#             not_simila_caracteristic[col] = [freq.idxmax()]
#
#     if modelo_cat == 'Blusa lisa escote pico':
#         # TODO: check color denim
#         charact_apriori = {'cuello': ['sin_clase', 'mao', 'solapa', 'mandarinacollar']}
#         for col in charact_apriori.keys():
#             print(col)
#             simila_caracteristic[col] = charact_apriori[col]
#             not_simila_caracteristic.pop(col, None)
#
#     elif modelo_cat == 'Camisa vaquera':
#         # TODO: check color denim
#         charact_apriori = {'tejido': ['sarga', 'vaquero']}
#         for col in charact_apriori.keys():
#             print(col)
#             simila_caracteristic[col] = charact_apriori[col]
#             not_simila_caracteristic.pop(col, None)
#
#         # simila_caracteristic2 = {k: v for k, v in simila_caracteristic.items() if pd.Series(v).notna().all()}
#         # {k: [elem for elem in v if elem is not np.nan] for k, v in simila_caracteristic.items()}
#         # simila_caracteristic2 = {k: v for k, v in simila_caracteristic.items() if not isnan(v)}
#
#     simila_caracteristic_list = list(simila_caracteristic)
#     not_simila_caracteristic_list = list(not_simila_caracteristic)
#     not_simila_caracteristic_list.remove('modelo')
#
#
#     df_modelo_similar = df_productos[np.logical_and.reduce([df_productos[k].isin(v) for k, v in simila_caracteristic.items()])]
#
#     df_modelo_similar = df_modelo_similar.drop_duplicates(subset='modelo')
#     if modelo_cat == 'Camisa vaquera':
#
#         list_modelo_drop = ['S1127', 'S1720', 'S2189', 'S2791']
#         df_modelo_similar = df_modelo_similar[~df_modelo_similar['modelo'].isin(list_modelo_drop)]
#     # Drop manually wrong informed by FL
#     df_modelo_similar['variety_category'] = str(modelo_cat)
#     df_all_category = df_all_category.append(df_modelo_similar)
#     df_modelo_similar_save = df_modelo_similar[['modelo', 'imagen_prenda'] + list(simila_caracteristic) + not_simila_caracteristic_list]
#
#
#     # df_modelo_similar_save.to_csv(os.path.join(path_save, 'Modelos_similares_' + str(modelo_cat.replace('/', '_')) + '.csv'))
#     # df_modelo_similar_save.to_csv(os.path.join(path_save, 'Modelos_similares_' + str(modelo_cat.replace('/', '_')) + '.xlsx'))
#
