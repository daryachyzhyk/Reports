import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os



file_eval_compra = ('/var/lib/lookiero/stock/stock_tool/eval_estimates_real_compra.csv.gz')

file_adapt = ('/var/lib/lookiero/stock/stock_tool/kpi/compra/recomendacion-adaptada - Sheet1.csv')

path_save = '/home/darya/Documents/Reports/2021-02-11-shopping-package'


remove_size = ['36', '37', '38', '39', '40', '41']
family_include = ['ABRIGO', 'BLUSA', 'CAMISETA', 'CARDIGAN', 'CHAQUETA', 'DENIM', 'FALDA', 'JERSEY', 'JUMPSUIT',
                  'PANTALON', 'PARKA', 'SHORT', 'SUDADERA', 'TOP', 'TRENCH', 'VESTIDO']

df_eval_compra_all = pd.read_csv(file_eval_compra)
df_eval_compra_all = df_eval_compra_all[~df_eval_compra_all['size'].isin(remove_size)]
df_eval_compra_all = df_eval_compra_all[df_eval_compra_all['family_desc'].isin(family_include)]

df_eval_compra_all['q_dif_abs'] = df_eval_compra_all['q_dif'].abs()
df_eval_compra_all['q_dif_real_m_rec'] = df_eval_compra_all['q_real'] - df_eval_compra_all['q_estimate']

df_eval_date_fam_size = df_eval_compra_all.groupby(['date_shopping', 'family_desc',
                                                    'size', 'size_desc']).agg({'q_estimate': 'sum',
                                                                                        'q_real': 'sum'}).reset_index()

#####################################################################################################################
# recomendacion adaptada

df_adapt_raw = pd.read_csv(file_adapt)

df_adapt = df_adapt_raw.rename(columns={'cantidad_pedida': 'q_adapt', 'size': 'size_desc'})
df_adapt.loc[df_adapt['family_desc'] == 'DENIM JEANS', 'family_desc'] = 'DENIM'
df_adapt = df_adapt[df_adapt['family_desc'].isin(family_include)]

##########################################

df = pd.merge(df_eval_date_fam_size, df_adapt, on=['date_shopping', 'family_desc', 'size_desc'], how='outer')

df = df[~((df['q_estimate'].isna()) & ((df['q_adapt'].isna()) | (df['q_adapt'] == 0)))]

df['q_adapt'] = df['q_adapt'].fillna(0)




#####################################################################################################################
# Distribution, removing the volume

df_gr_dist = df.groupby(['date_shopping', 'size_desc']).agg({'q_real': 'sum', 'q_estimate': 'sum', 'q_adapt': 'sum'}).reset_index()

df_gr_dist['q_real_norm'] = df_gr_dist.groupby(['date_shopping'])[['q_real']].transform(lambda x: (x - x.min()) / (x.max() - x.min()))
df_gr_dist['q_estimate_norm'] = df_gr_dist.groupby(['date_shopping'])[['q_estimate']].transform(lambda x: (x - x.min()) / (x.max() - x.min()))
df_gr_dist['q_adapt_norm'] = df_gr_dist.groupby(['date_shopping'])[['q_adapt']].transform(lambda x: (x - x.min()) / (x.max() - x.min()))


list_date = list(df_gr_dist['date_shopping'].unique())

# Normalised

fig, ax = plt.subplots(len(list_date), 1,  figsize=(6, 14), sharex=True)
i = 0
for shop_date in list_date:
    df_shop = df_gr_dist[df_gr_dist['date_shopping'] == shop_date]
    sns.barplot(data=df_shop, x='size_desc', y='q_real_norm', color="olive", alpha=0.5, label="Shopping", ax=ax[i])
    sns.barplot(data=df_shop, x='size_desc', y='q_estimate_norm', label="Recommendation",
                facecolor=(1, 1, 1, 0), edgecolor="tomato", linewidth=2.5, ax=ax[i])

    sns.barplot(data=df_shop, x='size_desc', y='q_adapt_norm', label="Modification",
                facecolor=(1, 1, 1, 0), edgecolor="teal", linewidth=1.5, ax=ax[i]) # , alpha=0.5

    ax[i].set_title('Shop date: ' + str(shop_date))
    ax[i].set_xticklabels(ax[i].get_xticklabels(), fontsize=14, rotation=90)
    ax[i].set(xlabel='', ylabel='quantity')
    plt.legend()
    i = i + 1

plt.savefig(os.path.join(path_save, 'Comparison_normalises_recommendation_modif_shop_date_size.png'))

# With quantity

fig, ax = plt.subplots(len(list_date), 1,  figsize=(6, 14), sharex=True)
i = 0
for shop_date in list_date:
    df_shop = df_gr_dist[df_gr_dist['date_shopping'] == shop_date]
    sns.barplot(data=df_shop, x='size_desc', y='q_real', color="olive", alpha=0.5, label="Shopping", ax=ax[i])
    sns.barplot(data=df_shop, x='size_desc', y='q_estimate', label="Recommendation",
                facecolor=(1, 1, 1, 0), edgecolor="tomato", linewidth=2.5, ax=ax[i])

    sns.barplot(data=df_shop, x='size_desc', y='q_adapt', label="Modification",
                facecolor=(1, 1, 1, 0), edgecolor="teal", linewidth=1.5, ax=ax[i]) # , alpha=0.5



    ax[i].set_title('Shop date: ' + str(shop_date))
    ax[i].set_xticklabels(ax[i].get_xticklabels(), fontsize=14, rotation=90)
    ax[i].set(xlabel='', ylabel='quantity')
    plt.legend()
    i = i + 1

plt.savefig(os.path.join(path_save, 'Comparison_quantity_recommendation_modif_shop_date_size.png'))


# Distribution for each date and family

df_gr_dist_fam = df.groupby(['date_shopping', 'family_desc', 'size_desc']).agg({'q_real': 'sum', 'q_estimate': 'sum', 'q_adapt': 'sum'}).reset_index()

df_gr_dist_fam['q_real_norm'] = df_gr_dist_fam.groupby(['date_shopping'])[['q_real']].transform(lambda x: (x - x.min()) / (x.max() - x.min()))
df_gr_dist_fam['q_estimate_norm'] = df_gr_dist_fam.groupby(['date_shopping'])[['q_estimate']].transform(lambda x: (x - x.min()) / (x.max() - x.min()))

df_gr_dist_fam['q_adapt_norm'] = df_gr_dist_fam.groupby(['date_shopping'])[['q_adapt']].transform(lambda x: (x - x.min()) / (x.max() - x.min()))


family_include_0 = ['ABRIGO', 'BLUSA', 'CAMISETA', 'CARDIGAN', 'CHAQUETA', 'DENIM', 'FALDA', 'JERSEY']
family_include_1 = ['JUMPSUIT', 'PANTALON', 'PARKA', 'SHORT', 'SUDADERA', 'TOP', 'TRENCH', 'VESTIDO']

for shop_date in list_date:
    fig, ax = plt.subplots(8, 2, figsize=(12, 14), sharex=True)
    plt.setp(ax, ylim=(0, 1))
    fig.suptitle('Shop date: ' + str(shop_date) + '. Normalized')

    i = 0

    for fam in family_include_0:
        j = 0
        df_shop = df_gr_dist_fam[(df_gr_dist_fam['date_shopping'] == shop_date) & (df_gr_dist_fam['family_desc'] == fam)]
        sns.barplot(data=df_shop, x='size_desc', y='q_real_norm', color="olive", alpha=0.5, label="Shopping", ax=ax[i, j])
        sns.barplot(data=df_shop, x='size_desc', y='q_estimate_norm', label="Recommendation",
                    facecolor=(1, 1, 1, 0), edgecolor="tomato", linewidth=2.5, ax=ax[i, j])

        sns.barplot(data=df_shop, x='size_desc', y='q_adapt_norm', label="Modification",
                    facecolor=(1, 1, 1, 0), edgecolor="teal", linewidth=1.5, ax=ax[i, j])

        ax[i, j].set_title(fam)
        ax[i, j].set_xticklabels(ax[i, j].get_xticklabels(), fontsize=14, rotation=90)
        ax[i, j].set(xlabel='', ylabel='quantity')
        plt.legend()
        i = i + 1

    i = 0

    for fam in family_include_1:
        j = 1
        df_shop = df_gr_dist_fam[(df_gr_dist_fam['date_shopping'] == shop_date) & (df_gr_dist_fam['family_desc'] == fam)]
        sns.barplot(data=df_shop, x='size_desc', y='q_real_norm', color="olive", alpha=0.5, label="Shopping",
                    ax=ax[i, j])
        sns.barplot(data=df_shop, x='size_desc', y='q_estimate_norm', label="Recommendation",
                    facecolor=(1, 1, 1, 0), edgecolor="tomato", linewidth=2.5, ax=ax[i, j])

        sns.barplot(data=df_shop, x='size_desc', y='q_adapt_norm', label="Modification",
                    facecolor=(1, 1, 1, 0), edgecolor="teal", linewidth=1.5, ax=ax[i, j])

        ax[i, j].set_title(fam)
        ax[i, j].set_xticklabels(ax[i, j].get_xticklabels(), fontsize=14, rotation=90)
        ax[i, j].set(xlabel='', ylabel='quantity')
        plt.legend()
        i = i + 1

    plt.savefig(os.path.join(path_save, 'Comparison_normalized_recommendation_modif_shop_date_family_size_' + str(shop_date) + '.png'))

# Quantity

for shop_date in list_date:
    fig, ax = plt.subplots(8, 2, figsize=(12, 14), sharex=True)
    # plt.setp(ax, ylim=(0, 1))
    fig.suptitle('Shop date: ' + str(shop_date) + '. Quantity')

    i = 0

    for fam in family_include_0:
        j = 0
        df_shop = df_gr_dist_fam[(df_gr_dist_fam['date_shopping'] == shop_date) & (df_gr_dist_fam['family_desc'] == fam)]
        sns.barplot(data=df_shop, x='size_desc', y='q_real', color="olive", alpha=0.5, label="Shopping", ax=ax[i, j])
        sns.barplot(data=df_shop, x='size_desc', y='q_estimate', label="Recommendation",
                    facecolor=(1, 1, 1, 0), edgecolor="tomato", linewidth=2.5, ax=ax[i, j])

        sns.barplot(data=df_shop, x='size_desc', y='q_adapt', label="Modification",
                    facecolor=(1, 1, 1, 0), edgecolor="teal", linewidth=1.5, ax=ax[i, j])

        ax[i, j].set_title(fam)
        ax[i, j].set_xticklabels(ax[i, j].get_xticklabels(), fontsize=14, rotation=90)
        ax[i, j].set(xlabel='', ylabel='quantity')
        plt.legend()
        i = i + 1

    i = 0

    for fam in family_include_1:
        j = 1
        df_shop = df_gr_dist_fam[(df_gr_dist_fam['date_shopping'] == shop_date) & (df_gr_dist_fam['family_desc'] == fam)]
        sns.barplot(data=df_shop, x='size_desc', y='q_real', color="olive", alpha=0.5, label="Shopping",
                    ax=ax[i, j])
        sns.barplot(data=df_shop, x='size_desc', y='q_estimate', label="Recommendation",
                    facecolor=(1, 1, 1, 0), edgecolor="tomato", linewidth=2.5, ax=ax[i, j])

        sns.barplot(data=df_shop, x='size_desc', y='q_adapt', label="Modification",
                    facecolor=(1, 1, 1, 0), edgecolor="teal", linewidth=1.5, ax=ax[i, j])

        ax[i, j].set_title(fam)
        ax[i, j].set_xticklabels(ax[i, j].get_xticklabels(), fontsize=14, rotation=90)
        ax[i, j].set(xlabel='', ylabel='quantity')
        plt.legend()
        i = i + 1

    plt.savefig(os.path.join(path_save, 'Comparison_quantity_recommendation_modif_shop_date_family_size_' + str(shop_date) + '.png'))
##############################################################################################################################
# stats

df_gr_shop = df_gr_dist_fam.groupby(['date_shopping'])['q_real'].sum()

# verificar
file_real = ('/var/lib/lookiero/stock/stock_tool/eval_estimates_real.csv.gz')

df_real = pd.read_csv(file_real)
df_real_gr_date = df_real.groupby(['id_stuart', 'info_type'])['q_real'].sum().reset_index()






sns.barplot(data=df_gr_date_size, x='size_desc', y='q_dif_abs', hue='date_shopping')


fig, ax = plt.subplots()
sns.barplot(data=df_gr_dist, x='size_desc', y='q_real', hue='date_shopping')
sns.barplot(data=df_gr_dist, x='size_desc', y='q_estimate', hue='date_shopping')

fig, ax = plt.subplots()
sns.barplot(data=df_gr_dist, x='size_desc', y='q_real')
fig, ax = plt.subplots()
sns.barplot(data=df_gr_dist, x='size_desc', y='q_estimate')


sns.barplot(data=df_gr_dist[['size_desc', 'q_real', 'date_shopping']], x='size_desc', hue='date_shopping')

sns.kdeplot(data=df_gr_dist[['size_desc', 'q_estimate', 'date_shopping']], x='size_desc', hue='date_shopping')


sns.displot(data=df_gr_dist['size_desc', 'q_estimate', 'date_shopping'], x='size_desc', hue='date_shopping')


sns.distplot(df_gr_dist["q_real"] , color="skyblue", label="Shopping")
sns.distplot(df_gr_dist["q_estimate"] , color="red", label="Recommendation")



# size
df_gr_date_size = df_eval_compra_all.groupby(['date_shopping', 'size_desc']).agg({'q_dif_abs': 'sum'}).reset_index()

fig, ax = plt.subplots()
sns.barplot(data=df_gr_date_size, x='size_desc', y='q_dif_abs', hue='date_shopping')

# family_size
df_gr_date_fam_size = df_eval_compra_all.groupby(['date_shopping', 'family_desc', 'size_desc']).agg({'q_dif_abs': 'sum'}).reset_index()

fig, ax = plt.subplots()

g = sns.catplot(data=df_gr_date_fam_size, x='size_desc', y='q_dif_abs', hue='date_shopping', col='family_desc',
            col_wrap=8, kind='bar', height=4, aspect=1)

for ax in g.axes.ravel():
    # ax.axhline(0, color="k", clip_on=False)
    ax.set_xticklabels(ax.get_xticklabels(), fontsize=14, rotation=90)

(g.set_axis_labels("", "Unidades").set_titles("{col_name}"))

# g.fig.suptitle('Stock real vs Proyeccion de Stuart, familia - talla')

g.fig.subplots_adjust(top=0.92, bottom=0.1)

# ax.set_xticklabels(ax.get_xticklabels(), rotation=40, ha="right")
# plt.tight_layout()


# family_size
df_gr_date_fam_size_dif = df_eval_compra_all.groupby(['date_shopping', 'family_desc', 'size_desc']).agg({'q_dif': 'sum'}).reset_index()

fig, ax = plt.subplots()

g = sns.catplot(data=df_gr_date_fam_size_dif, x='size_desc', y='q_dif', hue='date_shopping', col='family_desc',
            col_wrap=8, kind='bar', height=4, aspect=1)


# size
df_eval_compra_all['q_dif_real_m_rec'] = df_eval_compra_all['q_real'] - df_eval_compra_all['q_estimate']
df_gr_date_size_new = df_eval_compra_all.groupby(['date_shopping', 'size_desc']).agg({'q_dif_real_m_rec': 'sum'}).reset_index()

fig, ax = plt.subplots()
sns.barplot(data=df_gr_date_size_new, x='size_desc', y='q_dif_real_m_rec', hue='date_shopping')


# Distribution

df_gr_dist = df_eval_compra_all.groupby(['date_shopping', 'size_desc']).agg({'q_real': 'sum', 'q_estimate': 'sum'}).reset_index()
fig, ax = plt.subplots()

sns.kdeplot(data=df_gr_dist['size_desc', 'q_real', 'date_shopping'], y='q_real', hue='date_shopping')

sns.kdeplot(data=df_gr_dist['size_desc', 'q_real', 'date_shopping'], x='size_desc', hue='date_shopping')
sns.displot(data=df_gr_dist['size_desc', 'q_estimate', 'date_shopping'], x='size_desc', hue='date_shopping')





# df_gr_dist['q_real_norm'] = (df_gr_dist['q_real'] - df_gr_dist['q_real'].min()) / (df_gr_dist['q_real'].max() - df_gr_dist['q_real'].min())




# df_gr_dist['q_estimate_norm'] = (df_gr_dist['q_estimate'] - df_gr_dist['q_estimate'].min()) / (df_gr_dist['q_estimate'].max() - df_gr_dist['q_estimate'].min())


# normalized_df=(df-df.min())/(df.max()-df.min())


##################################################################3



# df_adapt_date_fam_size = df_adapt_raw.groupby(['date_shopping', 'family_desc', 'size'])['cantidad_pedida'].sum().reset_index().rename(columns={'cantidad_pedida': 'q_adapt'})
#
# df_adapt_date_size = df_adapt_raw.groupby(['date_shopping', 'size'])['cantidad_pedida'].sum().reset_index().rename(columns={'cantidad_pedida': 'q_adapt'})





from scipy.stats import pearsonr


# okr
okr_file = ('/home/darya/Documents/okr-weekly-tribe/okr_stuart.csv')

df_okr = pd.read_csv(okr_file)

df_env = df_okr[df_okr['okr_type'] == 'envios']
df_right_stock_all = df_okr[df_okr['okr_type'] == 'right_stock_all_families']
df_right_stock_filt = df_okr[df_okr['okr_type'] == 'right_stock_filtered_families']

pearsonr(df_env['okr_value_success_rescaled'], df_env['pct_agree_stuart_shopping'])

pearsonr(df_right_stock_all['okr_value_success_rescaled'], df_right_stock_all['pct_agree_stuart_shopping'])


pearsonr(df_right_stock_filt['okr_value_success_rescaled'], df_right_stock_filt['pct_agree_stuart_shopping'])

