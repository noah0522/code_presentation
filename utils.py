# -*- coding: utf-8 -*-

"""
    filename:    utils.py

    function：   tool file
"""

import numpy as np
import pandas as pd


def dif1(data):
    flied_name = 'loan_apply_time'
    dif(data, flied_name)

# dif1函数的作用是：计算同一个客户每笔申请时间的时间差，保留时间差在100天及以上的申请


def dif2(data):
    flied_name = 'research_over_time'
    dif(data, flied_name)

# dif2函数的作用是：计算同一个客户每笔调查时间的时间差，保留时间差在100天及以上的调查


def dif3(data):
    data = data.sort_values(by=['c_time']).reset_index(drop=False)
    res = data
    if len(data) > 1:
        day = data['c_time'].diff().apply(lambda x: x.total_seconds()).to_frame()
        data['cut'] = day['c_time']
        data['cut'] = data['cut'].fillna(190)
        data1 = data.loc[data['cut'] > 180]
        res = data
        if len(data1) < len(data):
            c = list(set(data.index) - set(data1.index))
            d = [x - 1 for x in c]
            index_ser = data.iloc[sorted(list(set(c + d)))]['index']
            res = data[~data['index'].isin(index_ser)]
    return res

# dif3函数的作用是：计算同一个经理不同建档数据的时间，如果两个建档时间小于3分钟，则两笔建档数据都不保存


def count_2(df):
    num = 2
    count(df, num)

# count_2函数的作用是：因为有效渠道铺设是选取当月第二笔，记为有效，所以选取每个二维码第二笔申请对应的申请时间


def count_3(df):
    num = 3
    count(df, num)

# count_3函数的作用是：因为老客户转介绍数是选取当月第三笔，记为有效，所以选取每个二维码第三笔申请对应的申请时间


def dif(data, flied_name):
    data = data.sort_values(by=[flied_name]).reset_index(drop=False)
    index = [data['index'][0]]
    i = 0
    if len(data) > 1:
        for j in range(1, len(data)):
            day = (data[flied_name][j] - data[flied_name][i]).days
            if day >= 100:
                i = j
                index.append(data['index'][j])
        data = data[data['index'].isin(index)]
        data_res = data
    else:
        data = data[data['index'].isin(index)]
        data_res = data
    return data_res


def count(df, num):
    df = df.sort_values('loan_apply_time').reset_index(drop=False)
    res = np.nan
    if len(df) >= num:
        res = str(df['loan_apply_time'][num - 1]) + ';' + str(df['loan_apply_id'][num - 1])
    return res


def str2time(col_name, df):
    df[col_name] = df[col_name].apply(lambda x: str(x)[0:19])
    df.loc[df[col_name] == 'None', col_name] = np.nan
    df[col_name] = pd.to_datetime(df[col_name])
    return df


def take_rec(df, func):
    index = df.groupby(['user_id', 'qrcode', 'year', 'month']).apply(func).reset_index(drop=False).rename(
        columns={0: 'concat'})
    index = index.dropna().reset_index(drop=True)
    index['loan_apply_time'] = index['concat'].apply(lambda x: x.split(';')[0])
    index['loan_apply_id'] = index['concat'].apply(lambda x: x.split(';')[1])

    return index


def dif1_1(data):
    data = data.sort_values(by=['loan_apply_time']).reset_index(drop=False)
    index = [data['index'][0]]
    data['day_gap'] = np.nan
    i = 0
    if len(data) > 1:
        for j in range(1, len(data)):
            day = (data['loan_apply_time'][j] - data['loan_apply_time'][i]).days
            data['day_gap'][j] = day
            if day >= 100:
                i = j
                index.append(data['index'][j])

        data.loc[data['index'].isin(index), '同一百天保留_手机号'] = '保留'
        data.loc[~data['index'].isin(index), '同一百天保留_手机号'] = '剔除'

        return data
    else:
        data = data[data['index'].isin(index)]
        data['同一百天保留_手机号'] = '保留'
        return data


def dif1_2(data):
    data = data.sort_values(by=['loan_apply_time']).reset_index(drop=False)
    index = [data['index'][0]]
    data['day_gap'] = np.nan
    i = 0
    if len(data) > 1:
        for j in range(1, len(data)):
            day = (data['loan_apply_time'][j] - data['loan_apply_time'][i]).days
            data['day_gap'][j] = day
            if day >= 100:
                i = j
                index.append(data['index'][j])

        data.loc[data['index'].isin(index), '同一百天保留_身份证'] = '保留'
        data.loc[~data['index'].isin(index), '同一百天保留_身份证'] = '剔除'

        return data
    else:
        data = data[data['index'].isin(index)]
        data['同一百天保留_身份证'] = '保留'
        return data


def dif2_1(data):
    data = data.sort_values(by=['research_over_time']).reset_index(drop=False)
    index = [data['index'][0]]
    data['day_gap'] = np.nan
    i = 0
    if len(data) > 1:
        for j in range(1, len(data)):
            day = (data['research_over_time'][j] - data['research_over_time'][i]).days
            data['day_gap'][j] = day
            if day >= 100:
                i = j
                index.append(data['index'][j])

        data.loc[data['index'].isin(index), '调查同一百天保留_手机号'] = '保留'
        data.loc[~data['index'].isin(index), '调查同一百天保留_手机号'] = '剔除'

        return data
    else:
        data = data[data['index'].isin(index)]
        data['调查同一百天保留_手机号'] = '保留'
        return data


def dif2_2(data):
    data = data.sort_values(by=['research_over_time']).reset_index(drop=False)
    index = [data['index'][0]]
    data['day_gap'] = np.nan
    i = 0
    if len(data) > 1:
        for j in range(1, len(data)):
            day = (data['research_over_time'][j] - data['research_over_time'][i]).days
            data['day_gap'][j] = day
            if day >= 100:
                i = j
                index.append(data['index'][j])

        data.loc[data['index'].isin(index), '调查同一百天保留_身份证'] = '保留'
        data.loc[~data['index'].isin(index), '调查同一百天保留_身份证'] = '剔除'

        return data
    else:
        data = data[data['index'].isin(index)]
        data['调查同一百天保留_身份证'] = '保留'
        return data


def dif3_1(data):
    data = data.sort_values(by=['c_time'])
    data = data.reset_index(drop=False)
    if len(data) > 1:
        day = data['c_time'].diff().apply(lambda x: x.total_seconds()).to_frame()
        data['cut'] = day['c_time']
        data['cut'] = data['cut'].fillna(190)
        data1 = data.loc[data['cut'] > 180]
        if len(data1) < len(data):
            c = list(set(data.index) - set(data1.index))
            d = [x - 1 for x in c]
            dele = data.iloc[sorted(list(set(c + d)))]['index']

            data.loc[~data['index'].isin(dele), '是否满足建档三分钟'] = '是'
            data.loc[data['index'].isin(dele), '是否满足建档三分钟'] = '否'
            return data
        else:
            data['是否满足建档三分钟'] = '是'
            return data
    else:
        data['是否满足建档三分钟'] = '是'
        return data





