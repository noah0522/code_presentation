# -*- coding: utf-8 -*-

"""
    Created on Wed Dec 25 2019

    @author: noah
"""

from impala.dbapi import connect
from impala.util import as_pandas
from utils import dif1_1, dif1_2, dif2_1, dif2_2, dif3_1
import numpy as np
import pandas as pd
import base64


def main():
    conn = connect(host='******', port=10000, database='default', auth_mechanism='PLAIN')
    cursor = conn.cursor()
    cursor.execute('show databases')
    print(as_pandas(cursor))

    # df用于有效老客户数、有效渠道数、申请数的计算
    sql = "select \
          a.bank_id as bank_id, \
          c.name as bank_name, \
          a.dot_id as dot_id, \
          if(a.qrcode IS NOT NULL,b.user_id,a.user_id) AS user_id, \
          a.research_over_time as research_over_time, \
          d.id_card as customer_id, \
          a.pro_id as pro_id, \
          a.loan_apply_id as loan_apply_id, \
          a.loan_apply_time as loan_apply_time, \
          b.label as label, \
          b.c_time as c_time, \
          a.qrcode as qrcode, \
          a.m_state as m_state, \
          a.a_user_id as a_user_id, \
          a.research_apply_time as research_apply_time, \
          a.research_apply_id as research_apply_id, \
          a.version_id as version_id, \
          a.research_status as research_status, \
          d.mobile as mobile, \
          d.id_card as id_card \
          from warehouse_atomic_hzx_research_task as a \
          left join warehouse_atomic_hzx_b_bank_qr_code as b \
          on a.qrcode=b.id \
          left join warehouse_atomic_hzx_b_bank_base_info as c \
          on a.bank_id=c.id \
          left join warehouse_atomic_hzx_c_customer as d \
          on a.customer_id=d.id \
          where a.loan_apply_time >='2019-09-01' and a.m_state=5 and c.name like '%村镇银行%' and d.name not like '%测试%' "

    df = pd.read_sql(sql, conn)

    # 银行基本信息
    sql = "select \
          id as bank_id, \
          name as bank_name  \
          from warehouse_atomic_hzx_b_bank_base_info  \
          where name like '%村镇银行%' and name not like '%已废弃%' "

    bank_info = pd.read_sql(sql, conn)

    # 建档客户信息
    sql = "select \
          a.id as id, \
          a.name as cus_name, \
          a.bank_id as bank_id, \
          a.c_date as c_date, \
          a.c_user as c_user, \
          a.id_card as id_card, \
          a.customer_owner_id as customer_owner_id, \
          if(a.extentive52 is not null,a.extentive52,a.mobile_phone) as mobile_phone, \
          a.data_source as data_source, \
          a.extentive45 as extentive45, \
          a.longitude as longitude, \
          a.latitude as latitude, \
          a.real_address as real_address \
          from warehouse_atomic_hzx_custmanage_c_customer as a \
          where a.data_source='0' "

    df1 = pd.read_sql(sql, conn)

    # 银行客户经理的基本信息
    sql = "select id as user_id, \
           user_name, \
           bank_id , \
           dot_id, \
           account \
           from warehouse_atomic_hzx_b_bank_user as a"

    df_info = pd.read_sql(sql, conn)

    # 调查问题完成度的计算
    sql = "select \
          ts.research_apply_id as research_apply_id, \
          count(DISTINCT t.xy_id) as all_count, \
          count(DISTINCT r.r_id) as result_count \
          from warehouse_atomic_hzx_research_task as ts \
          join warehouse_atomic_hzx_b_loan_research_group_relate g \
          on ts.version_id=g.version_id \
          join warehouse_atomic_hzx_b_research_content_templet t \
          on g.group_id= t.g_id \
          left join  (select \
                      r_id, \
                      research_id \
                      from warehouse_atomic_hzx_l_research_result \
              union all \
                      select \
                      question_id, \
                      research_id \
                      from warehouse_atomic_hzx_l_research_apply_photo) r \
          on t.xy_id = r.r_id and r.research_id = ts.research_apply_id \
          left join warehouse_atomic_hzx_l_cust_level_research_show s on s.task_id = ts.id and s.xy_id = t.xy_id \
          left join warehouse_atomic_hzx_l_cust_group_research_show gs on gs.task_id = ts.id and gs.group_id = g.group_id \
          where t.version_id = g.version_id and g.enable = True and t.enable= 1 and t.is_required = True \
          and t.is_entry = True and t.statement_type = 0 and s.id is null and gs.id is null \
          and ts.research_apply_time>='2019-09-20' \
          group by ts.research_apply_id"

    df_rr = pd.read_sql(sql, conn)
    df_rr['finish_rate'] = df_rr['result_count'] / df_rr['all_count']
    df_rr = df_rr[['research_apply_id', 'finish_rate']]

    # 剔除申请时间为空的数据
    df = df[df['bank_id'] != -9999].reset_index(drop=True)
    df = df.merge(df_rr, on='research_apply_id', how='left')

    # 取时间的前19位，转换时间格式
    df['research_over_time'] = df['research_over_time'].apply(lambda x: str(x)[0:19])
    df.loc[df['research_over_time'] == 'None', 'research_over_time'] = np.nan
    df['research_over_date'] = pd.DatetimeIndex(pd.to_datetime(df['research_over_time'])).date
    df['research_over_time'] = pd.to_datetime(df['research_over_time'])

    df['loan_apply_time'] = df['loan_apply_time'].apply(lambda x: str(x)[0:19])
    df.loc[df['loan_apply_time'] == 'None', 'loan_apply_time'] = np.nan
    df['loan_apply_time'] = pd.DatetimeIndex(pd.to_datetime(df['loan_apply_time'])).date

    df['c_time'] = df['c_time'].apply(lambda x: str(x)[0:19])
    df.loc[df['c_time'] == 'None', 'c_time'] = np.nan
    df['c_time'] = pd.DatetimeIndex(pd.to_datetime(df['c_time'])).date

    df['research_apply_time'] = df['research_apply_time'].apply(lambda x: str(x)[0:19])
    df.loc[df['research_apply_time'] == 'None', 'research_apply_time'] = np.nan
    df['research_apply_time'] = pd.to_datetime(df['research_apply_time'])
    df['research_apply_date'] = pd.DatetimeIndex(pd.to_datetime(df['research_apply_time'])).date

    # 取出日期对应的年月
    year = []
    for i in range(0, len(df)):
        year.append(df['loan_apply_time'][i].year)

    month = []
    for i in range(0, len(df)):
        month.append(df['loan_apply_time'][i].month)

    df['year'] = year
    df['month'] = month

    dff = df.copy()
    df = df[pd.isna(df['user_id']) is False]

    # 由于该代码是为了标出，为什么这个数据为无效数据，所以所有数据都应当保留
    # 计算每个日期的差值
    # 计算同一个客户每笔申请时间的时间差，保留时间差在100天及以上的申请

    time = pd.to_datetime('2019-09-20').date()

    # 分别使用用户手机号、用户身份证号码来进行同100天的筛选，标出该条数据是保留还是剔除
    df_gp = df.groupby(['mobile', 'bank_id']).apply(dif1_1).reset_index(drop=True).drop('index', axis=1)
    df_gp = df_gp.groupby(['id_card', 'bank_id']).apply(dif1_2).reset_index(drop=True).drop('index', axis=1)
    df_gp = df_gp[df_gp['loan_apply_time'] >= time]

    # 客户调查通过数
    # 使用调查的时间来计算100天

    dff['time_cut'] = dff['research_over_time'] - dff['research_apply_time']
    dff['time_second'] = dff['time_cut'].apply(lambda x: x.total_seconds())
    df_re = dff[dff['research_apply_date'] >= time]

    df_re = df_re.groupby(['mobile', 'bank_id']).apply(dif2_1).reset_index(drop=True).drop('index', axis=1)
    df_re = df_re.groupby(['id_card', 'bank_id']).apply(dif2_2).reset_index(drop=True).drop('index', axis=1)

    df_re.loc[df_re['time_second'] > 300, '调查耗时是否大于5分钟'] = '是'
    df_re.loc[df_re['time_second'] <= 300, '调查耗时是否大于5分钟'] = '否'
    df_re.loc[df_re['finish_rate'] > 0.95, '调查完成度是否大于95%'] = '是'
    df_re.loc[df_re['finish_rate'] <= 0.95, '调查完成度是否大于95%'] = '否'

    # 客户建档数

    df11 = df1.copy()
    df11['c_date'] = df11['c_date'].apply(lambda x: str(x)[0:19])
    df11['bank_id'] = df11['bank_id'].fillna(-1)
    df11 = df11[df11['bank_id'] != -1]
    df11['c_time'] = pd.to_datetime(df11['c_date'])
    df11['c_date'] = pd.DatetimeIndex(pd.to_datetime(df11['c_date'])).date
    df11 = df11[(df11['c_user'] != '-1') & (df11['c_date'] >= time)].reset_index(drop=True)
    df12 = df11[df11['real_address'].notna()].loc[(df11['real_address'] != 'null') & (df11['real_address'] != 'undefined')]
    df12 = df12.groupby(['c_user'])['real_address'].value_counts().to_frame().rename(
        columns={'real_address': 'count'}).reset_index(drop=False)

    df111 = df11.sort_values(['mobile_phone', 'c_time']).drop_duplicates(['mobile_phone'], keep='first')
    df112 = df111.groupby('c_user').apply(dif3_1)
    df112 = df112.drop(['index'], axis=1).reset_index(drop=True)

    df112 = df112[['c_user', 'mobile_phone', 'c_time', '是否满足建档三分钟']]

    df111 = df111[['c_user', 'mobile_phone', 'c_time']]
    df111['是否为第一条建档记录'] = '是'
    df1111 = pd.merge(df11, df111, on=['c_user', 'mobile_phone', 'c_time'], how='left')
    df1111['是否为第一条建档记录'] = df1111['是否为第一条建档记录'].fillna('否')
    df1111 = pd.merge(df1111, df112, on=['c_user', 'mobile_phone', 'c_time'], how='left')
    df1111['是否满足建档三分钟'] = df1111['是否满足建档三分钟'].fillna('不满足第一次建档已剔除')

    df1111['extentive45'] = df1111['extentive45'].fillna('-1')
    df1111.loc[df1111['extentive45'] == '-1', '是否淡旺季剔除'] = '是'
    df1111.loc[df1111['extentive45'] == '0', '是否淡旺季剔除'] = '是'
    df1111.loc[df1111['extentive45'] == '1,2,3,4,5,6,7,8,9,10,11,12', '是否淡旺季剔除'] = '是'
    df1111.loc[df1111['extentive45'] == '0,1,2,3,4,5,6,7,8,9,10,11,12', '是否淡旺季剔除'] = '是'

    df1111['是否淡旺季剔除'] = df1111['是否淡旺季剔除'].fillna('否')
    df1111 = df1111.merge(df12, on=['c_user', 'real_address'], how='left')
    df1111.loc[df1111['count'] > 8, '地址是否有效'] = "否"
    df1111.loc[df1111['count'] <= 8, '地址是否有效'] = "是"
    df1111['地址是否有效'] = df1111['地址是否有效'].fillna('否')

    # 改每周或每月时间
    star = pd.to_datetime('2019-12-16').date()
    final = pd.to_datetime('2019-12-22').date()

    # 建档客户
    sql = "select \
          bank_id, \
          id_card as customer_id,mobile, \
          name as customer_name \
          from warehouse_atomic_hzx_c_customer"

    cus_info = pd.read_sql(sql, conn)
    cus_info = cus_info[cus_info['bank_id'].notna()]
    cus_info = cus_info.sort_values(['bank_id', 'customer_id', 'mobile', 'customer_name']).drop_duplicates(
        ['bank_id', 'customer_id', 'mobile', 'customer_name'], keep='first')

    # 银行二维码
    sql = "select \
          id as qrcode, \
          name as code_name \
          from warehouse_atomic_hzx_b_bank_qr_code"

    qrcode_info = pd.read_sql(sql, conn)

    # 客户建档规则剔除
    bank_info['bank_id'] = bank_info['bank_id'].astype(str)
    df1111['bank_id'] = df1111['bank_id'].apply(lambda x: str(x))
    df1111 = df1111.merge(bank_info, on=['bank_id'], how='left')

    df_info['user_id'] = df_info['user_id'].apply(lambda x: str(x))
    df_info['bank_id'] = df_info['bank_id'].apply(lambda x: str(x))

    df1111['user_id'] = df1111['c_user'].apply(lambda x: str(x))

    map_detail = df1111.merge(df_info[['user_id', 'user_name', 'account']], on=['user_id'], how='left')

    map_detail = map_detail[
        ['bank_name', 'user_name', 'account', 'cus_name', 'c_time', 'extentive45', 'mobile_phone', 'real_address',
         '是否满足建档三分钟', '是否为第一条建档记录', '是否淡旺季剔除', '地址是否有效']]

    map_detail.columns = ['银行名称', '客户经理', '客户经理账号', '客户姓名', '创建时间', '淡旺季', '手机号', '信息位置', '是否满足建档三分钟', '是否为第一条建档记录',
                          '是否淡旺季剔除', '地址是否有效']
    map_detail['c_date'] = pd.DatetimeIndex(pd.to_datetime(map_detail['创建时间'])).date

    map_detail = map_detail[(map_detail['c_date'] >= star) & (map_detail['c_date'] <= final)]

    # 客户申请准入明细
    sql = "select \
          a.research_apply_id as research_apply_id, \
          a.rec_amount as rec_amount,d.name as pro_name \
          from warehouse_atomic_hzx_research_task as a \
          left join warehouse_atomic_hzx_b_bank_base_info as c \
          on a.bank_id=c.id \
          left join warehouse_atomic_hzx_bank_product_info as d \
          on a.bank_pro_id=d.id \
          where c.name like '%村镇银行%' and c.name not like '%已废弃%' and a.loan_apply_time>='2019-09-20' and a.m_state=5"

    pro_name = pd.read_sql(sql, conn)

    cus_info['bank_id'] = cus_info['bank_id'].apply(lambda x: str(x)[0:16])

    # df_gp['bank_id']=df_gp['bank_id'].astype(str)
    df_gp['bank_id'] = df_gp['bank_id'].apply(lambda x: str(x))

    appl_detail = df_gp.merge(cus_info, on=['bank_id', 'customer_id', 'mobile'], how='left')
    appl_detail = appl_detail.merge(pro_name, on='research_apply_id', how='left')

    appl_detail['qrcode'] = appl_detail['qrcode'].apply(lambda x: str(x)[0:16])

    qrcode_info['qrcode'] = qrcode_info['qrcode'].apply(lambda x: str(x))
    appl_detail = appl_detail.merge(qrcode_info, on='qrcode', how='left')

    appl_detail['user_id'] = appl_detail['user_id'].apply(lambda x: str(x)[0:16])
    appl_detail['a_user_id'] = appl_detail['a_user_id'].apply(lambda x: str(x)[0:16])

    appl_detail = appl_detail.merge(df_info[['user_id', 'user_name']],
                                    on='user_id', how='left').drop('user_id', axis=1).rename(columns={'user_name': '营销客户经理'})
    appl_detail = appl_detail.rename(columns={'a_user_id': 'user_id'})
    appl_detail = appl_detail.merge(df_info[['user_id', 'user_name']],
                                    on='user_id', how='left').drop('user_id', axis=1).rename(columns={'user_name': '主办客户经理'})

    appl_detail.loc[appl_detail['同一百天保留_手机号'] == '剔除', '同一百天保留'] = '剔除'
    appl_detail.loc[appl_detail['同一百天保留_身份证'] == '剔除', '同一百天保留'] = '剔除'
    appl_detail['同一百天保留'] = appl_detail['同一百天保留'].fillna('保留')

    appl_detail.loc[appl_detail['qrcode'] == 'nan', 'qrtype'] = 'APP'
    appl_detail['qrtype'] = appl_detail['qrtype'].fillna('二维码')

    appl_detail = appl_detail[['bank_name', 'customer_name', 'id_card', 'mobile', '营销客户经理', '主办客户经理', 'qrtype',
                               'code_name', 'label', 'pro_name', 'loan_apply_time', 'm_state',
                               'rec_amount', 'qrcode', '同一百天保留']]

    appl_detail.columns = ['银行名称', '客户姓名', '身份证号', '联系方式', '营销客户经理', '主办客户经理', '客户来源（APP/二维码）', '二维码名称',
                           '二维码标签', '申请产品名称', '申请时间', '申请进度', '申请预授信额度', 'qrcode', '同一百天保留']

    appl_detail.loc[appl_detail['二维码标签'] == 4, '二维码标签'] = '渠道'
    appl_detail.loc[appl_detail['二维码标签'] == 5, '二维码标签'] = '老客户'
    appl_detail.loc[appl_detail['二维码标签'] == 1, '二维码标签'] = '个人'
    appl_detail.loc[appl_detail['二维码标签'] == 2, '二维码标签'] = '商家'
    appl_detail.loc[appl_detail['二维码标签'] == 3, '二维码标签'] = '银行资料'
    appl_detail.loc[appl_detail['二维码标签'] == 0, '二维码标签'] = '其他'

    # 客户调查明细
    # df_re['bank_id']=df_re['bank_id'].astype(str)
    df_re['bank_id'] = df_re['bank_id'].apply(lambda x: str(x))
    rea_detail = df_re.merge(cus_info, on=['bank_id', 'customer_id', 'mobile'], how='left')
    rea_detail = rea_detail.merge(pro_name, on='research_apply_id', how='left')

    rea_detail['user_id'] = rea_detail['user_id'].apply(lambda x: str(x)[0:16])
    rea_detail['a_user_id'] = rea_detail['a_user_id'].apply(lambda x: str(x)[0:16])

    df_info['user_id'] = df_info['user_id'].apply(lambda x: str(x))

    rea_detail = rea_detail.merge(df_info[['user_id', 'user_name']],
                                  on='user_id', how='left').drop('user_id', axis=1).rename(columns={'user_name': '营销客户经理'})
    rea_detail = rea_detail.rename(columns={'a_user_id': 'user_id'})
    rea_detail = rea_detail.merge(df_info[['user_id', 'user_name']],
                                  on='user_id', how='left').drop('user_id', axis=1).rename(columns={'user_name': '主办客户经理'})

    rea_detail.loc[rea_detail['调查同一百天保留_手机号'] == '剔除', '同一百天保留'] = '剔除'
    rea_detail.loc[rea_detail['调查同一百天保留_身份证'] == '剔除', '同一百天保留'] = '剔除'
    rea_detail['同一百天保留'] = rea_detail['同一百天保留'].fillna('保留')
    rea_detail['research_status'] = rea_detail['research_status'].apply(lambda x: str(x))

    rea_detail = rea_detail[
        ['bank_name', 'customer_name', 'id_card', 'mobile', 'loan_apply_id', 'pro_name', '营销客户经理', '主办客户经理', 'finish_rate',
         'time_second', 'research_over_time', 'research_status', '同一百天保留', '调查耗时是否大于5分钟', '调查完成度是否大于95%']]

    rea_detail.columns = ['银行名称', '客户姓名', '身份证号', '联系方式', '贷款申请编码', '产品名称', '营销客户经理', '主办客户经理', '调查完成度',
                          '调查耗时（分）', '调查完成时间', '调查状态', '同一百天保留', '调查耗时是否大于5分钟', '调查完成度是否大于95%']

    rea_detail['调查耗时（分）'] = rea_detail['调查耗时（分）'] / 60
    rea_detail['贷款申请编码'] = rea_detail['贷款申请编码'].apply(lambda x: str(x)[0:16])
    rea_detail.loc[rea_detail['调查状态'] == '4', '调查状态'] = '调查完成'
    rea_detail.loc[rea_detail['调查状态'] == '5', '调查状态'] = '调查拒绝'

    appl_detail = appl_detail[(appl_detail['申请时间'] >= star) & (appl_detail['申请时间'] <= final)]

    rea_detail['调查完成时间'] = rea_detail['调查完成时间'].apply(lambda x: str(x)[0:19])
    rea_detail.loc[rea_detail['调查完成时间'] == 'None', '调查完成时间'] = np.nan
    rea_detail['date'] = pd.DatetimeIndex(pd.to_datetime(rea_detail['调查完成时间'])).date
    rea_detail = rea_detail[(rea_detail['date'] >= star) & (rea_detail['date'] <= final)]
    rea_detail = rea_detail.drop(['date'], axis=1)

    appl_detail['身份证号'] = appl_detail['身份证号'].apply(lambda x: base64.b64decode(x).decode('utf-8'))
    appl_detail['联系方式'] = appl_detail['联系方式'].apply(lambda x: base64.b64decode(x).decode('utf-8'))
    rea_detail['身份证号'] = rea_detail['身份证号'].apply(lambda x: base64.b64decode(x).decode('utf-8'))
    rea_detail['联系方式'] = rea_detail['联系方式'].apply(lambda x: base64.b64decode(x).decode('utf-8'))

    map_detail['手机号'] = map_detail['手机号'].apply(
        lambda x: base64.b64decode(x).decode('utf-8') if not x.startswith('1') else x)

    map_detail.to_excel('./output/客户建档规则剔除明细总表.xlsx', index=False)
    appl_detail.to_excel('./output/客户申请剔除明细总表.xlsx', index=False)
    rea_detail.to_excel('./output/客户调查剔除明细总表.xlsx', index=False)

    #################################################################################
    ################################################################################
    # 取有效+无效明细
    df_filing = map_detail.copy()
    df_application = appl_detail.copy()
    df_investigation = rea_detail.copy()

    # 客户调查明细剔除
    # df_investigation['同一百天保留']=='保留'
    # df_investigation['调查耗时是否大于5分钟']=='是'
    # df_investigation['调查完成度是否大于95%']=='是'

    condition1 = (df_investigation['同一百天保留'] == '保留') \
                 & (df_investigation['调查耗时是否大于5分钟'] == '是') \
                 & (df_investigation['调查完成度是否大于95%'] == '是')

    df_investigation_valid = df_investigation[condition1]
    df_investigation_invalid = df_investigation[~condition1]

    # 输出
    df_investigation_valid.to_excel('./output/有效客户调查明细_.xlsx', index=False)
    df_investigation_invalid.to_excel('./output/无效客户调查明细_.xlsx', index=False)

    # 客户建档明细剔除
    # df_filing['是否满足建档三分钟'] == '是'
    # df_filing['是否为第一条建档记录'] == '是'
    # df_filing['是否淡旺季剔除'] == '否'
    # df_filing['地址是否有效'] == '是'

    condition2 = (df_filing['是否满足建档三分钟'] == '是') \
                 & (df_filing['是否为第一条建档记录'] == '是') \
                 & (df_filing['是否淡旺季剔除'] == '否') \
                 & (df_filing['地址是否有效'] == '是')

    df_filing_valid = df_filing[condition2]
    df_filing_invalid = df_filing[~condition2]

    # 输出
    df_filing_valid.to_excel('./output/有效客户建档明细_.xlsx', index=False)
    df_filing_invalid.to_excel('./output/无效客户建档明细_.xlsx', index=False)

    # 客户申请明细剔除
    # df_application['同一百天保留'] == '保留'

    condition3 = df_application['同一百天保留'] == '保留'

    df_application_valid = df_application[condition3]
    df_application_invalid = df_application[~condition3]

    # 输出
    df_application_valid.to_excel('./output/有效客户申请明细_.xlsx', index=False)
    df_application_invalid.to_excel('./output/无效客户申请明细_.xlsx', index=False)

    cursor.close()
    conn.close()


if __name__ == '__main__':
    main()
