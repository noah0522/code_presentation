# -*- coding: utf-8 -*-

"""
    Created on Fri Dec 27 2019

    @author: noah
"""

from impala.dbapi import connect
from impala.util import as_pandas
from utils import dif1, dif2, dif3, count_2, count_3, str2time, take_rec
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

    # 用于客户有效建档数的计算
    sql = "select \
          a.id as id, \
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
          where a.data_source='0' and a.mobile_phone is not null"

    df1 = pd.read_sql(sql, conn)

    # 银行客户经理的基本信息
    sql = "select \
          id as user_id, \
          user_name, \
          bank_id, \
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
          join warehouse_atomic_hzx_b_loan_research_group_relate as g \
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
          left join warehouse_atomic_hzx_l_cust_level_research_show s \
          on s.task_id = ts.id and s.xy_id = t.xy_id \
          left join warehouse_atomic_hzx_l_cust_group_research_show gs \
          on gs.task_id = ts.id and gs.group_id = g.group_id \
          where t.version_id = g.version_id and g.enable = True \
          and t.enable=1 and t.is_required=True and t.is_entry=True and t.statement_type=0 and s.id is null \
          and gs.id is null and ts.research_apply_time>='2019-09-20' \
          group by ts.research_apply_id"

    df_rr = pd.read_sql(sql, conn)
    df_rr['finish_rate'] = df_rr['result_count'] / df_rr['all_count']
    df_rr = df_rr[['research_apply_id', 'finish_rate']]

    # 剔除银行id为空的数据
    df = df[df['bank_id'] != -9999].reset_index(drop=True)

    # 在df中添加完成度
    df = df.merge(df_rr, on='research_apply_id', how='left')

    # 取时间的前19位，转换时间格式
    # 调查完成时间、申请时间、二维码创建时间、调查开始时间

    col_names = ['research_over_time', 'loan_apply_time', 'c_time', 'research_apply_time']

    for col_name in col_names:
        df = str2time(col_name, df)

    df['research_over_date'] = pd.DatetimeIndex(pd.to_datetime(df['research_over_time'])).date

    df['research_apply_date'] = pd.DatetimeIndex(pd.to_datetime(df['research_apply_time'])).date

    # 取出申请时间日期对应的年月
    year = []
    for i in range(0, len(df)):
        year.append(df['loan_apply_time'][i].year)

    month = []
    for i in range(0, len(df)):
        month.append(df['loan_apply_time'][i].month)

    df['year'] = year
    df['month'] = month

    # dff是保留了客户经理id为空的数据，df没有
    # 只有计算有效调查才会使用到dff
    dff = df.copy()

    df = df[pd.isna(df['user_id']) is False]

    # 计算每个日期的差值

    # 以【手机号、银行号】或者【身份证号、银行号】为分组依据
    # 对申请时间进行排序以后，计算两两之间的时间差，如果时间差大于100，则保留后面一条数据

    # start,final用于截取时间段，每周只需修改这两个日期即可
    time = pd.to_datetime('2019-09-20').date()
    start = pd.to_datetime('2019-12-16').date()
    final = pd.to_datetime('2019-12-22').date()

    # 分别使用用户手机号、用户身份证号码来进行同100天的筛选
    df_gp = df.groupby(['mobile', 'bank_id']).apply(dif1).reset_index(drop=True).drop('index', axis=1)
    df_gp = df_gp.groupby(['id_card', 'bank_id']).apply(dif1).reset_index(drop=True).drop('index', axis=1)
    df_gp = df_gp[df_gp['loan_apply_time'] >= time]

    # 有效渠道铺设数_筛选其满足2次申请对应的时间
    df_qd = df_gp[df_gp['label'] == 4]    # 选取二维码标签为4的数据
    df_qd = df_qd[df_qd['c_time'] >= time]

    # 使用经理id、二维码id、年、月来进行分组，调用函数，选取当月第二笔
    index1 = take_rec(df_qd, count_2)

    # 对二维码进行去重，计算有效渠道铺设数
    index1 = index1.groupby(['user_id', 'loan_apply_time']).agg({"qrcode": pd.Series.nunique}).reset_index(
        drop=False).rename(columns={'qrcode': '有效渠道铺设数'})

    # 日期格式转换
    index1 = str2time('loan_apply_time', index1)

    index1['loan_apply_time'] = pd.DatetimeIndex(pd.to_datetime(index1['loan_apply_time'])).date

    # 老客户转介绍数_筛选其满足3次申请对应的时间
    df_old = df_gp[df_gp['label'] == 5]      # 选取二维码标签为5的数据
    df_old = df_old[df_old['c_time'] >= time]

    # 使用经理id、二维码id、年、月来进行分组，调用函数，选取当月第三笔
    index2 = take_rec(df_old, count_3)

    # 对二维码进行去重，计算老客户转介绍数
    index2 = index2.groupby(['user_id', 'loan_apply_time']).agg({"qrcode": pd.Series.nunique}).reset_index(
        drop=False).rename(columns={'qrcode': '老客户转介绍数'})

    # 日期格式转换
    index2 = str2time('loan_apply_time', index2)

    index2['loan_apply_time'] = pd.DatetimeIndex(pd.to_datetime(index2['loan_apply_time'])).date

    # 客户申请准入数
    # 在前面去同100天的数据上，对经理id、申请时间进行分组，计算申请客户的数量
    apply_count = df_gp.groupby(['user_id', 'loan_apply_time'])['customer_id'].count().reset_index().rename(
        columns={'customer_id': '客户申请准入数'})
    appl = df_gp.groupby(['qrcode', 'year', 'month']).agg({"loan_apply_id": pd.Series.nunique}).reset_index().rename(
        columns={'loan_apply_id': '有效申请量', 'qrcode': '二维码id', 'year': '申请年份', 'month': '申请月份'})

    # 客户调查通过数
    # 使用调查的时间来计算100天

    # 计算每一笔调查，所用的调查时间
    dff['time_cut'] = dff['research_over_time'] - dff['research_apply_time']

    # 转化成秒
    dff['time_second'] = dff['time_cut'].apply(lambda x: x.total_seconds())
    df_re = dff[dff['research_apply_date'] >= time]

    # 分别使用用户手机号、用户身份证号码来进行同100天的筛选
    df_re = df_re.groupby(['mobile', 'bank_id']).apply(dif2).reset_index(drop=True).drop('index', axis=1)
    df_re = df_re.groupby(['id_card', 'bank_id']).apply(dif2).reset_index(drop=True).drop('index', axis=1)

    # 筛选出调查时间大于5分钟以及调查完成度大于95%的调查
    df_re = df_re[(df_re['time_second'] > 300) & (df_re['finish_rate'] > 0.95)]

    # 使用调查经理和调查完成时间计算分组，对客户数进行求和
    inves_count = df_re.groupby(['a_user_id', 'research_over_date'])['customer_id'].count().reset_index().rename(
        columns={'research_over_date': 'loan_apply_time', 'customer_id': '客户调查通过数', 'a_user_id': 'user_id'})

    # 客户建档数
    df11 = df1.copy()

    # 对建档时间进行转换
    df11['c_date'] = df11['c_date'].apply(lambda x: str(x)[0:19])
    df11['c_time'] = pd.to_datetime(df11['c_date'])
    df11['c_date'] = pd.DatetimeIndex(pd.to_datetime(df11['c_date'])).date
    df11 = df11[(df11['c_user'] != '-1') & (df11['c_date'] >= time)].reset_index(drop=True)

    # 计算每一个地址出现的次数
    df12 = df11[df11['real_address'].notna()].loc[df11['real_address'] != 'null']
    df12 = df12[df12['real_address'] != 'undefined']
    df12 = df12.groupby(['c_user'])['real_address'].value_counts().to_frame().rename(
        columns={'real_address': 'count'}).reset_index(drop=False)

    # 由于一个客户只能保留一条建档数据，所以使用手机号码和创建时间进行排序，去重取第一条数据
    df11 = df11.sort_values(by=['mobile_phone', 'c_time']).drop_duplicates(['mobile_phone'], keep='first')
    df11 = df11.groupby('c_user').apply(dif3)
    df11 = df11.drop(['index'], axis=1).reset_index(drop=True)

    # extentive45为淡旺季，首先对空值进行填充，然后将淡旺季为空值的、0、0-12、1-12的数据删掉，此为无效数据
    df11['extentive45'] = df11['extentive45'].fillna('-1')
    df11 = df11.loc[(df11['extentive45'] != '0') & (df11['extentive45'] != '1,2,3,4,5,6,7,8,9,10,11,12') & (
                df11['extentive45'] != '0,1,2,3,4,5,6,7,8,9,10,11,12') & (df11['extentive45'] != '-1')]

    # 将数据进行合并，合并的目的是df12计算了每一个地址的个数
    df11 = df11.merge(df12, on=['c_user', 'real_address'], how='left')

    # 地址重复个数如果超过8个，则记为无效，删除
    df11 = df11.loc[df11['count'] <= 8]
    df1_gp = df11.groupby(['c_user', 'c_date'])['id'].count().reset_index().rename(
        columns={'c_date': 'loan_apply_time', 'id': '客户建档数', 'c_user': 'user_id'})
    df1_gp['loan_apply_time'] = pd.DatetimeIndex(pd.to_datetime(df1_gp['loan_apply_time'])).date

    # 合并数据
    data = apply_count.merge(inves_count, on=['user_id', 'loan_apply_time'], how='outer')

    if len(index1) == 0:
        data['有效渠道铺设数'] = 0
    else:
        index1 = index1[['user_id', 'loan_apply_time', '有效渠道铺设数']]
        data = data.merge(index1, on=['user_id', 'loan_apply_time'], how='left')

    if len(index2) == 0:
        data['老客户转介绍数'] = 0
    else:
        index2 = index2[['user_id', 'loan_apply_time', '老客户转介绍数']]
        data = data.merge(index2, on=['user_id', 'loan_apply_time'], how='left')

    data['user_id'] = data['user_id'].apply(lambda x: str(x)[0:16])

    df1_gp['user_id'] = df1_gp['user_id'].apply(lambda x: str(x))
    data = data.merge(df1_gp, on=['user_id', 'loan_apply_time'], how='outer')
    df_info['user_id'] = df_info['user_id'].apply(lambda x: str(x)[0:16])
    data = data.merge(df_info, on=['user_id'], how='left')
    data['bank_id'] = data['bank_id'].apply(lambda x: str(x))
    bank_info['bank_id'] = bank_info['bank_id'].apply(lambda x: str(x))
    data = data.merge(bank_info, on=['bank_id'], how='inner')
    data = data.fillna(0)

    # 月度数据执行该句代码：
    # data['当日积分']=data['客户建档数']*1 + data['有效渠道铺设数']*2.5 + data['老客户转介绍数']*2.5 +data['客户申请准入数']*3 +data['客户调查通过数']*4
    # 每周数据执行该句代码：因为有效渠道铺设数和老客户转介绍数据是按月计算的
    data['当日积分'] = data['客户建档数'] * 1 + data['客户申请准入数'] * 3 + data['客户调查通过数'] * 4

    data = data.drop('user_id', axis=1)
    data = data.rename(columns={'bank_id': '银行ID', 'bank_name': '银行名称', 'account': '客户经理账号', 'user_name': '客户经理姓名',
                                'loan_apply_time': '日期'})
    data = data[data['日期'] >= time]
    data = data.sort_values(by=['日期'])
    data = data[['日期', '银行ID', '银行名称', '客户经理账号', '客户经理姓名', '当日积分', '客户建档数', '有效渠道铺设数', '老客户转介绍数',
                 '客户申请准入数', '客户调查通过数']]
    data['银行ID'] = data['银行ID'].apply(lambda x: str(x)[0:16])
    data = data.drop(['有效渠道铺设数', '老客户转介绍数'], axis=1)    # 月度数据请注释掉该句代码

    data = data[(data['日期'] >= start) & (data['日期'] <= final)]
    data.to_excel('./output/汇总.xlsx', index=False)

    ############################################################################################################
    ############################################################################################################
    # 汇总个人排名和银行排名
    df_regimentwar = data.copy()

    # 客户经理数据，需入库
    df_manager = pd.read_excel('./input/客户经理数据.xlsx', index=False)

    df_manager['银行ID'] = df_manager['银行ID'].apply(lambda x: str(x))
    df_manager['客户经理ID'] = df_manager['客户经理ID'].apply(lambda x: str(x))
    df_manager['客户经理账号'] = df_manager['客户经理账号'].apply(lambda x: str(x))

    # 参加百日团战的客户经理数据汇总
    df_regimentwar_sum = df_regimentwar.groupby(
        ['银行名称', '客户经理姓名', '客户经理账号']).sum()[['当日积分', '客户建档数', '客户申请准入数', '客户调查通过数']]
    df_regimentwar_sum.columns = ['累计积分', '客户建档数合计', '客户申请准入数合计', '客户调查通过数合计']

    # 输出
    # df_regimentwar_sum.to_excel(path_output, sheet_name='汇总数据')

    # 客户经理的总排名和行内排名
    df_manager_all = df_manager.merge(
        df_regimentwar_sum, left_on=['客户经理账号', '银行名称'], right_on=['客户经理账号', '银行名称'], how='left')
    df_manager_all = df_manager_all[
        ['银行名称', '客户经理名称', '客户经理账号', '累计积分', '客户建档数合计', '客户申请准入数合计', '客户调查通过数合计']]
    df_manager_all = df_manager_all.fillna(0)

    df_manager_all['整体排名'] = df_manager_all['累计积分'].rank(method='min', ascending=False)

    df_manager_all['行内排名'] = df_manager_all[['银行名称', '累计积分']].groupby('银行名称').rank(method='min', ascending=False)

    df_manager_all = df_manager_all.sort_values(by=['银行名称'])

    df_manager_all = df_manager_all.set_index('银行名称')
    df_manager_all = df_manager_all[['客户经理名称', '客户经理账号', '累计积分', '整体排名', '行内排名', '客户建档数合计', '客户申请准入数合计', '客户调查通过数合计']]

    # 输出
    # df_manager_all.to_excel(path_output, sheet_name='个人排名')

    df_regimentwar_sum = df_manager_all.drop(['整体排名', '行内排名'], axis=1)
    df_regimentwar_sum = df_regimentwar_sum.reset_index(drop=True)

    # 输出
    # df_regimentwar_sum.to_excel(path_output, sheet_name='汇总数据')

    # 银行排名
    df_sum = df_manager_all.groupby('银行名称').sum()[['累计积分', '客户建档数合计', '客户申请准入数合计', '客户调查通过数合计']]
    df_sum.columns = ['总积分', '客户建档数合计', '客户申请准入数合计', '客户调查完成数']

    df_mean = df_manager_all.groupby('银行名称').mean()[['累计积分']]
    df_mean.columns = ['平均积分']

    df_count = df_manager_all.groupby('银行名称').count()[['客户经理账号']]
    df_count.columns = ['客户经理数量']

    # 合并
    df_bank_rank = df_sum.join([df_mean, df_count])

    df_bank_rank['排名'] = df_bank_rank['平均积分'].rank(method='min', ascending=False)
    df_bank_rank = df_bank_rank[['总积分', '平均积分', '排名', '客户经理数量', '客户建档数合计', '客户申请准入数合计', '客户调查完成数']]

    # 输出
    # df_bank_rank.to_excel(path_output, sheet_name='银行排名')

    # 输出
    path_output = './output/百日团战数据({}).xlsx'.format(final)
    with pd.ExcelWriter(path_output) as writer:
        df_regimentwar_sum.to_excel(writer, sheet_name='汇总数据', index=False)
        df_manager_all.to_excel(writer, sheet_name='个人排名')
        df_bank_rank.to_excel(writer, sheet_name='银行排名')
    ###############################################################################################################
    ###############################################################################################################
    # 明细数据计算

    # 有效扫街地图明细
    sql = "select \
          a.id as id, \
          b.name as bank_name, \
          d.user_name as user_name, \
          a.c_user as c_user, \
          d.account as account, \
          a.name as cus_name, \
          a.c_date as c_date, \
          a.extentive45 as extentive45, \
          if(a.extentive52 IS NOT NULL,a.extentive52,a.mobile_phone) as mobile_phone, \
          a.real_address as real_address \
          from warehouse_atomic_hzx_custmanage_c_customer as a \
          left join warehouse_atomic_hzx_b_bank_base_info as b \
          on cast(a.bank_id as bigint) = b.id \
          left join warehouse_atomic_hzx_b_bank_user as d \
          on cast (a.c_user as bigint) = d.id \
          where a.data_source='0' and a.extentive45 is not null and a.mobile_phone is not null \
          and b.name like '%村镇银行%' and a.c_date>='2019-09-20'"

    map_1 = pd.read_sql(sql, conn)

    # 日期格式转换、选择某个时间段的数据
    map_1['date'] = map_1['c_date'].apply(lambda x: str(x)[0:19])
    map_1.loc[map_1['date'] == 'None', 'date'] = np.nan
    map_1['date'] = pd.DatetimeIndex(pd.to_datetime(map_1['date'])).date
    map_1 = map_1[(map_1['date'] >= start) & (map_1['date'] <= final)]
    map_1 = map_1.drop(['date'], axis=1)
    map_1.columns = ['id', '银行名称', '客户经理', 'c_user', '客户经理账号', '客户姓名', '创建时间', '淡旺季', '手机号', '信息位置']
    map_1 = df11.merge(map_1, on=['id', 'c_user'], how='left')
    map_1 = map_1[['银行名称', '客户经理', '客户经理账号', '客户姓名', '创建时间', '淡旺季', '手机号', '信息位置', 'count']]
    map_1.columns = ['银行名称', '客户经理', '客户经理账号', '客户姓名', '创建时间', '淡旺季', '手机号', '信息位置', '地址重复次数']
    map_1 = map_1.dropna()

    # 客户调查明细
    sql = "select \
          c.name as bank_name, \
          a.customer_name as customer_name, \
          e.id_card as id_card, \
          e.mobile as mobile, \
          a.loan_apply_id as loan_apply_id, \
          d.name as pro_name, \
          a.user_id as user_id, \
          a.a_user_id as a_user_id, \
          a.research_apply_time as research_apply_time, \
          a.research_over_time as research_over_time, \
          a.research_apply_id as research_apply_id, \
          a.research_status as research_status \
          from warehouse_atomic_hzx_research_task as a \
          left join warehouse_atomic_hzx_b_bank_base_info as c \
          on a.bank_id=c.id \
          left join warehouse_atomic_hzx_bank_product_info as d \
          on a.bank_pro_id=d.id \
          left join warehouse_atomic_hzx_c_customer  as e \
          on a.customer_id=cast(e.id as bigint) \
          where c.name like '%村镇银行%' and c.name not like '%已废弃%'"

    rea_detail = pd.read_sql(sql, conn)

    # 字段格式、时间格式转化
    rea_detail['user_id'] = rea_detail['user_id'].apply(lambda x: str(x)[0:16])
    rea_detail['a_user_id'] = rea_detail['a_user_id'].apply(lambda x: str(x)[0:16])
    rea_detail['research_over_time'] = rea_detail['research_over_time'].apply(lambda x: str(x)[0:19])

    rea_detail.loc[rea_detail['research_over_time'] == 'None', 'research_over_time'] = np.nan
    rea_detail['date'] = pd.DatetimeIndex(pd.to_datetime(rea_detail['research_over_time'])).date
    rea_detail['research_over_time'] = pd.to_datetime(rea_detail['research_over_time'])
    # df_info['user_id']=df_info['user_id'].astype(str)
    df_info['bank_id'] = df_info['bank_id'].apply(lambda x: str(x))
    rea_detail['research_apply_time'] = rea_detail['research_apply_time'].apply(lambda x: str(x)[0:19])
    rea_detail.loc[rea_detail['research_apply_time'] == 'None', 'research_apply_time'] = np.nan
    rea_detail['research_apply_time'] = pd.to_datetime(rea_detail['research_apply_time'])

    # 计算调查时间的时间差，转换成分钟
    rea_detail['time_cut'] = rea_detail['research_over_time'] - rea_detail['research_apply_time']
    rea_detail['time_second'] = rea_detail['time_cut'].apply(lambda x: x.total_seconds())
    rea_detail['time_second'] = rea_detail['time_second'] / 60

    rea_detail['research_apply_id'] = rea_detail['research_apply_id'].apply(lambda x: str(x))
    df_rr['research_apply_id'] = df_rr['research_apply_id'].apply(lambda x: str(x))

    rea_detail = rea_detail.merge(df_rr, on='research_apply_id', how='left').drop(
        ['research_apply_time', 'time_cut', 'research_apply_id'], axis=1)
    rea_detail = rea_detail.merge(df_info[['user_id', 'user_name']], on='user_id', how='left').drop(
        ['user_id'], axis=1).rename(columns={'user_name': '营销客户经理'})

    rea_detail = rea_detail.rename(columns={'a_user_id': 'user_id'})
    rea_detail = rea_detail.merge(df_info[['user_id', 'user_name']], on='user_id', how='left').drop(
        ['user_id'], axis=1).rename(columns={'user_name': '主办客户经理'})
    rea_detail['research_status'] = rea_detail['research_status'].apply(lambda x: str(x))
    rea_detail = rea_detail[(rea_detail['research_status'] == '4') | (rea_detail['research_status'] == '5')]
    rea_detail = rea_detail[(rea_detail['date'] >= start) & (rea_detail['date'] <= final)]

    rea_detail = rea_detail[
        ['bank_name', 'customer_name', 'id_card', 'mobile', 'loan_apply_id', 'pro_name', '营销客户经理', '主办客户经理',
         'finish_rate', 'time_second', 'research_over_time', 'research_status']]
    rea_detail.columns = ['银行名称', '客户姓名', '身份证号', '联系方式', '贷款申请编码', '产品名称', '营销客户经理', '主办客户经理',
                          '调查完成度', '调查耗时（分）', '调查完成时间', '调查状态']
    rea_detail['贷款申请编码'] = rea_detail['贷款申请编码'].apply(lambda x: str(x)[0:16])
    rea_detail.loc[rea_detail['调查状态'] == '4', '调查状态'] = '调查完成'
    rea_detail.loc[rea_detail['调查状态'] == '5', '调查状态'] = '调查拒绝'

    ################################################################################

    '''
    # 此代码只在每月数据跑，因为统计指标为按月计算
    # 二维码明细
    sql = "select \
          c.name as bank_name, \
          if(a.qrcode is not null,b.user_id,a.user_id) as user_id, \
          b.name as code_name, \
          b.label as code_label, \
          b.c_time as c_time, \
          a.loan_apply_time as loan_apply_time, \
          a.qrcode as qrcode \
          from warehouse_atomic_hzx_research_task as a \
          left join warehouse_atomic_hzx_b_bank_qr_code as b \
          on a.qrcode = b.id \
          left join warehouse_atomic_hzx_b_bank_base_info as c \
          on a.bank_id=c.id \
          where c.name like '%村镇银行%' and c.name not like '%已废弃%' and b.c_time>='2019-09-20' \
          and a.loan_apply_time>='2019-09-20' and a.m_state=5"
    
    code_detail = pd.read_sql(sql,conn)
    
    # 字段格式、时间格式转化
    code_detail['user_id'] = code_detail['user_id'].apply(lambda x:str(x)[0:16])
    code_detail = code_detail.merge(df_info[['user_id', 'user_name', 'account']], on='user_id', how='left').drop('user_id', axis=1)
    code_detail = code_detail[['bank_name', 'user_name', 'account', 'code_name', 'code_label','c_time',
           'loan_apply_time', 'qrcode']]
    code_detail.columns = ['银行名称', '客户经理', '客户经理账号', '二维码名称',' 二维码标签', '创建时间', '申请时间', '二维码id']
    
    year = []
    for i in range(0,len(code_detail)):
        year.append(code_detail['申请时间'][i][0:4])
    month = []
    for i in range(0,len(code_detail)):
        month.append(code_detail['申请时间'][i][5:7])
    
    code_detail['申请年份'] = year
    code_detail['申请月份'] = month
    code_detail['申请年份'] = code_detail['申请年份'].astype(int)
    code_detail['申请月份'] = code_detail['申请月份'].astype(int)
    
    merg = code_detail[['银行名称', '客户经理', '客户经理账号', '二维码id', '二维码名称', '二维码标签']].drop_duplicates(
        ['银行名称', '客户经理', '客户经理账号', '二维码id', '二维码名称', '二维码标签'], keep='first')
    code_detail = code_detail.groupby(['银行名称', '客户经理', '客户经理账号', '二维码id', '申请年份', '申请月份'])['申请时间']\
        .count().reset_index(drop=False).rename(columns={'申请时间':'申请量'})
    code_detail = code_detail.merge(merg, on=['银行名称', '客户经理', '客户经理账号', '二维码id'], how='left')
    code_detail = code_detail.loc[(code_detail['二维码标签'] == 4)|(code_detail['二维码标签'] == 5)]
    code_detail.loc[code_detail['二维码标签'] == 4,'二维码标签'] = '渠道'
    code_detail.loc[code_detail['二维码标签'] == 5,'二维码标签'] ='老客户'
    appl['二维码id'] = appl['二维码id'].apply(lambda x :str(x)[0:16])
    code_detail['二维码id'] = code_detail['二维码id'].astype(str)
    
    code_detail = code_detail.merge(appl, on=['二维码id','申请年份','申请月份'], how='left')
    code_detail = code_detail[['银行名称', '客户经理', '客户经理账号', '二维码id', '二维码名称', '二维码标签', '申请年份', '申请月份',
                             '申请量', '有效申请量']]
    code_detail['二维码id'] = code_detail['二维码id'].astype(str)
    
    # 计算有效渠道对应的申请量
    code1 = code_detail.loc[(code_detail['二维码标签'] == '渠道') & (code_detail['有效申请量'] >= 2)]
    code2 = code_detail.loc[(code_detail['二维码标签'] == '老客户') & (code_detail['有效申请量'] >= 3)]
    code = code1.append(code2)
    
    # 计算渠道对应的无效申请
    code1 = code_detail.loc[(code_detail['二维码标签'] == '渠道') & (code_detail['有效申请量'] < 2)]
    code2 = code_detail.loc[(code_detail['二维码标签'] == '老客户') & (code_detail['有效申请量'] < 3)]
    code_not = code1.append(code2)
    '''
    ################################################################################

    # 申请明细（去除同100天）
    sql = "select \
          c.name as bank_name, \
          a.customer_name as customer_name, \
          e.id_card as id_card, \
          e.mobile as mobile, \
          if(a.qrcode is not null,b.user_id,a.user_id) as user_id, \
          a.a_user_id as a_user_id, \
          if(a.qrcode is not null,'二维码','APP') as qrtype, \
          b.name as code_name, \
          b.label as code_label, \
          d.name as pro_name, \
          a.loan_apply_time as loan_apply_time, \
          a.m_state as m_state, \
          a.rec_amount as rec_amount, \
          a.qrcode as qrcode, \
          a.bank_id as bank_id \
          from warehouse_atomic_hzx_research_task as a \
          left join warehouse_atomic_hzx_b_bank_qr_code as b \
          on a.qrcode = b.id \
          left join warehouse_atomic_hzx_b_bank_base_info as c \
          on a.bank_id=c.id \
          left join warehouse_atomic_hzx_bank_product_info as d \
          on a.bank_pro_id=d.id \
          left join warehouse_atomic_hzx_c_customer as e \
          on a.customer_id=cast(e.id as bigint) \
          where c.name like '%村镇银行%' and c.name not like '%已废弃%' and a.loan_apply_time>='2019-09-01' and a.m_state=5"

    appl_100 = pd.read_sql(sql, conn)

    # 字段格式、时间格式转化
    appl_100 = appl_100[pd.isna(appl_100['user_id']) is False].reset_index(drop=True)
    appl_100['user_id'] = appl_100['user_id'].apply(lambda x: str(x)[0:16])
    appl_100['a_user_id'] = appl_100['a_user_id'].apply(lambda x: str(x)[0:16])
    appl_100['loan_apply_time'] = appl_100['loan_apply_time'].apply(lambda x: str(x)[0:19])
    appl_100['loan_apply_time'] = pd.DatetimeIndex(pd.to_datetime(appl_100['loan_apply_time'])).date

    # 分别使用用户手机号、用户身份证号码来进行同100天的筛选
    appl_100 = appl_100.groupby(['mobile', 'bank_id']).apply(dif1).reset_index(drop=True).drop('index', axis=1)
    appl_100 = appl_100.groupby(['id_card', 'bank_id']).apply(dif1).reset_index(drop=True).drop('index', axis=1)
    appl_100 = appl_100[appl_100['loan_apply_time'] >= time]

    appl_100 = appl_100.merge(df_info[['user_id', 'user_name']], on='user_id', how='left').drop('user_id', axis=1).rename(
        columns={'user_name': '营销客户经理'})
    appl_100 = appl_100.rename(columns={'a_user_id': 'user_id'})
    appl_100 = appl_100.merge(df_info[['user_id', 'user_name']], on='user_id', how='left').drop('user_id', axis=1).rename(
        columns={'user_name': '主办客户经理'})
    appl_100 = appl_100[['bank_name', 'customer_name', 'id_card', 'mobile', '营销客户经理', '主办客户经理', 'qrtype',
                         'code_name', 'code_label', 'pro_name', 'loan_apply_time', 'm_state', 'rec_amount', 'qrcode']]

    appl_100.columns = ['银行名称', '客户姓名', '身份证号', '联系方式', '营销客户经理', '主办客户经理', '客户来源（APP/二维码）',
                        '二维码名称', '二维码标签', '申请产品名称', '申请时间', '申请进度', '申请预授信额度', '二维码id']
    appl_100.loc[appl_100['二维码标签'] == 4, '二维码标签'] = '渠道'
    appl_100.loc[appl_100['二维码标签'] == 5, '二维码标签'] = '老客户'
    appl_100.loc[appl_100['二维码标签'] == 1, '二维码标签'] = '个人'
    appl_100.loc[appl_100['二维码标签'] == 2, '二维码标签'] = '商家'
    appl_100.loc[appl_100['二维码标签'] == 3, '二维码标签'] = '银行资料'
    appl_100.loc[appl_100['二维码标签'] == 0, '二维码标签'] = '其他'

    appl_100['二维码id'] = appl_100['二维码id'].apply(lambda x: str(x)[0:16])
    appl_100['申请年份'] = appl_100['申请时间'].apply(lambda x: x.year)
    appl_100['申请月份'] = appl_100['申请时间'].apply(lambda x: x.month)

    ####################################################################################
    '''
    # 按月才跑
    appl_code = code.merge(appl_100, on=['银行名称', '二维码id', '二维码名称', '二维码标签', '申请年份', '申请月份'], how='left')
    appl_code = appl_code[['银行名称', '客户姓名', '身份证号', '联系方式', '营销客户经理', '主办客户经理', '客户来源（APP/二维码）', 
                           '二维码标签', '申请产品名称', '申请时间', '申请进度', '申请预授信额度']]
    
    appl_ncode = code_not.merge(appl_100, on=['银行名称', '二维码id', '二维码名称', '二维码标签', '申请年份', '申请月份'], how='left')
    appl_ncode = appl_ncode[['银行名称', '客户姓名', '身份证号', '联系方式', '营销客户经理', '主办客户经理', '客户来源（APP/二维码）', 
                             '二维码标签', '二维码名称', '申请产品名称', '申请时间', '申请进度', '申请预授信额度']]
    '''
    ################################################################################

    appl_100 = appl_100[(appl_100['申请时间'] >= start) & (appl_100['申请时间'] <= final)]

    # 调查明细（去除同100天）
    df_re = df_re.rename(columns={'loan_apply_id': '贷款申请编码'}).reset_index(drop=True)
    df_re['贷款申请编码'] = df_re['贷款申请编码'].apply(lambda x: str(x)[0:16])
    df_re = df_re[(df_re['research_over_date'] >= start) & (df_re['research_over_date'] <= final)]
    df_re = df_re.merge(rea_detail[['银行名称', '客户姓名', '身份证号', '联系方式', '贷款申请编码', '产品名称', '营销客户经理', '主办客户经理']],
                        on=['贷款申请编码'], how='left')
    df_re = df_re[['银行名称', '客户姓名', '身份证号', '联系方式', '贷款申请编码', '产品名称', '营销客户经理', '主办客户经理',
                   'finish_rate', 'time_second', 'research_over_time', 'research_status']]
    df_re['time_second'] = df_re['time_second'] / 60
    df_re.columns = ['银行名称', '客户姓名', '身份证号', '联系方式', '贷款申请编码', '产品名称', '营销客户经理', '主办客户经理',
                     '调查完成度', '调查耗时（分）', '调查完成时间', '调查状态']
    df_re.loc[df_re['调查状态'] == '4', '调查状态'] = '调查完成'
    df_re.loc[df_re['调查状态'] == '5', '调查状态'] = '调查拒绝'

    # 身份证号以及手机号转成明文
    map_1['手机号'] = map_1['手机号'].apply(lambda x: base64.b64decode(x).decode('utf-8') if not x.startswith('1') else x)
    appl_100['身份证号'] = appl_100['身份证号'].apply(lambda x: base64.b64decode(x).decode('utf-8'))
    appl_100['联系方式'] = appl_100['联系方式'].apply(lambda x: base64.b64decode(x).decode('utf-8'))
    df_re['身份证号'] = df_re['身份证号'].apply(lambda x: base64.b64decode(x).decode('utf-8'))
    df_re['联系方式'] = df_re['联系方式'].apply(lambda x: base64.b64decode(x).decode('utf-8'))

    ############################################################################################
    '''
    # 按月才跑
    appl_code['身份证号'] = appl_code['身份证号'].apply(lambda x: base64.b64decode(x).decode('utf-8'))
    appl_code['联系方式'] = appl_code['联系方式'].apply(lambda x: base64.b64decode(x).decode('utf-8'))
    appl_ncode['身份证号'] = appl_ncode['身份证号'].apply(lambda x: base64.b64decode(x).decode('utf-8'))
    appl_ncode['联系方式'] = appl_ncode['联系方式'].apply(lambda x: base64.b64decode(x).decode('utf-8'))
    '''
    ############################################################################################

    map_1.to_excel('./output/1-1 有效客户建档明细_1222.xlsx', index=False)
    # code.to_excel('./output/2-1 有效渠道及老客户统计明细_11月.xlsx',index=False)    # 月度数据使用该代码
    # code_not.to_excel('./output/2-1 无效渠道及老客户统计明细_11月.xlsx',index=False)    # 月度数据使用该代码
    # appl_code.to_excel('./output/2-2 有效渠道及老客户申请明细_11月.xlsx',index=False)    # 月度数据使用该代码
    # appl_ncode.to_excel('./output/2-2 无效渠道及老客户申请明细_11月.xlsx',index=False)    # 月度数据使用该代码
    appl_100.to_excel('./output/3-1 有效客户申请明细_1222.xlsx', index=False)
    df_re.to_excel('./output/4-1 有效客户调查明细_1222.xlsx', index=False)

    cursor.close()
    conn.close()


if __name__ == '__main__':
    main()
