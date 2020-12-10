import psycopg2
import pandas as pd
import numpy as np
import random as rd
import numpy.matlib
import scipy
from sklearn import linear_model
from time import gmtime, strftime
import sys
import time
from scipy.optimize import curve_fit
import time
import xlsxwriter
import functools as ft
import operator as op
import boto3
import logging
import traceback
from config import aws_credentials


def check_table_existance(conn_string,
                        metric_key_explanation_table,
                        dimension_ranking_table,
                        tokenized_table_name):

    table_list = [metric_key_explanation_table] + [tokenized_table_name] + [dimension_ranking_table]
    table_list = [x.strip(' ') for x in table_list]
    table_list = [x.lower() for x in table_list]

    table_list1 = [s.split('.', 1)[1] for s in table_list if '.' in s]
    table_list2 = [s for s in table_list if '.' not in s]
    table_list = table_list1 + table_list2  # can add schema checking later.
    table_input_count = len(table_list)
    table_list = str(table_list).strip('[').strip(']')
    sql_string = '''select count(*) from information_schema.tables
                   where table_name in ({})'''.format(table_list)
    conn = psycopg2.connect(conn_string)
    table_count = pd.read_sql(sql_string, conn).at[0, 'count']
    conn.close()

    if table_count == table_input_count:
        return True
    else:
        return False


def check_tokenized_privilege(tokenized, user, conn_string):
    
    conn = psycopg2.connect(conn_string)
    
    available_privileges=[]
    for p in ["select","update","delete"]:
        available_privileges.append( pd.read_sql('''select has_table_privilege('{}','{}','{}') as privilege'''.format(user, tokenized, p ), conn ).loc[0,'privilege'] )
    conn.close()

    if any([i==False for i in available_privileges]):
        return False
    else:
        return True


def check_mapping_privilege(mapping, user, conn_string):
    
    conn = psycopg2.connect(conn_string)

    available_privileges=[]
    for p in ["select"]:
        available_privileges.append( pd.read_sql('''select has_table_privilege('{}','{}','{}') as privilege'''.format(user, mapping, p ), conn ).loc[0,'privilege'] )
    
    conn.close()

    if any([i==False for i in available_privileges]):
        return False
    else:
        return True


"""
# when multiple metric explanation tables are in a list
def check_table_existance(conn_string,
                          metric_key_explanation_table,
                          tokenized_table_name):

    table_list = metric_key_explanation_table
    table_list = table_list + [tokenized_table_name]
    table_list = [x.strip(' ') for x in table_list]
    table_list = [x.lower() for x in table_list]

    table_list1 = [s.split('.', 1)[1] for s in table_list if '.' in s]
    table_list2 = [s for s in table_list if '.' not in s]
    table_list = table_list1 + table_list2  # can add schema checking later.
    table_input_count = len(table_list)
    table_list = str(table_list).strip('[').strip(']')
    sql_string = '''select count(*) from information_schema.tables
                   where table_name in ({})'''.format(table_list)
    conn = psycopg2.connect(conn_string)
    table_count = pd.read_sql(sql_string, conn).at[0, 'count']
    conn.close()

    if table_count == table_input_count:
        return True
    else:
        return False
"""



# find metric keys that have conversions 
def available_mkey(conn_string,
                    tactic_id,
                    tokenized_table_name,
                    ranked_dims_mkey,
                    ranked_dims_str,
                    kpi_col,
                    user_seg_col,
                    user_seg,
                    subset_threhold=20000
                ):

    conn = psycopg2.connect(conn_string)
    c = conn.cursor()      

    # Note that m0=0 for converted users before updating tokenized table, thus tokenized_slice does not contain rows where metric_key=="null"
    # However, since we don't do delete in multiple KPI case, double check to make sure "null" does not exist in metric_key
    # "and m0!=0???" --- m0 indicates converted users whose metric_key is very likely to be null instead of 0_0_0_0_0_......

    start_time_handle_size = time.clock()

    metric_key_sql_df = pd.read_sql('''with tokenized_slice as
        (select userid, %s, %s from %s where m0=%s and %s=%s)
        select count(distinct userid) as user_cnt,%s,%s 
        from tokenized_slice
        
        group by %s,%s 
        order by %s,%s;''' % (ranked_dims_str,
                                   
                                    kpi_col,
                                    tokenized_table_name,
                                    tactic_id,
                                    user_seg_col,
                                    user_seg, 
                                   ranked_dims_str,
                                    kpi_col,
                                   ranked_dims_str,
                                    kpi_col,
                               ranked_dims_str,
                                    kpi_col
                                    ), conn)# metric_key not in lists will have '0_0_..._0'?

#     print(metric_key_sql_df)
    # set user cap for conversion and non-conversion to <=5000 per metric key
    metric_key_sql_df.loc[metric_key_sql_df.user_cnt>subset_threhold, 'user_cnt']=subset_threhold
    
    metric_key_sql_df_need_subset=metric_key_sql_df[metric_key_sql_df.user_cnt==subset_threhold]
    print(metric_key_sql_df_need_subset)
    print( len(metric_key_sql_df_need_subset)*100/len(metric_key_sql_df),'% needs getting subsets')
#     print(metric_key_sql_df)


    string_test='create table user_sub_temp_{} as select * from '.format(tactic_id)
    for row in range(len(metric_key_sql_df)):
        string_test=string_test+' (select distinct userid from {} where m0={} and {}={} '.format(tokenized_table_name,
                                                                                                 tactic_id,
                                                                                                 user_seg_col,
                                                                                                 user_seg,)
        for col in metric_key_sql_df.columns[1:-1]:
            string_test=string_test+' and {}={}'.format(col,metric_key_sql_df.loc[row,col] )
        string_test=string_test+' and {}={} limit {}) union'.format(kpi_col,
                                                                   metric_key_sql_df.loc[row,kpi_col],
     
                                                                    
                                                                    metric_key_sql_df.loc[row,'user_cnt'])

    
    
#     string_test2='create table user_sub_temp_{} as select * from '.format(tactic_id)
#     for row in range(len(metric_key_sql_df)):
#         string_test2=string_test2+' (select distinct userid from {} where m0={}'.format(tokenized_table_name,tactic_id)
#         for col in metric_key_sql_df.columns[1:-1]:
#             string_test2=string_test2+' and {}={}'.format(col,metric_key_sql_df.loc[row,col] )
#         string_test2=string_test2+' and {}={} limit {}) union all '.format(kpi_col,
#                                                                    metric_key_sql_df.loc[row,kpi_col],
#                                                                    metric_key_sql_df.loc[row,'user_cnt'])

    
    
        
#     string_create_user_subset='''create table user_sub_temp as 
#                                         select distinct userid from {} 
#                                         where m0={} and {}="{}" and {}={} and {}={} limit {}'''.format(tokenized_table_name,                                                                                                           
#                                                                                  tactic_id,
#                                                                                                   ranked_dims_mkey,
#                                                                                                  metric_key_sql_df.loc[0,'metric_key'],
#                                                                                 user_seg_col,
#                                                                                 user_seg,
#                                                                                 kpi_col,
#                                                                                             metric_key_sql_df.loc[0,kpi_col],
#                                                                                                     metric_key_sql_df.loc[0,'user_cnt'])
    

    
    if c.closed:
        conn=psycopg2.connect(conn_string)
        c=conn.cursor()
      
    c.execute('drop table if exists user_sub_temp_{}'.format(tactic_id))
    
    
    start_time_query1 = time.clock()
    c.execute(string_test[:-5] )

    conn.commit()
    
#     logging.info("> Query1 used {} secs!".format(int(time.clock() - start_time_query1)))
    
#     c.execute('drop table if exists user_sub_temp_{}'.format(tactic_id))
#     start_time_query2 = time.clock()
#     c.execute(string_test2[:-11] )

#     conn.commit()
    
#     logging.info("> Query2 used {} secs!".format(int(time.clock() - start_time_query2)))
    



    logging.info("> Handling data size used {} secs!".format(int(time.clock() - start_time_handle_size)))        

    
    
    
    
    
    start_time_handle_size_old = time.clock()
    metric_key_sql_df = pd.read_sql('''with tokenized_slice as
        (select userid, vtimestamp, %s as metric_key, %s, %s from %s where m0=%s and %s=%s)

        select distinct metric_key 
        from tokenized_slice
        where %s=1 and metric_key is not null
        order by metric_key;''' % (ranked_dims_mkey,
                                    ranked_dims_str,
                                    kpi_col,
                                    tokenized_table_name,
                                    tactic_id,
                                    user_seg_col,
                                    user_seg, 
                                    kpi_col
                                    ), conn)
    print(metric_key_sql_df)
    logging.info("> Old Handling data size used {} secs!".format(int(time.clock() - start_time_handle_size_old)))  

    c.close()
    conn.close()
    

    

    return metric_key_sql_df






def check_kpi_col_existance(conn_string,tokenized_table_name,kpi_col):

    conn = psycopg2.connect(conn_string)
    c = conn.cursor()

    # delete left and right space
    kpi_col_processed = kpi_col.strip(" ")
    tokenized = pd.read_sql("select * from %s limit 1;" % tokenized_table_name, conn)

    conn.close()
    c.close()

    if kpi_col_processed in tokenized.columns:
        return True
    else:
        return False



def check_user_seg_col_existance(conn_string,tokenized_table_name,user_segment_col):

    conn = psycopg2.connect(conn_string)
    c = conn.cursor()

    # delete left and right space
    user_segment_col_processed = user_segment_col.strip(" ")
    tokenized = pd.read_sql("select * from %s limit 1;" % tokenized_table_name, conn)

    conn.close()
    c.close()

    if user_segment_col_processed in tokenized.columns:
        return True
    else:
        return False




def database_data_processing(
                             metric_key_sql_df,
                            ranked_dims_mkey,
                            ranked_dims_str,
                            kpi_col,
                            tactic_id,
                            user_seg_col,
                            user_seg,
                            mta_partial_seg,
                            conn_string,                            
                            tokenized_table_name,
                            timediscount,
                            ):
    user_subset_table='user_sub_temp_{}'.format(tactic_id)
    start_time = time.clock()
    conn = psycopg2.connect(conn_string)
    c = conn.cursor()

    kpi_col = kpi_col.strip(" ")

    # pd.options.display.max_colwidth=150
    metric_key_sql_df['processed_column'] = "case when metric_key='" + metric_key_sql_df['metric_key'] + \
        "' then sum_test else 0 end as m_" + metric_key_sql_df['metric_key']

    metric_key_sql_df['processed_column2'] = 'sum(m_' + metric_key_sql_df['metric_key'] + \
        ') as m_' + metric_key_sql_df['metric_key'] + ','

    metric_key_sql_df['processed_column3'] = 'sum(m_' + metric_key_sql_df['metric_key'] + ')/sum(case when m_' + \
        metric_key_sql_df['metric_key'] + '>0 then 1 else 0 end) as m_' + metric_key_sql_df['metric_key']

    metric_key_sql_df['processed_column4'] = 'cast(m_' + metric_key_sql_df['metric_key'] + \
        ' as int) as m_' + metric_key_sql_df['metric_key'] + ','
    
    metric_key_sql_df['processed_column5'] = 'm_' + metric_key_sql_df['metric_key']


    metric_key_sql_string = metric_key_sql_df['processed_column'][0]
    for i in range(1, len(metric_key_sql_df)):
        metric_key_sql_string = metric_key_sql_string + \
            "," + metric_key_sql_df['processed_column'][i]

    metric_key_sql_string2 = metric_key_sql_df['processed_column2'][0]
    for i in range(1, len(metric_key_sql_df)):
        metric_key_sql_string2 = metric_key_sql_string2 + \
            metric_key_sql_df['processed_column2'][i]

    metric_key_sql_string3 = metric_key_sql_df['processed_column3'][0]
    for i in range(1, len(metric_key_sql_df)):
        metric_key_sql_string3 = metric_key_sql_string3 + \
            "," + metric_key_sql_df['processed_column3'][i]

    metric_key_sql_string4 = metric_key_sql_df['processed_column4'][0]
    for i in range(1, len(metric_key_sql_df)):
        metric_key_sql_string4 = metric_key_sql_string4 + \
            metric_key_sql_df['processed_column4'][i]
    
    metric_key_sql_string5 = metric_key_sql_df['processed_column5'][0]
    for i in range(1, len(metric_key_sql_df)):
        metric_key_sql_string5 = metric_key_sql_string5 + \
            "," + metric_key_sql_df['processed_column5'][i]

#     sample_size_conversion=10000
    # conversion data
    if timediscount == 1:
        model_data = pd.read_sql('''with tokenized_slice as
                                        (select userid, vtimestamp, %s as metric_key,
                                        %s,
                                        %s 
                                        from %s 
                                        where m0=%s and metric_key is not null 
                                         and %s=%s and userid in (select userid from %s)),
        
        table1 as
            (select userid, vtimestamp as conversion_date from
                (select *, row_number() over (partition by userid order by vtimestamp desc) as rk from tokenized_slice) where rk=1 and %s=1),

        table2 as
            (select a.*, floor(datediff(mins,vtimestamp,conversion_date)/10080) as wk_from_conversion_weight 
            from tokenized_slice a join table1 on a.userid=table1.userid),
        
        model_data_mta as
            (select userid,%s 1 as %s
            from
            (select userid, %s from
                (select userid,
                metric_key,
                sum(1/((1+0.5)^wk_from_conversion_weight)) as sum_test
                from table2 t
                where t.%s=1
                group by userid,metric_key ) a )
            group by userid)

        select %s,%s from model_data_mta
       
        ;''' % (ranked_dims_mkey,
                                            ranked_dims_str,
                                            kpi_col,
                                            tokenized_table_name,
                                            tactic_id,
                                            user_seg_col,
                                            user_seg,
                                            user_subset_table,
                                            kpi_col,
                                            metric_key_sql_string2,
                                            kpi_col,
                                            metric_key_sql_string,
                                            kpi_col,
                                            metric_key_sql_string5,
                                            kpi_col), conn, chunksize=5000)
    else:
       
        
        model_data = pd.read_sql('''with tokenized_slice as
        (select userid, vtimestamp, %s as metric_key, %s, %s from %s where m0=%s and metric_key is not null and %s=%s and userid in (select userid from %s)),
        
        model_data_mta as
            (select userid,
            %s 1 as %s
            from (select userid,
                    %s
                    from
                        (select userid,
                            metric_key,
                            count(*) as sum_test
                            from tokenized_slice t
                            where t.%s=1
                            group by userid,metric_key) a
                    ) group by userid)
                            
        select %s, %s from model_data_mta
       ;''' % (ranked_dims_mkey,
                                            ranked_dims_str,
                                            kpi_col,
                                            tokenized_table_name,
                                            tactic_id,
                                            user_seg_col,
                                            user_seg,
                                            user_subset_table,
                                            metric_key_sql_string2,
                                            kpi_col,
                                            metric_key_sql_string,
                                            kpi_col,
                                            metric_key_sql_string5,
                                            kpi_col), conn, chunksize=5000)
    model_data = pd.concat(model_data).reset_index(drop=True)
    print('conversion user count',str(len(model_data)))
    conversion_metric_averages = pd.read_sql('''with tokenized_slice as
        (select userid, vtimestamp, %s as metric_key, %s, %s from %s where m0=%s and metric_key is not null and %s=%s and userid in (select userid from %s)),

        model_data_mta as
            (select userid,
            %s 1 as %s
            from (select userid,
                %s
                from
                (select userid,
                        metric_key,
                        count(*) as sum_test
                        from tokenized_slice t
                        where t.%s=1
                        group by userid,metric_key) a
                ) group by userid)
        
        select %s from model_data_mta;''' % (
                            ranked_dims_mkey,
                            ranked_dims_str,
                            kpi_col,
                            tokenized_table_name,
                            tactic_id,
                            user_seg_col,
                            user_seg,
                            user_subset_table,
                            metric_key_sql_string2,
                            kpi_col,
                            metric_key_sql_string,
                            kpi_col,
                            metric_key_sql_string3), conn)

    conversion_metric_averages = conversion_metric_averages.iloc[0, :]

    if c.closed:
        conn = psycopg2.connect(conn_string)
        c = conn.cursor()
    
#     sample_size=len(model_data)*100
 

    # non-conversion data
    if timediscount == 1 and mta_partial_seg==[1]:          # run MTA wth partial user segment info
        non_conversion_model_data = pd.read_sql('''with tokenized_slice as
        (select userid, vtimestamp, %s as metric_key, %s, %s from %s where m0=%s and metric_key is not null and userid in (select userid from %s)),

        table1 as
            (select userid, vtimestamp as conversion_date from
                (select *, row_number() over (partition by userid order by vtimestamp desc) as rk from tokenized_slice) where rk=1 and %s=0),
        
        table2 as
            (select a.*, floor(datediff(mins,vtimestamp,conversion_date)/10080) as wk_from_conversion_weight 
            from tokenized_slice a join table1 on a.userid=table1.userid),
        
        non_conversion_model_data_mta as
            (select userid,%s 0 as %s
            from
                (select userid, %s
                from
                    (select userid,
                        metric_key,
                        sum(1/((1+0.5)^wk_from_conversion_weight)) as sum_test
                    from table2
                    where metric_key in (select distinct metric_key from tokenized_slice where %s=1) 
                        and %s=0
                    group by userid,metric_key) a )
                    group by userid)

        select %s, %s from non_conversion_model_data_mta
        ;''' % (ranked_dims_mkey,
                        ranked_dims_str,
                        kpi_col,
                        tokenized_table_name,
                        tactic_id,
                        user_subset_table,
                        kpi_col,
                        metric_key_sql_string2,
                        kpi_col,
                        metric_key_sql_string,
                        kpi_col,
                        kpi_col,
                        metric_key_sql_string5,
                        kpi_col), conn, chunksize=5000)

    elif timediscount == 1 and mta_partial_seg!=[1]:            # run MTA with fully available user segment info or not using user segment info
        non_conversion_model_data = pd.read_sql('''with tokenized_slice as
        (select userid, vtimestamp, %s as metric_key, %s, %s from %s where m0=%s and metric_key is not null and %s=%s and userid in (select userid from %s)),

        table1 as
            (select userid, vtimestamp as conversion_date from
                (select *, row_number() over (partition by userid order by vtimestamp desc) as rk from tokenized_slice) where rk=1 and %s=0),
        
        table2 as
            (select a.*, floor(datediff(mins,vtimestamp,conversion_date)/10080) as wk_from_conversion_weight 
            from tokenized_slice a join table1 on a.userid=table1.userid),
        
        non_conversion_model_data_mta as
            (select userid,%s 0 as %s
            from
                (select userid, %s
                from
                    (select userid,
                        metric_key,
                        sum(1/((1+0.5)^wk_from_conversion_weight)) as sum_test
                    from table2
                    where metric_key in (select distinct metric_key from tokenized_slice where %s=1) 
                        and %s=0
                    group by userid,metric_key) a )
                    group by userid)

        select %s, %s from non_conversion_model_data_mta
       ;''' % (ranked_dims_mkey,
                        ranked_dims_str,
                        kpi_col,
                        tokenized_table_name,
                        tactic_id,
                        user_seg_col,
                        user_seg,
                        user_subset_table,
                        kpi_col,
                        metric_key_sql_string2,
                        kpi_col,
                        metric_key_sql_string,
                        kpi_col,
                        kpi_col,
                        metric_key_sql_string5,
                        kpi_col), conn, chunksize=5000)

    elif timediscount == 0 and mta_partial_seg==[1]:            # run MTA wth partial user segment info
        non_conversion_model_data = pd.read_sql('''with tokenized_slice as
        (select userid, vtimestamp, %s as metric_key, %s, %s from %s where m0=%s and metric_key is not null and userid in (select userid from %s)),
        
        non_conversion_model_data_mta as
            (select userid,
            %s 0 as %s
            from (select userid, %s from
                (select userid,
                        metric_key,
                        count(*) as sum_test
                from tokenized_slice
                where metric_key in (select distinct metric_key from tokenized_slice where %s=1)
                        and %s=0
                group by userid,metric_key) a)
            group by userid)

        select %s, %s from non_conversion_model_data_mta
       ;''' % (ranked_dims_mkey,
                        ranked_dims_str,
                        kpi_col,
                        tokenized_table_name,
                        tactic_id,
                        user_subset_table,
                        metric_key_sql_string2,
                        kpi_col,
                        metric_key_sql_string,
                        kpi_col,
                        kpi_col,
                        metric_key_sql_string5,
                        kpi_col), conn, chunksize=5000)
    
    elif timediscount==0 and mta_partial_seg!=[1]:          # run MTA with fully available user segment info or not using user segment info
        non_conversion_model_data = pd.read_sql('''with tokenized_slice as
        (select userid, vtimestamp, %s as metric_key, %s, %s from %s where m0=%s and metric_key is not null and %s=%s and userid in (select userid from %s)),
        
        non_conversion_model_data_mta as
            (select userid,
            %s 0 as %s
            from (select userid, %s from
                (select userid,
                        metric_key,
                        count(*) as sum_test
                from tokenized_slice
                where metric_key in (select distinct metric_key from tokenized_slice where %s=1)
                        and %s=0
                group by userid,metric_key) a)
            group by userid)

        select %s, %s from non_conversion_model_data_mta
      ;''' % (ranked_dims_mkey,
                        ranked_dims_str,
                        kpi_col,
                        tokenized_table_name,
                        tactic_id,
                        user_seg_col,
                        user_seg,
                        user_subset_table,
                        metric_key_sql_string2,
                        kpi_col,
                        metric_key_sql_string,
                        kpi_col,
                        kpi_col,
                        metric_key_sql_string5,
                        kpi_col), conn, chunksize=5000)
    non_conversion_model_data = pd.concat(non_conversion_model_data).reset_index(drop=True)
    print('non-conversion user count',str(len(non_conversion_model_data)))
#     print('non-conversion should be count',str(sample_size))
    # if user only has partial customer segment info but want to run MTA the proper way
    if mta_partial_seg!=[1] and non_conversion_model_data.shape[0]==0:
        mta_partial_seg_flag = 1
    else:
        mta_partial_seg_flag = 0

    if c.closed:
        conn = psycopg2.connect(conn_string)
        c = conn.cursor()

    logging.info("> Processing and loading data used {} secs!".format(int(time.clock() - start_time)))

    start_time_lastclick = time.clock()
    last_click = pd.read_sql('''with tokenized_slice as
        (select userid, vtimestamp, %s as metric_key, %s, %s from %s where m0=%s and metric_key is not null and %s=%s and userid in (select userid from %s)),
    
        converted as
            (select * from tokenized_slice
                    where %s=1),
        
        last_imp as
            (select userid,
                    max(vtimestamp) as max_previous
            from converted
            group by userid)

        select metric_key, count(*) as lastclick_conv_contri
        from converted b
        inner join last_imp a
        on b.userid=a.userid and b.vtimestamp=a.max_previous
        group by metric_key
        order by metric_key;''' % (ranked_dims_mkey,
                                    ranked_dims_str,
                                    kpi_col,
                                    tokenized_table_name,
                                    tactic_id,
                                    user_seg_col,
                                    user_seg,
                                    user_subset_table,
                                    kpi_col), conn, chunksize=5000)
    last_click = pd.concat(last_click).reset_index(drop=True)
    logging.info("> Processing and loading data for last click used {} secs!".format(int(time.clock() - start_time_lastclick)))

    # extract the column names (m1,m2,m3...) of raw tokenized table
    # tokenized_cols=pd.read_sql("select column_name from information_schema.columns where table_name='{}' and column_name like 'm%' and column_name not in ('m0','metric_key');".format(tokenized_table_name), conn)['column_name'].tolist()
    # information_schema.columns may not contain tokenized_table_name!
    tokenized_table_one_row=pd.read_sql("select * from {} limit 1;".format(tokenized_table_name), conn)
    tokenized_cols=[x for x in tokenized_table_one_row.columns.tolist() if x.startswith("m") and x not in ['m0','metric_key']]
        
    c.close()
    conn.close()

    # rename kpi column to cm1 no matter which kpi it is
    model_data.rename(columns={kpi_col: "cm1"},inplace=True)
    non_conversion_model_data.rename(columns={kpi_col: "cm1"},inplace=True)

    # To avoid column mismatch. Need second thoughts on this
    m = non_conversion_model_data.shape[1]
    if m < model_data.shape[1]:
        n = non_conversion_model_data.shape[0]
        non_conversion_model_data = non_conversion_model_data.append(
            model_data.head(1)).fillna(value=0)
        non_conversion_model_data = non_conversion_model_data.head(n)
    
    logging.error("> model_data's shape: {}".format(model_data.shape) )
    logging.error("> model_data's memory usage: {} bytes".format(sys.getsizeof(model_data)) )
    logging.error("> non_conversion_model_data's shape: {}".format(non_conversion_model_data.shape) )
    logging.error("> non_conversion_model_data's memory usage: {} bytes".format(sys.getsizeof(non_conversion_model_data) ) )
    logging.error("> last_click's shape: {}".format(last_click.shape) )
    logging.error("> last_click's memory usage: {} bytes".format(sys.getsizeof(last_click) ) )
    
    return model_data, non_conversion_model_data,conversion_metric_averages, last_click, tokenized_cols, mta_partial_seg_flag
    # , data_processing_time


def model_iteration(tokenized_table_name,
                    tactic_id,
                    ranked_dims,
                    conversion_metric_averages,
                    model_data,
                    non_conversion_model_data,
                    num_iterations=3,  # no need to show on MTA UI
                    quant=0.3):  # no need to show on MTA UI

    # create benchmark by using original data
    start_time = time.clock()
    model_data_all_users = model_data.append(
        non_conversion_model_data).fillna(value=0)
    logitbenchmark = linear_model.LogisticRegression(C=1.0)
    m = model_data_all_users.shape[1]

    # logitbenchmark.fit(
    #     model_data_all_users.iloc[:, 1:m - 1], np.ravel(model_data_all_users.iloc[:, -1]))
    logitbenchmark.fit(
        model_data_all_users.iloc[:, :m - 1], np.ravel(model_data_all_users.iloc[:, -1]))

    benchmark_model_coefs = logitbenchmark.coef_

    # predbenchmark = logitbenchmark.predict_proba(
    #     model_data_all_users.iloc[:, 1:m - 1])
    predbenchmark = logitbenchmark.predict_proba(
        model_data_all_users.iloc[:, :m - 1])

    actualbenchmark = model_data_all_users.iloc[:, m - 1:]

    # Select similar non-converted users and add them into model_data
    m = non_conversion_model_data.shape[1]

    # non_conversion_model_data_no_userids = non_conversion_model_data.iloc[:, 1:m - 1]
    non_conversion_model_data_no_userids = non_conversion_model_data.iloc[:, :m - 1]

    n = non_conversion_model_data_no_userids.shape[0]
    m = non_conversion_model_data_no_userids.shape[1]
    # need second thought on this
    avgs = non_conversion_model_data_no_userids <= conversion_metric_averages
    similar_non_converted = non_conversion_model_data[avgs.sum(
        axis=1) == m].reset_index(drop=True)

    # model_potential_data = non_conversion_model_data[~(non_conversion_model_data['userid'].astype(
    #     str)).isin(similar_non_converted['userid'].astype(str))].reset_index(drop=True)
    similar_non_converted_index=non_conversion_model_data.index[avgs.sum(axis=1) == m].tolist()
    model_potential_data=non_conversion_model_data.loc[~non_conversion_model_data.index.isin(similar_non_converted_index)].reset_index(drop=True)  

    logit = {}
    logit['1'] = linear_model.LogisticRegression(C=1.0)
    X_Y_model_data = {}
    X_Y_model_data['1'] = model_data.append(
        similar_non_converted).fillna(value=0)
    m = X_Y_model_data['1'].shape[1]

    # logit['1'].fit(X_Y_model_data['1'].iloc[:, 1:m - 1],
    #                np.ravel(X_Y_model_data['1'].iloc[:, -1]))
    logit['1'].fit(X_Y_model_data['1'].iloc[:, :m - 1],
        np.ravel(X_Y_model_data['1'].iloc[:, -1]))

    X_Y_model_potential = {}
    X_Y_model_potential['1'] = model_potential_data

    # pred = logit['1'].predict_proba(
    #     model_data.iloc[:, 1:m - 1])  # predict on converted users
    pred = logit['1'].predict_proba(
        model_data.iloc[:, :m - 1])  # predict on converted users

    pred = pd.Series(pd.DataFrame(pred)[1])
    z = pred.quantile(q=quant)  # this can change depending on the data and
    # how much is brings in. z is the prob. score within the converted. If we
    # get more negatives this time, decrease the q value

    new_pred = {}
    new_XY_to_add = {}
    coefs_from_models = {}
    coefs_from_models['1'] = logit['1'].coef_
    total_users_by_iteration = []
    total_users_by_iteration.append(len(X_Y_model_data['1']))

    for i in range(1, num_iterations):
        # new_pred[str(i)] = logit[str(i)].predict_proba(
        #     X_Y_model_potential[str(i)].iloc[:, 1:m - 1])
        new_pred[str(i)] = logit[str(i)].predict_proba(
            X_Y_model_potential[str(i)].iloc[:, :m - 1])

        new_pred[str(i)] = pd.Series(pd.DataFrame(
            new_pred[str(i)])[1])  # transform to series
        new_XY_to_add[str(i)] = X_Y_model_potential[str(
            i)][new_pred[str(i)] < z].reset_index(drop=True)
        X_Y_model_potential[str(i + 1)] = X_Y_model_potential[str(i)
                                                              ][new_pred[str(i)] >= z].reset_index(drop=True)
        X_Y_model_data[str(i + 1)] = X_Y_model_data[str(i)
                                                    ].append(new_XY_to_add[str(i)]).fillna(value=0)
        logit[str(i + 1)] = linear_model.LogisticRegression(C=1.0)

        # logit[str(i + 1)].fit(X_Y_model_data[str(i + 1)].iloc[:, 1:m - 1],
        #                       np.ravel(X_Y_model_data[str(i + 1)].iloc[:, -1]))
        logit[str(i + 1)].fit(X_Y_model_data[str(i + 1)].iloc[:, :m - 1],
                              np.ravel(X_Y_model_data[str(i + 1)].iloc[:, -1]))

        coefs_from_models[str(i + 1)] = logit[str(i + 1)].coef_
        total_users_by_iteration.append(len(X_Y_model_data[str(i + 1)]))
        if len(X_Y_model_data[str(i + 1)]) - len(X_Y_model_data[str(i)]) <= 50:
            break

    m = model_data_all_users.shape[1]

    # all_users_x = model_data_all_users.iloc[:, 1:m - 1]
    all_users_x = model_data_all_users.iloc[:, :m - 1]

    all_users_y = model_data_all_users.iloc[:, -1]
    # we define conversation rate as converted users that have seen the metric / total user that have seen the metric
    # if the denominator is number of times people have seen this metric, then
    # we won't convert values > 0 into 1. Similar for numerator.
    all_users_x[all_users_x > 0] = 1
    all_users_sums = pd.Series(all_users_x.sum(axis=0)).reset_index(drop=True)

    m = model_data.shape[1]

    # converted_x = model_data.iloc[:, 1:m - 1]
    converted_x = model_data.iloc[:, :m - 1]

    converted_y = model_data.iloc[:, -1]

    converted_x[converted_x > 0] = 1
    converted_users_sums = pd.Series(
        converted_x.sum(
            axis=0)).reset_index(
        drop=True)
    conversion_rate = converted_users_sums / all_users_sums

    metric_key_lookup = pd.DataFrame()
    metric_key_lookup['metric_key_db'] = converted_x.columns
    metric_key_lookup['metric_key'] = metric_key_lookup['metric_key_db'].map(
        lambda x: x.lstrip('m_'))
    metrics = [x.split('_') for x in metric_key_lookup['metric_key']]

    level = 0
    for i in range(0, len(metrics[0])):
        li = 0
        mi = [item[i] for item in metrics]
        for j in range(1, len(metrics)):
            li = li + int(mi[j])
        if li > 0:
            level = level + 1

    metric_key_conversion_rate = pd.DataFrame()
    metric_key_conversion_rate['metric_key'] = metric_key_lookup['metric_key']

    # for i in range(1, level + 1):
    #     metric_key_conversion_rate['m' +
    #                                str(i)] = [item[i - 1] for item in metrics]

    for i in ranked_dims:
        metric_key_conversion_rate[i] = [item[ranked_dims.index(i)] for item in metrics]

    metric_key_conversion_rate['conversion_rate'] = conversion_rate
    metric_key_conversion_rate['converted_users'] = converted_users_sums
    metric_key_conversion_rate['seen_users'] = all_users_sums
    mkey_lookup_all = {}

    decile_captured = {}
    top10perctile_captured = {}
    n = len(logit.keys())
    decile_index_values = []
    percentile_index_values = []
    top10percentile_index_values = []

    for i in range(1, n + 1):
        metric_key_conversion_rate_tmp = metric_key_conversion_rate
        metric_key_conversion_rate_tmp['coefs_from_model'] = pd.DataFrame(
            coefs_from_models[str(i)]).T
        # for j in range(1, level + 1):
        #     mj_coef = \
        #         metric_key_conversion_rate_tmp.loc[metric_key_conversion_rate_tmp['coefs_from_model'] > 0, :].groupby('m' + str(j),
        #                                                                                                               as_index=False)[
        #             'coefs_from_model'].min().rename(columns={'coefs_from_model': 'm' + str(j) + '_min_positive_coef'})
        #     metric_key_conversion_rate_tmp = pd.merge(
        #         metric_key_conversion_rate_tmp, mj_coef, on='m' + str(j), how='left')

        for j in ranked_dims:
            mj_coef = \
                metric_key_conversion_rate_tmp.loc[metric_key_conversion_rate_tmp['coefs_from_model'] > 0, :].groupby(j,
                                                                                                                    as_index=False)[
                    'coefs_from_model'].min().rename(columns={'coefs_from_model': j + '_min_positive_coef'})
            metric_key_conversion_rate_tmp = pd.merge(
                metric_key_conversion_rate_tmp, mj_coef, on=j, how='left')

        mkey_lookup = metric_key_conversion_rate_tmp
        mkey_lookup['final_coef'] = 0
        mkey_lookup.loc[mkey_lookup['coefs_from_model'] > 0,
                        'final_coef'] = mkey_lookup['coefs_from_model']

        # for h in range(level - 1, 0, -1):
        #     mkey_lookup.loc[mkey_lookup['final_coef'] == 0,
        #                     'final_coef'] = mkey_lookup['m' + str(h) + '_min_positive_coef'] * 0.1
        #     mkey_lookup['final_coef'].fillna(0, inplace=True)

        for h in range(level - 1, 0, -1):
            mkey_lookup.loc[mkey_lookup['final_coef'] == 0,
                            'final_coef'] = mkey_lookup[ranked_dims[h-1] + '_min_positive_coef'] * 0.1
            mkey_lookup['final_coef'].fillna(0, inplace=True)

        mkey_lookup.loc[mkey_lookup['final_coef'] == 0,
                        'final_coef'] = mkey_lookup['coefs_from_model'][mkey_lookup['coefs_from_model'] > 0].min() * 0.1

        mkey_lookup['intercept'] = float(logit[str(i)].intercept_)
        mkey_lookup_all[str(i)] = mkey_lookup

        coefs = np.matrix(mkey_lookup['final_coef'], dtype=np.float64)
        
        # e.g. if coefs is np.matrix([[0.001]] --> np.array(np.squeeze(np.asarray(coefs)))[np.newaxis] --> np.array([0.001]) --> will pose "IndexError: tuple index out of range" at next step
        # the expected output should be np.array([[0.001]]) when there is only one metric_key
        if coefs.shape[1]==1:
            logit[str(i)].coef_ = np.asarray(coefs)
        else:
            logit[str(i)].coef_ = np.array(
                np.squeeze(np.asarray(coefs)))[np.newaxis]

        # preds_after_imputation = logit[str(i)].predict_proba(
        #     X_Y_model_data[str(i)].iloc[:, 1:m - 1])
        preds_after_imputation = logit[str(i)].predict_proba(
            X_Y_model_data[str(i)].iloc[:, :m - 1])

        prob_afterimp = pd.DataFrame(preds_after_imputation.tolist())

        m = X_Y_model_data[str(i)].shape[1]
        YY = X_Y_model_data[str(i)].iloc[:, -1]
        YY.reset_index(drop=True, inplace=True)
        prob_afterimp['cm1'] = YY
        prob_afterimp.columns = ['prob_0', 'prob_1', 'cm1']
        prob_afterimp['decile'] = pd.qcut(
            prob_afterimp['prob_1'] + 0.0000000000001 * (np.random.rand(len(prob_afterimp)) - 0.5), 10,
            labels=np.arange(1, 11, 1))

        dec_afterimp = prob_afterimp[['cm1', 'decile']].groupby(
            ['decile']).agg(['sum'])
        afterimp_dec_val = dec_afterimp / prob_afterimp[['cm1']].sum()[0] * 100

        # when some bin edges are identical, pd.qcut would fail. https://stackoverflow.com/questions/20158597/how-to-qcut-with-non-unique-bin-edges
        # This fixes the issue but you might have that identical (pre-ranking) values go into different quantiles, which can be correct or not depending on your intent.
        try:
            prob_afterimp['perc'] = pd.qcut(
                prob_afterimp['prob_1'] + 0.0000000000001 * (np.random.rand(len(prob_afterimp)) - 0.5), 100,
                labels=np.arange(1, 101, 1))
        except:
            tmp_df=prob_afterimp.copy()
            tmp_df['rank'] = tmp_df['prob_1'].rank(method='first')
            prob_afterimp['perc'] = pd.qcut(tmp_df['rank'].values, 100, labels=np.arange(1, 101, 1)).codes
            logging.error("Some bin edges are identical. You might have identical (pre-ranking) values go into different quantiles.")

        # prob_afterimp['perc'] = pd.qcut(
        #     prob_afterimp['prob_1'] + 0.0000000000001 * (np.random.rand(len(prob_afterimp)) - 0.5), 100,
        #     labels=np.arange(1, 101, 1))

        perc_afterimp = prob_afterimp[['cm1', 'perc']].groupby(['perc']).agg([
            'sum'])
        afterimp_perc_val = perc_afterimp / \
            prob_afterimp[['cm1']].sum()[0] * 100

        # Decile Index
        n1 = 10
        decile_index = sum(
            afterimp_dec_val['cm1']['sum'] * list(range(1, n1 + 1))) / 10
        decile_index_values.append(decile_index)
        decile_captured[str(i)] = afterimp_dec_val['cm1']['sum']
        # percentile index
        n2 = 100
        perc_index = sum(
            afterimp_perc_val['cm1']['sum'] * list(range(1, n2 + 1))) / 100

        percentile_index_values.append(perc_index)
        top10perctile_captured[str(i)] = afterimp_perc_val['cm1']['sum'][90:, ]
        n3 = 10

        afterimp_perc_val_top10 = afterimp_perc_val['cm1']['sum'][90:, ]
        top10perc_index = sum(afterimp_perc_val_top10 * list(range(1, n3 + 1)))
        top10percentile_index_values.append(top10perc_index)

    index_com = [
        d + p for d,
        p in zip(
            decile_index_values,
            percentile_index_values)]
    best_model = index_com.index(max(index_com)) + 1

    logging.info("> {} model iterations used {} secs!".format(
        len(logit.keys()), int(time.clock() - start_time)))

    
    start_time_savemodelinfo = time.clock()
    # Output model iteration results as backup
    total_users_by_iteration_df = pd.DataFrame(
        total_users_by_iteration).rename(columns={0: "Total_Users"})
    decile_captured_df = pd.DataFrame(decile_captured)
    top10perctile_captured_df = pd.DataFrame(top10perctile_captured)
    decile_index_df = pd.DataFrame(decile_index_values).rename(
        columns={0: "decile_index"})
    percentile_index_df = pd.DataFrame(percentile_index_values).rename(
        columns={0: "percentile_index"})

    output_excel_path = r'S:\MTA_Model_Output\{}_{}.xlsx'.format(
        tokenized_table_name, strftime("%Y-%m-%d", gmtime()))

    writer = pd.ExcelWriter(output_excel_path, engine='xlsxwriter')

    n_r = 0
    txt_msg = pd.DataFrame()
    txt_msg.at[0, 'best_model'] = best_model
    txt_msg.to_excel(
        writer,
        sheet_name='CoefwoImputation',
        startrow=n_r + 1,
        index=False)

    n_r = n_r + 4
    Iterationidex = list(range(1, len(logit.keys()) + 1, 1))
    negativecoef = [sum(mkey_lookup_all[str(i)]['coefs_from_model'] < 0) /
                    len(mkey_lookup_all[str(i)]['coefs_from_model']) for i in Iterationidex]
    Iterationidex.append("benchmark")
    negativecoef.append(
        sum(benchmark_model_coefs[0] < 0) / len(benchmark_model_coefs[0]))
    d = {
        'Iterations': pd.Series(
            Iterationidex,
            index=Iterationidex),
        'Negative Coefficient from Model': pd.Series(
            negativecoef,
            index=Iterationidex)}

    negativecoefdf = pd.DataFrame(d)
    negativecoefdf.to_excel(
        writer,
        sheet_name='CoefwoImputation',
        startrow=n_r,
        index=False)

    n_r = n_r + 4 + len(logit.keys())
    worksheet = writer.sheets['CoefwoImputation']
    text = 'Model coefficient of each iteration'
    worksheet.write(n_r + 1, 0, text)
    n_r = n_r + 1
    for i in range(1, len(logit.keys()) + 1):
        mkey_lookup_all[str(i)].to_excel(
            writer, sheet_name='CoefwoImputation', startrow=n_r + 1, index=False)
        n_r = n_r + len(mkey_lookup_all[str(i)]) + 2

    performance = pd.concat(
        [total_users_by_iteration_df, decile_index_df, percentile_index_df], axis=1)
    performance['validation_metric'] = (
        performance['decile_index'] + performance['percentile_index']) / 2
    performance['Iteration'] = list(range(1, len(logit.keys()) + 1, 1))

    performance.to_excel(
        writer,
        sheet_name='Model Validation Info',
        startrow=3,
        index=False)
    worksheet = writer.sheets['Model Validation Info']
    text = 'Model validation metrics as each iteration'
    worksheet.write(1, 0, text)
    decile_captured_df.to_excel(
        writer,
        sheet_name='Model Validation Info',
        startrow=len(performance) + 5,
        index=True)
    top10perctile_captured_df.to_excel(
        writer,
        sheet_name='Model Validation Info',
        startrow=len(performance) +
        len(decile_captured_df) +
        7,
        index=True)

    writer.save()
    best_model_vadidation_metric = performance.loc[performance.Iteration ==
                                                   best_model, 'validation_metric'].round(1)
    logging.info("> Saving all model info used {} secs!".format( int(time.clock() - start_time_savemodelinfo))) 
    return X_Y_model_data[str(best_model)], mkey_lookup_all[str(best_model)], logit[str(
        best_model)], level, len(logit.keys()), best_model_vadidation_metric
        # , model_iteration_time


def decomp(
        last_click,
        level,
        ranked_dims,
        best_model_data,
        best_mkey_lookup_all,
        best_model,
        valid_num_iteration,
        best_mode_validation_metric,
        tactic_id,                                                           # mark tactic for decomp
        user_seg_col,
        user_seg,
        use_user_info,
        rankorder,                                          
        digital_incremental_volume,
        spend,
        margin):
    
    start_time = time.clock()
    # xy_convertion_best = best_model_data.iloc[:, 1:]
    start_time_prepdecomp = time.clock()
    xy_convertion_best = best_model_data

    xy_convertion_best[xy_convertion_best > 0] = 1
    seen_users_sums_best = pd.Series(
        xy_convertion_best.iloc[:, :-1].sum(axis=0)).reset_index(drop=True)
    converted_users = pd.Series(
        xy_convertion_best.iloc[:, :-1][xy_convertion_best['cm1'] > 0].sum(axis=0)).reset_index(drop=True)

    total_converted_users = xy_convertion_best['cm1'].sum(axis=0)

    level_string = ''
    # for i in range(1, level + 1):
    #     level_string = level_string + 'm' + \
    #         str(i) + ','  
    for i in ranked_dims:
        level_string = level_string + i + ','

    # Option1:backward

    # cols1 = list(pd.DataFrame(
    #     best_mkey_lookup_all).loc[:, 'metric_key':'m' + str(level)]) + ['final_coef']

    cols1 = list(pd.DataFrame(
        best_mkey_lookup_all).loc[:, 'metric_key': ranked_dims[level-1]]) + ['final_coef']
    
    logging.info("> Preparing data for decomp used {} secs!".format( int(time.clock() - start_time_prepdecomp))) 
    
#     start_time_backward = time.clock()
    decomp1 = pd.DataFrame(best_mkey_lookup_all)[cols1]
    m = best_model_data.shape[1]

    # decomp1['imps in the best model'] = best_model_data.iloc[:,
    #                                                          1:m - 1].sum(axis=0).tolist()
    decomp1['imps in the best model'] = best_model_data.iloc[:,
                                                            :m - 1].sum(axis=0).tolist()

    decomp1['avg imps in the best model'] = decomp1['imps in the best model'] / \
        len(best_model_data)
    decomp1['seen_users_sums_best'] = seen_users_sums_best
    decomp1['converted_users'] = converted_users

    average_imp = decomp1['avg imps in the best model']
    average_imp_matrix = pd.concat(
        [average_imp] * len(average_imp.index), axis=1)
    np.fill_diagonal(average_imp_matrix.values, 0)
    # average_imp_matrix = np.matrix(average_imp_matrix, dtype = np.float64)
    standard = np.matrix(average_imp).transpose()
    intercept = np.matrix(best_model.intercept_, dtype=np.float64)
    coefs = np.matrix(best_model.coef_, dtype=np.float64)
    preds_avg_imp_wo_single_metric = np.exp(intercept + np.dot(coefs, average_imp_matrix)) / (
        1 + np.exp(intercept + np.dot(coefs, average_imp_matrix)))
    preds_avg_imp = np.exp(intercept + np.dot(coefs, standard)) / \
        (1 + np.exp(intercept + np.dot(coefs, standard)))

    decomp1['preds_avg_imp'] = float(preds_avg_imp)
    decomp1['preds_avg_imp_wo_single_metric'] = preds_avg_imp_wo_single_metric.transpose()
    decomp1['incrementality_backward'] = preds_avg_imp - \
        preds_avg_imp_wo_single_metric.transpose()
    decomp1['spend'] = spend
    decomp1['margin'] = margin
    decomp1['Model Data Incremental Conversion'] = total_converted_users * \
        decomp1['incrementality_backward'] / sum(decomp1['incrementality_backward'])

    decomp1.loc[:,
                'Incremental_Conversion'] = digital_incremental_volume * decomp1.loc[:,
                                                                                     'incrementality_backward'] / decomp1.loc[:,
                                                                                                                              'incrementality_backward'].sum()
    decomp1['ROI'] = margin * \
        decomp1['Incremental_Conversion'] / decomp1['spend']
    decomp1['methodology'] = 'Backward Decomp'
#     logging.info("> Decomp - Backward used {} secs!".format( int(time.clock() - start_time_backward)))
    # Option2:forward
#     start_time_forward = time.clock()
    decomp2 = pd.DataFrame(best_mkey_lookup_all)[cols1]
    decomp2['imps in the best model'] = decomp1['imps in the best model']
    decomp2['avg imps in the best model'] = decomp1['avg imps in the best model']
    decomp2['seen_users_sums_best'] = seen_users_sums_best
    decomp2['converted_users'] = converted_users

    preds_intercept_only = np.exp(intercept) / (1 + np.exp(intercept))
    single_metric_matrix = np.matlib.zeros(
        (average_imp_matrix.shape[0], average_imp_matrix.shape[1]))
    np.fill_diagonal(single_metric_matrix, average_imp)
    preds_one_metric_with_intercept = np.exp(intercept + np.dot(coefs, single_metric_matrix)) / (
        1 + np.exp(intercept + np.dot(coefs, single_metric_matrix)))
    decomp2['preds_intercept_only'] = float(preds_intercept_only)
    decomp2['preds_one_metric_with_intercept'] = preds_one_metric_with_intercept.transpose()
    decomp2['incrementality_forward'] = decomp2['preds_one_metric_with_intercept'] - \
        decomp2['preds_intercept_only']
    decomp2['spend'] = spend
    decomp2['margin'] = margin
    decomp2['Model Data Incremental Conversion'] = total_converted_users * \
        decomp2['incrementality_forward'] / sum(decomp2['incrementality_forward'])

    decomp2['Incremental_Conversion'] = digital_incremental_volume * \
        decomp2['incrementality_forward'] / decomp2['incrementality_forward'].sum()
    decomp2['ROI'] = margin * \
        decomp2['Incremental_Conversion'] / decomp2['spend']
    decomp2['methodology'] = 'Forward Decomp'
#     logging.info("> Decomp - Forward used {} secs!".format( int(time.clock() - start_time_forward))) 
    # Op3:game theory
    start_time_gametheory = time.clock()
    mkey_cnt = len(best_mkey_lookup_all['metric_key'])
#     if mkey_cnt <= 20:
#         def ncr(n, r):
#             r = min(r, n - r)
#             if r == 0:
#                 return 1
#             numer = ft.reduce(op.mul, range(n, n - r, -1))
#             denom = ft.reduce(op.mul, range(1, r + 1))
#             return numer // denom

#         combination_cnt = 0
#         for i in range(0, mkey_cnt + 1):
#             combination_cnt = combination_cnt + ncr(mkey_cnt, i)

#         mkey = best_mkey_lookup_all.loc[:, 'metric_key'].tolist()
#         GT1 = pd.DataFrame(np.matlib.zeros((1, len(mkey))))
#         GT1.columns = mkey
#         GT1.iloc[0, :] = average_imp.tolist()
#         GT2 = pd.DataFrame(np.matlib.zeros((combination_cnt, len(mkey))))
#         GT2.columns = mkey
#         com_cnt_temp = combination_cnt

#         for i in range(0, len(mkey)):
#             com_cnt_temp = com_cnt_temp / 2
#             buck_cnt = int(combination_cnt / com_cnt_temp)
#             for j in range(0, buck_cnt, 2):
#                 GT2.loc[j *
#                         com_cnt_temp:(j +
#                                       1) *
#                         com_cnt_temp -
#                         1, mkey[i]] = GT1.loc[0, mkey[i]]

#         GT_prob = pd.DataFrame(best_model.predict_proba(GT2).tolist())
#         GT_prob.columns = ['prob_0', 'prob_1']
#         GT2['prob_1'] = GT_prob['prob_1']
#         ROI_r = []
#         for i in range(0, len(mkey)):
#             mk1 = GT2.loc[GT2.iloc[:, i] > 0].drop(GT2.columns[i], axis=1)
#             mk0 = GT2.loc[GT2.iloc[:, i] == 0].drop(GT2.columns[i], axis=1)
#             mk1 = mk1.rename(columns={'prob_1': 'mkey1_prob'})
#             mk0 = mk0.rename(columns={'prob_1': 'mkey0_prob'})
#             mg_df = pd.merge(mk1, mk0, how='inner',
#                              on=mk1.columns[0:len(mkey) - 1].tolist())           
#             ROI_r.append((mg_df['mkey1_prob'] - mg_df['mkey0_prob']).mean())
#         decomp3 = decomp1[cols1]
#         decomp3.loc[:, 'incrementality_GameTheory'] = ROI_r
#         decomp3.loc[:, 'spend'] = decomp1['spend']
#         decomp3.loc[:, 'margin'] = decomp1['margin']
#         decomp3.loc[:, 'seen_users_sums_best'] = seen_users_sums_best
#         decomp3.loc[:, 'converted_users'] = converted_users

#         decomp3.loc[:, 'Model Data Incremental Conversion'] = total_converted_users * \
#             decomp3['incrementality_GameTheory'] / sum(decomp3['incrementality_GameTheory'])
#         decomp3.loc[decomp3['Model Data Incremental Conversion'] >= decomp3.loc[:,
#                                                                                 'converted_users'], 'Model Data Incremental > converted'] = 'Yes'
#         decomp3.loc[decomp3['Model Data Incremental Conversion'] < decomp3[
#             'converted_users'], 'Model Data Incremental > converted'] = ' '
#         # scaling_factor3=total_converted_users*digital_incrementality/total_population/sum(decomp3['incrementality_GameTheory'])
#         # decomp3['ScalingFactor3_DigitalInc%s'%digital_incrementality]=scaling_factor3
#         decomp3.loc[:, 'Incremental_Conversion'] = digital_incremental_volume * \
#             decomp3['incrementality_GameTheory'] / sum(decomp3['incrementality_GameTheory'])
#         decomp3.loc[:, 'ROI'] = margin * \
#             decomp3['Incremental_Conversion'] / spend
#         decomp3.loc[:, 'methodology'] = 'Game Theory'

#     else:
    decomp3 = pd.DataFrame()

        
    logging.info("> Decomp - Game Theory used {} secs!".format( int(time.clock() - start_time_gametheory)))    
        
    # cols2 = list(pd.DataFrame(
    #     best_mkey_lookup_all).loc[:, 'metric_key':'m' + str(level)])

    cols2 = list(pd.DataFrame(
        best_mkey_lookup_all).loc[:, 'metric_key': ranked_dims[level-1] ])

    decomp4 = pd.DataFrame(best_mkey_lookup_all)[cols2]
    decomp4 = decomp4.merge(
        last_click,
        left_on=decomp4['metric_key'],
        right_on=last_click['metric_key'],
        how='left').fillna(
        value=0)
    decomp4 = decomp4.rename(columns={'metric_key_x': 'metric_key'})
    decomp4.drop(decomp4[['metric_key_y']], axis=1, inplace=True)
    decomp4['spend'] = spend
    decomp4['margin'] = margin
    decomp4['Incremental_Conversion'] = digital_incremental_volume * \
        decomp4['lastclick_conv_contri'] / decomp4['lastclick_conv_contri'].sum()
    decomp4.loc[:, 'converted_users'] = converted_users

    decomp4['ROI'] = margin * \
        decomp4['Incremental_Conversion'] / decomp4['spend']
    decomp4['methodology'] = 'Last Click'

    cols = cols2 + ['spend'] + ['margin'] +  ['converted_users'] + \
        ['Incremental_Conversion'] + ['ROI'] + ['methodology']



#     if mkey_cnt <= 20:
#         decomp_com = decomp1[cols].append(
#         decomp2[cols],
#         ignore_index=True).append(
#         decomp3[cols],
#         ignore_index=True).append(
#             decomp4[cols],
#         ignore_index=True)
#     else: 
    decomp_com = decomp1[cols].append(
    decomp2[cols],
    ignore_index=True).append(
        decomp4[cols],
    ignore_index=True)
    
    
    decomp_com['model_strength']=float(best_mode_validation_metric)
    decomp_com['total_conversions']=total_converted_users
    decomp_com['digital_incremental_volume']=digital_incremental_volume

    decomp_com['tactic_id']=tactic_id
    decomp_com['tactic_name']=rankorder.loc[rankorder['tactic_id']==tactic_id,"tactic"].drop_duplicates().values[0]

    # user segment
    if use_user_info==2:
        pass
    elif use_user_info==1:
        decomp_com[user_seg_col]=user_seg
    logging.info("> Decomp used {} secs!".format( int(time.clock() - start_time)))
    return decomp_com


def export_decomp_with_mkey_exp(
    # ranked_dims,
    tokenized_cols,
    conn_string,
    tokenized_table_name,  # use this table name to name output file
    tactic_id,
    user_seg_col,
    use_user_info,
    decomp_com,
    metric_key_explanation_table,
    appended_df
    # level
    ):

    conn = psycopg2.connect(conn_string)
    # for i in range(0, level):
    #     #string_temp = 'm%s' % (i + 1) + ', m%s' % (i + 1) + '_name'
    #     mkey_explanation = pd.read_sql(
    #         "select distinct * from %s where metric_id='%s';" %
    #         (metric_key_explanation_table,ranked_dims[i]), conn)

    #     mkey_explanation['dimension_id'] = mkey_explanation['dimension_id'].astype(str)
    #     mkey_explanation.rename(columns={'dimension_id': '%s' % (ranked_dims[i])}, inplace=True)
    #     mkey_explanation.rename(columns={'dimension_name': '%s_name' % mkey_explanation.loc[0,'metric_name']}, inplace=True)
    #     decomp_com['%s' % (ranked_dims[i])] = decomp_com['%s' % (ranked_dims[i])].astype(str)

    #     decomp_com = decomp_com.merge(
    #         mkey_explanation[[ranked_dims[i],'%s_name' % mkey_explanation.loc[0,'metric_name']]], on=ranked_dims[i], how='left')
    #     decomp_com['%s_name' % mkey_explanation.loc[0,'metric_name']]=decomp_com['%s_name' % mkey_explanation.loc[0,'metric_name']].fillna(value='unmatched')
    #     decomp_com.rename(columns={ranked_dims[i]: mkey_explanation.loc[0,'metric_name']}, inplace=True)

    if use_user_info==1:
        decomp_com_l=decomp_com.loc[:,['metric_key','tactic_id', 'tactic_name',user_seg_col]]
    elif use_user_info==2:
        decomp_com_l=decomp_com.loc[:,['metric_key','tactic_id', 'tactic_name']]


    decomp_com_m=pd.DataFrame(columns=tokenized_cols)                                   # m1,m2,m3,m4,m5...
    decomp_com_r=decomp_com.loc[:,['spend', 'margin','converted_users', 'Incremental_Conversion', 'ROI', 'methodology',
                                    'model_strength', 'total_conversions', 'digital_incremental_volume']]
    decomp_com_full=pd.concat([decomp_com_l,decomp_com_m,decomp_com_r],axis=1)
    decomp_com=decomp_com_full.merge(decomp_com,how="right").fillna(0)                  # allow original decomp_com to fill in values
    
    conn = psycopg2.connect(conn_string)
    mkey_explanation_table = pd.read_sql(
            "select distinct * from %s;" %
            (metric_key_explanation_table), conn)
    conn.close()

    for i in range(0, len(tokenized_cols)):
        #string_temp = 'm%s' % (i + 1) + ', m%s' % (i + 1) + '_name'
        # mkey_explanation = pd.read_sql(
        #     "select distinct * from %s where metric_id='%s';" %
        #     (metric_key_explanation_table,tokenized_cols[i]), conn)

        mkey_explanation = mkey_explanation_table.loc[mkey_explanation_table.metric_id == tokenized_cols[i]].reset_index(drop=True)
        dim_name = mkey_explanation['metric_name'].tolist()[0]

        mkey_explanation['dimension_id'] = mkey_explanation['dimension_id'].astype(str)
        mkey_explanation.rename(columns={'dimension_id': '%s' % (tokenized_cols[i])}, inplace=True)
        mkey_explanation.rename(columns={'dimension_name': '%s_name' % dim_name}, inplace=True)
        decomp_com['%s' % (tokenized_cols[i])] = decomp_com['%s' % (tokenized_cols[i])].astype(str)

        decomp_com = decomp_com.merge(
            mkey_explanation[[tokenized_cols[i],'%s_name' % dim_name]], on=tokenized_cols[i], how='left')
        decomp_com['%s_name' % dim_name]=decomp_com['%s_name' % dim_name].fillna(value='unmatched')
        decomp_com.rename(columns={tokenized_cols[i]: dim_name}, inplace=True)

    appended_df.append(decomp_com)

    # output_excel_path = "%s" % directory + tokenized_table_name + '_' + strftime("%Y-%m-%d", gmtime() ) + '.xlsx'
    # output to S3 instead of local directory
    # output_excel_path = "%s" % tokenized_table_name + '_' + strftime("%Y-%m-%d", gmtime() ) + '_' + str(int(time.time())) + '.xlsx'
    # print(output_excel_path)

    # writer = pd.ExcelWriter(output_excel_path, engine='xlsxwriter' )
    # decomp_com.to_excel(writer, sheet_name='Decomp Info', index=False )
    # writer.save()

    return appended_df






def upload_to_s3(filename,bucket,destination,aws_access_key_id,aws_secret_access_key):
    s3 = boto3.client('s3',
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key )
    s3.upload_file(filename, bucket, destination)
    print("upload successfully!")






"""
# merge output table with a list of metric_key explanation tables
def export_decomp_with_mkey_exp(directory,
                                conn_string,
                                tokenized_table_name,  # use this table name to name output file
                                decomp_com,
                                metric_key_explanation_table,
                                level
                                ):

    conn = psycopg2.connect(conn_string)
    for i in range(0, level):
        string_temp = 'm%s' % (i + 1) + ', m%s' % (i + 1) + '_name'
        mkey_explanation = pd.read_sql(
            "select distinct %s from %s;" %
            (string_temp, metric_key_explanation_table[i]), conn)
        mkey_explanation['m%s' %
                         (i + 1)] = mkey_explanation['m%s' %
                                                     (i + 1)].astype(str)
        decomp_com['m%s' % (i + 1)] = decomp_com['m%s' % (i + 1)].astype(str)
        decomp_com = decomp_com.merge(
            mkey_explanation, on='m{}'.format(
                i + 1), how='left')

    output_excel_path = '%s/' % directory + tokenized_table_name + \
        '_' + strftime("%Y-%m-%d", gmtime()) + '.xlsx'
    writer = pd.ExcelWriter(output_excel_path, engine='xlsxwriter')
    decomp_com.to_excel(writer, sheet_name='Decomp Info', index=False)
    writer.save()

    return output_excel_path
"""


# run MTA - multiple activities
def run_mta_multi(
    conn_string,
    tokenized_table_name,
    metric_key_explanation_table,
    dimension_ranking_table,
    kpi_col,
    user_seg_col,
    directory,
    use_user_info,
    mta_partial_seg,
    timediscount ):

    kpi_col = kpi_col.strip(" ")

    # before jump into for loop, map converted users first
    conn = psycopg2.connect(conn_string)
    c = conn.cursor()

    # when user has user segment info and the column exists, mapping converted users' segment info
    # if user_seg_col != 1:
    #     c.execute('''update %s 
    #                 set t1.%s = t2.%s
    #                 from %s t1
    #                 inner join
    #                 (select userid,%s from %s where metric_key is null or m0=0 group by userid) t2
    #                 on t1.userid=t2.userid;''' % (tokenized_table_name,user_seg_col,user_seg_col,tokenized_table_name,
    #                                             user_seg_col,tokenized_table_name))

    c.execute('''update %s set %s = 1
                where userid in
                (select distinct userid
                from %s
                where %s = 1);''' % (tokenized_table_name,kpi_col,tokenized_table_name,kpi_col))
    
    # multiple KPI
    # map converted users using userid but do not delete rows where metric_key is null
    # for one single user, cm1~cm5 might have single or multiple one(s)
    # for example, if we delete all rows where metric_key is null after we map conversions based on cm1
    # when we need to run MTA using cm2, we may fail to map conversions at the very beginning
    # since such information might have already beed deleted, meaning there could be no rows where cm2=1

    # c.execute(
    #     '''delete from %s where metric_key is null;''' %
    #     (tokenized_table_name) )    # need to check with CET on their final version sql code. They may set it to 0_0_0_0... for conversions.
    conn.commit()


    # read in rank order table
    rankorder = pd.read_sql("select * from %s;" % dimension_ranking_table,conn)
    tactic_ids = rankorder["tactic_id"].drop_duplicates().tolist()


    # read in the first row of tokenized table
    if user_seg_col == 1:
        user_segs=[1]
    else:
        user_seg_col=user_seg_col.strip(' ')
        user_segs=pd.read_sql("select distinct %s from %s where %s is not null;" %(user_seg_col,tokenized_table_name,
                                                                                    user_seg_col),conn)[user_seg_col].unique().tolist()   # make sure unique user segments don't contain NA

    # output filename indicates tokenized table name and kpi column name
    output_excel_path = "%s" % tokenized_table_name + '_' + kpi_col + '_' + strftime("%Y-%m-%d", gmtime() ) + '_' + str(int(time.time())) + '.xlsx'
    appended_df = []

    print(user_seg_col,user_segs,mta_partial_seg)

    for tactic_id in tactic_ids:
        for user_seg in user_segs:

            ranked_dims = rankorder.loc[(rankorder["tactic_id"]==tactic_id)].sort_values('rankorder',ascending=True)["metric_id"].tolist()
            ranked_dims_str = ', '.join(ranked_dims)
            ranked_dims_mkey = " || '_' || ".join(ranked_dims)

            metric_key_sql_df = available_mkey(conn_string,
                                                tactic_id,
                                                tokenized_table_name,
                                                ranked_dims_mkey,
                                                ranked_dims_str,
                                                kpi_col,
                                                user_seg_col,
                                                user_seg )
            
            if c.closed:
                conn = psycopg2.connect(conn_string)
                c = conn.cursor()

            mkey_explanation = pd.read_sql(
                "select distinct * from %s where metric_id='m0';" % (metric_key_explanation_table), conn)

            # if filtered segment has conversions
            if metric_key_sql_df.shape[0] == 0 and use_user_info==1:
                logging.info("> Activity: {} + User Segment: {} don't have conversions.".format(mkey_explanation.loc[(mkey_explanation["dimension_id"]==tactic_id),"dimension_name"].values[0], str(user_seg) ))
                continue
            elif metric_key_sql_df.shape[0] != 0 and use_user_info==1:
                logging.info("> Jump into Activity: {} + User Segment: {}.".format(mkey_explanation.loc[(mkey_explanation["dimension_id"]==tactic_id), "dimension_name"].values[0],
                                                                                    str(user_seg) ) )
            elif metric_key_sql_df.shape[0] == 0 and use_user_info==2:
                logging.info("> Activity: {} doesn't have conversions.".format(mkey_explanation.loc[(mkey_explanation["dimension_id"]==tactic_id),"dimension_name"].values[0] ))
                continue
            elif metric_key_sql_df.shape[0] != 0 and use_user_info==2:
                logging.info("> Jump into Activity: {}.".format(mkey_explanation.loc[(mkey_explanation["dimension_id"]==tactic_id), "dimension_name"].values[0] ) )



            try:
                model_data, non_conversion_model_data,conversion_metric_averages, last_click, tokenized_cols, mta_partial_seg_flag = database_data_processing(
                    metric_key_sql_df,
                    ranked_dims_mkey,
                    ranked_dims_str,
                    kpi_col,
                    tactic_id,
                    user_seg_col,
                    user_seg,
                    mta_partial_seg,
                    conn_string,
                    tokenized_table_name,
                    timediscount=1 )
                
                if mta_partial_seg_flag==1:
                    logging.info("> Activity: {} + User Segment: {} --> There are 0 rows in nonconversion data. If you only have user segment info for converted users, remember to check\
                                the box and run MTA with each converted segment against all non-converted users at your own risk!!")
                    continue

            except Exception as e:
                if use_user_info==2:
                    logging.info("> Activity: {} --> There is probably something wrong with data loading/cleaning process!".format(mkey_explanation.loc[(mkey_explanation["dimension_id"]==tactic_id),"dimension_name"].values[0] ) )
                    # logging.info("> {}: {}".format(sys.exc_info()[0], str(e)))                            
                    logging.error("{}: {}".format(sys.exc_info()[0], traceback.format_exc()))             # detailed traceback
                    continue
                elif use_user_info==1:
                    logging.info("> Activity: {} + User Segment: {} --> There is probably something wrong with data loading/cleaning process!".format(mkey_explanation.loc[(mkey_explanation["dimension_id"]==tactic_id),"dimension_name"].values[0],
                                                                                                                                                    str(user_seg) ) )
                    logging.error("{}: {}".format(sys.exc_info()[0], traceback.format_exc()))             
                    continue
                

            try:
                best_model_data, best_mkey_lookup_all, best_model,level, valid_num_iteration, best_mode_validation_metric = model_iteration(
                    tokenized_table_name,
                    tactic_id,
                    ranked_dims,
                    conversion_metric_averages,
                    model_data,
                    non_conversion_model_data,
                    num_iterations=3, # no need to show on MTA UI
                    quant=0.3 )
            except Exception as e:
                if use_user_info==2:
                    logging.info("> Activity: {} --> There is probably something wrong with model running process!".format(mkey_explanation.loc[(mkey_explanation["dimension_id"]==tactic_id),"dimension_name"].values[0] ) )
                    logging.error("{}: {}".format(sys.exc_info()[0], traceback.format_exc()))             
                    continue
                elif use_user_info==1:
                    logging.info("> Activity: {} + User Segment: {} --> There is probably something wrong with model running process!".format(mkey_explanation.loc[(mkey_explanation["dimension_id"]==tactic_id),"dimension_name"].values[0],
                                                                                                                                            str(user_seg) ) )
                    logging.error("{}: {}".format(sys.exc_info()[0], traceback.format_exc()))             
                    continue

            try:
                decomp_com = decomp(
                    last_click,
                    level,
                    ranked_dims,
                    best_model_data,
                    best_mkey_lookup_all,
                    best_model,
                    valid_num_iteration,
                    best_mode_validation_metric,
                    tactic_id,
                    user_seg_col,
                    user_seg,
                    use_user_info,
                    rankorder,
                    digital_incremental_volume=1,
                    spend=1,
                    margin=1)
                
                if use_user_info==2:
                    logging.info("> Activity: {} --> Decomp table is generated.".format(mkey_explanation.loc[(mkey_explanation["dimension_id"]==tactic_id),"dimension_name"].values[0] ) )
                elif use_user_info==1:
                    logging.info("> Activity: {} + User Segment: {} --> Decomp table is generated.".format(mkey_explanation.loc[(mkey_explanation["dimension_id"]==tactic_id),"dimension_name"].values[0], str(user_seg) ) )
            
            except Exception as e:
                if use_user_info==2:
                    logging.info("> Activity: {} --> There is probably something wrong with decomp calculation!".format(mkey_explanation.loc[(mkey_explanation["dimension_id"]==tactic_id),"dimension_name"].values[0]))
                    logging.error("{}: {}".format(sys.exc_info()[0], traceback.format_exc()))
                    continue
                elif use_user_info==1:
                    logging.info("> Activity: {} + User Segment: {} --> There is probably something wrong with decomp calculation!".format(mkey_explanation.loc[(mkey_explanation["dimension_id"]==tactic_id),"dimension_name"].values[0],
                                                                                                                                            str(user_seg) ) )
                    logging.error("{}: {}".format(sys.exc_info()[0], traceback.format_exc()))
                    continue

            try:
                mix_master = export_decomp_with_mkey_exp(
                    # ranked_dims,
                    tokenized_cols,
                    conn_string,
                    tokenized_table_name,  # use this table name to name output file
                    tactic_id,
                    user_seg_col,
                    use_user_info,
                    decomp_com,
                    metric_key_explanation_table,
                    appended_df
                    # level
                )
                if use_user_info==2:
                    logging.info("> Activity: {} --> Mix Master has been generated!".format(mkey_explanation.loc[(mkey_explanation["dimension_id"]==tactic_id),"dimension_name"].values[0]))
                elif use_user_info==1:
                    logging.info("> Activity: {} + User Segment: {} --> Mix Master has been generated!".format(mkey_explanation.loc[(mkey_explanation["dimension_id"]==tactic_id),"dimension_name"].values[0],
                                                                                                                str(user_seg) ) )
            except Exception as e:
                if use_user_info==2:
                    logging.info("> Activity: {} --> There is probably something wrong with mix master calculation!".format(mkey_explanation.loc[(mkey_explanation["dimension_id"]==tactic_id),"dimension_name"].values[0]))
                    logging.error("{}: {}".format(sys.exc_info()[0], traceback.format_exc()))
                    continue
                elif use_user_info==1:
                    logging.info("> Activity: {} + User Segment: {} --> There is probably something wrong with mix master calculation!".format(mkey_explanation.loc[(mkey_explanation["dimension_id"]==tactic_id),"dimension_name"].values[0],
                                                                                                                                                str(user_seg) ) )
                    logging.error("{}: {}".format(sys.exc_info()[0], traceback.format_exc()))
                    continue
    


    # concatenate mix master from different tactics
    try:
        appended_df_full = pd.concat(mix_master)
        writer = pd.ExcelWriter(output_excel_path, engine='xlsxwriter' )
        appended_df_full.to_excel(writer, sheet_name='Decomp Info', index=False )
        writer.save()
    except Exception as e:
        logging.info("> There is probably something wrong with concatenating decomp from tactic(s)!")
        # logging.info("> {}: {}".format(sys.exc_info()[0], str(e)))
        logging.error("{}: {}".format(sys.exc_info()[0], traceback.format_exc()))             # detailed traceback

    try:
        upload_to_s3(
            filename=output_excel_path,
            bucket='analytic-partners',
            destination=directory+output_excel_path,
            aws_access_key_id=aws_credentials['aws_access_key_id'],
            aws_secret_access_key=aws_credentials['aws_secret_access_key'])
        logging.info("> Full Mix Master file {} is now in S3://analytic-partners/{}!".format(output_excel_path,directory) )
    except Exception as e:
        logging.info("> There is probably something wrong with outputing mix master to S3!")
        # logging.info("> {}: {}".format(sys.exc_info()[0], str(e)))
        logging.error("{}: {}".format(sys.exc_info()[0], traceback.format_exc()))             # detailed traceback

