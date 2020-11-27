# -*- coding: utf-8 -*-
"""
Created on Mon Jan 28 14:58:49 2019

MTA app modelling tab

@author: yuanjing.han
"""

import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
from flask import request

import sys
import os
import logging
import traceback

from mta_helper import Semaphore
import MTAfunction_multi_activities as MTA_multi
import MTAfunction as MTA
from config import db_credentials, aws_credentials


semaphore = Semaphore()

def register_callback_m(app):
    @app.callback(
        Output('output_keypress', 'children'),
        [Input('my_run_button', 'n_clicks')],
        [State('dbname-dropdown', 'value'),
        State('directory', 'value'),
#       State('project_name', 'value'),
        State('tokenized_table_name', 'value'),
        State('metric_key_explanation_table', 'value'),
        State('dimension_ranking_table','value'),
        State('kpi_col','value'),
        State('use_user_info','value'),
        State('user_seg_col','value'),
        State('mta_partial_seg','value'),
        State('timediscount', 'value'),
        # State('single_multiple','value')
        # State('spend', 'value'),
        # State('margin', 'value'),
        # State('digital_incremental_volume', 'value')
    ])
    def update_output(
        run_n_clicks,
        dbname,
        directory,
        # project_name,
        tokenized_table_name,
        metric_key_explanation_table,
        dimension_ranking_table,
        kpi_col,
        use_user_info,
        user_seg_col,
        mta_partial_seg,        # list
        timediscount,
        # single_multiple
    ):

        # set the following parameters = 1
        # spend,
        # margin,
        # digital_incremental_volume

        if run_n_clicks>=1:
            logging.info("Button has been clicked.")
            
            if None in [dbname,tokenized_table_name,metric_key_explanation_table,dimension_ranking_table,kpi_col,timediscount]:
                logging.info('> Some Redshift Connection/Table/Column Info is missing. Please check and submit again!')
                logging.info("Button is available.")
            
            else:
                conn_string = """dbname='{}' port='5439' user='{}' password='{}'
                        host='rentrak.cyzketua53g1.us-east-1.redshift.amazonaws.com'""".format(dbname,
                                                                                                db_credentials['username'],
                                                                                                db_credentials['password'])
                
                all_table_exist = MTA_multi.check_table_existance(conn_string,
                                                                metric_key_explanation_table,
                                                                dimension_ranking_table,
                                                                tokenized_table_name)
                

                logging.info("> Database is connected.")

                if not all_table_exist:
                    logging.info('> Some of the tables does not exist! Please check and try again!')
                    logging.info("Button is available.")  

                else:
                    tokenized_grant_access = MTA_multi.check_tokenized_privilege(tokenized_table_name, 
                                                                            db_credentials['username'],
                                                                            conn_string)
                
                    metric_grant_access = MTA_multi.check_mapping_privilege(metric_key_explanation_table, 
                                                                            db_credentials['username'],
                                                                            conn_string)

                    tactic_grant_access = MTA_multi.check_mapping_privilege(dimension_ranking_table, 
                                                                            db_credentials['username'],
                                                                            conn_string)
                    
                    print(tokenized_grant_access,metric_grant_access,tactic_grant_access)   
               
                    if not tokenized_grant_access:
                        logging.info('> Please grant access (select, update, delete) to Tokenized table!')
                        logging.info("Button is available.") 
                    elif not metric_grant_access:
                        logging.info('> Please grant access (select) to Metric table!')
                        logging.info("Button is available.") 
                    elif not tactic_grant_access:
                        logging.info('> Please grant access (select) to Rank Order table!')
                        logging.info("Button is available.") 
                    
                    else:

                        kpi_col_exist = MTA_multi.check_kpi_col_existance(conn_string,
                                                                    tokenized_table_name,
                                                                    kpi_col)
                    
                        if use_user_info==1:
                            user_seg_col_exist = MTA_multi.check_user_seg_col_existance(conn_string,
                                                                                        tokenized_table_name,
                                                                                        user_seg_col ) 
                        else:                           
                            user_seg_col_exist=True

                        if not kpi_col_exist:
                            logging.info('> KPI column does not exist! Please check and try again!')
                            logging.info("Button is available.") 
                        elif not user_seg_col_exist:
                            logging.info('> User Segment column does not exist! Please check and try again!')
                            logging.info("Button is available.")
                        else:
                            logging.info("> All relevant tables/columns exist, start running model. Please hold on!")
                            
                            if semaphore.is_locked():
                                raise Exception('Resource is locked')
                                    
                            semaphore.lock()                                    

                            logging.info("> Thank you for the info! Your Mix Master will be generated in s3://analytic-partners/{} shortly!".format(directory))

                            MTA_multi.run_mta_multi(
                                conn_string,
                                tokenized_table_name,
                                metric_key_explanation_table,
                                dimension_ranking_table,
                                kpi_col,
                                user_seg_col,
                                directory,
                                use_user_info,
                                mta_partial_seg,
                                timediscount=1)
                            
                            semaphore.unlock()
                            logging.info("Button is available.")





    # "lock" the model running process if the model starts to run and unlock it when finishing running
    @app.callback(
        Output('lock', 'value'),
        [Input('interval', 'n_intervals') ] )
    def display_status(n):
        return 'Running...' if semaphore.is_locked() else 'Free'





    # if the model is still running, disable the run button to prevent the second click
    @app.callback(
        Output('my_run_button', 'disabled'),
        [Input('lock','value') ] )
    def update_run_button(is_running):
        if is_running == 'Running...':
            return True
        elif is_running == 'Free':
            return False





    # print the two most recent "button-click" outputs to screen
    @app.callback(
        Output('console_out','value'),
        [Input('interval2', 'n_intervals')])
    def update_console_message(n):

        sep_index=[]
        file=list(open("error.log",'r'))
        client_side_messages=[line.replace(" INFO","") for line in file if "INFO" in line and "NumExpr defaulting" not in line]
        for line in client_side_messages:
            if "Button has been clicked." in line:
                sep_index.append(client_side_messages.index(line))
            else:
                continue
        
        data=''
        try:
            lines=client_side_messages[(sep_index[len(sep_index)-2]): ]
            for line in lines:
                if "Button has been clicked." in line or "Button is available." in line or "Running on" in line or "Debugger" in line:
                    continue
                else:
                    data=data+line
        except:
            raise PreventUpdate
        
        return data
    




    @app.callback(
        [Output('user_seg_col','disabled'),
        Output('user_seg_col','placeholder'),
        Output('user_seg_col','value'),
        Output('user_seg_formgroup','style'),
        Output('mta_partial_seg','values')],
        [Input('use_user_info','value') ] )
    def use_customer_info(use_user_info):
        if use_user_info==1:
            return [False,'Enter User Segment column name','',{'display':''},[]]
        else:
            return [True,'',1,{'display':'none'},[]]



    
    # @app.callback(
    #         Output('dim_rank','style'),
    #         [Input('single_multiple','value') ] )
    # def single_multiple(value):
    #     if value==0:
    #         return {'display':'none'}
    #     else:
    #         return {'display':''}


    # if single_multiple==0:
    #     if None in [dbname,tokenized_table_name,metric_key_explanation_table,timediscount]:
    #         logging.info('> Some Redshift Connection/Table Info is missing. Please check and submit again!')
    #         logging.info("Button is available.")
        
    #     else:
    #         conn_string = """dbname='{}' port='5439' user='{}' password='{}'
    #                 host='rentrak.cyzketua53g1.us-east-1.redshift.amazonaws.com'""".format(dbname,
    #                                                                                         db_credentials['username'],
    #                                                                                         db_credentials['password'])

    #         all_table_exist = MTA.check_table_existance(conn_string,
    #                                                 metric_key_explanation_table,
    #                                                 tokenized_table_name)

        
    #         logging.info("> Database is connected.")

    #         if not all_table_exist:
    #             logging.info('> Some of the tables does not exist! Please check and try again!')
    #             logging.info("Button is available.")        
                
    #         else:
    #             logging.info("> All relevant tables exist, start running model. Please hold on!")
            
    #             if semaphore.is_locked():
    #                 raise Exception('Resource is locked')
                    
    #             semaphore.lock()                                       

    
    #             logging.info("> Thank you for the info! Your Mix Master will be generated in s3://analytic-partners/{} shortly!".format(directory))

    #             # load/clean data
    #             try:
    #                 model_data, non_conversion_model_data, conversion_metric_averages, last_click = MTA.database_data_processing(
    #                     conn_string,
    #                     tokenized_table_name,
    #                     timediscount)
    #                 except Exception as e:
    #                     logging.info("> There is probably something wrong with data loading/cleaning process! Please go check the tokenized/metric key explanation table.")   
    #                     # logging.info("> {}: {}".format(sys.exc_info()[0], str(e)))
    #                     logging.error("{}: {}".format(sys.exc_info()[0], traceback.format_exc()))             # detailed traceback
    #                     logging.info("Button is available.")
    #                     semaphore.unlock()
                
    #                 # run MTA model
    #                 try:
    #                     best_model_data, best_mkey_lookup_all, best_model,level, valid_num_iteration, best_mode_validation_metric = MTA.model_iteration(
    #                         tokenized_table_name,
    #                         conversion_metric_averages,
    #                         model_data,
    #                         non_conversion_model_data,
    #                         num_iterations=3, # no need to show on MTA UI
    #                         quant=0.3
    #                     )
    #                 except Exception as e:
    #                     logging.info("> There is probably something wrong with model running process! Please go check the tokenized/metric key explanation table.")
    #                     # logging.info("> {}: {}".format(sys.exc_info()[0], str(e)))
    #                     logging.error("{}: {}".format(sys.exc_info()[0], traceback.format_exc())) 
    #                     logging.info("Button is available.")    
    #                     semaphore.unlock()

    #                 # Calculate decomp
    #                 try:
    #                     decomp_com = MTA.decomp(
    #                         last_click,
    #                         level,
    #                         best_model_data,
    #                         best_mkey_lookup_all,
    #                         best_model,
    #                         valid_num_iteration,
    #                         best_mode_validation_metric,
    #                         digital_incremental_volume=1,
    #                         spend=1,
    #                         margin=1)
    #                     logging.info("> Decomp table is generated.")
                        
    #                 except Exception as e:
    #                     logging.info("> There is probably something wrong with decomp calculation! Please go check the tokenized/metric key explanation table.")
    #                     # logging.info("> {}: {}".format(sys.exc_info()[0], str(e)))
    #                     logging.error("{}: {}".format(sys.exc_info()[0], traceback.format_exc())) 
    #                     logging.info("Button is available.")           
    #                     semaphore.unlock()
                        
    #                 # Calculate mix master
    #                 try:
    #                     mix_master = MTA.export_decomp_with_mkey_exp(
    #                         directory,
    #                         conn_string,
    #                         tokenized_table_name,       
    #                         decomp_com,
    #                         metric_key_explanation_table,
    #                         level
    #                     )
    #                     logging.info("> Mix Master file {} has been generated!".format(mix_master) )
    #                 except Exception as e:
    #                     logging.info("> There is probably something wrong with mix master calculation! Please go check the tokenized/metric key explanation table.")
    #                     # logging.info("> {}: {}".format(sys.exc_info()[0], str(e)))
    #                     logging.error("{}: {}".format(sys.exc_info()[0], traceback.format_exc())) 
    #                     logging.info("Button is available.")    
    #                     semaphore.unlock()


    #                 # upload mix master to S3
    #                 try:
    #                     MTA.upload_to_s3(
    #                         filename=mix_master,
    #                         bucket='analytic-partners',
    #                         destination=directory + mix_master,
    #                         aws_access_key_id=aws_credentials['aws_access_key_id'],
    #                         aws_secret_access_key=aws_credentials['aws_secret_access_key'])
    #                     logging.info("> Mix Master file {} is now in S3://analytic-partners/{}!".format(mix_master,directory) )
    #                 except Exception as e:
    #                     logging.info("> There is probably something wrong with outputing mix master to S3! Please ask Science Team for results.")
    #                     # logging.info("> {}: {}".format(sys.exc_info()[0], str(e)))
    #                     logging.error("{}: {}".format(sys.exc_info()[0], traceback.format_exc())) 
    #                     logging.info("Button is available.")        
    #                     semaphore.unlock()

    #                 semaphore.unlock()
    #                 logging.info("Button is available.")

    # multiple activities
    # else:





