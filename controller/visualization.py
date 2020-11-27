import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
import dash_table
from plotly import graph_objs as go

import pandas as pd
import numpy as np
import re 
import flask
import io
import os
import time
import json
import urllib.parse
from mta_helper import indicator,parse_contents,create_hbar,create_pie,MTA_info_modal,create_dropdown_list,groupby_multi_func,linked_metrics,reorder_columns


font_fam=""" -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, "Noto Sans", sans-serif, "Apple Color Emoji", "Segoe UI Emoji", "Segoe UI Symbol", "Noto Color Emoji" """
# DOWNLOAD_DIRECTORY = "download_directory"

# if not os.path.exists(DOWNLOAD_DIRECTORY):
#     os.makedirs(DOWNLOAD_DIRECTORY)



def register_callback_v(app):


    # store uploaded data to hidden div
    @app.callback(
        # [
        Output('uploaded-data-container','children'),
        # Output('uploaded-data-dim-order','children') 
        # ],
        [Input('upload_data', 'contents')],
        [State('upload_data', 'filename')] )
    def store_uploaded_data(contents, filename):
        if not contents or not filename:
            return 
        
        df = parse_contents(contents, filename) 

        df['metric_key'] = df['metric_key'].astype(str)

        # # need to convert to json to store in a hidden div
        # dim_idx_mapping = {dim_name: df.columns.get_loc(dim_name) for dim_name in list(df.columns) if "_name" in dim_name }

        stored_data = df.to_json(date_format='iso', orient='split')  

        # stored_dim_idx_mapping = json.dumps(dim_idx_mapping)

        return stored_data                # stored_dim_idx_mapping






    # uploaded data --> metric dropdown
    # @app.callback(
    #     Output('metric_dropdown','options'),
    #     [Input('upload_data', 'contents')],
    #     [State('upload_data', 'filename')])
    # def update_metric_dropdown(contents,filename):
    #     if not contents or not filename:
    #         return 
    #     dff=parse_contents(contents, filename)  
    #     dropdown_options=create_dropdown_list(dff)
    #     return dropdown_options   





    # tactic
    @app.callback(
        Output('tactic_dropdown','options'),
        [Input('uploaded-data-container', 'children') ] )
    def update_tactic_dropdown(uploaded):

        if uploaded is None:
            return [{}]

        else:
            df = pd.read_json(uploaded, orient='split', dtype = {'metric_key': 'object'} )
            dropdown_options=[{'label': key, 'value': value} for (key,value) in zip(df['tactic_name'].unique(),df['tactic_id'].unique() ) ]
            return dropdown_options






    # user segment
    @app.callback(
        [Output('user_seg_dropdown','options'),
        Output('user_seg_dropdown','disabled')],
        [Input('uploaded-data-container','children'),
        Input('tactic_dropdown','value') ] )
    def update_user_seg_dropdown(uploaded,tactic):

        if uploaded is None or tactic is None:
            return [ [{}], True ]

        else:
            df = pd.read_json(uploaded, orient='split', dtype = {'metric_key': 'object'} )
            df_tactic = df.loc[(df["tactic_id"]==tactic)]
            if any(re.match('s[\d]',item) for item in df_tactic.columns.tolist()):
                user_seg_col=df_tactic.filter(regex='s[\d]').columns[0]
                # print(user_seg_col )
                dropdown_options=[{'label': key, 'value': value} for (key,value) in zip(df_tactic[user_seg_col].unique(),df_tactic[user_seg_col].unique())]
                return [ dropdown_options, False ]
            else:
                return [ [{}], True ]
        





    # method
    @app.callback(
        Output('method_dropdown','options'),
        [Input('uploaded-data-container', 'children'),
        Input('tactic_dropdown','value'),
        Input('user_seg_dropdown','value') ] )
    def update_method_dropdown(uploaded,tactic,user_seg):

        # no uploaded file or tactic isn't chosen
        if uploaded is None or tactic is None:
            return [{}]

        else:
            df = pd.read_json(uploaded, orient='split', dtype = {'metric_key': 'object'} )

            if any(re.match('s[\d]',item) for item in df.columns.tolist()):
                user_seg_col=df.filter(regex='s[\d]').columns[0]
                df_slice = df.loc[(df["tactic_id"]==tactic)&(df[user_seg_col]==user_seg)]
            else:
                df_slice = df.loc[(df["tactic_id"]==tactic)]

            dropdown_options=[{'label': key, 'value': value} for (key,value) in zip(df_slice['methodology'].unique(),df_slice['methodology'].unique())]
            return dropdown_options




    # metric - dropdown only shows metrics this tactic owns
    @app.callback(
        [Output('metric_dropdown','options'),
        Output('metric_dropdown','value'),
        Output('uploaded-data-dim-order','children')],
        [Input('uploaded-data-container', 'children'),
        Input('tactic_dropdown','value'),
        Input('method_dropdown','value'),
        Input('user_seg_dropdown','value') ] )
    def update_metric_dropdown(uploaded,tactic,method,user_seg):

        if uploaded is None or tactic is None:
            return [ [{}], [], [] ]

        else:
            df = pd.read_json(uploaded, orient='split', dtype = {'metric_key': 'object'} )

            if any(re.match('s[\d]',item) for item in df.columns.tolist()):
                user_seg_col = df.filter(regex='s[\d]').columns[0]
                df_slice = df.loc[(df["tactic_id"]==tactic) & (df['methodology']==method) & (df[user_seg_col]==user_seg ) ]
            else:
                df_slice = df.loc[(df["tactic_id"]==tactic) & (df['methodology']==method)]

            # slice one row and split metric_key, then find the associated column names based on metrics
            # some metric_key may contain 0 --> need to confirm with CET
            # imagine every metric_key at least contains one '0' --> use non-zero part to retrieve dimension name 
            # assume that metrics values are unique globally in each tactic, e.g. '2046' only exists in m3
            
            try:
                temp=np.array([i.split('_') for i in df_slice['metric_key']])
                splited_metrics = []
                for i in range(len(temp[0])):
                    # just in case metrics in one dimension are all 0
                    if np.sum(temp[:,i].astype(int)) == 0:
                        continue
                    splited_metrics.append(temp[:,i][(temp[:,i]!='0').argmax(axis=0)])    # pick one valid metric for each dimension
                # print(splited_metrics)

                # for i in range(df_slice.shape[0]):
                #     splited_metrics = df_slice['metric_key'].str.split('_').reset_index(drop=True)[i]
                #     if '0' not in splited_metrics:
                #         # print(splited_metrics)
                #         break
                #     else:
                #         continue

                dim_value = [df_slice.columns[df_slice.isin([int(i)]).any()][0]+"_name" for i in splited_metrics]
                dim_label = [df_slice.columns[df_slice.isin([int(i)]).any()][0] for i in splited_metrics]
                dropdown_options=[{'label': key, 'value': value} for (key,value) in zip(dim_label,dim_value)]

                # store the order (starting from 1) if dimensions in tactic to hidden div
                dim_idx_mapping = {dim: dim_value.index(dim)+1 for dim in dim_value}
                stored_dim_idx_mapping = json.dumps(dim_idx_mapping)
                return [dropdown_options, [], stored_dim_idx_mapping ]

            except:
                return [[{}], [], [] ]





    # uploaded data --> dashboard layout
    # @app.callback(
    #     Output('content', 'children'),
    #     [Input('upload_data', 'contents'),
    #     Input('metric_dropdown','value'),                                
    #     Input('method_dropdown','value')],
    #     [State('upload_data', 'filename')])
    # def update_data(contents,
    #                 dimensions,
    #                 method,
    #                 filename):
        

    #     if not contents or not filename:
    #         return 
    #     df = parse_contents(contents, filename)  

    @app.callback(
        Output('content', 'children'),
        [Input('uploaded-data-container', 'children'),
        Input('tactic_dropdown','value'),
        Input('metric_dropdown','value'),                                
        Input('method_dropdown','value'),
        Input('user_seg_dropdown','value')   ]  )
    def update_data(uploaded,
                    tactic,
                    dimensions,
                    method,
                    user_seg ):
        
        if uploaded is None:
            # pass
            return []  
        else:
            df = pd.read_json(uploaded, orient='split', dtype = {'metric_key': 'object'}) 
            if any(re.match('s[\d]',item) for item in df.columns.tolist()):
                user_seg_col = df.filter(regex='s[\d]').columns[0]
                df = df.loc[(df["tactic_id"]==tactic) & (df['methodology']==method) & (df[user_seg_col]==user_seg ) ]
            else:
                df = df.loc[(df["tactic_id"]==tactic) & (df['methodology']==method)]

            df = reorder_columns(df)

            # only Incremental Conversion and ROI needs digits limit, so hard-coded the limits here
            df[['Incremental_Conversion']]=df[['Incremental_Conversion']].round(4)
            df[['ROI']]=df[['ROI']].round(4)

            # options_index = next((index for (index, d) in enumerate(metric_dropdown_list) if d["value"] == value), None)

            return html.Div(
                children=[
                    dbc.Row(
                        [
                            dbc.Col(dbc.Card(
                                [
                                    dbc.CardBody(
                                        [   html.H5("Model Strength",className="card-text tab1_card_title"),
                                            html.H4(id='model_strength',className="card-title tab1_card_index", style={'text-align':'center','color':'#ffffff'}),
                                        ],style={'padding-bottom': '0.55rem','padding-top': '0.55rem', 'height':'5.7rem'}
                                    ),
                                ],
                                className='ap-green',
                                inverse=True),
                                style={'padding-right':'0px','padding-left':'10px'},
                                width=3
                            ),

                            dbc.Col(dbc.Card(
                                [
                                    dbc.CardBody(
                                        [   html.H5("# Metric Keys",className="card-text tab1_card_title"),
                                            html.H4(id='total_mk',className="card-title tab1_card_index"),
                                        ],style={'padding-bottom': '0.55rem','padding-top': '0.55rem','height':'5.7rem'}
                                    ),
                                ],
                                className='ap-yellow',
                                inverse=True),
                                style={'padding-right':'0px','padding-left':'10px'},
                                width=3
                            ),

                            dbc.Col(dbc.Card(
                                [
                                    dbc.CardBody(
                                        [   html.H5("# Conversions",className="card-text tab1_card_title"),
                                            html.H4(id='total_c',className="card-title tab1_card_index"),
                                        ],style={'padding-bottom': '0.55rem','padding-top': '0.55rem','height':'5.7rem'}
                                    ),
                                ],
                                className='ap-orange',
                                inverse=True),
                                style={'padding-right':'0px','padding-left':'10px'},
                                width=3
                            ),

                            dbc.Col(dbc.Card(
                                [
                                    dbc.CardBody(
                                        [   html.H5("Total Spend",className="card-text tab1_card_title"),
                                            html.H4(id='total_s',className="card-title tab1_card_index"),
                                        ],style={'padding-bottom': '0.55rem','padding-top': '0.55rem','height':'5.7rem'}
                                    ),
                                ],
                                className='ap-denim',
                                inverse=True),
                                style={'padding-right':'0px','padding-left':'10px'},
                                width=3
                            ),

                        ], style={'padding-bottom':'10px'}
                    ),

                    dbc.Row(
                        [
                            dbc.Col(
                                dbc.Card(
                                [
                                    dbc.CardBody(
                                        [     
                                            dcc.Loading(
                                                id="loading_graph_one",
                                                type="cube",
                                                color="#6689CC",
                                            ),
                                        ], 
                                        style = {'padding-top':'0px'},
                                    ), 
                                ],
                                inverse=True),
                                style={'padding-right':'0px','padding-left':'10px'},
                                width=6 ),

                            dbc.Col(
                                dbc.Card(
                                [
                                    dbc.CardBody(
                                        [     
                                            dcc.Loading(
                                                id="loading_graph_two",
                                                type="cube",
                                                color="#6689CC",
                                            ),
                                        ],
                                        style = {'padding-top':'0px'},
                                    ),
                                ],
                                inverse=True),
                                style={'padding-right':'0px','padding-left':'10px'},
                                width=6),
                        ],
                        style={'padding-bottom':'10px'}
                    ),

                    dbc.Row(
                    [
                            dbc.Col(dbc.Card(
                                [
                                    dbc.CardBody(
                                        [
                                            html.H6("Mix Master", style={'text-align':'center', 'margin-top': '8px'}),
                                            dash_table.DataTable(
                                                id='edit_datatable',
                                                data = df.to_dict('rows'),
                                                columns=[{'name': i, 'id': i} for i in df.columns],
                                                editable=True,
                                                sorting=True,
                                                sorting_type="multi",
                                                # row_selectable="multi",
                                                row_deletable=True,
                                                # selected_rows=[],
                                                # allow backend filtering
                                                filtering='be',
                                                filtering_settings='',

                                                style_table={'overflowX': 'auto',
                                                    'overflowY': 'auto',
                                                    "height": 300,
                                                    'width': 'auto'
                                                },
                                                style_header={
                                                    'backgroundColor': '#B7CFE5',
                                                    'fontWeight': 'bold'
                                                },
                                                style_cell={
                                                    'backgroundColor': '#FFFFFF',
                                                    'color': '#000000',
                                                    'height': '26px',
                                                    'textAlign': 'center',
                                                    'font-family': font_fam,

                                                },
                                                style_as_list_view=True,
                                            ),     

                                        ],
                                        style = {'padding-top':'0px'},
                                    ),
                                ],
                                inverse=True),
                                style={'padding-right':'0px','padding-left':'10px'},
#                                 className="chart-div",
                                width=6),


                            dbc.Col(dbc.Card(
                                [
                                    dbc.CardBody(
                                        [
                                            dcc.Loading(
                                                id="loading_graph_three",
                                                type="cube",
                                                color="#6689CC"
                                            )     
                                        ],
                                        style = {'padding-top':'0px'},
                                    ),
                                ],
                                inverse=True),
                                style={'padding-right':'0px','padding-left':'10px'},
                                width=6),  
                        ],
                    ),
                    
                    dbc.FormGroup([

                        html.A(
                            children=[
                                html.Span('Download  '),
                                html.I(className="fa fa-download")
                            ],
                            id='download_link',
                            download="downloadFile.csv",
                            target="_blank",
                            style = {"padding-left": "20px",
                                    "padding-top": "10px",
                                    "color": "#6689CC"}
                        ),

                        dbc.RadioItems(
                            options=[
                                {'label': 'Raw', 'value': 'raw'},
                                {'label': 'Pivot', 'value': 'pivot'}
                            ],
                            id='raw_pivot',
                            value='raw',
                            inline=True,
                            style = {"padding-left": "20px",
                                    "padding-top": "10px"}
                        ),

                    ], row=True )
                ])






    # dash table backend filtering (server side filtering when dataset is large)
    # @app.callback(
    #     Output('edit_datatable', "data"),
    #     [Input('upload_data', 'contents'),
    #     Input('metric_dropdown','value'),                                
    #     Input('method_dropdown','value'),
    #     Input('edit_datatable', "filtering_settings")],
    #     [State('upload_data', 'filename') ] )
    # def update_graph(contents,
    #                 dimensions,
    #                 method,
    #                 filtering_settings,
    #                 filename):
        
    #     if not contents or not filename:
    #         return 
    #     df = parse_contents(contents, filename)  







    @app.callback(
        Output('edit_datatable', "data"),
        [Input('uploaded-data-container', 'children'),
        Input('tactic_dropdown','value'),
        Input('metric_dropdown','value'),                                
        Input('method_dropdown','value'),
        Input('user_seg_dropdown','value'),
        Input('edit_datatable', "filtering_settings") ] )
    def update_graph(uploaded,
                    tactic,
                    dimensions,
                    method,
                    user_seg,
                    filtering_settings ):
        
        if uploaded is None:
            return                
    
        df = pd.read_json(uploaded, orient='split', dtype = {'metric_key': 'object'}) 

        if any(re.match('s[\d]',item) for item in df.columns.tolist()):
            user_seg_col=df.filter(regex='s[\d]').columns[0]
            df = df.loc[(df["tactic_id"]==tactic) & (df['methodology'] == method) & (df[user_seg_col]==user_seg)]
        else:
            df = df.loc[(df["tactic_id"]==tactic) & (df['methodology'] == method)]


        df = reorder_columns(df)

        # Incremental Conversion and ROI: 4-digit limit
        df[['Incremental_Conversion']]=df[['Incremental_Conversion']].round(4)
        df[['ROI']]=df[['ROI']].round(4)

        
        filtering_expressions = filtering_settings.split(' && ')
        dff = df

        # 2019-3-12: add replace('"','') is only a temporary fix of "keyError: '"ColumnName"'
        # Since currently a lot of changes are going on at the backend, we may want to change to a more stable version later.

        for filter in filtering_expressions:
            # if ' eq ' in filter:
            #     col_name = filter.split(' eq ')[0].replace('"', '')
            #     filter_value = filter.split(' eq ')[1]
            #     dff = dff.loc[dff[col_name] == filter_value]
            # if ' > ' in filter:
            #     col_name = filter.split(' > ')[0].replace('"', '')
            #     filter_value = float(filter.split(' > ')[1])
            #     dff = dff.loc[dff[col_name] > filter_value]
            # if ' < ' in filter:
            #     col_name = filter.split(' < ')[0].replace('"', '')
            #     filter_value = float(filter.split(' < ')[1])
            #     dff = dff.loc[dff[col_name] < filter_value]

            if ' eq ' in filter:
                col_name = filter.split(' eq ')[0].replace('"', '')
                filter_value = filter.split(' eq ')[1]
                dff = dff.loc[dff[col_name].astype(str) == filter_value]
            elif ' > ' in filter:
                col_name = filter.split(' > ')[0].replace('"', '')
                filter_value = float(filter.split('>')[1])
                dff = dff.loc[dff[col_name] > filter_value]
            elif ' >= ' in filter:
                col_name = filter.split(' >= ')[0].replace('"', '')
                filter_value = float(filter.split(' >= ')[1])
                dff = dff.loc[dff[col_name] >= filter_value]
            elif ' < ' in filter:
                col_name = filter.split(' < ')[0].replace('"', '')
                filter_value = float(filter.split(' < ')[1])
                dff = dff.loc[dff[col_name] < filter_value]
            elif ' <= ' in filter:
                col_name = filter.split(' <= ')[0].replace('"', '')
                filter_value = float(filter.split(' <= ')[1])
                dff = dff.loc[dff[col_name] <= filter_value]

        return dff.to_dict('rows')






    # Incremental Conversion
    @app.callback(Output('loading_graph_one', 'children'),
                [Input('edit_datatable', 'data'),
                Input('metric_dropdown','value'),
                Input('uploaded-data-dim-order','children') ] )
    def update_graph_one(rows, dimensions, dim_order):

        dff = pd.DataFrame(rows)

        if dimensions is not None:
            try:
                dim_idx_mapping = json.loads(dim_order)
                sorted_dims = sorted(dimensions, key=lambda x: dim_idx_mapping[x], reverse = False)   # the smaller the rank, the less granular the metric
                # print(sorted_dims,dim_idx_mapping)
                pivot_dff = dff.groupby(by=sorted_dims).apply(groupby_multi_func).reset_index()
                pivot_dff = linked_metrics(pivot_dff, sorted_dims, "metric_dimensions" )

                # make sure the highest value gets navy blue
                pivot_dff.sort_values(by='Incremental_Conversion',ascending=False,inplace=True)
                pie_chart = create_pie(pivot_dff,pie_var="Incremental_Conversion",labels=pivot_dff['metric_dimensions'])

                return [
                    html.H6("Incremental Conversion by metric dimensions", style={'text-align':'center', 'margin-top': '8px'} ),
                    dcc.Graph(
                        id="loading_one",
                        figure=go.Figure(
                            data=pie_chart['data'],
                            layout=pie_chart['layout']
                        ),
                        config=dict(displayModeBar=False),
                    )
                ]

            except:
                return []

        else:
            return []




   


    # Spend
    @app.callback(
        Output('loading_graph_two','children'),
        [Input('edit_datatable','data'),
        Input('metric_dropdown','value'),
        Input('uploaded-data-dim-order','children') ] )
    def update_graph_two(rows, dimensions, dim_order):

        dff = pd.DataFrame(rows)  # or, more generally json.loads(jsonified_cleaned_data)

        if dimensions is not None:
            try:
                dim_idx_mapping = json.loads(dim_order)
                sorted_dims = sorted(dimensions, key=lambda x: dim_idx_mapping[x], reverse = False)            
                pivot_dff = dff.groupby(by=sorted_dims).apply(groupby_multi_func).reset_index()
                pivot_dff = linked_metrics(pivot_dff, sorted_dims, "metric_dimensions")
                # make sure the highest value on top
                pivot_dff.sort_values(by='spend', ascending=True, inplace=True)
                hbar = create_hbar(pivot_dff,x_var="spend",y_var="metric_dimensions")

                return html.Div(
                    id="loading-2",
                    children=[
                        html.H6("Spend by metric dimensions", style={'text-align':'center', 'margin-top': '8px'}),
                        dcc.Graph(
                            id="loading_two",
                            figure=go.Figure(
                                data=hbar['data'],
                                layout=hbar['layout']
                            ),
                            config=dict(displayModeBar=False),
                            className="tab1_chart",
                        )
                    ])
            except:
                return []
        else:
            return []

    




    # ROI
    @app.callback(Output('loading_graph_three', 'children'),
                [Input('edit_datatable', 'data'),
                Input('metric_dropdown','value'),
                Input('uploaded-data-dim-order','children') ] )
    def update_graph_three(rows, dimensions, dim_order):
        dff = pd.DataFrame(rows)  # or, more generally json.loads(jsonified_cleaned_data)

        if dimensions is not None:
            try:
                dim_idx_mapping = json.loads(dim_order)
                sorted_dims = sorted(dimensions, key=lambda x: dim_idx_mapping[x], reverse = False)    
                pivot_dff = dff.groupby(by=sorted_dims).apply(groupby_multi_func).reset_index()
                pivot_dff = linked_metrics(pivot_dff, sorted_dims, "metric_dimensions")
                # make sure the highest value on top
                pivot_dff.sort_values(by='ROI', ascending=True, inplace=True)
                hbar = create_hbar(pivot_dff,x_var="ROI",y_var="metric_dimensions")
            
                return html.Div(
                    id="loading-3",
                    children=[
                        html.H6("ROI by metric dimensions", style={'text-align':'center', 'margin-top': '8px'}),
                        dcc.Graph(
                            id="loading_three",
                            figure=go.Figure(
                                data=hbar['data'],
                                layout=hbar['layout']
                            ),
                            config=dict(displayModeBar=False),
                            className="tab1_chart",
                        )
                    ])
            except:
                return []
        else:
            return []



    
    # download raw/pivot data
    @app.callback(
        Output('download_link', 'href'),
        [Input('edit_datatable', 'data'),
        Input('raw_pivot','value'),
        Input('metric_dropdown','value'),
        Input('uploaded-data-dim-order','children')
        ])
    def update_link(rows,table_viewing_mode,dimensions, dim_order):
        
        dff = pd.DataFrame(rows)
        # relative_filename = os.path.join(
        #     'download_directory',
        #     'downloadFile_{}_{}.xlsx'.format(table_viewing_mode,str(int(time.time())))     
        # )
        # absolute_filename = os.path.join(os.getcwd(), relative_filename)
        # writer = pd.ExcelWriter(absolute_filename,engine='xlsxwriter')

        if table_viewing_mode == 'raw':
            csv_string = dff.to_csv(index=False, encoding='utf-8')
            csv_string = "data:text/csv;charset=utf-8," + urllib.parse.quote(csv_string)        # %EF%BB%BF
            # return '/dash/urlToDownload?value={}'.format(dff.to_json(orient='split'))

        elif table_viewing_mode == 'pivot':

            dim_idx_mapping = json.loads(dim_order)
            sorted_dims = sorted(dimensions, key=lambda x: dim_idx_mapping[x], reverse = False)   

            pivot_dff = dff.groupby(by=sorted_dims).apply(groupby_multi_func).reset_index()

            pivot_dff = linked_metrics(pivot_dff, sorted_dims, "metric_dimensions")

            csv_string = pivot_dff.to_csv(index=False, encoding='utf-8')
            csv_string = "data:text/csv;charset=utf-8," + urllib.parse.quote(csv_string)

            # pivot_dff.to_excel(writer, 'Sheet1')
            # writer.save()

        return csv_string
        # '/{}'.format(relative_filename)
            # return '/dash/urlToDownload?value={}'.format(pivot_dff.to_json(orient='split') )




    # @app.server.route('/dash/urlToDownload')
    # def download_csv():
    #     value = flask.request.args.get('value')

    #     dff = pd.read_json(value, orient='split', dtype = {'metric_key': 'object'})

    #     # create a dynamic csv or file here using `StringIO` (instead of writing to the file system)
    #     str_io = io.StringIO()
    #     dff.to_csv(str_io)

    #     mem = io.BytesIO()
    #     mem.write(str_io.getvalue().encode('utf-8'))
    #     mem.seek(0)
    #     str_io.close()
    #     return flask.send_file(mem,
    #                     mimetype='text/csv',
    #                     attachment_filename='downloadFile.csv',
    #                     as_attachment=True)



    # @app.server.route('/download_directory/<path:path>')
    # def serve_static(path):
    #     root_dir = os.getcwd()
    #     return flask.send_from_directory(
    #         os.path.join(root_dir, 'download_directory'), path
    #     )









    # model strength --- indicator
    @app.callback(Output("model_strength", "children"),
                [Input("edit_datatable", "data")])
    def first_cases_indicator_callback(rows):
        dff = pd.DataFrame(rows)
        if dff.shape[0] != 0:
            m_s = round(dff['model_strength'].max(),2)
            return m_s
        else:
            raise PreventUpdate






    # total number of metric keys --- indicator
    @app.callback(Output("total_mk", "children"),
                [Input("edit_datatable", "data")])
    def second_cases_indicator_callback(rows):
        dff = pd.DataFrame(rows)
        if dff.shape[0] != 0:
            mk_num = len(dff['metric_key'].unique())
            return mk_num
        else:
            raise PreventUpdate







    # total number of conversions --- indicator
    @app.callback(Output("total_c", "children"),
                [Input("edit_datatable", "data")])
    def third_cases_indicator_callback(rows):
        dff = pd.DataFrame(rows)
        if dff.shape[0] != 0:
            total_c = round(dff['total_conversions'].max(),2)
            return total_c
        else:
            raise PreventUpdate






    # total spend --- indicator
    @app.callback(Output("total_s", "children"),
                [Input("edit_datatable", "data")])
    def fourth_cases_indicator_callback(rows):
        dff = pd.DataFrame(rows)
        if dff.shape[0] != 0:
            total_s = round(dff['spend'].sum(),2)
            return total_s
        else:
            raise PreventUpdate






    # # hide/show modal
    # @app.callback(Output("info_modal", "style"),
    #             [Input("info_button", "n_clicks")])
    # def display_leads_modal_callback(n):
    #     if n > 0:
    #         return {"display": "block"}
    #     return {"display": "none"}





    # # reset to 0 add button n_clicks property
    # @app.callback(Output("info_button", "n_clicks"),
    #             [Input("info_modal_close", "n_clicks")],
    # )
    # def close_modal_callback(n):
    #     return 0

