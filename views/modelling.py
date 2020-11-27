import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc

import MTAfunction as MTA
import config
from constant import DBLIST


layout = [
    html.Div(
        className="container-fluid row",
        children=[
            html.Div(
                className="col-3",
                children=[
                    html.Br(),
                    html.Div(
                        className="card bg-light mb-3 sideBarCard card-scrollbar",
                        children=[

                            html.Div(
                                className="card-header",
                                # className="card-title",
                                children=[html.H4("Model Inputs")]
                            ),

                            html.Div(
                                className="card-body",
                                children=[
                                    
                                    html.Br(),

                                    dbc.FormGroup([
                                        html.H6("Database Name"),
                                        dcc.Dropdown(
                                            id='dbname-dropdown',
                                            options=DBLIST,
                                            placeholder="Select Database"
                                        ),
                                    ]),
                                    html.Br(),

                                    dbc.FormGroup([
                                        html.H6("Table Name (tokenized)",id="tokenized_table_label"),
                                        dbc.Input(
                                            type="text", 
                                            id="tokenized_table_name", 
                                            placeholder="Enter tokenized table name",
                                        ),
                                        dbc.Tooltip("m0 indicates tactic(s).",target="tokenized_table_label"),
                                    ]),
                                    html.Br(),
                                    

                                    dbc.FormGroup([
                                        html.H6("Metric Table",id="metric_table_label"),
                                        dbc.Input(
                                            type="text", 
                                            id="metric_key_explanation_table", 
                                            placeholder="Enter metric key explanation table name",
                                        ),
                                        dbc.Tooltip("Contains mapping for tactic(s).",target="metric_table_label"),
                                    ]),
                                    html.Br(),

                                    dbc.FormGroup(
                                        id="dim_rank",
                                        children=[
                                            html.H6("Rank Order Table",id="rank_order_label"),
                                            dbc.Input(
                                                type="text", 
                                                id="dimension_ranking_table", 
                                                placeholder="Enter rank order table name",
                                            ),
                                            dbc.Tooltip("Contains dimension order across tactic(s).",target="rank_order_label"),
                                        ]),
                                    html.Br(),


                                    dbc.FormGroup([
                                        html.H6("KPI Column Name"),
                                        dbc.Input(
                                            type="text", 
                                            value="cm1",
                                            id="kpi_col", 
                                            placeholder="Enter KPI column Name",
                                        ),
                                    ]),
                                    html.Br(),


                                    dbc.FormGroup([

                                        dbc.Col(
                                            html.H6("Have User Segment Info?"),
                                            width=8
                                        ),

                                        dbc.Col([
                                            dbc.RadioItems(
                                                id="use_user_info",
                                                options=[
                                                    {'label':'Y','value':1},
                                                    {'label':'N','value':2}],
                                                value=2,
                                                inline=True
                                            )],
                                            width=4,
                                            style={'text-align': 'right'}
                                        ),
                                    ], row=True, style={'margin-bottom':'0px'}),

                                    dbc.FormGroup(
                                        id="user_seg_formgroup",
                                        children=[
                                            dbc.Input(
                                                type="text", 
                                                value="",
                                                id="user_seg_col", 
                                                placeholder="Enter user segment column name",
                                                disabled=True
                                            ),
                                            dbc.Checklist(
                                                id='mta_partial_seg',
                                                options=[{"label": "Only have user segment info for converted users? Run MTA with each converted segment against all non-converted users at your own risk.", \
                                                        "value": 1, "labelStyle":{'fontColor': ''} } ],
                                                values=[],
                                                labelStyle={"color": "red"}, 
                                            )
                                    ], style={'display':'none'}),
                                    html.Br(),

                                    dbc.FormGroup([
                                        html.H6("Mix Master Directory",id="directory_label"),
                                        dbc.Input(
                                            type="text", 
                                            id="directory", 
                                            placeholder="Enter S3 directory name in analytic-parners bucket",
                                            value='datascience/MTA_output/',
                                        ),
                                        dbc.Tooltip("Please add project or client name after datascience/MTA_output/, e.g. a valid mix master diectory would be datascience/MTA_output/projectname/",target="directory_label"),
                                    ]),
                                    html.Br(),
                                    
                                    dbc.FormGroup([
                                        html.H6("Time Discount"),
                                        dbc.Input(
                                            type="number", 
                                            id="timediscount", 
                                            placeholder="Enter timediscount",
                                            value=1,
                                            min=0,
                                            max=1
                                        ),
                                    ]),
                                    html.Br(),
                                    html.Br(),
                                    

                                    dbc.FormGroup(
                                        children=[
                                            dbc.Button(
                                                "Run",
                                                id="my_run_button",
                                                n_clicks_timestamp=123, 
                                                n_clicks=0,
                                                # color="info", 
                                                block=True,
                                                disabled=False,
                                                style={
                                                    'color': '#ffffff',
                                                    'background-color': '#971B2F',
                                                    'border-color': '#7C2128'
                                                },
                                            ),
                                        ] ),
                                    
                                    dcc.Interval(id='interval', interval=500, n_intervals=0),
                                    html.Br(),
                                    dbc.RadioItems(id='lock',
                                        options=[{'label': i, 'value': i} for i in ['Running...', 'Free']],
                                        value='',
                                        inline=True,
                                        style={'display':'none'} ),
                                ],
                            )
                        ]
                    ),
                ]
            ),

            html.Div(
                className="col-9",
                children=[
                    html.Br(),
                    html.Div(
                        className="card bg-light mb-3 mainBarCard",
                        children=[

                            html.Div(
                                className="card-body",
                                children=[
                                    dbc.Textarea(
                                        id="console_out",
                                        bs_size="sm",
                                        className="mb-3 card-scrollbar",
                                        value="",
                                        readOnly=True,
                                        style={
                                            'width':'100%',
                                            'height':'100%'
                                        }
                                    ),
                                ],
                                style={'padding':'5px 5px 5px 5px'} )
                        ]),
   
                ]
            ),
            
            html.Div(id="output_keypress"),
            html.Br(),
            html.Br(),

            dcc.Interval(id='interval2',interval=500,n_intervals=0),

        ], style={'padding-right': '0px'}
    )]





# dbc.FormGroup([
#     dbc.Col(
#         dbc.RadioItems(
#             options=[
#                 {'label': 'Single Activity', 'value': 0},
#                 {'label': 'Multiple Activities', 'value': 1}],
#             id="single_multiple",
#             value=0,
#             inline=True,
#             style={'font-weight': 'normal'}
#         ), width=9, style={'padding-right': '0px'} ),
#     dbc.Col(
#         dbc.Badge("NEW!",color="success",id="new_feature",className="mr-1"),
#         width=3,
#         style={'padding-left': '0px'} ),
#     dbc.Tooltip("CHECK OUT NEW FEATURE HERE!!! Touchpoint Analytics now allows multiple activities data with different dimension order across activities.",target="new_feature")
#     ], row=True
# ),
