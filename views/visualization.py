import pandas as pd
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
from mta_helper import indicator, parse_contents, MTA_info_modal


layout = [html.Div([    
            dbc.Row(
                [
                    # upload 
                    dbc.Col(
                        dcc.Upload(
                            id='upload_data',
                            multiple=False,
                            style={'padding-top': '2px'},
                            children=html.Button(
                                'Choose File', 
                                className="form-control-file")), 
                        style={'padding-right':'0px','padding-left':'10px'},
                        width=2, 
                    ),

                    # tactic
                    dbc.Col(
                        dcc.Dropdown(
                            id='tactic_dropdown',
                            className="tab1_form_width",
                            multi = False,
                            clearable = False,
                            placeholder='Tactic(s)'),
                            style={'padding-right':'0px','padding-left':'10px'},
                        width=1
                    ),

                    dbc.Col(
                        dcc.Dropdown(
                            id='user_seg_dropdown',
                            className="tab1_form_width",
                            multi = False,
                            clearable = False,
                            placeholder='User Segment(s)',
                            disabled=True ),
                            style={'padding-right':'0px','padding-left':'10px'},
                        width=1
                    ),
                    
                    # method
                    dbc.Col(
                        dcc.Dropdown(
                            id='method_dropdown',
                            className="tab1_form_width",
                            multi = False,
                            clearable=False,
                            placeholder='Methodology'), 
                            style={'padding-right':'0px','padding-left':'10px'},
                        width=3
                    ),
                    
                    # metric
                    dbc.Col(
                        dcc.Dropdown(
                            id='metric_dropdown',
                            className="tab1_form_width",
                            multi = True,
                            clearable=True,
                            placeholder='Dimension(s)'),
                        width=5,
                        style={'padding-right':'0px','padding-left':'12px'},
                    ),
                ],
                style={'padding-bottom':'10px'}
            ),

            html.Div(id='content'),


            # Hidden Div 1: user-uploaded data 
            html.Div(id="uploaded-data-container", style={'display': 'none'} ),

            # Hidden Div 2: user-uploaded data's dimension orders (least --> most granular) 
            html.Div(id="uploaded-data-dim-order", style={'display': 'none'} ),
    ],

    style={'margin-left':'50px','margin-right':'50px','padding-top':'10px'}
    )   
]