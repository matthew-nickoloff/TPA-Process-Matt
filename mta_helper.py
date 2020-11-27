# -*- coding: utf-8 -*-
"""
Created on Mon Jan 28 11:12:43 2019

helper functions for MTA app 

@author: yuanjing.han
"""

import base64
import io
import pandas as pd

import dash
import dash_html_components as html
import flask
import textwrap

import colorlover as cl
import plotly.graph_objs as go
from constant import *

font_fam=""" -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, "Noto Sans", sans-serif, "Apple Color Emoji", "Segoe UI Emoji", "Segoe UI Symbol", "Noto Color Emoji" """



# background info of the app
def MTA_info_modal():
    return html.Div(
        html.Div(
            [
                html.Div(
                    [

                # modal header
                html.Div(
                    [
                        html.Span(
                            "MTA model info",
                            style={
                                "color": "#506784",
                                "fontWeight": "bold",
                                "fontSize": "20",
                            },
                        ),
                        html.Span(
                            "×",
                            id="info_modal_close",
                            n_clicks=0,
                            style={
                                "float": "right",
                                "cursor": "pointer",
                                "marginTop": "0",
                                "marginBottom": "17",
                            },
                        ),
                    ],
                    className="row",
                    style={"borderBottom": "1px solid #C8D4E3"},
                ),

                # modal text
                html.Div(
                    [     
                        html.P(["Digital advertising is a multi-billion-dollar industry and is growing dramatically each year. \
                                 However, accuracy in measuring the efficiency and effectiveness of an online ad campaign remains elusive. \
                                 Incomplete information due to issues such as cookie deletion, non-addressable data, and digital fraud all \
                                 cause tremendous “noise” when modeling the consumer decision journey. Poor information inevitably results \
                                 in poor estimates and incorrect inferences when relying on traditional statistical models. For example, one \
                                 can observe a significant portion of marketing activities show negative impacts on conversion estimated from \
                                 the conventional discrete choice model, meaning people are more likely to convert if they receive fewer ads. \
                                 This is very counter-intuitive."]),
                            
                        html.P(["Analytic Partners (AP) presents an adaptive attribution modeling algorithm that iteratively profiles cookie \
                                 data to filter out the noise and, leveraging logistic regression and game theory to mathematically attributes \
                                 incremental sales to the appropriate ad campaign. Analytic Partners has also developed a new metric system and \
                                 validation process to systematically evaluate and compare the predictive power of different models based on \
                                 profiling data. This adaptive attribution model helps marketers to further calculate online marketing ROIs and \
                                 effectively develop future marketing strategies."])
                                                            
                    ],
                        
                className="row",
                style={"padding": "2% 8%",
                       "textAlign": "left"},
            ),

                ],
                className="modal-content",
                style={"textAlign": "center"},
            )
        ],
        className="modal",
    ),
    id="info_modal",
    style={"display": "none"},
)






################################################################################################################################################################







# KPI card
def indicator(color, text, id_value):
    return html.Div(
        [

            html.P(
                text,
                className="twelve columns indicator_text"
            ),
            html.P(
                id = id_value,
                className="indicator_value"
            ),
        ],
        className="four columns indicator",
    )







# Upload component: parse csv/excel files into data frame

def parse_contents(contents, filename):
    content_type, content_string = contents.split(',')

    decoded = base64.b64decode(content_string)
    try:
        if 'csv' in filename:
            # Assume that the user uploaded a CSV file
            df = pd.read_csv(io.StringIO(decoded.decode('utf-8')))

        elif 'xlsx' in filename:
            # Assume that the user uploaded an excel file
            df = pd.read_excel(io.BytesIO(decoded))

    except Exception as e:
        return None

    return df






    
def create_hbar(pivot_dff,x_var,y_var):
    
    data = [
        go.Bar(
            x = pivot_dff[x_var].round(4),
            y = pivot_dff[y_var],
            orientation="h",

            marker=dict(
                color='#75AADB',
                line=dict(
                    color='#75AADB',
                    width=1.5,
                    )),
            opacity=0.8
        )] # x could be any column value since its a count
    
    # Please don't add brackets around layout like what we do to data - it will make h-bar chart look very narrow
    layout = dict(
        font=dict(
            size=10, 
            color=chart_trace_cl,
            family=font_fam),
        xaxis=dict(
           autorange=True,
           showgrid=False,
           zeroline=False,
           showline=False,
           ticks='',
           showticklabels=True
        ),
               
        yaxis=dict(
           autorange=True,
           showgrid=False,
           zeroline=False,
           showline=False,
           ticks='',
           showticklabels=True
        ),
            
        barmode="stack",
        margin=dict(l=250, r=25, b=0, t=0, pad=4),
        hovermode='closest',
        paper_bgcolor="white",
        plot_bgcolor="white",
        height=300
    )

    return {"data": data, "layout": layout}







# grouped bar chart

def create_grouped_bar(dff,x_var,y_var,*arg):
    
    dff = dff.sort_values(by=[y_var])
    
    trace1 = go.Bar(
        x = dff[x_var],
        y = dff[y_var],
        name = ''
        )
   
    trace2 = go.Bar(
        x = dff[x_var],
        y = dff[arg[1]],
        name=''
        )
    
    data = [trace1, trace2]
        
    layout = go.Layout(
        font=dict(family=font_fam),
        xaxis=dict(
            autorange=True,
            showgrid=True,
            zeroline=True,
            showline=True,
            ticks='',
            showticklabels=True
        ),
                
        yaxis=dict(
            autorange=True,
            showgrid=True,
            zeroline=True,
            showline=True,
            ticks='',
            showticklabels=True
        ),    
        
        barmode='group',
#       margin=dict(l=30, r=0, b=0, t=0, pad=0),
        margin=dict(l=250, r=25, b=20, t=0, pad=4),
        paper_bgcolor="white",
        plot_bgcolor="white",
        height=300,
    )
    
    return {"data": data, "layout": layout}
 






# pie chart

def create_pie(
    dff,
    pie_var,
    labels):

    for i in range(len(labels.tolist())):
        labels[i] = '<br>'.join(textwrap.wrap(labels[i], width=45))

    layout = go.Layout(
        font=dict(
            size=12, 
            color=chart_trace_cl,
            family=font_fam),
        margin=dict(l=0, r=0, b=0, t=4),
        legend=dict(x=0.6, y=1, font=dict(size=10), orientation="v"),
        paper_bgcolor="white",
        plot_bgcolor="white",
        height=300,
        autosize=True
    )

    data = [go.Pie(
        values = dff[pie_var],
        labels = labels,
        # marker={"colors": cl.scales['div']['RdBu']},
        marker={ "colors": bluish_cl + greenish_cl + reddish_cl + yellowish_cl + pinkish_cl },
        domain={"x": [0,0.5] },
        showlegend=True
        # opacity=0.8
    )]

    fig = go.Figure(data=data,layout=layout)

    return fig






# donut chart

# def create_donut(dff,pie_var,labels):

#     values = dff[pie_var]

#     layout = go.Layout(
#         # margin=dict(l=0, r=0, b=0, t=4, pad=8),
#         legend=dict(orientation="v"),
#         paper_bgcolor="white",
#         plot_bgcolor="white",
#     )

#     trace = go.Pie(
#                 values = values,
#                 labels = labels,
#                 # marker={"colors": cl.scales[str(num_colors)]['div']['RdBu'][:num_colors]},
#                 # opacity=0.8
#     )

#     fig = {
#         "data": [
#             {
#             "values": ,
#             "labels": [
#                 "US",
#                 "China",
#                 "European Union",
#                 "Russian Federation",
#                 "Brazil",
#                 "India",
#                 "Rest of World"
#             ],
#             "domain": {"x": [0, .48]},
#             "name": "GHG Emissions",
#             "hoverinfo":"label+percent+name",
#             "hole": .4,
#             "type": "pie"
#             },
#             {
#             "values": [27, 11, 25, 8, 1, 3, 25],
#             "labels": [
#                 "US",
#                 "China",
#                 "European Union",
#                 "Russian Federation",
#                 "Brazil",
#                 "India",
#                 "Rest of World"
#             ],
#             "text":["CO2"],
#             "textposition":"inside",
#             "domain": {"x": [.52, 1]},
#             "name": "CO2 Emissions",
#             "hoverinfo":"label+percent+name",
#             "hole": .4,
#             "type": "pie"
#             }],
#         "layout": {
#                 "title":"Global Emissions 1990-2011",
#                 "annotations": [
#                     {
#                         "font": {
#                             "size": 20
#                         },
#                         "showarrow": False,
#                         "text": "GHG",
#                         "x": 0.20,
#                         "y": 0.5
#                     },
#                     {
#                         "font": {
#                             "size": 20
#                         },
#                         "showarrow": False,
#                         "text": "CO2",
#                         "x": 0.8,
#                         "y": 0.5
#                     }
#                 ]
#             }
#         }


#     return {"data": [trace], "layout": layout}






# dropdown based on uploaded data
def create_dropdown_list(df):

    metric_name_list=[col for col in df.columns if '_name' in col]
    metric_level_list=[w.replace('_name', '') for w in metric_name_list]
    dropdown_list=[{'label': key, 'value': value} for (key,value) in zip(metric_level_list,metric_name_list)]
    
    return dropdown_list    






# apply multiple functions to multiple groupby columns: "https://stackoverflow.com/questions/14529838/apply-multiple-functions-to-multiple-groupby-columns"
def groupby_multi_func(x):
    d = {}
    d['spend'] = x['spend'].sum()
    d['converted_users'] = x['converted_users'].sum()
    d['Incremental_Conversion'] = x['Incremental_Conversion'].sum()
    d['ROI'] = ((x['Incremental_Conversion'] * x['margin']).sum())/(x['spend'].sum())
    return pd.Series(d,index=['spend','converted_users','Incremental_Conversion','ROI'])






# link dimensions based on granularity (least --> most) and create a new column of linked-dimensions
def linked_metrics(dff, sorted_dims, col_name):

    for i in range(len(sorted_dims)):
        if i == 0:
            dff[col_name] = dff[sorted_dims[i]]
        else:
            dff[col_name] = dff[col_name].astype(str) + "_" + dff[sorted_dims[i]]
    # print(dff['metric_dimensions'])
    return dff





# reorder columns
def reorder_columns(df):
    dimension_names=[]
    dimensions=[]
    for col_name in df.columns:
        if "_name" in col_name and col_name != 'tactic_name':
            dimension_names.append(col_name)
    for dimension in dimension_names:
        dimensions.append(dimension.replace("_name",""))
    KPIs=[i for i in list(df) if i not in dimension_names and i not in dimensions and i not in ['metric_key']]
    cols=dimension_names+KPIs+dimensions+['metric_key']
    
    dff = df[cols]
    return dff






# Define a class to track whether a process is still running
class Semaphore:
    def __init__(self, filename='semaphore.txt'):
        self.filename = filename
        with open(self.filename, 'w') as f:
            f.write('done')

    def lock(self):
        with open(self.filename, 'w') as f:
            f.write('working')

    def unlock(self):
        with open(self.filename, 'w') as f:
            f.write('done')

    def is_locked(self):
        return open(self.filename, 'r').read() == 'working'

