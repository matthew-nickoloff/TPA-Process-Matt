import os
import dash
import flask
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State

from views.visualization import layout as v_layout
from views.modelling import layout as m_layout
from views.info import layout as i_layout

from controller.modelling import register_callback_m
from controller.visualization import register_callback_v
import logging
import authentication



# external stylesheets
external_stylesheets = [
    {
        'href': 'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.8.1/css/all.min.css',
        'rel': 'stylesheet',
        'integrity': 'sha256-7rF6RaSKyh16288E3hVdzQtHyzatA2MQRGu0cf6pqqM=',
        'crossorigin': 'anonymous'
    }
]


# external JavaScript files
external_scripts = [
    {'src': 'https://bootswatch.com/_vendor/jquery/dist/jquery.min.js'},
    {'src': 'https://bootswatch.com/_vendor/popper.js/dist/umd/popper.min.js'},
    {'src': 'https://bootswatch.com/_vendor/bootstrap/dist/js/bootstrap.min.js'},
    {'src': 'https://bootswatch.com/_assets/js/custom.js'},
]


server = flask.Flask(__name__)
app = dash.Dash(__name__, server=server, external_scripts=external_scripts, external_stylesheets=external_stylesheets)
app.config.suppress_callback_exceptions = True
app.title = "MTA"


isDev = os.environ.get('env', "") == "production"
if isDev:
    logging.warning('in dev mode, no auth.')
else:
    authentication.addAuth(app)


register_callback_m(app)
register_callback_v(app)



app.layout = html.Div(
    id='main-page-content',
    children=[
        html.Nav(
            className="navbar navbar-expand-lg navbar-dark bg-dark",
            children=[

                html.A(
                    html.H3('Touchpoint Analytics'),
                    className='navbar-brand',
                    href='https://en.wikipedia.org/wiki/Attribution_(marketing)'
                ),

                html.Button(
                    html.Span(className="navbar-toggler-icon"),
                    className="navbar-toggler",
                    type="button",
                    ** {
                        'data-toggle': "collapse", 
                        'data-target': "#navbarResponsive",
                        'aria-controls': "navbarResponsive", 
                        'aria-expanded': "false", 
                        'aria-label': "Toggle navigation"
                    },
                ),
                
                html.Div(
                    className="collapse navbar-collapse",
                    id="navbarResponsive",
                    children=[
                        html.Ul(
                            className="nav navbar-nav",
                            children=[

                                html.Li(
                                    html.A(
                                        'MODELING', 
                                        href='/',
                                        className="nav-link"
                                        ),
                                    className="nav-item"
                                    ),
                                
                                html.Li(
                                    html.A(
                                        'VISUALIZATION', 
                                        href='/visualization',
                                        className="nav-link"
                                        ),
                                    className="nav-item"
                                    ),

                                html.Li(
                                    html.A(
                                        'INFO', 
                                        href='/info',
                                        className="nav-link"
                                        ),
                                    className="nav-item",
                                    ),
                                ]),
                    ]
                ),

                html.A(
                    html.Img(
                        src="/assets/AP_Logo-H-White-new.png",
                        alt=""
                    ),
                    className='float-right',
                    href='https://analyticpartners.com/'
                ),
                    
            ]
        ),

        dcc.Location(id='url', refresh=False),
        html.Div(id="dashboard-content"),

])



# Update the index
@app.callback(dash.dependencies.Output('dashboard-content', 'children'),
              [dash.dependencies.Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/':
        return m_layout
    elif pathname == '/visualization':
        return v_layout
    elif pathname == '/info':
        return i_layout
   

if __name__ == '__main__':
    logging.basicConfig(filename='error.log',
                        level=logging.INFO,
                        format="%(asctime)-15s %(levelname)s: %(message)s",
                        datefmt='%m/%d/%Y %H:%M:%S')
#     app.run_server(debug=True)

    app.run_server(debug=True,
                    host='0.0.0.0',
                    port=8000)
    
    
