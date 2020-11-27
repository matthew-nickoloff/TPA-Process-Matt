# This Python file uses the following encoding: utf-8
import dash_html_components as html

# col_style_0 = "column col-6 col-xl-6 col-lg-6 col-md-12 col-sm-12"

layout = [

    html.Div(
        children=[
            html.Div(
                className="col-6",
                children=[
                    html.Br(),
                    html.H4("MTA GUI User Documentation"),
                    html.Details([
                        html.Summary("How does MTA GUI work?"),
                        html.P("MTA GUI includes three components: modelling, visualization and info page you’re looking at. In Modelling page, users are able \
                                to type in table names or choose from dropdown lists to provide the information the model requires. If the inputs are invalid, \
                                or tables required for computing don't exist... the according error messages will be printed into the console to guide users to find \
                                where the problems are. When the model is running, at each essential step, other processing messages will also be printed, such as how \
                                long it takes to load, process and model the data; in visualization page, users can upload the file generated after modelling process, \
                                choose the methodology and metric key dimensions, then see the most important indicators as well as charts to do sanity check and gain \
                                insights. If users needs to change labels of metric key names, they can make changes in the bottom-left conner of the visualization page \
                                and the changes will be applied to the other three charts immediately.")
                    ]),
                       
                    html.Details([
                        html.Summary("What are the model inputs? What are their formats?"),
                        html.Li("Database Name: choose the database you're using to run MTA model."),

                        # should figure out how to start a new line in dash html component 
                        html.Li("Table Name (tokenized): Tokenized table: input table at Redshift which contains all the information we need for \
                                the MTA model. It is a merged table of activity, impressions and clicks. Each row is an \
                                impression or click. Activity data are used in the cm1 column. It will later be transformed \
                                into a modeling-capable format."),
                        html.Li("Metric Key Explanation Table: a dimension table in Redshift explaining the metric key and related dimension names."),
                        html.Li("Rank Order Table: identifies dimension order across different activities. Available only under Multiple Activities option."),
                        html.Li("Mix Master Dir: the directory where model output stores."),
                        html.Li("Time Discount: can only take binary values. 1 when applying time deflation to ads when ads are seen long before conversions."),
                        html.Br()
                    ]),

                    html.Details([
                        html.Summary("What is our methodology?"),
                        html.P("Analytic Partners (AP) presents an adaptive attribution modeling algorithm that iteratively profiles cookie data to filter \
                                out the noise and, leveraging logistic regression and game theory, and mathematically attributes incremental sales to the \
                                appropriate ad campaign. Analytic Partners has also developed a new metric to systematically evaluate and compare the \
                                predictive power of different models based on profiling data. This adaptive attribution model helps marketers to further \
                                calculate online marketing ROIs and effectively develop future marketing strategies.")
                    ]),

                    html.Details([
                        html.Summary("MTA Modelling Platform"),
                        html.Div([
                            html.Img(src='/assets/MTA_model_sidebar.PNG',
                                    style={'align': 'left',
                                            'float': 'left',
                                            'width': '30%',
                                            'height': 'auto',
                                            'padding-right': '5px'}),
                            html.P("Choose database name from the first dropdown menu, type in the exact names of Table Name (tokenized) and Metric \
                                table, you may or may not want to change Mix Master Directory (it’s up to you), decide whether to apply time discount \
                                and finally click the “Run” button. When the model is running, the button below “Run” button will display “Running…”; \
                                when the model finishes running, it will display “Free”. If you happen to click the “Run” button a second time when the \
                                model is already running, don’t worry, you will not interrupt the current process. The model will continue to run until \
                                the Mix master file is generated. If you happen to close the browser when your model is running, don’t worry, the model \
                                is still running at the backend. You can check the Mix Master Directory you assigned for the output file after a while.",
                                style={'align': 'right'}),
                            html.Br(),
                            html.Img(src='/assets/console_messages.PNG',
                                    style={'align': 'left',
                                            'float': 'top',
                                            'width': '100%',
                                            'height': 'auto'}),
                            html.P("When the model is running, certain messages will be printed into the console which indicates the model’s running processes. \
                                    The screenshot above is an example of a successful model running (Mix Master file……has been generated!) When exceptions occur, \
                                    you will see other messages like “Some Redshift Connection/Table Info is missing. Please check and submit again!”, or “Some of \
                                    the tables does not exist! Please check and try again!” etc. These messages will give you a sense of where the process gets wrong \
                                    and how to correct it.",
                                    style={'align': 'bottom'})
                        ]),
                    ]),

                    html.Details([
                        html.Summary("Output Visualization"),
                        html.Div([
                            html.Img(src='/assets/upload_dropdowns_visuals.PNG',
                                    style={'align': 'left',
                                            'float': 'top',
                                            'width': '90%',
                                            'height': 'auto'}),
                            
                            html.P("Click the red button to upload the output file from MTA model. Choose one methodology from the left dropdown \
                                    menu (e.g. Forward, Backward, Last Click, [Game Theory]) \
                                    and one/multiple dimensions from the right dropdown menu to filter and pivot the data you uploaded. Then \
                                    the following dashboard will show up (one example).",
                                    style={'align': 'bottom'}),

                            html.Img(src='/assets/MTA_charts_table.PNG',
                                    style={'align': 'left',
                                            'float': 'top',
                                            'width': '100%',
                                            'height': 'auto'}),
                            html.P("The table at the bottom left corner is editable: you can edit it and the changes will be applied \
                                    immediately to the other three charts (this one can be very helpful when you want to change the labels of data.)",
                                    style={'align': 'bottom'}),    

                            html.Img(src='/assets/editable_table.jpg',
                                    style={'align': 'left',
                                            'float': 'top',
                                            'width': '100%',
                                            'height': 'auto'}),
                            html.P("This editable Mix Master table also provides two kinds of capabilities of filtering data: 1) Equals: eq. Example: \
                                    type in eq Facebook to filter “Site_Name”. No quotes are necessary for text. 2) Greater than > and less than < operations. \
                                    Example: type in > 50 to filter 'converted_users'.",
                                    style={'align': 'bottom'}), 

                            html.Img(src='/assets/download_raw_pivot.PNG',
                                    style={'align': 'left',
                                            'float': 'top',
                                            'width': '30%',
                                            'height': 'auto'}),
                            html.P("When “Raw” is chosen, after you click “download”, you’ll download the raw data --- the data currently display in the table \
                                    at the bottom left corner; when “Pivot” is chosen, after you click “download”, you’ll download the pivot table based on the \
                                    dimensions you already choose at the top right dropdown menu.",
                                    style={'align': 'bottom'}),               
                        ]),
                    ]),
                ], style={'padding-left':'25px','padding-top':'20px'}             
            ),
            
            html.Div(
                className="col-6",
                # className="col_style_0",
                children=[
                    # html.Img(
                    #     src="https://analyticpartners.com/wp-content/uploads/2017/05/AP-UMIA-GraphicAnim.gif"
                    # ),
                    # html.Img(src='assets/MTA_methodology.png')
                ],
                style={'width': '50%', 'display': 'inline-block','float': 'middle'}
            ),

            
        ]
    )
]


# className="info_page_header",

    #     html.Details([
    #         html.Summary("What are model inputs? What do they mean?"),
    #         html.Ul(
    #             html.Li("Database Name: name of the database you're using for specific MTA project."),
    #             html.Li("Tokenized Table Name: input table at Redshift which contains all the information we need for the MTA model. \
    #                     It is a merged table of activity, impressions, and clicks. Each row is an impression or clicks, activity data \
    #                     are used in the cm1 column. It will later be transformed into a modeling format. One possible table format of \
    #                     tokenized_table is shown below:"),

    #             # html.Ul(
    #             #     html.Li("userID: ID of different users"),
    #             #     html.Li("timestamp: timestamp of impressions or clicks"),
    #             #     html.Li("m1-m10: dimensions represent different sites, placement, site, etc. The dimensions are ordered by granularity, \
    #             #             the difference between two different m1 larger than another dimension. For example, m1 is usually campaign, \
    #             #             while m10 is usually the size of banner ads. This property is used in the imputation process to find similar ads."),
    #             #     html.Li("metric_key: a unique combination of all dimensions m1-m10")),
            
    #             # html.Li("Metric Key Explanation Table: a list of a string of names of a dimension table in Redshift explaining the metric_key and \
    #             #         related dimension names.")
    #                     ),
    #             html.Br(),  
    # ]),      
