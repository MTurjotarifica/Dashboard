from flask import Flask, render_template, request
import pymysql
import mysql.connector as mysql
import numpy as np
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy import text as sqlalctext  # edit st 2023-03-07
import pandas as pd
import scipy.signal

import matplotlib.dates as mdates
from datetime import datetime
import plotly.graph_objects as go


app = Flask(__name__)

# Your code for the visualization function and other utilities goes here
engine = create_engine(
    'mysql+pymysql://sandbox_read_only:ThX*AXrE%1W4X27@mysqldatabase.cmi5f1vp8ktf.us-east-1.rds.amazonaws.com:3306/sandbox')

# creating a connection object
connection = engine.connect()


# SQL query
stmt = "SELECT * FROM digital_demand WHERE (gt_category = 13) AND (country = 'DE') and (date >= '2022-11-01')"

# Execute the query and store the result in a DataFrame
df_raw = pd.read_sql(sqlalctext(stmt), connection)


connection.close()

df_raw.date = pd.to_datetime(df_raw['date'])

text = "vodafone"
texts = ["vodafone", "o2", "telekom", "1und1"]
init_date = "2022-12-01"
index_date = "2022-12-01"
output_type = "png"


def add_ma(df, var, window):

    var_new = var + '_ma'  # new ma variable to be added to df
    df = df.sort_values(by=['keyword',
                            'gt_category',
                            'country',
                            'date'
                            ])
    df[var_new] = df.groupby(['keyword',
                              'country',
                              'gt_category'
                              ])[var].transform(lambda x: x.rolling(window).mean())  # compute moving average

    df = df.rename(columns={var_new: var_new+str(window)})
    return df


def add_smoother(df, var, cutoff):

    b, a = scipy.signal.butter(3, cutoff)
    var_new = var + '_smooth'  # new ma variable to be added to df
    df = df.sort_values(by=['keyword',
                            'gt_category',
                            'country',
                            'date'
                            ])
    df[var_new] = df.groupby(['keyword',
                              'country',
                              'gt_category'
                              ])[var].transform(lambda x: scipy.signal.filtfilt(b, a, x))  # compute moving average
    return df


def add_indexing(df, var, index_date):

    var_ref = var + '_ref'  # variable for index computation
    var_new = var + '_index'  # new index variable to be added to df
    # create reference df with values from indexdate
    df_ref = df[df['date'] == index_date]
    df_ref = df_ref.rename(columns={var: var_ref})  # rename to avoid confusion
    # Add values of indexdate to original dataframe and compute index values
    df_w_index = pd.merge(df, df_ref[['keyword',
                                      'country',
                                      'gt_category',
                                      var_ref]],
                          how="left",
                          on=['keyword',
                              'country',
                              'gt_category'
                              ])
    df_w_index[var_new] = (df_w_index[var]/df_w_index[var_ref])*100
    return df_w_index


# we are creating manuals parameter dictionary for function values at the moment
params = {'key': f'{text.lower()}',
          'geo': 'DE',
          'cat': 13,
          'startdate': f'{init_date}',
          'index': True,
          'indexdate': f'{index_date}',
          'font_use': 'Roboto Mono Light for Powerline',
          'out_type': 'png'
          }

# function that produces and saves the vis


def single(texts, key, geo, cat, startdate, index, indexdate, font_use, out_type):
    fig = go.Figure()

    for text in texts:
        # df_key = df_raw[(df_raw.keyword == text.lower()) & (df_raw.country == geo) & (df_raw.gt_category == int(cat))]

        df_key = df_raw[(df_raw.keyword == text.lower()) & (
            df_raw.country == geo) & (df_raw.gt_category == int(cat))]

        if index:
            df_key = add_indexing(df_key, 'vl_value', indexdate)
            var_new = 'vl_value_index'
        else:
            var_new = 'vl_value'
            # running the functions we created to create moving average, smoother
        df_key = add_ma(df_key, var_new, 14)
        df_key = add_smoother(df_key, var_new, 0.02)

        df = df_key[df_key.date >= f'{params["startdate"]}']

        fig = go.Figure()

        fig.add_trace(
            go.Scatter(
                x=df.date,
                y=df[var_new],
                name=f'{text} - Original',
                mode='lines',
                opacity=0.3,
                line=dict(color='#024D83',
                          width=4),
                showlegend=True
            ))
        # creating the trendline values
        df_trend = df[['date', var_new]]  # i.e we need date and vl_value
        # dropping 0 because trendlines can't cope without numeric values
        df_trend0 = df_trend.dropna()
        x_sub = df_trend0.date
        y_sub = df_trend0[var_new]
        # transforming dates to numeric values, necessary for polynomial fitting
        x_sub_num = mdates.date2num(x_sub)
        z_sub = np.polyfit(x_sub_num, y_sub, 1)  # polynomial fitting
        p_sub = np.poly1d(z_sub)
        # adding the trendline trace
        fig.add_trace(
            go.Scatter(
                x=x_sub,
                y=p_sub(x_sub_num),
                name='trend',
                mode='lines',
                opacity=1,
                line=dict(color='green',
                          width=4,
                          dash='dash')
            ))
        # adding the 2 week's moving avg trace
        fig.add_trace(
            go.Scatter(
                x=df.date,
                y=df[var_new+'_ma'+str(14)],
                name=var_new+'_ma'+str(14),
                mode='lines',
                opacity=1,
                line=dict(color='red',
                          width=4),
                showlegend=True
            ))
        # adding the smoothed trace
        fig.add_trace(
            go.Scatter(
                x=df.date,
                y=df[var_new+'_smooth'],
                name='smoothed',
                mode='lines',
                opacity=1,
                line=dict(color='purple',
                          width=6),
                showlegend=True
            ))
        fig.update_layout(
            xaxis={'title': None,
                   'titlefont': {'color': '#BFBFBF',
                                 'family': font_use},
                   'tickfont': {'color': '#002A34',
                                'size': 30,
                                'family': font_use},
                   'gridcolor': '#4A4A4A',
                   'linecolor': '#000000',
                   'showgrid': False},
            yaxis={'title': 'Digital Demand',
                   'titlefont': {'color': '#002A34',
                                 'size': 50,
                                 'family': font_use},
                   'tickfont': {'color': '#002A34',
                                'size': 30,
                                'family': font_use},
                   'showgrid': False,
                   'zeroline': False},
            margin={'l': 170,
                    'b': 150,
                    't': 150,
                    'r': 40},
            title={'text': f'Digital Demand for {", ".join(texts).capitalize()}',
                   'font': {'color': '#000000',
                            'size': 40,
                            'family': font_use},
                   'yanchor': "top",
                   'xanchor': "center"},
            legend={'font': {'size': 20,
                             'color': '#333',
                             'family': font_use},
                    'yanchor': "top",
                    'xanchor': "center",
                    'y': 0.9,
                    'x': .95,
                    'orientation': 'v'},
            template='none',
            hovermode='x unified',
            # width=1920,
            # height=1080,

        )

        # write image
        if out_type == 'svg':
            fig.write_image(f"{text}.{output_type}")
        elif out_type == 'html':
            fig.write_html(f"{text}.{output_type}")
        else:
            fig.write_image(f"{text}.{output_type}")
        print(fig)
        return fig.to_html()


@app.route('/')
def index():
    # Call your single function here or modify the code accordingly
    fig_html = single(
        texts=texts,
        key=f'{text.lower()}',
        geo='DE',
        cat=13,
        startdate=f'{init_date}',
        index=True,
        indexdate=f'{index_date}',
        font_use='Roboto Mono Light for Powerline',
        out_type=f'{output_type}'
    )

    # Render the HTML template to display the chart
    return render_template('index.html', fig_html=fig_html)


@app.route('/generate_graph')
def generate_graph():
    # Get the user-entered text from the query parameter
    text = request.args.get('text')

    # Call the single function with the entered text and other parameters
    fig_html = single(
        key=text.lower(),
        geo='DE',
        cat=13,
        startdate=init_date,
        index=True,
        indexdate=index_date,
        font_use='Roboto Mono Light for Powerline',
        out_type=output_type
    )

    # Render the HTML template to display the new graph
    return render_template('graph.html', fig_html=fig_html)


if __name__ == '__main__':
    app.run(debug=True, port=8080)
