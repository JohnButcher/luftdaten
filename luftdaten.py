"""Script to collect data from your luftdaten online sensor archive and plot"""

import os, sys, pandas as pd, argparse, json, boto3, mimetypes
import plotly.offline as pyoff, plotly.graph_objs as go
from datetime import datetime, timedelta
from botocore.exceptions import ClientError


def get_data(now, config, days):

    begin_date = now - timedelta(days=days)
    end_date = now - timedelta(days=1)
    print (f"Date range to fetch will be {begin_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    all_df = pd.DataFrame()
    day = begin_date
    while day <= end_date:

        yyyy_mm_dd = day.strftime("%Y-%m-%d")
        url = f"{config['archive_url']}/{yyyy_mm_dd}/{yyyy_mm_dd}_{config['sensor_csv_suffix']}"
        print(f"Fetching archive from {url}")
        day = day + timedelta(days=1)
        try:
            df = pd.read_csv(url, delimiter=';')
        except Exception as e:
            print(e)
            continue

        if day != begin_date:
            all_df = all_df.append(df)
        else:
            all_df = df

        df = all_df

        df = df[config['relevant_columns']]
        df = df.rename(columns=config['column_renames'])
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.set_index('timestamp')

    return df


def push_to_s3(html_file, config):

    s3bucket_name = config.get('s3bucket')
    if s3bucket_name and os.path.exists(html_file):
        s3dir = config.get('s3dir','')
        s3 = boto3.resource('s3')
        s3bucket = s3.Bucket(s3bucket_name)
        s3_path = s3dir + '/' + os.path.basename(html_file)
        print("Uploading %s to s3://%s" % (html_file, s3bucket_name + '/' + s3_path))
        try:
            content_type = mimetypes.guess_type(html_file)[0]
            s3bucket.upload_file(Filename=html_file,Key=s3_path,
                ExtraArgs={'ACL': 'public-read', 'ContentType': content_type}
            )
        except ClientError as e:
            print(e)


def plot_period_line(df, now, config, days):

    begin_date = now - timedelta(days=days)
    end_date = now - timedelta(days=1)
    df = df[(df.index > begin_date) & (df.index <= end_date)]
    df = df.sort_index(axis=0)

    html_file = f"luftdaten_{days}.html"
    traces = []
    for stat in df.columns:

        t = go.Scatter(x=df.index, y=df[stat], name=stat, mode='lines')
        traces.append(t)

    # add some threshold line text

    for threshold in config.get('thresholds', []):
        col, val, descrip = threshold['column'], threshold['value'], threshold['description']
        text = f"{col} {descrip}"
        threshold_text_start = df.index[int(len(df)/4)]
        traces.append(go.Scatter(
            y=[val+3], x=[threshold_text_start],
            text=[text],
            mode="text",
            showlegend=False
        ))

    # layout and title

    title = "{0} over last {1} days, source <a href='{2}'>{2}</a>".\
            format(config['sensor_csv_suffix'], days, config['archive_url'])

    layout = go.Layout(title=title,
                       xaxis=dict(title='timestamp',
                                  titlefont=dict(family='Courier New, monospace', size=14, color='#7f7f7f')),
                       yaxis=dict(title='Count',
                                  titlefont=dict(family='Courier New, monospace', size=14, color='#7f7f7f'))
                       )
    # plot

    fig = go.Figure(data=traces, layout=layout)

    # add threshold lines

    for threshold in config.get('thresholds', []):
        col, val, descrip = threshold['column'], threshold['value'], threshold['description']
        fig.add_shape(
            type="line",
            x0=df.index.min(),
            y0=val,
            x1=df.index.max(),
            y1=val,
            line=dict(
                color="LightSeaGreen",
                width=2,
                dash="dashdot",
            ),
        )

    # write to file
    pyoff.plot(fig, filename=html_file, auto_open=False, include_plotlyjs='cdn')

    return fig, html_file

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--show", default=False, action="store_true", help="Show figures in browser")
    args = parser.parse_args()
    args.show = True

    with open(os.path.join(os.path.dirname(__file__),'config.json')) as c:
        config = json.loads(c.read())
    now = datetime.now()

    test_data_file = "/tmp/luftdaten_df.csv"
    if os.path.exists(test_data_file):
        print(f"getting data from test data file {test_data_file}")
        df = pd.read_csv(test_data_file)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.set_index('timestamp')
    else:
        df = get_data(now, config, 30)
        df.to_csv(test_data_file)

    for period_days in (7, 30):

        fig, html_file = plot_period_line(df, now, config, period_days)
        push_to_s3(html_file, config)
        # open browser
        if args.show:
            fig.show()


if __name__ == "__main__":
    main()