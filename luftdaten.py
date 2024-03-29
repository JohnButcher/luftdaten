"""Script to collect data from your luftdaten online sensor archive and plot"""

import os, sys, pandas as pd, argparse, json, boto3, mimetypes, requests, io, urllib3
import plotly.offline as pyoff, plotly.graph_objs as go
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

def fetch(url):

    print(f"Fetching from {url}")
    try:
        r = requests.get(url, verify=False)
    except Exception as e:
        print(e)
        return None
    r.encoding = 'utf-8'
    if '<' in r.text:
        print("Something wrong with fetched file")
        return None
    return r

def get_data(now, end_date, config, days, ewm_alpha):

    begin_date = now - timedelta(days=days)

    first_tx = datetime.strptime(config.get('first_tx_yyyy_mm_dd','1970_01_01'),'%Y_%m_%d')
    if first_tx > begin_date:
        print(f"Adjusting begin date to match first transmission date {first_tx.strftime('%Y_%m_%d')}")
        begin_date = first_tx

    print (f"Date range to fetch will be {begin_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    all_df = pd.DataFrame()
    day = begin_date
    while day <= end_date:

        yyyy_mm_dd = day.strftime("%Y-%m-%d")
        csv_filepath = f"{yyyy_mm_dd}/{yyyy_mm_dd}_{config['sensor_csv_suffix']}"
        local_path = os.path.join(config['data_dir'], csv_filepath)
        day = day + timedelta(days=1)

        if os.path.exists(local_path) and day < end_date:
            print(f"Reading data from local file {local_path}")
            try:
                df = pd.read_csv(local_path, delimiter=';')
            except Exception as e:
                print(e)
                os.remove(local_path)
        else:
            archive_url = f"{config['archive_url']}/{csv_filepath}"
            local_url = f"{config['local_url']}/{csv_filepath}"

            try:
                #df = pd.read_csv(url, delimiter=';') # fails with certificate hostname mismatchon archive.luftdaten.info
                r = fetch(local_url)
                if not r:
                    r = fetch(archive_url)
                if not r:
                    continue
                csvio = io.StringIO(r.text, newline="")
                df = pd.read_csv(csvio, delimiter=';')
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                df.to_csv(local_path, index=False, sep=';')
            except Exception as e:
                print(e)
                os.remove(local_path)
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

        # ref: https://kanoki.org/2020/04/23/how-to-remove-outliers-in-python/
        if ewm_alpha > 0:
            for c in df.columns:
                df[c] = df[c].ewm(alpha=ewm_alpha).mean()

        # backfill gaps in data, assuming a 3 minute period of data recording

        df = df.loc[~df.index.duplicated(keep='first')]
        df = df.resample('3min').fillna("backfill")

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


def plot_period_line(df, now, config, html_file, title, show, mode='line'):

    traces = []
    for stat in df.columns:

        t = go.Scatter(x=df.index, y=df[stat], name=stat, mode='lines') if mode == 'line' else go.Bar(x=df.index, y=df[stat], name=stat)
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

    title = "{0} [{1}], source <a href='{2}'>{2}</a>".\
            format(title, config['sensor_csv_suffix'].split('.')[0], config['archive_url'])

    layout = go.Layout(title=title,
                       xaxis=dict(title=df.index.name,
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
            x0=df.index[0],
            y0=val,
            x1=df.index[-1],
            y1=val,
            line=dict(
                color="LightSeaGreen",
                width=2,
                dash="dashdot",
            ),
        )

    # write to file
    pyoff.plot(fig, filename=html_file, auto_open=False, include_plotlyjs='cdn')

    # open browser
    if show:
        fig.show()

    return fig


def ampm(hour):

    if hour == 0:
        ret = "midnight"
    elif hour == 12:
        ret ="noon"
    elif hour <= 11:
        ret = str(int(hour))+" a.m."
    else:
        ret = str(abs(int(12-hour)))+" p.m."

    return ret

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--show", default=False, action="store_true", help="Show figures in browser")
    parser.add_argument("--ewm", default=0.1, action="store", help="Exponential smoothing alpha value")
    args = parser.parse_args()
    #args.show = True

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    with open(os.path.join(os.path.dirname(__file__),'config.json')) as c:
        config = json.loads(c.read())

    os.makedirs(config['data_dir'],exist_ok=True)

    now = datetime.now()
    if not config.get('local_url'):
        end_date = now - timedelta(days=1)
    else:
        end_date = now
    dayOfWeek = {0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 3: 'Thursday', 4: 'Friday', 5: 'Saturday', 6: 'Sunday'}

    df = get_data(now, end_date, config, 366, args.ewm)
    #df.to_csv('/tmp/luft.csv')

    index_file = os.path.join(config.get('output_dir',''),'index.html')
    idx = open(index_file,'w')
    idx.write("""<html><head><title>Luftdaten plots</title></head><body>""")
    for period_days in (7, 30, 366):

        pdf = df
        begin_date = now - timedelta(days=period_days)
        pdf = pdf[(df.index > begin_date) & (df.index <= end_date)]
        pdf = pdf.sort_index(axis=0)

        title = f"Luftdaten time series for last {period_days} days"
        html_file = os.path.join(config.get('output_dir',''),title.lower().replace(" ","_") + ".html")
        base_html = os.path.basename(html_file)
        fig = plot_period_line(pdf, now, config, html_file, title, args.show, 'line')
        push_to_s3(html_file, config)
        idx.write(f"\n<h3><a href={base_html}>{title}</a></h3>\n")

        pdf_daily = pdf.groupby(pdf.index.floor('D')).mean()
        pdf_daily.index = pdf_daily.index.rename('day')
        title = f"Luftdaten daily averages for last {period_days} days"
        html_file = os.path.join(config.get('output_dir',''),title.lower().replace(" ","_") + ".html")
        base_html = os.path.basename(html_file)
        fig = plot_period_line(pdf_daily, now, config, html_file, title, args.show, 'bar')
        push_to_s3(html_file, config)
        idx.write(f"\n<h3><a href={base_html}>{title}</a></h3>\n")

        pdf_hourly = pdf.groupby(pdf.index.hour).mean()
        pdf_hourly = pdf_hourly.reset_index()

        pdf_hourly['hour_of_day'] = pdf_hourly.apply(lambda x: ampm(x.timestamp),axis=1)
        pdf_hourly = pdf_hourly.set_index('hour_of_day', drop=True)
        pdf_hourly = pdf_hourly.drop('timestamp', axis=1)

        title = f"Luftdaten hourly averages for last {period_days} days"
        html_file = os.path.join(config.get('output_dir',''),title.lower().replace(" ","_") + ".html")
        base_html = os.path.basename(html_file)
        fig = plot_period_line(pdf_hourly, now, config, html_file, title, args.show, 'bar')
        push_to_s3(html_file, config)
        idx.write(f"\n<h3><a href={base_html}>{title}</a></h3>\n")

        dow_df = pdf.groupby(pdf.index.dayofweek.map(dayOfWeek)).mean()
        dow_df['day_of_week'] = dow_df.index
        dow_df = dow_df.set_index('day_of_week', drop=True)
        dow_df = dow_df.set_index(pd.CategoricalIndex(dow_df.index,ordered=True,
                                  categories=['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'])).sort_index()
        title = f"Luftdaten day of week averages for last {period_days} days"
        html_file = os.path.join(config.get('output_dir',''),title.lower().replace(" ","_") + ".html")
        base_html = os.path.basename(html_file)
        fig = plot_period_line(dow_df, now, config, html_file, title, args.show, 'bar')
        push_to_s3(html_file, config)
        idx.write(f"\n<h3><a href={base_html}>{title}</a></h3>\n")


    idx.write("\n</body></html>")
    idx.close()
    push_to_s3(index_file, config)


if __name__ == "__main__":
    main()
