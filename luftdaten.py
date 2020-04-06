"""Script to collect data from your luftdaten online sensor archive and plot"""

import os, plotly, cufflinks as cf, pandas as pd, argparse, json
from datetime import datetime, timedelta

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--days", default=5, help="Number of days ago to plot from")
    args = parser.parse_args()

    with open(os.path.join(os.path.dirname(__file__),'config.json')) as c:
        config = json.loads(c.read())
    print (config)

    now = datetime.now()
    begin_date = now - timedelta(days=args.days)
    end_date = now - timedelta(days=1)
    print (f"Date range will be {begin_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

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



    df = all_df[config['relevant_columns']]
    df = df.rename(columns=config['column_renames'])
    df = df.set_index(config['index_column'])
    fig = df.iplot(asFigure=True, xTitle=config['index_column'],
                   yTitle="Count", title="Luftdaten readings", kind="bar")
    fig.show()

if __name__ == "__main__":
    main()