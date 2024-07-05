# src/data_sources/bls_data_source.py
from data_sources.base_data_source import BaseDataSource
from datetime import datetime, timedelta
import pandas as pd
import logging
import json
import requests
import time

class BlsDataSource(BaseDataSource):
    def __init__(self):
        pass

    def fetch_data(self, symbol: str, column_name_remote: str, column_name: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        print(symbol)
        try:
            data = self.fetch_bls_data(symbol, start_date.year, end_date.year)
            data = data[['value']].rename(columns={column_name_remote: column_name})
            data.index.rename('DATE', inplace=True)
            logging.info(f"{column_name} data has been fetched from BLS.")
            return data
        except Exception as e:
            logging.error(f"Error fetching {column_name} data from BLS: {e}")
            return pd.DataFrame()
    
    def fetch_bls_data(self, series_id, start_year, end_year):
        headers = {'Content-type': 'application/json'}
        data_list = []
        for year in range(start_year, end_year, 10):
            batch_end_year = min(year + 9, end_year)
            data = json.dumps({
                "seriesid": [series_id],
                "startyear": str(year),
                "endyear": str(batch_end_year)
            })
            p = requests.post('https://api.bls.gov/publicAPI/v1/timeseries/data/', data=data, headers=headers)
            json_data = json.loads(p.text)
            print(json_data)
            for series in json_data['Results']['series']:
                seriesId = series['seriesID']
                for item in series['data']:
                    year = item['year']
                    period = item['period']
                    value = item['value']
                    footnotes = ""
                    for footnote in item['footnotes']:
                        if footnote:
                            footnotes = footnotes + footnote['text'] + ','
                    if 'M01' <= period <= 'M12':
                        month = int(period[1:])
                        date = datetime(int(year), month, 1) + timedelta(days=31)
                        date = date.replace(day=1)
                        data_list.append({
                            "series_id": seriesId,
                            "year": year,
                            "period": period,
                            "value": value,
                            "footnotes": footnotes[0:-1],
                            "date": date
                        })

        df = pd.DataFrame(data_list)
        df.set_index('date', inplace=True)
        df.drop(columns=["series_id", "year", "period", "footnotes"], inplace=True)
        df.sort_index(inplace=True)
        return df
