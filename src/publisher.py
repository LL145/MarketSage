from config_reader import ConfigReader
import pandas as pd
import joblib
import json
import boto3
import os
import yfinance as yf
import util
from datetime import datetime, time, timedelta


class Publisher:
  def __init__(self, config: ConfigReader):
    self.config = config
    self.s3 = boto3.client('s3', 
                           aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID'),
                           aws_secret_access_key= os.getenv('AWS_SECRET_ACCESS_KEY'),
                           region_name='us-west-2')
    
  def predict(self):
    
    data = self.config.read_data_processed()
    last_data = data.drop(columns=['SP500_TR']).iloc[-1]
    model = joblib.load(self.config.get_model_absolute_path())
    prediction = model.predict_proba([last_data])[0][1] * 100
    data_filled = self.config.read_data_filled().iloc[-1]
    data_procesed = self.config.read_data_processed().iloc[-1]

    map = {}
    map['最近更新'] = data.index[-1].strftime('%Y-%m-%d')
    map['AI乐观指数'] = round(prediction)
    map['联邦基金利率'] = last_data['FEDFUNDS']
    map['失业率'] = last_data['US_UNEMPLOYMENT']
    map['标普500市盈率'] = last_data['SP500_PE']
    map['消费者信心指数'] = last_data['CONSUMER_SENTIMENT']
    map['VIX恐慌指数'] = last_data['VIX']
    map['美元指数'] = last_data['DXY']
    map['黄金价格'] = data_filled['GOLD']
    map['原油价格'] = data_filled['OIL']
    map['非农就业人数'] = data_filled['PAYEMS']

    data_filled = self.config.read_data_filled()
    map['CPI'] = (data_filled['US_CPI'].iloc[-1]/data_filled['US_CPI'].iloc[-261] - 1) * 100
    map['PPI'] = (data_filled['US_PPI'].iloc[-1]/data_filled['US_PPI'].iloc[-261] - 1) * 100

    end = util.get_ny_date_without_timezone()
    start = end - timedelta(days=5)
    
    df = yf.download("^FTSE", start = start, end = end)
    map['英国富时100指数'] = df['Close'].iloc[-1]
    map['英国富时100指数涨幅'] = (df['Adj Close'].iloc[-1] / df['Adj Close'].iloc[-2] - 1) * 100

    df = yf.download("^N225", start = start, end = end)
    map['日本日经225指数'] = df['Close'].iloc[-1]
    map['日本日经225指数涨幅'] = (df['Adj Close'].iloc[-1] / df['Adj Close'].iloc[-2] - 1) * 100

    df = yf.download("^GSPC", start = start, end = end)
    map['美国标普500指数'] = df['Close'].iloc[-1]
    map['美国标普500指数涨幅'] = (df['Adj Close'].iloc[-1] / df['Adj Close'].iloc[-2] - 1) * 100

    df = yf.download("^NDX", start = start, end = end)
    map['美国纳斯达克100指数'] = df['Close'].iloc[-1]
    map['美国纳斯达克100指数涨幅'] = (df['Adj Close'].iloc[-1] / df['Adj Close'].iloc[-2] - 1) * 100

    df = yf.download("^DJI", start = start, end = end)
    map['美国道琼斯指数'] = df['Close'].iloc[-1]
    map['美国道琼斯指数涨幅'] = (df['Adj Close'].iloc[-1] / df['Adj Close'].iloc[-2] - 1) * 100

    df = yf.download("^HSI", start = start, end = end)
    map['香港恒生指数'] = df['Close'].iloc[-1]
    map['香港恒生指数涨幅'] = (df['Adj Close'].iloc[-1] / df['Adj Close'].iloc[-2] - 1) * 100

    df = yf.download("000001.SS", start = start, end = end)
    map['中国上证指数'] = df['Close'].iloc[-1]
    map['中国上证指数涨幅'] = (df['Adj Close'].iloc[-1] / df['Adj Close'].iloc[-2] - 1) * 100

    df = yf.download("^GDAXI", start = start, end = end)
    map['德国DAX指数'] = df['Close'].iloc[-1]
    map['德国DAX指数涨幅'] = (df['Adj Close'].iloc[-1] / df['Adj Close'].iloc[-2] - 1) * 100
    
    for key, value in map.items():
      if isinstance(value, (int, float)):
        map[key] = round(value, 2)

    json_data = json.dumps(map, ensure_ascii=False, indent=4)

    bucket_name = 'marketsage'
    file_name = 'prediction.json'
    try:
        self.s3.put_object(Bucket=bucket_name, Key=file_name, Body=json_data.encode('utf-8'), ContentType='application/json; charset=utf-8')
        print(f"File uploaded to {bucket_name}/{file_name}")
    except Exception as e:
        print(f"Error uploading file: {e}")
     