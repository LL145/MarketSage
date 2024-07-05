from config_reader import ConfigReader
import pandas as pd
import joblib
import json
import boto3
import os


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

    json_data = json.dumps(map, ensure_ascii=False, indent=4)

    bucket_name = 'marketsage'
    file_name = 'prediction.json'
    try:
        self.s3.put_object(Bucket=bucket_name, Key=file_name, Body=json_data.encode('utf-8'), ContentType='application/json; charset=utf-8')
        print(f"File uploaded to {bucket_name}/{file_name}")
    except Exception as e:
        print(f"Error uploading file: {e}")
     