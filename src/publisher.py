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
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name='us-west-2')
        self.model = joblib.load(self.config.get_model_absolute_path())

    def predict(self):

        data_raw = self.config.read_data_raw()
        data = self.config.read_data_processed()
        last_data = data.drop(columns=['SP500_TR']).iloc[-1]

        prediction = self.model.predict_proba([last_data])[0][1] * 100
        data_filled = self.config.read_data_filled()

        map = {}
        map['最近更新'] = data.index[-1].strftime('%Y-%m-%d')
        map['AI乐观指数'] = round(prediction)
        map['联邦基金利率'] = {
            '数值': toString(last_data['FEDFUNDS']) + '%',
            '更新频率': '每年8次'
        }

        x1 = data_raw['US_UNEMPLOYMENT'].dropna().iloc[-1]
        x2 = data_raw['US_UNEMPLOYMENT'].dropna().iloc[-2]
        map['失业率(美国)'] = {
            '数值': toString(x1) + '%',
            '更新频率': '每月',
            '涨跌': zd(x1, x2)
        }

        x1 = data['SP500_PE'].dropna().iloc[-1]
        x2 = data['SP500_PE'].dropna().iloc[-2]
        map['标普500市盈率'] = {'数值': toString(x1), '更新频率': '每天', '涨跌': zd(x1, x2)}

        x1 = data['CONSUMER_SENTIMENT'].dropna().iloc[-1]
        x2 = data['CONSUMER_SENTIMENT'].dropna().iloc[-2]
        map['消费者信心指数(美国)'] = {
            '数值': toString(x1),
            '更新频率': '每月',
            '涨跌': zd(x1, x2)
        }

        x1 = data['VIX'].dropna().iloc[-1]
        x2 = data['VIX'].dropna().iloc[-2]
        map['VIX指数(芝加哥期权交易所市场波动率指数)'] = {
            '数值': toString(x1),
            '更新频率': '每天',
            '涨跌': zd(x1, x2)
        }

        x1 = data['DXY'].dropna().iloc[-1]
        x2 = data['DXY'].dropna().iloc[-2]
        map['美元指数'] = {'数值': toString(x1), '更新频率': '每天', '涨跌': zd(x1, x2)}

        x1 = data_filled['GOLD'].iloc[-1]
        x2 = data_filled['GOLD'].iloc[-2]
        map['黄金价格'] = {
            '数值': toString(x1),
            '单位': '美元每盎司',
            '更新频率': '每天',
            '涨跌': zd(x1, x2)
        }

        x1 = data_filled['OIL'].iloc[-1]
        x2 = data_filled['OIL'].iloc[-2]
        map['原油价格'] = {
            '数值': toString(x1),
            '单位': '美元每桶',
            '更新频率': '每天',
            '涨跌': zd(x1, x2)
        }

        x1 = data_filled['PAYEMS'].iloc[-1]
        x2 = data_filled['PAYEMS'].iloc[-2]
        map['美国非农就业人数'] = {
            '数值': toString(x1),
            '单位': '千人',
            '更新频率': '每月',
            '涨跌': zd(x1, x2)
        }

        cpi_non_nan = data_raw['US_CPI'].dropna()
        x1 = cpi_non_nan.iloc[-1] / cpi_non_nan.iloc[-13] - 1
        x2 = cpi_non_nan.iloc[-2] / cpi_non_nan.iloc[-14] - 1
        map['CPI'] = {
            '数值': toString((x1) * 100) + '%',
            '更新频率': '每月',
            '涨跌': zd(x1, x2)
        }

        ppi_non_nan = data_raw['US_PPI'].dropna()
        x1 = ppi_non_nan.iloc[-1] / ppi_non_nan.iloc[-13] - 1
        x2 = ppi_non_nan.iloc[-2] / ppi_non_nan.iloc[-14] - 1
        map['PPI'] = {
            '数值': toString((x1) * 100) + '%',
            '更新频率': '每月',
            '涨跌': zd(x1, x2)
        }

        end = util.get_ny_date_without_timezone()
        start = end - timedelta(days=5)

        df = yf.download("^FTSE", start=start, end=end)
        map['英国富时100指数'] = {
            '指数点':
            toString(df['Close'].iloc[-1]),
            '涨幅':
            toString(
                (df['Adj Close'].iloc[-1] / df['Adj Close'].iloc[-2] - 1) *
                100) + '%',
            '更新频率':
            '每天'
        }

        df = yf.download("^N225", start=start, end=end)
        map['日本日经225指数'] = {
            '指数点':
            toString(df['Close'].iloc[-1]),
            '涨幅':
            toString(
                (df['Adj Close'].iloc[-1] / df['Adj Close'].iloc[-2] - 1) *
                100) + '%',
            '更新频率':
            '每天'
        }

        df = yf.download("^GSPC", start=start, end=end)
        map['美国标普500指数'] = {
            '指数点':
            toString(df['Close'].iloc[-1]),
            '涨幅':
            toString(
                (df['Adj Close'].iloc[-1] / df['Adj Close'].iloc[-2] - 1) *
                100) + '%',
            '更新频率':
            '每天'
        }

        df = yf.download("^NDX", start=start, end=end)
        map['美国纳斯达克100指数'] = {
            '指数点':
            toString(df['Close'].iloc[-1]),
            '涨幅':
            toString(
                (df['Adj Close'].iloc[-1] / df['Adj Close'].iloc[-2] - 1) *
                100) + '%',
            '更新频率':
            '每天'
        }

        df = yf.download("^DJI", start=start, end=end)
        map['美国道琼斯指数'] = {
            '指数点':
            toString(df['Close'].iloc[-1]),
            '涨幅':
            toString(
                (df['Adj Close'].iloc[-1] / df['Adj Close'].iloc[-2] - 1) *
                100) + '%',
            '更新频率':
            '每天'
        }

        df = yf.download("^HSI", start=start, end=end)
        map['香港恒生指数'] = {
            '指数点':
            toString(df['Close'].iloc[-1]),
            '涨幅':
            toString(
                (df['Adj Close'].iloc[-1] / df['Adj Close'].iloc[-2] - 1) *
                100) + '%',
            '更新频率':
            '每天'
        }

        df = yf.download("000001.SS", start=start, end=end)
        map['中国上证指数'] = {
            '指数点':
            toString(df['Close'].iloc[-1]),
            '涨幅':
            toString(
                (df['Adj Close'].iloc[-1] / df['Adj Close'].iloc[-2] - 1) *
                100) + '%',
            '更新频率':
            '每天'
        }

        df = yf.download("^GDAXI", start=start, end=end)
        map['德国DAX指数'] = {
            '指数点':
            toString(df['Close'].iloc[-1]),
            '涨幅':
            toString(
                (df['Adj Close'].iloc[-1] / df['Adj Close'].iloc[-2] - 1) *
                100) + '%',
            '更新频率':
            '每天'
        }

        json_data = json.dumps(map, ensure_ascii=False, indent=4)

        bucket_name = 'marketsage'
        file_name = 'prediction.json'
        try:
            self.s3.put_object(Bucket=bucket_name,
                               Key=file_name,
                               Body=json_data.encode('utf-8'),
                               ContentType='application/json; charset=utf-8')
            print(f"File uploaded to {bucket_name}/{file_name}")
        except Exception as e:
            print(f"Error uploading file: {e}")

        date, l1, l2 = self.helper()
        html_content = self.generate_html(date, l1, l2)
        file_name = 'table.html'
        try:
            self.s3.put_object(Bucket=bucket_name,
                               Key=file_name,
                               Body=html_content.encode('utf-8'),
                               ContentType='text/html; charset=utf-8')
            print(f"File uploaded to {bucket_name}/{file_name}")
        except Exception as e:
            print(f"Error uploading file: {e}")

    def helper(self):
        length = 130
        data = self.config.read_data_processed()
        X = data.drop(columns=['SP500_TR'])
        data['Predicted_Probability'] = self.model.predict_proba(X)[:, 1]
        timestamps = data.index.tolist()[-length:]
        date_list = [
            timestamp.strftime('%Y-%m-%d') for timestamp in timestamps
        ]
        sp500_list = data['SP500_TR'].tolist()[-length:]
        sp500_list2 = [round(num, 2) for num in sp500_list]
        ai_list = (data['Predicted_Probability'] * 100).tolist()[-length:]
        ai_list2 = [round(num, 2) for num in ai_list]
        return date_list, sp500_list2, ai_list2

    def generate_html(self, date, l1, l2):
        # 生成包含表格的HTML字符串
        html_content = '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Table</title>
    </head>
    <body>
        <table border="1">
            <tr>
                <th>日期</th>
                <th>标普500全收益指数</th>
                <th>AI乐观指数</th>
            </tr>
    '''
        # 将每一行数据添加到HTML表格中
        for d, l1_val, l2_val in zip(date, l1, l2):
            html_content += f'''
              <tr>
                  <td>{d}</td>
                  <td>{l1_val}</td>
                  <td>{l2_val}</td>
              </tr>
          '''
            # 关闭HTML标签
        html_content += '''
        </table>
      </body>
      </html>
      '''
        return html_content


def toString(f):
    return f'{f:.2f}'


def zd(x1, x2):
    if x1 > x2:
        return '上涨'
    if x2 > x1:
        return '下跌'
    return '无变化'
