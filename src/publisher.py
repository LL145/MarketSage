from config_reader import ConfigReader
import pandas as pd
import joblib
import json
import boto3
import os
import yfinance as yf
import util
from datetime import datetime, time, timedelta
import lightgbm as lgb
from lightgbm import LGBMClassifier
import shap
import anthropic



class Publisher:

    def __init__(self, config: ConfigReader):
        self.config = config
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name='us-west-2')
        self.model = joblib.load(self.config.get_model_absolute_path())
        self.jsonstr = None
        self.ai_prediction = 50


    def predict(self):
        data_raw = self.config.read_data_raw()
        data = self.config.read_data_processed()
        last_data = data.drop(columns=['SP500_TR']).iloc[-1]

        prediction = self.model.predict_proba([last_data])[0][1] * 100
        data_filled = self.config.read_data_filled()

        map = {}
        map['最近更新'] = data.index[-1].strftime('%Y-%m-%d')
        map['AI乐观指数'] = round(prediction)
        self.ai_prediction = round(prediction)
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

        #x1 = data['SP500_PE'].dropna().iloc[-1]
        #x2 = data['SP500_PE'].dropna().iloc[-2]
        #map['标普500市盈率'] = {'数值': toString(x1), '更新频率': '每天', '涨跌': zd(x1, x2)}

        x1 = data_raw['CONSUMER_SENTIMENT'].dropna().iloc[-1]
        x2 = data_raw['CONSUMER_SENTIMENT'].dropna().iloc[-2]
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
        map['CPI年增率'] = {
            '数值': toString((x1) * 100) + '%',
            '更新频率': '每月',
            '涨跌': zd(x1, x2)
        }

        ppi_non_nan = data_raw['US_PPI'].dropna()
        x1 = ppi_non_nan.iloc[-1] / ppi_non_nan.iloc[-13] - 1
        x2 = ppi_non_nan.iloc[-2] / ppi_non_nan.iloc[-14] - 1
        map['PPI年增率'] = {
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
        self.jsonstr = json_data

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
        
        report = self.report()
        report_file_name = 'report.txt'
        try:
            self.s3.put_object(Bucket=bucket_name,
                               Key=report_file_name,
                               Body=report.encode('utf-8'),
                               ContentType='text/plain; charset=utf-8')
            print(f"File uploaded to {bucket_name}/{report_file_name}")
        except Exception as e:
            print(f"Error uploading file: {e}")


    def report(self):
        data = self.config.read_data_processed()
        datestr = data.index[-1].strftime('%Y-%m-%d')
        s = f'这是{datestr}的宏观经济数据和市场数据：\n'
        s = s + self.jsonstr
        s = s + '\n'
        s += f'我们的AI模型认为今日的"AI乐观指数"为:{self.ai_prediction }.\nAI乐观指数的定义是：标普500全收益指数在未来6个月后涨幅有多大概率超过其历史涨幅中位数.\n使用SHAP解释模型输出, 得到影响今天预测结果的正面因素与负面因素: '
        s += '\n'

        X_sample = data.drop(columns=['SP500_TR']).iloc[[-1]]
        explainer = shap.Explainer(self.model)
        shap_values = explainer(X_sample)

        top_n = 4  # 设置前 N 项
        contributions = sorted(zip(X_sample.columns,shap_values.data[0],shap_values.values[0]), key=lambda x: x[2], reverse=True)

        def get_data_config_map():
          df = pd.read_csv('data_config.csv')
          config_map = {}
          for _, row in df.iterrows():
            config_map[row['column_name']] = row.to_dict()
          return config_map

        config_map = self.config.get_data_config_map()
        map2 = {'MA1M': '过去1个月均值', 'MA3M': '过去3个月均值', 'MA6M': '过去6个月均值','MA1Y': '过去1年均值','MA3Y': '过去3年均值','MA5Y': '过去5年均值'}
        map3 = {'D': '差值', 'R': '比值'}

        s += "正面影响最大的特征:\n"
        for feature, data, contribution in contributions[:top_n]:
            x = feature.split('-')
            if len(x) == 1:
                feature_meaning = config_map[x[0]]['description']
            elif len(x) == 3:
                feature_meaning = '[' + config_map[x[0]]['description'] + ']' + '当前值与' + map2[x[1]] + '的' + map3[x[2]]
            s += f"{feature_meaning} = {data:.3f}\n"

        s += "负面影响最大的特征:\n"

        for feature, data, contribution in reversed(contributions[-top_n:]):
            x = feature.split('-')
            if len(x) == 1:
              feature_meaning = config_map[x[0]]['description']
            elif len(x) == 3:
              feature_meaning = '[' + config_map[x[0]]['description'] + ']' + '当前值与' + map2[x[1]] + '的' + map3[x[2]]
            s += f"{feature_meaning} = {data:.3f}\n"
        s += "\n"
        s += "结合今天的宏观经济数据以及AI的预测, 用中文写一篇财经报告.\n你可以先回顾近期的经济数据,然后给出AI预测并且解释模型为什么这样预测.\n在解释模型的时候, 挑出3到5项你认为有代表性, 且容易用经济, 金融的知识解释特征, 介绍这些特征, 并给出得出AI乐观指数的理由. \n不要在文章里给出模型内部的信息, 比如shap value, 也不要明确说出我们用到的特征, 比如不说\"标普500全收益指数当前值与过去5年均值的比值为xxx\" 这样的话, 你可以说\"标普500在过去一段时间上涨过多\".\n你可以宣传一下我的AI预测模型, 这个模型是根据200多项市场和经济数据以及过去30年的数据训练出来的. \n你不需要罗列这些数据, 要有所取舍, 让文章更生动易读.\n你可以敢于表达明确的观点. 比如, 当AI乐观指数超过75时可以推荐买入/抄底, 如果AI乐观指数过低, 你可以劝读者谨慎.\n你可以使用一些经济, 金融, 以及人工智能领域的专业词汇,让文章显得更专业."
        print(s)
        client = anthropic.Anthropic()

        message = client.messages.create(
            model="claude-3-5-sonnet-20240620",
            max_tokens=4096,
            temperature= 0.1,
            system="你是一个金融分析师以及人工智能专家",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": s
                        }
                    ]
                }
            ]
        )
        report = message.content[0].text
        return report
        

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
