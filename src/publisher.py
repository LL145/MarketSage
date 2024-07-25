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
    
    data_raw = self.config.read_data_raw()
    data = self.config.read_data_processed()
    last_data = data.drop(columns=['SP500_TR']).iloc[-1]
    model = joblib.load(self.config.get_model_absolute_path())
    prediction = model.predict_proba([last_data])[0][1] * 100
    data_filled = self.config.read_data_filled()
    data_procesed = self.config.read_data_processed().iloc[-1]

    map = {}
    map['最近更新'] = data.index[-1].strftime('%Y-%m-%d')
    map['AI乐观指数'] = round(prediction)
    map['联邦基金利率'] = {'数值': toString(last_data['FEDFUNDS']) + '%', '更新频率': '每年8次'}

    x1 = data_raw['US_UNEMPLOYMENT'].dropna().iloc[-1]
    x2 = data_raw['US_UNEMPLOYMENT'].dropna().iloc[-2]
    map['失业率(美国)'] = {'数值': toString(x1) + '%', '更新频率': '每月', '涨跌': zd(x1, x2)}

    x1 = data['SP500_PE'].dropna().iloc[-1]
    x2 = data['SP500_PE'].dropna().iloc[-2]
    map['标普500市盈率'] = {'数值': toString(x1), '更新频率': '每天', '涨跌': zd(x1, x2)}

    x1 = data['CONSUMER_SENTIMENT'].dropna().iloc[-1]
    x2 = data['CONSUMER_SENTIMENT'].dropna().iloc[-2]
    map['消费者信心指数(美国)'] = {'数值': toString(x1), '更新频率': '每月', '涨跌': zd(x1, x2)} 
    
    x1 = data['VIX'].dropna().iloc[-1]
    x2 = data['VIX'].dropna().iloc[-2]
    map['VIX指数(芝加哥期权交易所市场波动率指数)'] = {'数值': toString(x1), '更新频率': '每天', '涨跌': zd(x1, x2)}

    x1 = data['DXY'].dropna().iloc[-1]
    x2 = data['DXY'].dropna().iloc[-2]
    map['美元指数'] = {'数值': toString(x1), '更新频率': '每天', '涨跌': zd(x1, x2)}

    x1 = data_filled['GOLD'].iloc[-1]
    x2 = data_filled['GOLD'].iloc[-2]
    map['黄金价格'] = {'数值': toString(x1), '单位': '美元每盎司', '更新频率': '每天', '涨跌': zd(x1, x2)}

    x1 = data_filled['OIL'].iloc[-1]
    x2 = data_filled['OIL'].iloc[-2]
    map['原油价格'] = {'数值': toString(x1), '单位': '美元每桶', '更新频率': '每天', '涨跌': zd(x1, x2)}

    x1 = data_filled['PAYEMS'].iloc[-1]
    x2 = data_filled['PAYEMS'].iloc[-2]
    map['美国非农就业人数'] = {'数值': toString(x1), '单位': '千人', '更新频率': '每月', '涨跌': zd(x1, x2)}


    cpi_non_nan = data_raw['US_CPI'].dropna()
    x1 = cpi_non_nan.iloc[-1]/cpi_non_nan.iloc[-13] - 1
    x2 = cpi_non_nan.iloc[-2]/cpi_non_nan.iloc[-14] - 1
    map['CPI'] = {'数值': toString((x1) * 100)+ '%', '更新频率': '每月', '涨跌': zd(x1, x2)} 

    
    ppi_non_nan = data_raw['US_PPI'].dropna()
    x1 = ppi_non_nan.iloc[-1]/ppi_non_nan.iloc[-13] - 1
    x2 = ppi_non_nan.iloc[-2]/ppi_non_nan.iloc[-14] - 1
    map['PPI'] = {'数值': toString((x1) * 100)+ '%', '更新频率': '每月', '涨跌': zd(x1, x2)}


    

    end = util.get_ny_date_without_timezone()
    start = end - timedelta(days=5)
    
    df = yf.download("^FTSE", start = start, end = end)
    map['英国富时100指数'] = {'指数点' : toString(df['Close'].iloc[-1]), '涨幅': toString((df['Adj Close'].iloc[-1] / df['Adj Close'].iloc[-2] - 1) * 100) + '%', '更新频率': '每天'}


    df = yf.download("^N225", start = start, end = end)
    map['日本日经225指数'] = {'指数点' : toString(df['Close'].iloc[-1]), '涨幅': toString((df['Adj Close'].iloc[-1] / df['Adj Close'].iloc[-2] - 1) * 100) + '%', '更新频率': '每天'}


    df = yf.download("^GSPC", start = start, end = end)
    map['美国标普500指数'] = {'指数点' : toString(df['Close'].iloc[-1]), '涨幅': toString((df['Adj Close'].iloc[-1] / df['Adj Close'].iloc[-2] - 1) * 100) + '%', '更新频率': '每天'}


    df = yf.download("^NDX", start = start, end = end)
    map['美国纳斯达克100指数'] = {'指数点' : toString(df['Close'].iloc[-1]), '涨幅': toString((df['Adj Close'].iloc[-1] / df['Adj Close'].iloc[-2] - 1) * 100) + '%', '更新频率': '每天'}


    df = yf.download("^DJI", start = start, end = end)
    map['美国道琼斯指数'] = {'指数点' : toString(df['Close'].iloc[-1]), '涨幅': toString((df['Adj Close'].iloc[-1] / df['Adj Close'].iloc[-2] - 1) * 100) + '%', '更新频率': '每天'}


    df = yf.download("^HSI", start = start, end = end)
    map['香港恒生指数'] = {'指数点' : toString(df['Close'].iloc[-1]), '涨幅': toString((df['Adj Close'].iloc[-1] / df['Adj Close'].iloc[-2] - 1) * 100) + '%', '更新频率': '每天'}


    df = yf.download("000001.SS", start = start, end = end)
    map['中国上证指数'] = {'指数点' : toString(df['Close'].iloc[-1]), '涨幅': toString((df['Adj Close'].iloc[-1] / df['Adj Close'].iloc[-2] - 1) * 100) + '%', '更新频率': '每天'}


    df = yf.download("^GDAXI", start = start, end = end)
    map['德国DAX指数'] = {'指数点' : toString(df['Close'].iloc[-1]), '涨幅': toString((df['Adj Close'].iloc[-1] / df['Adj Close'].iloc[-2] - 1) * 100) + '%', '更新频率': '每天'}


    json_data = json.dumps(map, ensure_ascii=False, indent=4)

    bucket_name = 'marketsage'
    file_name = 'prediction.json'
    try:
        self.s3.put_object(Bucket=bucket_name, Key=file_name, Body=json_data.encode('utf-8'), ContentType='application/json; charset=utf-8')
        print(f"File uploaded to {bucket_name}/{file_name}")
    except Exception as e:
        print(f"Error uploading file: {e}")


def toString(f):
  return f'{f:.2f}'

def zd(x1, x2):
  if x1 > x2:
    return '上涨'
  if x2 > x1:
    return '下跌'
  return '无变化'

def generate_html_with_chart(dates, stock_prices, ai_scores, output_file='chart.html'):
  dates_str = [date.strftime('%Y-%m-%d') for date in dates]
  html_content = f"""
  <!DOCTYPE html>
  <html>
  <head>
      <meta charset="UTF-8">
      <title>股票价格图表</title>
      <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
      <style>
          #chartContainer {{
              width: 1600px;
              height: 800px;
              margin: auto;
          }}
          canvas {{
              width: 100% !important;
              height: 100% !important;
          }}
      </style>
  </head>
  <body>
      <div id="chartContainer">
          <canvas id="stockChart"></canvas>
      </div>
      <script>
          const ctx = document.getElementById('stockChart').getContext('2d');
          const data = {{
              labels: {dates_str},
              datasets: [
                  {{
                      label: 'S&P 500 (TR)',
                      data: {stock_prices},
                      borderColor: 'blue',
                      fill: false,
                      pointRadius: 5,
                      pointHoverRadius: 10,
                      pointBackgroundColor: 'blue',
                      yAxisID: 'y-axis-1'
                  }},
                  {{
                      label: 'AI Score',
                      data: {ai_scores},
                      borderColor: 'green',
                      fill: false,
                      pointRadius: 5,
                      pointHoverRadius: 10,
                      pointBackgroundColor: 'green',
                      yAxisID: 'y-axis-2'
                  }}
              ]
          }};

          const options = {{
              responsive: false,
              plugins: {{
                  tooltip: {{
                      mode: 'index',
                      intersect: false,
                      callbacks: {{
                          label: function(context) {{
                              let label = context.dataset.label || '';
                              if (label) {{
                                  label += ': ';
                              }}
                              label += context.parsed.y;

                              return label;
                          }}
                      }}
                  }}
              }},
              hover: {{
                  mode: 'nearest',
                  intersect: true
              }},
              scales: {{
                  x: {{
                      display: true,
                      title: {{
                          display: true,
                          text: 'DATE'
                      }}
                  }},
                  'y-axis-1': {{
                      display: true,
                      position: 'left',
                      title: {{
                          display: true,
                          text: 'S&P 500 (TR)'
                      }}
                  }},
                  'y-axis-2': {{
                      display: true,
                      position: 'right',
                      title: {{
                          display: true,
                          text: 'AI Score'
                      }},
                      grid: {{
                          drawOnChartArea: false
                      }}
                  }}
              }}
          }};

          new Chart(ctx, {{
              type: 'line',
              data: data,
              options: options
          }});
      </script>
  </body>
  </html>
  """

  with open(output_file, 'w', encoding='utf-8') as file:
      file.write(html_content)

  