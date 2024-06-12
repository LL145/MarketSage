import pandas as pd
from datetime import datetime
import logging
from config_reader import ConfigReader
import xgboost as xgb
import matplotlib.pyplot as plt
import seaborn as sns


def prepare_data(df):
    df['Future_SP500_TR'] = df['SP500_TR'].shift(-22)
    df['Return'] = (df['Future_SP500_TR'] / df['SP500_TR'] - 1) * 100
    df['Target'] = (df['Return'] > 1).astype(int)
    df.drop(columns=['Future_SP500_TR', 'Return'], inplace=True)
    df.dropna(inplace=True)
    return df

def plot_predictions(df, predictions):
    # 绘制SP500_TR时间序列
    plt.figure(figsize=(30, 10))
    positive = df[predictions == 1]
    negative = df[predictions == 0]

    # 绘制预测为阳性的点，设置点的大小为10
    plt.scatter(positive.index, positive['SP500_TR'], color='green', s=1, label='Predicted Positive (Gain > 1%)')
    # 绘制预测为阴性的点，设置点的大小为10
    plt.scatter(negative.index, negative['SP500_TR'], color='red', s=1, label='Predicted Negative (Gain ≤ 1%)')

    plt.title('S&P 500 Total Return Index with Predictions')
    plt.xlabel('Date')
    plt.ylabel('SP500_TR')
    plt.legend()
    plt.show()

def train_and_predict(df):
    # 准备数据
    X = df.drop(columns=['Target', 'SP500_TR'])
    y = df['Target']

    # 数据集划分
    clf = xgb.XGBClassifier(objective='binary:logistic', n_estimators=100, learning_rate=0.05, max_depth=4)
    clf.fit(X, y)

    # 进行全量数据预测
    predictions = clf.predict(X)
    plot_predictions(df, predictions)

class Trainer:
    def __init__(self, config: ConfigReader):
        self.config = config
    
    def train(self):
        df = self.config.read_data_processed()
        df_processed = prepare_data(df)
        train_and_predict(df_processed)
        
