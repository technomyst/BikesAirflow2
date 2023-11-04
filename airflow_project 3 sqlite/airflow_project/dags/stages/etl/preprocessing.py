import pandas as pd
from sklearn.preprocessing import LabelEncoder


class Preprocessing:

    def __init__(self, categorical_feautures):
        self.categorical_feautures = categorical_feautures


    def process_data(self, data):
        data = data.fillna(-1)
        for i in self.categorical_feautures:
            lb = LabelEncoder()
            encode_data = lb.fit_transform(data[i].astype(str))
            data[f'{i}_lb'] = encode_data
        named_lb = [f'{x}_lb' for x in self.categorical_feautures]
        data['target'] = data['DIY'].map(lambda x: 1 if x == 'Yes' else 0)
        needed_columns = [
            'Name',
            'target',
            'Kit Cost',
                         'Sell',
                         'Internal ID',
                         'Miles Price',
                         'Total Catches to Unlock',
                         'Stack Size',
                         '#1',
                         '#2',
                         '#3',
                         '#4',
                         '#5',
                         '#6',
                         'Recipes to Unlock']
        data = data[needed_columns + named_lb]
        return data

