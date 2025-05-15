class DataProcessor:
    def transform_data(self, df):
        return df.filter(df['age'] > 30)
