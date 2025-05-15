class DataWriter:
    def write_csv(self, df, output_path):
        df.write.mode("overwrite").option("header", "true").csv(output_path)
