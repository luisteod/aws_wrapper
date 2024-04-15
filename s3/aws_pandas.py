import io
import pandas as pd
import os

class AwsPandas:
    """
    Class to handle pandas dataframes in S3.
    """
    def __init__(self, aws_client):
        self.aws_client = aws_client
    
    def read_folder(self, bucket: str, prefix: str) -> pd.DataFrame:
        df = pd.DataFrame()
        files = self.aws_client.list_files(bucket, prefix)
        for file in files:
            data = self.aws_client.download_file(bucket, file)
            df = pd.concat([df, pd.read_parquet(io.BytesIO(data))])
        return df
    
    
    def save_dataframe(self, bucket: str, prefix: str, filename:str, df: pd.DataFrame) -> None:
            pq_bin = df.to_parquet(index=False, engine='pyarrow', compression="snappy")
            path = os.path.join(prefix, f"{filename}.snappy.parquet")
            self.aws_client.upload_file(pq_bin, bucket, path)
            part_num += 1

    def save_dataframe_in_chunks(self, bucket: str, prefix: str, df: pd.DataFrame, chunks: int) -> None:
        part_num = 0
        for i in range(0, len(df), chunks):
            start = i 
            if i + chunks >= len(df):
                end = len(df)
            else:
                end = i + chunks
            pq_bin = df[start:end].to_parquet(index=False, engine='pyarrow', compression="snappy")
            path = os.path.join(prefix, f"part-{part_num:05}.snappy.parquet")
            self.aws_client.upload_file(pq_bin, bucket, path)
            part_num += 1