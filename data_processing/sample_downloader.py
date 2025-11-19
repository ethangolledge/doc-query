from pathlib import Path
import sys
import polars as pl
from duckdb.handler import LocalDB
from googledrive.downloader import orchestrate_download
from googledrive.gdrive_base import GoogleDrive

class SampleDownload:
    def __init__(
            self,
            folder_id: str,
            download_dir: str = None
    ):
        self.folder_id = folder_id
        self.download_dir = download_dir
    
    def download_files(self, df: pl.DataFrame, gdrive_instance: GoogleDrive) -> pl.DataFrame:
        """download files in a loop, passing contents of the dataframe as a dictionary with each iteration"""
        # shuffle the dataframe to randomise the order
        df = df.sample(n=df.height, shuffle=True, seed=42)

        download_results = []

        for i, row in enumerate(df.iter_rows(named=True)):
            # loop first 100 rows
            if i == 10:
                break
            
            status = gdrive_instance.download_file(row)
            print(status)
            # create logic to determine if there was an error in the download
            err_flag = 'Error' in status

            download_results.append({
                'id': row['id'],
                'error': err_flag
            })
        # create a df for errors
        down_results_df = pl.DataFrame(
        data=download_results,
        schema={
            'id': pl.Utf8,
            'error': pl.Boolean
        })
        # left join the errors onto the original dataframe
        final_df = df.join(down_results_df, on="id", how="left")

        return final_df

    def main(self) -> pl.DataFrame:
        """main fucntion to pull 100 random files from gdrive""" 
        download_df, gdrive_instance = orchestrate_download(
            folder_id=self.folder_id,
            download_dir=self.download_dir)
        download_df = self.download_files(df = download_df, gdrive_instance = gdrive_instance)
        #Local_DB.insert_df_tracking_table(download_df)
        return

if __name__ == '__main__':
    FOLDER_ID = '1hTNH5woIRio578onLGElkTWofUSWRoH_'
    sample = SampleDownload(folder_id=FOLDER_ID)
    sample.main()

