from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
import os
from dotenv import load_dotenv
import time
import polars as pl
from typing import List

class GoogleDrive:
    """
    A modular class that allows a user to download files from a Google Drive folder.
    - The API key can be passed at runtime or configured in a .env file.
    - The download path can be specified, otherwise it defaults to a directory within the project root.
    """
    def __init__(self, folder_id: str, api_key: str = None, download_dir: str = None):
        load_dotenv()
        self.folder_id = folder_id

        self.api_key = api_key or os.getenv("API_KEY")
        if not self.api_key:
            raise ValueError("API_KEY not passed to the class or set as an environment variable.")

        if download_dir:
            self.download_root = os.path.abspath(download_dir)
        else:
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
            self.download_root = os.path.join(project_root, 'gdrive_downloads')

        try:
            os.makedirs(self.download_root, exist_ok=True)
        except PermissionError:
            raise PermissionError(f"Permission denied to create directory: {self.download_root}")
        except OSError as e:
            raise OSError(f"Could not create download directory at {self.download_root}: {e}")

        print(f"Download location configured: {self.download_root}")

        self.service = build('drive', 'v3', developerKey=self.api_key)

    def scan_all_files(self) -> pl.DataFrame:
        """recursively scans the Google Drive folder and returns a list of all files"""
        all_files: [Dict] = []
        print('Starting a full scan of the Google Drive folder... Please bear with, this may take a while.')
        self._scan_recursive(self.folder_id, all_files)
        print(f"Scan complete. Found {len(all_files)} total files.")

        df = pl.DataFrame(
            data = all_files,
            schema = {
                'id': pl.Utf8,
                'name': pl.Utf8,
                'mimeType': pl.Utf8,
                'size': pl.Int64,
                'createdTime': pl.Utf8
            }
        )

        if not df.is_empty():
            grouped_df = df.group_by('mimeType').agg(
                pl.col('id').count().alias('count'),
                (pl.col('size').sum() / 1_000_000).round(0).cast(pl.Int64).alias('total_size_MB')
            )
            print(grouped_df)

        return df

    def _scan_recursive(self, folder_id: str, all_files: list) -> None:
        """a helper method to recursively scan for all files, collecting metadata"""
        try:
            page_token = None
            while True:
                response = self.service.files().list(
                    q=f"'{folder_id}' in parents and trashed=false",
                    fields='nextPageToken, files(id, name, mimeType, size, createdTime)',
                    pageSize=1000,
                    pageToken=page_token
                ).execute()

                for file in response.get('files', []):
                    mime_type = file.get('mimeType')
                    if mime_type == 'application/vnd.google-apps.folder':
                        self._scan_recursive(file.get("id"), all_files)
                    elif mime_type:
                        all_files.append({
                            'id': file.get('id'),
                            'name': file.get('name'),
                            'mimeType': mime_type,
                            'size': file.get('size'),
                            'createdTime': file.get('createdTime')
                        })
                
                page_token = response.get('nextPageToken')
                if not page_token:
                    break
        except HttpError as error:
            print(f'Error accessing folder {folder_id}: {error}')

    def download_file(self, file_info) -> str:
        """downloads a single file into a flat directory and returns a status"""
        file_id = file_info['id']
        file_name = file_info['name']
        mime_type = file_info['mimeType']

        if 'google-apps' in mime_type:
            return f"Skipped: '{file_name}' (is a native Google Workspace file)"

        type_specific_dir = mime_type.replace('/', '_')
        type_specific_dir_path = os.path.join(self.download_root, type_specific_dir)
        os.makedirs(type_specific_dir_path, exist_ok=True)
        file_path = os.path.join(type_specific_dir_path, file_name)

        try:
            request = self.service.files().get_media(fileId=file_id)
            with open(file_path, 'wb') as f:
                downloader = MediaIoBaseDownload(f, request)
                done = False
                while not done:
                    status, done = downloader.next_chunk()
            
            return f"Success: '{file_name}' saved to '{file_path}'"
        except HttpError as error:
            return f"Error on '{file_name}': {error}"
        except (OSError, IOError) as e:
            return f"Error writing file '{file_name}': {e}"
        finally:
            time.sleep(1)
