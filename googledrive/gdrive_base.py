from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2.service_account import Credentials
import os
from dotenv import load_dotenv
import time
import polars as pl
from typing import List
import json 
from pathlib import Path

class GoogleDrive:
    """
    A modular class that allows a user to download files from a Google Drive folder.
    - The API key can be passed at runtime or configured in a .env file.
    - The download path can be specified, otherwise it defaults to a directory within the project root.
    """
    def __init__(self, folder_id: str, download_dir: str = None):
        load_dotenv()
        self.folder_id = folder_id

        self.service_account_json = os.getenv("SERVICE_ACCOUNT_JSON")
        if not self.service_account_json:
            raise ValueError("SERVICE_ACCOUNT_JSON not set as an environment variable.")

        if download_dir:
            self.download_root = os.path.abspath(download_dir)
        else:
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
            self.download_root = os.path.join(project_root, 'gdrive_downloads')
        
        config_path = Path(__file__).resolve().parent.parent / 'config.json'
        try:
            os.makedirs(self.download_root, exist_ok=True)
            try:
                with config_path.open("r") as f:
                    config = json.load(f)
            except (FileNotFoundError, json.JSONDecodeError):
                config = {}

            config["download_dir"] = self.download_root

            with config_path.open("w") as f:
                json.dump(config, f, indent=4)

        except PermissionError:
            raise PermissionError(f"Permission denied to create directory: {self.download_root}")
        except OSError as e:
            raise OSError(f"Could not create download directory at {self.download_root}: {e}")


        print(f"Download location configured: {self.download_root}")

        creds = Credentials.from_service_account_file(
            self.service_account_json,
            scopes=["https://www.googleapis.com/auth/drive.readonly"]
        )
        self.service = build('drive', 'v3', credentials=creds)

        # for pulling the gdrive file path
        self.folder_map = {}
        self.parent_map = {}

    def scan_all_files(self) -> pl.DataFrame:
        """recursively scans the Google Drive folder and returns a list of all files"""
        all_files: List[Dict] = [] # needs to be statically typed
        print('Starting a full scan of the Google Drive folder... Please bear with, this may take a while.')
        self._scan_recursive(self.folder_id, all_files)
        print(f"Scan complete. Found {len(all_files)} total files.")

        for f in all_files:
            f['gdrive_path'] = self._get_full_path(f)
            f['local_path'] = None # handled in download_file

        df = pl.DataFrame(
            data = all_files,
            schema = {
                'id': pl.Utf8,
                'name': pl.Utf8,
                'mimeType': pl.Utf8,
                'size': pl.Int64,
                'createdTime': pl.Utf8,
                'modifiedTime': pl.Utf8,
                'owners': pl.Utf8,
                'parents': pl.List(pl.Utf8),
                'webViewLink': pl.Utf8,
                'gdrive_path': pl.Utf8,
                'local_path': pl.Utf8
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
                    fields='nextPageToken, files(id, name, mimeType, size, createdTime, modifiedTime, parents, owners, webViewLink)',
                    pageSize=1000,
                    pageToken=page_token
                ).execute()

                for file in response.get('files', []):
                    mime_type = file.get('mimeType')
                    if mime_type == 'application/vnd.google-apps.folder':
                        self.folder_map[file['id']] = file['name']
                        self.parent_map[file['id']] = file.get('parents', [None])[0]
                        self._scan_recursive(file.get('id'), all_files)
                    elif mime_type:
                        all_files.append({
                            'id': file.get('id'),
                            'name': file.get('name'),
                            'mimeType': mime_type,
                            'size': file.get('size'),
                            'createdTime': file.get('createdTime'),
                            'modifiedTime': file.get('modifiedTime'),
                            'owners': ', '.join([o['emailAddress'] for o in file.get('owners', [])]),
                            'parents': file.get('parents', []),
                            'webViewLink': file.get('webViewLink'),
                        })
                
                page_token = response.get('nextPageToken')
                if not page_token:
                    break
        except HttpError as error:
            print(f'Error accessing folder {folder_id}: {error}')

    def _get_full_path(self, file: Dict) -> str:
        """construct full path from root to file"""
        path_parts = [file['name']]
        parents = file.get('parents', [])
        while parents:
            parent_id = parents[0]
            if parent_id == self.folder_id:  # stop at root folder
                break
            parent_name = self.folder_map.get(parent_id, 'UNKNOWN')
            path_parts.insert(0, parent_name)
            parents = [self.parent_map.get(parent_id)]
        return '/'.join(path_parts)

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
        local_path = os.path.join(type_specific_dir_path, file_name)

        try:
            request = self.service.files().get_media(fileId=file_id)
            with open(local_path, 'wb') as f:
                downloader = MediaIoBaseDownload(f, request)
                done = False
                while not done:
                    status, done = downloader.next_chunk()
           
            file_info['local_path'] = local_path
            return f"Success: '{file_name}' saved to '{local_path}'"
        except HttpError as error:
            return f"Error on '{file_name}': {error}"
        except (OSError, IOError) as e:
            return f"Error writing file '{file_name}': {e}"
        finally:
            time.sleep(0.1)
