from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
import os
from dotenv import load_dotenv

class GoogleDrive:
    def __init__(self, folder_id):
        load_dotenv()
        self.folder_id = folder_id
        self.api_key = os.getenv("API_KEY")
        self.cwd = os.getcwd()
        self.root_dir = os.chdir(..)
    
        if not self.api_key:
            raise ValueError("API_KEY environment variable not set.")

        self.service = build('drive', 'v3', developerKey=self.api_key)

    def get_distinct_file_types(self):
        """finds all unique file mime types by searching recursively"""
        all_file_types = set()
        self._find_all_types(self.folder_id, all_file_types)
        all_file_types.discard("application/vnd.google-apps.folder")
        return sorted(list(all_file_types))

    def _find_all_types(self, folder_id, all_file_types):
        """helper method to recursively find all file types"""
        try:
            page_token = None
            while True:
                response = self.service.files().list(
                    q=f"'{folder_id}' in parents and trashed=false",
                    fields='nextPageToken, files(id, name, mimeType)',
                    pageSize=1000,
                    pageToken=page_token
                ).execute()

                for file in response.get("files", []):
                    mime_type = file.get("mimeType")
                    if mime_type:
                        all_file_types.add(mime_type)
                    if mime_type == "application/vnd.google-apps.folder":
                        self._find_all_types(file.get("id"), all_file_types)
                
                page_token = response.get('nextPageToken')
                if not page_token:
                    break
        except HttpError as error:
            print(f"Warning: Could not access folder {folder_id}. Error: {error}")

    def get_files_by_type(self, file_type: str):
        """gets a list of all files of a specific type"""
        files_found = []
        self._find_files_by_type(self.folder_id, file_type, files_found)
        return files_found

    def _find_files_by_type(self, folder_id, file_type, file_list):
        """helper to recursively find files"""
        try:
            page_token = None
            while True:
                response = self.service.files().list(
                    q=f"'{folder_id}' in parents and trashed=false",
                    fields='nextPageToken, files(id, name, mimeType)',
                    pageSize=1000,
                    pageToken=page_token
                ).execute()

                for file in response.get("files", []):
                    file_id = file.get("id")
                    file_name = file.get("name")
                    mime_type = file.get("mimeType")
                    
                    if mime_type == file_type:
                        file_list.append({
                            'id': file_id,
                            'name': file_name,
                            'mimeType': mime_type,
                        })

                    if mime_type == "application/vnd.google-apps.folder":
                        self._find_files_by_type(file_id, file_type, file_list)

                page_token = response.get('nextPageToken')
                if not page_token:
                    break
        except HttpError as error:
            print(f"Warning: Could not access folder {folder_id}. Error: {error}")

    def download_file(self, file_info, base_download_path="epstein-docs"):
        """downloads a single file into a flat directory and returns a status"""
        file_id = file_info['id']
        file_name = file_info['name']
        mime_type = file_info['mimeType']

        output_dir = os.path.join(self.root_dir, base_download_path)
        os.makedirs(output_dir, exist_ok=True)
        file_path = os.path.join(output_dir, file_name)

        try:
            request = self.service.files().get_media(fileId=file_id)
            downloader = MediaIoBaseDownload(open(file_path, 'wb'), request)
            
            done = False
            while not done:
                status, done = downloader.next_chunk()
            
            return f"Success: '{file_name}' saved to '{output_dir}'"
        except HttpError as error:
            return f"Error on '{file_name}': {error}"
