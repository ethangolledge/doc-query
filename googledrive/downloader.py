import sys
import os
import polars as pl
from typing import List

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from gdrive_base import GoogleDrive
from models import ProcessableFileTypes

def format_list_with_and(items: list[str]) -> str:
    if not items:
        return ""
    if len(items) == 1:
        return items[0]
    if len(items) == 2:
        return f"{items[0]} and {items[1]}"
    return ", ".join(items[:-1]) + ", and " + items[-1]

def orchestrate_download():
    FOLDER_ID = '1hTNH5woIRio578onLGElkTWofUSWRoH_'
    
    gdrive_instance = GoogleDrive(folder_id=FOLDER_ID)
    all_files_df = gdrive_instance.scan_all_files()

    if all_files_df.is_empty():
        print("No files found in the Google Drive folder.")
        return

    supported_config = ProcessableFileTypes()
    files_to_download_df = None

    while True:
        print("\nPlease input the file types you would like to download (type 'all', 'none', or comma-separated):")
        raw_input_str = input()
        user_choice = raw_input_str.strip().lower()

        if user_choice == 'none':
            print('No downloads will occur.')
            return
        
        elif user_choice == 'all':
            print('Commencing download of all file types...')
            files_to_download_df = all_files_df
            break

        else:
            requested_mimetypes = {m.strip() for m in raw_input_str.split(',') if m.strip()}
            
            compatible_types = [
                req for req in requested_mimetypes if req in supported_config.processable_mimetypes
            ]
            incompatible_types = [
                req for req in requested_mimetypes if req not in supported_config.processable_mimetypes
            ]

            if incompatible_types:
                incompatible_str = format_list_with_and(incompatible_types)
                print(f"\nWarning: The following types are not supported: {incompatible_str}")
                
                proceed = input("Continue with supported types only? (y/n): ").strip().lower()
                if proceed != 'y':
                    print("Please try again.")
                    continue

            if not compatible_types:
                print("No supported file types were requested. Please try again.")
                continue

            types_str = format_list_with_and(compatible_types)
            print(f"\nProceeding with supported types: {types_str}")
            files_to_download_df = all_files_df.filter(pl.col("mimeType").is_in(compatible_types))
            break

    if files_to_download_df is None or files_to_download_df.is_empty():
        print("No files found matching your criteria.")
        return

    print(f"\nFound {len(files_to_download_df)} files to download. Starting process...")
   
    
    download_results = []

    for i, row in enumerate(files_to_download_df.iter_rows(named=True)):
        if i == 3:
            break
    
        status = gdrive_instance.download_file(row)
        print(status)

        err_flag = ("Error" in status or "Skipped" in status)

        download_results.append({
            "id": row["id"],
            "error": err_flag
        })

    down_results_df = pl.DataFrame(
    data=download_results,
    schema={
        "id": pl.Utf8,
        "error": pl.Boolean
    })

    final_df = files_to_download_df.join(down_results_df, on="id", how="left")

    total_errs = final_df["error"].sum()
    print(f"\nDownload process finished. Total Errors: {total_errs}")

if __name__ == "__main__":
    orchestrate_download()
