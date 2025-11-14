from gdrive_base import GoogleDrive
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

FOLDER_ID = "1hTNH5woIRio578onLGElkTWofUSWRoH_"  # public Epstein docs GoogleDrive
MAX_WORKERS = 10

def download_files_concurrently(google, files_to_download):
    """downloads a list of files in parallel using a thread pool"""
    total_files = len(files_to_download)
    print(f"\nStarting concurrent download of {total_files} files with {MAX_WORKERS} workers...")
    
    success_count = 0
    skipped_count = 0
    error_count = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # create a furture for each download 
        futures = [executor.submit(google.download_file, file) for file in files_to_download]
        
        # process results as they complete
        for i, future in enumerate(as_completed(futures)):
            result = future.result()
            print(f"({i + 1}/{total_files}) {result}")
            
            if result.startswith("Success"):
                success_count += 1
            elif result.startswith("Skipped"):
                skipped_count += 1
            else:
                error_count += 1
    
    print("\n--- Download Summary ---")
    print(f"Total files processed: {total_files}")
    print(f"Successfully downloaded: {success_count}")
    print(f"Skipped: {skipped_count}")
    print(f"Errors: {error_count}")
    print("------------------------")

def main():
    """main function to find, select, and download files from Google Drive"""
    try:
        google = GoogleDrive(folder_id=FOLDER_ID)
        
        # get and save the list of unique file types
        print("Finding all unique file types...")
        types = google.get_distinct_file_types()
        
        if not types:
            print("No distinct file types found.")
            return

        output_filename = f"distinct_file_types_{google.folder_id}.txt"
        with open(output_filename, 'w') as f:
            f.write('\n'.join(types))
        print(f"List of unique file types saved to '{output_filename}'")

        # ask user to select a file type
        print("\nChoose a file type to download from the list below:")
        for file_type in types:
            print(f"- {file_type}")
        
        chosen_type = input("\nFile type: ").strip()
        if not chosen_type:
            print("No file type entered. Exiting.")
            return

        # get the list of files
        print(f"\nSearching for all files of type: '{chosen_type}'...")
        files_to_download = google.get_files_by_type(chosen_type)
        
        if not files_to_download:
            print("No files found for the selected type.")
            return
            
        download_files_concurrently(google, files_to_download)

    except (ValueError, RuntimeError) as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    main()
