from gdrive_base import GoogleDrive

if __name__ == '__main__':
    try:
        # public epstein docs GoogleDrive
        google = GoogleDrive(folder_id="1hTNH5woIRio578onLGElkTWofUSWRoH_")
        
        print("Step 1: Finding all unique file types...")
        types = google.get_distinct_file_types()
        
        if not types:
            print("\nNo distinct file types found.")
        else:
            # save the distinct types to a file
            output_filename = f"distinct_file_types_{google.folder_id}.txt"
            with open(output_filename, 'w') as f:
                for file_type in types:
                    f.write(f"{file_type}\n")
            print(f"List of unique file types saved to '{output_filename}'")

            print("\nStep 2: Choose a file type to download from the list below:")
            for file_type in types:
                print(f"- {file_type}")
            
            file_type_to_download = input("\nCopy and paste the file type you want to download and press Enter: ")
            
            if not file_type_to_download.strip():
                print("No file type entered. Exiting.")
            else:
                print(f"\nSearching for all files of type: '{file_type_to_download}'...")
                files_to_download = google.get_files_by_type(file_type_to_download)
                
                if not files_to_download:
                    print("No files found for the selected type.")
                else:
                    print(f"Found {len(files_to_download)} files. Starting download...")
                    for file in files_to_download:
                        google.download_file(
                            file_id=file['id'],
                            file_name=file['name'],
                            mime_type=file['mimeType']
                        )
                    print("\nDownload process complete.")

    except (ValueError, RuntimeError) as e:
        print(f"Error: {e}")
