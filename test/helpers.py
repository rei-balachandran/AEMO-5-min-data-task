import os

def delete_files(directory: str):
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        if os.path.isfile(file_path):
            os.remove(file_path)
    print(f"All files from {directory} has been deleted")