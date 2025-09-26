
import os
from pathlib import PureWindowsPath
if __name__ == "__main__":



    file_location = PureWindowsPath(r'\\corefs2.med.umich.edu\shared4\path-aperio\prod\images\Hodgin\KPMP\KPMP slides 20250730 47377\S-2412-001898_082919.svs')

    filename_with_extension = file_location.name
    print(filename_with_extension)
    parent_folder = file_location.parent.name
    print(parent_folder)
    # source_file_name = os.path.basename(file_location)
    # source_folder_name = os.path.dirname(file_location)
    # print(source_folder_name)
    # print("hi")
    # print(source_file_name)
    # print("bye")
