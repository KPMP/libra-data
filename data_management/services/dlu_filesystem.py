import shutil
import os
from pathlib import Path
import logging
import shutil
from hashlib import md5
import uuid
from zarr_checksum import compute_zarr_checksum
from zarr_checksum.generators import yield_files_local
from mmap import mmap, ACCESS_READ
import subprocess
import tempfile

logger = logging.getLogger("DLUFilesystem")
logger.setLevel(logging.INFO)


def calculate_checksum(file_path: str):

    if os.path.isdir(file_path):
        return "0"
    if os.path.getsize(file_path) == 0:
        # This is apparently the md5 returned for an empty file
        return 'd41d8cd98f00b204e9800998ecf8427e'
    elif ".zarr" not in file_path:
        with open(file_path) as f, mmap(f.fileno(), 0, access=ACCESS_READ) as f:
            return md5(f).hexdigest()
    else:
        return compute_zarr_checksum(yield_files_local(file_path)).md5


class DLUFile:

    def __init__(self, name: str, path: str, checksum: str, size: int, metadata: dict = {}):
        self.name = name
        self.path = path
        self.checksum = checksum
        self.size = size
        self.file_id = str(uuid.uuid4())
        self.metadata = metadata
        self.modified_at = None

    # Returns path without top directory, i.e. package dir or participant dir (bulk uploads)
    def get_short_path(self):
        return "/".join(self.path.split("/")[1:])

    # Returns the filename without path prefix, if it has it.
    def get_short_filename(self):
        return self.name.split("/")[-1:][0]


class DirectoryInfo:
    def __init__(self, directory_path: str, calculate_checksums: bool = True):
        self.dir_contents = os.listdir(directory_path)
        self.subdir_count = 0
        self.file_count = 0
        self.file_details = []
        self.valid_for_dlu = False
        self.directory_path = directory_path
        self.calculate_checksums = calculate_checksums
        self.get_directory_information()
        self.check_if_valid_for_dlu()

    def get_directory_information(self):
        for item in self.dir_contents:
            full_path = os.path.join(self.directory_path, item)
            if os.path.isdir(full_path) and ".zarr" not in full_path:
                self.subdir_count += 1
                checksum = "0"
            else:
                self.file_count += 1
                checksum = "0" if not self.calculate_checksums else calculate_checksum(full_path)
            self.file_details.append(DLUFile(item, full_path, checksum, os.path.getsize(full_path)))

    def check_if_valid_for_dlu(self):
        self.valid_for_dlu = (len(self.dir_contents) != 0)


class DLUFileHandler:

    def __init__(self):
        self.globus_data_directory = '/globus'
        self.dlu_data_directory = '/data'
        self.dlu_package_dir_prefix = 'package_'
        self.globus_dir_prefix = ''
    
    def set_recall_package_directories(self):
        self.globus_data_directory = '/data'
        self.dlu_data_directory = '/globus'
        self.dlu_package_dir_prefix = ''
        self.globus_dir_prefix = 'package_'

    def split_path(self, path: str, preserve_path: bool = False):
        if len(path.split("/")) > 0:
            if preserve_path:
                file_name = "/".join(path.replace(self.globus_data_directory, "").split("/")[1:])
            else:
                file_name = path.split("/")[-1]
            file_path_arr = path.split("/")[:-1]
            file_path = "/".join(file_path_arr)
        else:
            file_name = path
            file_path = ""

        return {"file_name": file_name, "file_path": file_path}
    
    def chown_dir(self, package_id: str, files: list[DLUFile], user_id):
        try:
            package_path = self.dlu_data_directory + "/" + self.dlu_package_dir_prefix + package_id
            if os.stat(package_path).st_uid != user_id or os.stat(package_path).st_gid != int(os.environ['dlu_group']):
                os.chown(package_path, user_id, int(os.environ['dlu_group']))
                for file in files:
                    os.chown(package_path + "/" + file.name, user_id, int(os.environ['dlu_group']))
            for root, dirs, _ in os.walk(package_path):
                for dir in dirs:
                    subdir_path = os.path.join(root, dir)
                    if os.stat(subdir_path).st_uid != user_id or os.stat(subdir_path).st_gid != int(os.environ['dlu_group']):
                        os.chown(subdir_path, user_id, int(os.environ['dlu_group']))
        except Exception as e:
            logger.error("Error changing ownership of directory %s: %s", package_path, str(e))
            

    def rename_and_move_files(self, file_list: list[DLUFile], slide_name_map, package_id ):
        dluFiles = []
        dest_package_directory = os.path.join(self.dlu_data_directory, self.dlu_package_dir_prefix + package_id)
        if os.path.exists(dest_package_directory):
            shutil.rmtree(dest_package_directory)
        if not os.path.exists(dest_package_directory):
            logger.info("Creating directory " + dest_package_directory)
            os.makedirs(dest_package_directory, exist_ok=True)

        source_package_directory = self.globus_data_directory + '/' + self.globus_dir_prefix + package_id
        for file in file_list:
            dest_file = os.path.join(dest_package_directory, slide_name_map[file.name])
            logger.info("Copying file " + os.path.join(source_package_directory, file.name) + " to "
                        + os.path.join(dest_package_directory, slide_name_map[file.name]))
            shutil.copy(os.path.join(source_package_directory, file.name),
                        dest_file)
            file = DLUFile(name=slide_name_map[file.name], path=dest_package_directory,
                           checksum=calculate_checksum(dest_file), size=os.path.getsize(dest_file))
            dluFiles.append(file)
        return dluFiles
    
    def copy_files( self, package_id: str, file_list: list[DLUFile], preserve_path: bool = False, no_src_package: bool = False):
        files_copied = 0

        final_dest_package_directory = os.path.join(
            self.dlu_data_directory,
            self.dlu_package_dir_prefix + package_id,
        )

        dest_parent_directory = os.path.dirname(final_dest_package_directory)
        dest_package_basename = os.path.basename(final_dest_package_directory)

        if not file_list:
            logger.warning(
                "No files provided for package %s. Existing package will not be modified.",
                package_id,
            )
            return 0

        if not os.path.isdir(dest_parent_directory):
            self.dlu_management.set_dlu_package_error(package_id)
            raise FileNotFoundError(
                f"Destination parent directory does not exist: {dest_parent_directory}"
            )

        if not os.access(dest_parent_directory, os.W_OK | os.X_OK):
            self.dlu_management.set_dlu_package_error(package_id)
            raise PermissionError(
                f"Process does not have permission to write to destination parent directory: "
                f"{dest_parent_directory}"
            )

        if os.path.exists(final_dest_package_directory) and not os.path.isdir(
            final_dest_package_directory
        ):
            self.dlu_management.set_dlu_package_error(package_id)
            raise NotADirectoryError(
                f"Destination package path exists but is not a directory: "
                f"{final_dest_package_directory}"
            )

        temp_dest_package_directory = tempfile.mkdtemp(
            prefix=f".{dest_package_basename}.tmp-",
            dir=dest_parent_directory,
        )

        backup_dest_package_directory = None

        logger.info(
            "Building replacement package %s in temporary directory %s",
            package_id,
            temp_dest_package_directory,
        )

        copied_wrapper_directories = set()

        def count_files(path: str) -> int:

            if os.path.isfile(path):
                return 1

            total = 0

            for _, _, filenames in os.walk(path):
                total += len(filenames)

            return total

        def copy_path(src_path: str, dst_path: str) -> int:

            logger.info("Preparing to copy from %s to %s", src_path, dst_path)

            if not os.path.exists(src_path):
                self.dlu_management.set_dlu_package_error(package_id)
                raise FileNotFoundError(f"Source path does not exist: {src_path}")

            dst_parent = os.path.dirname(dst_path)

            if dst_parent:
                os.makedirs(dst_parent, exist_ok=True)

            if os.path.exists(dst_path):
                logger.warning("%s already exists. Skipping.", dst_path)
                return 0

            try:
                if os.path.isdir(src_path):
                    logger.info("Copying directory %s to %s", src_path, dst_path)
                    shutil.copytree(src_path, dst_path)
                    return count_files(src_path)

                if os.path.isfile(src_path):
                    logger.info("Copying file %s to %s", src_path, dst_path)
                    shutil.copy2(src_path, dst_path)
                    return 1

                raise FileNotFoundError(
                    f"Source path exists but is neither a regular file nor directory: "
                    f"{src_path}"
                )

            except FileNotFoundError:
                self.dlu_management.set_dlu_package_error(package_id)
                logger.exception(
                    "Source disappeared or destination parent was missing during copy."
                )
                raise

        def copy_directory_contents(src_dir: str, dst_dir: str) -> int:
            if not os.path.isdir(src_dir):
                self.dlu_management.set_dlu_package_error(package_id)
                raise FileNotFoundError(f"Source directory does not exist: {src_dir}")

            logger.info("Copying contents of %s into %s", src_dir, dst_dir)

            os.makedirs(dst_dir, exist_ok=True)

            copied_count = 0

            for root, dirnames, filenames in os.walk(src_dir):
                relative_root = os.path.relpath(root, src_dir)

                if relative_root == ".":
                    target_root = dst_dir
                else:
                    target_root = os.path.join(dst_dir, relative_root)

                os.makedirs(target_root, exist_ok=True)

                # Create directories even if they are empty.
                for dirname in dirnames:
                    target_dir = os.path.join(target_root, dirname)
                    os.makedirs(target_dir, exist_ok=True)

                for filename in filenames:
                    src_file = os.path.join(root, filename)
                    dst_file = os.path.join(target_root, filename)

                    if os.path.exists(dst_file):
                        logger.warning("%s already exists. Skipping.", dst_file)
                        continue

                    logger.info("Copying file %s to %s", src_file, dst_file)
                    shutil.copy2(src_file, dst_file)
                    copied_count += 1

            return copied_count

        def replace_destination_with_temp():

            nonlocal backup_dest_package_directory

            if os.path.exists(final_dest_package_directory):
                backup_dest_package_directory = tempfile.mkdtemp(
                    prefix=f".{dest_package_basename}.backup-",
                    dir=dest_parent_directory,
                )

                # tempfile.mkdtemp creates the backup directory. Remove it so
                # os.rename can move the existing package to that path.
                os.rmdir(backup_dest_package_directory)

                logger.info(
                    "Moving existing destination %s to backup %s",
                    final_dest_package_directory,
                    backup_dest_package_directory,
                )

                os.rename(
                    final_dest_package_directory,
                    backup_dest_package_directory,
                )

            try:
                logger.info(
                    "Moving temporary package %s into final destination %s",
                    temp_dest_package_directory,
                    final_dest_package_directory,
                )

                os.rename(
                    temp_dest_package_directory,
                    final_dest_package_directory,
                )

            except Exception:
                self.dlu_management.set_dlu_package_error(package_id)
                logger.exception(
                    "Failed to move temporary package into final destination."
                )

                if (
                    backup_dest_package_directory
                    and os.path.exists(backup_dest_package_directory)
                    and not os.path.exists(final_dest_package_directory)
                ):
                    logger.warning(
                        "Attempting to restore backup package from %s to %s",
                        backup_dest_package_directory,
                        final_dest_package_directory,
                    )

                    os.rename(
                        backup_dest_package_directory,
                        final_dest_package_directory,
                    )

                raise

            if (
                backup_dest_package_directory
                and os.path.exists(backup_dest_package_directory)
            ):
                try:
                    logger.info(
                        "Removing old backup package directory %s",
                        backup_dest_package_directory,
                    )
                    shutil.rmtree(backup_dest_package_directory)
                except Exception:
                    logger.exception(
                        "Package replacement succeeded, but backup cleanup failed: %s",
                        backup_dest_package_directory,
                    )

        try:
            for file in file_list:
                source_package_directory = os.path.join(
                    self.globus_data_directory,
                    self.globus_dir_prefix + ("" if no_src_package else package_id),
                )

                # This is the base temporary package directory.
                # All copying happens here first, not directly into the final destination.
                dest_package_directory = temp_dest_package_directory

                if preserve_path:
                    short_path = file.get_short_path()

                    if short_path:
                        dest_package_directory = os.path.join(
                            dest_package_directory,
                            short_path,
                        )

                logger.info("Base source package directory: %s", source_package_directory)
                logger.info("Temporary package base directory: %s", temp_dest_package_directory)
                logger.info("Per-file destination directory: %s", dest_package_directory)
                logger.info("File path: %s", getattr(file, "path", None))
                logger.info("Short filename: %s", file.get_short_filename())

                if not os.path.isdir(source_package_directory):
                    self.dlu_management.set_dlu_package_error(package_id)
                    raise FileNotFoundError(
                        f"Source package directory does not exist or is not a directory: "
                        f"{source_package_directory}"
                    )

                os.makedirs(dest_package_directory, exist_ok=True)
                
                # If source_package_directory contains exactly one item and that
                # item is a directory, treat it as a top-level wrapper directory.
                # Important:
                # Even with preserve_path=True, wrapper contents are copied into
                # temp_dest_package_directory, not dest_package_directory.
                
                try:
                    top_level_items = os.listdir(source_package_directory)
                except FileNotFoundError:
                    raise FileNotFoundError(
                        f"Cannot list source package directory because it does not exist: "
                        f"{source_package_directory}"
                    )

                if len(top_level_items) == 1:
                    only_item_name = top_level_items[0]
                    only_item_path = os.path.join(
                        source_package_directory,
                        only_item_name,
                    )

                    if os.path.isdir(only_item_path):
                        wrapper_destination = temp_dest_package_directory

                        wrapper_key = (
                            os.path.abspath(only_item_path),
                            os.path.abspath(wrapper_destination),
                        )

                        if wrapper_key not in copied_wrapper_directories:
                            logger.info(
                                "Source package directory contains a single wrapper directory. "
                                "Copying contents of %s into base package destination %s",
                                only_item_path,
                                wrapper_destination,
                            )

                            files_copied += copy_directory_contents(
                                src_dir=only_item_path,
                                dst_dir=wrapper_destination,
                            )

                            copied_wrapper_directories.add(wrapper_key)

                        else:
                            logger.info(
                                "Wrapper directory %s has already been copied to %s. Skipping.",
                                only_item_path,
                                wrapper_destination,
                            )

                        continue

                source_directory_for_file = source_package_directory

                if file.path:
                    candidate_source_directory = os.path.join(
                        source_package_directory,
                        file.path,
                    )

                    if os.path.isdir(candidate_source_directory):
                        source_directory_for_file = candidate_source_directory

                short_filename = file.get_short_filename()

                source_file = os.path.join(
                    source_directory_for_file,
                    short_filename,
                )

                dest_file = os.path.join(
                    dest_package_directory,
                    short_filename,
                )

                if os.path.exists(source_file):
                    files_copied += copy_path(
                        src_path=source_file,
                        dst_path=dest_file,
                    )
                    continue

                fallback_source_file = None

                if file.path:
                    fallback_source_file = os.path.join(
                        source_package_directory,
                        file.path,
                    )

                    if os.path.exists(fallback_source_file):
                        fallback_dest_file = os.path.join(
                            dest_package_directory,
                            os.path.basename(file.path.rstrip(os.sep)),
                        )

                        files_copied += copy_path(
                            src_path=fallback_source_file,
                            dst_path=fallback_dest_file,
                        )

                        continue

                raise FileNotFoundError(
                    "Could not find source file or directory. Tried:\n"
                    f"  source_file={source_file}\n"
                    f"  fallback_source_file={fallback_source_file}\n"
                    f"  source_package_directory={source_package_directory}\n"
                    f"  source_directory_for_file={source_directory_for_file}\n"
                    f"  file.path={getattr(file, 'path', None)}\n"
                    f"  short_filename={short_filename}"
                )

            # The entire temporary package was built successfully.
            # Only now replace the existing package.
            replace_destination_with_temp()

            logger.info(
                "Successfully copied %s files for package %s into %s",
                files_copied,
                package_id,
                final_dest_package_directory,
            )

            return files_copied

        except Exception:
            logger.exception(
                "Failed to build replacement package for %s. Existing package was not replaced.",
                package_id,
            )

            # If failure happens before final replacement, remove temp dir.
            if os.path.exists(temp_dest_package_directory):
                logger.info(
                    "Removing failed temporary package directory %s",
                    temp_dest_package_directory,
                )

                shutil.rmtree(
                    temp_dest_package_directory,
                    ignore_errors=True,
                )
            raise
    def validate_package_directories(self, package_id: str):
        source_package_directory = self.globus_data_directory + '/' + self.globus_dir_prefix + package_id
        source_directory_info = DirectoryInfo(source_package_directory, False)
        success = True

        # Make sure the directory is not empty
        if not source_directory_info.valid_for_dlu:
            success = False
            logger.error("Directory for package " + package_id + " failed validation.")
        return success

    def process_globus_directory(self, directory_listing, globus_directories: list[DirectoryInfo], package_id,
                                 initial_dir, calculate_checksums: bool = True):
        for dir in globus_directories:
            prefix = ""
            if not initial_dir == "":
                prefix = initial_dir + "/"
            current_dir = prefix + os.path.basename(dir.directory_path)

            globus_files = []
            globus_directories = []
            for item in dir.file_details:
                if os.path.isdir(item.path):
                    globus_directories.append(DirectoryInfo(item.path, calculate_checksums=calculate_checksums))
                else:
                    globus_files.append(item)
            directory_listing[current_dir] = globus_files
            if len(globus_directories) > 0:
                self.process_globus_directory(directory_listing, globus_directories, package_id, current_dir,
                                              calculate_checksums)
        return directory_listing

    def match_files(self, package_id: str, calculate_checksums: bool = True) -> list[DLUFile]:
        top_level_dir = DirectoryInfo(self.globus_data_directory + '/' + self.globus_dir_prefix + package_id,
                                      calculate_checksums=calculate_checksums)
        globus_files = []
        globus_directories = []
        for obj in top_level_dir.file_details:
            if os.path.isdir(obj.path):
                directory = DirectoryInfo(obj.path, calculate_checksums=calculate_checksums)
                globus_directories.append(directory)
            else:
                globus_files.append(obj)
        files_in_globus_directories = {}
        files_in_globus_directories[""] = globus_files
        current_dir = ""
        files_in_globus_directories = self.process_globus_directory(files_in_globus_directories, globus_directories,
                                                                    package_id, current_dir, calculate_checksums)
        return self.get_globus_file_paths(files_in_globus_directories)

    def get_globus_file_paths(self, files_in_globus_directories: dict[str, list[DLUFile]]) -> list[DLUFile]:
        fileList = []
        for dir, files in files_in_globus_directories.items():
            for file in files:
                prefix = dir + "/" if dir else ""
                file.name = prefix + file.name
                fileList.append(file)
        return fileList

    def validate_all_wsi_files_present(self, ):
        return True