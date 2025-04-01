import os
import shutil


def get_unique_filename(file_path):
    """确保文件名唯一，防止覆盖已有文件。"""
    directory = os.path.dirname(file_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)

    if not os.path.exists(file_path):
        return file_path

    base, ext = os.path.splitext(file_path)
    counter = 1
    new_path = f"{base}_{counter}{ext}"
    while os.path.exists(new_path):
        counter += 1
        new_path = f"{base}_{counter}{ext}"
    return new_path


def copy_and_rename_json(src_path, dest_path):
    """复制并重命名 JSON 文件，防止覆盖。"""
    if not os.path.exists(src_path):
        raise FileNotFoundError(f"源文件不存在: {src_path}")

    unique_dest = get_unique_filename(dest_path)
    shutil.copy2(src_path, unique_dest)
    print(f"✅ 文件已复制并重命名为: {unique_dest}")
    
    
def delete_file(file_path):
    """删除文件，如果文件存在的话."""
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"✅ 删除文件: {file_path}")
    else:
        print(f"❌ 文件不存在: {file_path}")
