import os
import shutil

# 文件夹路径
folder_path = os.getcwd()
file_list = [f for f in os.listdir(folder_path) if f.endswith('.csv')]

# 遍历并按顺序重命名文件
for i, file_name in enumerate(file_list):
    old_path = os.path.join(folder_path, file_name)
    file_name = file_name[30:]
    file_name = file_name[:4]
    new_file_name = file_name + '.csv'  # 新文件名
    new_path = os.path.join(folder_path, new_file_name)
    os.rename(old_path, new_path)