# import stock_dataset_v0002 as sds
import os


def get_stock_db_path(location = None):
    if location is None:
        location = 'D:\wsl-work\python\projects\stock_db\stock_data'
    return location

def clear_db(path, db_type, ext):
    # print(path)
    # cmd = f"del {path}\*"
    # print(f"{cmd}")
    # os.system(cmd)

    files = os.listdir(path)
    for file in files:
        if file.endswith(ext):
            os.remove(os.path.join(path, file))

# def renew_db(symbol_list, path):
#     # clear_db(path)
#     # clear_stock_data(location)
#     res=read_batch(*symbol_list)

def get_db_sym_list(path, db_type):
    ext = db_type
    sym_list = []
    for file in os.listdir(path):
        if file.endswith(ext):
            sym_list.append( file.strip(f'.{ext}') )
            # print(os.path.join("/mydir", file))
    return sym_list
