# import stock_dataset_v0002 as sds
import os

def get_stock_db_path(location = None):
    if location is None:
        location = 'D:\wsl-work\python\projects\stock_db\stock_data'
    return location

def clear_stock_data(path):
    print(f"Deleteing Database...")
    cmd = f"del {path}\* -Force"
    os.system(cmd)

def renew(symbol_list, db):
    clear_stock_data(db.location)
    # clear_stock_data(location)
    print(f"Reload dataset {symbol_list}")
    res=db.read_batch(*symbol_list)

def get_db_sym_list(path, db_type):
    ext = db_type
    sym_list = []
    for file in os.listdir(path):
        if file.endswith(ext):
            sym_list.append( file.strip(f'.{ext}') )
            # print(os.path.join("/mydir", file))
    return sym_list
