import stock_dataset_v0002 as sds
import os

def get_stock_db_path(location = None):
    if location is None:
        location = 'D:\wsl-work\python\projects\stock-sheet-update\stock_data_json'
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

# import pandas as pd 
# path = get_stock_db_path()
# print(path)
# db = sds.Stock_Dataset()

# # clear_stock_data(path)
# renew(['SPY'], db )
# print (db.data('spy'))
