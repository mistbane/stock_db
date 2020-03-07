import stock_db_lib as sdbl
import db_update as dbu
import stock_dataset_v0002 as sdbs
import data_slice as ds


class Stock_Db(object):
    def __init__(self, path = None):
        self.path = sdbl.get_stock_db_path(path)
        self.db = sdbs.Stock_Dataset(self.path)  #todo change to new.

    def update_diff(self, sym):
        pass

    
    def update_whole(self, sym):
        df, rec= dbu.update_whole(sym ,self.path)
        return df, rec

    def read(self, sym):
        df = dbu.read_db_sym(sym, self.path)
        return df

    def is_sym_exist(self, sym):
        pass

    def read_sym_list(self):
        pass

    
    
    def df(self, sym = 'SPY'):  #todo change this to read()
        return self.db.data(sym)
        # return self.db.db.data(sym)

    def slice(self, sym = 'SPY', begin=None, end= None):
        df = self.df(sym)
        adf = ds.slice(df, begin , end )
        return adf

    def add_sym(self, sym):
        pass





    def read_from_src(self, sym, reader = None):
        df, rec =dbu.read_from_src(sym, reader)
        return df, rec
    
    def save_to_disk(self, sym, df, path, file_ext= None): 
        dbu.save_to_disk(sym, df, path, file_ext)
