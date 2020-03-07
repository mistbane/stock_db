import stock_db_lib as sdbl
import db_update as dbu
import stock_dataset_v0002 as sdbs

class Stock_Db(object):
    def __init__(self, path = None):
        self.path = sdbl.get_stock_db_path(path)
        self.db = sdbs.Stock_Dataset(self.path)  #todo change to new.

    def update_diff(self, sym):
        pass
    
    def update_whole(self, sym):
        df, rec= dbu.update_whole(sym ,self.path)
        return df, rec

    def save(self, sym):
        pass

    def read(self, sym):
        df = dbu.read_db_sym(sym, self.path)
        return df

    def is_sym_exist(self, sym):
        pass

    def read_sym_list(self):
        pass

    

    def df(self, sym = 'SPY'):
        return self.db.db.data(sym)

    def add_sym(self, sym):
        pass


db = Stock_Db()
# print (db.df())
print(db.db.location)
# df, rec= dbu.update_whole('SPY',path= db.db.location)
# print (df)
# print(rec)


# df = dbu.read_db_sym('SPY', db.db.location)
# print(df)

# db.update_whole('SPY')
df = db.read('SPY')
print(f"{df}\nFinish reading...")
