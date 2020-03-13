import stock_db.sdb_lib as sdbl
import stock_db.db_update as dbu
# import stock_dataset_v0002 as sdbs
import stock_db.data_slice as ds
import stock_db.differential_loading as dfl
import pathlib
import df_reader as dfr

# TODO: Add an audit between two dfs, audit(df1, df2, start_index)
# TODO  Add an autodetecting is data in db is too old, if so automatically update it.
class Stock_Db(object):
    def __init__(self, path = None):
        # self.path = sdbl.get_stock_db_path(path)
        self.path = self.db_path(path) 

    def from_src_to_db(self, sym):
        df, rec= dbu.from_src_to_db(sym ,self.path)
        return df, rec

    def from_db(self, sym):
        df = dbu.from_db(sym, self.path)
        # print(f"Retriving data from {fullpath}")
        # print(f"Retriving data from {self.path}")
        print(f"Read {sym} {len(df) } rec.\tPath: {self.path}")
        return df

    def df(self, sym = 'SPY'): 
        df =self.from_db(sym)
        return df

    def is_sym_exist(self, sym, db_type=None):
        '''
        Return True if sym exists in Stock_database
        '''
        if db_type is None:
            db_type = 'parquet'
        ext = db_type  #todo find a way to do search for file name only and not ext or to auto identify readable ext.
        path = pathlib.Path("{}/{}.{}".format(self.path, sym, ext))
        return path.is_file()

    def get_sym_list(self, path = None, db_type = "parquet"):
        path = self.path
        sym_list = sdbl.get_db_sym_list(path, db_type)
        return sym_list

    def differential_loading_to_db(self, sym, df= None, end_str= None):
        
        # ------------------------------------- DEBUG -----
        df, rec = dfl.differential_loading_to_db(sym, df, end_str)
        
        
        return df, rec

    def update_batch(self, sym_list, path= None):
        if path is None:
            path = sdbl.get_stock_db_path()
        res={}
        if type(sym_list).__name__ == 'str':
            sym_list= sym_list.split(', ')

        for sym in sym_list:
            _, aRes = self.update_db(sym, path )
            res={**res, **aRes}
            # _, area =  
        return res

    def update_db(self, sym, path= None, reader=None): #BUG off by 1 in db.
        '''
        if sym not in stock dataset then loading it otherwise
        call differential_loading
        '''

        if reader is None:
            reader = dfr.DF_Yahoo_reader()
        sym=sym.upper()
        if not self.is_sym_exist(sym):
            print('Adding {} into dataset.'.format(sym))
            df,ares= self.from_src_to_db(sym  )
        else:
            adf = self.df(sym)
            df, ares=self.differential_loading_to_db(sym, df= adf)
        
        return df, ares

    def slice(self, sym = 'SPY', begin=None, end= None):
        df = self.df(sym)
        adf = ds.slice(df, begin , end )
        return adf

    def from_src(self, sym, reader = None):
        """[ummary]
            Read from data source and retreive df and relative info.
            good with multiple retieve.
        
        Arguments:  
            sym {[string]} -- [symbol the will be retrieved]
        
        Keyword Arguments:
            reader {[DF_Reader]} -- [DF_Reader class that will handle reeading from all kind of source] (default: {None})
        Returns:
            [df] -- [pandas dataframe],
            [rec] -- [a dictionary of result info, inclduing the df itself.]
        """
        df, rec =dbu.from_src(sym, reader)
        return df, rec
    
    def to_db(self, sym, df, path, file_ext= 'parquet'): 
        fullpath = dbu.to_db(sym, df, path, file_ext)
        # print("Saving to {}".format(fullpath))
        print(f"Write {sym} {len(df)}rec.\tPath: {fullpath}")

    def db_path(self, path=None):
        return sdbl.get_stock_db_path(path)
    
    def db_type(self, type_str):
        return 'parquet'
    
    def clear_db(self, path= None, db_type= None):
        path = self.db_path(path)
        ext = self.db_type(db_type)
        print(f"Deleteing Databasei: {path}")
        sdbl.clear_db(path,path, ext)

    def renew_db(self, sym_list, path = None, ext= None):
        ext = self.db_type(ext)
        path = self.db_path(path)
        self.clear_db(path, )
        self.update_batch(sym_list, path)
        print(f"Reloaded dataset {sym_list}")



