# import sdb_lib as sdbl
# import db_update as dbu
# # import stock_dataset_v0002 as sdbs
# import data_slice as ds
# import differential_loading as dfl
# import pathlib
# import df_reader as dfr

import stock_db.sdb_lib as sdbl
import stock_db.db_update as dbu
# import stock_dataset_v0002 as sdbs
import stock_db.data_slice as ds
import stock_db.differential_loading as dfl
import pathlib
import df_reader as dfr

class Stock_Db(object):
    """[Directory oriented file based (.parguet) stock data system]
    
    Property:
        path: Where the stock db file reside, if set to None then default will be used.
    
    Method:
    ---- Often use function ----
        dfs: Reads symbol list (list or str) and return a dict of {"sym":{df}, "sym2":{df}...}
        df: Check for last trading day from today , if db is older update the difference.

    ---- Basic operation ----
        from_src: Read a symbol from SRC.
        to_db: Write a df to DB.
        from_src_to_db:   Read from SRC ( default yfinance) and save it to DB
        from_db:  Get data from DB

    ---- Support Function ----
        is_sym_exist: check if sym in DB
        get_sym_list : Return a list of sym in DB.
        differential_loading_to_db : check between df and end_str dates then update the shift days of data to DB.

    ---- Maintainence Function ----
        update_batch: Read a sym_list (str or list) if sym not exist then update from SRC otherwise update the difference.
        update_db: Read a sym, if sym not exist then update from SRC otherwise update the difference.

    ---- Utility Function ----
        slice: Return a dataframe between start and end of given df.
        clear_db: Delete all symbol files in DB
        renew_db: Refresh every symbol in symlist from SRC to DB.

    ---- Class default ----
        db_path: Return default DB path.
        db_type: Currently will return 'parguet' only.
    """

    def __init__(self, path = None):
        # self.path = sdbl.get_stock_db_path(path)
        self.path = self.db_path(path) 

    #-------------------------------------- Often use function ----
    def dfs(self, symlist, path = None, force= False):
        self.path = path 
        res = sdbl.gdfs(symlist, path, force)
        return res

    def df(self, sym = 'SPY', path= None, force=False): 
        self.path = path 
        adf =sdbl.gdf(sym, self.path, force= force)
        return adf

    #-------------------------------------- Basic operation ----
    def from_src_to_db(self, sym, path=None):
        self.path = path
        df, rec= dbu.from_src_to_db(sym ,self.path)
        return df, rec

    def from_db(self, sym, path = None):
        self.path = path 
        df = dbu.from_db(sym, self.path)
        # print(f"Retriving data from {fullpath}")
        # print(f"Retriving data from {self.path}")
        print(f"Read {sym} {len(df) } rec.\tPath: {self.path}")
        return df

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
    
    def to_db(self, sym, df, path = None, file_ext= 'parquet'): 
        self.path = path 
        fullpath = dbu.to_db(sym, df, self.path, file_ext)
        # print("Saving to {}".format(fullpath))
        print(f"Write {sym} {len(df)}rec.\tPath: {fullpath}")

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
        self.path = path 
        sym_list = sdbl.get_db_sym_list(self.path, db_type)
        return sym_list

    def differential_loading_to_db(self, sym, df= None, end_str= None, path = None,  shift =10):
        self.path = path 
        # ------------------------------------- DEBUG -----
        df, rec = dfl.differential_loading_to_db(sym, df, end_str, path= self.path, shift=shift)
        
        
        return df, rec

    def update_batch(self, sym_list, path= None):
        self.path = path 
        res={}
        if type(sym_list).__name__ == 'str':
            sym_list= sym_list.split(', ')

        for sym in sym_list:
            _, aRes = self.update_db(sym, self.path )
            res={**res, **aRes}
            # _, area =  
        return res

    def update_db(self, sym, path= None, reader=None, shift=10): 
        '''
        if sym not in stock dataset then loading it otherwise
        call differential_loading
        '''
        self.path = path 

        if reader is None:
            reader = dfr.DF_Yahoo_reader()
        sym=sym.upper()
        if not self.is_sym_exist(sym):
            print('Adding {} into dataset.'.format(sym))
            df,ares= self.from_src_to_db(sym, path= self.path  )
        else:
            adf = self.df(sym)
            df, ares=self.differential_loading_to_db(sym, df= adf, shift= shift, path = self.path)
        
        return df, ares

    def slice(self, sym = 'SPY', begin=None, end= None):
        df = self.df(sym)
        adf = ds.slice(df, begin , end )
        return adf


    def db_path(self, path=None):
        return sdbl.get_stock_db_path(path)
    
    def db_type(self, type_str):
        return 'parquet'
    
    def clear_db(self, path= None, db_type= None):
        self.path = path 
        ext = self.db_type(db_type)
        print(f"Deleteing Databasei: {path}")
        sdbl.clear_db(path, self.path, ext)

    def renew_db(self, sym_list, path = None, ext= None):
        ext = self.db_type(ext)
        self.path = path 
        self.clear_db(self.path, )
        self.update_batch(sym_list, self.path)
        print(f"Reloaded dataset {sym_list}")

    @property 
    def path(self,):
        return self._path
    
    @path.setter
    def path(self, value):
        if value is None:
            value = self.db_path()
        self._path = value



