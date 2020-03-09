import stock_db.stock_db_lib as sdbl
import stock_db.db_update as dbu
import stock_dataset_v0002 as sdbs
import stock_db.data_slice as ds
import pathlib


class Stock_Db(object):
    def __init__(self, path = None):
        self.path = sdbl.get_stock_db_path(path)
        # self.db = sdbs.Stock_Dataset(self.path)  #todo change to new.

    def from_src_to_db(self, sym):
        df, rec= dbu.from_src_to_db(sym ,self.path)
        return df, rec

    def from_db(self, sym):
        df = dbu.from_db(sym, self.path)
        return df

    def df(self, sym = 'SPY'):  #todo change this to read()
        df = dbu.from_db(sym, self.path)
        return df

    def is_sym_exist(self, sym):
        '''
        Return True if sym exists in Stock_database
        '''
        ext = 'parquet'  #todo find a way to do search for file name only and not ext or auto identify readable ext.
        path = pathlib.Path("{}/{}.{}".format(self.path, sym, ext))
        return path.is_file()

    def read_sym_list(self):
        pass

    def read_batch(self, sym_list, path= None):
        res={}
        for item in sym_list:
            # _, area =   # todo : a routine automatically decide to download full or differential. 
            pass
    

    

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
    
    def to_db(self, sym, df, path, file_ext= None): 
        dbu.to_db(sym, df, path, file_ext)
