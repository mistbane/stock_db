from df_reader import *
import pandas as pd
file_ext= 'parquet'

def update_whole(sym, path, reader=DF_Yahoo_reader, file_ext=file_ext):
    '''
    Similar to read() but return both df and status object
    Load data from internet provider (currently Yahoo)
    At moment only locaiton is used
    file_ext, start, end are not Used
    Download full length of ticker.
    '''
    res={}
    record={}
    reader= DF_Yahoo_reader(sym= sym)
    reader.df.dropna(inplace=True)
    fullpath= "{}/{}.{}".format(path, sym, file_ext)
    print("Saving to {}".format(fullpath))
    # reader.df.to_json("{}".format(fullpath))
    # reader.df.to_parquet(f"{fullpath}", compression='None')
    reader.df.to_parquet(f"{fullpath}", compression='gzip')
    # self.info.set_dl_time(ticker, (datetime.now().strftime('%Y.%m.%d')))
    # self.info.save()

    res['symbol']= sym
    res['portion_start']= datetime.date(reader.df.Date[0])
    res['portion_end']= datetime.date(reader.df.Date[-1])
    res['action']='new_symbol'
    res['dataset']= reader.df
    record[sym]= res
    return reader.df, record


def read_db_sym( sym, path, file_ext=file_ext):
    '''
    Read data from location
    Only return df part.
    ticker : symbol , filename
    location: file path, None for default
    '''
    sym= sym.upper()
    # fullpath= "{}/{}.{}".format(self.location, ticker, self.file_ext)
    fullpath= r"{}\{}.{}".format(path, sym, file_ext)
    # df= pd.read_json(fullpath)
    print(fullpath)
    df = pd.read_parquet(fullpath)
    df =df.sort_index()
    df.Volume= df.Volume.astype(float)
    return df