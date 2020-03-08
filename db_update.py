from df_reader import *
import pandas as pd
file_ext= 'parquet'

def read_from_src(sym, reader=DF_Yahoo_reader):  #todo how does it read portion of data instead of whole?
    res={}
    record={}
    print(f"reading {sym} from Yahoo")  #todo DF_Yahoo_reader should have id_string.
    reader= DF_Yahoo_reader(sym= sym)
    df = reader.df
    size_read =  len(df)
    df.dropna(inplace=True)
    size_dropped = size_read - len(df)
    print(f"{size_dropped} rec. dropped, {len(df)} totla data rec.")

    res['symbol']= sym
    res['portion_start']= datetime.date(reader.df.Date[0])
    res['portion_end']= datetime.date(reader.df.Date[-1])
    res['action']='new_symbol'
    res['dataset']= df

    print(f"from {res['portion_start']} to {res['portion_end']}")
    record[sym]= res
    
    return df, record

def save_to_disk(sym, df, path, file_ext= file_ext): 
    fullpath= "{}/{}.{}".format(path, sym, file_ext)
    print("Saving to {}".format(fullpath))
    # reader.df.to_json("{}".format(fullpath))
    # reader.df.to_parquet(f"{fullpath}", compression='None')
    df.to_parquet(f"{fullpath}", compression='gzip')
    print(f"{len(df)} recrods writed")

def update_whole(sym, path, reader=DF_Yahoo_reader, file_ext=file_ext):
    '''
    Similar to read() but return both df and status object
    Load data from internet provider (currently Yahoo)
    At moment only locaiton is used
    file_ext, start, end are not Used
    Download full length of ticker.
    '''
    df, record= read_from_src(sym, reader=DF_Yahoo_reader)    
    save_to_disk(sym, df, path, file_ext)
    return df, record


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
    # print(fullpath)
    df = pd.read_parquet(fullpath)
    df =df.sort_index()
    df.Volume= df.Volume.astype(float)
    return df