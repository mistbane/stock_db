from df_reader import *
import pandas as pd
file_ext= 'parquet'

def from_src(sym, reader=DF_Yahoo_reader):  #todo how does it read portion of data instead of whole?
    """[ummary]
        Read from data source and retreive df and relative info.
        good with multiple retieve.
    
    Arguments:  
        sym {[string]} -- [symbol the will be retrieved]
    
    Keyword Arguments:
        reader {[DF_Reader]} -- [DF_Reader class that will handle reeading from all kind of source] (default: {DF_Yahoo_reader}
    Returns:
        [df] -- [pandas dataframe],
        [rec] -- [a dictionary of result info, inclduing the df itself.]
    """
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

def to_db(sym, df, path, file_ext= file_ext): 
    fullpath= "{}/{}.{}".format(path, sym, file_ext)
    # print("Saving to {}".format(fullpath))
    # reader.df.to_json("{}".format(fullpath))
    # reader.df.to_parquet(f"{fullpath}", compression='None')
    df.to_parquet(f"{fullpath}", compression='gzip')
    # print(f"{len(df)} recrods writed")
    return fullpath

def from_src_to_db(sym, path, reader=DF_Yahoo_reader, file_ext=file_ext):
    '''
    Similar to read() but return both df and status object
    Load data from internet provider (currently Yahoo)
    At moment only locaiton is used
    file_ext, start, end are not Used
    Download full length of ticker.
    '''
    df, record= from_src(sym, reader=DF_Yahoo_reader)    
    to_db(sym, df, path, file_ext)
    return df, record


def from_db( sym, path, file_ext=file_ext):
    '''
    Read data from location
    Only return df part.
    ticker : symbol , filename
    location: file path, None for default
    '''
    sym= sym.upper()
    # fullpath= "{}/{}.{}".format(self.location, ticker, self.file_ext)
    fullpath= r"{}\{}.{}".format(path, sym, file_ext)
    df = pd.read_parquet(fullpath)
    df =df.sort_index()
    df.Volume= df.Volume.astype(float)
    return df