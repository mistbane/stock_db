import datetime
import yfinance as yf
import dj.datetime.us_working_day as uwb
import stock_db.db_update as dbu
import stock_db.sdb_lib as sdbl

def df_differential_portion(sym, adf, end_str):
    '''
    Given a partial dataframe,  compare it with given range, load missing parts
    '''
    
    data_begin= adf.index[-1]
    # data_begin = data_begin +datetime.timedelta(days =1)
    # print(f" diff data_begin= {data_begin}")
    end_date_str = uwb.get_us_bday(end_str).strftime("%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date_str, "%Y-%m-%d")
    end_date += datetime.timedelta(days=1)
    end_date_str = datetime.datetime.strftime(end_date, "%Y-%m-%d")
    df=None
    start = data_begin
    # print(f"Reading data from yahoo service")
    print(f"Retrieveng {sym} from {start} to {end_date_str} from Yahoo service")
#     df = yf.download(sym, start=start, end=str_end)
    df = yf.download(sym, start=start, end=end_date_str)
    print(f"Retrieve {sym} {len(df)} rec. read")
    df['Date']=df.index
    df.Volume = df.Volume.astype(float)
    return df


# todo convert time zone between -8 and +8 
def differential_loading_to_db(sym, df= None, end= None, path= None, ext=None, shift=10): # todo change ext to more flexible format.
    '''
    Loading differential portion of data and merge it into main database.
    '''
    if df is None:
        df= dbu.from_db(sym)
    if end is None:
        end =datetime.datetime.now()+datetime.timedelta(days=3)
    if path is None:
        path= sdbl.get_stock_db_path() 
    if ext is None:
        ext = 'parquet'
    
    # rec_shift = shift # recrod that will be overwritten
    
    # df = df[:-1]  # Remove last record because last day data might be incorrect.
    df = df[:-shift]  # Remove last record because last day data might be incorrect.
    end_str = end.strftime('%Y-%m-%d')
    adf=df_differential_portion(sym, df, end_str)
    
    if len(adf.index) >0:
        df= df.append(adf)
        print(f"{sym} {len(adf.index)} rec. appened, Total: {len(df.index)} rec.")
        res={}
        record={}        
        res['symbol']=sym
        # res['portion_start']= adf.Date[0].date() # ??? do we need this?
        # res['portion_end']= adf.Date[-1].date()
        res['action']='diff'
        res['dataset']=adf
        record[sym]=res        
        dbu.to_db(sym, df, path, ext)
        #     fullpath= "{}/{}.{}".format(self.location, sym, self.file_ext)
        #     print("Saving to {}".format(fullpath))
        #     adf.drop_duplicates(subset='Date',inplace=True)
        #     adf.to_json("{}".format(fullpath))
        #     self.info.set_dl_time(sym, (datetime.now().strftime('%Y.%m.%d')))
        #     self.info.save()
    else:
        record= {}
    return df, record
