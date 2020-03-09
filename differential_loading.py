import datetime
import yfinance as yf
import dj.datetime.us_working_day as uwb
import stock_db.db_update as dbu
def df_differential_portion(sym, adf, str_end):
    '''
    Given a partial dataframe,  compare it with given range, load missing parts
    '''
    # end=datetime.now()
    data_end= adf.index[-1]
    data_end = data_end +datetime.timedelta(days =1)
    str_end = uwb.get_us_bday(str_end).strftime("%Y-%m-%d")
    # print(f"str_end ={str_end}")
    
    # data_end= adf.Date[-1]

    # print (f"Updating {sym}: {data_end} - {str_end}")
    df=None
    start = data_end
    print(f"Reading data from yahoo service")
    print(f"Reading {sym} from {start} to {str_end}")
    df = yf.download(sym, start=start, end=str_end)
    print(f"Total {len(df)} rec. read")
    # print(df)
    df['Date']=df.index
    df.Volume = df.Volume.astype(float)
    return df


# todo convert time zone between -8 and +8 
def differential_loading(sym, df= None, end= datetime.datetime.now()):
    '''
    Loading differential portion of data and merge it into main database.
    '''
    if df is None:
        df= dbu.read_db_sym(sym)
        # adf= adf.sort_index()
    print(end)
    print(df.index[-1])
    # print(f"{end} < {adf.index[-1]}, is {end <= adf.index[-1]}")
    end_str = end.strftime('%Y-%m-%d')
    adf=df_differential_portion(sym, df, end_str)
    if len(adf.index) >0:
        df= df.append(adf)
        print(f"{sym} {len(adf.index)} rec. appened, Total: {len(df.index)} rec.")
        # print(f"Rec. read: {len(adf)}")
        # return df
        # if end <= adf[-1].index:
        # if not self.match_ticker_download_time(sym, adf):
        res={}
        record={}        
        idf= self.df_differential_portion(sym, adf, end)
        res['symbol']=sym
        res['portion_start']= datetime.date(idf.Date[0])
        res['portion_end']= datetime.date(idf.Date[-1])
        res['action']='diff'
        res['dataset']=idf
        record[sym]=res        

        #     fullpath= "{}/{}.{}".format(self.location, sym, self.file_ext)
        #     print("Saving to {}".format(fullpath))
        #     adf.drop_duplicates(subset='Date',inplace=True)
        #     adf.to_json("{}".format(fullpath))
        #     self.info.set_dl_time(sym, (datetime.now().strftime('%Y.%m.%d')))
        #     self.info.save()
    else:
        print("failed")
        record= {}
    return df, record
