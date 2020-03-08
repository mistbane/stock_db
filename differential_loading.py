import datetime
import yfinance as yf
import dj.datetime.us_working_day as uwb

def df_differential_portion(sym, adf, str_end):
    '''
    Given a partial dataframe,  compare it with given range, load missing parts
    '''
    # end=datetime.now()
    data_end= adf.index[-1]
    data_end = data_end +datetime.timedelta(days =1)
    str_end = uwb.get_us_bday(str_end).strftime("%Y-%m-%d")
    print(f" str_end ={str_end}")
    
    # data_end= adf.Date[-1]

    # print (f"Updating {sym}: {data_end} - {str_end}")
    df=None
    start = data_end
    print(f"Reading data from yahoo service")
    print(f"Reading {sym} from {start} to {str_end}")
    df = yf.download(sym, start=start, end=str_end)
    print(f"Total {len(df)} rec. read")
    print(df)
    df['Date']=df.index
    df.Volume = df.Volume.astype(float)
    return df