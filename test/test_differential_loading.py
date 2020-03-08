
import stock_db.stock_db_class as sdc
import stock_db.differential_loading as dfl
import datetime as dt

sym ='SPY'
db = sdc.Stock_Db()
df, rec =db.update_whole('SPY')
# print(rec)
df = db.read(sym)
# print(f"{df.tail()}\nFinish reading...")
df = df[0:-2]
print(df)
print(f"After reduce len ={len(df)}")
end = dt.datetime.now().strftime('%Y-%m-%d')
adf = dfl.df_differential_portion(sym, df, end)
print(adf)
print(f"Total {len(adf)} rec. read")
df= df.append(adf)
print(df.tail(10))


