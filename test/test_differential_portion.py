
import stock_db.sdb_class as sdc
import stock_db.differential_loading as dfl
import datetime as dt

sym ='SPY'
db = sdc.Stock_Db()
# df, rec =db.update_whole('SPY')
# print(rec)
df = db.from_db(sym)
# print(f"{df.tail()}\nFinish reading...")
df = df[0:-2]
print(df)
print(f"After reduce len ={len(df)}")
end_str = dt.datetime.now().strftime('%Y-%m-%d')
adf = dfl.df_differential_portion(sym, df, end_str)
print(adf)
print(f"Total {len(adf)} rec. read")
df= df.append(adf)
print(df.tail(10))
#-----------------------------
print('-'*20)
df= df[:-3]
print(df)
df_len = len(df.index)
adf,rec =dfl.differential_loading_to_db(sym, df,)
print(adf)
print(f"Before update: totla {df_len}")
print(f"After update: Total {len(adf.index)}")
print(rec)
# print(f"Rec. read: {len(adf)}")


