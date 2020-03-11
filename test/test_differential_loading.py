
import stock_db.sdb_class as sdc
import stock_db.differential_loading as dfl
import datetime as dt

sym ='SPY'
db = sdc.Stock_Db()
# df, rec =db.update_whole('SPY')
# print(rec)
df = db.from_db(sym)
df= df[:-4]
print(df)

#----------------------------------------- test begin --------------
df_len = len(df.index)
adf,rec =dfl.differential_loading_to_db(sym, df,)
# print(adf)
print(f"Before update: totla {df_len}")
print(f"After update: Total {len(adf.index)}")
print(f"rec= {rec}") 
print(adf.tail(20))
# print(f"adf = {rec['SPY']['dataset']}") #  NOTE don't forget to use sym to access data.