import db_update as dbu
import stock_db_class as sdc

db = sdc.Stock_Db()
# print (db.df())
print(db.db.location)
df, rec= dbu.update_whole('SPY',path= db.db.location)
print(rec)
print (f"Length of df {len(df)}")



df = dbu.read_db_sym('SPY', db.db.location)
print(df)
