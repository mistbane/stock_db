import db_update as dbu
import stock_db_class as sdc

db = sdc.Stock_Db()
# print (db.df())
print(db.path)
df, rec= dbu.from_src_to_db('SPY',path= db.path)
print(rec)
print (f"Length of df {len(df)}")



df = dbu.from_db('SPY', db.path)
print(df)
