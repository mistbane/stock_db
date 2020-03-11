import stock_db.sdb_class as sdc
db = sdc.Stock_Db()
df, rec =db.from_src_to_db('SPY')
print(rec)
df = db.from_db('SPY')
print(f"{df.tail()}\nFinish reading...")
