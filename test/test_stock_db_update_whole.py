import stock_db.stock_db_class as sdc
db = sdc.Stock_Db()
df, rec =db.from_src_to_db('SPY')
print(rec)
df = db.from_db('SPY')
print(f"{df.tail()}\nFinish reading...")

print(f"is 'goog' exist ? {db.is_sym_exist('goog')}")
print(f"is 'SPY' exist ? {db.is_sym_exist('spy')}")