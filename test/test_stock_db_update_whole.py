import stock_db_class as sdc
db = sdc.Stock_Db()
df, rec =db.update_whole('SPY')
print(rec)
df = db.read('SPY')
print(f"{df.tail()}\nFinish reading...")

print(f"is 'goog' exist ? {db.is_sym_exist('goog')}")
print(f"is 'SPY' exist ? {db.is_sym_exist('spy')}")