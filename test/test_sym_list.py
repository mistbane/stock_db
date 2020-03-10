import stock_db.stock_db_class as sdbc

db = sdbc.Stock_Db()
symlist = db.get_sym_list()
print(symlist)


print(f"is 'spy' exist ? {db.is_sym_exist('spy')}")
print(f"is 'SPY' exist ? {db.is_sym_exist('SPY')}")
print(f"is 'SPYX' exist ? {db.is_sym_exist('SPYX')}")
