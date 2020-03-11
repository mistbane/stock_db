import stock_db.sdb_class as sdc
import stock_db.db_update as dbu
import stock_db.sdb_lib as dbl


path =  dbl.get_stock_db_path()
db = sdc.Stock_Db()

df= db.from_db('SPY')
df=df[:-2]
db.to_db('SPY', df, path)
df= db.from_db('GOOG')
df=df[:-4]
db.to_db('GOOG', df, path)

res = db.update_batch(["amzn", "ibm"])
res = db.update_batch('spy, goog')
print(res)