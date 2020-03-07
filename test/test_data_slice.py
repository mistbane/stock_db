import data_slice as ds
import stock_db_class as sdc

db = sdc.Stock_Db()
df = db.df()
print(df.tail(3))
adf = ds.slice(df, end= -5)
print(adf.tail(3))

print(adf.head(3))
adf = ds.slice(df, begin =1000)
print(adf.head(3))

adf = ds.slice(df, begin =1000, end =-5)
print(adf.head(3))
print(adf.tail(3))

