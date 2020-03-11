import sdb_class as dbc

db = dbc.Stock_Db()
df = db.df('spy')
print(df.head(3))
adf = db.slice('SPY', begin=2000)
print(adf.head(3))
print('-----------------------------')
print(df.tail(3))
adf = db.slice('spy', end =-5)
print(adf.tail(3))
adf= db.slice('spy', begin =4000, end = -10)
print(adf)

