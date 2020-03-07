
def slice( df , begin=None, end= None):

    if begin == None:
        begin = 0
    if end == None:
        end = -1
    
    begin = df.index[begin]
    end = df.index[end]
    print(f"begin: {begin},end : {end}")
    adf = df[begin:end]
    # print(adf)
    return adf
    