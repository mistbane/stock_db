# import stock_dataset_v0002 as sds
import os
import dj.datetime.us_working_day as usw
import stock_db.differential_loading as dfl
import datetime
import stock_db.db_update as dbu
import pathlib


def get_stock_db_path(location = None):
    if location is None:
        location = 'D:\wsl-work\python\projects\stock_db\stock_data'
    return location

def clear_db(path, db_type, ext):
    # print(path)
    # cmd = f"del {path}\*"
    # print(f"{cmd}")
    # os.system(cmd)

    files = os.listdir(path)
    for file in files:
        if file.endswith(ext):
            os.remove(os.path.join(path, file))

# def renew_db(symbol_list, path):
#     # clear_db(path)
#     # clear_stock_data(location)
#     res=read_batch(*symbol_list)

def get_db_sym_list(path, db_type):
    ext = db_type
    sym_list = []
    for file in os.listdir(path):
        if file.endswith(ext):
            sym_list.append( file.strip(f'.{ext}') )
            # print(os.path.join("/mydir", file))
    return sym_list

def gdfs(symlist, path, db_type, force ):
    symlist = to_list(symlist)
    res = {}
    for sym in symlist:
        sym= sym.upper()
        df= gdf( sym, path, db_type, force)
        res[sym]=df
    return res

def gdf(sym, path, db_type,  force= False):
    db_type = db_ext(db_type)
    if is_sym_exist(sym, db_type, path):
        update_db_flag = False
        df = dbu.from_db( sym, path, db_type)
        
        # if df is update to date
        begin_date = df.iloc[-1]['Date'].date()

        
        end_date = datetime.datetime.now().date()       #Get end_date (usually today()), exclue holiday.
        print(f"Today (or Data end date) {end_date}")
        end_date_str = datetime.datetime.strftime(end_date, "%Y-%m-%d")
        end_date = usw.get_us_bday(end_date_str, 0, 0)
        print(f"Last trading day {end_date}")
        print(f"Begin of new data ( Date of last rec. in DB) {begin_date}")
    #     print(begin_date, end_date)
        ddiff =  end_date -begin_date
        print(f"force ={force}")
        if ddiff > datetime.timedelta(days=1):       # check if today is same as end_date
            update_db_flag = True
        else:
            if force:     # if so , lookup force flag 
                update_db_flag = True
            
        
        if update_db_flag :   # Update differential
            df , _ = dfl.differential_loading_to_db(sym,df,  end= end_date)

        # if not force update then skip
        # otherise update.
    else:
        df, _ =dbu.from_src_to_db(sym, path, )
    
    return df

def to_list(symlist):
    if type(symlist).__name__ =='str':
        symlist = symlist.replace(' ','').split(',')
    return symlist
    
def is_sym_exist(sym, db_type, path ):
    ext = db_type  #todo find a way to do search for file name only and not ext or to auto identify readable ext.
    path = pathlib.Path("{}/{}.{}".format(path, sym, ext))
    return path.is_file()

def db_ext(db_type):
    if db_type is None:
        db_type = 'parquet'
    return db_type