import dj.io.json as djj
# from dj.io import from_json

class SDB_Info(object):

    def __init__(self, location):
        self.location= location
        self.filename = 'stock_dataset.info'
        self.data={'download_time':{}}
        self.load ()
    def save(self):
        djj.to_json(self.data, self.fullname)

    def load(self):
        path = pathlib.Path(self.fullname)

        if path.is_file():
            self.data= djj.from_json(self.fullname)
    @property
    def fullname(self):
        return self.location+'/'+self.filename

    def set_dl_time(self, sym, atime):
        self.data['download_time'][sym]=atime

    def dl_time(self, sym):
        return self.data['download_time'][sym]