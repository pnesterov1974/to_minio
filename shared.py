from datetime import datetime
from enum import Enum

class ImportAs(Enum):
    file = 1
    folder = 2
    big_file = 3

def square_oname(oname: str):
    _name = oname.replace('[', '').replace(']', '')
    return ''.join(['[', _name, ']'])

def make_name_for_pq(oname):
    return oname.replace('[', '').replace(']', '').replace(' ', '_')

def get_filename_timestamp(d):
    dt = datetime.date(d)    
    dy = dt.year
    dm = dt.month
    dd = dt.day

    tt = datetime.time(d)
    th = tt.hour
    tm = tt.minute
    ts = tt.second
    tms = tt.microsecond

    def _process_date_parts(date_part):
        _str = str(date_part)
        return _str if len(_str) > 1 else ''.join(['0', _str])
    
    di = '-'.join(list(map(_process_date_parts,[dy, dm, dd])))
    ti = '-'.join(list(map(_process_date_parts,[th, tm, ts, tms])))

    return '_'.join([di, ti])

# ---------------------------------------------------------------------------------------
if __name__ == '__main__': pass