import os.path

from settings import DATA_DIR, FILE_EXT
from shared import ImportAs#, get_filename_timestamp
 # BASE_DIR - не менятеся, самый верхний уровнеь
 # DEST_DIR - верхний уровень - объект импорта = source_table
            #   вычитывается из SetupGlobalTables
# SNAPSHOT_DIR - отметка текущего времени(=base_dir для pyarrow)
#dest_dir = self.__setup.dest_name
# dt конкатенируется с DEST_DIR, добавляется расширение .pq

class LFS:
    __base_dir = DATA_DIR
    __file_ext = FILE_EXT
    snapshot_dt = None

    @staticmethod
    def __walk_lfs(lfs_full_source_name: str):
        tree = os.walk(lfs_full_source_name)
        for addr, _, files in tree:
            for name in files:
                source_file_fullpath = os.path.join(addr, name)
                #print(f'{source_file_fullpath=}')
                # далее из source_file_fullpath нужно убрать base_dir
                dest_file_short_path = source_file_fullpath.replace(LFS.__base_dir, '').replace('\\','/')
                yield (source_file_fullpath, dest_file_short_path)

    def __init__(self, dest_name: str, import_as: ImportAs):
        self.__lfs_info = None
        self.__dest_name = dest_name
        self.__import_as = import_as
        self.__build_lfs_info()

    lfs_info = property(lambda self: self.__lfs_info)

    def __build_lfs_info(self):
        d = {}
        if self.__import_as == ImportAs.folder:
            sdt = '-'.join(['SnapshotDT', LFS.snapshot_dt])
            _full_source_name = os.path.join(LFS.__base_dir, self.__dest_name, sdt)
            d['full_source_name'] = _full_source_name
            print(f'{d=}')
        elif self.__import_as in [ImportAs.file, ImportAs.big_file]:
            _file_name = '.'.join([self.__dest_name, LFS.__file_ext])
            d['full_source_name'] = os.path.join(LFS.__base_dir, _file_name)
            d['short_dest_name'] = _file_name
        self.__lfs_info = d
        
    def walk_lfs(self):
        return [
            (s, d) for s, d in LFS.__walk_lfs(self.lfs_info['full_source_name'])
        ]
        
# ---------------------------------------------------------------------------------------
if __name__ == '__main__': pass