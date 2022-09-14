from decimal import Decimal
from uuid import UUID

import pyarrow as pa
from setup_source import SetupSource

class ImportPqSchema:
    snapshot_dt = None

    def __init__(self):
        self.__arr_table = None
        self.__import_schema = {}

    def init_columns(self, rec):
        for k, v in rec.items():
            self.__import_schema[k] = {
                #'type': type(v),
                'lst': [],
                'arr': None
            }
        self.__import_schema['RunPackID'] = {
            #'type': 'int',
            'lst': [],
            'arr': None
        }
        self.__import_schema['SnapshotDT'] = {
            #'type': 'varchar',
            'lst': [],
            'arr': None
        }
        #self.__import_schema = d

    def clear_lists(self):
        if self.__import_schema:
            for _, v in self.__import_schema.items():
                #v['type'] = None
                v['lst'] = []
                v['arr'] = None
            self.__arr_table = None

    def __adjust_field_value(self, value):
        if isinstance(value, UUID):
            return str(value)
        elif isinstance(value, bool):
            return 1 if value else 0
        else:
            return value

    def __append_value_to_list(self, field_name: str, value):
        # добавление конкретного value в список v['lst']
        # то, в какой список добавляеть, ищется в структуре по ключу - "имя поля"
        _v = self.__adjust_field_value(value)
        if self.__import_schema:
            self.__import_schema[field_name]['lst'].append(_v)

    def append_record_to_list(self, rec: dict):
        # добавление values для всех полей записи rec
        if rec:
            for k, v in rec.items():
                self.__append_value_to_list(k, v)
        else:
            raise ValueError(
                    f'для добавления в схему передана пустая запись'
                )

    def __make_array(self, field_name: str):
        # все запсии добавили - для переданного имени поля создаем pa.array
        tpp = None
        _data = self.__import_schema[field_name]['lst']
        for v in _data:
            if v != None:
                if isinstance(v, str):
                    tpp = pa.string()
                    break
                elif isinstance(v, int):
                    tpp = pa.int32()
                    break
                elif isinstance(v, Decimal):
                    tpp = tpp = pa.decimal128(38, 20)
                    break
                else:
                    # Неизвестный тип
                    print(f'Неизвестный тип данных: tp={type(v)}, v={v}')
                    raise ValueError
        print(f'field_name: {field_name} .. pyarrow type:{tpp} .. real python type:{type(v)}')
        if tpp:
            self.__import_schema[field_name]['arr'] = pa.array(_data, type=tpp)
        else:
            raise ValueError(
                    f'схема данных не инициализирована'
                )

    def __make_arrays(self):
        # создание pa.array для всех полей
        print('==== Внутренние поля =')
        for k in self.__import_schema:
            if len(self.__import_schema[k]['lst']) > 0: # Если есть значения для поля с именем k
                self.__make_array(k)
        print('\n')

    def make_table(self):
        #  по созданным pa.array-ам создаем pyarrow.table
        if self.__import_schema:
            self.__make_arrays()
            l_fieleldnames = []
            l_arr = []
            for k, v in self.__import_schema.items():
                field_name = k
                #print(f'k={k}, field_name={field_name}')
                arr = v['arr']
                if field_name and arr:
                    l_fieleldnames.append(field_name)
                    l_arr.append(arr)
            self.__arr_table = pa.table(l_arr, names=l_fieleldnames)
            return self.__arr_table
    
    def schema_is_clean(self):
        if self.__import_schema:
            field_name = list(self.__import_schema.keys())[0]
            l = self.__import_schema[field_name]['lst']
            return len(l) == 0
        else:
            return True

# ---------------------------------------------------------------------------------------
if __name__ == '__main__': pass