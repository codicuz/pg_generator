import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def main():
    print('Hello')

def create_db():
    conn = psycopg2.connect(host="localhost", database="postgres", password="postgres", user="postgres")
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    
    cursor.execute('DROP DATABASE IF EXISTS test_db;')
    print(cursor.query.decode())
    cursor.execute('CREATE DATABASE test_db;')
    print(cursor.query.decode())
    cursor.close()
    conn.close()

def create_test_table():
    conn = psycopg2.connect(host="localhost", database="test_db", password="postgres", user="postgres")
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    
    cursor.execute('drop table if exists test_tbl;')
    print(cursor.query.decode())
    cursor.execute('create table if not exists test_tbl (sername varchar(40), name varchar(40), middle_name varchar(40));')
    print(cursor.query.decode())
    cursor.close()
    conn.close()

class TestRecord():
    @property
    def sername(self):
        return self.__sername
    
    @sername.setter
    def sername(self, sername):
        self.__sername = sername

    @property
    def name(self):
        return self.__name
    
    @property
    def middle_name(self):
        return self.__middle_name


    def get_insert(self):
        return f'''insert into test_tbl values('{self.sername}', '{self.name}', '{self.middle_name}');'''


    def __init__(self, sername, name, middle_name):
        self.__sername = sername
        self.__name = name
        self.__middle_name = middle_name


def get_record_by_sequence():
    
    conn = psycopg2.connect(host="localhost", database="test_db", password="postgres", user="postgres")
    cursor = conn.cursor()

    conn.commit()
    
    for i in range(1000):
        rec = TestRecord('Пушкин', 'Александ', 'Сергеевич')
        rec.sername += '_' + str(i)
        cursor.execute(rec.get_insert())
        print(cursor.query.decode())

        if i % 10 == 0:
            conn.commit()
    
    conn.commit()

if __name__ == '__main__':
    main()
    # create_db()
    create_test_table()
    get_record_by_sequence()
