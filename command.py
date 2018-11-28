import os
from DataBase import CreateDB, DropDB
from Table import CreateTable, DropTable, ChangeValue, ShowTable, AlterTable
from index import CreateIndex


class Main(object):
    def __init__(self):
        self.database = ''
        self.all_table = []
        self.all_db = CreateDB().database()

    def check_name(self, name):
        if name[0] == '_' or ('Z' >= name[0] >= 'A') or ('z' >= name[0] >= 'a'):
            return True
        else:
            return False

    def main(self):
        while True:
            line = input('  >')
            line = line.strip()
            while line[-1] != ';':
                line += input('-->')

            line = line[:-1]
            line = line.split(' ', 2)
            line[0] = line[0].lower()

            if line[0] == 'create':
                if line[1] == 'database':
                    if self.check_name(line[2]):
                        ct_db = CreateDB()
                        if ct_db.create_db(line[2]):
                            self.all_db.append(line[2])
                        else:
                            print('已有同名数据库')
                    else:
                        print('数据库命名必须以下划线或字母开头')

                elif line[1] == 'table':
                    if not self.database:
                        print('建表前必须选择数据库')
                    else:
                        if self.check_name(line[2]):
                            ct_tb = CreateTable(self.database)
                            a = ct_tb.creating(line[2])
                            if a[0] is True:
                                self.all_table.append(a[1])
                            else:
                                print(a)
                        else:
                            print('表命名必须以下划线或字母开头')

                elif line[1] == 'index':
                    ind = CreateIndex()
                    ind.crete_index(self.database, line[2])
                else:
                    print('语法错误')

            elif line[0] == 'drop':
                if line[1] == 'database':
                    dp_db = DropDB()
                    if dp_db.drop_db(line[2]):
                        print(self.all_db.remove(line[2]))
                elif line[1] == 'table':
                    if not self.database:
                        print('删表前必须选择数据库')
                    else:
                        dp_tb = DropTable(self.database)
                        dp_tb.drop_table(line[2])
                        self.all_table.remove(line[2])
                elif line[1] == 'index':
                    if not self.database:
                        print('删表前必须选择数据库')
                    else:
                        CreateIndex().drop_index(self.database, line[2])
                else:
                    print('语法错误')

            elif line[0] == 'use':
                if line[1] == 'database':
                    if line[2] in self.all_db:
                        self.database = line[2]
                        self.all_table = CreateTable(self.database).table()
                        if self.all_table:
                            if '.DS_S' in self.all_table:
                                self.all_table.remove('.DS_S')
                else:
                    print('语法错误')

            elif line[0] == 'insert' and line[1] == 'into':
                if self.database:
                    a = line[2].split(' ', 1)
                    print(a[1])
                    if a[0] not in self.all_table:
                        print('没有该表')
                    else:
                        insert = ChangeValue(self.database)
                        if not insert.insert_value(a[0], a[1]):
                            continue
                else:
                    print('请先选择数据库')

            elif line[0] == 'alter':
                a = line[2].split(' ', 3)

                if a[0] not in self.all_table:
                    print('没有该表')
                    continue
                al = AlterTable(self.database, a[0])
                if a[1].lower() == 'add':
                    print(al.add_column(a[2], a[3]))
                elif a[1].lower() == 'drop':
                    print(al.del_column(a[3]))
                else:
                    print('语法错误')

            elif line[0] == 'delete' and line[1] == 'from':
                try:
                    c = ChangeValue(self.database)
                    c.del_value(line[2])
                except IndexError:
                    print('语法错误')

            elif line[0] == 'desc':
                if not self.database:
                    print('请先选择数据库')
                    continue
                if line[1] not in self.all_table or self.database == '':
                    print('没有该表')
                else:
                    show = ShowTable(self.database)
                    show.desc_table(line[1])

            elif line[0] == 'select':
                if line[1] == '*':
                    if not self.database:
                        print('请先选择数据库')
                    else:
                        a = line[2].split()
                        if a[1] in self.all_table:
                            show = ShowTable(self.database)
                            show.all_rows(line[2])
                        else:
                            print('没有该表')

            elif line[0] == 'show':
                if line[1].lower() == 'tables':
                    if not self.database:
                        print('请先选择数据库')
                    else:
                        print('Tables_In_' + self.database)
                        print('-' * len('Tables_In_' + self.database))
                        for i in self.all_table:
                            print(i)
                elif line[1].lower() == 'databases':
                    print('Database')
                    print('-'*8)
                    for i in self.all_db:
                        print(i)
                else:
                    print('语法错误')

            elif line[0] == 'update':
                if not self.database:
                    print('请先选择数据库')
                elif line[1] not in self.all_table:
                    print('没有该表')
                else:
                    ch = ChangeValue(self.database)
                    ch.update_value(line[1], line[2])
            else:
                print('语法错误')


if __name__ == '__main__':
    '''
    命令：
    1. crate database DB_name √
    2. drop database DB_name √
    3. use database DB_name √
    4. insert into table_name values(...) √
    5. insert into table_name (...) values (...) √
    6. select * from table √
    7. desc table √
    8. alter table add column (not null default) √
    9. alter table del column √
    10. update √
    11. create index on table_name(column_name) √
    12. drop index col_name on tb_name √
    '''

    main = Main()
    main.main()
