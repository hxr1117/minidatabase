import os
from DataBase import CreateDB, DropDB
from Table import CreateTable, DropTable, ChangeValue, ShowTable, AlterTable
from index import CreateIndex
from User import User
from Selection import Selected


class Main(object):
    def __init__(self):
        self.database = ''
        self.all_table = []
        self.all_db = CreateDB().database()
        self.all_user = User().get_user()
        self.user = []
        self.root = User().root

    def check_name(self, name):
        if name[0] == '_' or ('Z' >= name[0] >= 'A') or ('z' >= name[0] >= 'a'):
            return True
        else:
            return False

    def check_right(self, line):
        try:

            if line in self.user[2]:
                return True
        except IndexError:
            pass
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

            if line[0] == 'create' and self.check_right(line[0]):
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
                elif line[1] == 'user':
                    if self.user[0] != 'root':
                        print('无此权限')
                        continue
                    else:
                        User().crate_user(line[2])

                elif line[1] == 'index':
                    ind = CreateIndex()
                    ind.crete_index(self.database, line[2])
                else:
                    print('语法错误')

            elif line[0] == 'drop' and self.check_right(line[0]):
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

            elif line[0] == 'use' and self.check_right(line[0]):
                if line[1] == 'database':
                    if self.user[3][0] != '*':
                        if line[2] not in self.user[3][0]:
                            print('无此权限')
                            continue
                    if line[2] in self.all_db:
                        self.database = line[2]
                        self.all_table = CreateTable(self.database).table()
                        if self.all_table:
                            if '.DS_S' in self.all_table:
                                self.all_table.remove('.DS_S')
                else:
                    print('语法错误')

            elif line[0] == 'insert' and line[1] == 'into' and self.check_right(line[0]):
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

            elif line[0] == 'alter' and self.check_right(line[0]):
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

            elif line[0] == 'delete' and line[1] == 'from' and self.check_right(line[0]):
                line[2] = line[2].strip()
                if line[2][:4].lower() == 'user':
                    if self.user[0] == 'root':
                        User().delete_user(line[2])
                        continue
                try:
                    c = ChangeValue(self.database)
                    c.del_value(line[2])
                except IndexError:
                    print('语法错误')

            elif line[0] == 'desc' and self.check_right(line[0]):
                if not self.database:
                    print('请先选择数据库')
                    continue
                if line[1] not in self.all_table or self.database == '':
                    print('没有该表')
                else:
                    show = ShowTable(self.database)
                    show.desc_table(line[1])

            elif line[0] == 'select' and self.check_right(line[0]):
                if not self.database:
                    print('请选择数据库')
                else:
                    a = line[0] + line[1] + line[2]
                    s = Selected(self.database)
                    s.selection(a, self.database)

            elif line[0] == 'show' and self.check_right(line[0]):
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
                elif line[1].lower() == 'user':
                    user = User().get_user()
                    for i in user:
                        print(i)
                else:
                    print('语法错误')

            elif line[0] == 'update' and self.check_right(line[0]):
                if not self.database:
                    print('请先选择数据库')
                elif line[1] not in self.all_table:
                    print('没有该表')
                else:
                    ch = ChangeValue(self.database)
                    ch.update_value(line[1], line[2])

            elif line[0] == 'sql' and line[1] == '-u':
                _ = line[2].split()
                try:
                    if _[0] == self.root[0]:
                        if _[1] == self.root[1]:
                            self.user = self.root
                            continue
                        else:
                            print('密码错误')
                            continue
                except IndexError:
                    pass
                _ = User().sign_in(line[2])
                if _:
                    self.user = _
                    print(self.user)

            elif line[0] == 'grant':
                if self.user[0] == 'root':
                    User().grant_rights(line[1], line[2], self.all_db, self.all_table)
                else:
                    print('无此权限')

            elif line[0] == 'clear':
                if self.user[0] == 'root':
                    User().clear_rights(line[2])
                else:
                    print('无此权限')

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
    12. drop index col_name on tb_name   √
    13. create user id='...',pw='...'  √
    14. delete from user where user='...'  √
    15. clear rights //on user='...'  √
    16. grant rights on db.tb to (id,pw) √
    '''

    main = Main()
    main.main()
