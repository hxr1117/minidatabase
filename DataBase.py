import os
from Table import CreateTable


class CreateDB(object):
    def __init__(self):
        self.db_path = '/Users/hexinrong/PycharmProjects/minidatabase/AllDB/'

    def database(self):
        all_db = os.listdir(self.db_path)
        all_db.remove('.DS_Store')
        return all_db

    def create_db(self, name):
        all_db = self.database()
        if name in all_db:
            return False
        else:
            os.chdir(self.db_path)
            os.mkdir(name)
            now_path = self.db_path + name
            os.chdir(now_path)
            dic_name = name + '_dic.txt'
            # os.mknod(dic_name)
            fp = open(dic_name, 'w')
            fp.close()
        return True


class DropDB(object):
    def __init__(self):
        self.db_path = '/Users/hexinrong/PycharmProjects/minidatabase/AllDB/'

    def drop_db(self, name):
        all_db = os.listdir(self.db_path)
        all_db.remove('.DS_Store')
        if name not in all_db:  # there isn't the db
            print('没有该数据库')
            return False
        else:
            now_path = self.db_path + name
            self.del_file(now_path)
            os.rmdir(now_path)
        return True

    def del_file(self, path):
        for i in os.listdir(path):
            path_file = os.path.join(path, i)  # get file path
            if os.path.isfile(path_file):
                os.remove(path_file)
            else:
                self.del_file(path_file)


class UseDB(object):
    def __init__(self):
        self.db_path = '/Users/hexinrong/PycharmProjects/minidatabase/AllDB/'

    def use_db(self, db_name):
        all_db = os.listdir(self.db_path)[1:]
        if db_name not in all_db:
            return False
        else:
            all_table = CreateTable(db_name).table()
            return all_table


if __name__ == '__main__':
    c = CreateDB()
    print(c.database())