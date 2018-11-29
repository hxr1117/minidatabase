import re


class User(object):
    def __init__(self):
        fp = open('/Users/hexinrong/PycharmProjects/minidatabase/AllDB/user.txt', 'r')
        usr = fp.readlines()
        fp.close()
        usr = [eval(i) for i in usr]
        self.user = usr
        self.right = ['select', 'update', 'drop', 'create', 'show', 'desc', 'delete', 'alter', 'insert', 'use']
        self.root = ['root', 'root', self.right, ['*', '*']]

    # create user //id='...',pw='...'
    def crate_user(self, line):
        try:
            id, pw = re.findall('id=(.*?)\s*?,\s*?pw=(.*?)', line)[0]
        except ValueError:
            print('语法错误')
            return False

        id, pw = id.strip("'"), pw.strip("'")  # 去除首尾'
        name = [i[0] for i in self.user]
        if id in name:
            print('已有该用户')
            return False

        new = [id, pw, [], []]  # id pw 权限表 数据库.表名
        self.user.append(new)
        self.updata_file()

    def updata_file(self):
        fp = open('/Users/hexinrong/PycharmProjects/minidatabase/AllDB/user.txt', 'w')
        for i in self.user:
            fp.write(str(i)+'\n')
        fp.close()

    # 如果存在该用户，返回用户所在位置，方便调用函数修改或确认
    def check_name_pw(self, id, pw):
        for i in range(len(self.user)):
            if id == self.user[i][0]:
                if pw == self.user[i][1]:
                    return i+1
                else:
                    print('密码错误')
                    return False
        print('无此用户')
        return False

    # grant //权限 //on 数据库.* to (id, pw);
    def grant_rights(self, rights, line, database, table):
        rights = rights.split(',')
        rights = [i.strip() for i in rights]
        rights = list(set(rights))  # 去重

        try:
            db, tb = re.findall('on\s*?(.*?)\s*?\.\s*?(.*?)\s*?to', line)[0]
            db, tb = db.strip(), tb.strip()

            id, pw = re.findall('to\s*?\((.*),(.*)\)', line)[0]
            id, pw = id.strip(), pw.strip()
        except ValueError:
            print('语法错误')
            return False

        if db not in database:
            print('无该数据库')
            return False

        if tb != '*' and tb not in table:
            print('无该表')
            return False

        check = self.check_name_pw(id, pw)
        if not check:
            return False

        check -= 1
        if 'all' in rights:
            self.user[check][2] = self.right
        else:
            for i in rights:
                if i not in self.right:
                    print('无此权限')
                    return False
            self.user[check][2] = rights

        self.user[check][3] = [db, tb]
        self.updata_file()

    # sql -u //id pw
    def sign_in(self, line):
        try:
            id, pw = line.split()
        except ValueError:
            print('语法错误')
            return False

        id, pw = id.strip("'"), pw.strip("'")  # 去除首尾'
        _ = self.check_name_pw(id, pw)
        if _:
            return self.user[_-1]
        else:
            return False

    def get_user(self):
        return self.user

    # Delete FROM user //Where User='test' and Host='localhost';
    def delete_user(self, line):
        id = re.findall('where user=(.*)', line)[0]
        id = id.strip("'")
        j = -1
        for i in range(len(self.user)):
            if self.user[i][0] == id:
                j = i
                break
        if j == -1:
            print('无此用户')
            return False
        else:
            self.user.pop(j)
            self.updata_file()
            return True

    def check_db_tb_rights(self, user, db=[], tb=[]):
        if user[3][0] == '*':
            return True
        for i in db:
            if i not in user[3][0]:
                print('您无使用该数据库的权限')
                return False
        if user[3][1] == '*':
            return True
        else:
            for i in tb:
                if i not in user[3][1]:
                    print('您无权使用该表')
                    return False

    def clear_rights(self, line):
        line = line.strip()
        id = re.findall(r"on user=(.*)", line)[0]
        id = id.strip("'")

        for i in range(len(self.user)):
            if id == self.user[i][0]:
                self.user[i][2] = []
                self.updata_file()
                return True

        print('无此用户')
        return False


if __name__ == '__main__':
    u = User()
    # u.crate_user("id='test',pw='test'")
    # u.grant_rights('select, all', 'on ccc . * to ("222", "333")', [], '')

