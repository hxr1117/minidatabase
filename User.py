import re


class User(object):
    def __init__(self):
        fp = open('/Users/hexinrong/PycharmProjects/minidatabase/AllDB/user.txt', 'r')
        usr = fp.readlines()
        fp.close()
        usr = [eval(i) for i in usr]
        self.root = ['root', 'root', self.right, ['*', '*']]
        self.user = usr
        self.right = ['select', 'update', 'drop', 'create', 'show', 'desc', 'delete', 'alter', 'insert', 'user']

    # create user //id='...',pw='...'
    def crate_user(self, line):
        try:
            id, pw = re.findall('id=(.*?)\s*?,\s*?pw=(.*?)', line)[0]
        except IndexError:
            print('语法错误')
            return False
        name = [i[0] for i in self.user]
        if id in name:
            print('已有该用户')
            return False

        new = [id, pw, [], []]  # id pw 权限表 数据库.表名
        self.user.append(new)
        self.change_file()

    def change_file(self):
        fp = open('/Users/hexinrong/PycharmProjects/minidatabase/AllDB/user.txt', 'w')
        for i in self.user:
            fp.write(str(i)+'\n')
        fp.close()

    # 如果存在该用户，返回用户所在位置，方便调用函数修改或确认
    def check_name_pw(self, id, pw):
        for i in range(len(self.user)):
            if id == self.user[i][0]:
                if pw == self.user[i][1]:
                    return i
                else:
                    print('密码错误')
                    return False
        print('无此用户')
        return False

    # grant //权限 //on 数据库.* to (id, pw);
    def grant_rights(self, rights, line, db, table):
        rights = rights.split(',')
        rights = [i.strip() for i in rights]
        rights = list(set(rights))  # 去重

        try:
            db, tb = re.findall('on\s*?(.*?)\s*?\.\s*?(.*?)\s*?to', line)[0]
            db, tb = db.strip(), tb.strip()

            id, pw = re.findall('to\s*?\((.*),(.*)\)', line)[0]
            id, pw = id.strip(), pw.strip()
            print(id, pw)
        except IndexError:
            print('语法错误')
            return False

        if db not in db:
            print('无该数据库')
            return False

        if tb != '*' and tb not in tb:
            print('无该表')
            return False

        check = self.check_name_pw(id, pw)
        if not check:
            return False

        if 'all' in rights:
            self.user[check][2] = self.right
        else:
            for i in rights:
                if i not in self.right:
                    print('无此权限')
                    return False
            self.user[check][2] = rights

        self.user[check][2] = [db, tb]

        self.change_file()

    # sql -u //id pw
    def sign_in(self, line):
        try:
            id, pw = line.split()
        except IndexError:
            print('语法错误')
            return False

        _ = self.check_name_pw(id, pw)
        if _:
            return _
        else:
            return False

    def get_user(self):
        return self.user


if __name__ == '__main__':
    u = User()
    u.crate_user("id='test',pw='test'")
    u.grant_rights('select, all', 'on ccc . * to ("222", "333")', [], '')

