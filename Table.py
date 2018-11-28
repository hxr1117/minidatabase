import os
import re
from index import CreateIndex
from operator import itemgetter, attrgetter
from copy import deepcopy
# import pandas

'''
data dict:
    ['Table name'] = {'id':[1, 0, varchar, 20, 0]}
    
    [type, length, can_not_empty]
    
    char = ('CHAR', 'char', 'Char')
    int = ('INT', 'int')
    verchar = ('verchar', 'VERCHAR')
    float = ('float', 'FLOAT')
    bool = ('bool', 'BOOL')
    string = ('string', 'STRING')
    type_nolength = ('char', 'int', 'float', 'bool', 'string', 'verchar')
    type_length = ('char', 'int', 'float', 'bool', 'string', 'verchar')
'''


class CreateTable(object):
    def __init__(self, db_name):
        self.db_path = '/Users/hexinrong/PycharmProjects/minidatabase/AllDB/' + db_name + '/'
        self.type = ('char', 'int', 'float', 'bool', 'string', 'verchar')
        self.db_name = db_name

    # 判断是否有同名表
    def check_name(self, tname):
        all_table = self.table()
        # print(all_table)
        if tname in all_table:
            return False
        else:
            return True

    # 返回该库的所有表名
    def table(self):
        all_table = os.listdir(self.db_path)
        # all_table.remove('.DS_Store')
        all_table = [i[:-4] for i in all_table]
        return all_table

    def creating(self, command):
        # 接受并用正则简单处理参数
        table_name, args = re.findall(r'(.*?)\s*?\((.*)\)$', command)[0]  # [table_name, args]
        # 判断是否有该表
        if not self.check_name(table_name):
            return False
        self.table_name = table_name
        os.chdir(self.db_path)

        # 读数据字典里的每个表的信息
        fp = open(self.db_path + self.db_name + '_dic.txt', 'r')
        data = fp.readlines()
        fp.close()
        if data:
            data = eval(data[0])
        else:
            data = {}  # 数据库的数据字典的字典

        # 稍微处理下参数
        args = args.split(',')
        for i in range(len(args)):
            args[i] = args[i].strip().split()

        # 将新的表的信息存入数据字典
        table = {}
        data[table_name] = table
        table['primary'] = []
        table['foreign'] = []
        table['index'] = []

        for i in args:
            if len(i) < 2:
                return 0  # 缺少东西，名字或长度类型
            for j in range(len(i)):
                key = i[j].lower()
                if j == 0:
                    name = i[j]
                    if name[0] == "'":
                        name = name[1:]
                    if name[-1] == "'":
                        name = name[:-1]
                    table[name] = [0] * 3
                if j == 1:
                    try:
                        mess = re.findall(r'(.*)\s*?\((.*)\)$', key)[0]
                    except IndexError:
                        return 0
                    if len(mess) < 2:
                        return 1
                    if mess[0] not in self.type:
                        return '类型错误'
                    table[name][0] = mess[0]
                    table[name][1] = int(mess[1])
                if key == 'primary':  # 这个是主键
                    table['primary'].append(name)
                if key == 'null':
                    table[name][2] = 1
                if key == 'foreign':
                    table['foreign'].append(name)

        # 将新的数据字典存入文件
        fp = open(self.db_path + self.db_name + '_dic.txt', 'w')
        data[table_name] = table
        fp.write(str(data))
        fp.close()
        table_name += '.txt'
        fp = open(table_name, 'w')
        fp.close()
        return True, table_name[:-4]


class DropTable(object):
    def __init__(self, db_name):
        self.db_path = '/Users/hexinrong/PycharmProjects/minidatabase/AllDB/' + db_name + '/'
        self.db_name = db_name

    def drop_table(self, tb_name):
        all_tb = os.listdir(self.db_path)
        all_tb.remove('.DS_Store')
        if tb_name + '.txt' not in all_tb:  # 没有这个表
            return False
        else:
            os.remove(self.db_path + tb_name + '.txt')  # 删除这个表的内容
            # 在数据字典删除这个表的信息
            fp = open(self.db_path + self.db_name + '_dic.txt', 'r')
            data = eval(fp.readlines()[0])
            if 'index' in data[tb_name]:
                index = data[tb_name]['index']  # 该表的索引
                for i in index:
                    os.remove(self.db_path + tb_name + '_' + i + '_index' + '.txt')  # 删除该表的索引
            fp.close()
            fp = open(self.db_path + self.db_name + '_dic.txt', 'w')
            del data[tb_name]
            if not data:
                data = ''
            fp.write(str(data))
            fp.close()
            return True


class ChangeValue(object):
    def __init__(self, db_name):
        self.db_path = '/Users/hexinrong/PycharmProjects/minidatabase/AllDB/' + db_name + '/'
        self.db_name = db_name
        # self.tb_name = tb_name

    def sort_primary(self, pri, rows):
        rows = sorted(rows, key=lambda k: ([k[i] for i in pri]))
        return rows

    def get_rows(self, tb):
        fp = open(self.db_path + tb + '.txt', 'r')
        rows = fp.readlines()
        rows = [eval(i) for i in rows]
        return rows

    def not_repeat(self, value, tb_name, db_dic={}, rows=[]):  # value是新的元组的字典
        p = []
        if not db_dic or not rows:
            # 获取该表所有内容
            fp = open(self.db_path + tb_name + '.txt', 'r')
            rows = fp.readlines()
            rows = [eval(i) for i in rows]
            fp.close()

            # 从数据字典获取该表的主键名称
            f_dic = open(self.db_path + self.db_name + '_dic.txt', 'r')
            db_dic = f_dic.readlines()[0]
            db_dic = eval(db_dic)  # 获取该数据库所有数据信息
            tb_dic = db_dic[tb_name]  # 提取该表的数据字典
            p_name = tb_dic['primary']  # 获取该表主键列表

        else:
            tb_dic = db_dic[tb_name]
            p_name = tb_dic['primary']

        # 提取所有元组的主键内容
        for i in p_name:  # 如果有多个主键也可以提取（只要没有bug）
            j = [k.get(i) for k in rows]  # 每个元组的每个主键的值都存到p里
            p.append(j)

        if len(p_name) == 1:
            try:
                p_name = p_name[0]
                p = [k.get(p_name) for k in rows]
                primary = value[p_name]
                if primary in p:  # 如果新的主键已存在
                    return False
                else:
                    return True
            except Exception:
                return False

        else:  # 多个主键
            primary = []  # 新的元组的主键
            for i in p_name:
                kk = [k.get(i) for k in rows]  # 把每个主键的值都提取出来
                # p.append(kk)  # 存放多个主键的值, 每个主键的值的个数一样
                # print(kk)
                # print('vvv', value)
                try:
                    primary.append(value[i])
                except KeyError:
                    return False
            _ = []

            if p and primary:
                for i in range(len(p[0])):
                    kk = []
                    for j in range(len(p)):
                        kk.append(p[j][i])

                    _.append(kk)
                p = _
                if primary in p:
                    return False
                else:
                    return True
            else:
                return True

    def check_type(self, l, value):
        if l[0] == 'int':
            try:
                value = int(value)
                return value
            except ValueError:
                print('属性类型错误')
                return False
        elif l[0] == 'float':
            try:
                value = float(value)
                return value
            except ValueError:
                print('属性类型错误')
                return False
        else:
            if len(value) <= int(l[1]):
                return value
            else:
                print('超出属性长度')
                return False

    def insert_value(self, tb_name, value):
        f_dic = open(self.db_path + self.db_name + '_dic.txt', 'r')
        dic = f_dic.readlines()
        f_dic.close()
        if not dic:
            return False
        dic = eval(dic[0])  # 获取数据字典
        db_dic = deepcopy(dic)
        dic = dic[tb_name]  # 表数据字典
        pri = dic['primary']  # 获取主键名称

        value = value[:6].lower() + value[6:]
        v = re.findall(r'\((.*)\)\s*?values', value)

        ind = []
        col_name = []
        rows = self.get_rows(tb_name)
        if not v:  # values前面没有东西，全部列插入
            value = re.findall(r"values\s*?\((.*)\)", value)  # 要插入的值

            if value:
                s = {}
                value = value[0].strip().split(',')
                value = [i.strip() for i in value]
                if 'index' in dic:
                    index = dic['index']  # 获取索引属性列表
                    del dic['index']
                else:
                    index = []

                del dic['primary']
                del dic['foreign']

                if len(dic) == len(value):
                    for i, j in zip(list(dic.keys()), value):
                        if i == 'primary' or i == 'foreign' or i == 'index':
                            continue
                        else:  # i是主键名称
                            condition = dic[i]  # 每个属性的信息核对
                            check = self.check_type(condition, j)
                            if check:
                                s[i] = check
                            else:
                                print('语法错误')
                                return False
                            # 如果这个属性在索引列表里
                            if i in index:  # 如果这个表有多个索引也可以???
                                ind.append([j, -1])
                                col_name.append(i)

                else:
                    print('属性不足')
                    return False
            else:
                print('语法错误')
                return False

        else:
            p = []
            v = v[0].split(',')
            v = [i.strip() for i in v]

            _ = re.findall(r"values\s*?\((.*)\)", value)[0].split(',')
            _ = [i.strip() for i in _]
            _ = [i[1:-1] for i in _]

            value = [v, _]

            if len(value[0]) == len(value[1]):
                s = {}
                # 检查主键是否完整
                pri = dic['primary']  # 主键列表

                # 获取不可为空的属性名称
                not_null = []
                for i in dic:
                    if i != 'primary' and i != 'foreign' and i != 'index':
                        if dic[i][2] == 1:
                            not_null.append(i)

                for i in range(len(value[1])):  # 有值的主键从主键列表删除
                    if value[1][i] in pri:
                        pri.remove(value[1][i])
                        p.append(value[0][i])
                    if value[1][i] in not_null:
                        not_null.remove(value[1][i])

                if pri:
                    print('请插入主键值完整的元组')
                    return False  # 有的主键没有值
                if not_null:
                    print('请插入不可为空的属性完整的元组')
                    return False  # 有不可为空的属性为空

                inddex = []
                pri = dic['primary']
                del dic['primary']
                del dic['foreign']
                if 'index' in dic:
                    inddex = dic['index']
                    del dic['index']
                ind = []

                for i in dic:
                    if i in value[1]:  # 如果这个属性存在
                        # 匹配位置，哪个位置我也忘了……
                        index = value[1].index(i)
                        v = value[0][index]
                        check = self.check_type(dic[i], v)
                        if check:
                            s[i] = check
                        else:
                            print('语法错误')
                            return False
                        if i in inddex:
                            ind.append([v, -1])  # v是索引的值
                            col_name.append(i)

        fp = open(self.db_path + tb_name + '.txt', 'w')
        if self.not_repeat(s, tb_name, db_dic, rows):
            rows.append(s)
            rows = self.sort_primary(pri, rows)
            for i in rows:
                fp.write(str(i) + '\n')
            fp.close()
        else:
            return False

        # 如果这张表有索引，把索引的值存到索引文件
        if ind:
            for i in range(len(col_name)):
                index_file = self.db_path + tb_name + '.txt'
                col_num = len(["" for line in open(index_file, "r")])
                ind[i][1] = col_num - 1
                if col_name[i] in pri:  # 这个要建索引的属性是主键，用稀疏索引
                    CreateIndex().create_sparse_index(self.db_name, tb_name, col_name[i])
                else:
                    CreateIndex().add_dense(self.db_name, tb_name, ind[i], col_name[i])

    def del_value(self, line, ok=0):
        line = line.strip().split(' ', 2)
        all_tb = CreateTable(self.db_name).table()

        if len(line) == 1:  # 没有where，删除表的所有内容
            if line[0] in all_tb:  # 有这个表的话
                fp = open(self.db_path + line[0] + '.txt', 'w')
                fp.close()

        else:  # 假设这个where就一个条件吧……
            a = line[2]
            aa = list(re.findall(r'(.*)\s*?=\s*?(.*)', a)[0])  # key是属性名, con是值
            key, con = aa[0], aa[1]
            a = []
            # 检查数据字典
            fp = open(self.db_path + self.db_name + '_dic.txt', 'r')
            dic = eval(fp.readlines()[0])  # 数据库的数据字典
            fp.close()
            if line[0] in all_tb:  # 存在该表
                dic = dic[line[0]]  # 表的字典

                if key not in dic:
                    print('没有该值的元组')
                    return False
                mess = dic[key]
                if mess[0] == 'int':
                    con = int(con)
                elif mess[0] == 'float':
                    con = float(con)
                fp = open(self.db_path + line[0] + '.txt', 'r')
                rows = fp.readlines()
                fp.close()
                new_row = []
                if rows:
                    rows = [eval(i) for i in rows]
                    for i in rows:
                        if i[key] != con:  # 如果元素相等就删除
                            new_row.append(i)
                        else:
                            a.append(i)
                    fp = open(self.db_path + line[0] + '.txt', 'w')
                    for i in new_row:
                        fp.write(str(i)+'\n')
                    fp.close()
                if ok == 1:
                    return a
            else:
                print('没有该表')
                return False

    def update_value(self, tb_name, value):
        value = value.strip().split()

        if len(value) == 2 and value[0] == 'set':  # 没有where
            value = value[1]
            key, value = re.findall(r'(.*)\s*?=\s*?(.*)', value)[0]  # key是要修改的属性名，value是改后的值
            if not value or not key:
                return False
            fp = open(self.db_path + tb_name + '.txt', 'r')
            rows = fp.readlines()
            fp.close()
            fp = open(self.db_path + self.db_name + '_dic.txt', 'r')
            dic = eval(fp.readlines()[0])
            fp.close()
            dic = dic[tb_name]
            pri = dic['primary']
            index = []
            if 'index' in dic:
                index = dic['index']

            key = key[1:-1]
            if key not in dic:
                print('没有这个属性')
                return False

            mess = dic[key]
            old, new = [], []
            rows = [eval(i) for i in rows]
            if rows:
                for i in range(len(rows)):
                    # if key in rows[i]:
                    check = self.check_type(mess, value)  # 为什么要check这个变量？？不应该check value吗
                    if check:
                        value = check
                        if key in pri:  # 修改的是主键
                            _ = rows[i]
                            _[key] = value
                            if not self.not_repeat(_, tb_name):
                                print('主键重复')
                                return False
                        if key in index:
                            old.append([rows[i][key], i])
                            new.append([value, i])
                        rows[i][key] = value

                    else:
                        return False

            # # 修改的属性有索引
            # if key in index:
            #     if key not in pri:
            #         CreateIndex().update_dense(self.db_name, tb_name, old, new, key)
            #     else:
            #         CreateIndex().create_sparse_index(self.db_name, tb_name, key)

        else:
            change = value[1]
            key = value[3]

            change = list(re.findall(r'(.*)\s*?=\s*?(.*)', change)[0])  # 'pw'='456' 要修改的
            key = list(re.findall(r'(.*)\s*?=\s*?(.*)', key)[0])  # 'id'='admin' 限定条件
            if '' in change or '' in key:
                return 'No Key'
            else:
                change[0] = change[0][1:-1]
                key[0] = key[0][1:-1]
                fp = open(self.db_path + self.db_name + '_dic.txt', 'r')
                dic = eval(fp.readlines()[0])
                db_dic = dic
                fp.close()
                if tb_name not in dic:
                    print('没有该表')
                    return False  # 没有这个表
                dic = dic[tb_name]  # 表的数据字典
                pri = dic['primary']
                index = []
                if 'index' in dic:
                    index = dic['index']

                if change[0] not in dic or key[0] not in dic:
                    print('没有该列')  # 没有这个列
                    return False

                mess = dic[change[0]]
                check = self.check_type(mess, change[1])

                # 修改后的类型是否符合
                if check:
                    change[1] = check
                else:
                    return False

                fp = open(self.db_path + tb_name + '.txt', 'r')
                rows = fp.readlines()
                fp.close()
                old, new = [], []
                rows = [eval(i) for i in rows]
                for i in range(len(rows)):
                    if rows[i][key[0]] == key[1]:
                        if change[0] in pri:  # 如果修改的是主键
                            rr = deepcopy(rows)  # 复制，之后的判断重复用
                            # rr[i][change[0]] = change[1]
                            r = deepcopy(rr[i])
                            r[change[0]] = change[1]
                            rr.remove(rows[i])  # 去掉要修改的，再判断修改完的是否重复
                            if not self.not_repeat(r, tb_name, db_dic, rr):  # 有重复
                                print('主键重复')
                                return False
                            else:
                                if change[0] in index:
                                    old.append([rows[i][change[0]], i])
                                    new.append([change[1], i])
                        if change[0] in index:
                            old.append([rows[i][change[0]], i])
                            new.append([change[1], i])
                        rows[i][change[0]] = change[1]

                key = change[0]

        rows = self.sort_primary(pri, rows)
        fp = open(self.db_path + tb_name + '.txt', 'w')
        for i in rows:
            fp.write(str(i)+'\n')
        fp.close()

        if key in index:
            if key in pri:
                CreateIndex().create_sparse_index(self.db_name, tb_name, key)
            else:
                CreateIndex().update_dense(self.db_name, tb_name, old, new,key)


class AlterTable(object):
    def __init__(self, db_name, tb_name):
        self.path = '/Users/hexinrong/PycharmProjects/minidatabase/AllDB/' + db_name + '/'
        self.db_name = db_name
        self.tb_name = tb_name
        self.all_type = ('char', 'int', 'float', 'bool', 'string', 'verchar')

    def add_column(self, col_name, data_type):  # 不用修改索引
        # 修改数据库的数据字典
        fp = open(self.path + self.db_name + '_dic.txt', 'r')
        dic = eval(fp.readlines()[0])
        fp.close()
        tb_dic = dic[self.tb_name]  # 打开表的数据字典

        data_type = data_type.lower()

        mess = list(re.findall(r'(.*?)\s*?\((.*?)\)(.*)', data_type)[0])  # 第一个是数据类型，第二个是长度，int也要有长度
        if mess[2] == '':
            mess = mess[:-1]

        if len(mess) == 2:  # 没有not null
            if mess[0] in self.all_type:
                try:
                    mess[1] = int(mess[1])
                    tb_dic[col_name] = [mess[0], mess[1], 0]
                except TypeError:
                    return '长度请输入数字'

        elif len(mess) > 2:
            # 如果加入了主属性
            p = True if re.search('primary key', data_type, re.I) else False
            if p:
                if tb_dic['primary']:
                    tb_dic['primary'].append(col_name)
                else:
                    return '没有主属性的表不可加入主属性'
            mess = [i.strip() for i in mess]
            if mess[0] in self.all_type:
                try:
                    mess[1] = int(mess[1])
                    if p:
                        v = re.findall('default=(.*)primary key', mess[2])
                    else:
                        v = re.findall('default=(.*)', mess[2])

                    if not v:
                        return '没有默认初始值'
                    v = v[0]
                    v = v.split()
                    if len(v) > mess[1]:
                        return '默认值超过设定长度'
                    if mess[0] == 'int':
                        v = int(v)
                    tb_dic[col_name] = [mess[0], mess[1], 1]
                except TypeError:
                    return '长度请输入数字'

                # 修改所有数据值
                fp = open(self.path + self.tb_name + '.txt', 'r')
                rows = fp.readlines()
                rows = [eval(i) for i in rows]
                fp.close()
                for i in range(len(rows)):
                    rows[i][col_name] = v
                fp = open(self.path + self.tb_name + '.txt', 'w')
                for i in rows:
                    fp.write(str(i)+'\n')
                fp.close()

        else:
            return '语法错误'

        dic[self.tb_name] = tb_dic
        # print(dic)
        fp = open(self.path + self.db_name + '_dic.txt', 'w')
        fp.write(str(dic))
        fp.close()
        return '添加成功'

    def del_column(self, col):
        # 修改数据库的数据字典
        fp = open(self.path + self.db_name + '_dic.txt', 'r')
        dic = eval(fp.readlines()[0])
        fp.close()
        tb_dic = dic[self.tb_name]  # 打开表的数据字典

        if col in tb_dic:
            del tb_dic[col]  # 从数据字典删除属性信息
            dic[self.tb_name] = tb_dic
            fp = open(self.path + self.db_name + '_dic.txt', 'w')
            fp.write(str(dic))
            fp.close()

            # 从表文件删除该列
            fp = open(self.path + self.tb_name + '.txt', 'r')
            mess = fp.readlines()
            fp.close()
            mess = [eval(i) for i in mess]
            fp = open(self.path + self.tb_name + '.txt', 'w')
            for i in mess:
                if col in i:
                    del i[col]
                fp.write(str(i) + '\n')
            fp.close()

        else:
            return '该表没有该属性'


class ShowTable(object):
    def __init__(self, db_name):
        self.db_path = '/Users/hexinrong/PycharmProjects/minidatabase/AllDB/' + db_name + '/'
        self.db_name = db_name
        self.db_dic = eval(open(self.db_path + db_name + '_dic.txt').readlines()[0])

    # 显示表的属性
    def desc_table(self, tb_name):
        if tb_name not in self.db_dic:
            return 'No this Table'

        mess = self.db_dic[tb_name]  # 属性信息
        a = ['Field', 'Type', 'Null', 'Key']
        for i in a:
            print('{:<15}'.format(i), end='')
        print()
        print('-'*4*15)

        for i in mess:
            if i != 'primary' and i != 'foreign':
                print('{:<15}'.format(i), end='')  # 输出属性名称
                a = mess[i]  # 单个属性的信息列表
                b = []
                b.append(a[0]+'('+str(a[1])+')')
                if a[2] == 0:
                    b.append('Yes')
                else:
                    b.append('No')
                if i in mess['primary']:
                    b.append('PRI')
                else:
                    b.append('')
                for j in b:
                    print('{:<15}'.format(j), end='')
            else:
                continue
            print()

    def all_rows(self, value):
        value = value.strip().split()  # 第一个是from，第二个是表名
        if len(value) > 2:
            return '语法错误'
        try:
            if value[1] not in self.db_dic:  # 如果表不存在
                return '没有这个表'
        except IndexError:
            return '语法错误'

        tb_dic = self.db_dic[value[1]]
        del tb_dic['primary']
        del tb_dic['foreign']

        # 得到所有元组
        fp = open(self.db_path + value[1] + '.txt', 'r')
        rows = fp.readlines()
        rows = [eval(i) for i in rows]
        fp.close()

        # 输出表头
        for i in tb_dic:
            print('{:<18}'.format(i), end='|')
        print()
        print('-'*len(tb_dic)*18)

        for i in rows:
            for j in tb_dic:
                if j in i:
                    a = i[j]
                else:
                    a = ''
                print('{:<18}'.format(a), end='|')
            print()


if __name__ == '__main__':
    s = [{'course_id': "'222'", 'student_id': "'444'", 'score': 80},
         {'course_id': "'111'", 'student_id': "'555'", 'score': 30},
         {'course_id': "'222'", 'student_id': "'555'", 'score': 80},
         {'course_id': "'444'", 'student_id': "'555'", 'score': 90},
         {'course_id': "'222'", 'student_id': "'999'", 'score': 80},
         {'course_id': "'666'", 'student_id': "'333'", 'score': 30}]
    pri = ['course_id', 'student_id']
    s.sort(key=lambda k: ([k[i] for i in pri]))
    print(s)
    # c = AlterTable('ccc', 'teacher')
    # c.add_column('bbb', 'char (10)')



