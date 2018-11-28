from operator import itemgetter, attrgetter
import os, re


# CREATE INDEX ON table_name (column_name) ;
class CreateIndex(object):
    def crete_index(self, db, line):
        fp = open('/Users/hexinrong/PycharmProjects/minidatabase/AllDB/' + db + '/' + db + '_dic.txt', 'r')
        dic = fp.readlines()[0]
        dic = eval(dic)
        fp.close()
        tables = dic.keys()

        line = line[:2].lower() + line[2:]
        try:
            tb_name, col_name = re.findall(r'on\s*?(.*)\s*?\((.*)\)', line)[0]
            tb_name, col_name = tb_name.strip(), col_name.strip()
        except Exception:
            print('语法错误')
            return False

        if tb_name not in tables:
            print('不存在该表')
            return False

        tb_dic = dic[tb_name]
        if col_name not in tb_dic:
            print('不存在该属性')
            return False

        if 'index' in tb_dic:
            if col_name in tb_dic['index']:
                print('该属性已有索引')
                return False
        else:
            tb_dic['index'] = []

        if self.check_index_type(dic[tb_name]['primary'], col_name):  # 建立稀疏索引
            self.create_sparse_index(db, tb_name, col_name)
            if 'index' in tb_dic:
                tb_dic['index'].append(col_name)
            else:
                tb_dic['index'] = [col_name]
        else:
            self.create_dense_index(db, tb_name, col_name)
            if 'index' in tb_dic:
                tb_dic['index'].append(col_name)
            else:
                tb_dic['index'] = [col_name]
        dic[tb_name] = tb_dic
        fp = open('/Users/hexinrong/PycharmProjects/minidatabase/AllDB/' + db + '/' + db + '_dic.txt', 'w')
        fp.write(str(dic))
        fp.close()

    def create_dense_index(self, db, tb, name):  # 创建稠密索引，生成索引文件
        fp = open('/Users/hexinrong/PycharmProjects/minidatabase/AllDB/' + db + '/' + db + '_dic.txt', 'r')
        dic = eval(fp.readlines()[0])
        dic = dic[tb]
        fp.close()
        fp = open('/Users/hexinrong/PycharmProjects/minidatabase/AllDB/' + db + '/' + tb + '.txt', 'r')
        rows = fp.readlines()
        rows = [eval(i) for i in rows]
        index = []  # [mess, row]
        if name in dic:
            fp = open('/Users/hexinrong/PycharmProjects/minidatabase/AllDB/' +
                      db + '/' + tb + '_' + name + '_index.txt', 'w')
            for i in range(len(rows)):
                index.append([rows[i][name], i])
            index.sort(key=lambda x: x[0])
            for i in index:
                fp.write(str(i)+'\n')
            fp.close()

    # value是索引的属性值和行数的列表
    def add_dense(self, db, tb, value, row_name):  # 维护索引的时候索引文件一定已经存在
        fp = open('/Users/hexinrong/PycharmProjects/minidatabase/AllDB/' + db + '/' +
                  tb + '_' + row_name+'_index.txt', 'r')
        index = fp.readlines()
        fp.close()
        fp = open('/Users/hexinrong/PycharmProjects/minidatabase/AllDB/' + db + '/' +
                  tb + '_' + row_name+'_index.txt', 'w')
        index = [eval(i) for i in index]
        index.append(value)
        index.sort(key=lambda x: x[0])
        for i in index:
            fp.write(str(i)+'\n')
        fp.close()

    def update_dense(self, db, tb, old, new, row_name):  # old和new都是列表，里面是索引文件要改的行的值，new是改后的值
        # 更新的时候的维护（其实我很想把索引文件重新create一遍就行了……不然多个值的update还要多次打开
        fp = open('/Users/hexinrong/PycharmProjects/minidatabase/AllDB/' + db + '/' +
                  tb + '_' + row_name + '_index.txt', 'r')
        index_rows = fp.readlines()
        index_rows = [eval(i) for i in index_rows]
        fp.close()

        for i in range(len(old)):
            try:
                ii = index_rows.index(old[i])  # 在索引文件中找到要修改的值的位置
                index_rows[ii] = new[i]  # 修改索引文件
            except ValueError:
                continue

        index_rows.sort(key=lambda x: x[0])
        fp = open('/Users/hexinrong/PycharmProjects/minidatabase/AllDB/' + db + '/' +
                  tb + '_' + row_name + '_index.txt', 'w')
        for i in index_rows:
            fp.write(str(i)+'\n')
        fp.close()

    # name是创建索引的属性名
    def create_sparse_index(self, db, tb, name):  # 建立稀疏索引，创建索引文件
        fp = open('/Users/hexinrong/PycharmProjects/minidatabase/AllDB/' + db + '/' + db + '_dic.txt', 'r')
        dic = eval(fp.readlines()[0])
        dic = dic[tb]
        pri = dic['primary']
        fp.close()
        fp = open('/Users/hexinrong/PycharmProjects/minidatabase/AllDB/' + db + '/' + tb + '.txt', 'r')
        rows = fp.readlines()
        fp.close()
        fp = open('/Users/hexinrong/PycharmProjects/minidatabase/AllDB/' + db + '/' + tb + '.txt', 'w')
        rows = [eval(i) for i in rows]
        # 有问题，如果是多个主键，已有一个主键建了索引，再排序会有问题。
        if len(pri) <= 1:
            rows = sorted(rows, key=itemgetter(name))  # 排序之后方便写回
        # 多个主键不排序
        for i in rows:
            fp.write(str(i) + '\n')
        fp.close()

        block_num = 3  # 人为设置每块存3条记录

        new_row = [i[name] for i in rows if i in rows]  # 单独把要建索引的属性的值提出来
        index = list(set(new_row))
        index.sort(key=new_row.index)
        index = [[i, -1] for i in index]
        new_row = [new_row[i:i+block_num] for i in range(0, len(new_row), block_num)]  # 按每block_num个分组

        for i in range(len(index)):
            for j in range(len(new_row)):
                if index[i][0] in new_row[j] and index[i][1] == -1:  # 初值是-1表示这个域值没有分配块号
                    index[i][1] = j

        index = []
        for i in range(len(new_row)):
            index.append([new_row[i][0], i*block_num])

        index = sorted(index, key=lambda x: x[0])
        fp = open('/Users/hexinrong/PycharmProjects/minidatabase/AllDB/' +
                  db + '/' + tb + '_' + name + '_index.txt', 'w')
        for i in index:
            fp.write(str(i)+'\n')
        fp.close()

    def drop_index(self, db, line):
        # 删除索引文件
        line = line.strip()
        try:
            name, tb = re.findall(r'(.*)\s*?on\s*?(.*)', line)[0]
            tb, name = tb.strip(), name.strip()
        except Exception:
            print('语法错误')
            return False

        try:
            path = '/Users/hexinrong/PycharmProjects/minidatabase/AllDB/' + db + '/' + tb + '_' + name + '_index.txt'
            os.remove(path)

            # 删除数据字典里的索引记录
            fp = open('/Users/hexinrong/PycharmProjects/minidatabase/AllDB/' + db + '/' + db + '_dic.txt', 'r')
            dic = eval(fp.readlines()[0])
            fp.close()

            fp = open('/Users/hexinrong/PycharmProjects/minidatabase/AllDB/' + db + '/' + db + '_dic.txt', 'w')
            tb_dic = dic[tb]
            tb_dic['index'].remove(name)
            dic[tb] = tb_dic
            fp.write(str(dic))
            fp.close()
        except FileNotFoundError:
            print('不存在该索引')
            return False

    def check_index_type(self, pri, name):  # pri主键列表，name属性名
        if name in pri:
            return True  # 主键，建立稀疏索引
        return False


if __name__ == '__main__':
    c = CreateIndex()
    c.create_dense_index('ccc', 'teacher', 'pw')
    # c = [{'a': 2, 'b': 2}, {'a': 1, 'b': 4}, {'a': 3, 'b': 1, 'c': 4}]
    # # print(type(i for i in c))
    # # print(sorted(c, key=itemgetter('a', 'b')))
    # d = ['a', 'b']
    # print(CreateIndex().multikeysort(c, [i for i in d]))
