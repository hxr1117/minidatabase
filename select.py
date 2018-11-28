class Selected(object):
    def __init__(self, db='', tb='', db_dic={}):
        self.operate = ['<', '>', '=', '<=', '>=', '!=', '!']
        self.db = db
        self.tb = tb
        self.db_dic = db_dic

    def check_col_in_table(self, table, col):
        tb = []
        for i in table:
            if col in self.db_dic[i]:
                tb.append(i)



    def projection(self, line, table, db):  # 投影
        line = line.split(',')
        line = [i.strip() for i in line]

        fp = open('/Users/hexinrong/PycharmProjects/minidatabase/AllDB/'+db+'/'+db+'_dic.txt', 'r')
        dic = eval(fp.readlines()[0])
        fp.close()

        for i in range(len(line)):
            line[i] = line[i].strip()
            if line[i] not in dic[table]:
                print('该表没有该属性')
                return False

        fp = open('/Users/hexinrong/PycharmProjects/minidatabase/AllDB/' + db + '/' + table + '.txt', 'r')
        rows = fp.readlines()
        rows = [eval(i) for i in rows]
        fp.close()

        for i in line:
            print('{:<15}'.format(i), end='')
        print()
        print('-' * len(line) * 15)

        for i in rows:
            for j in line:
                if j in i:
                    print('{:<15}'.format(i[j]), end='')
            print()

    # select * from 关系名 where 条件表达式
    def where(self, line):
        line = line.split('and')
        line = [i.strip() for i in line]
        condition = []
        oper = []
        value = []
        ok = 0
        for i in line:  # 把where的条件分开考虑
            for j in range(len(i)):  # 对每个字符进行处理
                if ok != 1:  # 上一个不是操作符
                    if i[j] in self.operate:  # 如果当前字符是操作符（下一个也有可能是操作符）
                        ok = 1
                        condition.append(i[:j])
                else:  # 上一个是操作符，这个有可能也是
                    if i[j] in self.operate:  # 这个也是操作符
                        # where id <= 1000
                        # 01234567890
                        oper.append(i[j-1:j+1])
                        value.append(i[j+1:])
                    else:  # 这一个不是操作符
                        oper.append(i[j-1])
                        value.append(i[j:])
                    ok = 0
                    break

        condition, value = [i.strip() for i in condition], [i.strip() for i in value]
        print(condition, oper, value)
        return condition, oper, value

    def between_select_from(self, line):
        line = line.split(',')
        line = [i.split() for i in line]

        tb = []
        col = []
        for i in line:
            if '.' in i:  # 规范的语法的分割
                a = i.split('.')
                tb.append(a[0].strip())

if __name__ == '__main__':
    s = Selected()
    # s.projection('id,pw', 'root', 'bbb')
    s.where("pw='123' and id != 1000")