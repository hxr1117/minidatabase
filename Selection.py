from Table import ShowTable, CreateTable
import re


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

    # select * from 关系名 where 条件表达式
    def condition_interpret(self, line):
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
        return condition, oper, value

    # select(,,,)from
    def attribute_interpret(self, line):
        line = line.split(',')
        line = [i.strip() for i in line]

        tb = []
        col = []
        for i in line:
            if '.' in i:  # 规范的语法的分割
                a = i.split('.')
                tb.append(a[0].strip())
                col.append(a[1].strip())
            else:
                tb.append('')
                col.append(i)
        return tb, col

    # from(,,,)where
    def table_interpret(self, line):
        line = line.split(',')
        line = [i.strip() for i in line]
        return line

    def selection(self, line, database):
        attribute = re.findall(r'select(.*)from', line)[0].strip()
        attribute = self.attribute_interpret(attribute)

        table = []
        condition, oper, value = [], [], []
        constant = 0  #

        if 'where' in line:
            table = re.findall(r'from(.*)where', line)[0].strip()
            table = self.table_interpret(table)
            for i in table:
                if not CreateTable(database):
                    print('无'+i+'表')
                    return False

            # 匹配属性里的表名和属性
            if attribute[1] == '*' and len(table) == 1:
                attribute = ShowTable(database).match_table_col(attribute[0], attribute[1], table)
            else:
                print('语法错误')
                return False

            _ = re.findall(r'where(.*)', line)[0].strip()
            condition, oper, value = self.condition_interpret(_)  # 条件，操作符，常量/属性

            # 加了有.的处理
            # 处理value。如果是属性就放到列表里，如果是常量就还是string
            for i in range(len(value)):
                if value[i][0] == "'" and value[i][-1] == "'":  # 常量
                    continue
                else:
                    a = self.attribute_interpret(value[i])
                    print(a)
                    a = ShowTable(database).match_table_col(a[0], a[1], table)
                    value[i] = [a[0][0], a[1][0]]


            # 处理condition
            for i in range(len(condition)):
                a = self.attribute_interpret(condition[i])
                a = ShowTable(database).match_table_col(a[0], a[1], table)
                print('b'+str(a))
                condition[i] = [a[0][0], a[1][0]]

        else:
            table = re.findall(r'from(.*)', line)[0].strip()
            table = self.table_interpret(table)
            for i in table:
                if not CreateTable(database):
                    print('无'+i+'表')
                    return False
            # 匹配属性里的表名和属性
            if attribute[1][0] == '*' and len(table) == 1:
                attribute = ShowTable(database).match_table_col(attribute[0], attribute[1], table)
            else:
                print('语法错误')
                return False

        # s = ShowTable(database)
        print(table, attribute, condition, oper, value)




if __name__ == '__main__':
    s = Selected()
    # s.projection('id,pw', 'root', 'bbb')
    # s.where("pw='123' and id != 1000")
    line = "select id, sex from teacher, root"
    # table = s.table_interpret('teacher')
    # tb, col = s.attribute_interpret('3')
    # print(tb, col)
    # t = ShowTable('ccc')
    # tb, col = t.match_table_col(tb, col, table)
    # s.between_select_from('a.b, c')
    s.selection("select sex from teacher", 'ccc')
