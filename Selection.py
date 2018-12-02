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
        attribute = self.attribute_interpret(attribute)  # 有投影要的一部分属性

        table = []  # 用到的表
        condition, oper, value = [], [], []  # 条件属性，操作符，值/属性
        projection_list = {}  # 投影属性字典，一张表一个key
        selection_list = []  # 选择列表
        link_list = []  # 连接的属性列表
        cartesian_list = []  # 要笛卡尔积的表

        if 'where' in line:
            table = re.findall(r'from(.*)where', line)[0].strip()
            table = self.table_interpret(table)
            for i in table:
                if not CreateTable(database):
                    print('无'+i+'表')
                    return False
                projection_list[i] = []

            # 匹配属性里的表名和属性
            attribute = list(ShowTable(database).match_table_col(attribute[0], attribute[1], table))
            for i in range(len(attribute[0])):
                projection_list[attribute[0][i]].append(attribute[1][i])

            _ = re.findall(r'where(.*)', line)[0].strip()
            condition, oper, value = self.condition_interpret(_)  # 条件，操作符，常量/属性

            # 加了有.的处理
            # 处理value。如果是属性就放到列表里，如果是常量就还是string
            for i in range(len(value)):
                # 处理condition
                a = self.attribute_interpret(condition[i])
                a = ShowTable(database).match_table_col(a[0], a[1], table)
                condition[i] = [a[0][0], a[1][0]]
                projection_list[condition[i][0]].append(condition[i][1])

                if value[i][0] == "'" and value[i][-1] == "'":  # 常量
                    selection_list.append([condition[i], oper[i], value[i]])  # [[tb, col], oper, value]
                else:  # 是属性的话肯定是连接了
                    a = self.attribute_interpret(value[i])
                    a = ShowTable(database).match_table_col(a[0], a[1], table)
                    value[i] = [a[0][0], a[1][0]]
                    projection_list[value[i][0]].append(value[i][1])
                    link_list.append([value[i], condition[i]])

            # 投影属性去重
            for i in projection_list.keys():
                projection_list[i] = list(set(projection_list[i]))

            # 先把每个表需要的列投影出来
            s = ShowTable(database)
            tables_rows = {}
            for i in projection_list.keys():
                if projection_list[i]:
                    tables_rows[i] = s.projection(projection_list[i], i)
            print('投影', tables_rows)

            # 选择
            # 选择条件来自不同的表也要笛卡尔积_(´ཀ`」 ∠)_
            # 这里笛卡尔积了的话，连接里的笛卡尔积感觉会出错
            print(selection_list)
            for i in selection_list:
                tables_rows[i[0][0]] = s.get_rows_by_condition(i[0][0], i[0][1], i[1], i[2], tables_rows[i[0][0]])

            print('选择', tables_rows)

            # 如果任何一个选择条件的结果为空，返回值都应该是空
            for i in tables_rows:
                if not tables_rows[i]:
                    return []
            # 连接
            # link(self, tb1, tb2, col1, col2, row1=[], row2=[]):
            new_rows = []
            link_table = []
            selet_table = []
            used_table = {}
            if link_list:
                for i in range(len(link_list)):
                    _ = link_list[i]
                    if i == 0:  # 两张表普通连接
                        link_table += [x[0] for x in _]  # 连接用到的表
                        _ = list(s.link(_[0][0], _[1][0], _[0][1], _[1][1], tables_rows[_[0][0]], tables_rows[_[1][0]]))
                        new_rows = _
                        print(new_rows)
                    else:  # 连接过的表的下一次连接
                        new_table = {x[0]: x[1] for x in _}  # 新的重新用来连接的表
                        for j in range(i):
                            for k in range(2):
                                used_table[link_list[j][k][0]] = link_list[j][k][1]
                        new_rows = s.multi_table_link(used_table, new_table, new_rows, tables_rows)

                # 如果有连接的表和选择的表不一样要笛卡尔积
                selet_table += [x[0][0] for x in selection_list]  # 选择用到的表
                link_table = set(link_table)
                selet_table = set(selet_table)
                # 如果连接用到的表没有完全包含选择的表，要和差集笛卡尔积
                if selet_table:
                    cartesian_list += list(selet_table-link_table)
                # new_rows = new_rows[0]
                for i in cartesian_list:
                    new_rows = s.cartesian_product(row1=new_rows, row2=tables_rows[i])

            # 最后结果的投影


        else:
            table = re.findall(r'from(.*)', line)[0].strip()
            table = self.table_interpret(table)
            for i in table:
                if not CreateTable(database):
                    print('无' + i + '表')
                    return False

            # 匹配属性里的表名和属性
            attribute = list(ShowTable(database).match_table_col(attribute[0], attribute[1], table))


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
    s.selection("select course.name, teacher.name from course, teacher, selection "
                "where tid<='222' and cid<'333'", 'ccc')
