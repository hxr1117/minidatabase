class Selected(object):
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


if __name__ == '__main__':
    s = Selected()
    s.projection('id,pw', 'root', 'bbb')
