import os
import pymssql
import sys
import glob

class FileReader:
    #初始化
    def __init__(self,host,user,pwd,db):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.db = db

        if not self.db:
            raise (NameError, "没有设置数据库信息")
        self.conn = pymssql.connect(host=self.host, user=self.user, password=self.pwd, database=self.db, charset="utf8")
        self.cur = self.conn.cursor()
        if not self.cur:
            raise (NameError, "连接数据库失败")

        try:
            self.cur.execute("""Create TABLE NewTbl(
                        id           int          IDENTITY   PRIMARY KEY  NOT NULL, 
                        ip         varchar(50)  ,
                        devicetype  varchar(50) ,
                        os          varchar(50)  ,
                        osv         varchar(50)   ,
                        did        varchar(50)  ,
                        dpid        varchar(50)   ,
                        mac         varchar(50)   ,
                        ua          varchar(max)   ,
                        make        varchar(50)  ,
                        model       varchar(50)  ,
                        h           varchar(50)   ,
                        w           varchar(50)   ,
                        ppi          varchar(50)  ,
                        carrier       varchar(50)  ,
                        connectiontype   varchar(50)  ,
                        screen_orientation   varchar(50)   ,
                        didmd5           varchar(50)   , 
                        dpidmd5          varchar(50)  ,
                        macmd5           varchar(50)   ,           
                        );""")
            self.conn.commit()
            print('新表创建完成')
        except Exception as err:
            print(err)

        self.conn.close()

    #链接数据库
    def __Connect(self):
        try:
            self.conn = pymssql.connect(host=self.host, user=self.user, password=self.pwd,
                                            database=self.db)
            cur = self.conn.cursor()
        except Exception as err:
            print(err)

        return cur


    # 执行插入语句
    # 插入不同的表格需修改execute部分内容
    def ExecInsert(self,SQL):
        try:
            cur = self.__Connect()
            cur.execute("""INSERT INTO NewTbl([ip],[devicetype],[os],[osv],[did],[dpid],
                            [mac],[ua],[make],[model],[h],[w],[ppi],[carrier],[connectiontype],[screen_orientation],
                            [didmd5],[dpidmd5],[macmd5])
                            VALUES
                            ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',
                             '%s','%s','%s','%s','%s','%s','%s','%s')"""
                            %(SQL['ip'], SQL['devicetype'],SQL['os'],SQL['osv'],SQL['did'],SQL['dpid'],
                            SQL['mac'],SQL['ua'],SQL['make'],SQL['model'],SQL['h'],SQL['w'],SQL['ppi'],
                            SQL['carrier'],SQL['connectiontype'],SQL['screen_orientation'],SQL['didmd5'],
                            SQL['dpidmd5'],SQL['macmd5']))
            self.conn.commit()
            self.conn.close()
            #print('========insert complete=====')
        except Exception as err:
            print(err)


    #剔除不需要的数据
    def LineExtract(self,strpath):
        with open(strpath, "r") as file:
            for line in file:
                if 'ip' in line:
                    self.ItemExtract(line)
                else:
                    continue

    def ItemExtract(self,line):
        Item = line.split('}')[1].split(',')[1:]
        #字典模板
        SQLITEM = {'ip':'',
                   'devicetype':'',
                   'os':'',
                   'osv':'',
                   'did':'',
                   'dpid':'',
                   'mac':'',
                   'ua':'',
                   'make':'',
                   'model': '',
                   'h': '',
                   'w':'',
                   'ppi':'',
                   'carrier':'',
                   'connectiontype':'',
                   'screen_orientation':'',
                   'didmad':'',
                   'dpidmd5':'',
                   'macmd5':''}
        for i in range(len(Item)):
            tp = Item[i].split(':',1)
            title = tp[0]
            value = tp[1]
            SQLITEM[eval(title)] = eval(value)


            #self.ExecInsert(sqltitle = eval(title),sqlvalue = eval(value))
      #  print(SQLITEM)
        self.ExecInsert(SQL=SQLITEM)



# 测试&主程序入口
if __name__ == '__main__':
    FR = FileReader(host='localhost:1433',user='PythonAccount',pwd='1024',db='NetData')
    txt_filenames = glob.glob('./*txt')
    filenum = len(txt_filenames)
    index = 0

    for filename in txt_filenames:
        print('======共计文件：%s个==============导入开始，当前第%s个' % (filenum,index+1))
        FR.LineExtract(filename)
        index = index +1
        print('===============当前文件：%s导入完成，当前第%s个' %(filename,index))

    print('======================================导入完成')

