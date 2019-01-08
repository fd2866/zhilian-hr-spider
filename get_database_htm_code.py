# -*- coding:utf-8 -*-
"""
@author:Levy
@file:get_database_htm_code.py
@time:2019/1/410:13
"""
import pymysql
import datetime

def connect_database():
	""" 连接数据库，创建表"""
	date = datetime.date.today()
	table_name = "tb_job_"+ str(date.strftime("%Y%m%d"))
	all_htm_code = []
	try:
		db = pymysql.connect(
			host = 'localhost',
			port = 3306,
			user = 'root',
			password = 'root',
			db = 'hr_analysis',
		)
	except Exception as e:
		print(e)

	cursor = db.cursor()
	sql = "SELECT htm_code_fk FROM {0}".format(table_name)
	try:
		cursor.execute(sql)
	except Exception as e:
		all_htm_code = []
		print(e)
	all_code = cursor.fetchall()
	for code in all_code:
		all_htm_code.append(code[0])

	db.close()
	return all_htm_code


if __name__ == '__main__':
	connect_database()