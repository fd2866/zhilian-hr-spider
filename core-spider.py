# -*- coding:utf-8 -*-
"""
@author:zy
@file:core-spider.py
@time:2018/12/813:31
"""

import datetime
import json
import re
import time
import pymysql
import requests
from bs4 import BeautifulSoup
import get_database_htm_code

global all_detail_htm


def get_one_page(page,keyword,cityid,cursor,db,date,table_name,):
	"""爬去参数传入的网页数据"""
	global all_detail_htm
	pattern = re.compile("""['"]""") #匹配单引号和双引号
	url = 'https://fe-api.zhaopin.com/c/i/sou?start={0}&pageSize=60&cityId={2}&workExperience=-1&education=-1\&companyType=-1&employmentType=-1&jobWelfareTag=-1&kw={1}&kt=3'
	header = {
		'User-Agent':'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36',
		'Connection':'close'
	}
	res = get_res(url.format(page,keyword,cityid),header)
	print("----当前搜索关键词----"+keyword)
	print(res)
	if res:
		job_dict = {}
		for i in res['data']['results']:
			detailed_url = "https://jobs.zhaopin.com/" + i['number'] + '.htm'  #构造岗位详情页面url
			if '人力资源' in i['jobType']['display']:
				if i['number'] not in all_detail_htm:
					response = get_detailres(detailed_url)
					all_detail_htm.append(i['number'])
					if response:
						job_detail = detail_page(response,pattern)
						print(detailed_url)
						job_dict['city'] = i['city']['items'][0]['name']  #城市
						job_dict['company_name'] = i['company']['name'] #公司名称
						job_dict['company_size'] = i['company']['size']['name'] #公司规模
						job_dict['companyType'] = i['company']['type']['name']  #公司类型
						job_dict['eduLevel'] = i['eduLevel']['name'] #教育水平
						job_dict['emplType'] = i['emplType'] #工作类别 “实习、全职、兼职”
						job_dict['jobname'] = i['jobName'] #职位名称
						job_class = job_classify(job_dict['jobname'])
						job_dict['job_function'] = job_class['function'] #岗位职能
						job_dict['job_rank'] = job_class['rank']#岗位职级
						job_dict['jobType'] = i['jobType']['display'] #岗位类别
						job_dict['salary'] = i['salary'] #工资
						job_dict['welfare'] = '.'.join(i['welfare']) #福利
						job_dict['updateDate'] = i['updateDate'] #上传日期
						job_dict['workingExp'] = i['workingExp']['name'] #工作经验
						job_dict['htm_code_fk'] = i['number'] #详细页面url关键字
						if i['geo']['lat'] in ['-1','0','','null','NULL','1','0.0']:
							job_dict['lat'] = ''
							job_dict['lon'] = ''
						else:
							job_dict['lat'] = i['geo']['lat'] #纬度
							job_dict['lon'] = i['geo']['lon'] #经度
						job_dict['job_describe'] = job_detail[0] #岗位描述
						#job_dict['job_demand'] = job_detail[1] #岗位要求
						job_dict['salary_down'] = job_detail[2] #起薪 0为面议
						job_dict['salary_up'] = job_detail[3] #最高薪   0为面议
						job_dict['industry'] = job_detail[4] #行业
						key_result = key_word_classify(job_dict['job_describe']) #岗位职责 岗位要求
						job_dict['data'] = key_result['数据'] #数据
						job_dict['innovation'] = key_result['创新'] #创新
						job_dict['communication'] = key_result['沟通']#沟通
						job_dict['coordination'] = key_result['协调']#协调
						job_dict['labor_law'] = key_result['劳动法']
						job_dict['logic'] = key_result['逻辑']
						job_dict['responsibility'] = key_result['责任']
						job_dict['team'] = key_result['团队']
						job_dict['resist_compression'] = key_result['抗压']
						job_dict['learning'] = key_result['学习']
						job_dict['analysis'] = key_result['分析']
						job_dict['optimize'] = key_result['优化']
						#print(job_dict)
						operate_and_save(job_dict,cursor,db,date,table_name)
					else:
						print("----该网页404----" + detailed_url)
						continue
				else:
					print("----发现重复数据----"+i['number'])
					continue
			else:
				print("----发现异常工作类别----" + i['jobType']['display'])
				continue
	else:
		print("----主页搜索没有没有响应----")
	return res['data']['numFound']


def get_res(url,header):
	"""请求主页，若发生超时进行重复访问"""
	try:
		response = requests.get(url,headers = header).json()
		if response['code'] == 200:
			return response
		elif response['code'] == 500:
			print("----请求超时了-----再次尝试一下")
			time.sleep(10)
			response = requests.get(url,headers = header).json()
			if response['code'] == 200:
				return response
			elif response['code'] == 500:
				return None
	except Exception as e:
		print(e)
		return None


def get_detailres(url):
	"""请求岗位详细页面数据"""
	try:
		response = requests.get(url)
		if response.status_code == 200:
			return response.text.encode('utf-8')
		elif response.status_code == 500:
			print("----详情页面出现未响应----")
			for i in range(1,10):
				print("重复请求第{0}次".format(i))
				time.sleep(5)
				response = requests.get(url)
				if response.status_code == 200:
					return response.text.encode('utf-8')
		elif response.status_code == 404:
			print("----详情页面出现404----")
			return None
	except Exception as e:
		print(e)
		return None


def detail_page(response,pattern):
	""""获取岗位要求和岗位工作内容"""
	soup = BeautifulSoup(response,'lxml') #构造soup
	target_tag = soup.div(class_ = "pos-ul")#截取div class："pos-ul"标签
	if target_tag:
		tag_str = target_tag[0].get_text()
		tag_str = pattern.sub('-',tag_str)
		job_describe = tag_str
		#print(job_describe)
		job_demand = ''
		#获取工资 最高-最低
		salary_str = soup.strong.text
		if "面议" in salary_str:
			salary_down = 0
			salary_up = 0
		elif "以上" in salary_str:
			salary_down = re.sub("\D","",salary_str)
			salary_up = 0
		elif "以下" in salary_str:
			salary_down = 0
			salary_up = re.sub("\D","",salary_str)
		else:
			salary_num = salary_str.find('-')
			salary_down = int(re.sub("\D","",salary_str[:salary_num]))
			salary_up = int(re.sub("\D","",salary_str[salary_num:]))
		job_detail = [job_describe,job_demand,salary_down,salary_up]
	else:
		job_detail = ['','',0,0]

	try:
		industry = soup.select('ul.promulgator-ul  a')[0].text
		job_detail.append(industry)
	except Exception as e:
		job_detail.append('未知')
		print(e)
	return job_detail


def job_classify(job_name):
	"""对工作名称进行职能职级分类"""
	job_class = {'function':'','rank':''}
	functions = ['HRBP','人力','人事','招聘','培训','绩效','关系','组织发展','薪酬','猎头','人资']
	ranks = ['实习生','专员','总监','主管','经理']
	for fn in functions:
		if fn in job_name.upper():
			job_class['function'] = fn
			break
		elif '组织' in job_name or '发展' in job_name:
			job_class['function'] = '组织发展'
			break
		else:
			job_class['function'] = '未识别'

	for rk in ranks:
		if '助理'in job_name:
			job_class['rank'] = '专员'
		elif rk in job_name:
			job_class['rank'] = rk
		elif '管培'in job_name or '员'in job_name:
			job_class['rank'] = '专员'
		elif '主任'in job_name:
			job_class['rank'] = '经理'
		elif '负责人' in job_name:
			job_class['rank'] = '总监'
	return job_class


def key_word_classify(job_describe):
	"""对岗位描述与岗位职责进行关键词提取"""
	key_word = ['数据','创新','沟通','协调','劳动法','逻辑','责任','团队','抗压','学习','分析','优化']
	result = {'数据':3,'创新':3,'沟通':3,'协调':3,'劳动法':3,'逻辑':3,'责任':3,'团队':3,'抗压':3,'学习':3,'分析':3,'优化':3}
	for k in range(len(key_word)):
		if key_word[k] in job_describe:
			result[key_word[k]] = 1
		else:
			result[key_word[k]] = 0
	return result

	"""
	result = {'data':3,'innovation':3,'communication':3,'coordination':3,'labor_law':3,'logic':3,'responsibility':3,
	          'team':3,'resist_compression':3,'learning':3,'analysis':3,'optimize':3}
	if key_word[0] in job_describe:
		result['data'] = 1
	else:
		result['data'] = 0
"""


def city_position(city_name):
	with open('city-lat&lon.txt','r',encoding = 'utf-8') as f:
		f = f.read()
		jf = json.loads(f)

	try:
		num = jf[city_name].find(",")
		position = {"lon":jf[city_name][:num],"lat":jf[city_name][num+1:]}
	except Exception as e:
		position = {"lon":0,"lat":0}
		print(e)

	return position


def pre_database(date):
	""" 连接数据库，创建表"""
	table_name = "tb_zhilian_job_"+str(date.strftime("%Y%m%d"))
	try:
		db = pymysql.connect(
			host = 'localhost',
			port = 3306,
			user = 'root',
			password = 'root',
		)
	except Exception as e:
		print(e)
	try:
		cursor = db.cursor()
		sql = """CREATE DATABASE IF NOT EXISTS hr_analysis DEFAULT CHARACTER SET utf8 """
		cursor.execute(sql)
		sql = """USE hr_analysis"""
		cursor.execute(sql)
		sql = """CREATE TABLE IF NOT EXISTS {0}(id INT(20) NOT NULL AUTO_INCREMENT PRIMARY KEY UNIQUE,
			city VARCHAR(254),
			company_name VARCHAR(254),
			company_size VARCHAR(254),
			company_type VARCHAR(254),
			company_industry VARCHAR(254),
			edu_level VARCHAR(254),
			empl_type VARCHAR(254),
			job_name VARCHAR(254),
			job_function VARCHAR (254),
			job_rank VARCHAR (254),
			job_type VARCHAR(254),
			salary VARCHAR(254),
			salary_down INT (20),
			salary_up INT (20),
			welfare VARCHAR(254),
			update_date VARCHAR(254),
			working_exp VARCHAR(254),
			latitude DECIMAL(10,6) ,
			longitude DECIMAL(10,6) ,
			job_describe VARCHAR(4000) ,
			got_day DATE ,
			htm_code_fk VARCHAR(254) NOT NULL UNIQUE ,
			kw_data INT(10),kw_innovation INT(10),kw_communication INT(10),kw_coordination INT(10),
 			kw_labor_law INT(10),kw_logic INT(10),kw_responsibility INT(10),kw_team INT(10),
 			kw_resist_compression INT(10),kw_learning INT(10),kw_analysis INT(10),kw_optimize INT(10)
			);""".format(table_name)
		cursor.execute(sql)
		db.commit()
	except Exception as e:
		print(e)

	global all_detail_htm
	all_detail_htm = get_database_htm_code.connect_database() #查询数据库中的htm_code 用于去重
	return cursor,db,table_name


def operate_and_save(jobdata,cursor,db,getdate,table_name):
	"""操作存储数据库"""
	sql = ''
	if jobdata['lat'] and jobdata['salary_down'] and jobdata['salary_up']:
		sql = """INSERT INTO {0} (city,company_name,company_size,company_type,company_industry,edu_level,empl_type,job_name,job_type,salary,salary_down, salary_up,welfare,update_date,working_exp,
	 latitude,longitude,job_describe,got_day,job_function,job_rank,htm_code_fk,kw_data,kw_innovation,kw_communication,kw_coordination,kw_labor_law,kw_logic,kw_responsibility,
	 kw_team,kw_resist_compression,kw_learning,kw_analysis,kw_optimize)VALUES ("{1}","{2}","{3}","{4}","{5}","{6}","{7}","{8}","{9}","{10}","{11}","{12}","{13}","{14}","{15}",
	 "{16}","{17}","{18}","{19}","{20}","{21}","{22}","{23}","{24}","{25}","{26}","{27}","{28}","{29}","{30}","{31}","{32}","{33}","{34}")""".format(table_name,jobdata['city'],
		jobdata['company_name'],jobdata['company_size'],jobdata['companyType'],jobdata['industry'],jobdata['eduLevel'],jobdata['emplType'],jobdata['jobname'],jobdata['jobType'],jobdata['salary'],
		jobdata['salary_down'],jobdata['salary_up'],jobdata['welfare'],jobdata['updateDate'],jobdata['workingExp'], jobdata['lat'],jobdata['lon'],jobdata['job_describe'],getdate,
		jobdata['job_function'],jobdata['job_rank'],jobdata['htm_code_fk'],jobdata['data'],	jobdata['innovation'],jobdata['communication'],jobdata['coordination'],
		jobdata['labor_law'],jobdata['logic'],jobdata['responsibility'],jobdata['team'],jobdata['resist_compression'],jobdata['learning'],jobdata['analysis'],jobdata['optimize'])

	elif jobdata['lat'] == '' and jobdata['salary_down'] and jobdata['salary_up']:
		position = city_position(jobdata['city'])
		sql = """ INSERT INTO {0} (city,company_name,company_size,company_type,company_industry,edu_level,empl_type,job_name,job_type,salary,salary_down, salary_up,welfare,update_date,working_exp,
 	 latitude,longitude,job_describe,got_day,job_function,job_rank,htm_code_fk,kw_data,kw_innovation,kw_communication,kw_coordination,kw_labor_law,kw_logic,kw_responsibility,
	 kw_team,kw_resist_compression,kw_learning,kw_analysis,kw_optimize)VALUES ("{1}","{2}","{3}","{4}","{5}","{6}","{7}","{8}",	"{9}", "{10}","{11}","{12}","{13}","{14}",
	 "{15}","{16}","{17}","{18}","{19}","{20}","{21}","{22}","{23}","{24}","{25}","{26}","{27}","{28}","{29}","{30}","{31}","{32}","{33}","{34}")""".format(table_name,jobdata['city'],
	 jobdata['company_name'],jobdata['company_size'],jobdata['companyType'],jobdata['industry'],jobdata['eduLevel'],jobdata['emplType'],jobdata['jobname'],jobdata['jobType'],jobdata['salary'],
	 jobdata['salary_down'],jobdata['salary_up'],jobdata['welfare'],jobdata['updateDate'],jobdata['workingExp'],position['lat'],position['lon'],jobdata['job_describe'],
	 getdate,jobdata['job_function'],	 jobdata['job_rank'],jobdata['htm_code_fk'],jobdata['data'],jobdata['innovation'],jobdata['communication'],jobdata['coordination'],
	 jobdata['labor_law'],jobdata['logic'],jobdata['responsibility'],jobdata['team'],jobdata['resist_compression'],jobdata['learning'],jobdata['analysis'],jobdata['optimize'])

	elif jobdata['lat'] == '' and jobdata['salary_down'] == 0 and jobdata['salary_up'] == 0:
		position = city_position(jobdata['city'])
		sql = """ INSERT INTO {0} (city,company_name,company_size,company_type,company_industry,edu_level,empl_type,job_name,job_type,salary,salary_down,salary_up,welfare,update_date,working_exp,
	 latitude,longitude,job_describe,got_day,job_function,job_rank,htm_code_fk,kw_data,kw_innovation,kw_communication,kw_coordination,kw_labor_law,kw_logic,kw_responsibility,
	 kw_team,kw_resist_compression,kw_learning,kw_analysis,kw_optimize)VALUES ("{1}","{2}","{3}","{4}","{5}","{6}","{7}","{8}","{9}", NULL ,NULL ,"{10}","{11}","{12}","{13}",
	 "{14}","{15}","{16}","{17}","{18}","{19}","{20}","{21}","{22}","{23}","{24}","{25}","{26}","{27}","{28}","{29}","{30}","{31}","{32}")""".format(table_name,jobdata['city'],
	 jobdata['company_name'],jobdata['company_size'],jobdata['companyType'],jobdata['industry'],jobdata['eduLevel'],jobdata['emplType'],jobdata['jobname'],jobdata['jobType'],jobdata['salary'],
	 jobdata['welfare'],jobdata['updateDate'],jobdata['workingExp'],position['lat'],position['lon'],jobdata['job_describe'],getdate,jobdata['job_function'],jobdata['job_rank'],
	 jobdata['htm_code_fk'],jobdata['data'],jobdata['innovation'],jobdata['communication'],jobdata['coordination'],jobdata['labor_law'],jobdata['logic'],jobdata['responsibility'],
	 jobdata['team'],jobdata['resist_compression'],	jobdata['learning'],jobdata['analysis'],jobdata['optimize'])

	elif jobdata['lat'] == '' and jobdata['salary_down'] == 0 and jobdata['salary_up']:
		position = city_position(jobdata['city'])
		sql = """ INSERT INTO {0} (city,company_name,company_size,company_type,company_industry,edu_level,empl_type,job_name,job_type,salary,salary_down,salary_up,welfare,update_date,working_exp,
 	 latitude,longitude,job_describe,got_day,job_function,job_rank,htm_code_fk,kw_data,kw_innovation,kw_communication,kw_coordination,kw_labor_law,kw_logic,kw_responsibility,
	 kw_team,kw_resist_compression,kw_learning,kw_analysis,kw_optimize)VALUES ("{1}","{2}","{3}","{4}","{5}","{6}","{7}","{8}","{9}",NULL ,"{10}","{11}","{12}","{13}","{14}",
	 "{15}","{16}","{17}","{18}","{19}","{20}","{21}","{22}","{23}","{24}","{25}","{26}","{27}","{28}","{29}","{30}","{31}","{32}","{33}")""".format(table_name,jobdata['city'],
	 jobdata['company_name'],jobdata['company_size'],jobdata['companyType'],jobdata['industry'],jobdata['eduLevel'], jobdata['emplType'],jobdata['jobname'],jobdata['jobType'],jobdata['salary'],
	 jobdata['salary_up'],jobdata['welfare'],jobdata['updateDate'],jobdata['workingExp'],position['lat'],position['lon'],jobdata['job_describe'],getdate,jobdata['job_function'],
	 jobdata['job_rank'],jobdata['htm_code_fk'],jobdata['data'],jobdata['innovation'],jobdata['communication'],jobdata['coordination'],jobdata['labor_law'],jobdata['logic'],
	 jobdata['responsibility'],jobdata['team'],jobdata['resist_compression'],jobdata['learning'],jobdata['analysis'],jobdata['optimize'])

	elif jobdata['lat'] == '' and jobdata['salary_down'] and jobdata['salary_up'] == 0:
		position = city_position(jobdata['city'])
		sql = """ INSERT INTO {0} (city,company_name,company_size,company_type,company_industry,eduLevel,empl_type,job_name,job_type,salary,salary_down,salary_up,welfare,update_date,working_exp,
	 latitude,longitude,job_describe,got_day,job_function,job_rank,htm_code_fk,kw_data,kw_innovation,kw_communication,kw_coordination,kw_labor_law,kw_logic,kw_responsibility,
	 kw_team,kw_resist_compression,kw_learning,kw_analysis,kw_optimize)VALUES ("{1}","{2}","{3}","{4}","{5}","{6}","{7}","{8}","{9}","{10}", NULL ,"{11}","{12}",,"{13}",
	 "{14}","{15}","{16}","{17}","{18}","{19}","{20}","{21}","{22}","{23}","{24}","{25}","{26}","{27}","{28}","{29}","{30}","{31}","{32}","{33}")""".format(table_name, jobdata['city'],
	jobdata['company_name'],jobdata['company_size'],jobdata['companyType'],jobdata['industry'],jobdata['eduLevel'],jobdata['emplType'],jobdata['jobname'],jobdata['jobType'],jobdata['salary'],
	jobdata['salary_down'],jobdata['welfare'],jobdata['updateDate'],jobdata['workingExp'],position['lat'],position['lon'],jobdata['job_describe'],getdate, jobdata['job_function'],
	jobdata['job_rank'],jobdata['htm_code_fk'],jobdata['data'],jobdata['innovation'],jobdata['communication'],jobdata['coordination'],jobdata['labor_law'],jobdata['logic'],
	jobdata['responsibility'],jobdata['team'],jobdata['resist_compression'],	jobdata['learning'],jobdata['analysis'],jobdata['optimize'])

	elif jobdata['lat'] and jobdata['salary_down'] == 0 and jobdata['salary_up'] == 0:
		sql = """ INSERT INTO {0} (city,company_name,company_size,company_type,company_industry,eduLevel,empl_type,job_name,job_type,salary,salary_down,salary_up,welfare,update_date,working_exp,
	 latitude,longitude,job_describe,got_day,job_function,job_rank,htm_code_fk,kw_data,kw_innovation,kw_communication,kw_coordination,kw_labor_law,kw_logic,kw_responsibility,
	 kw_team,kw_resist_compression,kw_learning,kw_analysis,kw_optimize)VALUES ("{1}","{2}","{3}","{4}","{5}","{6}","{7}","{8}","{9}", NULL ,NULL ,"{10}","{11}","{12}","{13}",
	 "{14}","{15}","{16}","{17}","{19}","{20}","{21}","{22}","{23}","{24}","{25}","{26}","{27}","{28}","{29}","{30}","{31}","{32}")""".format(table_name,jobdata['city'],
	 jobdata['company_name'],jobdata['company_size'],jobdata['companyType'],jobdata['industry'], jobdata['eduLevel'],jobdata['emplType'],jobdata['jobname'],jobdata['jobType'],jobdata['salary'],
	 jobdata['welfare'],jobdata['updateDate'],jobdata['workingExp'],jobdata['lat'],jobdata['lon'],jobdata['job_describe'],getdate,jobdata['job_function'],jobdata['job_rank'],
	 jobdata['htm_code_fk'],jobdata['data'],jobdata['innovation'],jobdata['communication'],jobdata['coordination'],jobdata['labor_law'],jobdata['logic'],jobdata['responsibility'],
	 jobdata['team'],jobdata['resist_compression'],	jobdata['learning'],jobdata['analysis'],jobdata['optimize'])

	elif jobdata['lat'] and jobdata['salary_down'] == 0 and jobdata['salary_up']:
		sql = """ INSERT INTO {0} (city,company_name,company_size,company_type,company_industry,eduLevel,empl_type,job_name,job_type,salary,salary_down,salary_up,welfare,update_date,working_exp,
	 latitude,longitude,job_describe,got_day,job_function,job_rank,htm_code_fk,kw_data,kw_innovation,kw_communication,kw_coordination,kw_labor_law,kw_logic,kw_responsibility,
	 kw_team,kw_resist_compression,kw_learning,kw_analysis,kw_optimize)VALUES ("{1}","{2}","{3}","{4}","{5}","{6}","{7}","{8}","{9}" ,NULL ,"{10}","{11}","{12}","{13}",
	 "{14}","{15}","{16}","{17}","{19}","{20}","{21}","{22}","{23}","{24}","{25}","{26}","{27}","{28}","{29}","{30}","{31}","{32}")""".format(table_name,jobdata['city'],
	 jobdata['company_name'],jobdata['company_size'],jobdata['companyType'],jobdata['industry'], jobdata['eduLevel'],jobdata['emplType'],jobdata['jobname'],jobdata['jobType'],jobdata['salary'],
	 jobdata['salary_up'],jobdata['welfare'],jobdata['updateDate'],jobdata['workingExp'],jobdata['lat'],jobdata['lon'],jobdata['job_describe'],getdate,jobdata['job_function'],
	 jobdata['job_rank'],jobdata['htm_code_fk'],jobdata['data'],jobdata['innovation'],jobdata['communication'],jobdata['coordination'],jobdata['labor_law'],jobdata['logic'],
	 jobdata['responsibility'],jobdata['team'],jobdata['resist_compression'],jobdata['learning'],jobdata['analysis'],jobdata['optimize'])

	elif jobdata['lat'] and jobdata['salary_down'] and jobdata['salary_up'] == 0:
		sql = """ INSERT INTO {0} (city,company_name,company_size,company_type,company_industry,eduLevel,empl_type,job_name,job_type,salary,salary_down,salary_up,welfare,update_date,working_exp,
	 latitude,longitude,job_describe,got_day,job_function,job_rank,htm_code_fk,kw_data,kw_innovation,kw_communication,kw_coordination,kw_labor_law,kw_logic,kw_responsibility,
	 kw_team,kw_resist_compression,kw_learning,kw_analysis,kw_optimize)VALUES ("{1}","{2}","{3}","{4}","{5}","{6}","{7}","{8}","{9}","{10}" ,NULL ,"{11}","{12}","{13}",
	 "{14}","{15}","{16}","{17}","{19}","{20}","{21}","{22}","{23}","{24}","{25}","{26}","{27}","{28}","{29}","{30}","{31}","{32}")""".format(table_name,jobdata['city'],
	 jobdata['company_name'],jobdata['company_size'],jobdata['companyType'],jobdata['industry'], jobdata['eduLevel'],jobdata['emplType'],jobdata['jobname'],jobdata['jobType'],jobdata['salary'],
	 jobdata['salary_up'],jobdata['welfare'],jobdata['updateDate'],jobdata['workingExp'],jobdata['lat'],jobdata['lon'],jobdata['job_describe'],getdate,jobdata['job_function'],
	 jobdata['job_rank'],jobdata['htm_code_fk'],jobdata['data'],jobdata['innovation'],jobdata['communication'],jobdata['coordination'],jobdata['labor_law'],jobdata['logic'],
	 jobdata['responsibility'],jobdata['team'],jobdata['resist_compression'],jobdata['learning'],jobdata['analysis'],jobdata['optimize'])

	#print(sql)
	try:
		cursor.execute(sql)
		db.commit()
	except Exception as e:
		print(e)
		db.rollback()


def main(cityid,keyword,start):
	date = datetime.date.today()
	cursor,db,table_name = pre_database(date)
	while True:
		num_found = get_one_page(start,keyword,cityid,cursor,db,date,table_name)
		print('第{0}条-----共{1}条'.format(start,num_found))
		if start < num_found:
			start += 60
			time.sleep(1)
		else:
			db.close()
			break


if __name__ == '__main__':
	jb_name = ['招聘专员','招聘助理','招聘主管','招聘经理','培训专员','培训助理','培训主管','培训经理','薪资福利','绩效考核','员工关系','劳动关系','组织发展','人员发展','HRBP','人力资源助理',
	 '人力资源专员','人力资源主管','人力资源经理','人力资源总监','人力资源负责人','HRD','人事专员','人事助理','人事主管','人事经理','社保专员','社保服务专员','社保代理专员','人资专员',
	 '人事行政专员','人事行政文员','行政人事专员','薪资福利专员','薪资福利助理','薪资福利主管','薪资福利经理','薪酬专员','薪酬福利专员','薪酬福利主管','薪酬福利经理','薪酬绩效专员','薪酬绩效主管',
	 '薪酬绩效经理','薪酬管理助理','猎头助理','猎头顾问','猎头顾问助理','猎头专员','猎头经理','猎头助理顾问','猎头合伙人']
	for n in jb_name:
		main(489,n,0) #489为全国  0为第一页，60递增
