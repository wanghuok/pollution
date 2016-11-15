#!/usr/bin/python2.7
# -*- coding:utf-8 -*- 
import scrapy
import time
import os,sys,re,sqlite3,urllib2
import json
from pyquery import PyQuery as pq
from scrapy.http.cookies import CookieJar
import urllib

default_encoding = 'utf-8'
if sys.getdefaultencoding() != default_encoding:
    reload(sys)
    sys.setdefaultencoding(default_encoding)

provinces={2:'安徽', 3:'内蒙古', 4:'重庆', 5:'福建', 6:'甘肃', 7:'广东', 8:'广西', 9:'贵州', 10:'海南', 11:'河北', \
		12:'河南', 13:'黑龙江', 14:'湖北', 15:'湖南', 16:'吉林', 17:'江苏', 18:'辽宁', 19:'江西', 20:'青海',21:'山东', \
		22:'山西', 23:'陕西', 24:'上海', 25:'四川', 27:'浙江', 28:'西藏', 30:'新疆', 31:'云南', \
		32:'天津', 33:'北京', 35:'宁夏'}

class MyPollSpider(scrapy.Spider):
	name = 'MyPoll'
	allowed_domains=['ipe.org.cn']
	
	def __init__(self):
               # self.cities = parse_city()
                self.hour = time.strftime("%Y%m%d%H%M%S",time.localtime(time.time()))
                self.directory='/home/rock/wh/polldata/'+self.hour+'/'
		self.poll_url='http://www.ipe.org.cn/data_ashx/GetAirData.ashx'
		self.start_url='http://www.ipe.org.cn/MapPollution/Pollution.aspx'
		self.cookie=''
		self.dbfile='/root/company_geo.db'
		self.con=None
		self.cur=None

	def init_db(self):
		self.con=sqlite3.connect(self.dbfile)	
		self.cur=self.con.cursor()
		#self.cur.execute("create table companygeo(name varchar(256) primary key, lat varchar(20), lng varchar(20))")

	def insert_db(self, name, city):
		url="http://restapi.amap.com/v3/geocode/geo?address={0}&city={1}&key=37649c16e05d961eea5a946823519e9d".format(name, city)
		res=urllib2.urlopen(urllib2.Request(url))
		data=json.loads(res.read())
		lat=''
		lng=''
		if()
		location=data['geocodes'][0]['location'].split(',')
		self.logger.info('经度:%s+++++++++++维度:%s', location[0], location[1])
				

	def get_frmdata(self, pageindex, province):
		return {'headers[Cookie]':self.cookie,
                        'cmd':'getpollution_fenye',
                        'pageindex':str(pageindex),
                        'pagesize':'8',
                        'province':str(province),
                        'city':'0',
                        'key':'',
                        'pollution':'0',
                        'enterprisids':'0/1/2/3',
                        'standardids':'0/1/2/3',
                        'feedbacks':'0/1/2',
                        'type':'1'}


	def start_requests(self):
		self.init_db()
		#req_url='http://www.ipe.org.cn:88/data_ashx/GetAirData.ashx'
		#for k,v in provinces.items():
		#	frmdata=self.get_frmdata(1, k)
		#	yield scrapy.FormRequest(url=self.req_url, formdata=frmdata, callback=self.parse_page, meta={'k':k, 'v':v})	
		yield scrapy.Request(url=self.start_url, callback=self.parse_province)

	def parse_province(self, response):
		cookieJar = CookieJar()
    		cookieJar.extract_cookies(response, response.request)
		self.logger.info('++++++++++++++++++++++++++%s****%d', cookieJar._cookies, len(cookieJar._cookies))	
		name=cookieJar._cookies['www.ipe.org.cn']['/']['ajaxkey'].name
		value=cookieJar._cookies['www.ipe.org.cn']['/']['ajaxkey'].value
		self.cookie='{0}={1}'.format(name, value)
		self.logger.info('self.cookie=%s', self.cookie)
		for k,v in provinces.items():
                       frmdata=self.get_frmdata(1, k)
                       yield scrapy.FormRequest(url=self.poll_url, formdata=frmdata, callback=self.parse_page, meta={'k':k, 'v':v})

	def parse_page(self,response):
		#respJson=re.sub(r"(,?)(\w+?)\s*?:", r"\1'\2':", response.body);
                #respJson=respJson.replace("'", "\"");
		res=json.loads(response.body)
		# isSuccess, Content, Page, TJ
		self.logger.info('isSuccess************%d',res['isSuccess'])
		fenye_content=pq(urllib.unquote(res['Content'].replace('%u', '\\u').decode('unicode-escape')))
		fenye_page=pq(urllib.unquote(res['Page'].replace('%u', '\\u').decode('unicode-escape')))
		fenye_tj=pq(urllib.unquote(res['TJ'].replace('%u', '\\u').decode('unicode-escape')))
		nPages=int(fenye_page('.pager-info').html().split('/')[1])
		nTotal=int(fenye_tj('strong').eq(0).text())
		nPollu=int(fenye_tj('strong').eq(1).text())
		nOK=int(fenye_tj('strong').eq(2).text())
		self.logger.info('nPages=%d***nTotal=%d***nPollu=%d***nOK=%d', nPages, nTotal, nPollu, nOK)
		#print len(v("tr[class='']"))
		t1=fenye_content("tr[class='']")
		t2=fenye_content("tr[class='even']")
		for i in range(len(t1)):
			entry=t1.eq(i)('td')
			company=entry.eq(0)('a').attr['title']
			pollutant=entry.eq(1).text()
			value=entry.eq(2).text()
			standard=entry.eq(3).text()
			multiple=entry.eq(4).text()
			time=entry.eq(5).text()
			city=entry.eq(6).text()
			self.insert_db(company, city)
			self.logger.error('%s	%s	%s	%s	%s	%s	%s	%s', company, pollutant, value, standard, multiple, time, response.meta['v'], city)
		
		for i in range(len(t2)):
			entry=t2.eq(i)('td')
                        company=entry.eq(0)('a').attr['title']
                        pollutant=entry.eq(1).text()
                        value=entry.eq(2).text()
                        standard=entry.eq(3).text()
                        multiple=entry.eq(4).text()
                        time=entry.eq(5).text()
                        city=entry.eq(6).text()
                        self.logger.error('%s    %s      %s      %s      %s      %s      %s	%s', company, pollutant, value, standard, multiple, time, response.meta['v'], city)
		
		for i in range(2, nPages+1):
			#self.logger.info('------------------------------%d', i)
			yield scrapy.FormRequest(url=self.poll_url, formdata=self.get_frmdata(i, response.meta['k']), callback=self.parse_page_next, meta={'v':response.meta['v']})
	
	def parse_page_next(self, response):
		#print response.body
		respJson=re.sub(r"(,?)(\w+?)\s*?:", r"\1'\2':", response.body);
		respJson=respJson.replace("'", "\"");
		res=json.loads(respJson)
		fenye_content=pq(urllib.unquote(res['Content'].replace('%u', '\\u').decode('unicode-escape')))
		t1=fenye_content("tr[class='']")
                t2=fenye_content("tr[class='even']")
                for i in range(len(t1)):
                        entry=t1.eq(i)('td')
                        company=entry.eq(0)('a').attr['title']
                        pollutant=entry.eq(1).text()
                        value=entry.eq(2).text()
                        standard=entry.eq(3).text()
                        multiple=entry.eq(4).text()
                        time=entry.eq(5).text()
                        city=entry.eq(6).text()
                        self.logger.error('%s    %s      %s      %s      %s      %s	%s      %s', company, pollutant, value, standard, multiple, time, response.meta['v'], city)

                for i in range(len(t2)):
                        entry=t2.eq(i)('td')
                        company=entry.eq(0)('a').attr['title']
                        pollutant=entry.eq(1).text()
                        value=entry.eq(2).text()
                        standard=entry.eq(3).text()
                        multiple=entry.eq(4).text()
                        time=entry.eq(5).text()
                        city=entry.eq(6).text()
                        self.logger.error('%s    %s      %s      %s      %s      %s      %s	%s', company, pollutant, value, standard, multiple, time, response.meta['v'], city)


