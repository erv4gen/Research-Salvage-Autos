from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.proxy import Proxy, ProxyType
from bs4 import BeautifulSoup
import json
import datetime
import os
import time
import glob
import traceback
import random
import pandas as pd
from selenium.webdriver.firefox.options import Options
from tqdm import tqdm
import logging
import logging.config
from random import randint
import multiprocessing as mp
from pprint import pprint
import ipdb

import sys


class SelBot:

	def __init__(self, name='',output_path='',log_path='',proxy_f = True,home_path=''):
		#self.down_path = '/home/vlad/Downloads/'+name+'/'
		self.output_path = output_path
		self.log_path = log_path
		self.name = name
		self.home_path = home_path
		self.proxy_f = proxy_f
		
		logging.info('Make selected: '+str(name))
	def open_ff(self):
		logging.info('Starting new session')
		
		options = Options()
		options.headless = True
		
		
		with open('nogit/pass.txt','r') as f:
			creds = f.read().splitlines()
		HOST = creds[0]
		PORT = creds[1]
		USER = creds[2]
		PASSWD = creds[3]
			
		profile = webdriver.FirefoxProfile()
		
		#ipdb.set_trace()
		if self.proxy_f:
			profile.set_preference("network.proxy.type", 1)

			profile.set_preference("network.proxy.http", HOST)

			profile.set_preference("network.proxy.http_port", PORT)

			profile.set_preference("network.proxy.socks_username", USER)

			profile.set_preference("network.proxy.socks_password", PASSWD)

			 

			profile.update_preferences()
		#profile.set_preference("browser.download.folderList", 2)
		#profile.set_preference("browser.download.manager.showWhenStarting", False)
		#profile.set_preference("browser.download.dir", self.down_path)

		#with open('profile.txt', 'r') as f:
		#	profile_string = f.read().split(',')

		#profile.set_preference(*profile_string)

		# self.driver = webdriver.Firefox(executable_path = 'geckodriver/geckodriver.exe',firefox_profile=profile)
		driver_dir = self.home_path.strip()+'selenium/geckodriver'
		#print('Searching for driver in:',driver_dir)
		#ipdb.set_trace()
		self.driver = webdriver.Firefox(
#executable_path=driver_dir,
		 options=options
			,firefox_profile=profile
			,firefox_binary='/usr/bin/firefox'
										)

	def close_ff(self):
		self.driver.quit()
		logging.info('All work done, closing the session')

	def test_(self):
		self.driver.get('https://ifconfig.co/')

		self.driver.save_screenshot('png/ifconfig.png')

	def main_page(self,year_to_export):

		logging.info('Opening main page')
		# open url
		self.driver.get('https://www.salvageautosauction.com/price_history/')
		time.sleep(1)
		self.yte = year_to_export
		# select from year
		select = self.driver.find_element_by_name('cboFrYear')
		options = [x for x in select.find_elements_by_tag_name("option")]
		print('Starting work for {}; year:{}'.format(name,year_to_export))
		#create a folder for a year
		self.downlad_folder = self.output_path+'{}/{}'.format(self.name,year_to_export)
		if not os.path.exists(self.downlad_folder):
			print('Creating output year folder')
			os.makedirs(self.downlad_folder)
		for element in options:
			if element.get_attribute("value") == year_to_export: #'1900'
				element.click()


		# select to year 
		select = self.driver.find_element_by_name('cboToYear')
		options = [x for x in select.find_elements_by_tag_name("option")]

		for element in options:
			if element.get_attribute("value") == year_to_export: #'1900'
				element.click()
		
		# Select category
		select = self.driver.find_element_by_name('cboMake')
		options = [x for x in select.find_elements_by_tag_name("option")]

		for element in options:
			if element.get_attribute("value") == self.name:
				element.click()

		# submit search
		sumbit = self.driver.find_element_by_class_name(
			'form-group.text-center')
		sumbit.click()

		# save page
		with open('{}/page_1.html'.format(self.downlad_folder), 'w') as f:
			f.write(self.driver.page_source)
#		 for index in range(len(select.options)):
#			 select = Select(self.driver.find_element_by_name('Acura'))
#			 select.select_by_index(index)


#		 inputElement.send_keys('1')
#		 inputElement.send_keys(Keys.ENTER)
#		 ActionChains(self.driver).move_to_element(login_button).click(login_button).perform()
#		 time.sleep(2)
#		 self.driver.find_element_by_name('email').send_keys('ul.webanalytics@gmail.com')
#		 self.driver.find_element_by_name('password').send_keys('pfingsten333')
#		 login_button = self.driver.find_element_by_class_name("sso-centered")
#		 time.sleep(2)
#		 ActionChains(self.driver).move_to_element(login_button).click(login_button).perform()
#		 time.sleep(2)


	def take_screenshot(self):
	
		screen_path = 'headless_firefox_test.png'
		#el = self.driver.find_element_by_tag_name('body')
		#el.screenshot(screen_path)
		#el.screenshot(screen_path)
		
		self.driver.save_screenshot(screen_path)
		
	def export_data(self, start_with=None):
	
		self.soft_pause = 300
		self.long_pause = 900
		self.eps = 4
		if start_with is not None:
			url = 'https://www.salvageautosauction.com/price_history/{}'.format(
				start_with)
			print('Starting with: ', url)

			self.driver.execute_script(
				"javascript:gotoPage(this, '{}')".format(url))
			#path = 'headless_firefox_test.png'
			# self.driver.get(url)
			#el = self.driver.find_element_by_tag_name('body')
			#el.screenshot(path)
			# self.driver.save_screenshot(path)
			# return 0
			time.sleep(random.randint(1, 2))
		# iterate through each page
		
		max_pages = 2000
		print('files will be saved to: ',self.downlad_folder)
		for page_id in range(max_pages):
		
			pr_m = True
			status = 'ERROR'
			page_num = ''
			if (page_id % self.soft_pause == 0) and page_id>0:
				slp = 60
				self.eps+=1
			elif (page_id % self.long_pause == 0) and page_id>0 :
				slp = 360
			else:
				slp = 2
			tts = random.randint(slp, slp+self.eps)
			try:
				
				# click next page
				time.sleep(tts)
				next_page = self.driver.find_element_by_class_name('item.next')
				next_page.click()
				page_num = self.driver.current_url.split('/')[-1]
				with open('{}/page_{}.html'.format(self.downlad_folder,page_num), 'w') as f:
					f.write(self.driver.page_source)
					status = 'DONE'
					
				msg = '('+self.yte+'-'+str(page_id)+' of '+self.last_year+')'+self.driver.current_url+':'+' name-'+self.name+' ;status-'+status+'; slp. time- '+str(tts)
				print(msg)
				logging.info(msg)
				
				
				#print('Checking pause option')
			
				#while True:
				#	with open('pause_at.txt','r') as f:
				#		p = f.read()
				#	
				#	if page_num == p:
				#		print('Process is on pause ...')
				#		time.sleep(10)
				#	else:
				#		break
				#
				#page_id+=1
			except Exception as e:
				
				msg = '('+self.yte+'-'+str(page_id)+' of '+self.last_year+')'+self.driver.current_url+':'+' name-'+self.name+' ;status-'+status+'; slp. time- '+str(tts) +'\nError details:' +str(e)
				print(msg)
				logging.error(msg)
				self.take_screenshot()
				
				if page_id >1 and 'item.next' in msg:
					#while True:
					#	with open('pause_err.txt','r') as f:
					#		p = f.read()
					#	
					#	if p == 'yes':
					#		if pr_m:
					#			print('Process is on pause due to the error!')
					#			pr_m = False
					#		time.sleep(20)
					#	else:
					#		self.driver.execute_script("window.history.go(-2)")
					#		with open('pause_err.txt','w') as f:
					#			f.write('yes')
					#		break
					break
				else: continue
					
					
	def run_process(self,years):
		# create logger
		
		try:
			logging.config.fileConfig("logg_config.ini")
			self.open_ff()
			self.test_()
			print('Years to export:',str(years))
			self.last_year = years[-1]
			for year in years:
				self.main_page(year_to_export = year)
				self.export_data()
				time.sleep(15)
			print('All work done')
		
		except Exception as e:
		 print('Error ',e)
		
		finally:
			self.close_ff()



if __name__ == '__main__':
	if len(sys.argv) < 2:
		print('string format : <name> <no pages>')
	else:
		try:
			name = sys.argv[1]
			#pages = int(sys.argv[2])
			proxy_f = False#bool(sys.argv[3])
			year_start = sys.argv[2]
			year_end = sys.argv[3]
			years = [str(y) for y in range(int(year_start),int(year_end)+1)]	
			years.reverse()
			#output_path = '/home/vlad/csv/Cars/'
			with open('config/vars','r') as f:
				home_path = f.read()
			output_path = home_path.strip()+'csv/'
			log_path = home_path.strip()+'logs/'
			
			print('Use proxy: ',proxy_f)
			if not os.path.exists(output_path):
				print('Creating output folder')
				os.makedirs(output_path)
				
			if not os.path.exists(log_path):
				print('Creating log folder')
				os.makedirs(log_path)

			if not os.path.exists(output_path+name):
				print('Creating make folder')
				os.makedirs(output_path+name)
				
			
			#reset default parameters
			with open('pause_at.txt','w') as f:
				f.write('-1')
			with open('pause_err.txt','w') as f:
				f.write('yes')
			bot = SelBot(name,output_path=output_path,log_path=log_path,proxy_f=proxy_f,home_path=home_path)
			
			
			bot.run_process(years)
			print('terminal is active')
		except:
			print("Exception occured:", traceback.format_exc())
