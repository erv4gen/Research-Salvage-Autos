from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
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

import sys


class SelBot:

	def __init__(self, name='',output_path=''):
		#self.down_path = '/home/vlad/Downloads/'+name+'/'
		self.output_path = output_path
		self.name = name
		logging.info('Make selected: '+str(name))
	def open_ff(self):
		logging.info('Starting new session')
		
		options = Options()
		options.headless = True
		profile = webdriver.FirefoxProfile()

		#profile.set_preference("browser.download.folderList", 2)
		#profile.set_preference("browser.download.manager.showWhenStarting", False)
		#profile.set_preference("browser.download.dir", self.down_path)

		#with open('profile.txt', 'r') as f:
		#	profile_string = f.read().split(',')
		#import ipdb
		#ipdb.set_trace()
		#profile.set_preference(*profile_string)

		# self.driver = webdriver.Firefox(executable_path = 'geckodriver/geckodriver.exe',firefox_profile=profile)
		self.driver = webdriver.Firefox(executable_path='/home/vlad/selenium/geckodriver', options=options, firefox_profile=profile, firefox_binary='/usr/bin/firefox'
										)
		self.soft_pause = 300
		self.long_pause = 1000
		self.eps = 5
	def close_ff(self):
		self.driver.quit()
		logging.info('All work done, closing the session')

	def test_(self):
		self.driver.get('https://www.duckduckgo.com')

		self.driver.save_screenshot('headless_firefox_test.png')

	def main_page(self):

		logging.info('Opening main page')
		# open url
		self.driver.get('https://www.salvageautosauction.com/price_history/')
		time.sleep(1)


		# select all pages
		select = self.driver.find_element_by_name('cboFrYear')
		options = [x for x in select.find_elements_by_tag_name("option")]

		for element in options:
			if element.get_attribute("value") == '1900':
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
		with open(self.output_path+'{}/page_1.html'.format(self.name), 'w') as f:
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

	def export_data(self, pages=10, start_with=None):
		if start_with is not None:
			url = 'https://www.salvageautosauction.com/price_history/{}'.format(
				start_with)
			print('Starting with: ', url)

			self.driver.execute_script(
				"javascript:gotoPage(this, '{}')".format(url))
			# self.driver.get(url)
			# self.driver.save_screenshot('headless_firefox_test.png')
			# return 0
			time.sleep(random.randint(1, 2))
		# iterate through each page

		for page_id in range(pages):
			if page_id % self.soft_pause == 0:
				slp = 60
				self.eps+=1
			elif page_id % self.long_pause == 0:
				slp = 120
			else:
				slp = 3
			tts = random.randint(slp, slp+self.eps)
			try:
				
				# click next page
				next_page = self.driver.find_element_by_class_name('item.next')
				next_page.click()
				page_num = self.driver.current_url.split('/')[-1]
				with open(self.output_path+'{}/page_{}.html'.format(self.name, page_num), 'w') as f:
					f.write(self.driver.page_source)
				status = 'DONE'
				print(self.driver.current_url,status)
				
				
				time.sleep(tts)
				logging.info('Page: '+str(page_id+1)+':'+'status-'+status+'; slp. time- '+str(tts))
			except Exception as e:
				status = 'ERROR'
				print('Error at:', page_id, '\nError details:', e)
				
				logging.error('Page: '+str(page_id+1)+':'+'status-'+status
								+'; slp. time- '+str(tts)
								+';error- '+str(e)
								)
				time.sleep(random.randint(5, 15))
				
			
#			 self.driver.save_screenshot('headless_firefox_test.png')
#			 if unlog_f:
#				 break
#			 if first_url_flg:
#				 links_obj = self.driver.find_elements_by_partial_link_text('')
#				 links =[]
#				 for link in links_obj:
#					 try:
#						 url_to_app = link.get_attribute("href")
#						 if 'https' in url_to_app:
#							 links.append(url_to_app)
#					 except:
#						 continue
#				 first_url_flg =False

#			 for date in self.date_month_year:
#				 if unlog_f:
#					 break
#			 #for date in tqdm(part_date):
#				 for country in self.geos:
#					 if unlog_f:
#						 break
#					 try:
#						 req_url = 'https://www.semrush.com/analytics/traffic/subdomains/{}?dateStart={}&country={}'.format(domain,date,country)
#						 if not silence:
#							 print('-'*20,'\nCountry: ',country,'\nData: ',date,'\nDomain: ',domain,'URL: ', req_url)
#						 #domain  = 'africa.dnvgl.com'
#						 #date = '2017-02-01'
#						 #country = 'cn'

#						 self.driver.get(req_url)
#						 time.sleep(2)
#						 try:
#							 logging_error = self.driver.find_element_by_class_name('RLDDG')
#							 if 'This report is available for users with the' in logging_error.get_attribute('innerHTML'):
#								 print('Session logged out')
#								 unlog_f = True
#								 break
#						 except:
#							 pass
#						 download_button = self.driver.find_element_by_class_name("sc-1_4_5-btn__size_xs")
#						 ActionChains(self.driver).move_to_element(download_button).click(download_button).perform()
#						 time.sleep(2)
#						 list_of_files = glob.glob(self.down_path+'*')
#						 latest_file = max(list_of_files, key=os.path.getctime)
#						 if country =='':
#							 country = 'global'
#						 new_name = self.down_path+domain+'_'+date+'_'+country+'.csv'
#						 os.rename(latest_file, new_name)
#						 if not silence:
#							 print('Result: ',new_name, ' - Downloaded')
#						 time.sleep(2)
#						 #open a random link
#						 l= links[randint(0, len(links)-1)]
#						 self.driver.get(l)
#						 time.sleep(2)
#					 except Exception as e:
#						 if not silence:
#							 print('Result: ',e)
#						 continue

	def run_process(self, pages=1, start_with=None):
		# create logger
		logging.config.fileConfig("logg_config.ini")
		self.open_ff()
		self.main_page()
		self.export_data(pages=pages, start_with=start_with)
		self.close_ff()
		print('All work done')


if __name__ == '__main__':
	if len(sys.argv) < 2:
		print('string format : <name> <no pages>')
	else:
		try:
			name = sys.argv[1]
			pages = int(sys.argv[2])
			output_path = '/home/vlad/csv/Cars/'
			if not os.path.exists(output_path+name):
				print('Creating folder')
				os.makedirs(output_path+name)
			print('Starting work for {} with {} pages'.format(name, pages))
			bot = SelBot(name,output_path=output_path)
			bot.run_process(pages=pages)
			print('terminal is active')
		except:
			print("Exception occured:", traceback.format_exc())
