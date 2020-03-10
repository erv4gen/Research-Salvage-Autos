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

import sys


class SelBot:

	def __init__(self, name='',output_path='',log_path='',proxy_f = True,home_path=''):
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
		
		
		if self.proxy_f:
			profile.set_preference("network.proxy.type", 1)

			profile.set_preference("network.proxy.http", HOST)

			profile.set_preference("network.proxy.http_port", PORT)

			profile.set_preference("network.proxy.socks_username", USER)

			profile.set_preference("network.proxy.socks_password", PASSWD)

			 

			profile.update_preferences()
		# driver_dir = 'c:/selenium/geckodriver.exe'
		from selenium.webdriver.firefox.firefox_binary import FirefoxBinary

		binary = FirefoxBinary(r'C:\Program Files\Mozilla Firefox\firefox.exe')
		
		
		self.driver = webdriver.Firefox(

		 options=options
			,firefox_profile=profile
			,firefox_binary=binary
										)

	def close_ff(self):
		self.driver.quit()
		logging.info('All work done, closing the session')

	def test_(self):
		self.driver.get('https://ifconfig.co/')

		self.driver.save_screenshot('png/ifconfig.png')

	def main_page(self,year_to_export):
		pass

	def take_screenshot(self):
	
		screen_path = 'png/headless_firefox_test.png'
		
		self.driver.save_screenshot(screen_path)
		
	def export_data(self, start_with=None):
		pass
					
					
	def run_process(self,years):
		try:
			pass
		
		except Exception as e:
		 print('Error ',e)
		
		finally:
			self.close_ff()
