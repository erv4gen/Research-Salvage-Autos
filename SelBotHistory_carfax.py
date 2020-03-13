from SelBot import SelBot
import sys , os , traceback , logging ,time , glob
class SelBotCarFax(SelBot):
	def __init__(self,models,years,name,output_path,log_path,proxy_f,home_path):
		super().__init__(name,output_path,log_path,proxy_f,home_path) 
		self.models = models
		self.years = years

	def save_page(self,model,year):
		self.driver.get('https://www.carfax.com/car-research')
		
		#select make
		select = self.driver.find_element_by_xpath("//div[contains(text(),'Choose a Make')]").click()
		select = self.driver.find_element_by_xpath(f"//ul/li[contains(text(),'{self.name}')]").click()
		
		#select model
		select = self.driver.find_element_by_xpath("//div[contains(text(),'Choose a Model')]").click()
		select = self.driver.find_element_by_xpath(f"//ul/li[contains(text(),'{model}')]").click()

		#select year
		select = self.driver.find_element_by_xpath("//div[contains(text(),'Choose a Year')]").click()
		select = self.driver.find_element_by_xpath(f"//ul/li[contains(text(),'{year}')]").click()

		#select go
		select = self.driver.find_element_by_xpath("//button[contains(text(),'Go')]").click()

		
		self.take_screenshot()
		
		with open('{}/{}/{}_{}_{}.html'.format(self.output_path,self.name,self.name,model,year), 'w') as f:
			f.write(self.driver.page_source)
		print('DONE!')
		time.sleep(1)
	def run_process(self,name):
		try:
			logging.config.fileConfig("logg_config.ini")
			self.open_ff()
			#self.test_()
			for model in self.models:
				for year in self.years:
					print('Make/Model/Year: ',make,'/', model,'/',year)
					self.save_page(model,year)
					time.sleep(1)
		
		except Exception as e:
			print('Error ',e)
		
		finally:
			self.close_ff()

def run_selenium(name,models,years):
	proxy_f = False#bool(sys.argv[3])

	with open('config/vars','r') as f:
		home_path = f.read()
	output_path = home_path.strip()+'csv/'
	log_path = home_path.strip()+'logs/'
	
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
	
	
	

	bot = SelBotCarFax(name=name,models=models,years=years
	
	,output_path=output_path
	,log_path=log_path,proxy_f=proxy_f,home_path=home_path
	)
	

	
	bot.run_process(name)
if __name__ == '__main__':
	try:
		with open('input/years', 'r') as f:
			years = f.read().splitlines()
		to_pars_models = glob.glob('input/models/to_parse/*')
		make_model = {}
		for file in to_pars_models:
			with open(file, 'r') as f:
				make = file.split('\\')[1].split('.txt')[0]
				make_model[make] = f.read().splitlines()
		

			print('Starting for make ', make)
			
			run_selenium(make,make_model[make],years)
			time.sleep(5)

		print('terminal is active')
	except:
		print("Exception occured:", traceback.format_exc())