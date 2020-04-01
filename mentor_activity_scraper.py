from bs4 import BeautifulSoup
from selenium import webdriver
import config as cfg
from selenium.webdriver.support.ui import WebDriverWait
from configparser import ConfigParser

def login(username, password):
    login_url = 'https://knowledge.udacity.com/'
    email_html = '//input[type="email"]'
    password_html = '//input[type="email"]'
    button_html = '//button[type="button"]'

    driver = webdriver.Chrome()
    driver.get(login_url)

    login_element = WebDriverWait(driver, 10).until(lambda driver: driver.find_element_by_xpath(login_url))
    login_element.click()

    gmail_element = WebDriverWait(driver, 10).until(lambda driver: driver.find_element_by_xpath(email_html))
    gmail_element.click(username)

    password_element = WebDriverWait(driver, 10).until(lambda driver: driver.find_element_by_xpath(password_html))
    password_element.send_keys(password)

    button_element = WebDriverWait(driver, 10).until(lambda driver: driver.find_element_by_xpath(button_html))
    button_element.click()

    return driver

def scrape_pages(driver, project_list, nanodegree_list):
    pass

def main():  
    # Get config values
    config = ConfigParser()
    config.read('config.cfg')
    username = config['USERNAME']
    password = config['PASSWORD']

    driver = login(username, password)

    nanodegree_project_dict = {}
    for nanodegree,project_list in nanodegree_project_dict.items():
        scrape_pages(driver, nanodegree, project_list)
    
    driver.quit()

if __name__ == "__main__":
    main()