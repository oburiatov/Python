from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager
import time

driver = webdriver.Chrome(ChromeDriverManager().install())
driver.get("https://twitter.com/explore")
time.sleep(5)
search_tweet=driver.find_element_by_css_selector("[data-testid=\"SearchBox_Search_Input\"]")
search_tweet.send_keys("qeragq123")
search_tweet.send_keys(Keys.RETURN)
time.sleep(5)
actual_result= driver.find_elements_by_xpath("//*[@id=\"react-root\"]/div/div/div[2]/main/div/div/div/div[1]/div/div[2]/div/div/div/div[1]/span")[0].text
expected_result="По запросу «qeragq123» ничего не найдено"
assert actual_result == expected_result


search_tweet=driver.find_element_by_css_selector("[data-testid=\"SearchBox_Search_Input\"]")
time.sleep(5)
search_tweet.clear()
search_tweet.send_keys("BarackObama")
search_tweet.send_keys(Keys.RETURN)
time.sleep(5)
actual_result=driver.find_element_by_xpath("//*[@id=\"react-root\"]/div/div/div[2]/main/div/div/div/div/div/div[2]/div/div/section/div/div/div[3]/div/div/div/div[2]/div[2]/span").text
expected_result="Dad, husband, President, citizen."
assert actual_result==expected_result


driver.close()