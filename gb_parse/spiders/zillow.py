import scrapy
from selenium import webdriver
from selenium.webdriver.common.keys import Keys


class ZillowSpider(scrapy.Spider):
    name = 'zillow'
    allowed_domains = ['www.zillow.com']
    start_urls = ['https://www.zillow.com/homes/for_sale/San-Francisco,-CA_rb/']
    x_path = {
        'pagination': '//div[@class="search-pagination"]//ul/li/a/@href',
        'ads': '//article/div/a/@href',
    }
    
    def __init__(self, *args, **kwargs):
        super(ZillowSpider, self).__init__(*args, **kwargs)
        self.browser = webdriver.Firefox()
    
    def parse(self, response):
        for p_url in response.xpath(self.x_path['pagination']):
            yield response.follow(p_url, callback=self.parse)
        
        for ad_url in response.xpath(self.x_path['ads']):
            yield response.follow(ad_url, callback=self.ads_parse)
    
    def ads_parse(self, response):
        self.browser.get(response.url)
        media_col = self.browser.find_element_by_xpath('//div[contains(@class, "ds-media-col")]')
        len_photos = len(media_col.find_elements_by_xpath('//picture[contains(@class, "media-stream-photo")]'))
        tic = 0
        while True:
            tic += 1
            for _ in range(10):
                media_col.send_keys(Keys.PAGE_DOWN)
            tmp_len_photos = len(media_col.find_elements_by_xpath('//picture[contains(@class, "media-stream-photo")]'))
            if tmp_len_photos == len_photos:
                break
            len_photos = tmp_len_photos
        print(tic)
        
        print(1)
