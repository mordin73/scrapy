import scrapy
import re
import json
import requests




class KraskiSpider(scrapy.Spider):
    name = 'kraski'
    allowed_domains = ['yarkraski.ru']


    def cleanhtml(self, raw_html):
        CLEANR = re.compile('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});')
        CLEANR2 = re.compile('\r|\n|\t|\s{2,20}')
        cleantext = re.sub(CLEANR, '', raw_html)
        cleantext = cleantext.replace('###', '<br>')
        text = re.sub(CLEANR2, '', cleantext)
        return text

    def start_requests(self):
        yield scrapy.Request(url='https://yarkraski.ru/products/?filter=filtered&brand=144&section=all&SHOWALL_1=1', callback=self.parse_category)

    def parse_category(self, response):
        links = response.xpath('//h2/a/@href').getall()
        for link in links:
            absolute_url = f'https://yarkraski.ru{link}'
            response = requests.get(url=absolute_url)
            data_all = re.search('(?<=new\ JCCatalogElement\()[\w\W]*?(?=\);\n<\/script>)',
                                 response.text)
            data_json = data_all.group(0)
            yield scrapy.Request(url=absolute_url, callback=self.parse_page, meta={'data_json': data_json})


    def parse_page(self, response):
        BRAND = ''
        name = response.xpath('normalize-space(//h1/text())').get()
        loose = response.xpath('normalize-space(//div[@class="properties-item"]/b[contains(text(), "Расход")]'
                               '/following::text())').get()
        garant = response.xpath('normalize-space(//div[@class="properties-item"]/b[contains(text(), "Гарантия")]'
                                '/following::text())').get()
        descr_all = response.xpath('//div[@class="panes"]/div[1]/p/text() | //div[@class="panes"]/div[1]/p/b/text() | '
                                   '//div[@class="panes"]/div[1]/ul/li/node()| //div[@class="panes"]/div[1]/text() | '
                                   '//div[@class="panes"]/div[1]/div/text()').getall()
        descr_str = '<br>'.join(descr_all)
        descr_str = self.cleanhtml(descr_str)
        if len(descr_str) <= 5:
            descr_all = response.xpath('//span[contains(@style,"sans-serif; color: #333333;")]/span/node()').getall()
            descr_list = list(dict.fromkeys(descr_all))
            descr_str = '###'.join(descr_list)
            descr_str = self.cleanhtml(descr_str)
            descr_str = descr_str.replace('###', '<br>')

        compound = response.xpath('//b[contains(text(), "Состав")]/following::text() | '
                                  '//div/b[contains(text(), "Состав")]/following::br/following::text() | '
                                  '(//b[contains(text(), "СОСТАВ")]/following::p[1]/node())[2]').get()
        if compound:
            compound = self.cleanhtml(compound)

        koler = response.xpath(
            '//b[contains(text(), "Колеровка")]/following::text() | '
            '//div/b[contains(text(), "Колеровка")]/following::br/following::text() | '
            '(//b[contains(text(), "КОЛЕРОВКА")]/following::p[1]/node())[2]').get()
        if koler:
            koler = self.cleanhtml(koler)

        transport = response.xpath(
            '//b[contains(text(), "Транспор")]/following::text()[3] | '
            '//div/b[contains(text(), "Транспор")]/following::br/following::text() | '
            '(//b[contains(text(), "ТРАНСПОРТИРОВАНИЕ И ХРАНЕНИЕ")]/following::p[1]/node())[2]').get()
        if transport and not len(transport) <= 10:
            transport = self.cleanhtml(transport)
        elif not transport:
            transport = None
        elif len(transport) <= 10 or transport == compound:
            transport_all = response.xpath('//b[contains(text(), "Транспор")]/following::div/node()').getall()
            transport_list = list(dict.fromkeys(transport_all))
            transport_str = '###'.join(transport_list)
            transport_str = self.cleanhtml(transport_str)
            transport = transport_str.replace('###', '<br>')


        # category = response.xpath('//ul[@class="breadcrumb-navigation"]/li[@class="last"]/a/text()').get()




        # use = response.xpath('(//*[contains(text(), "ПРИМЕНЕНИЕ:")]/following::p/node())[1]').get()
        use = list(response.xpath('//div[@class="panes"]/div[3]').extract())
        if use:
            use = '###'.join(use)
            use = self.cleanhtml(use)


        data_all = response.meta['data_json'].replace('\'', '"')

        data_json = json.loads(data_all)

        name = data_json['PRODUCT']['NAME']
        category = data_json['PRODUCT']['CATEGORY']

        try:
            offers = data_json['OFFERS']
            for offer in offers:
                img_url = offer['SLIDER'][0]['SRC']
                img_full_url = f'https://yarkraski.ru{img_url}'
                codes = re.findall('(?<=<dd>)[\w\W]*?(?=<)', offer['DISPLAY_PROPERTIES'])
                art = codes[0]
                try:
                    barcode = codes[1]
                except:
                    barcode = ''
                name_full = f'{offer["NAME"]} ({art})'
                weight = re.search('(\d{1,3}(\.|\,)\d{1,3}.*|\d{1,3}.*)', offer['NAME']).group(0)
                yield {
                    'Артикул': art,
                    'Индивидуальный штрихкод': barcode,
                    'Бренд': BRAND,
                    'Индивидуальное название (Название модели)': name,
                    'Название и Артикул в интернет магазине': name_full,
                    'Вес товара (Вес нетто), кг.': weight,
                    'Полное описание, Особенности, Преимущества': descr_str,
                    'Фото': img_full_url,
                    'Гарантия': garant,
                    'Расход': loose,
                    'Состав': compound,
                    'Колеровка': koler,
                    'Транспортирование': transport,
                    'применение': use,
                    'Категория': category
                }
        except:
            img_url = data_json['PRODUCT']['SLIDER'][0]['SRC']
            img_full_url = f'https://yarkraski.ru{img_url}'
            art = data_json['PRODUCT']['ID']
            yield {
                'Артикул': art,
                'Бренд': BRAND,
                'Индивидуальное название (Название модели)': name,
                'Название и Артикул в интернет магазине': name,
                'Вес товара (Вес нетто), кг.': '',
                'Полное описание, Особенности, Преимущества': descr_str,
                'Фото': img_full_url,
                'Гарантия': garant,
                'Расход': loose,
                'Состав': compound,
                'Колеровка': koler,
                'Транспортирование': transport,
                'применение': use,
                'Категория': category
            }



