import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.selector import Selector
import pandas as pd
from datetime import datetime
import re
import json
from urllib.parse import urlencode
import requests
import random
import tls_client
from scrapy.http import TextResponse
from scrapy.exceptions import IgnoreRequest
from scrapy.dupefilters import RFPDupeFilter
from scrapy.utils.request import fingerprint
from dotenv import load_dotenv
import os
import boto3

load_dotenv()

aws_key = os.getenv("aws_key")
aws_secret = os.getenv("aws_secret")

today = datetime.today()
date = today.strftime("%Y-%m-%d")
output_date = today.strftime("%Y%m%d")

retailer_locale = 'Notino'
retailer_locale_name = 'Notino DE'


logs_name = "notinode-logs.txt"


def upload_to_s3(file_path, object_name):
    # Create an S3 client
    s3 = boto3.client('s3')
    bucket_name = 'bucketbatchtest'
    # Upload the file to S3
    try:
        s3 = boto3.client('s3', aws_access_key_id=aws_key, aws_secret_access_key=aws_secret, region_name='eu-central-1')
        s3.upload_file(file_path, bucket_name, object_name)
        #s3.put_object(Body=df_data.to_csv(index=False), Bucket='etl-test-weatherapi', Key=filename)
        print(f"Upload to S3 successful: {object_name}")
    except Exception as e:
        print(f"Error uploading to S3: {e}")


class Notino(scrapy.Spider):
   name = "notino"

   base_url = "https://www.notino.de"
   starting_url = "https://www.notino.de/kosmetikmarken/"

   headers = {
      'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
      'referer': None,
    }
    
   custom_settings = {
    "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
     "DOWNLOAD_DELAY": 2,
     'DOWNLOADER_MIDDLEWARES': {
            'notinoDE.MyCustomMiddleware': 543,
      },
      "ROBOTSTXT_OBEY": False,
      'AUTOTHROTTLE_ENABLED': True,
      'RETRY_TIMES': 2,
      'RETRY_ENABLED': True,
      'DOWNLOAD_TIMEOUT': 9,
      'REQUEST_FINGERPRINTER_IMPLEMENTATION': "2.7",
      'LOG_FILE': logs_name,
   }
    
   final_created = False
   products_list_created = False

   def start_requests(self):
      yield scrapy.Request(url=self.starting_url, headers=self.headers, callback=self.parse_brands)
    
   def api_headers(self):
        api_headers = {
          'authority': 'www.notino.de',
          'accept': 'application/json, text/plain, */*',
          'accept-language': 'en-US,en;q=0.9,ar;q=0.8,en-AU;q=0.7,en-GB;q=0.6,it;q=0.5',
          'cache-control': 'no-cache',
          'content-type': 'application/json',
          'cookie': 'ab80=1; grd=67207979090106332; npcount=1; db_ui=688a6e69-823d-5a0f-e025-e914a2dbe3d9; db_uicd=4540e27b-7616-a0e0-bf0f-b16602472421; __exponea_etc__=e0eb91a0-0b48-415d-a4c1-dd7fe63d4305; _bamls_usid=8704025a-e31a-4290-97c7-8426411de9c2; pppbnr=1; source=www.upwork.com; source45=www.upwork.com; lastProds=16090771-511673-16067216-16091217-16090566-16090380-16147298-16143900-16092821-16136656-15959161-16089261-16096621-16085134-16136223; lastSource=direct; USER=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzaG9wIjoibm90aW5vLmRlIiwiY2dycCI6IjI1MCIsImxhbmciOiI1IiwibHRhZyI6ImRlLURFIiwiY3VyciI6IjEiLCJjYXJ0IjoiMEE1MTAwMDAtRjFFRC1BQTg1LUMxMjUtMDhEQUMyRjFCODZCIiwicm9sZSI6IkFub255bW91cyIsImdyZCI6IjY3MjA3OTc5MDkwMTA2MzMyIiwic2lkIjoiMEE1MTAwMDAtRjFFRC1BQTg1LUMxMEItMDhEQUMyRjFCODZCIiwiY2xpZW50Ijoid2ViIiwiaWF0IjoxNjcwMjYyMzg1LCJpc3MiOiJub3Rpbm8ifQ.K67YhhkWFAeTR9Lox6PsgOb7UDklYA-bnjsT797Blj0; lpv=aHR0cHM6Ly93d3cubm90aW5vLmRlLzNpbmEvP2Y9Mi0zLTcxMjAz; TS01c0a98c=016bdf2fdc1bafdadfc5e8f6c00567b055c1184e3d69952c813f6d725c685ed388dbc705b29c3c1564b22caf6670083fdfb69b7e7aacefae9aa7f82e05229995493c493073; TS0178d2ea=016bdf2fdc93a13ab01453b20ce6c9507bc77a417269952c813f6d725c685ed388dbc705b225a73f599b5e9544e99bc3e2aaf6124c0891ee15f45920f0b83aa50cd71d025bdae7b17d725f5c874a126a14ed52e6a416cfccba8289190cd860f69b39fcf3a0c5a69da8f4e46bdb5fc1d4bdb92ddf742c09d85c8eb06050a1358a08605c7ba4; __exponea_time2__=-0.33794140815734863; TS8ffb2f78027=08a5d12542ab2000533924848106d348d488fdcc789dfa3785d58bf6683079de3c4e4fb619d2eb050817304e45113000050db1815ce30dbeb8bb2874b891370897faa9b1708013bd38072653ed2b9f49bd614928a032c75f14842c6f0f5da9c0; USER=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzaG9wIjoibm90aW5vLmRlIiwiY2dycCI6IjI1MCIsImxhbmciOiI1IiwibHRhZyI6ImRlLURFIiwiY3VyciI6IjEiLCJjYXJ0IjoiMEE1MTAwMDAtRjFFRC1BQTg1LUMxMjUtMDhEQUMyRjFCODZCIiwicm9sZSI6IkFub255bW91cyIsImdyZCI6IjY3MjA3OTc5MDkwMTA2MzMyIiwic2lkIjoiMEE1MTAwMDAtRjFFRC1BQTg1LUMxMEItMDhEQUMyRjFCODZCIiwiY2xpZW50Ijoid2ViIiwiaWF0IjoxNjY4MDcyNzk0LCJpc3MiOiJub3Rpbm8ifQ.v_dKADqzrTFOG0lyozQY_Lf9GQiTs2zU-rm5IjWhyu8',
          'origin': 'https://www.notino.de',
          'pragma': 'no-cache',
          'referer': None,
          'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
          'sec-ch-ua-mobile': '?0',
          'sec-ch-ua-platform': '"Windows"',
          'sec-fetch-dest': 'empty',
          'sec-fetch-mode': 'cors',
          'sec-fetch-site': 'same-origin',
          'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
        }

        return api_headers
      
   def parse_brands(self, response):
      links = response.css("div.crossroad-brands ul.reset li a")
      for index, link in enumerate(links):
            item = {
               "brand_url": self.base_url + link.css("::attr(href)").get(),
            }
            yield scrapy.Request(url=item['brand_url'], headers=self.headers, callback=self.parse_brand_page,
                                 meta={"href": link.css("::attr(href)").get().replace("/", "")})
            break
    
   def parse_brand_page(self, response):
        href = response.meta.get("href")

        page_number = 1
        cat_number = re.search(r'(?<=\"category\"\:\s\[).+?(?=\])', response.text)[0]
        page = f"{page_number}-3-{cat_number}"
        
        body = f'{{"urlPart":"{href}","pageSize":24,"filterString":"{page}","include":{{"filtration":false,"breadcrumbs":false,"navigationTree":false,"searchCategories":false,"listing":true,"specialPageData":false}}}}'
        url = "https://www.notino.de/api/navigation/navigation/notino.de"
        yield scrapy.Request(url=url, headers=self.api_headers(), method='POST', 
                            meta={"href": str(href), "cat_number": str(cat_number)},
                            callback=self.parse_brands_api,
                            body=body
                            )

   def parse_brands_api(self, response):
        json_response = json.loads(response.text)
        href = response.meta.get("href")
        cat_number = response.meta.get("cat_number")
        

        for p in json_response['listing']['products'][0:3]:
            url = self.base_url + p['url']
            yield scrapy.Request(url=url, headers=self.headers, callback=self.parse_details, )
                                
        # current_page = json_response['listing']['currentPage']
        # number_of_pages = json_response['listing']['numberOfPages']
        
        # if number_of_pages > 0 and current_page != number_of_pages:
        #     page_number = current_page + 1
        #     page = f"{page_number}-3-{cat_number}"
            
        #     body = f'{{"urlPart":"{href}","pageSize":24,"filterString":"{page}","include":{{"filtration":false,"breadcrumbs":false,"navigationTree":false,"searchCategories":false,"listing":true,"specialPageData":false}}}}'

        #     url = "https://www.notino.de/api/navigation/navigation/notino.de"
        #     yield scrapy.Request(url=url, headers=self.api_headers(), method='POST', body=body, 
        #                          callback=self.parse_brands_api,
        #                          meta={"href": href, "cat_number": cat_number})
       
   def get_size(self, text):
        size = ""
        find_size = re.search(r"\d+[.,]?\d*\s?(ml(\s|$)|fl\.\s?oz|g(\s|$)|oz\.|kg(\s|$)|St\.)", text, flags=re.IGNORECASE)
        if find_size:
            size = find_size[0]
        return size

   def parse_details(self, response):
        text = response.text
        
        req_url = response.request.url.split("/")[0:-1]
        if len(req_url) == 7:
            brand_name = req_url[4].replace("-", " ").replace("_", " ").title()
        else:
            brand_name = req_url[3].replace("-", " ").replace("_", " ").title()
    
        tags = ""
        active_ingreds = ""
        
        try:
            primary_name = response.css("span.sc-3sotvb-4 *::text").get().strip()
        except:
            primary_name = ""
            
        try:
            secondary_name = response.css("span.sc-3sotvb-5 *::text").get().strip()
        except:
            secondary_name = ""
            
        product_name = f"{primary_name} {secondary_name}"
        
        product_url = re.split(r"\/p\-\d+", response.request.url)[0]
        
        try:
            ratings_params = re.findall(r"(?<=\"aggregateRating\"\:).*?(?=\})", text)[0]
            rating = re.findall(r"(?<=\"ratingValue\"\:).*?(?=\,)", ratings_params)[0]
            num_reviews = re.findall(r"(?<=\"ratingCount\"\:).*?$", ratings_params)[0]
        except:
            rating = ""
            num_reviews = ""

        try:
            breadcrumbs = " > ".join([n.css("*::text").get().strip() for n in response.css('div[class^=styled__BreadcrumbWrapper-sc]')[0].css('a[class^=styled__BreadcrumbLink]')])
        except:
            breadcrumbs = "N/A"
        unit = ""
        backup_size = ""
        final = []
        
        item = {
            "retailer_locale": retailer_locale,
            "retailer_locale_name": retailer_locale_name,
            "Brand Name": brand_name if brand_name else "N/A", 
            "product_name": product_name.strip().replace("=", "") if product_name else "N/A",
            "product_url": product_url,
            "Descriptions": "N/A",
            "Full Ingredients": "N/A",
            "Key Ingredients": active_ingreds if active_ingreds else "N/A",
            "Price": "N/A",
            "Sale": "N",
            "Sale Price": "N/A",
            "Sold Out?": "N/A",
            "Product Size": "N/A",
            "Variant Name": "N/A",
            "num_reviews": num_reviews if num_reviews else "N/A",
            "star_rating": rating if rating else "N/A",
            "tags": tags if tags else "N/A",
            "scrape_date": date,
            "product_category": breadcrumbs,
        }  
        try:
            b = "".join(response.css("div#pdSelectedVariant div.ihLyFa span *::text").extract()).strip()
            splitted = b.split(" ")
            if len(splitted) > 1:
                unit = splitted[-1]
            backup_size = re.search(r"\d+[.]?\d*", b)[0]
        except:
            pass
        
        if response.css('li[data-testid^="color-picker-item"]'):
            variants = response.css('li[data-testid^="color-picker-item"]')
            var_type = "Color"
            variant_ids = [var.css('a[id^="pd-variant-"]::attr(id)').get().split("-")[-1] for var in variants]
        elif response.css('div#pdVariantsTile ul li'):
            variants = response.css('div#pdVariantsTile ul li')
            var_type = "Size"
            variant_ids = [var.css('a[id^="pd-variant-"]::attr(id)').get().split("-")[-1] for var in variants]
        else:
            variant_ids = [response.css("input[name='productId']::attr(value)").get()]
            variant_ids = list(filter(None, variant_ids))
            var_type = ""
            if not variant_ids:
                variant_ids = [re.findall(r"(?<=productId\=)\d+(?=&)", text)[0]]

        for var in variant_ids:
            current_item = item.copy()

            try:
                unit
            except:
                unit = ""
                
            sale_price = "N/A"
            
            json_select = re.findall(fr"(?<=Variant:{var}\":).*\"primaryCategories\".*?}}}}", text, flags=re.MULTILINE|re.DOTALL)[0]

            variant_name = re.findall(r"(?<=\"additionalInfo\"\:\").*?(?=\")", json_select, flags=re.MULTILINE|re.DOTALL)[0]
            sold_out = "N" if re.findall(r"(?<=\"state\"\:).*?(?=\,)", json_select, flags=re.MULTILINE|re.DOTALL)[0].replace("\"", "") == "CanBeBought" else "Y"
            price_pars = re.findall(r"(?<=\"price\"\:\{\").*?(?=\},)", json_select, flags=re.MULTILINE|re.DOTALL)[0]
            normal_price = re.search(r"(?<=\"value\"\:).*?(?=\,)", price_pars)[0]
            variant_id = re.findall(r"(?<=\"webId\"\:\").*?(?=\")", json_select, flags=re.MULTILINE|re.DOTALL)[0]
            
            try:
                ingreds = re.findall(r"(?<=\"ingredients\"\:).*?(?=\,\")", json_select)[0].strip()
            except:
                ingreds = ""
            
            try:
                desc = re.findall(r"(?<=\"description\"\:).*?(?=\,\")", text)[0]
            except:
                desc = ""
            try:
                characteristics = re.findall(r"(?<=characteristics\"\:).*?(?=\}\]\,)", json_select)[0]
                labels = re.findall(r"(?<=\"name\"\:\").*?(?=\"\,)", characteristics)
                values = re.findall(r"(?<=\"values\"\:\[).*?(?=\])", characteristics)
                characteristics_list = []
                for i in range(len(values)):
                    characteristics_list.append("{}: {}".format(labels[i].strip(), values[i].replace('\"', '').strip()))
                if characteristics_list:
                    desc += "\n Characteristics\n " + "\n ".join(characteristics_list)
            except:
                pass
        
            desc = re.sub('\n+', '\n ', re.sub(r"<.*?\>", "\n", desc.replace("&gt;", ">").replace("&lt;", "<").replace("\"", "")).strip())            

            try:
                original_price_pars = re.findall(r"(?<=\"originalPrice\"\:\{\").*?(?=\},)", json_select, flags=re.MULTILINE|re.DOTALL)[0]
                original_price = re.search(r"(?<=\"value\"\:).*?(?=\,)", original_price_pars)[0]
            except:
                original_price = "N/A"
        
            if original_price != "N/A":
                if original_price == normal_price:
                    price = original_price
                    sale_price = "N/A"
                else:
                    price = original_price
                    sale_price = normal_price
            else:
                price = normal_price
                sale_price = "N/A"
        
            current_item["Price"] = price
            current_item["Sale Price"] = sale_price
            current_item["Sold Out?"] = sold_out
            
            if not var_type:
                find_size = self.get_size(variant_name)
                current_item["Variant Name"] = "N/A"
                current_item["Product Size"] = find_size if find_size else "N/A"
            elif var_type == "Size":
                current_item["Product Size"] = variant_name.strip() if variant_name else "N/A"
            elif var_type == "Color":
                current_item["Variant Name"] = variant_name.strip() if variant_name else "N/A"
            
            if sale_price == "N/A":
                current_item["Sale"] = "N"
            else:
                current_item["Sale"] = "Y"
            
            if ingreds.strip("\"").strip():
                current_item["Full Ingredients"] = ingreds.replace("\"", "").replace("\n", "").replace("\r", "").strip() if ingreds.strip() != "null" else "N/A"
            else:
                current_item["Full Ingredients"] = "N/A"
                
            current_item["Descriptions"] = desc if desc else "N/A"
            
            if unit and unit not in current_item["Product Size"] and current_item["Product Size"] != "N/A":
                current_item["Product Size"] = current_item["Product Size"] + " " + unit
            
            if current_item["Product Size"] == "N/A" and current_item["Variant Name"] != "N/A":
                find_size = self.get_size(current_item["Variant Name"])
                current_item["Product Size"] = find_size.strip() if find_size else "N/A"
            
            if current_item["Product Size"] == "N/A":
                
                current_item["Product Size"] = f"{backup_size} {unit}" if backup_size else "N/A"
            
            if current_item["Variant Name"] == current_item["Product Size"] and current_item["Product Size"] != "N/A":
                current_item["Variant Name"] = "N/A"
            
            current_item["Product Size"] = re.sub(r"(?<=\d)\,", ".", current_item["Product Size"]).strip() if current_item["Product Size"] != "N/A" else "1 ks"
            
            if current_item["Product Size"] == current_item["Variant Name"] or current_item["Variant Name"] == "N/A":
                current_item["Variant Name"] = variant_id
                
            final.append(current_item)
    
        if self.final_created:
            pd.DataFrame(final).to_csv(f"product_details_table_{retailer_locale_name}_{output_date}.csv", 
                                          encoding='utf-8-sig', mode='a', index=False, header=False)
        else:
            self.final_created = True
            pd.DataFrame(final).to_csv(f"product_details_table_{retailer_locale_name}_{output_date}.csv", 
                              encoding='utf-8-sig', mode='w', index=False)

class MyCustomDupeFilter(RFPDupeFilter):
    def __init__(self, path=None, debug=False):
        super().__init__(path=path, debug=debug)

    def request_seen(self, request):
        fp = fingerprint(request)
        if fp in self.fingerprints:
            return True
        self.fingerprints.add(fp)
        return False


class MyCustomMiddleware(object):
    def __init__(self):
        self.dupefilter = MyCustomDupeFilter()
        
    def process_request(self, request, spider):
        """
        Process a request before it is sent to the downloader.

        Args:
            request: The request to be processed.
            spider: The spider that made the request.

        Returns:
            A new request object, or None if the request should be dropped.
        """
        # Only process tagged request or delete this if you want all
        request_url = request.url

        # Configure TLS session
        session = tls_client.Session(
            client_identifier="chrome112",
            random_tls_extension_order=True
        )

        # Select a random proxy from the list

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        }
        # Make the request with the appropriate method
        if request.method == "GET":
            if self.dupefilter.request_seen(request):
                raise IgnoreRequest("Duplicate request found: {}".format(request_url))
            else:
                response = session.get(
                    request_url,
                    headers=headers,
#                    proxy={"ip": ip}
                )
                
        else:
            request_body = request.body
            response = session.post(
                request_url,
                headers=headers,
                json=request_body,
 #               proxy={"ip": ip}
            )

        # Assuming response.content is binary data, convert it to text
        response_text = response.content.decode('utf-8-sig')  # Adjust encoding if needed

        # Create a TextResponse object for Scrapy
        scrapy_response = TextResponse(
            url=response.url,
            body=response_text,
            encoding='utf-8-sig',
        )

        return scrapy_response


if __name__ == '__main__':
    try:
        process = CrawlerProcess()
        process.crawl(Notino)
        process.start()
    
        details_table = pd.read_csv(f"product_details_table_{retailer_locale_name}_{output_date}.csv", 
                            encoding='utf-8-sig', na_filter = False)
        details_table = details_table.sort_values(by=['product_name'])
        details_table.to_csv(f"product_details_table_{retailer_locale_name}_{output_date}.csv", 
                                              encoding='utf-8-sig', mode='w', index=False)
        
        
        list_table = details_table.drop_duplicates(subset='product_url', keep="first")
        list_table = list_table[['retailer_locale', 'retailer_locale_name',
                        'Brand Name', 'product_name', 'product_url',
                        'scrape_date']].rename({'Brand Name': 'brand_name'}, axis=1)
                        
        list_table.to_csv(f"product_list_table_{retailer_locale_name}_{output_date}.csv", 
                     encoding='utf-8-sig', index=False)
    except Exception as e:
        print(f"There was an error running the script for {retailer_locale_name}: ", e)
    finally:
        current_dir = os.getcwd()
        details_path = f"{current_dir}/product_list_table_{retailer_locale_name}_{output_date}.csv"
        list_path = f"{current_dir}/product_details_table_{retailer_locale_name}_{output_date}.csv"
        logs_path = f"{current_dir}/{logs_name}"
        try:
            upload_to_s3(details_path, f"{retailer_locale_name}/details_{output_date}.csv")
            upload_to_s3(list_path, f"{retailer_locale_name}/list_{output_date}.csv")
            upload_to_s3(logs_path, f"{retailer_locale_name}/{logs_name}")
        except:
            pass
        finally:
            os.remove(details_path)
            os.remove(list_path)
            os.remove(logs_name)