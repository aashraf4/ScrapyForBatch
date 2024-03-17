import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.selector import Selector
import pandas as pd
from datetime import datetime
import re
import json
from urllib.parse import urlencode
from dotenv import load_dotenv
import os
import boto3

#parent_directory = os.path.dirname(os.getcwd())
load_dotenv()

aws_key = os.getenv("aws_key")
aws_secret = os.getenv("aws_secret")

today = datetime.today()
date = today.strftime("%Y-%m-%d")
output_date = today.strftime("%Y%m%d")

retailer_locale = 'Superdrug'
retailer_locale_name = 'Superdrug UK'
logs_name = "superdrug-logs.txt"


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

class Superdrug(scrapy.Spider):
   name = "superdrug"

   base_url = "https://www.superdrug.com"
   starting_url = "https://www.superdrug.com/a-z-brands"

   headers = {
    }
   custom_settings = {
    "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
    "DOWNLOAD_DELAY": 2,
    "ROBOTSTXT_OBEY": False,
    #    'LOG_LEVEL': 'INFO',
    'RETRY_TIMES': 30,
    'RETRY_ENABLED': True,
    'DOWNLOAD_FAIL_ON_DATALOSS': False,
    'DOWNLOAD_TIMEOUT': 9,
    'LOG_FILE': "superdrug-logs.txt"
   }
    
   final_created = False
   products_list_created = False
   
   def product_details_headers(self):
      headers = {
        'authority': 'api.superdrug.com',
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-US,en;q=0.9,ar;q=0.8,en-AU;q=0.7,en-GB;q=0.6,it;q=0.5',
        'cache-control': 'no-cache',
        'origin': 'https://www.superdrug.com',
        'pragma': 'no-cache',
        'referer': 'https://www.superdrug.com/',
        'sec-ch-ua': '"Chromium";v="106", "Google Chrome";v="106", "Not;A=Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
        'x-anonymous-consents': '%5B%5D'
      }
      return headers

   def brand_page_headers(self):
        headers = {
            'authority': 'api.superdrug.com',
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.9,ar;q=0.8,en-AU;q=0.7,en-GB;q=0.6,it;q=0.5',
            'cache-control': 'no-cache',
            'origin': 'https://www.superdrug.com',
            'pragma': 'no-cache',
            'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        }
        return headers

   def start_requests(self):
      yield scrapy.Request(url=self.starting_url, headers=self.headers, callback=self.parse_brands)

   def parse_brands(self, response):
      links = response.css("a.az-brands__list-letter-group-brand-item-link")
      for index, link in enumerate(links):
            item = {
               "brand_name": link.css("*::text").get().strip(),
               "brand_url": self.base_url + link.css("::attr(href)").get(),
            }
            sku = item['brand_url'].split("/b/")[-1]
            url = "https://api.superdrug.com/api/v2/sd/search"
            querystring = {"fields":"FULL","currentPage":"0","pageSize":"60","categoryCode": sku,"lang":"en_GB","curr":"GBP"}
            yield scrapy.Request(url=url + "?" + urlencode(querystring), 
                                 headers=self.brand_page_headers(), callback=self.parse_page,
                                 meta={"brand_name": item['brand_name'], "page": 0,
                                       "sku": sku, "brand_url": item['brand_url']})
            if index > 2:
                break
            
   def parse_page(self, response):
      json_response = json.loads(response.text)

      brand_name = response.meta.get("brand_name")
      brand_url = response.meta.get("brand_name")
      sku = response.meta.get("sku")
      current_page = int(response.meta.get("page"))
        
      for product in json_response['products'][0:3]:
         code = product['code']
         url = product['url']
         url = f"https://api.superdrug.com/api/v2/sd/products/{code}?fields=FULL&lang=en_GB&curr=GBP"
         yield scrapy.Request(url=url, 
                              headers=self.product_details_headers(), callback=self.parse_product,
                              meta={
                                     "brand_name": brand_name,
                              })

    #   current_page = json_response['pagination']['currentPage']
    #   total_page = json_response['pagination']['totalPages']
    #   print(current_page, total_page)
    #   if total_page != 0 and total_page != current_page + 1:
    #         current_page += 1
    #         url = "https://api.superdrug.com/api/v2/sd/search"
    #         querystring = {"fields":"FULL","pageSize":"60", "currentPage":current_page,"categoryCode": sku,"lang":"en_GB","curr":"GBP"}
    #         yield scrapy.Request(url=url + "?" + urlencode(querystring), 
    #                              headers=self.brand_page_headers(), callback=self.parse_page,
    #                              meta={"brand_name": brand_name, "page": 0,
    #                                   "sku": sku, "brand_url": brand_url})

   def parse_product(self, response):
      brand_name = response.meta.get("brand_name")
      json_response = json.loads(response.text)
      sku = json_response['url'].split("/p/")[-1]

      def calc_size(price, json_unit_price):
            price = float(price.replace("£", ""))
            split_unit = json_unit_price.split("per")
            unit = re.search(r"[^\d+ .]+", split_unit[-1])
            monetary_unit =  re.search(r"[^\d+ .]+", split_unit[0])
            
            if unit:
                unit = unit[0]
                
            unit_price = float(re.search(r"\d+[.]?\d*", split_unit[0])[0]) / float(split_unit[-1].replace(unit, ""))
            amount = price / unit_price
            if monetary_unit and monetary_unit[0].strip() == "p":
                amount = amount * 100
                amount = round(amount)
            else:
                amount = round(amount, 1)
            return str(amount) + unit


      product_name = json_response['name']
      
      product_url = self.base_url + "/p/" + sku

      try:
        price_data = json_response['price']
        try:
          price = price_data['formattedOldValue']
          sale_price = price_data['formattedValue']
        except:
          price = price_data['formattedValue']
          sale_price = ""
      except:
          price = ""
          sale_price = ""

      desc = []
      product_spec = []
      active_ingreds = ""
      ingreds = ""

      product_information = json_response['productInformation']
      short_desc = json_response['shortDescription']
      desc.append(short_desc)
      
      try:
        desc_query = product_information['entry']
      except:
        desc_query = []
      
      for e in desc_query:
          
          key = e['key'].title()
          if "Uses" not in key:
              if "Informativetext1" in key:
                  key = "Tips and Advice"
              if "Informativetext2" in key:
                  key = "Benefit"
              elif "Otherfeatures" in key:
                  key = "Features"
              elif "Recyclinginfo" in key:
                  key = "Recyclable"
              value = re.sub(r"(?<=\/p\/)(.*?\/.*?)\/", "", e['value']).replace(short_desc, "")
              if "Ingredients" in key:
                  if "Active ingredients" in value:
                      active_ingreds = value.replace("Active ingredients:", "").strip()
                  else:
                      ingreds = re.sub(r"\<.*?\>", " ", value).strip()
              else:
                  if "Depth" in key or "Width" in key or "Height" in key:
                      product_spec.append(f"{key}: {value}")
                  else:
                      desc.append(f"{key}\n{value}")
      desc.append(f'Product Specification:\n {", ".join(product_spec)}')
      desc = re.sub(r"\<.*?\>", "", "\n ".join(desc).strip().replace("ABbRtYfFdAU,", ""))


      selected = json_response['baseOptions'][0]['selected']
      sold_out = "N" if selected['stock']['stockLevel'] > 0 else "Y"  
      try:
         num_reviews = json_response['bazaarVoiceReviewSummary']['numberOfReviews']
         rating = json_response['bazaarVoiceReviewSummary']['averageRating']
      except:
         num_reviews = ""
         rating = ""

      product_size = ""
      variant_name = ""
      
      variant_type = selected['variantOptionQualifiers']
      p = sale_price if sale_price else price
      if variant_type:
        for var in variant_type:
            variant_type = var['name']
            if variant_type == "Base Curve":
              find_pattern = re.search(r"(?<=\-)\d+\-?\w+(?=\-?\/p\/)", selected['url'])
              if find_pattern:
                  product_size = find_pattern[0].replace("-", " ")
            if variant_type == "Size":
              if var['value'] != "0EA":
                  try:
                      contentUnitPrice = json_response['contentUnitPrice']
                  except:
                      contentUnitPrice = ""
                  if "each" in contentUnitPrice:
                      product_size = var['value']
                  else:
                      try:
                          te = json_response['contentUnitPrice'].replace("£", "")
                          product_size = calc_size(p, te)
                      except:
                          pass
            elif variant_type == "Color":
              variant_name = selected['variantOptionQualifiers'][0]['value']

      if product_size == "0EA":
          product_size = ""
      def get_size(text):
        size = ""
        find_size = re.search(r"\d+[.]?\d*\s?(ml|fl\.\s?oz|g|oz\.)", text, flags=re.IGNORECASE)
        if find_size:
            size = find_size[0]
        return size

      if not product_size:
        try:
            contentUnitPrice = json_response['contentUnitPrice']
            product_size = calc_size(p, contentUnitPrice)
        except:
            contentUnitPrice = ""
            
      if not product_size:
        size_from_name = get_size(json_response['name'])
        product_size = size_from_name
    
      try:
          breadcrumbs = " > ".join(json_response['categoryNameHierarchy'].split("/")[:-1])
      except:
          breadcrumbs = "N/A"

      final = []
      item = {
           "retailer_locale": retailer_locale,
           "retailer_locale_name": retailer_locale_name,
           "Brand Name": brand_name, 
           "product_name": product_name,
           "product_url": product_url,
           "Descriptions": desc,
           "Full Ingredients": ingreds if ingreds else "N/A",
           "Key Ingredients": active_ingreds if active_ingreds else "N/A",
           "Price": price.replace("£", "").strip() if price else "N/A",
           "Sale": "Y" if sale_price else "N",
           "Sale Price": sale_price.replace("£", "").strip() if sale_price else "N/A",
           "Sold Out?": sold_out,
           "Product Size": product_size if product_size else "N/A",
           "Variant Name": variant_name.replace("#N/A", "N/A") if variant_name else "N/A",
           "num_reviews": num_reviews if num_reviews else "N/A",
           "star_rating": round(rating, 1) if rating else "N/A",
           "tags": "N/A",
           "scrape_date": date,
           "product_category": breadcrumbs,
      }
      final.append(item)

      if self.final_created:
         pd.DataFrame(final).to_csv(f"product_details_table_{retailer_locale_name}_{output_date}.csv", 
                                           encoding='utf-8-sig', mode='a', index=False, header=False)
      else:
         self.final_created = True
         pd.DataFrame(final).to_csv(f"product_details_table_{retailer_locale_name}_{output_date}.csv", 
                               encoding='utf-8-sig', mode='w', index=False)


        
if __name__ == '__main__':
    try:
        process = CrawlerProcess()
        process.crawl(Superdrug)
        process.start()
       
        details_table = pd.read_csv(f"product_details_table_{retailer_locale_name}_{output_date}.csv", 
                            encoding='utf-8-sig', na_filter = False)
        details_table = details_table.sort_values(by=['product_name'])
        details_table.to_parquet(f"product_details_table_{retailer_locale_name}_{output_date}.parquet", engine='auto', 
                                    compression='snappy',
                                    index=False)
        
        list_table = details_table.drop_duplicates(subset='product_url', keep="first")
        list_table = list_table[['retailer_locale', 'retailer_locale_name',
                        'Brand Name', 'product_name', 'product_url',
                        'scrape_date']].rename({'Brand Name': 'brand_name'}, axis=1)
                        
        list_table.to_parquet(f"product_list_table_{retailer_locale_name}_{output_date}.parquet", engine='auto', 
                                    compression='snappy',
                                    index=False)
                
    except Exception as e:
        print(f"There was an error running the script for {retailer_locale_name}: ", e)
    finally:
        current_dir = os.getcwd()
        details_path = f"{current_dir}/product_details_table_{retailer_locale_name}_{output_date}.parquet"
        list_path = f"{current_dir}/product_list_table_{retailer_locale_name}_{output_date}.parquet"
        
        try:
            upload_to_s3(details_path, f"{retailer_locale_name}/details_{output_date}.parquet")
            upload_to_s3(list_path, f"{retailer_locale_name}/list_{output_date}.parquet")
        except:
            pass
        finally:
            os.remove(details_path)
            os.remove(details_path.replace(".parquet", ".csv"))
            os.remove(list_path)
    logs_path = f"{current_dir}/{logs_name}"
    upload_to_s3(logs_path, f"{retailer_locale_name}/{logs_name}")
    os.remove(logs_name)