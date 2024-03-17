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

parent_directory = os.path.dirname(os.getcwd())
load_dotenv(parent_directory)

aws_key = os.getenv("aws_key")
aws_secret = os.getenv("aws_secret")

today = datetime.today()
date = today.strftime("%Y-%m-%d")
output_date = today.strftime("%Y%m%d")

retailer_locale = 'Matas'
retailer_locale_name = 'Matas DK'
logs_name = "matas-logs.txt"


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



class Matas(scrapy.Spider):
   name = "matas"

   base_url = "https://www.matas.dk"

   headers = {
      'authority': 'www.matas.dk',
      'accept': 'text/html',
      'accept-language': 'en-US,en;q=0.9,ar;q=0.8,en-AU;q=0.7,en-GB;q=0.6,it;q=0.5',
      'cache-control': 'no-cache',
      'content-length': '0',
      'content-type': 'text/html',
      'origin': 'https://www.matas.dk',
      'pragma': 'no-cache',
      'referer': None,
      'sec-ch-ua': '"Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
      'sec-ch-ua-mobile': '?0',
      'sec-ch-ua-platform': '"Windows"',
      'sec-fetch-dest': 'empty',
      'sec-fetch-mode': 'cors',
      'sec-fetch-site': 'same-origin',
      'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
      'x-requested-with': 'XMLHttpRequest'
    }

   custom_settings = {
    "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
    "DOWNLOAD_DELAY": 2,
    "ROTATING_PROXY_PAGE_RETRY_TIMES": 10, 
    "ROBOTSTXT_OBEY": False,
    'RETRY_TIMES': 6,
    'RETRY_ENABLED': True,
    'DOWNLOAD_FAIL_ON_DATALOSS': False,
    'DOWNLOAD_TIMEOUT': 9,
    'LOG_FILE': logs_name,
   }
    
   final_created = False

   def start_requests(self):
        urls = ["https://www.matas.dk/hudpleje", "https://www.matas.dk/dufte",
                "https://www.matas.dk/makeup", "https://www.matas.dk/haar-styling"]
        for url in urls:
            querystring = {"sort":"name_asc", "pagesize": 36, "page": 1}
            url = url + "?" + urlencode(querystring)
            yield scrapy.Request(url=url,headers=self.headers, 
                                 callback=self.parse_products, meta={"page": querystring['page']})
            break
      
   def parse_products(self, response):
        page_number = response.meta.get("page")
        products_container = response.css("div[class^=ProductList__Grid-sc]")
        products = products_container.css("a[class^='ProductItem__AnchorOverlay-sc-1wmia3i-1']")
        for p in products[0:3]:
            url = self.base_url + p.css("::attr(href)").get()
            yield scrapy.Request(url=url, headers=self.headers, callback=self.parse_details,)

        # next_page = response.css("a[data-js-tracking-value='next'].paging__link--hidden")
        # if not next_page:
        #     querystring = {"sort":"name_asc", "pagesize": 180, "page": page_number+1}
        #     url = response.request.url.split("?")[0] + "?" + urlencode(querystring)
        #     yield scrapy.Request(url=url,headers=self.headers, 
        #                          callback=self.parse_products, meta={"page": querystring['page']})

   def get_size(self, text):
        size = ""
        find_size = re.search(r"\d+[.,]?\d*\s?(ml(\s|$)|fl\.\s?oz|g(\s|$)|oz\.|kg(\s|$)|stk($|\s|\.))", text, flags=re.IGNORECASE)
        if find_size:
            size = find_size[0]
        return size
 
   def parse_details(self, response):
        variants = response.meta.get("variants")
        gotten = response.meta.get("gotten")

        try:
            brand_name = response.css("div[class*=PageTitleNameRenderer__Brand] *::text").get().strip()
            product_name = brand_name + " " + " ".join(list(filter(None, [str(d).strip() for d in 
                                            response.css(".PageTitleNameRenderer__Title-sc-ohshxm-2 *::text").extract()])))
        except:
            try:
                product_name = " ".join(list(filter(None, [str(d).strip() for d in 
                                        response.css(".PageTitleNameRenderer__Title-sc-ohshxm-2 *::text").extract()])))
            except:
                product_name = "N/A"
            brand_name = "N/A"        

        desc = []
        ingreds = "N/A"
        product_size = ""
        for i in response.css("div[class^='ProductAccordionItem__Accordion']"):
            title = i.css("p[class^='Text__TextElement-sc'] *::text").get()
            if title == "Ingredienser":
                ingreds = " ".join(list(filter(None, [str(d).strip() for d in 
                              i.css("div[class^='ProductAccordionItem__ContentContainer'] *::text").extract()])))
                ingreds = re.sub("OBS\:.+$", "", ingreds)
                ingreds = re.sub("Produktets Ingrediensliste Kan Blive Opdateret Over Tid. Tjek Altid Ingredienslisten Som Findes På Emballagen På Det Købte Produkt.", "", ingreds)
            elif title == "Specifikationer":
                table = i.css("table tr")
                #print(table)
                for tr in table:
                    td = tr.css("td")
                    if len(td) == 2:
                        title = td[0].css("*::text").get()
                        value = td[1].css("*::text").get()
                        if title == "Indhold":
                            product_size = value
                        desc.append(f"{title}: {value}")
            elif title == "Betaling" or title == "Levering & returnering":
                pass
            else:
                d = "\n ".join(list(filter(None, [str(d).strip() for d in 
                        i.css("div[class^='TabBlock__Wrapper-sc'] *::text").extract()])))
                desc.append(d.replace('Læs mere', ''))
                table = i.css("table.Table__StyledTable-sc-t1jnuj-1.dcwziJ tr")
                for tr in table:
                    td = tr.css("td")
                    if len(td) == 2:
                        title = td[0].css("*::text").get()
                        value = td[1].css("*::text").get()
                        desc.append(f"{title}: {value}")

        rating = response.css("div#anchorReviews div[class^='ratingOverview__AverageRating-sc'] *::text").get()
        if not rating:
            rating = "N/A"

        try:
            num_reviews = re.search("\d+", response.css("div[class^='ratingOverview__BasedOn-sc'] *::text").get())
            num_reviews = num_reviews[0]
        except:
            num_reviews = "N/A"

        try:
            breadcrumbs = " > ".join([i.css("*::text").get().replace("\xa0", "") for i in response.css("div[class^=Breadcrumbs__Items-sc] > a")])
        except:
            breadcrumbs = "N/A"

        sold_out = sold_out = "N" if "Læg i kurv" in response.css(".PDPProductActionRenderer__StyledProductActionRenderer-sc-1puad6u-0 *::text").get() else "Y"

        tags = ", ".join([i.css("*::text").get() for i in response.css("div[class^='Highlights__HighlightContainer-sc'] div[class^='Tag__Wrapper-sc']")])

        try:
            sale_price = response.css("span[class*=HorizontalPriceModule__StyledPriceBefore] *::text").get()
        except:
            sale_price = ""
        if sale_price:
            sale_price = sale_price.strip()
            sale = "Y"
            price = response.css("span[class*=HorizontalPriceModule__StyledPrice-sc-93lexh-0] *::text").get()
            
            price, sale_price = sale_price, price
        else:
            sale = "N"
            sale_price = "N/A"
            price_q = response.css("span[class*=HorizontalPriceModule__StyledPrice-sc-93lexh-0] *::text").get()
            if price_q:
                price = price_q.strip()
            else:
                price = ""
            if not price:
                try:
                    price = " ".join(list(filter(None, [str(d).strip() for d in 
                                                                response.css("div[class^=Price-sc]")[0].css("*::text").extract()])))
                except:
                    price = ""

        if not product_size:
            product_size = self.get_size(product_name)

        variant_name_q = response.css("span.selection__current-text")
        if variant_name_q:
            variant_name = variant_name_q[0].css("*::text").get()
            if self.get_size(variant_name):
                variant_name = "N/A"
        else:
            variant_name = "N/A"
        final = []
        item = {
            "retailer_locale": retailer_locale,
            "retailer_locale_name": retailer_locale_name,
            "Brand Name": brand_name if brand_name else "N/A", 
            "product_name": product_name if product_name else "N/A",
            "product_url": response.request.url,
            "Descriptions": "\n ".join(desc) if desc else "N/A",
            "Full Ingredients": ingreds,
            "Key Ingredients": "N/A",
            "Price": re.sub("stk\.?", "", price.replace("kr.", "").replace(".", "").replace(",", ".").replace("Medlemspris", "")).strip() if price else "N/A",
            "Sale": sale,
            "Sale Price": re.sub("stk\.?", "", sale_price.replace("kr.", "").replace(".", "").replace(",", ".").replace("Medlemspris", "")).strip() if sale_price != "N/A" else sale_price,
            "Sold Out?": sold_out,
            "Product Size": product_size.replace(".", "").replace(",", ".") if product_size else "N/A",
            "Variant Name": variant_name,
            "num_reviews": num_reviews if num_reviews else "N/A",
            "star_rating": rating if rating else "N/A",
            "tags": tags if tags else "N/A",
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

        if not variants and not gotten:
            variants = []
            variants_q = response.css("div[data-js='productPage:variants:truncate'] ul.product-page__variants-list") or response.css("ul[class^=PickColorVariant__SwatchesListElement-sc]")

            if variants_q:
                for i in variants_q[0].css("li"):
                    value = i.css("a::attr(href)").get()
                    if value not in response.request.url:
                        variants.append(value)
        if variants:
            var = variants.pop()
            yield scrapy.Request(url=self.base_url + var, headers=self.headers, callback=self.parse_details,
                                 meta={"variants": variants, "gotten": True})



if __name__ == '__main__':
    try:
        process = CrawlerProcess()
        process.crawl(Matas)
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
        details_path = f"{current_dir}/product_list_table_{retailer_locale_name}_{output_date}.parquet"
        list_path = f"{current_dir}/product_details_table_{retailer_locale_name}_{output_date}.parquet"
        
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