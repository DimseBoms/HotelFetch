import asyncio
import json
import re
import os
import datetime
import traceback
from collections import defaultdict
from tkinter import W
from tokenize import String
from typing import Dict, List, TypedDict
from urllib.parse import urlencode
from requests.structures import CaseInsensitiveDict
from scrapfly import ScrapeApiResponse, ScrapeConfig, ScrapflyClient
from dotenv import load_dotenv
from pprint import pprint
from pandas import *


def create_search_page_url(
    query,
    checkin: str = "",
    checkout: str = "",
    number_of_rooms=1,
    offset: int = 0,
):
    """scrapes a single hotel search page of booking.com"""
    checkin_year, checking_month, checking_day = checkin.split("-") if checkin else "", "", ""
    checkout_year, checkout_month, checkout_day = checkout.split("-") if checkout else "", "", ""

    url = "https://www.booking.com/searchresults.html"
    url += "?" + urlencode(
        {
            "ss": query,
            "checkin_year": checkin_year,
            "checkin_month": checking_month,
            "checkin_monthday": checking_day,
            "checkout_year": checkout_year,
            "checkout_month": checkout_month,
            "checkout_monthday": checkout_day,
            "no_rooms": number_of_rooms,
            "offset": offset,
        }
    )
    return url


def parse_search_total_results(result: ScrapeApiResponse) -> int:
    """parse total number of results from search page HTML"""
    # parse total amount of pages from heading1 text:
    # e.g. "London: 1,232 properties found"
    total_results = int(result.selector.css("h1").re("([\d,]+) properties found")[0].replace(",", ""))
    if total_results > 50:
        return 50 # limits number of hotels per city
    return total_results


class HotelPreview(TypedDict):
    """type hint for hotel preview storage (extracted from search page)"""

    name: str
    location: str
    score: str
    review_count: str
    stars: str
    image: str


def parse_search_page(result: ScrapeApiResponse) -> Dict[str, HotelPreview]:
    """parse hotel preview data from search page HTML"""
    hotel_previews = {}
    for hotel_box in result.selector.xpath('//div[@data-testid="property-card"]'):
        url = hotel_box.xpath('.//h3/a[@data-testid="title-link"]/@href').get("").split("?")[0]
        hotel_previews[url] = {
            "name": hotel_box.xpath('.//h3/a[@data-testid="title-link"]/div/text()').get(""),
            "location": hotel_box.xpath('.//span[@data-testid="address"]/text()').get(""),
            "score": hotel_box.xpath('.//div[@data-testid="review-score"]/div/text()').get(""),
            "review_count": hotel_box.xpath('.//div[@data-testid="review-score"]/div[2]/div[2]/text()').get(""),
            "stars": len(hotel_box.xpath('.//div[@data-testid="rating-stars"]/span').getall()),
            "image": hotel_box.xpath('.//img[@data-testid="image"]/@src').get(),
        }
    return hotel_previews


async def scrape_search(
    query,
    session: ScrapflyClient,
    checkin: str = "",
    checkout: str = "",
    number_of_rooms=1,
):
    """scrape all hotel previews from a given search query"""
    first_page_url = create_search_page_url(
        query=query, checkin=checkin, checkout=checkout, number_of_rooms=number_of_rooms
    )
    first_page = await session.async_scrape(ScrapeConfig(url=first_page_url, country="US"))
    total_results = parse_search_total_results(first_page)
    hotel_previews = parse_search_page(first_page)
    other_page_urls = [
        create_search_page_url(
            query=query,
            checkin=checkin,
            checkout=checkout,
            number_of_rooms=number_of_rooms,
            offset=offset,
        )
        for offset in range(25, total_results, 25)
    ]
    async for result in session.concurrent_scrape([ScrapeConfig(url, country="US") for url in other_page_urls]):
        hotel_previews.update(parse_search_page(result))
    return hotel_previews


class Hotel(TypedDict):
    """type hint for hotel data storage"""

    title: str
    description: str
    address: str
    lat: str
    lng: str
    features: dict
    id: str
    url: str
    price: dict


def parse_hotel(result: ScrapeApiResponse) -> Hotel:
    """parse hotel page for hotel information (no pricing or reviews)"""
    css = lambda selector, sep="": sep.join(result.selector.css(selector).getall()).strip()
    css_first = lambda selector: result.selector.css(selector).get("")
    lat, lng = css_first(".show_map_hp_link::attr(data-atlas-latlng)").split(",")
    features = defaultdict(list)
    #title_class_name = re.findall(r"[A-Za-z0-9]+ pp-header__title", result.content)[0]
    #print(title_class_name)
    for feat_box in result.selector.css(".hotel-facilities-group"):
        type_ = "".join(feat_box.css(".bui-title__text::text").getall()).strip()
        features[type_].extend([f.strip() for f in feat_box.css(".bui-list__description::text").getall() if f.strip()])
    data = {
        "title": css(f"h2.pp-header__title::text"),
        "description": css("div#property_description_content ::text", "\n"),
        "address": css(".hp_address_subtitle::text"),
        "lat": lat,
        "lng": lng,
        "features": dict(features),
        "id": re.findall(r"b_hotel_id:\s*'(.+?)'", result.content)[0],
    }
    return data


async def scrape_hotels(urls: List[str], session: ScrapflyClient, price_start_dt: str, price_n_days=30) -> List[Hotel]:
    """scrape list of hotel urls with pricing details"""

    async def scrape_hotel(url: str) -> Hotel:
        url += "?" + urlencode({"cur_currency": "usd"})
        _scrapfly_session = ""
        result_hotel = await session.async_scrape(ScrapeConfig(url, country="US"))
        pprint(url)
        hotel = parse_hotel(result_hotel)
        hotel["url"] = str(result_hotel.context["url"])

        # for background requests we need to find some secret tokens:
        csrf_token = re.findall(r"b_csrf_token:\s*'(.+?)'", result_hotel.content)[0]
        aid = re.findall(r"b_aid:\s*'(.+?)'", result_hotel.content)[0]
        sid = re.findall(r"b_sid:\s*'(.+?)'", result_hotel.content)[0]
        price_calendar_form = {
            "name": "hotel.availability_calendar",
            "result_format": "price_histogram",
            "hotel_id": hotel["id"],
            "search_config": json.dumps(
                {
                    # we can adjust pricing configuration here but this is the default
                    "b_adults_total": 2,
                    "b_nr_rooms_needed": 1,
                    "b_children_total": 0,
                    "b_children_ages_total": [],
                    "b_is_group_search": 0,
                    "b_pets_total": 0,
                    "b_rooms": [{"b_adults": 2, "b_room_order": 1}],
                }
            ),
            "checkin": price_start_dt,
            "n_days": price_n_days,
            "respect_min_los_restriction": 1,
            "los": 1,
        }
        result_price = await session.async_scrape(
            ScrapeConfig(
                url="https://www.booking.com/fragment.json?cur_currency=usd",
                method="POST",
                data=price_calendar_form,
                # we need to use cookies we received from hotel scrape to access background requests like this one
                cookies=CaseInsensitiveDict({v['name']: v['value'] for v in result_hotel.scrape_result['cookies']}),
                headers={
                    "X-Booking-CSRF": csrf_token,
                    "X-Requested-With": "XMLHttpRequest",
                    "X-Booking-AID": aid,
                    "X-Booking-Session-Id": sid,
                },
                country="US",
            )
        )
        hotel["price"] = json.loads(result_price.content)["data"]
        return hotel

    hotels = await asyncio.gather(*[scrape_hotel(url) for url in urls])
    return hotels


class Review(TypedDict):
    """type hint for review information storage"""

    id: str
    score: str
    title: str
    date: str
    user_name: str
    user_country: str
    text: str
    lang: str


def parse_reviews(result: ScrapeApiResponse) -> List[Review]:
    """parse review page for review data"""
    parsed = []
    for review_box in result.selector.css(".review_list_new_item_block"):
        get_css = lambda css: review_box.css(css).get("").strip()
        parsed.append(
            {
                "id": review_box.xpath("@data-review-url").get(),
                "score": get_css(".bui-review-score__badge::text"),
                "title": get_css(".c-review-block__title::text"),
                "date": get_css(".c-review-block__date::text"),
                "user_name": get_css(".bui-avatar-block__title::text"),
                "user_country": get_css(".bui-avatar-block__subtitle::text"),
                "text": "".join(review_box.css(".c-review__body ::text").getall()),
                "lang": review_box.css(".c-review__body::attr(lang)").get(),
            }
        )
    return parsed


async def scrape_reviews(hotel_id: str, session: ScrapflyClient) -> List[dict]:
    """scrape all reviews of a hotel"""

    def create_review_url(page, page_size=25):  # 25 is largest possible page size for this endpoint
        """create url for specific page of hotel review pagination"""
        return "https://www.booking.com/reviewlist.html?" + urlencode(
            {
                "type": "total",
                "lang": "en-us",
                "sort": "f_recent_desc",
                "cc1": "gb",
                "dist": 1,
                "pagename": hotel_id,
                "rows": page_size,
                "offset": page * page_size,
            }
        )

    first_page = await session.async_scrape(ScrapeConfig(url=create_review_url(1), country="US"))
    total_pages = first_page.selector.css(".bui-pagination__link::attr(data-page-number)").getall()
    total_pages = max(int(page) for page in total_pages)
    other_page_urls = [create_review_url(page) for page in range(2, total_pages + 1)]
    reviews = parse_reviews(first_page)
    async for result in session.concurrent_scrape([ScrapeConfig(url, country="US") for url in other_page_urls]):
        reviews.extend(parse_reviews(result))
    return reviews


# populates hotel listings for a given search
async def fetch_listings(query_str: str):
    hotel_listings = await scrape_search(query_str, session)
    return hotel_listings


# Collects information about hotel listings found with a given query.
# Since ScrapFly has a bug where it cannot keep track of scraping sessions
# within the `scrape_hotels()` method. This process has to be done manually,
# and as seen below the limit is 5 scraping instances at one time. If I try
# to scrape with more than 5 instances at one given time with this method
# the scraping returns empty objects and crashes the program.
async def drill_listings(_hotel_listings):

    # splits urls in parts of 5 and inserts them in a meta list
    def create_nested_list(urls_list):
        chunk_size = 5
        return [urls_list[i:i+chunk_size] for i in range(0, len(urls_list), chunk_size)]

    # scrapes hotels, but takes only five at a time
    async def drill_next_five(_urls_dict):
        result_hotels = await scrape_hotels(
        urls=_urls_dict,
        session=session,
        price_start_dt=datetime.date.today().strftime('%Y-%m-%d'), # start date: checkin today
        price_n_days=7, # how many nights to stay
        )
        return result_hotels

    # extract URLs
    urls_list = []
    for url, data in _hotel_listings.items():
        urls_list.append(url)

    # gathers data
    chunked_lists = create_nested_list(urls_list)
    drill_results = []
    for nested_list in chunked_lists:
        drill_results.append(await drill_next_five(nested_list))

    # reorganizes result into one list instead of sublists
    result = []
    for drill_result in drill_results:
        for listing in drill_result:
            result.append(listing)

    return result


def read_worldcities():
    # reading CSV file
    data = read_csv("./data/filtered_worldcities.csv")
    
    # converting column data to list
    cities = data['city'].tolist()
    # city_ascii = data['city_ascii'].tolist()
    # lat = data['lat'].tolist()
    # lng = data['lng'].tolist()
    countries = data['country'].tolist()
    res_dict = {}
    #TODO: Make Dict of country: [cities]
    # creates key -> value structure with {country: [cities]}
    for index in data.index:
        #print(data['city_ascii'][index], data['country'][index])
        if (data['country'][index] in res_dict.keys()): # if country key already exists in dict and has sublist
            res_dict[data['country'][index]].append(data['city_ascii'][index]) # append city to list
        else:
            res_dict[data['country'][index]] = [data['city_ascii'][index]] # if not then create new list and insert the city

    return res_dict


# first search to find hotel listings and their urls
# TODO: add memory function to remember the last scraped city so the scraper
#       is able to continue from the last added city.
async def run():
    first_run = True
    filename = f"./data/results/result_{datetime.datetime.now()}.json"
    country_cities_dict = read_worldcities()
    final_result = {}
    for country in country_cities_dict.keys():
        final_result[country] = {} # creates empty dict to add items to
        for city in country_cities_dict[country]:
            try:
                hotel_listings = await fetch_listings(city) # starts fetching hotel listings for current city
                result_hotels = await drill_listings(hotel_listings) # extracts information about listings
                final_result[country][city] = [] # creates empty list to append items to
                i = 0
                for hotel_data in result_hotels: # appends data from current city to the final result
                    final_result[country][city].append(hotel_data)
                    with open(filename, "w") as f:
                        json.dump(final_result, f, indent=4)
                    i+=1
                print(f"fetched {i} hotels from {city} in {country}")
            except Exception as e:
                print(f"no hotels found in {city} in {country}")
                #traceback.print_exc()
        with open("./data/results/final_result.json", "w") as f:
                json.dump(final_result, f, indent=4)




if __name__ == "__main__":
    # loads environment variables and initializes ScrapFly session
    load_dotenv()
    with ScrapflyClient(key=os.getenv("SCRAPFLY_API_KEY"), max_concurrency=5) as session:
        asyncio.run(run()) # initializes scraping
