import asyncio
import json
import re
import os
import datetime
from collections import defaultdict
from tkinter import W
from tokenize import String
from typing import Dict, List, TypedDict
from urllib.parse import urlencode
from requests.structures import CaseInsensitiveDict

from scrapfly import ScrapeApiResponse, ScrapeConfig, ScrapflyClient
from dotenv import load_dotenv
from pprint import pprint



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
    for feat_box in result.selector.css(".hotel-facilities-group"):
        type_ = "".join(feat_box.css(".bui-title__text::text").getall()).strip()
        features[type_].extend([f.strip() for f in feat_box.css(".bui-list__description::text").getall() if f.strip()])
    data = {
        "title": css("h2#hp_hotel_name::text"),
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


# Example use:
async def run():
    load_dotenv()
    with ScrapflyClient(key=os.getenv("SCRAPFLY_API_KEY"), max_concurrency=10) as session:
        # we can find hotel previews
        result_search = await scrape_search(
            "Oslo",
            session,
            datetime.date.today().strftime('%Y-%m-%d'), # checkin today
            (datetime.date.today() + datetime.timedelta(days=1)).strftime('%Y-%m-%d') # checkout tomorrow
        )

        # # and scrape hotel data itself
        # result_hotels = await scrape_hotels(
        #     urls=["https://www.booking.com/hotel/gb/gardencourthotel.html"],
        #     session=session,
        #     # get pricing data of last 7 days
        #     price_start_dt="2022-05-25",
        #     price_n_days=7,
        # )
        # result_reviews = await scrape_reviews("gardencourthotel", session)
        # return result_search, result_hotels, result_reviews
        return result_search


if __name__ == "__main__":
    resultList = asyncio.run(run())
    with open("./data/results/resultater.json", "w") as f:
        json.dump(resultList, f, indent=4)
