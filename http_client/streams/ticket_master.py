from requests import Response, utils
from urllib import parse
from ..http_handler import HTTPHandler


class TicketMaster(HTTPHandler):
    def __init__(self, **kwargs):
        self.config = kwargs.get("config")
        self.config["url"] = "https://app.ticketmaster.com/discovery/v2"
        super().__init__(config=self.config)

    def get_data(self, response):
        return {}

    def get_headers(self, response: Response) -> dict[str, str]:
        return {}

    def get_params(self, response: Response) -> dict[str, str]:

        startDateTime: str = self.config["startDateTime"]

        json_response: dict = {}
        params: dict = {}
        parts: dict = {}
        next_page: int =  0

        if response:
            
            json_response = response.json()
            links = json_response.get("_links")
        
            if links and links.get("next"):
                href = links.get("next").get("href")
                parts = dict(parse.parse_qsl(parse.urlsplit(href).query))

                startDateTime = parts.get("startDateTime")
                next_page = parts.get("page")

                if next_page == '5':
                    next_page = 0

                    events = json_response.get("_embedded", {}).get("events", [])

                    latest_events = events[-20:]

                    last_valid_event = next(
                        (
                            e for e in reversed(latest_events)
                            if e is not None
                            and e.get("dates", {}).get("start", {}).get("dateTime")
                        ),
                        None
                    )

                    if not last_valid_event:
                        raise ValueError("All of the last 10 events have no StartDateTime. Aborting process.")
                    
                    startDateTime = (
                        last_valid_event
                            .get("dates")
                            .get("start")
                            .get("dateTime")
                    )  

        params = {
            "size": 200,
            "sort": "date,asc",
            "startDateTime": startDateTime,
            "apikey": self.config.get("apiKey"),
            "page": next_page
        }

        return params
    
    def is_end_of_stream(self, response) -> bool:
        json_response = response.json()
        links = json_response.get("_links")
        
        if links and links.get("next"):
            return False
        
        return True

    def get_response_lenght(self, response) -> int:
        json_response = response.json()
        events = json_response.get("_embedded", {}).get("events", [])
        return len(events)
    
    def stream_bookmark(self, response):
        json_response = response.json()
        events = json_response["_embedded"]["events"]

        last_event = events[-1] if events else None

        return (
                    last_event.get("dates", {})
                    .get("start", {})
                    .get("dateTime")
                )

class EventsStream(TicketMaster):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def path(self) -> str:
        return "/events.json"


class VenuesStream(TicketMaster):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def path(self) -> str:
        return "/venues.json"


class AttractionsStream(TicketMaster):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def path(self) -> str:
        return "/attractions.json"