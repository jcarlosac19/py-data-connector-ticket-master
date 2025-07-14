import requests
import logging
from abc import abstractmethod, ABC
from typing import Generator, Any, Union, TypedDict, Optional
import time

_BASE_BACKOFF_TIME = 15000
_MAX_RETRIES = 3

class Configuration(TypedDict):
    url: str

class IHttpClientOptions(TypedDict, total=False):
    params: Any
    data: Any
    headers: Any

class HTTPHandler(ABC):

    _RETRIES_COUNT: int = 0
    _UNRECHABLE_ENDPOINT: bool = False
    
    def __init__(self, config: Configuration):
        self.session = requests.Session()
        self.url = config.get("url")

        self.logger = logging.getLogger("HTTPHandler")

    @property
    def HTTP_Method(self) ->str:
        return "GET"

    @abstractmethod
    def get_headers(self, response: requests.Response) -> Union[str, None]:
        pass

    @abstractmethod
    def get_params(self, response: requests.Response) -> Union[str, None]:
        pass

    @abstractmethod
    def get_data(self, response: requests.Response) -> Union[str, None]:
        pass

    @abstractmethod
    def get_response_lenght(self, response: requests.Response) -> Union[str, None]:
        pass

    @abstractmethod
    def stream_bookmark(self, response: requests.Response, )-> Union[str, None]:
        pass

    @abstractmethod
    def is_end_of_stream(self, response)-> bool:
        pass
        
    @property
    def path(self) -> str:
        return None
    
    def backoff_time(self, response) -> int:
        return _BASE_BACKOFF_TIME + (_BASE_BACKOFF_TIME * self._RETRIES_COUNT)
    
    def should_retry(self, error: Optional[dict]) -> bool:
        if not error:
            return True
        status = error.get("status")
        if status == 429 or (500 <= status <= 599):
            return True
        return False

    def join_url(self) -> str:
        return self.url + self.path()
    
    def sleep(self, ms: int):
        time.sleep(ms / 1000)

    def request_parts(self, options: Optional[IHttpClientOptions]) -> requests.PreparedRequest:
        if self.HTTP_Method not in ("GET", "POST"):
            raise ValueError(f"Invalid HTTP method: {self.method}")
        
        full_url = self.join_url()

        headers = {}

        if options.get("headers"):
            headers.update(options["headers"])
    
        args = {
                "method": self.HTTP_Method,
                "url": full_url, 
                "headers": headers,
                "params": options.get("params"),
                "data": options.get("data")
        }
        return self.session.prepare_request(requests.Request(**args))
    
    def send_request(self, options: dict[str, Any]) -> Any:
        request_config = self.request_parts(options)
        response = self.session.send(request_config)
        return response

    def request_with_retry_operation(self, request_config: dict[str, Any], max_retries: int) -> Optional[requests.Response]:
        retries = max_retries
        response = None

        while retries > 0:
            try:
                return self.send_request(request_config)
            except requests.HTTPError as error:
                response = error.response
                should_retry = self.should_retry(response)
                if should_retry:
                    backoff_time = self.backoff_time(response)
                    self.logger.info(f"Waiting for {backoff_time} ms before next retry.")
                    self.sleep(backoff_time)
                    self.retries_count += 1
                    retries -= 1
                else:
                    break

        if response and (response.status_code < 200 or response.status_code >= 300):
            self._UNRECHABLE_ENDPOINT = True

        self.retries_count = 0
        return response
    
    def fetch_next_page(self, response: requests.Response) -> requests.Response:
        
        params = self.get_params(response)
        headers = self.get_headers(response)
        data = self.get_data(response)

        request_options = {
            "params": params,
            "headers": headers,
            "data": data
        }

        self.logger.info(f"date: {params["startDateTime"]}, page: {params["page"]}")
        
        req_response = self.request_with_retry_operation(request_options, _MAX_RETRIES)
        return req_response 
    
    def read_pages(self) -> Generator[dict[str, Any], None, None]:
        is_end_of_stream = False
        response: Optional[requests.Response] = None

        while not is_end_of_stream:
            response = self.fetch_next_page(response)
            is_end_of_stream = self.is_end_of_stream(response)
            record_count = self.get_response_lenght(response)
            bookmark = self.stream_bookmark(response)

            yield {
                "response": response,
                "isEndOfStream": is_end_of_stream,
                "bookmark": bookmark,
                "record_count": record_count
            }

            if self._UNRECHABLE_ENDPOINT:
                break
    