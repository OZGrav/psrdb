import json
import logging
import copy
import requests as r
from requests.packages.urllib3.util.retry import Retry

log = logging.getLogger(__name__)


class GraphQLClient:
    """Provides a HTTP client connection to the GraphQL endpoint"""

    def __init__(self, url, verbose):
        """Initialise GraphQL connection for the url."""
        self.graphql_url = url
        self.connect(verbose)

    def connect(self, verbose):
        """Connect to the GraphQL URL."""
        if verbose:
            try:
                import http.client as http_client
            except ImportError:
                # Python 2
                import httplib as http_client
            http_client.HTTPConnection.debuglevel = 1

        if self.graphql_url is None:
            raise RuntimeError("GraphQL URL is required")

        retry_strategy = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        adapter = r.adapters.HTTPAdapter(max_retries=retry_strategy)

        self.graphql_session = r.Session()
        self.graphql_session.mount(self.graphql_url, adapter)

    def handle_error_msg(self, content):
        """Handle logging of error messages in GraphQL response."""
        if "errors" in content.keys():
            message = None
            if "message" in content["errors"][0]:
                message = content["errors"][0]["message"]
            logging.error(f"Error: {message}")

    def post(self, url, payload, **header):
        """Post the payload and header to the GraphQL URL."""
        logging.debug(f"Using url: {url}")
        logging.debug(f"Using payload: {payload}")
        header_log = copy.deepcopy(header)
        if "Authorization" in header.keys():
            if "JWT" in header_log["Authorization"]:
                header_log["Authorization"] = "JWT [redacted]"
        logging.debug(f"Using header: {header_log}")
        response = self.graphql_session.post(url, headers=header, json=payload, timeout=(15, 15))
        content = json.loads(response.content)

        if response.status_code != 200:
            logging.error(f"GraphQL response.status_code != {response.status_code}")
            self.handle_error_msg(content)
        elif "errors" in content.keys():
            logging.error("GraphQL error")
            self.handle_error_msg(content)
        else:
            logging.debug("Success")
        return response
