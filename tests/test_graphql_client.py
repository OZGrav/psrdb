import json
import logging
import pytest
import responses
from unittest.mock import Mock, patch, MagicMock

from psrdb.graphql_client import GraphQLClient


class TestGraphQLClient:
    """Test suite for the GraphQLClient class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.test_url = "https://test.example.com"
        self.test_token = "test_token_123"
        self.test_graphql_url = f"{self.test_url}/graphql/"
        self.test_rest_api_url = f"{self.test_url}/upload/"

    def test_init_basic(self):
        """Test basic initialization of GraphQLClient."""
        with patch.object(GraphQLClient, 'connect') as mock_connect:
            client = GraphQLClient(self.test_url, self.test_token)
            
            assert client.graphql_url == self.test_graphql_url
            assert client.rest_api_url == self.test_rest_api_url
            assert client.token == self.test_token
            assert client.header == {"Authorization": f"Bearer {self.test_token}"}
            mock_connect.assert_called_once_with(False)

    def test_init_with_verbose(self):
        """Test initialization with verbose mode enabled."""
        with patch.object(GraphQLClient, 'connect') as mock_connect:
            client = GraphQLClient(self.test_url, self.test_token, verbose=True)
            mock_connect.assert_called_once_with(True)

    def test_init_with_custom_logger(self):
        """Test initialization with custom logger."""
        custom_logger = Mock(spec=logging.Logger)
        with patch.object(GraphQLClient, 'connect'):
            client = GraphQLClient(self.test_url, self.test_token, logger=custom_logger)
            assert client.logger == custom_logger

    def test_init_with_default_logger(self):
        """Test initialization creates default logger when none provided."""
        with patch.object(GraphQLClient, 'connect'):
            with patch('logging.getLogger') as mock_get_logger:
                client = GraphQLClient(self.test_url, self.test_token)
                mock_get_logger.assert_called_once_with('psrdb.graphql_client')

    @patch('psrdb.graphql_client.r.Session')
    @patch('psrdb.graphql_client.r.adapters.HTTPAdapter')
    def test_connect_basic(self, mock_http_adapter, mock_session):
        """Test basic connection setup."""
        mock_session_instance = Mock()
        mock_session.return_value = mock_session_instance
        mock_adapter_instance = Mock()
        mock_http_adapter.return_value = mock_adapter_instance

        with patch.object(GraphQLClient, '__init__', lambda x, y, z, **kwargs: None):
            client = GraphQLClient.__new__(GraphQLClient)
            client.graphql_url = self.test_graphql_url
            client.connect(False)

            mock_session.assert_called_once()
            mock_session_instance.mount.assert_called_once_with(self.test_graphql_url, mock_adapter_instance)
            assert client.graphql_session == mock_session_instance

    @patch('psrdb.graphql_client.r.Session')
    def test_connect_with_verbose(self, mock_session):
        """Test connection with verbose mode enabling debug logging."""
        with patch('http.client.HTTPConnection') as mock_http_connection:
            with patch.object(GraphQLClient, '__init__', lambda x, y, z, **kwargs: None):
                client = GraphQLClient.__new__(GraphQLClient)
                client.graphql_url = self.test_graphql_url
                client.connect(True)

                assert mock_http_connection.debuglevel == 1

    def test_connect_no_url_raises_error(self):
        """Test that connect raises RuntimeError when graphql_url is None."""
        with patch.object(GraphQLClient, '__init__', lambda x, y, z, **kwargs: None):
            client = GraphQLClient.__new__(GraphQLClient)
            client.graphql_url = None

            with pytest.raises(RuntimeError, match="GraphQL URL is required"):
                client.connect(False)

    def test_handle_error_msg_with_message(self):
        """Test error message handling when message is present."""
        mock_logger = Mock()
        with patch.object(GraphQLClient, '__init__', lambda x, y, z, **kwargs: None):
            client = GraphQLClient.__new__(GraphQLClient)
            client.logger = mock_logger

            content = {
                "errors": [
                    {"message": "Test error message"}
                ]
            }
            client.handle_error_msg(content)
            mock_logger.error.assert_called_once_with("Error: Test error message")

    def test_handle_error_msg_without_message(self):
        """Test error message handling when message is not present."""
        mock_logger = Mock()
        with patch.object(GraphQLClient, '__init__', lambda x, y, z, **kwargs: None):
            client = GraphQLClient.__new__(GraphQLClient)
            client.logger = mock_logger

            content = {
                "errors": [
                    {"code": "SOME_ERROR"}
                ]
            }
            client.handle_error_msg(content)
            mock_logger.error.assert_called_once_with("Error: None")

    def test_handle_error_msg_no_errors(self):
        """Test error message handling when no errors are present."""
        mock_logger = Mock()
        with patch.object(GraphQLClient, '__init__', lambda x, y, z, **kwargs: None):
            client = GraphQLClient.__new__(GraphQLClient)
            client.logger = mock_logger

            content = {"data": "some_data"}
            client.handle_error_msg(content)
            mock_logger.error.assert_not_called()

    @responses.activate
    def test_post_success(self):
        """Test successful POST request."""
        # Setup mock response
        response_data = {"data": {"pulsar": {"id": "1"}}}
        responses.add(
            responses.POST,
            self.test_graphql_url,
            json=response_data,
            status=200
        )

        mock_logger = Mock()
        with patch.object(GraphQLClient, '__init__', lambda x, y, z, **kwargs: None):
            client = GraphQLClient.__new__(GraphQLClient)
            client.graphql_url = self.test_graphql_url
            client.header = {"Authorization": f"Bearer {self.test_token}"}
            client.logger = mock_logger
            client.graphql_session = Mock()
            
            # Mock the session.post response
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.content = json.dumps(response_data)
            client.graphql_session.post.return_value = mock_response

            payload = {"query": "{ pulsar { id } }"}
            response = client.post(payload)

            assert response.status_code == 200
            client.graphql_session.post.assert_called_once_with(
                self.test_graphql_url,
                headers={"Authorization": f"Bearer {self.test_token}"},
                json=payload,
                timeout=(60, 60)
            )
            mock_logger.debug.assert_called_with("Success")

    def test_post_http_error(self):
        """Test POST request with HTTP error status code."""
        mock_logger = Mock()
        with patch.object(GraphQLClient, '__init__', lambda x, y, z, **kwargs: None):
            client = GraphQLClient.__new__(GraphQLClient)
            client.graphql_url = self.test_graphql_url
            client.header = {"Authorization": f"Bearer {self.test_token}"}
            client.logger = mock_logger
            client.graphql_session = Mock()
            
            # Mock error response
            error_data = {"errors": [{"message": "Server error"}]}
            mock_response = Mock()
            mock_response.status_code = 500
            mock_response.content = json.dumps(error_data)
            client.graphql_session.post.return_value = mock_response

            with patch.object(client, 'handle_error_msg') as mock_handle_error:
                payload = {"query": "{ pulsar { id } }"}
                response = client.post(payload)

                assert response.status_code == 500
                mock_logger.error.assert_called_with("GraphQL response.status_code != 500")
                mock_handle_error.assert_called_once_with(error_data)

    def test_post_graphql_error(self):
        """Test POST request with GraphQL errors in response."""
        mock_logger = Mock()
        with patch.object(GraphQLClient, '__init__', lambda x, y, z, **kwargs: None):
            client = GraphQLClient.__new__(GraphQLClient)
            client.graphql_url = self.test_graphql_url
            client.header = {"Authorization": f"Bearer {self.test_token}"}
            client.logger = mock_logger
            client.graphql_session = Mock()
            
            # Mock GraphQL error response
            error_data = {"errors": [{"message": "GraphQL syntax error"}]}
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.content = json.dumps(error_data)
            client.graphql_session.post.return_value = mock_response

            with patch.object(client, 'handle_error_msg') as mock_handle_error:
                payload = {"query": "invalid query"}
                response = client.post(payload)

                assert response.status_code == 200
                mock_logger.error.assert_called_with("GraphQL error")
                mock_handle_error.assert_called_once_with(error_data)

    def test_post_logging_payload_and_headers(self):
        """Test that POST method logs payload and headers correctly."""
        mock_logger = Mock()
        with patch.object(GraphQLClient, '__init__', lambda x, y, z, **kwargs: None):
            client = GraphQLClient.__new__(GraphQLClient)
            client.graphql_url = self.test_graphql_url
            client.header = {"Authorization": f"Bearer {self.test_token}"}
            client.logger = mock_logger
            client.graphql_session = Mock()
            
            # Mock successful response
            response_data = {"data": {"test": "success"}}
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.content = json.dumps(response_data)
            client.graphql_session.post.return_value = mock_response

            payload = {"query": "{ test }"}
            client.post(payload)

            # Check that debug logs were called with correct parameters
            calls = mock_logger.debug.call_args_list
            assert len(calls) >= 3
            
            # Check URL logging
            assert f"Using url: {self.test_graphql_url}" in str(calls[0])
            
            # Check payload logging
            assert "Using payload:" in str(calls[1])
            assert '"query": "{ test }"' in str(calls[1])
            
            # Check header logging (should redact Bearer token)
            assert "Using header:" in str(calls[2])
            assert "Bearer [redacted]" in str(calls[2])
            assert self.test_token not in str(calls[2])

    def test_post_header_redaction(self):
        """Test that Bearer token is properly redacted in logs."""
        mock_logger = Mock()
        with patch.object(GraphQLClient, '__init__', lambda x, y, z, **kwargs: None):
            client = GraphQLClient.__new__(GraphQLClient)
            client.graphql_url = self.test_graphql_url
            client.header = {"Authorization": f"Bearer {self.test_token}"}
            client.logger = mock_logger
            client.graphql_session = Mock()
            
            # Mock response
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.content = json.dumps({"data": {}})
            client.graphql_session.post.return_value = mock_response

            client.post({"query": "test"})

            # Verify that the original header is not modified
            assert client.header["Authorization"] == f"Bearer {self.test_token}"
            
            # Verify that the logged header has redacted token
            debug_calls = [call for call in mock_logger.debug.call_args_list 
                          if "Using header:" in str(call)]
            assert len(debug_calls) == 1
            header_log = str(debug_calls[0])
            assert "Bearer [redacted]" in header_log
            assert self.test_token not in header_log


