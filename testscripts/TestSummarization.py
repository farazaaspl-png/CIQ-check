"""
(test_handle_stream_completion - ream Completion Handling: Tests for normal processing flow with mock data, ensuring calls to dependencies like get_latest_sow_request, main, and get_recommendations are made 
(test_handle_db_error) - Database Read Errors: Checks if the handler correctly handles and logs database exceptions, calling send_failure 
test_handle_main_exception - Main Function Exceptions: Verifies error handling when the main processing function raises an exception
test_handle_recommendations_exception - Recommendations Fetch Errors: Ensures proper exception handling when get_recommendations fails during processing
test_handle_non_stream - Non-Stream Event Handling: Checks that the handler gracefully handles event subtypes that are not stream completions, expecting a no-operation
"""

import unittest
from unittest.mock import patch, MagicMock
import sys
import os
import asyncio
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from services.consultant_recommendation.handlers import Consultant_Recommendation
from core.exceptions import CustomBaseException, DatabaseReadError
 
class TestConsultant_Recommendation(unittest.TestCase):
    @patch('services.summarization.handlers.KafkaMessageProducer')
    def setUp(self, mock_kafka_producer_class):
        mock_producer_instance = MagicMock() #Creates a new mock object that simulates a Kafka producer instance with flexible behavior.
        mock_kafka_producer_class.return_value = mock_producer_instance
        self.handler = Consultant_Recommendation(mock_producer_instance, 'output_topic')
        self.handler.send_failure = MagicMock() 


    #    ERROR: recrequestid )= rec.pop('requestid')
    @patch('pandas.read_sql')
    @patch('services.summarization.handlers.DatabaseManager')
    @patch.object(Consultant_Recommendation, 'get_latest_sow_request', new_callable=MagicMock)
    @patch('services.summarization.handlers.main')
    @patch('services.summarization.handlers.get_recommendations')
    def test_handle_stream_completion(self, mock_get_recommendations, mock_main, mock_get_latest_sow_request, mock_db, mock_read_sql):

        header = {
            "eventType": "PROJECT_REQUEST_RECOMMENDATION_ACK",
            "eventSubType": "STREAM_COMPLETION",
            "createdOn": "Fri Oct 31 12:55:00 UTC 2025",
            "requestId": "test-request-id"
        }

        payload = {
            "projectId": "P001",
            "name": "sample_file.docx",
            "daFileId": "DA001"
        }
 
        async def fake_get_latest_sow_request(request_id):
            return header, payload

        mock_get_latest_sow_request.side_effect = fake_get_latest_sow_request
        async def fake_main(h, p):
            return {
                "FF_ProjectId": "P001",
                "templateid": "T001",
                "daFileId": "DA001",
                "filename": "sample_file.docx",
                "projectid": "P001"
            }

        mock_main.side_effect = fake_main
 
        async def fake_get_recommendations(h, p):
            return [{"FF_ProjectId": "P001", "templateid": "T001", "daFileId": "DA001"}]

        mock_get_recommendations.side_effect = fake_get_recommendations
        self.handler.handle(header, payload)

        mock_get_latest_sow_request.assert_called_once_with(header["requestId"])
        mock_main.assert_called_once_with(header, payload)
        mock_get_recommendations.assert_called_once_with(header, payload)
 
    @patch('services.summarization.handlers.DatabaseManager')
    @patch.object(Consultant_Recommendation, 'get_latest_sow_request', new_callable=MagicMock)

    def test_handle_db_error(self, mock_get_latest_sow_request, mock_db):
        header = {
            "eventType": "PROJECT_REQUEST_RECOMMENDATION_ACK",
            "eventSubType": "STREAM_COMPLETION",
            "createdOn": "Fri Oct 31 12:55:00 UTC 2025",
            "requestId": "test-request-id"
        }

        payload = {
            "projectId": "P001",
            "name": "sample_file.docx",
            "daFileId": "DA001"
        }
 
        async def raise_db_error(request_id):
            raise DatabaseReadError("E007", "DB error")

        mock_get_latest_sow_request.side_effect = raise_db_error

        self.handler.handle(header, payload) 
        self.handler.send_failure.assert_called_once_with(
            header["requestId"],
            {'status': 'FAILED', 'error_code': 'E007', 'error_message': 'E007-ERROR: DB error'}, project_id='P001', stage='Summarization'
        )
 
    @patch('services.summarization.handlers.DatabaseManager')
    @patch.object(Consultant_Recommendation, 'get_latest_sow_request', new_callable=MagicMock)
    @patch('services.summarization.handlers.main')



    def test_handle_main_exception(self, mock_main, mock_get_latest_sow_request, mock_db):
        header = {
            "eventType": "PROJECT_REQUEST_RECOMMENDATION_ACK",
            "eventSubType": "STREAM_COMPLETION",
            "createdOn": "Fri Oct 31 12:55:00 UTC 2025",
            "requestId": "test-request-id"
        }
        payload = {
            "projectId": "P001",
            "name": "sample_file.docx",
            "daFileId": "DA001"
        }
 
        async def fake_get_latest_sow_request(request_id):
            return header, payload

        mock_get_latest_sow_request.side_effect = fake_get_latest_sow_request
        async def raise_main_error(*args, **kwargs):
            raise CustomBaseException("E000", "Main error")

        mock_main.side_effect = raise_main_error
        self.handler.handle(header, payload)

        self.handler.send_failure.assert_called_once_with(
            header["requestId"],
            {'status': 'FAILED', 'error_code': 'E000', 'error_message': 'Main error'}, project_id='P001', stage='Summarization'
        )
    @patch('pandas.read_sql')
    @patch('services.summarization.handlers.DatabaseManager')
    @patch.object(Consultant_Recommendation, 'get_latest_sow_request', new_callable=MagicMock)
    @patch('services.summarization.handlers.main')
    @patch('services.summarization.handlers.get_recommendations')

    def test_handle_recommendations_exception(self, mock_get_recommendations, mock_main, mock_get_latest_sow_request, mock_db, mock_read_sql):
        header = {
            "eventType": "PROJECT_REQUEST_RECOMMENDATION_ACK",
            "eventSubType": "STREAM_COMPLETION",
            "createdOn": "Fri Oct 31 12:55:00 UTC 2025",
            "requestId": "test-request-id"
        }

        payload = {
            "projectId": "P001",
            "name": "sample_file.docx",
            "daFileId": "DA001"
        }
 
        async def fake_get_latest_sow_request(request_id):
            return header, payload

        mock_get_latest_sow_request.side_effect = fake_get_latest_sow_request
 
        async def fake_main(h, p):
            return {
                "FF_ProjectId": "P001",
                "templateid": "T001",
                "daFileId": "DA001",
                "filename": "sample_file.docx",
                "projectid": "P001"
            }

        mock_main.side_effect = fake_main

        async def raise_recommendations_error(*args, **kwargs):
            raise CustomBaseException("E009", "Recommendations error")

        mock_get_recommendations.side_effect = raise_recommendations_error
        self.handler.handle(header, payload)
        self.handler.send_failure.assert_called_once_with(
            header["requestId"],
            {'status': 'FAILED', 'error_code': 'E009', 'error_message': 'Recommendations error'}, project_id='P001', stage='Recommendation'
        )
 
    def test_handle_non_stream(self):
        header = {
            "eventType": "PROJECT_REQUEST_RECOMMENDATION_ACK",
            "eventSubType": "NON_STREAM_COMPLETION",
            "createdOn": "Fri Oct 31 12:55:00 UTC 2025",
            "requestId": "test-non-stream"
        }

        payload = {
            "projectId": "P002",
            "name": "not_a_stream.docx",
            "daFileId": "DA999"
        }

        result = self.handler.handle(header, payload)
        self.assertIsNone(result)
 
if __name__ == "__main__":
    unittest.main()

 