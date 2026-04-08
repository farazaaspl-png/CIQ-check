"""
test_remove_keys_removes_specified_keys - Verifies that the handler’s remove_keys method 
                                        correctly removes specific metadata keys from dictionaries.
test_handle_accept_skip_recommendation_success - Checks that when required fields are present, 
                                                the handler processes accept/skip events 
                                                by calling record_feedback without invoking failure handling.
test_handle_accept_skip_recommendation_missing_fields - Confirms the handler detects missing
                                                 required fields in accept/skip events and 
                                                 properly calls send_failure with appropriate error information.
test_handle_record_feedback_raises_exception - Ensures that exceptions from record_feedback 
                                                during handling are caught and cause send_failure to be invoked with the error details.

test_generate_search_results_success - Tests the async recommendation refinement pipeline 
                                        end-to-end with mocks for document search, 
                                        download/upload, and database interactions, verifying recommendations are returned.

test_remove_keys_function_removes_keys - Validates the standalone remove_keys utility removes 
                                        specified keys correctly in an async context.

test_search_documents_calls_generate - Confirms that the async search_documents method 
                                    correctly calls and awaits generate_search_results,
                                    returning the expected results list.


"""


import unittest
from unittest.mock import patch, MagicMock, AsyncMock
import pendulum
import asyncio
import sys
import os
import copy

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from services.consultant_feedback.handlers import RecommendationFeedbackHandler
from services.consultant_feedback.refine_recommendations import generate_search_results, search_documents
from core.exceptions import CustomBaseException, InvalidMetadataError

class TestRecommendationFeedbackHandler(unittest.TestCase):
    @patch('services.recommendation_feedback.handlers.KafkaMessageProducer')
    def setUp(self, mock_kafka_producer_class):
        mock_producer_instance = MagicMock()
        mock_producer_instance.send_message = MagicMock()  # mock send_message
        mock_kafka_producer_class.return_value = mock_producer_instance
        self.handler = RecommendationFeedbackHandler(mock_producer_instance, 'output_topic')


    def send_failure(self, requestUuid: str, payload: dict, eventSubType: str = "PROCESSING_ERROR", project_id: str = "", requestId: str = ""):
        response_headers = {
            'eventType': "PROJECT_UPDATE_RECOMMENDATION_ACK",
            'eventSubType': eventSubType,
            'createdOn': pendulum.now("UTC").format("ddd MMM DD HH:mm:ss [UTC] YYYY")
        }
        if len(requestId) > 0:
            response_headers['requestId'] = requestId
        payload['requestUuid'] = requestUuid

        # Send the failure message with a deep copy of headers
        self.producer.send_message(
            self.output_topic,
            copy.deepcopy(response_headers),
            payload
        )

        # Change eventSubType for the stream completion message
        response_headers["eventSubType"] = "STREAM_COMPLETION"
        statuspayload = {
            "Status": "Completed",
            "FF_ProjectId": project_id,
            'requestUuid': requestUuid
        }

        # Send the stream completion message with another deep copy of headers
        self.producer.send_message(
            self.output_topic,
            copy.deepcopy(response_headers),
            statuspayload
        )


    def test_remove_keys_removes_specified_keys(self):
        dict_to_clean = {"createdBy": 1, "updatedBy": 2, "createdOn": 3, "updatedOn": 4, "other": 5}
        self.handler.remove_keys(dict_to_clean)
        for key in ['createdBy', 'updatedBy', 'createdOn', 'updatedOn']:
            self.assertNotIn(key, dict_to_clean)
        self.assertIn("other", dict_to_clean)

    @patch('services.recommendation_feedback.handlers.record_feedback')
    def test_handle_accept_skip_recommendation_success(self, mock_record_feedback):
        # Patch send_failure to MagicMock to assert its calls
        self.handler.send_failure = MagicMock()

        header = {"eventSubType": "ACCEPT_RECOMMENDATION", "requestId": "req-123"}
        payload = {"uuid": "uuid-123", "status": "accepted", "recommendationId": "rec-789"}
        self.handler.handle(header, payload)
        mock_record_feedback.assert_called_once_with(header, payload)
        self.handler.send_failure.assert_not_called()

    def test_handle_accept_skip_recommendation_missing_fields(self):
        # Patch send_failure to MagicMock to assert called
        self.handler.send_failure = MagicMock()

        header = {"eventSubType": "ACCEPT_RECOMMENDATION", "requestId": "req-123"}
        payload = {"uuid": "uuid-123", "status": None, "recommendationId": None}
        self.handler.handle(header, payload)

        self.handler.send_failure.assert_called_once()
        called_args = self.handler.send_failure.call_args[0]
        self.assertEqual(called_args[0], "uuid-123")
        self.assertIsInstance(called_args[1], dict)
        self.assertIn('status', called_args[1]['error_message'])

    @patch('services.recommendation_feedback.handlers.record_feedback')
    def test_handle_record_feedback_raises_exception(self, mock_record_feedback):
        mock_record_feedback.side_effect = CustomBaseException("E999", "fake error")

        # Patch send_failure to MagicMock to assert called
        self.handler.send_failure = MagicMock()

        header = {"eventSubType": "SKIP_RECOMMENDATION", "requestId": "req-123"}
        payload = {"uuid": "uuid-123", "status": "skipped", "recommendationId": "rec-456"}
        self.handler.handle(header, payload)
        self.handler.send_failure.assert_called_once()
        called_args = self.handler.send_failure.call_args[0]
        self.assertEqual(called_args[0], "uuid-123")
        self.assertIn("fake error", called_args[1]['error_message'])

class TestRefineRecommendations(unittest.IsolatedAsyncioTestCase):
    @patch('services.recommendation_feedback.refine_recommendations.search_vectorized_documents')
    @patch('services.recommendation_feedback.refine_recommendations.DellAttachments.download')
    @patch('services.recommendation_feedback.refine_recommendations.DellAttachments.upload')
    @patch('services.recommendation_feedback.refine_recommendations.DatabaseManager')
    async def test_generate_search_results_success(self, mock_db_manager, mock_upload, mock_download, mock_search_vector):
        # Mock search vector returns DataFrame-like mock with to_dict
        mock_search_vector.return_value = MagicMock(shape=(1,1), to_dict=lambda orient: [{"dafileid":"file1","filename":"file1.txt","ipid":"ip1","dtpm_phase":"phase1"}])
        mock_download.return_value = {"filepath": "/tmp/file1.txt"}
        mock_upload.return_value = "new_file_id"

        instance = mock_db_manager.return_value
        instance.insert_recommendation.return_value = None

        class DummyConnection:
            def __enter__(self): return self
            def __exit__(self, exc_type, exc_val, exc_tb): return False
            def execute(self, *args, **kwargs): pass
            def commit(self): pass

        instance.engine = MagicMock()
        instance.engine.connect.return_value = DummyConnection()

        import pandas as pd
        df = pd.DataFrame([{"projectid":"proj1","dafileid":"new_file_id"}])
        with patch('pandas.read_sql', return_value=df):
            results = await generate_search_results(requestId="reqid1", projectId="proj1", refineText="test", phases=["phase1"], includeSkipped="FALSE")
            self.assertIsInstance(results, list)
            self.assertGreater(len(results), 0)

    async def test_remove_keys_function_removes_keys(self):
        d = {"createdBy":1, "updatedBy":2, "key":"value"}
        from services.consultant_feedback.refine_recommendations import remove_keys
        remove_keys(d)
        self.assertNotIn("createdBy", d)
        self.assertNotIn("updatedBy", d)
        self.assertIn("key", d)

    @patch('services.recommendation_feedback.refine_recommendations.generate_search_results', new_callable=AsyncMock)
    async def test_search_documents_calls_generate(self, mock_generate):
        header = {"requestId":"rid", "projectId":"pid"}
        payload = {"refineText": "text", "phases": ["p1"]}
        mock_generate.return_value = [{"rec1":"result1"}]
        res = await search_documents(header, payload)
        mock_generate.assert_awaited_once()
        self.assertIsInstance(res, list)

if __name__ == '__main__':
    unittest.main()
