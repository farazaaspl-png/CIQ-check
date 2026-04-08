import unittest
from unittest.mock import patch, MagicMock, AsyncMock
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from kafka_framework.producer import KafkaMessageProducer
from services.gtl_feedback.handlers import GTLFeedbackHandler
from core.db.crud import TDocument

class TestGTLFeedbackHandler(unittest.TestCase):

    @patch('kafka_framework.producer.KafkaMessageProducer')
    def setUp(self, mock_kafka_producer):
        self.mock_producer_instance = mock_kafka_producer.return_value
        self.handler = GTLFeedbackHandler(self.mock_producer_instance, 'test_output_topic')

    @patch('services.gtl_feedback.handlers.DatabaseManager')
    @patch('services.gtl_feedback.handlers.VectorInterface')
    def test_approve_document(self, mock_vector_interface, mock_db_manager):
        mock_vector = MagicMock()
        mock_vector_interface.return_value = mock_vector
        mock_db = MagicMock()
        mock_db_manager.return_value = mock_db

        header = {'eventType': 'IP_RECOMMENDATION_UPDATE', 'eventSubType': 'APPROVE_DOCUMENT'}
        payload = {'daFileId': 'DA001', 'status': 'APPROVED', 'ipId': None}
        self.handler.handle(header, payload)

        mock_db.update_document.assert_called_once_with(
            where_clause={'dafileid': 'DA001'},
            update_values={'status': 'APPROVED', 'ipid': None}
        )
        mock_vector.vectorize_documents_by_dafileids.assert_called_once_with(['DA001'])

    @patch('services.gtl_feedback.handlers.DatabaseManager')
    @patch('services.gtl_feedback.handlers.VectorInterface')
    def test_archive_document_and_delete_vector(self, mock_vector_interface, mock_db_manager):
        mock_vector = MagicMock()
        mock_vector_interface.return_value = mock_vector
        mock_db = MagicMock()
        mock_db_manager.return_value = mock_db

        header = {'eventType': 'IP_RECOMMENDATION_DELETE', 'eventSubType': 'ARCHIVE_DOCUMENT'}
        payload = {'daFileId': 'DA002', 'status': 'ARCHIVED', 'ipId': None}
        self.handler.handle(header, payload)

        mock_db.update_document.assert_called_once_with(
            where_clause={'dafileid': 'DA002'},
            update_values={'status': 'ARCHIVED', 'ipid': None}
        )
        mock_vector.delete_documents_by_dafileids.assert_called_once_with(['DA002'])

    @patch('services.gtl_feedback.handlers.DatabaseManager')
    def test_reprocess_document_feedback_insert(self, mock_db_manager):
        mock_db = MagicMock()
        mock_db_manager.return_value = mock_db

        header = {'eventSubType': 'REPROCESS_DOCUMENT'}
        payload = {
            'daFileId': 'DA010',
            'uuid': 'UUID10',
            'filename': 'file10.txt',
            'reprocessCommand': 'Reprocess this file',
            'ipId': None
        }
        self.handler.handle(header, payload)

        mock_db.update_document.assert_called_once_with(
            where_clause={'dafileid': 'DA010'},
            update_values={'status': 'REPROCESS_REQUESTED', 'ipid': None}
        )
        mock_db.insert_feedback.assert_called_once_with(
            msguuid='UUID10',
            filename='file10.txt',
            dafileid='DA010',
            status='REPROCESS_REQUESTED',
            feedback='Reprocess this file'
        )

    @patch('services.gtl_feedback.handlers.DatabaseManager')
    def test_send_feedback_insert(self, mock_db_manager):
        mock_db = MagicMock()
        mock_db_manager.return_value = mock_db

        header = {'eventSubType': 'SEND_FEEDBACK'}
        payload = {
            'daFileId': 'DA006',
            'uuid': 'UUID2',
            'filename': 'file.txt',
            'feedback': 'Great',
            'status': 'FEEDBACK_RECEIVED',
            'ipId': None
        }
        self.handler.handle(header, payload)

        mock_db.update_document.assert_called_once_with(
            where_clause={'dafileid': 'DA006'},
            update_values={'status': 'FEEDBACK_RECEIVED', 'ipid': None}
        )
        mock_db.insert_feedback.assert_called_once_with(
            msguuid='UUID2',
            filename='file.txt',
            dafileid='DA006',
            status='FEEDBACK_RECEIVED',
            feedback='Great'
        )

    @patch('services.gtl_feedback.handlers.DatabaseManager')
    @patch('services.gtl_feedback.handlers.VectorInterface')
    @patch('services.gtl_feedback.handlers.inspect')
    def test_file_info_update(self, mock_inspect, mock_vector_interface, mock_db_manager):
        mock_vector = MagicMock()
        mock_vector_interface.return_value = mock_vector
        mock_db = MagicMock()
        mock_db_manager.return_value = mock_db

        mock_inspect.return_value.get_columns.return_value = [
            {'name': 'document_type'}, {'name': 'ip_type'}, {'name': 'dtpm_phase'},
            {'name': 'practice'}, {'name': 'offerfamily'}, {'name': 'offer'},
            {'name': 'filename'}, {'name': 'title'}, {'name': 'iptype'}
        ]

        header = {'eventType': 'IP_RECOMMENDATION_UPDATE', 'eventSubType': 'FILE_INFO_UPDATE'}
        payload = {
            'daFileId': 'DA005',
            'newIpTypes': ['type1', 'type2'],
            'oldIpTypes': 'type1',
            'newDocument_type': 'NewDocType',
            'oldDocument_type': 'OldDocType',
            'owner': 'test_user'
        }
        self.handler.handle(header, payload)

        mock_db.insert_change_document.assert_called()
        mock_db.update_document.assert_called_once()
        mock_vector.update_vectors.assert_called_once_with(['DA005'])


    # @patch('services.gtl_feedback.handlers.DatabaseManager')
    # @patch('services.gtl_feedback.handlers.VectorInterface')
    # @patch('services.gtl_feedback.handlers.DellAttachments')
    # @patch('services.gtl_feedback.handlers.Dispatcher')
    # @patch('services.gtl_feedback.handlers.generate_summary')
    # def test_migration_approve_full_flow(self, mock_generate_summary, mock_dispatcher,
    #                                     mock_dell_attachments, mock_vector_interface,
    #                                     mock_db_manager):
    #     # Mock DBManager instance correctly
    #     mock_db = MagicMock()
    #     mock_db_manager.return_value = mock_db

    #     # Mock VectorInterface instance
    #     mock_vector = MagicMock()
    #     mock_vector_interface.return_value = mock_vector

    #     # Mock DellAttachments instance with async download
    #     mock_dell_instance = mock_dell_attachments.return_value
    #     mock_dell_instance.download = AsyncMock(return_value={
    #         'filepath': '/tmp/filepath',
    #         'id': 'DA100'
    #     })

    #     # Mock Dispatcher instance
    #     mock_dispatch_instance = mock_dispatcher.return_value
    #     mock_extractor = MagicMock()
    #     mock_extractor.extract_content.return_value = "text content"
    #     mock_dispatch_instance.getExtractor.return_value = mock_extractor

    #     # Mock generate_summary
    #     mock_generate_summary.return_value = {'title': 'Title', 'description': 'Description'}

    #     header = {'eventType': 'IP_RECOMMENDATION_UPDATE', 'eventSubType': 'MIGRATION_APPROVE'}
    #     payload = {
    #         'uuid': 'UUID100',
    #         'daFileId': 'DA100',
    #         'name': 'file100.txt',
    #         'phase': 'Phase1',
    #         'ipTypes': ['patent', 'design'],
    #         'source': 'SourceType',
    #         'offer': 'OfferValue',
    #         'status': 'APPROVED',
    #         'ipId': 'IP100',
    #         'initialGrade': 'A',
    #         'priority': 'High'
    #     }

    #     expected_doc_dict = {
    #         'requestid': '00000000-0000-0000-0000-000000000001',
    #         'daoriginal_fileid': payload.get('uuid'),        # match payload['uuid']
    #         'dafileid': payload.get('daFileId'),                   # match payload['daFileId']
    #         'filename': payload.get('daFinameleId'),             # match payload['name']
    #         'title' : 
    #         'description' : 
    #         'confidence_score' : 

    #         'dtpm_phase': payload.get('phase'),                 # match payload['phase']
    #         'ip_type': payload.get('iptype'),            # joined from payload['ipTypes']
    #         'document_type': payload.get('source'),
    #         'offer': payload.get('offer'),
    #         'status': payload.get('status'),
    #         'ipid': payload.get('ipId'),
    #         'initialgrade': payload.get('initialGrade'),
    #         'priority': payload.get('priority'),
    #         'created_by': 'system'
    #     }

        


    #     self.handler.handle(header, payload)
    #     print("Handler done")
    #     mock_db.insert_document.assert_called_once_with(**expected_doc_dict)
    #     mock_db.insert_document.assert_called_once()
    #     mock_dell_instance.download.assert_awaited_once()
    #     mock_generate_summary.assert_called_once()
    #     mock_db.update_document.assert_called()
    #     mock_vector.vectorize_documents_by_dafileids.assert_called_once_with(['DA100'])


    @patch('services.gtl_feedback.handlers.DatabaseManager')
    @patch('services.gtl_feedback.handlers.VectorInterface')
    def test_invalid_event_type(self, mock_vector_interface, mock_db_manager):
        mock_vector_interface.return_value = MagicMock()
        mock_db_manager.return_value = MagicMock()

        header = {'eventType': 'INVALID_EVENT_TYPE'}
        payload = {}
        result = self.handler.handle(header, payload)
        self.assertIsNone(result)  # Handler returns None on invalid eventType

    @patch('services.gtl_feedback.handlers.DatabaseManager')
    @patch('services.gtl_feedback.handlers.VectorInterface')
    def test_error_in_db_update(self, mock_vector_interface, mock_db_manager):
        mock_vector = MagicMock()
        mock_vector_interface.return_value = mock_vector
        mock_db = MagicMock()
        mock_db.update_document.side_effect = Exception("DB error")
        mock_db_manager.return_value = mock_db

        header = {'eventType': 'IP_RECOMMENDATION_UPDATE', 'eventSubType': 'APPROVE_DOCUMENT'}
        payload = {'daFileId': 'DA007', 'status': 'APPROVED', 'ipId': None}

        self.handler.handle(header, payload)
        # No assertion needed here; test ensures error is caught and logged

    @patch('services.gtl_feedback.handlers.DatabaseManager')
    @patch('services.gtl_feedback.handlers.VectorInterface')
    def test_error_in_vectorization(self, mock_vector_interface, mock_db_manager):
        mock_vector = MagicMock()
        mock_vector.vectorize_documents_by_dafileids.side_effect = Exception("Vectorization error")
        mock_vector_interface.return_value = mock_vector
        mock_db = MagicMock()
        mock_db_manager.return_value = mock_db

        header = {'eventType': 'IP_RECOMMENDATION_UPDATE', 'eventSubType': 'APPROVE_AND_CREATE_DELIVERY_KIT', 'requestId': 'test-request'}
        payload = {'ipId': 'IP3', 'daFileId': 'DA008', 'status': 'APPROVED'}

        self.handler.handle(header, payload)
    #     # No assertion needed; test checks exception handling during vectorization

if __name__ == '__main__':
    unittest.main()
