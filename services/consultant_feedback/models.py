from pydantic import BaseModel
from typing import List, Optional


class Header(BaseModel):
    eventType: str
    eventSubType: Optional[str] = None
    createdOn: str

class RecommendationFeedbackPayload(BaseModel):
    createdBy: int
    createdOn: int
    updatedOn: int
    uuid: str
    status: str
    reason: Optional[str] = None
    recommendationId: str

class RefinedRecommendationPayload(BaseModel):
    createdBy: int
    createdOn: int
    updatedOn: int
    uuid: str
    phase: str
    status: str
    reason: Optional[str] 
    includeSkipped: str
    recommendationId: str


class FeedbackRequest(BaseModel):
    header: Header
    payload: RecommendationFeedbackPayload

    @classmethod
    def from_dict(cls, header_data: dict, payload_data: dict):
        header = Header(**header_data)
        payload = RecommendationFeedbackPayload(**payload_data)
        return cls(header=header, payload=payload)
    

class RefineRecommendationRequest(BaseModel):
    header: Header
    payload: RefinedRecommendationPayload

    @classmethod
    def from_dict(cls, header_data: dict, payload_data: dict):
        header = Header(**header_data)
        payload = RefinedRecommendationPayload(**payload_data)
        return cls(header=header, payload=payload)
    
