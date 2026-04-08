from pydantic import BaseModel
from typing import List, Optional


class Header(BaseModel):
    eventType: str
    eventSubType: Optional[str] = None
    createdOn: str
    requestId: str


class Payload(BaseModel):
    createdBy: int
    updatedBy: Optional[int] = None
    createdOn: int
    updatedOn: int
    ipId: Optional[str] = None
    uuid: str
    name: str
    daFileId: str
    status: Optional[str] = None
    documentType: Optional[str] = None
    ownerId: Optional[str] = None
    projectId: str
    projectFileId: str


class SAWSummary(BaseModel):
    FF_ProjectId: str
    daFileId: str
    filename: str
    practice: str
    offer_family: List[str]
    offers: List[str]

class RecommendationRequest(BaseModel):
    header: Header
    payload: Payload

    @classmethod
    def from_dict(cls, header_data: dict, payload_data: dict):
        header = Header(**header_data)
        payload = Payload(**payload_data)
        return cls(header=header, payload=payload)
    
class SummaryResponse(BaseModel):
    header: Header
    payload: SAWSummary

    @classmethod
    def from_dict(cls, header_data: dict, payload_data: dict):
        header = Header(**header_data)
        payload = Payload(**payload_data)
        return cls(header=header, payload=payload)