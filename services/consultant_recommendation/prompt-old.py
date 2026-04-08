# prompt.py
from core.db.crud import DatabaseManager
def get_offer_list():
   db = DatabaseManager()
   offerdf = db.get_vwofferfamilydata()
   return '\n'.join(offerdf['offer'].drop_duplicates().to_list())


def build_prompt(text: str) -> str:
    return (f"""
 You are a concise TECHNICAL WRITER for Dell Group of Companies .
 From below statement of work,
    1. Extract CUSTOMER NAME
    2. generate a summary of the all the works 
    3. EXTRACT all the OFFERS provided to customer in it.
    4. provide confidence score for each OFFER
 <<<
{text}
>>>
{get_offer_list()}

Return ONLY a JSON array of objects in this format:  
[
  {{
"Customer Name": "Name of customer or company",
"Objective": "What is the project trying to achieve? (2-5 sentences)",
"Scope of Work": "What is overall scope of work?",
"Deliverables": "List tangible outputs (documents, systems, integrations, prototypes, development, infrastructure provisioning)",
"Offer": ["OfferName":"Name of offer provided", "Confidence Score": "Confidence score of the offer"] }}
]

STRICT RULES:
- IF DOCUMENT DOESN'T LOOK LIKE SOW, RETURN [{{"ERROR": "DOCUMENT DOESN'T LOOK LIKE SOW"}}]
- Do not generate Scope of Work and Deliverables if it is not there in the document
- Scope of Work and Deliverables should be extracted from the document and should be | separated
- DO NOT GENERATE NEW OFFERS, ONLY USE EXISTING OFFERS
- OFFERS should be REASONED and SELECTED from the list of offers provided
- Atleast 1 OFFER should be SELECTED
""")

# build_prompt('Testing')