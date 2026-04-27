from core.db.crud import DatabaseManager
from core.utility import get_custom_logger

logger = get_custom_logger(__name__)

def get_offer_list(withhints=False):
    db = DatabaseManager()
    offerdf = db.get_vwofferfamilydata()
 
    if withhints:
        logger.info('using offer list with hints')
        return '\n'.join(offerdf[['offer','hints']].drop_duplicates().apply(lambda rows: f'{rows.offer.upper()}  [HINTS: Look for "{rows.hints.lower()}" keywords]' if rows.hints else rows.offer.upper(),axis=1).to_list())
    else:
        logger.info('using offer list without hints')
        return '\n'.join(offerdf['offer'].drop_duplicates().to_list())

def build_description_prompt(text: str,
                             vector_search_context: list, 
                             ischunked=False) -> str:
    """Extract customer name and generate summary (Objective, Scope, Deliverables)"""
    logger.info('Using description prompt version:- 00000000-0000-0000-0000-000000000028')
    return (f"""
You are a TECHNICAL CONSULTANT for Dell Group of Companies.
From below {"chunk of" if ischunked else ''} STATEMENT OF WORK,
    1. Extract CUSTOMER NAME
    2. Generate a SUMMARY of the all the works

 Consider the knowledge base content suggestions but make your own assessment based on the actual Content of STATEMENT OF WORK.
 knowledge base content: 
--------------------------------------------------------------------------------
{vector_search_context}
Use this knowledge base content to:
1. Validate the SUMMARY 
2. Find additional relevant keywords
3. Cross-reference with known use cases
--------------------------------------------------------------------------------


Content of STATEMENT OF WORK:
<
{text}
>>>
 
Return ONLY a JSON array of objects in this format:  
[
  {{
"Customer Name": "Name of customer or company",
"Objective": "What is the project trying to achieve? (2-5 sentences)",
"Scope of Work": "List of 1 line sentences (of explanation) for all services covered in Scope of Work. (| separated)",
"Deliverables": "List tangible outputs (like documents, reports, systems, integrations, prototypes, development, infrastructure provisioning etc) (| separated)",
"Language": "Actual LANGUAGE of the STATEMENT OF WORK"
  }}
]
 
STRICT RULES:
- Do not generate Scope of Work and Deliverables if it is not provided in the STATEMENT OF WORK
{"- LANGUAGE of Objective, Scope of Work and Deliverables should be same as of the STATEMENT OF WORK" if not ischunked else ''}
""")

 
def build_description_consolidation_prompt(descriptionlist, language='english'):
    """Consolidate descriptions from multiple chunks"""
    logger.info('Using description consolidation prompt version:- 00000000-0000-0000-0000-000000000028')
    return f"""
You are a TECHNICAL CONSULTANT for Dell Group of Companies.
For provided list of SUMMARY, generated from multiple chunks of a STATEMENT OF WORK:
    1. Generate a consolidated Summary (i.e. Objective, Scope of Work and Deliverables)
 
Below is the list of SUMMARY of each chunk of STATEMENT OF WORK:
-----------------------------------------------------------------------------------------------------------------
{descriptionlist}
-----------------------------------------------------------------------------------------------------------------
 
OUTPUT FORMAT:
Return ONLY a JSON array of objects in this format:  
[
  {{
"Objective": "Consolidated Objective of what is the project trying to achieve? (2-5 sentences)",
"Scope of Work": "Consolidated list of maximum 10, 1 line sentences of all the Scope Of Work. (| separated)",
"Deliverables": "Consolidated list of less than 10 most important Deliverables (| separated)"
  }}
]
 
STRICT RULES:
- Do not include Scope of Work that is too generic.
- Do not include Deliverables that is too generic.
- AVOID GENERIC TERMS in Scope of Work and Deliverables. 
- Examples of terms to AVOID for Scope of Work and Deliverables: 
"Implementation Planning","Project Management","providing services","general support", "consulting", "assessment", "standard deliverables", "documentation", "reports" (without specifics)
- BE SPECIFIC: Instead of "cloud migration", use "migration of 50 VMs from on-premise to Azure"; instead of "security assessment", use "vulnerability scanning of web applications and API endpoints"
- Each Scope of Work item must reference specific technologies, quantities, systems, or methodologies mentioned in the SOW
- Each Deliverable must be concrete and measurable (e.g., "PowerBI dashboard with 15 KPIs" not "reporting solution")
{"- Objective, Scope of Work and Deliverables should be in "+language.upper()+" LANGUAGE." if language.lower() != 'english' else ''}
"""

def build_offer_prompt(text: str,offers: str,vector_search_context: list,suggested_offers: list,ischunked: bool = False) -> str:
                       
    #                    vector_search_context: list,suggested_offers: list,
    # # response_context: str = None

    logger.info('Using offer prompt version with vector search:- 00000000-0000-0000-0000-000000000030')
    
    return (f"""
You are a TECHNICAL CONSULTANT for Dell Group of Companies.
From below {"chunk of" if ischunked else ''} STATEMENT OF WORK,
    1. SELECT all the OFFERS from provided LIST OF OFFERS that are relevant to this STATEMENT OF WORK (Use HINTS if available).
    2. Calculate relevance score between 0-1 for each SELECTED OFFER using the scoring methodology below
    3. Validate and filter out SELECTED OFFERS based on relevance
    4. Provide final relevance score for each SELECTED OFFER

TASK: Analyze the STATEMENT OF WORK and identify relevant OFFERS

knowledge base content: 
--------------------------------------------------------------------------------
{vector_search_context}
{suggested_offers}
Use this knowledge base content to:
1. Validate your offer selections
2. Find additional relevant keywords
3. Cross-reference with known use cases
Consider these suggestions but make your own assessment based on the actual SOW content.
--------------------------------------------------------------------------------
Content of STATEMENT OF WORK:
<<<
{text}
>>>
 
List of available OFFERS:
-----------------------------------------------------------------------------------------------------------------
{offers}
-----------------------------------------------------------------------------------------------------------------
RELEVANCE SCORING METHODOLOGY:
STEP 1: EXTRACT KEYWORDS FROM EACH OFFER
For EACH offer in the list:
  1a. Check if HINTS exist and are non-empty
      - IF HINTS exist: Extract keywords ONLY from HINTS
      - IF HINTS are empty/missing: Extract keywords ONLY from offer name
      - NEVER combine both sources
  
  1b. Parse operators in HINTS (if using HINTS):
      - Split by '+' operator → ALL keywords must match (AND logic)
      - Split by '/' operator → ANY keyword can match (OR logic)
  
  1c. Create a normalized keyword list (lowercase, trimmed)
  
  SHOW YOUR WORK: List the offer name, keyword source (HINTS or NAME), and extracted keywords
STEP 2: FIND EXACT MATCHES IN SOW
For EACH keyword from Step 1:
  2a. Convert SOW text to lowercase
  2b. Search for EXACT word matches (case-insensitive)
  2c. Count occurrences of each matched keyword
  2d. Mark keyword as MATCHED or NOT MATCHED
  
  SHOW YOUR WORK: List each keyword with match status and frequency count

STEP 3: CALCULATE BASE SCORE
  3a. Count total keywords from Step 1
  3b. Count matched keywords from Step 2
  3c. Calculate: Base Score = (Matched Keywords) / (Total Keywords)
  
  VALIDATION CHECK:
  - If using '+' operator (AND): ALL keywords must match, otherwise Base Score = 0
  - If using '/' operator (OR): At least ONE keyword must match
  
  SHOW YOUR WORK: "Base Score = X/Y = Z.ZZ"

STEP 4: CALCULATE FREQUENCY BONUS
For EACH matched keyword:
  4a. Get frequency count from Step 2
  4b. Calculate: Keyword Multiplier = min(1 + 0.1 * Frequency, 1.5)
  4c. Average all multipliers
  4d. Calculate: Frequency Bonus Score = Base Score * Average Multiplier
  
  SHOW YOUR WORK: List each keyword with its multiplier, then show average and final calculation

STEP 5: SEMANTIC ADJUSTMENT (OPTIONAL, MAX ±0.15)
  5a. Review SOW context around matched keywords
  5b. Check if keywords appear in relevant context (not random mentions)
  5c. Adjust by -0.15 to +0.15 ONLY if there's strong evidence
  
  DEFAULT: If uncertain, use 0.0 adjustment
  
  SHOW YOUR WORK: Explain why you're adjusting (or not adjusting)

STEP 6: FINAL SCORE
  6a. Calculate: Final Score = Frequency Bonus Score + Semantic Adjustment
  6b. Apply cap: Final Score = min(Final Score, 1.0)
  
  SHOW YOUR WORK: "Final = X.XX + Y.YY = Z.ZZ (capped at 1.0)"

STEP 7: SELECTION CRITERIA
  7a. Include offer ONLY if Final Score >= 0.25
  7b. If NO offers score >= 0.25, include the top 1 offer with highest score

Return ONLY a JSON array in this EXACT format:
[
  {{
    "Offer": [
      {{
        "OfferName": "exact offer name from list",
        "Relevance Score": "0.XX",
        "KeywordSource": "HINTS or NAME",
        "TotalKeywords": "number",
        "MatchedKeywords": "number",
        "KeywordDetails": "keyword1(freq:3,mult:1.3), keyword2(freq:1,mult:1.1)",
        "BaseScore": "0.XX",
        "FrequencyBonus": "0.XX", 
        "SemanticAdjustment": "+/-0.XX",
        "Calculation": "Base: 0.XX * AvgMult: 1.XX = 0.XX, Semantic: +/-0.XX, Final: 0.XX",
        "Reason": "Matched keywords: [list]. Calculation shown above. Context: [brief context check]"
      }}
    ]
  }}
]
 
STRICT RULES:
1. NEVER invent keywords not in HINTS or offer name
2. NEVER claim a keyword matched if it doesn't appear in SOW (exact word match only)
3. ALWAYS show complete calculation in Calculation field
4. If '+' operator: Base Score = 0 unless ALL keywords match
5. Use HINTS if present, otherwise use NAME - never both
6. Semantic adjustment is OPTIONAL and should default to 0.0
7. Include KeywordDetails field showing every matched keyword with its frequency
""")
# RELEVANCE SCORING METHODOLOGY
# Step 1 – Keyword Matching:
#     Keyword Source Selection (Strict Priority Rule):
# 		- If the offer contains HINTS and they are non-empty, extract keywords exclusively from HINTS.
# 		- If HINTS are missing, null, or empty, then extract keywords from the offer name.
# 		- Do NOT combine keywords from both sources under any condition.
#     Keyword Parsing Rules (Applicable only when HINTS are used):
# 		- + operator denotes logical AND → all separated keywords must be present.
# 		- / operator denotes logical OR → any one of the separated keywords is sufficient.
# 	Matching Logic:
# 		- Normalize SOW text and keywords (case-insensitive, trimmed, optional stemming/lemmatization if applicable).
# 		- Identify and count unique keyword matches in the SOW text.
#     Base Score Calculation:
# 		- Base Score = (Number of Matched Keywords) / (Total Keywords from Selected Source)

# Step 2 – Frequency Bonus:
#     Count occurrences of each matched keyword in the SOW text.
#     For each keyword with frequency N:
# 		-Frequency Multiplier = min(1 + 0.1 * N, 1.5)
# 	Compute:
# 		- Average Frequency Multiplier = mean(all keyword multipliers)
# 	Frequency Bonus Score:
# 		- Frequency Bonus Score = Base Score * Average Frequency Multiplier

# Step 3 – Semantic Relevance:
# 	Evaluate contextual and semantic alignment between the offer and SOW content.
# 	If knowledge base results are available, use them as supporting evidence for validation.
# 	Apply an adjustment:
# 		- Semantic Adjustment ∈ [-0.2, +0.2]
# 		- Positive adjustment → strong contextual relevance
# 		- Negative adjustment → weak or misleading keyword match
# Final Score:
# 		Final Score = min(Frequency Bonus Score ± Semantic Adjustment, 1.0)
# knowledge base content: 
# --------------------------------------------------------------------------------
# {vector_search_context}
# {suggested_offers}
# Use this knowledge base content to:
# 1. Validate your offer selections
# 2. Find additional relevant keywords
# 3. Cross-reference with known use cases
# Consider these suggestions but make your own assessment based on the actual SOW content.
# --------------------------------------------------------------------------------

def build_offer_consolidation_prompt(offerlist, language='english'):
    """Consolidate and re-score offers from multiple chunks"""
    logger.info('Using offer consolidation prompt version:- 00000000-0000-0000-0000-000000000029')
    return f"""
You are a TECHNICAL CONSULTANT for Dell Group of Companies.
For provided list of OFFERS, generated from multiple chunks of a STATEMENT OF WORK:
    1. Validate and recalculate the Relevance Score of all the OFFERS
    2. Use chunk frequency to boost scores using the formula below
 
Below is the list of OFFERS from each chunk of STATEMENT OF WORK:
-----------------------------------------------------------------------------------------------------------------
{offerlist}
-----------------------------------------------------------------------------------------------------------------

CHUNK FREQUENCY SCORING METHODOLOGY:
Step 1 - Identify Original Score:
  - Take the maximum Relevance Score across all chunks for each offer
 
Step 2 - Calculate Chunk Frequency Weight:
  - Chunk Frequency Weight = 1 + (0.2 * NoOfChunks_offer_appeared_in)
  - This gives 20% boost per chunk appearance
  - Appearing in 5 chunks = 2.0x multiplier (doubles the score)
 
Step 3 - Apply Weight:
  - Adjusted Score = min(Original Score * Chunk Frequency Weight, 1.0)
  - Cap the final score at 1.0 to maintain 0-1 scale

OUTPUT FORMAT:
Return ONLY a JSON array of objects in this format:  
[
  {{
"Offer": [
  {{
    "OfferName": "OfferName as provided",
    "Relevance Score": "recalculated relevance score with chunk frequency boost (0-1)",
    "Reason": "Original reason + chunk frequency calculation (show: Original Score, NoOfChunks, Weight, Final Calculation)",
    "ChunkAppearances": "number of chunks where this offer appeared"
  }}
]
  }}
]
 
STRICT RULES:
- ALWAYS show the chunk frequency calculation in the Reason field
- Format: "Original: X.XX, Chunks: N, Weight: 1+(0.2*N)=Y.Y, Final: min(X.XX*Y.Y, 1.0)=Z.ZZ"
- Sort offers by final Relevance Score in descending order
- If an offer appears in more chunks, it should generally score higher (unless original score was very low)
- Maximum possible score is 1.0
"""