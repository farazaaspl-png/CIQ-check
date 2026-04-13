from asyncio.log import logger

# Common KPI definitions
KPI_DEFINITIONS = """

- Relevance: How well content aligns with expected business context (0-100).
- Coverage: Breadth of topics covered; completeness of information (0-100).
- Clarity: Readability and structure; clear organization (0-100).
- Redundancy: Repetition or duplicate content; efficiency of structure (0-100, higher = better).
- Coherence: Logical flow and consistency throughout (0-100).
"""

# Common output schema
OutputFormat = """ Return ONLY valid JSON in this format:
[
{{
  "document_summary": {
    "notes": ["string"]
    "detected_sections_count": int,
    "missing_sections": ["string"],
    "duplicate_sections": ["string"],
  },
  "document_kpis": {
    "coherence": 0.0,
    "clarity": 0.0,        
    "relevance": 0.0,    
    "coverage": 0.0,       
    "redundancy": 0.0   
  }
}}
]"""

# Common rules
Rules = """
- Use prior JSON as base; merge/update.
- Return only JSON.
- KPIs: floats 0-100.
- No suggestions; validate only.
- Update document_kpis each chunk.
- In "notes", explain precisely why each KPI was scored as it was, citing evidence.
- List duplicate sections in "duplicate_sections" with brief justification.
"""


def build_doc_contentcheck_prompt(chunk: str, previous_result: str = None, document_title: str = None, filename: str = None) -> str:
    """Incremental KPI-based content validation for Dell/Dell-family documents."""
    logger.info('Using prompt version:- 00000000000000001605')

    return f"""
You are validator for Dell/Dell-family documents. No predefined standards. Build context across chunks.

TITLE: {document_title or filename}

CHUNK:
<<<
{chunk}
>>>

PRIOR RESULT:
<<<
{previous_result if previous_result else "None"}
>>>

TASK:
- Infer and merge sections; do NOT restart.
- Evaluate KPIs per section, then combine into document-level KPIs.
- Ignore < > redactions and empty tables; do not penalize them as missing content.
- Maintain running document-level KPIs across all sections seen so far.

{KPI_DEFINITIONS}

{OutputFormat}

{Rules}

- Redactions/empty tables are expected; do not flag as missing.
"""


def build_excel_contentcheck_prompt(chunk: str, previous_result: str = None, document_title: str = None, filename: str = None) -> str:
    """Incremental KPI-based content validation for Excel spreadsheets."""
    logger.info('Using Excel prompt version:- 00000000000000001606')

    title = document_title or (filename.split('.')[0] if filename else "Unknown")

    return f"""
You are validator for Excel spreadsheets and workbooks. Focus on data structure, formulas, and content quality. Build context across chunks.

TITLE: {title}
FILENAME: {filename or "Unknown"}

CHUNK:
<<<
{chunk}
>>>

PRIOR RESULT:
<<<
{previous_result if previous_result else "None"}
>>>

TASK:
- Infer and merge worksheet sections; do NOT restart.
- Evaluate KPIs per worksheet, then combine into workbook-level KPIs.
- Ignore empty cells and #REF/#N/A errors; do not penalize them as missing content.
- Maintain running workbook-level KPIs across all sheets seen so far.
- Focus on data integrity, formula consistency, and spreadsheet structure.

{KPI_DEFINITIONS}

{OutputFormat}

{Rules}

- Empty cells and formula errors are expected; do not flag as missing.
- Cite evidence from the spreadsheet in notes.
"""


def build_pptx_contentcheck_prompt(chunk: str, previous_result: str = None, document_title: str = None, filename: str = None) -> str:
    """Incremental KPI-based content validation for PowerPoint presentations."""
    logger.info('Using PPTX prompt version:- 00000000000000001607')

    title = document_title or (filename.split('.')[0] if filename else "Unknown")

    return f"""
You are validator for PowerPoint presentations (.pptx). Focus on slide content, visual structure, and presentation flow. Build context across chunks.

TITLE: {title}
FILENAME: {filename or "Unknown"}

CHUNK:
<<<
{chunk}
>>>

PRIOR RESULT:
<<<
{previous_result if previous_result else "None"}
>>>

TASK:
- Infer and merge slide content; do NOT restart.
- Evaluate KPIs per slide section, then combine into presentation-level KPIs.
- Ignore empty slides and placeholder text; do not penalize them as missing content.
- Maintain running presentation-level KPIs across all slides seen so far.
- Focus on presentation structure, content organization, and visual communication effectiveness.

{KPI_DEFINITIONS}

{OutputFormat}

{Rules}

- Empty slides and placeholder text are expected; do not flag as missing.
- Cite evidence from the presentation slides in notes.
- Consider presentation-specific elements like titles, bullet points, tables, and image placeholders.
- Duplicate content excludes slide titles and company logos.
"""