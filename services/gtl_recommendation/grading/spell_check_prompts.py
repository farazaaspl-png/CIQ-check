from asyncio.log import logger

OutputFormat = """
OUTPUT (JSON only):
{
  "misspelled": <number of misspelled words>,
  "incorrectline": <number of SENTENCES with grammatical errors>
}
"""

# Common rules for spellchecker
Rules = """
- Ignore technical terms, acronyms, and domain-specific vocabulary.
- Ignore content inside < > (treated as redacted).
- Be conservative: if uncertain, do not count as an error.
- Never hallucinate; if unsure, return empty lists.
"""

# Common task description
Task = """
- Count clear, undeniable spelling errors.
- Count objectively incorrect grammar that breaks meaning.
"""


def build_docx_spellcheck_prompt(document_title: str, section_title: str, section_text: str) -> str:
    """Build the LLM prompt for spelling/grammar validation of a document section."""
    logger.info("Using spellchecker prompt version: 00000000-0000-0000-0000-000000000099")

    return f"""
You are a document quality validator focused on spelling and grammar.
Lines between `--- Table Start ---` and `--- Table End ---` represent key-value dictionary format generated from tables in the document.
Images referenced in the text are represented as `[IMAGE: <image name>]`.

{Task}

{Rules}

DOCUMENT TITLE: {document_title}
SECTION: {section_title}

SECTION CONTENT:
<<<
{section_text}
>>>

{OutputFormat}
""".strip()


def build_xlsx_spellcheck_prompt(document_title: str, section_title: str, section_text: str) -> str:
    """Build the LLM prompt for spelling/grammar validation of an XLSX section."""
    logger.info("Using XLSX spellchecker prompt version: 00000000-0000-0000-0000-000000000101")

    return f"""
You are a STRICT .xlsx quality validator focused on spelling and grammar.
Lines enclosed in `===` are the excel sheet name.
Lines between `--- Table Start ---` and `--- Table End ---` represent key-value dictionary format generated from tables in the excel file.
Images referenced in the text are represented as `[IMAGE: <image name>]`.


{Task}

{Rules}
- Ignore numbers, IDs, codes, file paths.
- Ignore abbreviations, acronyms, IT/Infra technical terms, product names.
- Do not consider lines shorter than 20 characters as sentences.

DOCUMENT: {document_title}
SECTION: {section_title}

SECTION CONTENT:
<<<
{section_text}
>>>

{OutputFormat}
""".strip()


def build_pptx_spellcheck_prompt(document_title: str, section_title: str, section_text: str) -> str:
    """Build the LLM prompt for spelling/grammar validation of a PPTX section."""
    logger.info("Using PPTX spellchecker prompt version: 00000000-0000-0000-0000-000000000102")

    return f"""
You are a STRICT .pptx quality validator focused ONLY on spelling and grammar.
Lines starting with `=== Slide` marks the start and end of slide data.
Lines between `--- Table Start ---` and `--- Table End ---` represent key-value dictionary format generated from tables in the excel file.
Images referenced in the text are represented as `[IMAGE: <image name>]`.

{Task}

{Rules}
- Ignore numbers, IDs, codes, URLs, abbreviations, acronyms, and technical terms.
- Ignore bullet fragments, title-case headings, and incomplete sentences — these are intentional in PPTX.

DOCUMENT: {document_title}
SECTION: {section_title}

SECTION CONTENT:
<<<
{section_text}
>>>

{OutputFormat}
""".strip()