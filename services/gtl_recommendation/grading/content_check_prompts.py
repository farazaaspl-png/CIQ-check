from asyncio.log import logger
import re

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# PROMPTS FOR KPI Generation
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

############################################################################################
# Consolidation Prompt
############################################################################################

def build_consolidation_prompt(response_context: list[dict], document_title: str, fext: str, description:str = None,document_type: str = None, phase: str = None) -> str:
    """Consolidate multiple chunk responses into a single document-level assessment."""
    if fext.lower() == '.docx': file_type = 'document'
    elif fext.lower() == '.pptx': file_type = 'ppt presentation'
    elif fext.lower() =='.xlsx': file_type = 'excel spreadsheet'
    else: file_type = 'file'
    document_details = f"DOCUMENTS METADATA:\n\tTITLE: {document_title.upper()}\n"
    if description is not None:
      document_details = document_details + f"\tDESCRIPTION: {description}\n"
    if document_type is not None:
      # doc_instruction = f'Provided text is {chunk_title.upper()} from {document_type.upper().strip('s')} document titled {document_title.upper()}.'
      document_details = document_details + f"\tTYPE: {document_type.upper().strip('s')}\n"
    if phase is not None:
      document_details = document_details + f"\tPHASE (of project used in): {phase.upper()}\n"

    return f"""
You are Technical Reviewer for Dell group of companies

{document_details}

Below is the list of validation results for each chunk of the {file_type}.
{response_context}

Your task is to consolidate these results into a single {file_type} level assessment.

Return ONLY valid JSON in this format:
[
{{
  "issues": "List of Issues found",
  "kpis": {{
    "grammer_accuracy": {{"score": 0.0, "reason": "one sentence"}},
    "spelling_accuracy": {{"score": 0.0, "reason": "one sentence"}},
    "coherence": {{"score": 0.0, "reason": "one sentence"}},
    "clarity": {{"score": 0.0, "reason": "one sentence"}},
    "relevance": {{"score": 0.0, "reason": "one sentence"}},
    "coverage": {{"score": 0.0, "reason": "one sentence"}},
    "redundancy": {{"score": 0.0, "reason": "one sentence"}}
  }}
}}
]"""

############################################################################################
# Prompt Builder for .docx files
############################################################################################
def build_doc_contentcheck_prompt(chunk: str, document_title: str, chunk_title: str, description:str = None, response_context: list[dict] = None, document_type: str = None, phase: str = None) -> str:
    """Incremental KPI-based content validation for Dell/Dell-family documents."""
  #  logger.info('Using prompt version:- 00000000000000000006')
    is_chunked = chunk_title.strip() != 'Chunk 1/1'
    is_first_chunk = chunk_title.startswith('Chunk 1/')
    role_def = 'You are Technical Reviewer for Dell group of companies'

    # if offer is None:
    #   role_def = 'You are Documentation ANALYST for Dell group of companies'
    # else:
    #   role_def = f'You are CONSULTANT of {offer.upper()}.'
    document_details = f"DOCUMENTS METADATA:\n\tTITLE: {document_title.upper()}\n"
    if description is not None:
      document_details = document_details + f"\tDESCRIPTION: {description}\n"
    if document_type is not None:
      # doc_instruction = f'Provided text is {chunk_title.upper()} from {document_type.upper().strip('s')} document titled {document_title.upper()}.'
      document_details = document_details + f"\tTYPE: {document_type.upper().strip('s')}\n"
    if phase is not None:
      document_details = document_details + f"\tPHASE (of project used in): {phase.upper()}\n"

    task_instruction = f"Your task is to EVALUATE and VALIDATE the CONTENT of a TEMPLATE DOCUMENT and provide KPI Scoring"

    if (not is_chunked):
      task_steps = """STEPS:
- Evaluate the content for the KPIs defined above.
- Generate a summarised list of issues
- check if document is as per DELLS standard\n"""

      chunk_title = f"extracted"
      previous_responses = ""
      context_awareness = """RULES:
- DO NOT consider placeholders (<placeholder>) as issues. 
- DO NOT evaluate the content for formating like page numbers, headers, footers, etc.
"""

      OutputFormat = """Return ONLY valid JSON in this format:
[
{{
  "issues": "List of Issues found",
  "kpis": {
    "grammar_accuracy": {{"score": 0.0, "reason": "one sentence"}},
    "spelling_accuracy": {{"score": 0.0, "reason": "one sentence"}},
    "coherence": {{"score": 0.0, "reason": "one sentence"}},
    "clarity": {{"score": 0.0, "reason": "one sentence"}},
    "relevance": {{"score": 0.0, "reason": "one sentence"}},
    "coverage": {{"score": 0.0, "reason": "one sentence"}},
    "redundancy": {{"score": 0.0, "reason": "one sentence"}}
  }
}}
]"""
    else:
     
      if is_first_chunk:
        task_steps = """STEPS (FIRST CHUNK EVALUATION):
- Evaluate the content using the KPIs defined above
- Generate initial list of ISSUES found in this first chunk
- Assess document content against Dell template standards
- Establish baseline scores for subsequent chunks to build upon"""
        context_awareness =""
        previous_responses = ""
      else:
        task_steps = """STEPS:
- Evaluate current chunk content using above KPIs
- Update and maintain cumulative list of ISSUES found across all chunks
- Assess document content with Dell template standards
- Re-evaluate and update KPI scores based on cumulative document assessment
""" 
        context_awareness =f"""CONTEXT AWARENESS:
- This is an incremental evaluation building upon previous chunks
- Maintain consistency with scores and observations from earlier chunks
- Track document content evolution across chunks
- Use the responses from previous chunks to understand the overall content of the document.
- List of issues should reflect findings for entire document
- DO NOT penalize for missing sections that are covered in later chunks
- DO NOT consider PLACEHOLDERS (<placeholder>) as an ISSUES\n"""
        previous_responses = f"""USE BELOW {len(response_context)} RESPONSES FROM PREVIOUS CHUNKS:
======================================================================
{'\n'.join([f'Chunk {len(response_context)-idx}: {str(resp)}' for idx, resp in enumerate(reversed(response_context))])}
======================================================================\n"""

      OutputFormat = """Return ONLY valid JSON in this format:
[{{
  "issues": "List of Issues found in this chunk",
  "kpis": {
    "grammar_accuracy": {{"score": 0.0, "reason": "one sentence"}},
    "spelling_accuracy": {{"score": 0.0, "reason": "one sentence"}},
    "coherence": {{"score": 0.0, "reason": "one sentence"}},
    "clarity": {{"score": 0.0, "reason": "one sentence"}},
    "relevance": {{"score": 0.0, "reason": "one sentence"}},
    "coverage": {{"score": 0.0, "reason": "one sentence"}},
    "redundancy": {{"score": 0.0, "reason": "one sentence"}}
  }
}}]"""
    
    return f"""
{role_def}

{task_instruction}

{document_details}
{previous_responses if len(previous_responses) > 0 else ''}
{context_awareness if len(context_awareness) > 0 else ''}
EVALUATION CRITERIA:
* Coverage: Breadth of topics covered; completeness of information (0-100).
  - 0-30: Very limited or missing major sections
  - 30-60: Partial coverage with noticeable gaps
  - 60-80: Good coverage with minor gaps
  - 80-100: Comprehensive and complete
* Clarity: Readability and structure; clear organization (0-100).
  - 0-30: Hard to understand, poorly structured
  - 30-60: Some structure but lacks clarity
  - 60-80: Mostly clear with minor issues
  - 80-100: Very clear and well structured
* Coherence: Logical flow and consistency throughout (0-100).
  - 0-30: Disjointed, no logical flow
  - 30-60: Some flow but inconsistent
  - 60-80: Logical flow with minor gaps
  - 80-100: Strong logical consistency
* Relevance: How well content aligns with title of the document (0-100).
  - 0-30: Mostly irrelevant content
  - 30-60: Partially aligned
  - 60-80: Mostly relevant
  - 80-100: Highly aligned with document title
* Redundancy: Content efficiency and minimal repetition (0-100, higher = better).
  - 0-30: Highly repetitive
  - 30-60: Some repetition
  - 60-80: Minimal repetition
  - 80-100: Concise and efficient
* Grammar Accuracy: Correctness of grammar and syntax (0-100).
  - 0-30: Frequent grammatical errors
  - 30-60: Some grammatical errors
  - 60-80: Minor grammatical errors
  - 80-100: No grammatical errors
* Spelling Accuracy: Correctness of spelling (0-100).
  - 0-30: Frequent spelling errors
  - 30-60: Some spelling errors
  - 60-80: Minor spelling errors
  - 80-100: No spelling errors
  
{task_steps}
READING INSTRUCTIONS:
- Lines between `--- Table Start ---` and `--- Table End ---` represent key-value dictionary format generated from tables in the document.
- Images referenced in the text are represented as `[IMAGE: <image name>]`.
- Text enclosed in < > brackets represents placeholders.
- Lines having `~` delimiter are headers of empty tables.
- DO NOT consider placeholders (<placeholder>) as issues.

INPUT TEXT:- {chunk_title if is_chunked else ""}
<<<
{chunk}
>>>

{OutputFormat.lstrip()}
"""

############################################################################################
# Prompt Builder for .pptx files
############################################################################################
def build_ppt_contentcheck_prompt(chunk: str, document_title: str, chunk_title: str, description:str = None, response_context: list[dict] = None, document_type: str = None, phase: str = None) -> str:
    """Incremental KPI-based content validation for Dell/Dell-family documents."""
  #  logger.info('Using prompt version:- 00000000000000000006')
    is_chunked = chunk_title.strip() != 'Chunk 1/1'
    is_first_chunk = chunk_title.startswith('Chunk 1/')
    role_def = 'You are Technical Reviewer for Dell group of companies'

    # if offer is None:
    #   role_def = 'You are Documentation ANALYST for Dell group of companies'
    # else:
    #   role_def = f'You are CONSULTANT of {offer.upper()}.'
    document_details = f"PPT METADATA:\n\tTITLE: {document_title.upper()}\n"
    if description is not None:
      document_details = document_details + f"\tDESCRIPTION: {description}\n"
    if document_type is not None:
      # doc_instruction = f'Provided text is {chunk_title.upper()} from {document_type.upper().strip('s')} document titled {document_title.upper()}.'
      document_details = document_details + f"\tTYPE: {document_type.upper().strip('s')}\n"
    if phase is not None:
      document_details = document_details + f"\tPHASE (of project used in): {phase.upper()}\n"

    task_instruction = f"Your task is to EVALUATE and VALIDATE the CONTENT of a TEMPLATE PPT presentation and provide KPI Scoring"

    if (not is_chunked):
      task_steps = """STEPS:
- Evaluate the content for the KPIs defined above.
- Generate a summarised list of issues
- check if PPT presentation is as per DELLS standard\n"""

      chunk_title = f"extracted"
      previous_responses = ""
      context_awareness = """RULES:
- DO NOT consider placeholders (<placeholder>) as issues. 
- DO NOT evaluate the content for formating like page numbers, headers, footers, etc.
"""

      OutputFormat = """Return ONLY valid JSON in this format:
[
{{
  "issues": "List of Issues found",
  "kpis": {
    "grammar_accuracy": {{"score": 0.0, "reason": "one sentence"}},
    "spelling_accuracy": {{"score": 0.0, "reason": "one sentence"}},
    "coherence": {{"score": 0.0, "reason": "one sentence"}},
    "clarity": {{"score": 0.0, "reason": "one sentence"}},
    "relevance": {{"score": 0.0, "reason": "one sentence"}},
    "coverage": {{"score": 0.0, "reason": "one sentence"}},
    "redundancy": {{"score": 0.0, "reason": "one sentence"}}
  }
}}
]"""
    else:
     
      if is_first_chunk:
        task_steps = """STEPS (FIRST CHUNK EVALUATION):
- Evaluate the content using the KPIs defined above
- Generate initial list of ISSUES found in this first chunk
- Assess PPT presentation content against Dell template standards
- Establish baseline scores for subsequent chunks to build upon"""
        context_awareness =""
        previous_responses = ""
      else:
        task_steps = """STEPS:
- Evaluate current chunk content using above KPIs
- Update and maintain cumulative list of ISSUES found across all chunks
- Assess PPT presentation content with Dell template standards
- Re-evaluate and update KPI scores based on cumulative PPT presentation assessment
""" 
        context_awareness =f"""CONTEXT AWARENESS:
- This is an incremental evaluation building upon previous chunks
- Maintain consistency with scores and observations from earlier chunks
- Track PPT presentation content evolution across chunks
- Use the responses from previous chunks to understand the overall content of the PPT presentation.
- List of issues should reflect findings for entire PPT presentation
- DO NOT penalize for missing sections that are covered in later chunks
- DO NOT consider PLACEHOLDERS (<placeholder>) as an ISSUES\n"""
        previous_responses = f"""USE BELOW {len(response_context)} RESPONSES FROM PREVIOUS CHUNKS:
======================================================================
{'\n'.join([f'Chunk {len(response_context)-idx}: {str(resp)}' for idx, resp in enumerate(reversed(response_context))])}
======================================================================\n"""

      OutputFormat = """Return ONLY valid JSON in this format:
[{{
  "issues": "List of Issues found in this chunk",
  "kpis": {
    "grammar_accuracy": {{"score": 0.0, "reason": "one sentence"}},
    "spelling_accuracy": {{"score": 0.0, "reason": "one sentence"}},
    "coherence": {{"score": 0.0, "reason": "one sentence"}},
    "clarity": {{"score": 0.0, "reason": "one sentence"}},
    "relevance": {{"score": 0.0, "reason": "one sentence"}},
    "coverage": {{"score": 0.0, "reason": "one sentence"}},
    "redundancy": {{"score": 0.0, "reason": "one sentence"}}
  }
}}]"""
    
    return f"""
{role_def}

{task_instruction}

{document_details}
{previous_responses if len(previous_responses) > 0 else ''}
{context_awareness if len(context_awareness) > 0 else ''}
EVALUATION CRITERIA:
* Coverage: Breadth of topics covered; completeness of information (0-100).
  - 0-30: Very limited or missing major sections
  - 30-60: Partial coverage with noticeable gaps
  - 60-80: Good coverage with minor gaps
  - 80-100: Comprehensive and complete
* Clarity: Readability and structure; clear organization (0-100).
  - 0-30: Hard to understand, poorly structured
  - 30-60: Some structure but lacks clarity
  - 60-80: Mostly clear with minor issues
  - 80-100: Very clear and well structured
* Coherence: Logical flow and consistency throughout (0-100).
  - 0-30: Disjointed, no logical flow
  - 30-60: Some flow but inconsistent
  - 60-80: Logical flow with minor gaps
  - 80-100: Strong logical consistency
* Relevance: How well content aligns with METADATA of the PPT presentation (0-100).
  - 0-30: Mostly irrelevant content
  - 30-60: Partially aligned
  - 60-80: Mostly relevant
  - 80-100: Highly aligned with METADATA of the PPT presentation
* Redundancy: Content efficiency and minimal repetition (0-100, higher = better).
  - 0-30: Highly repetitive
  - 30-60: Some repetition
  - 60-80: Minimal repetition
  - 80-100: Concise and efficient
* Grammar Accuracy: Correctness of grammar and syntax (0-100).
  - 0-30: Frequent grammatical errors
  - 30-60: Some grammatical errors
  - 60-80: Minor grammatical errors
  - 80-100: No grammatical errors
* Spelling Accuracy: Correctness of spelling (0-100).
  - 0-30: Frequent spelling errors
  - 30-60: Some spelling errors
  - 60-80: Minor spelling errors
  - 80-100: No spelling errors
  
{task_steps}
READING INSTRUCTIONS:
- Lines between `--- Table Start ---` and `--- Table End ---` represent key-value dictionary format generated from tables in the PPT presentation.
- Images referenced in the text are represented as `[IMAGE: <image name>]`.
- Text enclosed in < > brackets represents placeholders.
- Lines having `~` delimiter are headers of empty tables.
- DO NOT consider placeholders (<placeholder>) as issues.

INPUT TEXT:- {chunk_title if is_chunked else ""}
<<<
{chunk}
>>>

{OutputFormat.lstrip()}
"""

############################################################################################
# Prompt Builder for .xlsx files
############################################################################################
def build_xls_contentcheck_prompt(chunk: str, document_title: str, chunk_title: str, description:str = None, response_context: list[dict] = None, document_type: str = None, phase: str = None) -> str:
    """Incremental KPI-based content validation for Dell/Dell-family documents."""
  #  logger.info('Using prompt version:- 00000000000000000006')
    is_chunked = chunk_title.strip() != 'Chunk 1/1'
    is_first_chunk = chunk_title.startswith('Chunk 1/')
    role_def = 'You are Technical Reviewer for Dell group of companies'

    # if offer is None:
    #   role_def = 'You are Documentation ANALYST for Dell group of companies'
    # else:
    #   role_def = f'You are CONSULTANT of {offer.upper()}.'
    document_details = f"EXCEL FILE METADATA:\n\tTITLE: {document_title.upper()}\n"
    if description is not None:
      document_details = document_details + f"\tDESCRIPTION: {description}\n"
    if document_type is not None:
      # doc_instruction = f'Provided text is {chunk_title.upper()} from {document_type.upper().strip('s')} document titled {document_title.upper()}.'
      document_details = document_details + f"\tTYPE: {document_type.upper().strip('s')}\n"
    if phase is not None:
      document_details = document_details + f"\tPHASE (of project used in): {phase.upper()}\n"

    task_instruction = f"Your task is to EVALUATE and VALIDATE the CONTENT of a TEMPLATE EXCEL FILE and provide KPI Scoring"

    if (not is_chunked):
      task_steps = """STEPS:
- Evaluate the content for the KPIs defined above.
- Generate a summarised list of issues
- check if excel file is as per DELLS standard\n"""

      chunk_title = f"extracted"
      previous_responses = ""
      context_awareness = """RULES:
- DO NOT consider placeholders (<placeholder>) as issues. 
"""

      OutputFormat = """Return ONLY valid JSON in this format:
[
{{
  "issues": "List of Issues found",
  "kpis": {
    "grammar_accuracy": {{"score": 0.0, "reason": "one sentence"}},
    "spelling_accuracy": {{"score": 0.0, "reason": "one sentence"}},
    "coherence": {{"score": 0.0, "reason": "one sentence"}},
    "clarity": {{"score": 0.0, "reason": "one sentence"}},
    "relevance": {{"score": 0.0, "reason": "one sentence"}},
    "coverage": {{"score": 0.0, "reason": "one sentence"}},
    "redundancy": {{"score": 0.0, "reason": "one sentence"}}
  }
}}
]"""
    else:
     
      if is_first_chunk:
        task_steps = """STEPS (FIRST CHUNK EVALUATION):
- Evaluate the content using the KPIs defined above
- Generate initial list of ISSUES found in this first chunk
- Assess excel file content against Dell template standards
- Establish baseline scores for subsequent chunks to build upon"""
        context_awareness =""
        previous_responses = ""
      else:
        task_steps = """STEPS:
- Evaluate current chunk content using above KPIs
- Update and maintain cumulative list of ISSUES found across all chunks
- Assess excel file content with Dell template standards
- Re-evaluate and update KPI scores based on cumulative excel file assessment
""" 
        context_awareness =f"""CONTEXT AWARENESS:
- This is an incremental evaluation building upon previous chunks
- Maintain consistency with scores and observations from earlier chunks
- Track excel file content evolution across chunks
- Use the responses from previous chunks to understand the overall content of the excel file.
- List of issues should reflect findings for entire excel file
- DO NOT penalize for missing sections that are covered in later chunks
- DO NOT consider PLACEHOLDERS (<placeholder>) as an ISSUES\n"""
        previous_responses = f"""USE BELOW {len(response_context)} RESPONSES FROM PREVIOUS CHUNKS:
======================================================================
{'\n'.join([f'Chunk {len(response_context)-idx}: {str(resp)}' for idx, resp in enumerate(reversed(response_context))])}
======================================================================\n"""

      OutputFormat = """Return ONLY valid JSON in this format:
[{{
  "issues": "List of Issues found in this chunk",
  "kpis": {
    "grammar_accuracy": {{"score": 0.0, "reason": "one sentence"}},
    "spelling_accuracy": {{"score": 0.0, "reason": "one sentence"}},
    "coherence": {{"score": 0.0, "reason": "one sentence"}},
    "clarity": {{"score": 0.0, "reason": "one sentence"}},
    "relevance": {{"score": 0.0, "reason": "one sentence"}},
    "coverage": {{"score": 0.0, "reason": "one sentence"}},
    "redundancy": {{"score": 0.0, "reason": "one sentence"}}
  }
}}]"""
    
    return f"""
{role_def}

{task_instruction}

{document_details}
{previous_responses if len(previous_responses) > 0 else ''}
{context_awareness if len(context_awareness) > 0 else ''}
EVALUATION CRITERIA:
* Coverage: Breadth of topics covered; completeness of information (0-100).
  - 0-30: Very limited or missing major sections
  - 30-60: Partial coverage with noticeable gaps
  - 60-80: Good coverage with minor gaps
  - 80-100: Comprehensive and complete
* Clarity: Readability and structure; clear organization (0-100).
  - 0-30: Hard to understand, poorly structured
  - 30-60: Some structure but lacks clarity
  - 60-80: Mostly clear with minor issues
  - 80-100: Very clear and well structured
* Coherence: Logical flow and consistency throughout (0-100).
  - 0-30: Disjointed, no logical flow
  - 30-60: Some flow but inconsistent
  - 60-80: Logical flow with minor gaps
  - 80-100: Strong logical consistency
* Relevance: How well content aligns with METADATA of excel file (0-100).
  - 0-30: Mostly irrelevant content
  - 30-60: Partially aligned
  - 60-80: Mostly relevant
  - 80-100: Highly aligned with METADATA of excel file
* Redundancy: Content efficiency and minimal repetition (0-100, higher = better).
  - 0-30: Highly repetitive
  - 30-60: Some repetition
  - 60-80: Minimal repetition
  - 80-100: Concise and efficient
* Grammar Accuracy: Correctness of grammar and syntax (0-100).
  - 0-30: Frequent grammatical errors
  - 30-60: Some grammatical errors
  - 60-80: Minor grammatical errors
  - 80-100: No grammatical errors
* Spelling Accuracy: Correctness of spelling (0-100).
  - 0-30: Frequent spelling errors
  - 30-60: Some spelling errors
  - 60-80: Minor spelling errors
  - 80-100: No spelling errors
  
{task_steps}
READING INSTRUCTIONS:
- Lines between `--- Table Start ---` and `--- Table End ---` represent key-value dictionary format generated from tables in the excel file.
- Images referenced in the text are represented as `[IMAGE: <image name>]`.
- Text enclosed in < > brackets represents placeholders.
- Lines having `~` delimiter are headers of empty tables.
- DO NOT consider placeholders (<placeholder>) as issues.

INPUT TEXT:- {chunk_title if is_chunked else ""}
<<<
{chunk}
>>>

{OutputFormat.lstrip()}
"""


############################################################################################
# Prompt Builder for .xlsx files
############################################################################################
def build_others_contentcheck_prompt(chunk: str, document_title: str, chunk_title: str, description:str = None, response_context: list[dict] = None, document_type: str = None, phase: str = None) -> str:
    """Incremental KPI-based content validation for Dell/Dell-family documents."""
  #  logger.info('Using prompt version:- 00000000000000000006')
    is_chunked = chunk_title.strip() != 'Chunk 1/1'
    is_first_chunk = chunk_title.startswith('Chunk 1/')
    role_def = 'You are Technical Reviewer for Dell group of companies'

    # if offer is None:
    #   role_def = 'You are Documentation ANALYST for Dell group of companies'
    # else:
    #   role_def = f'You are CONSULTANT of {offer.upper()}.'
    document_details = f"FILE METADATA:\n\tTITLE: {document_title.upper()}\n"
    if description is not None:
      document_details = document_details + f"\tDESCRIPTION: {description}\n"
    if document_type is not None:
      # doc_instruction = f'Provided text is {chunk_title.upper()} from {document_type.upper().strip('s')} document titled {document_title.upper()}.'
      document_details = document_details + f"\tTYPE: {document_type.upper().strip('s')}\n"
    if phase is not None:
      document_details = document_details + f"\tPHASE (of project used in): {phase.upper()}\n"

    task_instruction = f"Your task is to EVALUATE and VALIDATE the CONTENT of a TEMPLATE FILE and provide KPI Scoring"

    if (not is_chunked):
      task_steps = """STEPS:
- Evaluate the content for the KPIs defined above.
- Generate a summarised list of issues
- check if file is as per DELLS standard\n"""

      chunk_title = f"extracted"
      previous_responses = ""
      context_awareness = """RULES:
- DO NOT consider placeholders (<placeholder>) as issues. 
- DO NOT evaluate the content for formating like page numbers, headers, footers, etc.
"""

      OutputFormat = """Return ONLY valid JSON in this format:
[
{{
  "issues": "List of Issues found",
  "kpis": {
    "grammar_accuracy": {{"score": 0.0, "reason": "one sentence"}},
    "spelling_accuracy": {{"score": 0.0, "reason": "one sentence"}},
    "coherence": {{"score": 0.0, "reason": "one sentence"}},
    "clarity": {{"score": 0.0, "reason": "one sentence"}},
    "relevance": {{"score": 0.0, "reason": "one sentence"}},
    "coverage": {{"score": 0.0, "reason": "one sentence"}},
    "redundancy": {{"score": 0.0, "reason": "one sentence"}}
  }
}}
]"""
    else:
     
      if is_first_chunk:
        task_steps = """STEPS (FIRST CHUNK EVALUATION):
- Evaluate the content using the KPIs defined above
- Generate initial list of ISSUES found in this first chunk
- Assess file content against Dell template standards
- Establish baseline scores for subsequent chunks to build upon"""
        context_awareness =""
        previous_responses = ""
      else:
        task_steps = """STEPS:
- Evaluate current chunk content using above KPIs
- Update and maintain cumulative list of ISSUES found across all chunks
- Assess file content with Dell template standards
- Re-evaluate and update KPI scores based on cumulative file assessment
""" 
        context_awareness =f"""CONTEXT AWARENESS:
- This is an incremental evaluation building upon previous chunks
- Maintain consistency with scores and observations from earlier chunks
- Track file content evolution across chunks
- Use the responses from previous chunks to understand the overall content of the file.
- List of issues should reflect findings for entire file
- DO NOT penalize for missing sections that are covered in later chunks
- DO NOT consider PLACEHOLDERS (<placeholder>) as an ISSUES\n"""
        previous_responses = f"""USE BELOW {len(response_context)} RESPONSES FROM PREVIOUS CHUNKS:
======================================================================
{'\n'.join([f'Chunk {len(response_context)-idx}: {str(resp)}' for idx, resp in enumerate(reversed(response_context))])}
======================================================================\n"""

      OutputFormat = """Return ONLY valid JSON in this format:
[{{
  "issues": "List of Issues found in this chunk",
  "kpis": {
    "grammar_accuracy": {{"score": 0.0, "reason": "one sentence"}},
    "spelling_accuracy": {{"score": 0.0, "reason": "one sentence"}},
    "coherence": {{"score": 0.0, "reason": "one sentence"}},
    "clarity": {{"score": 0.0, "reason": "one sentence"}},
    "relevance": {{"score": 0.0, "reason": "one sentence"}},
    "coverage": {{"score": 0.0, "reason": "one sentence"}},
    "redundancy": {{"score": 0.0, "reason": "one sentence"}}
  }
}}]"""
    
    return f"""
{role_def}

{task_instruction}

{document_details}
{previous_responses if len(previous_responses) > 0 else ''}
{context_awareness if len(context_awareness) > 0 else ''}
EVALUATION CRITERIA:
* Coverage: Breadth of topics covered; completeness of information (0-100).
  - 0-30: Very limited or missing major sections
  - 30-60: Partial coverage with noticeable gaps
  - 60-80: Good coverage with minor gaps
  - 80-100: Comprehensive and complete
* Clarity: Readability and structure; clear organization (0-100).
  - 0-30: Hard to understand, poorly structured
  - 30-60: Some structure but lacks clarity
  - 60-80: Mostly clear with minor issues
  - 80-100: Very clear and well structured
* Coherence: Logical flow and consistency throughout (0-100).
  - 0-30: Disjointed, no logical flow
  - 30-60: Some flow but inconsistent
  - 60-80: Logical flow with minor gaps
  - 80-100: Strong logical consistency
* Relevance: How well content aligns with METADATA of the FILE (0-100).
  - 0-30: Mostly irrelevant content
  - 30-60: Partially aligned
  - 60-80: Mostly relevant
  - 80-100: Highly aligned with METADATA of the file
* Redundancy: Content efficiency and minimal repetition (0-100, higher = better).
  - 0-30: Highly repetitive
  - 30-60: Some repetition
  - 60-80: Minimal repetition
  - 80-100: Concise and efficient
* Grammar Accuracy: Correctness of grammar and syntax (0-100).
  - 0-30: Frequent grammatical errors
  - 30-60: Some grammatical errors
  - 60-80: Minor grammatical errors
  - 80-100: No grammatical errors
* Spelling Accuracy: Correctness of spelling (0-100).
  - 0-30: Frequent spelling errors
  - 30-60: Some spelling errors
  - 60-80: Minor spelling errors
  - 80-100: No spelling errors
  
{task_steps}

INPUT TEXT:- {chunk_title if is_chunked else ""}
<<<
{chunk}
>>>

{OutputFormat.lstrip()}
"""
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# PROMPTS FOR RECOMMENDATIONS
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++