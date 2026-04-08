IMAGE_CATEGORY = {
    "DIAGRAM": [
        "Flowchart / process diagram",
        "System / network architecture diagram",
        "Org chart / hierarchy diagram",
        "Generic diagram / chart / graph"
    ],
    "LOGO": [
        "CUSTOMER LOGO",
        "DELL TECHNOLOGIES LOGO",
        "INTERNAL LOGO",
        "VENDOR LOGO"
    ],
    "SIGNATURE AND STAMP": [
        "Handwritten Signature",
        "Digital Signature",
        "Official Seal"
    ],
    "INTELLECTUAL PROPERTY": [
        "Product design",
        "Proprietary algorithm",
        "Blueprint",
        "Patent application"
    ],
    "FINANCIAL DETAIL": [
        "Bank Statement excerpt",
        "Invoice Data",
        "Billing Detail",
        "Accounting/GL codes",
        "Non-public financial report"
    ],
    "INTERNAL": [
        "Internal to DELL TECHNOLOGIES",
        "DELL TECHNOLOGIES Partners content",
        "UI screenshot / Dell application screen"
    ],
    "OTHER": [
        "Photograph / realistic picture",
        "Scanned text page (no clear diagram)",
        "Garbage / low-information image",
        "Any Other Image"
    ],
}


def get_image_detection_prompt() -> str:
    img_cat = "\n".join(
        f"Category: {k}\n\tSub-Categories: {', '.join(v)}"
        for k, v in IMAGE_CATEGORY.items()
    )

    prompt = f"""
You are an AI Assistant for DELL TECHNOLOGIES.
Analyze the image and classify it into one category.

Step 1: DESCRIBE
- context of what the image is depicting (maximum 5 lines)

Step 2: CATEGORIZE
- Pick EXACTLY ONE category and ONE sub-category from the list below.
- Confidence score must be between 0.000 and 1.000.

Visual rules:
- If the image is a photo of physical hardware (servers, racks, chassis, cables, devices) with no boxes/arrows/diagram shapes:
  -> NEVER use any "DIAGRAM" sub-category.
  -> Use Category "OTHER" and Sub-category "Any Other Image".
- If the image is mostly plain text (code, config, logs, documents) with NO boxes, arrows, or graph axes:
  -> NEVER use any "DIAGRAM" sub-category.
  -> Prefer:
     - Category "INTERNAL" and sub-category "Internal to DELL TECHNOLOGIES" for Dell-related/internal text, OR
     - Category "OTHER" and sub-category "Scanned text page (no clear diagram)" for generic text.
- "Flowchart / process diagram": steps or decisions connected with arrows or lines.
- "System / network architecture diagram": servers, services, databases or networks shown as BOXES/ICONS with connecting lines (not photos).
- "Org chart / hierarchy diagram": people or roles arranged in a tree or hierarchy.
- "Generic diagram / chart / graph": charts, graphs, matrices or abstract diagrams.
- If the image is mostly blank, tiny, heavily blurred or meaningless artifacts:
  -> use Category "OTHER" and Sub-category "Garbage / low-information image".

Sensitivity rules:
- Use "INTERNAL" when:
  - visible text clearly shows Dell internal or partner-only content, OR
  - the image is a UI or application screenshot from a Dell or partner tool.
- Use "INTELLECTUAL PROPERTY" only for obvious blueprints, patent drawings or proprietary technical designs.
- If unsure, prefer neutral categories like "DIAGRAM" or "OTHER" instead of "INTELLECTUAL PROPERTY".

Ignore all logos, names, or content related to:
DELL TECHNOLOGIES, Dell, Dell EMC, Dell Industries, Dell Inc., Microsoft, Nvidia, Red Hat, Intel.

IMAGE CATEGORIES:
{img_cat}

Example 1:
Image: Front photo of a Dell XE9680 server chassis showing labeled slots.
Output:
[
  {{
    "description": "Front photo of a server chassis with labeled slots.",
    "category": "OTHER",
    "sub-category": "Any Other Image",
    "category-subcategory-score": "0.910",
    "reason": <"Reasoning behind the category-subcateogry pair prediction">
  }}
]

Example 2:
Image: YAML configuration text for NFS and BeeGFS, with no boxes or arrows.
Output:
[
  {{
    "description": "Screenshot of YAML configuration for storage settings.",
    "category": "INTERNAL",
    "sub-category": "Internal to DELL TECHNOLOGIES",
    "category-score": "0.890"
  }}
]

Step 3: RE-VALIDATE
- Check if the description from Step 1 matches the chosen category and sub-category.
- If they do not match, adjust the category or the confidence score.

Return ONLY this JSON array with one object:
[
  {{
    "description": "<concise description from Step 1>",
    "category": "<final category>",
    "sub-category": "<final sub-category>",
    "category-score": "<confidence between 0.000 and 1.000>"
  }}
]

STRICT RULES:
- Exactly 1 category and 1 sub-category.
- Score format like "0.823".
- JSON array with 1 object only.
- No extra text outside the JSON.
"""
    return prompt
