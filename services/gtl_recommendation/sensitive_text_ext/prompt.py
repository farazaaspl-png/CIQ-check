from core.utility import get_custom_logger
 
logger = get_custom_logger(__name__)
 
ParameterList={
    "Personal":["Customer Organisation Name", "Vendor Organisation Name", "Dell Team Name", "Vendor Team Name", "Customer Team Name", "Dell Internal Project Name", "Vendor Project Name"],
    "People Name":["Customer Person Name", "Vendor Person Name", "Dell Internal Person Name"],
    "AddressAndDates":["Datetime", "Date", "Year", "Month", "Phone Numbers", "Home Address", "Mailing Address"],
    "IdentificationNumber": ["Social Security Number or SSN", "Passport Number", "Driver License Number or DL", "Bank Account Number"],
    "ServiceTickets": ["Service Tag Number", 
                             "Service Request Number",
                             "Incident Number",
                             "Change Request Number", 
                             "Express Service Code",
                             "Serial Number"],
    "Secrets": ["VPN Credential", "SSH Credential", "API Key ", "Authentication Token","Password","Username",
                "Passphrase", "Encryption key(AES)", "SSL/TLS Certificate", "Session ID",
                 "Cookies", 'Client Secret'],
    "NetworkDetails": ["IP Address","MAC Address","Dell Internal Absolute URL", "Customer Specific Absolute URL", 
                       "Server Name", "Internal Server Name", "Computer Name", "Network Device Name",
                       "Windows File Share Name","Active Directory Domain Name","HostName"
                       "Active Directory Domain Forest","Active Directory Site","Active Directory Forest Name",
                       "Active Directory Structure","Active Directory Organisation Unit","Firewall Rule"],
    "SoftwareCode": ["Source Code"] 
                 }
 
System_Prompts = {
    'Personal': "You are a Data Protection Assistant for DELL Group of Companies",
    'People Name': "You are a Data Protection Assistant for DELL Group of Companies",
    'AddressAndDates': "You are a Data Protection Assistant for DELL Group of Companies",
    'IdentificationNumber': "You are a Data Protection Assistant for DELL Group of Companies",
    'ServiceTickets': "You are a Support Agent for DELL Group of Companies",
    'Secrets': "You are a Data Protection Assistant for DELL Group of Companies",
    'NetworkDetails': "You are a Network Security Agent for DELL Group of Companies",
    'SoftwareCode': "You are a Software Engineer for DELL Group of Companies"
}

Pattern_Hints = {"Personal":['DO NOT EXTRACT GENERIC team names like "IT Team", "Support Team", "Network Team", "Security Team", "Data Team" etc.'],
                 "AddressAndDates":['Phone numbers contain digits and may include +, spaces, or dashes.',
                                    'Dates may appear as YYYY, YYYY-MM-DD, DD/MM/YYYY, or Month names. DO NOT CONSIDER INTERVALS like "15 min", "T0+60 Min" etc'],
                 "People Name":["Real human names only, e.g. `John, Smith`,`James Ward`, `Nick Furry`, `Priyankha  Ramu`, `Jyoti Shrestha`"],
                 "ServiceTickets":["Service Tag Number can starts with ST and 7 alphanumeric characters",
                                   "Service Request Number can starts with ST and followed by numeric characters",
                                   "Incident Number can starts with INC and followed by numeric characters",
                                   "Change Request Number can starts with CR and followed by numeric characters",
                                   "Express Service Code can starts with ET and followed 11 numeric characters",
                                   "Serial Number can starts with S/N and followed (20-28 alphanumeric characters"],
                "NetworkDetails":["Internal entities will have word dell in them",
                                  "Firewall rules will be more than one lines and will have words like inbound, outbound, allow, deny, protocol, port, ip, etc."
                                  "Firewall rules should not have Non Firewall rule specific words.",
                                  "MAC Address can be in format XX-XX-XX-XX-XX-XX or XX:XX:XX:XX:XX:XX"],
                 "Secrets":["SSL/TLS certificate STARTS with -----BEGIN CERTIFICATE-----"],
                 "SoftwareCode": ["Source code can be any software code of any programming language with more than 2 lines"]
    
}
 
 
OutputFormat = """Return ONLY valid JSON in this format::  
[
  {{
    "label": "category",
    "sensitivetext": "exact text from input",
    "reason": "why it matches the category",
    "score" : "0-1"
  }}
]"""

Rules = """- Extract ALL the entities that clearly match the categories.
- Extract the EXACT text appearing in the input.
- Ignore placeholders in <> such as <IP_ADDRESS>, <CUSTOMER>.
- Ignore labels such as CUSTOMER, CONTACT, ADDRESS.
- Ignore references to Dell partner companies:
  Microsoft, Nvidia, Red Hat, Intel, Cisco, VMware.
- Ignore entities that are part of those partner references.
- If no entities are found return []."""

for_format = {".xlsx":"INPUT TEXT is extracted from an Excel file.\n All the table data from excel is converted in key-value dictionary format. \n Lines enclosed in === are the excel sheet name",
              ".docx":"INPUT TEXT is extracted from a Word document.\n All the table data from word document is converted in key-value dictionary format."}

def build_prompt(category: str, input_text: str,fileformat:str ='') -> str:
    logger.info('Using prompt version:- 00000000-0000-0000-0000-000000000006')
    return f"""
{System_Prompts[category]}.

TASK
Extract ALL the sensitive entities from the INPUT TEXT and assign a confidence score.
{for_format.get(fileformat, '')}
CATEGORIES 
{'- '+('\n- '.join(ParameterList[category]))}

RULES
{Rules}

{'\nPATTERN HINTS\n'+'- '+('\n- '.join(Pattern_Hints[category]))+'\n' if Pattern_Hints.get(category) is not None else ''}
INPUT TEXT
<<<
{input_text}
>>>

{OutputFormat}
""".strip()

def build_validation_prompt(sensitive_item:list[dict]) -> str:
    # logger.info('Using validation prompt version:- 00000000-0000-0000-0000-000000000002')
    return f"""
You are a Data-Security Assistant for DELL Group of Companies.
 
Task:
- For each item in the input list, examine the matched lines in `contexts` and decide whether the `sensitivetext` truly matches the declared `label`.
- Correct score i.e confidence score (between 0 - 1) of the item indicating how certain you are that the labeling is correct.

Remember below RULES:
- `sensitivetext` should clearly match the `label`.
- `sensitivetext` like below are not sensitive data:
  * Placeholders like IP_ADDRESS, CUSTOMER,  CONTACT, ADDRESS
  * References to Dell partner companies:
    Microsoft, Nvidia, Red Hat, Intel, Cisco, VMware
  * `sensitivetext` related to these partner companies
  * Time interval like `T0 + 120 min`, `T1 + 30 min`
  * ALL generic TECHNICAL words like `TEST-VM`, `servers`, `firewall rules`, `firewall policies`, `Jump Server`, `Applications`, `VmWare`, `Databases`, `network`, `SSL VPN` etc
  * ALL the TECHNICAL team name like `Networks Team`, `System Team`, `Backup Team`, `Tenant Administrator`, `Database Team`, `Security Team`, `Cloud Team`, `DevOps Team`, `IT Operations Team`, `IT Support Team` etc
  * meaningfull words in`sensitivetext` with labels like Authentication tokens, API keys, passwords, certificates etc

 
Input list:
{'\n'.join([str(item) for item in sensitive_item])}
 
Return ONLY a JSON array of objects in this format: 
[
  {{
    "label": "<label same as provided>",
    "sensitivetext": "<sensitive_item same as provided>",
    "score": "<confidence score>",
    "reason": "<why it matches the category>"
  }}
]
""".strip()
