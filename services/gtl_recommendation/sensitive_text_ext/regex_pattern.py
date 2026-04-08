"""Regex patterns for identifying sensitive information."""

# REGEX_NETWORK ={
#     'URL': r'(?:https?://[^\s"\'<>]+|www\.[^\s"\'<>]+)'
# }
 
REGEX_PATTERNS = {
    # Email patterns
    'INTERNAL_EMAIL': r'\b[A-Za-z0-9._%+-]+@(?:dell|corp|internal|example)\.[A-Za-z]{2,}\b',
    'EMAIL_ADDRESS': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b',
    # Phone and identification numbers
    'PHONE': r'\+(?:\d{1,3}[-\s]?)?(?:\(\d{1,4}\)|\d{1,4})(?:[-\s]?\d{3,4}){2,3}\b',
    'SSN': r'\b\d{3}-\d{2}-\d{4}\b',
   
    # Network and URLs
    'HTTP_URL': r'\b(?:https?://[^\s"\'<>]+|www\.[^\s"\'<>]+)\b',
    # 'HTTP_URL': r'((?:https?://[^\s"\'<>]+|www\.[^\s"\'<>]+))',
    # 'SERVER': r"Server[:=]\s*([A-Za-z0-9._-]+)",
    # 'SERVER': r"Server Name:\s*([a-zA-Z0-9.-]+\.[a-zA-Z]{2,}.(com|local|net))",
    'SERVER': r'\bServer[:=]\s*([A-Za-z0-9._-]+\b)',  # Fixed
    'SERVER_NAME': r'\bServer Name:\s*([a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\.(?:com|local|net))\b',
    
    # 'IP_ADDRESS': r'\b((?:(?:25[0-5]|2[0-4]\d|1?\d?\d)\.){3}(?:25[0-5]|2[0-4]\d|1?\d?\d)\b)',
    'IP_ADDRESS': r'\b(((?:(?:25[0-5]|2[0-4]\d|1?\d?\d)\.){3}(?:25[0-5]|2[0-4]\d|1?\d?\d))(?:/(?:[12]?\d|3[0-2]))?)\b',
    # 'IP_ADDRESS': r'\b((?:(?:25[0-5]|2[0-4]\d|1?\d?\d)\.){3}(?:25[0-5]|2[0-4]\d|1?\d?\d))(?:/(?:[12]?\d|3[0-2]))?\b',
    # 'AD_DOMAIN' : r'((?:[a-zA-Z0-9-]+\.)+(com|local))|((?:DC=[a-zA-Z0-9_-]+)(?:,DC=[a-zA-Z0-9_-]+)+)',
    # 'WIND_FILESHARE_NAME': r'\\\\[a-zA-Z0-9._-]+\\[a-zA-Z0-9._-]+(?:\\[a-zA-Z0-9._-]+)*',
 
    # Credentials and secrets
    'PASSWORDS': r'\bpassword\s*[:=]\s*(["\']?[^\s"\'\\]+["\']?)',
    'SESSION_ID': r'\b(?:session[_-]?id|sessionid)\s*[:=]\s*(["\']?[^\s"\'\\]+["\']?)',
    # 'SESSION_ID': r'\bsessionid\s*[:=]\s*(["\']?[^\s"\'\\]+["\']?)',
    'API_KEYS': r'\b(?:(?:key|token|api[_\s]*key|bearer)\b\s*[:=\-]*\s*["\']?[A-Za-z0-9_\-]{20,}|AIza[0-9A-Za-z\-_]{35}|AKIA[0-9A-Z]{16}|sk_test_[0-9a-zA-Z]{24,}|sk_live_[0-9a-zA-Z]{24,}|ghp_[A-Za-z0-9]{36}|github_pat_[A-Za-z0-9_]{22,255}|xox[baprs]-[A-Za-z0-9]{10,48}|eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,})\b',
    'AUTH_TOKEN': r'(?:Bearer\s+[A-Za-z0-9\-\._~\+\/]+=*)|(?:eyJ[0-9A-Za-z_-]+\.[0-9A-Za-z_-]+\.[0-9A-Za-z_-]+)|(?:[A-Za-z0-9\+\/]{32,}={0,2})|(?:[A-Fa-f0-9]{32,})',
   
    # Cookies and encryption
    # 'COOKIES': r'(?:^|(?<=\s))(?:cookie|set-cookie)\s*:\s*[^;\n\r]+(?:;[^;\n\r]+)*',
    'COOKIES': r'\b(?:cookie|set-cookie)\s*:\s*[^;\n\r]+(?:;[^;\n\r]+)*',
    # 'COOKIES': r"(?:^|\b)(?:cookie|set-cookie)\s*:\s*[^;\n\r]+(?:;[^;\n\r]+)*",
    'PASSPHRASE': r'\b(?:passphrase|pass[_-]?phrase|ssh[_-]?passphrase)\s*[:=]\s*(["\']?[^\s"\';]+["\']?)',
    # 'PASSPHRASE': r'passphrase|pass[_-]?phrase|ssh[_-]?passphrase\s*[:=]\s*((["\']?)([^\s"\';]+)\2)',
    # 'ENCRYPTION_KEY': r'-----BEGIN [A-Z ]+-----[\s\S]+?-----END [A-Z ]+-----|(?:[A-Za-z0-9+/]{32,}={0,2})',
   
    # AD & network naming
    # 'AD_DOMAIN': r'\b[a-z0-9\-]+\.(?:local|corp|internal|intra|lan)\b',
    # 'AD_DN': r'\b(?:OU|DC|CN)=[A-Za-z0-9 _-]+(?:,(?:OU|DC|CN)=[A-Za-z0-9 _-]+)+\b',
    # 'AD_SITE': r'(?i)\b(ad\s*site|site\s*name|forest)\b[:\s\-]*([A-Za-z0-9._-]+)',
    # 'AD_OU': r'(?i)\bOU\s*[:=\-]\s*([A-Za-z0-9 _/-]+)',
 
    # Business identifiers
    # 'NETWORK_DEVICE': r'\b(?:srv|server|db|web|git|ci|edge|fw|gw|lb)[-_]?[A-Za-z0-9\-]{2,}\b',
    # 'FIREWALL_RULE': r'(?i)\b(allow|deny|block)\s+(tcp|udp|icmp)\s+\d{1,5}\s+(from|src)\s+\S+\s+(to|dst)\s+\S+',
    # 'INTERNAL_PROJECT': r'(?i)\b(project|codename|operation)\s*[:\s\-]*([A-Za-z0-9][A-Za-z0-9._\- ]{1,40})',
    # 'SERVICE_TAG': r'(?i)\b(service\s*tag|svc\s*tag)[:\s\-]*([A-Z0-9\-]{5,12})',
}
 