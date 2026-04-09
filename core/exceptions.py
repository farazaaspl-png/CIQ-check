# import sys
# import inspect

class CustomBaseException(Exception):
    def __init__(self, error_code, message, user_message):
        self.error_code = error_code
        self.message = message
        self.user_message= user_message
        super().__init__(message)

    def to_dict(self):
        return {
            'status' : 'FAILED',
            'error_code': self.error_code,
            'error_message': self.user_message,
            'internal_message' : self.message
        }
    
    def __str__(self):
        return  f"Error {self.error_code}: {self.message}"

class UnExpectedError(CustomBaseException):
    """Exception raised when an item is not found."""
    error_code = 'E000'
    user_message='Internal server error occured. Please contact support team.'
    def __init__(self, error = None):
        self.message = f'{self.user_message} Error: {error}'
        super().__init__(self.error_code, self.message, self.user_message)

class TextExtractionForZipError(CustomBaseException):
    """Exception raised when an item is not found."""
    error_code = 'Z000'
    user_message='Text Extraction Failed for Zip Contents.'
    def __init__(self, error = None):
        if error is not None:
            self.message = f'{self.user_message} Error: {error}'
        else: self.message = f'{self.user_message}'
        super().__init__(self.error_code, self.message, self.user_message)

class ZipSummaryGenerationError(CustomBaseException):
    """Exception raised when an item is not found."""
    error_code = 'Z001'
    user_message='Zip Summay Generation Failed.'
    def __init__(self, error = None):
        if error is not None:
            self.message = f'{self.user_message} Error: {error}'
        else: self.message = f'{self.user_message}'
        super().__init__(self.error_code, self.message, self.user_message)

##changedbysiddhesh
class NoValidFilesInZip(CustomBaseException):
    """Exception raised when Zip has no valid files"""
    error_code = 'Z002'
    user_message='Zip has no valid files.'
    def __init__(self, error = None):
        if error is not None:
            self.message = f'{self.user_message} Error: {error}'
        else: self.message = f'{self.user_message}'
        super().__init__(self.error_code, self.message, self.user_message)

class SOWFileNotFoundOnDellAttachments(CustomBaseException):
    """Exception raised when an item is not found."""
    error_code = 'SOW001'
    user_message='Unable to find SOW file on Dell Attachments.'
    def __init__(self):
        self.message = f'{self.user_message}'
        super().__init__(self.error_code, self.message, self.user_message)

class FileFormatNotSupported(CustomBaseException):
    """Exception raised when an item is not found."""
    error_code = 'E001'
    user_message='The uploaded file format is not supported.'
    def __init__(self, fileformat = None):
        if fileformat:
            self.message = f"{self.user_message} - {fileformat}"
        else:
            self.message = self.user_message
        super().__init__(self.error_code, self.message, self.user_message)

class IpTypeNotSupported(CustomBaseException):
    """Exception raised when an item is not found."""
    error_code = 'E002'
    user_message='The uploaded IP Type is not supported.'
    def __init__(self, ip_type = None):
        if ip_type:
            self.message = f"{self.user_message} - {ip_type}"
        else:
            self.message = f'{self.user_message}'
        super().__init__(self.error_code, self.message, self.user_message)

class EmptyFileError(CustomBaseException):
    """Exception raised when a file is empty and contains no extractable text."""
    error_code = 'T001'
    user_message='The file is empty – no text found.'
    def __init__(self, fileid = None, message = ""):
        if message:
            self.message = f"{self.user_message} - {message}"
        else:
            self.message = self.user_message
        if fileid:
            self.message = f"{fileid} - {self.message}" 
        else:
            self.message = self.user_message
        super().__init__(self.error_code, self.message, self.user_message)

class NoSensitiveItemFound(CustomBaseException):
    """Exception raised when an item is not found."""
    error_code = 'R000'
    user_message = "0 Sensitive Items found."
    def __init__(self, fileid = None, message = ""):
        if message:
            self.message = f"{self.user_message} - {message}"
        else:
            self.message = self.user_message
        if fileid:
            self.message = f"{fileid} - {self.message}"
        else:
            self.message = self.user_message
        super().__init__(self.error_code, self.message, self.user_message)

class TextExtractionError(CustomBaseException):
    """Exception raised when an item is not found."""
    error_code = 'T002'
    user_message = "Text extraction failed."
    def __init__(self, Error,fileid = None, message = ""):
        if message:
            self.message = f"{self.user_message} - {message}"
        else:
            self.message = self.user_message
        if fileid:
            self.message = f"{self.message} with Error - {Error} for {fileid}"
        else:
            self.message = self.user_message
        super().__init__(self.error_code, self.message, self.user_message)

class InvalidMetadataError(CustomBaseException):
    """Raised when required fields are missing or malformed in the request."""
    error_code = "E003"
    user_message = "Invalid request metadata."
    def __init__(self, missing_fields: list[str] | str, message: str = ""):
        if message:
            self.message = f"{self.user_message} - {message}"
        else:
            self.message = self.user_message

        if isinstance(missing_fields, list):
            fields = ", ".join(missing_fields)
        else:
            fields = missing_fields
        self.message = f"{self.message} Missing/invalid fields: {fields}"
        super().__init__(self.error_code, self.message, self.user_message)

class NotAStatementOfWork(CustomBaseException):
    """Raised when the uploaded file is not recognised as a Statement of Work."""
    error_code = "E004"
    user_message = "Not a statement of work."
    def __init__(self, fileid: str | None = None, message: str = ""):
        if message:
            self.message = f"{self.user_message} - {message}"
        else:
            self.message = self.user_message

        if fileid:
            self.message = f"{fileid} - {self.message}"
        else:
            self.message = self.user_message
        super().__init__(self.error_code, self.message, self.user_message)

class FileNotFoundError(CustomBaseException):
    """Raised when a required file cannot be located."""
    error_code = "E005"
    user_message = "The requested file could not be found."
    def __init__(self, path: str, message: str = ""):
        if message:
            self.message = f"{self.user_message} - {message}"
        else:
            self.message = self.user_message
        self.message = f"{self.message} Path: {path}"
        super().__init__(self.error_code, self.message, self.user_message)


class FileWriteError(CustomBaseException):
    """Raised when a file exists but cannot be read (permission, corruption, etc.)."""
    error_code = "E013"
    user_message = "Unable to write the file."
    def __init__(self, path: str, original_exc: Exception | None = None, message: str = ""):
        if message:
            self.message = f"{self.user_message} - {message}"
        else:
            self.message = self.user_message
        self.message = f"{self.message} Path: {path}"
        if original_exc:
            self.message += f" Original error: {original_exc}"
        super().__init__(self.error_code, self.message, self.user_message)

class FileReadError(CustomBaseException):
    """Raised when a file exists but cannot be read (permission, corruption, etc.)."""
    error_code = "E006"
    user_message = "Unable to read the file."
    def __init__(self, path: str, original_exc: Exception | None = None, message: str = ""):
        if message:
            self.message = f"{self.user_message} - {message}"
        else:
            self.message = self.user_message
        self.message = f"{self.message} Path: {path}"
        if original_exc:
            self.message += f" Original error: {original_exc}"
        super().__init__(self.error_code, self.message, self.user_message)

class DatabaseReadError(CustomBaseException):
    """Raised when a connection to the DB cannot be established."""
    error_code = "E007"
    user_message = "Failed while reading from Database."
    def __init__(self, message: str = "", error: Exception = None):
        if message:
            self.message = f"{self.user_message} - {message}"
        else:
            self.message = self.user_message
        self.message = f"{self.message} - ERROR: {error}"
        super().__init__(self.error_code, self.message, self.user_message)

class DatabaseWriteError(CustomBaseException):
    """Raised when an INSERT / UPDATE operation fails."""
    error_code = "E008"
    user_message = "Database write failed."
    def __init__(self, message: str = "", error: Exception = None):
        if message:
            self.message = f"{self.user_message} - {message}"
        else:
            self.message = self.user_message
        self.message = f"{self.message} - ERROR: {error}"
        super().__init__(self.error_code, self.message, self.user_message)

class AuthenticationError(CustomBaseException):
    """Raised when authentication with an external service fails."""
    error_code = "E009"
    user_message = "Authentication failed."
    def __init__(self, service_name: str, message: str = ""):
        if message:
            self.message = f"{self.user_message} - {message}"
        else:
            self.message = self.user_message
        self.message = f"{self.message} - Service: {service_name}"
        super().__init__(self.error_code, self.message, self.user_message)


class TimeoutError(CustomBaseException):
    """Raised when a call to an external service exceeds its timeout."""
    error_code = "E010"
    user_message = "Operation timed out."
    def __init__(self, service_name: str, timeout_seconds: int, message: str = ""):
        if message:
            self.message = f"{self.user_message} - {message}"
        else:
            self.message = self.user_message

        self.message = (
            f"{self.message} - Service: {service_name},"
            f"Timeout: {timeout_seconds}s"
        )
        super().__init__(self.error_code, self.message, self.user_message)


class RateLimitError(CustomBaseException):
    """Raised when an external API returns a rate‑limit / throttling response."""
    error_code = "E011"
    user_message = "Rate limit exceeded."
    def __init__(self, service_name: str, retry_after_seconds: int | None = None, message: str = ""):
        if message:
            self.message = f"{self.user_message} - {message}"
        else:
            self.message = self.user_message
        self.message = f"{self.message} - Service: {service_name}"
        if retry_after_seconds is not None:
            self.message += f", Retry‑After: {retry_after_seconds}s"
        super().__init__(self.error_code, self.message, self.user_message)


class DellAttachmentsDownloadError(CustomBaseException):
    """Raised when downloading a file from Dell Attachments fails."""
    error_code = "DA_E001"
    user_message = "Dell Attachment Failed during file download."
    def __init__(self, error: str, fileid: str | None = None, filename: str | None = None, message: str | None = None):
        if message:
            self.message = f"{self.user_message} - {message}"
        else:
            self.message = self.user_message

        if fileid and filename:
            self.message = (
                f"ERROR: {error} occured while downloading file - {filename} "
                f"for {fileid}.\n{message}"
            )
        else:
            self.message = f"{self.message} - ERROR: {error} occured while downloading file."
        super().__init__(self.error_code, self.message, self.user_message)


class DellAttachmentsUploadError(CustomBaseException):
    """Raised when uploading a file to Dell Attachments fails."""
    error_code = "DA_E002"
    user_message = "Unable to upload file to Dell attachments."
    def __init__(self, message: str = ""):
        if message:
            self.message = f"{self.user_message} - {message}"
        else:
            self.message = self.user_message
        super().__init__(self.error_code, self.message, self.user_message)

class DellAttachmentsApiError(CustomBaseException):
    """Exception raised when an item is not found."""
    error_code = 'DA_E003'
    user_message = "Error occured from Api Side."
    def __init__(self, error):
        self.message = f"ERROR: {error} {self.user_message}"
        super().__init__(self.error_code, self.message, self.user_message)

class UnableToFindAnyDocument(CustomBaseException):
    """Exception raised when an item is not found."""
    error_code = 'REF_E001'
    user_message = "Unable to find any document for provided query."
    def __init__(self,message = ""):
        if message:
            self.message = f"{self.user_message} - {message}"
        else:
            self.message = self.user_message
        super().__init__(self.error_code, self.message, self.user_message)

class DocumentsAlreadyVectorized(CustomBaseException):
    """Exception raised when an item is not found."""
    error_code = 'VEC_E001'
    user_message = "Documents for this request id are already vectorized."
    def __init__(self,message = ""):
        if message:
            self.message = f"{self.user_message} - {message}"
        else:
            self.message = self.user_message
        super().__init__(self.error_code, self.message, self.user_message)

class UnableToFindAnyRecommendation(CustomBaseException):
    """Exception raised when an item is not found."""
    error_code = 'REC_E001'
    user_message = "Unable to find any relevant document for this SOW's."
    def __init__(self,message = ""):
        if message:
            self.message = f"{self.user_message} - {message}"
        else:
            self.message = self.user_message
        super().__init__(self.error_code, self.message, self.user_message)

class UnableToGenerateSummary(CustomBaseException):
    """Raised when the system cannot produce a summary for a document."""
    error_code = "E012"
    user_message = "Unable to generate summary."
    def __init__(self, fileid: str | None = None, message: str = ""):
        if message:
            self.message = f"{self.user_message} - {message}"
        else:
            self.message = self.user_message

        if fileid:
            self.message = f"{fileid} - {self.message}"
        super().__init__(self.error_code, self.message, self.user_message)
    







# def list_class_names():
#     current_module = sys.modules[__name__]
#     class_names = {
#     name:obj for name, obj in inspect.getmembers(current_module, inspect.isclass)
#     if obj.__module__ == __name__
#     }
#     return class_names