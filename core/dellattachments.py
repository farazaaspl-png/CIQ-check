import asyncio
import inspect
import nest_asyncio
import pendulum
import logging
import json, shutil, time, aiofiles, pandas as pd, requests, httpx
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Optional, List
from httpx import WriteTimeout,AsyncClient
from config import Configuration

from core.db.crud import DatabaseManager
from core.exceptions import DellAttachmentsApiError, DellAttachmentsDownloadError, DellAttachmentsUploadError

from core.utility import get_custom_logger
logger = get_custom_logger(__name__)
# logger.propagate = False
nest_asyncio.apply()

class DellAttachments:
    """Utility class to upload a file to the Dell “attachments” service."""

    # --------------------------------------------------------------------- #
    # Construction & configuration
    # --------------------------------------------------------------------- #
    def __init__(self, debug: bool = False):
        self.debug = debug
        if self.debug:
            logger.setLevel(logging.DEBUG)
        self.cfg = Configuration()
        self.cfg.load_active_config()

        # Config values (all are strings in the ini file)
        self._SERVICE_TYPE = self.cfg.DELL_ATTACHMENTS_SERVICE_TYPE
        self.chunk_size = self.cfg.DELL_ATTACHMENTS_CHUNK_SIZE
        self.base_url = self.cfg.DELL_ATTACHMENTS_URL.rstrip("/")
        self.__auth_url = self.cfg.DELL_ATTACHMENTS_AUTH_URL
        self.__client_id = self.cfg.DELL_ATTACHMENTS_CLIENT_ID
        self.__client_secret = self.cfg.DELL_ATTACHMENTS_CLIENT_SECRET
        self.__USER = self.cfg.DELL_ATTACHMENTS_USER_ID

        # Runtime attributes
        self.__token: Optional[str] = None
        self.__token_expires_at: pendulum.DateTime = pendulum.now().subtract(days=2)
        self.__headers: Dict[str, str] = {}
        self.db = DatabaseManager()

    # --------------------------------------------------------------------- #
    # Helper utilities
    # --------------------------------------------------------------------- #
    @staticmethod
    def _backoff(attempt: int) -> None:
        """Sleep `2 ** attempt + 1` seconds"""
        delay = 10 * (attempt + 1)
        logger.info(f"Retrying in {delay} seconds...")
        time.sleep(delay)

    @staticmethod
    def _retry(
        fn: Callable[[], Any],
        *,
        attempts: int = 3,
        on_error: Optional[Callable[[int, Exception], None]] = None,
        on_auth_error: Optional[Callable[[int, Exception], None]] = None,
    ) -> Any:
        """
        Generic retry wrapper.

        Parameters
        ----------
        fn: Callable
            The operation to execute. It must raise an exception on failure.
        attempts: int
            Number of attempts (including the first try).
        on_error: Callable(attempt, exc) | None
            Optional hook called after each failure.
        """
        attempt = 0
        retried_401 = False

        while attempt < attempts:
            try:
                if inspect.iscoroutinefunction(fn):
                    return asyncio.run(fn())
                else:
                    return fn()
            except (WriteTimeout,Exception) as exc:  # noqa: BLE001 – we want to catch everything
                if isinstance(exc, httpx.HTTPStatusError):
                    if exc.response.status_code == 401 and not retried_401:
                        if on_auth_error:
                            on_auth_error(attempt, exc)
                            retried_401 = True
                            logger.error(f"Attempt {attempt + 1} failed with 401. Re-authenticating and retrying...")
                            continue 
                # logger.error(f"Attempt {attempt + 1} failed: {exc}", exc_info=True)
                if on_error:
                    on_error(attempt, exc)
                if attempt < attempts - 1:
                    DellAttachments._backoff(attempt)
                    attempt = attempt + 1 
                else:
                    raise  # re‑raise the last exception

    # --------------------------------------------------------------------- #
    # Authentication / header handling
    # --------------------------------------------------------------------- #
    def _ensure_auth(self, force: bool = False) -> None:
        """Make sure a valid token exists and the Authorization header is set."""
        now = pendulum.now() 

        if self.__token and not force and self.__token_expires_at > now:
            logger.debug(f"Using cached token, expires at: {self.__token_expires_at}")
            return

        payload = (
            f"client_id={self.__client_id}"
            f"&client_secret={self.__client_secret}"
            f"&grant_type=client_credentials"
        )
        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        # if self.debug:
        logger.info(
                f"Auth request – url: {self.__auth_url}, payload: {payload}, headers: {headers}"
            )

        def do_request() -> None:
            try:
                resp = requests.post(self.__auth_url, data=payload, headers=headers)
                resp.raise_for_status()
                response_data = resp.json()
                self.__token = response_data.get("access_token")
                self.__token_expires_at = pendulum.now().add(seconds=(int(response_data.get("expires_in")) - 120))

                if not self.__token:
                    raise RuntimeError("Auth response does not contain an access_token")
                logger.info(f"Obtained auth token: {self.__token}")
                status="success"
            except Exception as e:
                if 'resp' in locals():
                    status=str(e)+'-'+resp.text
                else:
                    status=str(e)
                raise e
            finally:
                log={'method':'post',
                     'url':self.__auth_url,
                     'payload':payload,
                     'headers':headers,
                     'status':status}
                self.db.insert_tda_api_call_log(log)

        # Retry the whole auth request (same 3‑attempt logic as original)
        self._retry(
            do_request,
            attempts=3,
            on_error=lambda a, e: logger.warning(
                f"Auth attempt {a + 1} failed: {e}"
            ),
        )

        # Populate the static part of the headers (will be copied later)
        self.__headers = {
            "Authorization": f"Bearer {self.__token}",
            "User": self.__USER,
            "servicetype": self._SERVICE_TYPE,
            "Content-Type": "application/json",
        }

    # --------------------------------------------------------------------- #
    # File chunking
    # --------------------------------------------------------------------- #
    def _chunk_file(self, file_obj: Any) -> Iterable[bytes]:
        """Yield `self.chunk_size`‑byte chunks from an opened binary file."""
        while True:
            data = file_obj.read(self.chunk_size)
            if not data:
                break
            yield data

    # --------------------------------------------------------------------- #
    # API calls
    # --------------------------------------------------------------------- #
    async def _initiate(self, filepath: Path, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Create an upload session and return the JSON response."""
        self._ensure_auth()
        url = f"{self.base_url}{self.cfg.DELL_ATTACHMENTS_INITIATE_ENDPOINT}"
        path_obj = Path(filepath)

        # Populate required metadata fields
        metadata.update(
            {
                "FileName": path_obj.name,
                "FileSize": f"{round(path_obj.stat().st_size / (1024 * 1024), 3)} mb",
                "FileVisibleExternal": "false",
            }
        )

        payload = json.dumps(
            {"fileName": metadata["FileName"], "metadata": metadata}
        )

        # if self.debug:
        logger.info(
                f"Initiate – url: {url}, payload: {payload}, headers: {self.__headers}"
            )

        async def do_post() -> Dict[str, Any]:
            async with AsyncClient() as client:
                try:
                    resp = await client.post(url, headers=self.__headers, data=payload)
                    resp.raise_for_status()
                    status = 'success'
                    response_data = resp.json()
                    return response_data
                except Exception as e:
                    if 'resp' in locals():
                        status=str(e)+'-'+resp.text
                    else:
                        status=str(e)
                    raise e
                finally:
                    log={'method':'post',
                         'url':url,
                         'data':payload,
                         'headers':self.__headers,
                         'status':status}
                    self.db.insert_tda_api_call_log(log)

        return self._retry(
            do_post,
            attempts=3,
            on_error=lambda a, e: logger.warning(
                f"Initiate attempt {a + 1} failed: {e}"
            ),
            on_auth_error=lambda a, e: self._ensure_auth(force=True),
        )

    async def _stream(self, initiate_resp: Dict[str, Any], filepath: Path) -> httpx.Response:
        """Upload the file in chunks using the session created by `_initiate`."""
        self._ensure_auth()
        url = f"{self.base_url}{self.cfg.DELL_ATTACHMENTS_STREAM_ENDPOINT}"


        # Base query‑string parameters required for every chunk
        params = {
            "fileId": initiate_resp["fileId"],
            "uploadId": initiate_resp["uploadId"],
            "noOfChunks": (filepath.stat().st_size + self.chunk_size - 1)
            // self.chunk_size,
        }

        # `Content-Type` must be omitted for multipart uploads
        chunk_headers = {k: v for k, v in self.__headers.items() if k != "Content-Type"}

        # if self.debug:
        logger.info(
                f"Stream - url: {url}, base params: {params}, headers: {chunk_headers}"
            )

        async with AsyncClient() as client:
            with filepath.open("rb") as f:
                for idx, chunk in enumerate(self._chunk_file(f), start=1):
                    params["chunkNumber"] = idx
                    files = {"file": (filepath.name, chunk)}

                    async def upload_chunk() -> httpx.Response:
                        try:
                            resp = await client.post(
                                url,
                                headers=chunk_headers,
                                params=params,
                                files=files,
                            )
                            resp.raise_for_status()
                            status="success"
                            logger.info(f"Chunk {idx}/{params['noOfChunks']} uploaded – status {resp.status_code}")
                            return resp
                        except Exception as e:
                            if 'resp' in locals():
                                status=str(e)+'-'+resp.text
                            else:
                                status=str(e)
                            raise e
                        finally:
                            log={'method':'post',
                                 'url':url,
                                 'params':params,
                                 'files':filepath.name,
                                 'headers':chunk_headers,
                                 'status':status}
                            self.db.insert_tda_api_call_log(log)

                    # Each chunk gets its own retry loop (3 attempts)
                    resp = self._retry(
                        upload_chunk,
                        attempts=3,
                        on_error=lambda a, e: logger.warning(
                            f"Chunk {idx} attempt {a + 1} failed: {e}"
                        ),
                        on_auth_error=lambda a, e: self._ensure_auth(force=True),
                    )
        return resp
    
    async def _complete(self, initiate_resp: Dict[str, Any]) -> Dict[str, Any]:
        """Create an upload session and return the JSON response."""
        self._ensure_auth()
        url = f"{self.base_url}{self.cfg.DELL_ATTACHMENTS_COMPLETE_ENDPOINT}"

        # if self.debug:
        logger.info(
                f"Complete - url: {url}, params: {initiate_resp}, headers: {self.__headers}"
            )

        async def do_post() -> Dict[str, Any]:
            async with httpx.AsyncClient() as client:
                try:
                    resp = await client.post(url, headers=self.__headers, params=initiate_resp)
                    resp.raise_for_status()
                    status="success"
                    return resp
                except Exception as e:
                    if 'resp' in locals():
                        status=str(e)+'-'+resp.text
                    else:
                        status=str(e)
                    raise e
                finally:
                    log={'method':'post',
                         'url':url,
                         'params':initiate_resp,
                         'headers':self.__headers,
                         'status':status}
                    self.db.insert_tda_api_call_log(log)

        return self._retry(
            do_post,
            attempts=3,
            on_error=lambda a, e: logger.warning(
                f"Initiate attempt {a + 1} failed: {e}"
            ),
            on_auth_error=lambda a, e: self._ensure_auth(force=True),
        )
    
    async def _confirm_status(self, fileIds: str|list[str]) -> Dict[str, Any]:
        """Create an upload session and return the JSON response."""
        self._ensure_auth()
        url = f"{self.base_url}{self.cfg.DELL_ATTACHMENTS_STATUS_ENDPOINT}"
        # url = url + "/"+fileIds+"/fileID"
        if isinstance(fileIds, str):
            payload: List[str] = [fileIds,]
        else:
            payload: List[str] = fileIds

        # params = {
        #     "fileID": fileIds,}
        # if self.debug:
        logger.info(
                f"status - url: {url}, payload: {payload}, headers: {self.__headers}"
            )
        
        
        async def do_post() -> Dict[str, Any]:
            async with AsyncClient() as client:
                try:
                    resp = await client.post(url, headers=self.__headers, json=payload)
                    resp.raise_for_status()
                    status="success"
                    return resp
                except Exception as e:
                    if 'resp' in locals():
                        status=str(e)+'-'+resp.text
                    else:
                        status=str(e)
                    raise e
                finally:
                    log={'method':'post',
                         'url':url,
                         'json':payload,
                         'headers':self.__headers,
                         'status':status}
                    self.db.insert_tda_api_call_log(log)

        return self._retry(
            do_post,
            attempts=3,
            on_error=lambda a, e: logger.warning(
                f"Initiate attempt {a + 1} failed: {e}"
            ),
            on_auth_error=lambda a, e: self._ensure_auth(force=True),
        )

    # --------------------------------------------------------------------- #
    # Public façade
    # --------------------------------------------------------------------- #
    async def upload(self, filepath: str, metadata: Optional[Dict[str, Any]] = None) -> str:
        metadata = metadata or {}
        filepath = Path(filepath)
        if not filepath.exists():
            raise FileNotFoundError(f"File not found: {filepath}")
        try:
            # Initiate the upload
            initiate_resp = await self._initiate(filepath, metadata)
            # Stream the file in chunks to DA
            stream_resp = await self._stream(initiate_resp, filepath)
            # Complete the upload
            complete_resp = await self._complete(initiate_resp)
        

            # Check the upload status
            recheck_cnt = 1 
            while recheck_cnt < 4:
                time.sleep(10*recheck_cnt)
                status_resp = await self._confirm_status(initiate_resp["fileId"])
                if status_resp.json()[0]['status'].lower() in  ["completed","clean"]:
                    logger.info(f"Complete - File : {filepath} uploaded successfully")
                    # return status_resp.json()[0]['status']
                    return initiate_resp["fileId"]
                else:
                    logger.info(f"CheckStatus {recheck_cnt} - Current upload status : {status_resp.json()[0]['status']} for file : {filepath}")
                    recheck_cnt += 1
        except httpx.HTTPStatusError as hse:
            raise DellAttachmentsApiError(hse)
        
        logger.warning(f"FAILED - while uploading file : {filepath} with status : {status_resp.json()[0]['status']}")
        return initiate_resp["fileId"]
        # raise DellAttachmentsUploadError(f"FAILED - while uploading file : {filepath} with status : {status_resp.json()[0]['status']}")
        # return f"Failed-{filepath}"
        # return (initiate_resp, stream_resp, complete_resp)
    
    async def download(self, filedict: dict) -> Dict[str, Any]:
        """Create an download session and return the JSON response."""
        self._ensure_auth()
        url = f"{self.base_url}{self.cfg.DELL_ATTACHMENTS_DOWNLOAD_ENDPOINT.format(fileid=filedict['id'])}"
        headers = self.__headers.copy()
        headers['username'] = 'lekhnath.pandey@dellteam.com'
        headers.pop('User')

        # if self.debug:
        logger.info(
                f"download - url: {url}, headers: {headers}"
            )
        
        
        async def do_get() -> Dict[str, Any]:
            async with httpx.AsyncClient() as client:
                try:
                    resp = await client.get(url, headers=headers)
                    resp.raise_for_status()
                    status="success"
                    return resp
                except Exception as e:
                    if 'resp' in locals():
                        status=str(e)+'-'+resp.text
                    else:
                        status=str(e)
                    raise e
                finally:
                    log={'method':'get',
                         'url':url,
                         'headers':headers,
                         'status':status}
                    self.db.insert_tda_api_call_log(log)

        try:
            response = self._retry(
                                        do_get,
                                        attempts=3,
                                        on_error=lambda a, e: logger.warning(
                                            f"Initiate attempt {a + 1} failed: {e}"
                                        ),
                                        on_auth_error=lambda a, e: self._ensure_auth(force=True),
                                    )
        except httpx.HTTPStatusError as hse:
            raise DellAttachmentsApiError(hse)
        except Exception as e:
            logger.error(f"{e} --> Error occured while getting download url for file - {filedict['name']} and {filedict['id']}.", exc_info=True)
            raise DellAttachmentsDownloadError(error=e, message = f"Error occured while getting download url for file - {filedict['name']} and {filedict['id']}.")
        
        logger.info(f"Get Download URL: {response.json()}")
        # logger.info(f"Get Download URL: {response.json()[0]['url']}")
        dest_path = Path(filedict['filepath'])
        if not dest_path.parent.exists():
            dest_path.parent.mkdir(parents=True, exist_ok=True)
        else:
            shutil.rmtree(dest_path.parent, ignore_errors=True)
            dest_path.parent.mkdir(parents=True, exist_ok=True)

        async def do_stream() -> str:
            # try:
            async with httpx.AsyncClient() as client:
                try:
                    async with client.stream("GET", response.json()[0]['url'], headers=self.__headers) as stream:
                        stream.raise_for_status()
                        async with aiofiles.open(dest_path, "wb") as out_f:
                            async for chunk in stream.aiter_bytes(self.chunk_size):
                                await out_f.write(chunk)
                    status="success"
                except Exception as e:
                    status=str(e)
                    raise e
                finally:
                    log={'method':'get',
                         'url':response.json()[0]['url'],
                         'headers':self.__headers,
                         'status':status,
                         'filepath':str(dest_path)}
                    self.db.insert_tda_api_call_log(log)
            logger.info(f"Download completed: {dest_path}")
            filedict['filepath'] = dest_path
            return filedict
        
        try:
            status = self._retry(
                                        do_stream,
                                        attempts=3,
                                        on_error=lambda a, e: logger.warning(
                                            f"Initiate attempt {a + 1} failed: {e}"
                                        ),
                                        on_auth_error=lambda a, e: self._ensure_auth(force=True),
                            )
        except httpx.HTTPStatusError as hse:
            raise DellAttachmentsApiError(hse)
        except Exception as e:
            if 'status' not in locals(): status = ''
            logger.error(f"{e} -{status} --> Error occured while downloading file from url- {response.json()[0]['url']}", exc_info=True)
            raise DellAttachmentsDownloadError(error=e, fileid=filedict['id'], filename=filedict['name'])
        
        return status
        
    
    async def getListOfFile(self, metadata= {}) -> pd.DataFrame:
        """Create an download session and return the JSON response."""
        def get_dataframe(response: httpx.Response) -> pd.DataFrame:
            try:
                df = pd.DataFrame(response.json())
            except json.JSONDecodeError:
                df = pd.DataFrame()
            return df
        self._ensure_auth()
        url = f"{self.base_url}{self.cfg.DELL_ATTACHMENTS_LISTFILES_ENDPOINT}"
        payload = {"metaData":metadata}

        # if self.debug:
        logger.info(f"metadata - url: {url}, payload: {payload}, headers: {self.__headers}")
        
        
        async def do_post() -> Dict[str, Any]:
            async with httpx.AsyncClient() as client:
                try:
                    resp = await client.post(url, headers=self.__headers, json=payload)
                    resp.raise_for_status()
                    status="success"
                    return resp
                except Exception as e:
                    if 'resp' in locals():
                        status=str(e)+'-'+resp.text
                    else:
                        status=str(e)
                    raise e
                finally:
                    log={'method':'post',
                         'url':url,
                         'json':payload,
                         'headers':self.__headers,
                         'status':status}
                    self.db.insert_tda_api_call_log(log)
        try:
            response =  self._retry(
                                        do_post,
                                        attempts=3,
                                        on_error=lambda a, e: logger.warning(
                                            f"Initiate attempt {a + 1} failed: {e}"
                                        ),
                                        on_auth_error=lambda a, e: self._ensure_auth(force=True),
                                    )
            df = get_dataframe(response)
            if df.empty:
                raise DellAttachmentsDownloadError("No files found")
        except httpx.HTTPStatusError as hse:
            raise DellAttachmentsApiError(hse)
        
        return df