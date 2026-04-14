import os, subprocess, tempfile, shutil, threading
from pathlib import Path
import uuid

from config import Configuration
from core.s3_helper import StorageManager
from core.utility import get_custom_logger

logger = get_custom_logger(__name__)

# Shared across all LibreOfficeConverter instances and all threads.
# Limits simultaneous LibreOffice processes to half the available CPU cores
# to leave headroom for Kafka workers, DB queries, and LLM calls.

# _LIBREOFFICE_SEMAPHORE = threading.Semaphore(max(1, (os.cpu_count() or 4) // 2))


class LibreOfficeConverter:
    """Centralized LibreOffice conversion utilities.

    Each conversion creates a temporary UserInstallation profile so that
    multiple LibreOffice processes can run in parallel without conflicting
    on the default profile lock file.
    """

    def __init__(self, filepath: str | Path, fileid=None):
        self.filepath = Path(filepath)
        self.fileid = fileid
        cfg = Configuration()
        cfg.load_active_config()
        self.libreoffice_path = getattr(cfg, 'LIBREOFFICE_PATH')

    def _run(self, command: list[str]):
        """Run a LibreOffice command with an isolated temporary user profile."""
        unique_id = uuid.uuid4()
        tmp_profile = tempfile.mkdtemp(prefix=f"lo_profile_{unique_id}_")
        try:
            # with _LIBREOFFICE_SEMAPHORE:
            #     self._run_with_profile(command, tmp_profile)
            self._run_with_profile(command, tmp_profile)
        finally:
            shutil.rmtree(tmp_profile, ignore_errors=True)

    def _run_with_profile(self, command: list[str], tmp_profile: str):
        """Run a single LibreOffice command using a given profile directory."""
        profile_url = Path(tmp_profile).as_uri()
        cmd = [command[0], f"-env:UserInstallation={profile_url}", "--headless","--invisible","--nodefault","--nologo","--nolockcheck","--writer","-nocrashreport"] + command[1:]
        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True, timeout = 1800)
        except subprocess.TimeoutExpired:
            logger.error(f"{self.fileid}-LibreOffice command timed out: {cmd}")
            raise
        except subprocess.CalledProcessError as e:
            logger.error(f"{self.fileid}-LibreOffice command failed: {cmd}")
            logger.error(f"{self.fileid}-Stdout: {e.stdout}")
            logger.error(f"{self.fileid}-Error output: {e.stderr}")
            raise

    def _run_steps(self, commands: list[list[str]]):
        """Run multiple LibreOffice commands sequentially sharing one temp profile."""
        unique_id = uuid.uuid4()
        tmp_profile = tempfile.mkdtemp(prefix=f"lo_profile_{unique_id}_")
        try:
            # with _LIBREOFFICE_SEMAPHORE:
            #     for command in commands:
            #         self._run_with_profile(command, tmp_profile)
            for idx, command in enumerate(commands):
                logger.info(f"{self.fileid}-Converting via LibreOffice (Step {idx+1}/{len(commands)})...")
                self._run_with_profile(command, tmp_profile)
        finally:
            shutil.rmtree(tmp_profile, ignore_errors=True)

    def _upload_to_s3(self, converted_path: Path):
        s3 = StorageManager()
        s3.upload(str(converted_path), overwrite=True)
        logger.info(f"{self.fileid}-Uploaded {converted_path.name} to S3")

    # ── single-step conversions ──────────────────────────────────────

    def convert_doc_to_docx(self, upload: bool = False) -> Path:
        """Convert DOC to DOCX using LibreOffice (single step)."""
        parent_dir = str(self.filepath.parent)
        base_name = self.filepath.stem

        logger.info(f"{self.fileid}-Converting {self.filepath.name} to DOCX via LibreOffice...")

        command = [
            self.libreoffice_path, "--headless",
            "--convert-to", "docx",
            "--outdir", parent_dir,
            str(self.filepath.resolve())
        ]

        try:
            self._run(command)
            converted_path = self.filepath.with_suffix('.docx')
            logger.info(f"{self.fileid}-Successfully converted to {base_name}.docx")
            if upload:
                self._upload_to_s3(converted_path)
            return converted_path
        except subprocess.CalledProcessError as e:
            logger.error(f"{self.fileid}-LibreOffice DOC to DOCX conversion failed: {e.stderr}", exc_info=True)
            raise Exception(f"{self.fileid}-LibreOffice DOC to DOCX conversion failed: {e.stderr}")

    def convert_ppt_to_pptx(self, upload: bool = False) -> Path:
        """Convert PPT to PPTX using LibreOffice (single step)."""
        parent_dir = str(self.filepath.parent)
        base_name = self.filepath.stem

        logger.info(f"{self.fileid}-Converting {self.filepath.name} to PPTX via LibreOffice...")

        command = [
            self.libreoffice_path, "--headless",
            "--convert-to", "pptx",
            "--outdir", parent_dir,
            str(self.filepath.resolve())
        ]

        try:
            self._run(command)
            converted_path = self.filepath.with_suffix('.pptx')
            logger.info(f"{self.fileid}-Successfully converted to {base_name}.pptx")
            if upload:
                self._upload_to_s3(converted_path)
            return converted_path
        except subprocess.CalledProcessError as e:
            logger.error(f"{self.fileid}-LibreOffice PPT to PPTX conversion failed: {e.stderr}", exc_info=True)
            raise Exception(f"{self.fileid}-LibreOffice PPT to PPTX conversion failed: {e.stderr}")

    def convert_xls_to_xlsx(self, upload: bool = False) -> Path:
        """Convert XLS to XLSX using LibreOffice (2-step: XLS->ODS->XLSX)."""
        parent_dir = str(self.filepath.parent)
        base_name = self.filepath.stem
        ods_path = self.filepath.with_suffix('.ods')

        logger.info(f"{self.fileid}-Converting {self.filepath.name} to ODS via LibreOffice")

        command_step1 = [
            self.libreoffice_path, "--headless",
            "--convert-to", "ods",
            "--outdir", parent_dir,
            str(self.filepath.resolve())
        ]

        command_step2 = [
            self.libreoffice_path, "--headless",
            "--convert-to", "xlsx",
            "--outdir", parent_dir,
            str(ods_path.resolve())
        ]

        try:
            self._run_steps([command_step1, command_step2])
            logger.info(f"{self.fileid}-Converted {base_name}.xls to XLSX via ODS")

            if ods_path.exists():
                os.remove(ods_path)

            converted_path = self.filepath.with_suffix('.xlsx')
            logger.info(f"{self.fileid}-Successfully converted to {base_name}.xlsx")
            if upload:
                self._upload_to_s3(converted_path)
            return converted_path
        except subprocess.CalledProcessError as e:
            ods_path.unlink(missing_ok=True)
            logger.error(f"{self.fileid}-LibreOffice XLS to XLSX conversion failed: {e.stderr}", exc_info=True)
            raise Exception(f"{self.fileid}-LibreOffice XLS to XLSX conversion failed: {e.stderr}")

    # ── PDF conversions (2-step) ─────────────────────────────────────

    def convert_pdf_to_pptx(self, upload: bool = False) -> Path:
        """Convert PDF to PPTX using LibreOffice (2-step: PDF->PPT->PPTX)."""
        parent_dir = str(self.filepath.parent)
        filename = self.filepath.name
        base_name = self.filepath.stem

        logger.info(f"{self.fileid}-Converting {filename} to PPT via LibreOffice")

        command_step1 = [
            self.libreoffice_path, "--headless",
            "--infilter=impress_pdf_import",
            "--convert-to", "ppt",
            "--outdir", parent_dir,
            str(self.filepath)
        ]

        command_step2 = [
            self.libreoffice_path, "--headless",
            "--convert-to", "pptx",
            "--outdir", parent_dir,
            str(self.filepath.with_suffix('.ppt'))
        ]

        try:
            self._run_steps([command_step1, command_step2])
            logger.info(f"{self.fileid}-Converted {base_name}.pdf to PPTX via PPT")

            ppt_intermediate = self.filepath.with_suffix('.ppt')
            if ppt_intermediate.exists():
                os.remove(ppt_intermediate)

            converted_path = self.filepath.with_suffix('.pptx')
            logger.info(f"{self.fileid}-Successfully converted to {base_name}.pptx")
            if upload:
                self._upload_to_s3(converted_path)
            return converted_path
        except subprocess.CalledProcessError as e:
            self.filepath.with_suffix('.ppt').unlink(missing_ok=True)
            logger.error(f"{self.fileid}-LibreOffice PDF to PPTX conversion failed: {e.stderr}", exc_info=True)
            raise Exception(f"{self.fileid}-LibreOffice PDF to PPTX conversion failed: {e.stderr}")

    def convert_pdf_to_docx(self, upload: bool = False) -> Path:
        """Convert PDF to DOCX using LibreOffice (2-step: PDF->DOC->DOCX)."""
        parent_dir = str(self.filepath.parent)
        filename = self.filepath.name
        base_name = self.filepath.stem

        logger.info(f"{self.fileid}-Converting {filename} to DOC via LibreOffice")

        command_step1 = [
            self.libreoffice_path, "--headless",
            "--infilter=writer_pdf_import",
            "--convert-to", "doc",
            "--outdir", parent_dir,
            str(self.filepath)
        ]

        command_step2 = [
            self.libreoffice_path, "--headless",
            "--convert-to", "docx",
            "--outdir", parent_dir,
            str(self.filepath.with_suffix('.doc'))
        ]

        try:
            self._run_steps([command_step1, command_step2])
            logger.info(f"{self.fileid}-Converted {base_name}.pdf to DOCX via DOC")

            doc_intermediate = self.filepath.with_suffix('.doc')
            if doc_intermediate.exists():
                os.remove(doc_intermediate)

            converted_path = self.filepath.with_suffix('.docx')
            logger.info(f"{self.fileid}-Successfully converted to {base_name}.docx")
            if upload:
                self._upload_to_s3(converted_path)
            return converted_path
        except subprocess.CalledProcessError as e:
            self.filepath.with_suffix('.doc').unlink(missing_ok=True)
            logger.error(f"{self.fileid}-LibreOffice PDF to DOCX conversion failed: {e.stderr}", exc_info=True)
            raise Exception(f"{self.fileid}-LibreOffice PDF to DOCX conversion failed: {e.stderr}")
