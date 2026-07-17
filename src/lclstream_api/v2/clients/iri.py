import asyncio
import logging
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any, NoReturn

from amsc_iri import JobSpec, JobState
from amsc_iri.exceptions import ForbiddenException, UnauthorizedException
from amscrot.facility import FacilityClient
from amscrot.facility.filesystem import FilesystemClient
from amscrot.facility.models import Resource
from amscrot.serviceclient import DestroyError
from amscrot.serviceclient.filesystem import FilesystemError
from pydantic import AwareDatetime, BaseModel, ConfigDict, ValidationError

from .. import config
from ..config import IriClientSettings

logger = logging.getLogger(__name__)

JobId = str


class IriAuthenticationError(RuntimeError):
    """IRI rejected a delegated bearer token."""


class IriOperationError(RuntimeError):
    """IRI failed without exposing upstream request or credential details."""


def _is_auth_error(exc: BaseException) -> bool:
    current: BaseException | None = exc
    while current is not None:
        if isinstance(current, ForbiddenException | UnauthorizedException):
            return True
        message = str(current).lower()
        if "401" in message or "403" in message or "unauthorized" in message:
            return True
        current = current.__cause__ or current.__context__
    return False


def _raise_auth_error(exc: BaseException) -> None:
    if _is_auth_error(exc):
        raise IriAuthenticationError("IRI rejected the delegated credential") from None


def _raise_operation_error(exc: BaseException, operation: str) -> NoReturn:
    """DBOS persists step exceptions, so keep credentials out of the message."""

    _raise_auth_error(exc)
    logger.warning("IRI %s failed: %s", operation, exc, exc_info=exc)
    raise IriOperationError(f"IRI {operation} failed") from None


class PosixStat(BaseModel):
    """The flat POSIX stat dict S3DF IRI returns in a completed task result.
    TODO: this may change as we add other facilities, but for now it matches:
    https://github.com/slaclab/fs-facade-service/blob/main/app/models/filesystem.py
    """

    model_config = ConfigDict(extra="ignore")

    mode: int | None = None
    ino: int | None = None
    dev: int | None = None
    nlink: int | None = None
    uid: int | None = None
    gid: int | None = None
    size: int | None = None
    atime: int | None = None
    ctime: int | None = None
    mtime: int | None = None

    @property
    def modified_at(self) -> AwareDatetime | None:
        if self.mtime is None:
            return None
        return datetime.fromtimestamp(self.mtime, UTC)


class LogStat(BaseModel):
    """Best-effort metadata about a remote log file."""

    exists: bool
    stat: PosixStat | None = None

    @property
    def size(self) -> int | None:
        return self.stat.size if self.stat else None

    @property
    def modified_at(self) -> AwareDatetime | None:
        return self.stat.modified_at if self.stat else None


# amscrot's upper-case states; unmapped means job gone.
_STATE_MAP: dict[str, JobState] = {
    "NEW": JobState.NEW,
    "QUEUED": JobState.QUEUED,
    "ACTIVE": JobState.ACTIVE,
    "COMPLETED": JobState.COMPLETED,
    "FAILED": JobState.FAILED,
    "CANCELED": JobState.CANCELED,
}


class IriClient:
    """Async adapter over the IRI facility API.

    Every method takes the calling user's token; no shared service-account
    credential is ever held.
    """

    def __init__(self, settings: IriClientSettings) -> None:
        self._settings = settings

    def _facility(self, token: str) -> FacilityClient:
        return FacilityClient(
            endpoint=str(self._settings.base_url),
            token=token,
            name=self._settings.facility,
        )

    def _resource(self, token: str) -> Resource:
        return Resource(
            data={"id": self._settings.resource}, facility_client=self._facility(token)
        )

    def _fs(self, token: str) -> FilesystemClient:
        return Resource(
            data={"id": self._settings.fs_resource},
            facility_client=self._facility(token),
        ).fs

    def _submit(self, jobspec: JobSpec, token: str) -> JobId:
        try:
            job = self._resource(token).submit(job_spec=jobspec, name=jobspec.name)
        except Exception as exc:
            _raise_operation_error(exc, "job submission")
        if job.id is None:
            raise RuntimeError("IRI submission returned no job id")
        return job.id

    def _get(self, job_id: JobId, token: str) -> JobState | None:
        # IRI tries the live queue first, then slurmdbd, in one call.
        try:
            job = self._resource(token).job(job_id)
            state = job.refresh(historical=True)
        except Exception as exc:
            _raise_operation_error(exc, "job status")
        if state == "UNKNOWN" and job.message:
            error = RuntimeError(job.message)
            _raise_auth_error(error)
        return _STATE_MAP.get(state)

    def _cancel(self, job_id: JobId, token: str) -> None:
        try:
            self._resource(token).job(job_id).cancel()
        except DestroyError as exc:
            _raise_auth_error(exc)
            # Cancelling a job that is already gone/terminal is a no-op.
            pass
        except Exception as exc:
            _raise_operation_error(exc, "job cancellation")

    def _upload(self, path: Path, content: str, token: str) -> None:
        try:
            fs = self._fs(token)
            _ = fs.mkdir(str(PurePosixPath(path).parent), parents=True).result
            _ = fs.upload_bytes(content.encode(), str(path)).result
        except Exception as exc:
            _raise_operation_error(exc, "file upload")

    def _remove(self, path: Path, token: str) -> None:
        try:
            _ = self._fs(token).rm(str(path)).result
        except Exception as exc:
            _raise_operation_error(exc, "file deletion")

    @staticmethod
    def _extract_stat(result: Any) -> LogStat:
        try:
            parsed = PosixStat.model_validate(result)
        except ValidationError:
            # best effort, it still exists if this could not be validated
            return LogStat(exists=True)
        return LogStat(exists=True, stat=parsed)

    def _stat(self, path: Path, token: str) -> LogStat:
        try:
            result = self._fs(token).stat(str(path)).result
        except FilesystemError as exc:
            _raise_auth_error(exc)
            # amscrot's FilesystemError only ever carries status in
            # {"failed", "canceled", "timeout"} with no errno/
            # reason distinguishing ENOENT from something broken.
            # No way to differentiate, so stat failure reads as "missing".
            # TODO: if this changes, we should update this...
            return LogStat(exists=False)
        except Exception as exc:
            _raise_operation_error(exc, "file stat")
        return self._extract_stat(result)

    def _head(
        self, path: Path, lines: int | None, bytes_: int | None, token: str
    ) -> str:
        try:
            return self._fs(token).head(str(path), lines=lines, bytes_=bytes_).result
        except Exception as exc:
            _raise_operation_error(exc, "file head")

    def _tail(
        self, path: Path, lines: int | None, bytes_: int | None, token: str
    ) -> str:
        try:
            return self._fs(token).tail(str(path), lines=lines, bytes_=bytes_).result
        except Exception as exc:
            _raise_operation_error(exc, "file tail")

    async def submit_job(self, jobspec: JobSpec, token: str) -> JobId:
        return await asyncio.to_thread(self._submit, jobspec, token)

    async def get_job(self, job_id: JobId, token: str) -> JobState | None:
        return await asyncio.to_thread(self._get, job_id, token)

    async def cancel_job(self, job_id: JobId, token: str) -> None:
        await asyncio.to_thread(self._cancel, job_id, token)

    async def upload_file(self, path: Path, content: str, token: str) -> None:
        await asyncio.to_thread(self._upload, path, content, token)

    async def delete(self, path: Path, token: str) -> None:
        await asyncio.to_thread(self._remove, path, token)

    async def stat(self, path: Path, token: str) -> LogStat:
        return await asyncio.to_thread(self._stat, path, token)

    async def head(
        self,
        path: Path,
        token: str,
        *,
        lines: int | None = None,
        bytes_: int | None = None,
    ) -> str:
        return await asyncio.to_thread(self._head, path, lines, bytes_, token)

    async def tail(
        self,
        path: Path,
        token: str,
        *,
        lines: int | None = None,
        bytes_: int | None = None,
    ) -> str:
        return await asyncio.to_thread(self._tail, path, lines, bytes_, token)


_client: IriClient | None = None


def startup() -> None:
    global _client
    _client = IriClient(config.get_iri())


async def shutdown() -> None:
    global _client
    _client = None


def client() -> IriClient:
    if _client is None:
        raise RuntimeError("iri client not initialized; call clients.startup()")
    return _client
