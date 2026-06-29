import asyncio
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

from amsc_iri import JobSpec, JobState
from amscrot.facility import FacilityClient
from amscrot.facility.models import Resource
from amscrot.serviceclient import DestroyError
from amscrot.serviceclient.filesystem import FilesystemError
from pydantic import AwareDatetime, BaseModel, ConfigDict, ValidationError

from .. import config
from ..config import IriClientSettings

JobId = str


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


# amscrot's status() returns AmSCROT JobState strings (upper case). Map them
# back to the canonical amsc_iri JobState the core consumes; an unmapped/UNKNOWN
# state means the job is gone (reads as None).
_STATE_MAP: dict[str, JobState] = {
    "NEW": JobState.NEW,
    "QUEUED": JobState.QUEUED,
    "ACTIVE": JobState.ACTIVE,
    "COMPLETED": JobState.COMPLETED,
    "FAILED": JobState.FAILED,
    "CANCELED": JobState.CANCELED,
}


class IriClient:
    """Async adapter over the IRI facility API, backed by amscrot directly."""

    def __init__(self, settings: IriClientSettings) -> None:
        self._settings = settings
        self._facility = FacilityClient(
            endpoint=str(settings.base_url),
            token=settings.s3df_token.get_secret_value(),
            token_provider=self._read_token,
            name=settings.facility,
        )
        # TODO: this is a bit of an annoyance I will bring to interfaces team
        self._resource = Resource(
            data={"id": settings.resource}, facility_client=self._facility
        )
        self._fs = Resource(
            data={"id": settings.fs_resource}, facility_client=self._facility
        ).fs

    def _read_token(self) -> str:
        return self._settings.s3df_token.get_secret_value()

    def _submit(self, jobspec: JobSpec) -> JobId:
        job = self._resource.submit(job_spec=jobspec, name=jobspec.name)
        if job.id is None:
            raise RuntimeError("IRI submission returned no job id")
        return job.id

    def _get(self, job_id: JobId) -> JobState | None:
        state = self._resource.job(job_id).refresh(historical=True)
        return _STATE_MAP.get(state)

    def _cancel(self, job_id: JobId) -> None:
        try:
            self._resource.job(job_id).cancel()
        except DestroyError:
            # Cancelling a job that is already gone/terminal is a no-op.
            pass

    def _upload(self, path: Path, content: str) -> None:
        _ = self._fs.mkdir(str(PurePosixPath(path).parent), parents=True).result
        _ = self._fs.upload_bytes(content.encode(), str(path)).result

    def _remove(self, path: Path) -> None:
        _ = self._fs.rm(str(path)).result

    @staticmethod
    def _extract_stat(result: Any) -> LogStat:
        try:
            parsed = PosixStat.model_validate(result)
        except ValidationError:
            # best effort, it still exists if this could not be validated
            return LogStat(exists=True)
        return LogStat(exists=True, stat=parsed)

    def _stat(self, path: Path) -> LogStat:
        try:
            result = self._fs.stat(str(path)).result
        except FilesystemError:
            # Ambiguous (not-found vs infra); report unavailable by agreement.
            return LogStat(exists=False)
        return self._extract_stat(result)

    def _head(self, path: Path, lines: int | None, bytes_: int | None) -> str:
        return self._fs.head(str(path), lines=lines, bytes_=bytes_).result

    def _tail(self, path: Path, lines: int | None, bytes_: int | None) -> str:
        return self._fs.tail(str(path), lines=lines, bytes_=bytes_).result

    async def submit_job(self, jobspec: JobSpec) -> JobId:
        return await asyncio.to_thread(self._submit, jobspec)

    async def get_job(self, job_id: JobId) -> JobState | None:
        return await asyncio.to_thread(self._get, job_id)

    async def cancel_job(self, job_id: JobId) -> None:
        await asyncio.to_thread(self._cancel, job_id)

    async def upload_file(self, path: Path, content: str) -> None:
        await asyncio.to_thread(self._upload, path, content)

    async def delete(self, path: Path) -> None:
        await asyncio.to_thread(self._remove, path)

    async def stat(self, path: Path) -> LogStat:
        return await asyncio.to_thread(self._stat, path)

    async def head(
        self, path: Path, *, lines: int | None = None, bytes_: int | None = None
    ) -> str:
        return await asyncio.to_thread(self._head, path, lines, bytes_)

    async def tail(
        self, path: Path, *, lines: int | None = None, bytes_: int | None = None
    ) -> str:
        return await asyncio.to_thread(self._tail, path, lines, bytes_)


_client: IriClient | None = None


def startup() -> None:
    """Build the IRI client singleton (call at app startup)."""
    global _client
    _client = IriClient(config.iri)


async def shutdown() -> None:
    """Release the IRI client singleton (call at app shutdown)."""
    global _client
    _client = None


def client() -> IriClient:
    if _client is None:
        raise RuntimeError("iri client not initialized; call clients.startup()")
    return _client
