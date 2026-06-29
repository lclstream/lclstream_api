import asyncio
from pathlib import Path, PurePosixPath

from amsc_iri import JobSpec, JobState
from amscrot.facility import FacilityClient
from amscrot.facility.models import Resource
from amscrot.serviceclient import DestroyError

from ..config import IriClientSettings

JobId = str

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
