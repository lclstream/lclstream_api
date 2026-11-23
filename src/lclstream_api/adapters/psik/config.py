import psik
from pydantic import BaseModel


class LCLStreamerConfig(BaseModel):
    jobspec: psik.JobSpec


class ForwarderConfig(BaseModel):
    ip: str
    start_port: int = 30001
    end_port: int = 34000
    jobspec: psik.JobSpec


class ReplayConfig(BaseModel):
    cache_fmt: str | None = None
    jobspec: psik.JobSpec = psik.JobSpec(script="")
