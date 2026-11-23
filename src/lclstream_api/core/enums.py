from enum import StrEnum


class JobState(StrEnum):
    new = "new"
    queued = "queued"
    active = "active"
    completed = "completed"
    failed = "failed"
    canceled = "canceled"

    def is_final(self) -> bool:
        return self.value in ["completed", "failed", "canceled"]
