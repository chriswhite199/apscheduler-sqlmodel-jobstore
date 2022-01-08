import pickle
from datetime import datetime
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import Type

from apscheduler.job import Job
from apscheduler.jobstores.base import BaseJobStore
from apscheduler.jobstores.base import ConflictingIdError
from apscheduler.jobstores.base import JobLookupError
from apscheduler.schedulers.base import BaseScheduler
from apscheduler.util import datetime_to_utc_timestamp
from sqlalchemy import delete
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Query
from sqlalchemy.sql import Delete
from sqlmodel import and_
from sqlmodel import col
from sqlmodel import Field
from sqlmodel import Session
from sqlmodel import SQLModel


class BaseJobModel(SQLModel):
    id: str = Field(primary_key=True)
    next_run_time: datetime = Field(index=True)
    job_state: bytes = Field(nullable=False, index=False)


class JobModel(BaseJobModel, table=True):
    __tablename__ = 'apscheduler_jobs'


class SQLModelJobStore(BaseJobStore):
    def __init__(self,
                 engine: Engine,
                 pickle_protocol: int = pickle.HIGHEST_PROTOCOL,
                 model_clz: Type[BaseJobModel] = JobModel) -> None:
        super().__init__()
        self.engine = engine
        self.pickle_protocol = pickle_protocol
        self.model_clz = model_clz

    def start(self, scheduler: Optional[BaseScheduler], alias: str) -> None:
        super().start(scheduler, alias)
        SQLModel.metadata.create_all(bind=self.engine, tables=[self.model_clz.__table__])  # type: ignore

    def lookup_job(self, job_id: str) -> Optional[BaseJobModel]:
        job = self._get_job_model(job_id)
        return self._reconstitute_job(job.job_state) if job else None

    def get_due_jobs(self, now) -> List[Job]:
        return self._get_jobs(self.model_clz.next_run_time <= now)

    def get_next_run_time(self) -> Optional[datetime]:
        with Session(self.engine) as session:
            job: Optional[BaseJobModel] = session.query(self.model_clz) \
                .where(col(self.model_clz.next_run_time).is_not(None)) \
                .order_by(self.model_clz.next_run_time) \
                .limit(1) \
                .scalar()

            return job.next_run_time if job else None

    def get_all_jobs(self) -> List[Job]:
        jobs = self._get_jobs()
        self._fix_paused_jobs_sorting(jobs)
        return jobs

    def add_job(self, job: Job) -> None:
        obj: BaseJobModel = self.model_clz(id=job.id,
                                           next_run_time=datetime_to_utc_timestamp(job.next_run_time),
                                           job_state=pickle.dumps(job.__getstate__(), self.pickle_protocol))
        with Session(self.engine) as session:
            try:
                session.add(obj)
                session.commit()
            except IntegrityError:
                raise ConflictingIdError(job.id)

    def update_job(self, job: Job) -> None:
        with Session(self.engine) as session:
            obj: Optional[BaseJobModel] = session.get(self.model_clz, job.id)

            if obj is None:
                raise JobLookupError(job.id)

            obj.next_run_time = job.next_run_time
            obj.job_state = pickle.dumps(job.__getstate__(), self.pickle_protocol)
            session.commit()

    def remove_job(self, job_id: str) -> None:
        with Session(self.engine) as session:
            session.execute(delete(self.model_clz).where(self.model_clz.id == job_id))
            session.commit()

    def remove_all_jobs(self) -> None:
        with Session(self.engine) as session:
            session.execute(delete(self.model_clz))
            session.commit()

    def _reconstitute_job(self, job_state_bytes: bytes) -> Job:
        job_state: Dict[str, Any] = pickle.loads(job_state_bytes)
        job_state["jobstore"] = self
        job = Job.__new__(Job)
        job.__setstate__(job_state)
        job._scheduler = self._scheduler
        job._jobstore_alias = self._alias
        return job

    def _get_job_model(self, job_id: str) -> Optional[BaseJobModel]:
        with Session(self.engine) as session:
            return session.get(self.model_clz, job_id)

    def _get_jobs(self, *conditions) -> List[Job]:
        with Session(self.engine) as session:
            query: Query = session.query(self.model_clz)
            if conditions:
                query = query.where(and_(*conditions))

            rows: List[BaseJobModel] = query.order_by(self.model_clz.next_run_time).all()

            failed_job_ids: Set[str] = set()
            jobs: List[Job] = []
            for row in rows:
                try:
                    jobs.append(self._reconstitute_job(row.job_state))
                except BaseException:
                    self._logger.exception('Unable to restore job "%s" -- removing it', row.id)
                    failed_job_ids.add(row.id)

            if failed_job_ids:
                stmt: Delete = delete(self.model_clz).where(col(self.model_clz.id).in_(failed_job_ids))
                session.execute(stmt)
                session.commit()

            return jobs
