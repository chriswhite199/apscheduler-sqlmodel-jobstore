import pickle
from datetime import datetime
from typing import Optional
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
                 pickle_protocol=pickle.HIGHEST_PROTOCOL,
                 model_clz: Type[BaseJobModel] = JobModel) -> None:
        super().__init__()
        self.engine = engine
        self.pickle_protocol = pickle_protocol
        self.model_clz = model_clz

    def start(self, scheduler: Optional[BaseScheduler], alias: str):
        super().start(scheduler, alias)
        SQLModel.metadata.create_all(bind=self.engine, tables=[self.model_clz.__table__])  # type: ignore

    def lookup_job(self, job_id):
        job = self._get_job_model(job_id)
        return self._reconstitute_job(job.job_state) if job else None

    def get_due_jobs(self, now):
        return self._get_jobs(self.model_clz.next_run_time <= now)

    def get_next_run_time(self):
        with Session(self.engine) as session:
            job = session.query(self.model_clz) \
                .where(col(self.model_clz.next_run_time).is_not(None)) \
                .order_by(self.model_clz.next_run_time) \
                .limit(1) \
                .scalar()

            return job.next_run_time if job else None

    def get_all_jobs(self):
        jobs = self._get_jobs()
        self._fix_paused_jobs_sorting(jobs)
        return jobs

    def add_job(self, job):
        obj = self.model_clz(id=job.id, next_run_time=datetime_to_utc_timestamp(job.next_run_time),
                             job_state=pickle.dumps(job.__getstate__(), self.pickle_protocol))
        with Session(self.engine) as session:
            try:
                session.add(obj)
                session.commit()
            except IntegrityError:
                raise ConflictingIdError(job.id)

    def update_job(self, job):
        with Session(self.engine) as session:
            obj = session.get(self.model_clz, job.id)

            if obj is None:
                raise JobLookupError(job.id)

            obj.next_run_time = job.next_run_time
            obj.job_state = pickle.dumps(job.__getstate__(), self.pickle_protocol)
            session.commit()

    def remove_job(self, job_id):
        with Session(self.engine) as session:
            session.execute(delete(self.model_clz).where(self.model_clz.id == job_id))
            session.commit()

    def remove_all_jobs(self):
        with Session(self.engine) as session:
            session.execute(delete(self.model_clz))
            session.commit()

    def _reconstitute_job(self, job_state):
        job_state = pickle.loads(job_state)
        job_state["jobstore"] = self
        job = Job.__new__(Job)
        job.__setstate__(job_state)
        job._scheduler = self._scheduler
        job._jobstore_alias = self._alias
        return job

    def _get_job_model(self, job_id: str):
        with Session(self.engine) as session:
            return session.get(self.model_clz, job_id)

    def _get_jobs(self, *conditions):
        with Session(self.engine) as session:
            query = session.query(self.model_clz)
            if conditions:
                query = query.where(and_(*conditions))

            rows = query.order_by(self.model_clz.next_run_time).all()

            failed_job_ids = set()
            jobs = []
            for row in rows:
                try:
                    jobs.append(self._reconstitute_job(row.job_state))
                except BaseException:
                    self._logger.exception('Unable to restore job "%s" -- removing it', row.id)
                    failed_job_ids.add(row.id)

            if failed_job_ids:
                stmt = delete(self.model_clz).where(col(self.model_clz.id).in_(failed_job_ids))
                session.execute(stmt)
                session.commit()

            return jobs
