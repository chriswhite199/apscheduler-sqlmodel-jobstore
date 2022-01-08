import datetime
import os
from typing import Generator

import pytest
from apscheduler.jobstores.base import ConflictingIdError
from apscheduler.jobstores.base import JobLookupError
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.base import BaseScheduler
from apscheduler.triggers.interval import IntervalTrigger
from sqlmodel import create_engine
from sqlmodel import Session

from apscheduler_sqlmodel_jobstore import __version__
from apscheduler_sqlmodel_jobstore.sqlmodel import BaseJobModel
from apscheduler_sqlmodel_jobstore.sqlmodel import JobModel
from apscheduler_sqlmodel_jobstore.sqlmodel import SQLModelJobStore


def test_version():
    assert __version__ == '0.1.0'


class CustomJobModel(BaseJobModel, table=True):
    __tablename__ = 'custom_table_name'


@pytest.fixture
def engine(tmpdir):
    db = f'{tmpdir}/sqlite.db'
    engine = create_engine(url=f'sqlite:///{db}', echo=True)
    yield engine
    if os.path.exists(db):
        os.remove(db)


@pytest.fixture
def model_clz():
    return JobModel


@pytest.fixture
def sqlmodel_jobstore(engine, model_clz):
    store = SQLModelJobStore(engine=engine, model_clz=model_clz)
    yield store
    store.shutdown()


@pytest.fixture
def apscheduler(sqlmodel_jobstore) -> Generator[BaseScheduler, None, None]:
    scheduler = BackgroundScheduler(jobstores=dict(default=sqlmodel_jobstore))
    scheduler.start()
    yield scheduler
    scheduler.shutdown()


@pytest.fixture
def job_params():
    return dict(func=test_func,
                name='job #1',
                trigger=IntervalTrigger(seconds=1, timezone='UTC'),
                id='id123')


def test_func():
    print('executed')


def test_crud(apscheduler, job_params):
    apscheduler.add_job(**job_params)
    assert len(apscheduler.get_jobs()) == 1
    job = apscheduler.get_job('id123')
    assert job
    job.modify(name='updated name')
    assert apscheduler.get_job('id123').name == 'updated name'

    # add job with duplicate id
    with pytest.raises(ConflictingIdError):
        apscheduler.add_job(**job_params)

    # remove job
    apscheduler.remove_job(job.id)
    assert apscheduler.get_job('id123') is None

    # remove unk job
    apscheduler.remove_job('unk')

    # remove all jobs
    apscheduler.remove_all_jobs()

    # update unk job
    with pytest.raises(JobLookupError):
        job.modify(**dict(name='updated'))


@pytest.mark.parametrize('model_clz', [CustomJobModel])
def test_custom_extension(engine, apscheduler, job_params, caplog):
    apscheduler.add_job(**job_params)
    assert 'custom_table_name' in caplog.text
    assert 'apscheduler_name' not in caplog.text


def test_get_all_with_corrupted_bytes(apscheduler, engine):
    with Session(engine) as session:
        session.add(JobModel(id='id123',
                             next_run_time=datetime.datetime.now(),
                             job_state=bytes('\x00'.encode())))
        session.commit()

    assert apscheduler.get_jobs() == []
