import os,time,json
from pathlib import Path
from redis import Redis
from sqlalchemy import select,update
from .database import Session
from .models import FuelSample,Tank
from .batch_models import ComparisonJob,TankBatch
from .density_model import compare

r=Redis.from_url(os.getenv('REDIS_URL','redis://redis:6379/0'),decode_responses=True)

def stamp():return time.strftime('%Y-%m-%dT%H:%M:%S+00:00',time.gmtime())

def heartbeat():
    try:
        path=Path('/artifacts');path.mkdir(parents=True,exist_ok=True)
        (path/'bunker-heartbeat').write_text(str(time.time()))
    except OSError:pass

def save_evidence(sample_id,evidence):
    """Evidence is authoritative in the database; the shared file is a convenience copy."""
    try:
        path=Path('/artifacts');path.mkdir(parents=True,exist_ok=True)
        (path/f'sample-{sample_id}-evidence.json').write_text(evidence)
    except OSError:pass

def claim(job_id):
    """Claim a queued job exactly once in its own short transaction. A repeated Redis
    notification or a second worker can never flip the same job twice."""
    with Session.begin() as s:
        job=s.get(ComparisonJob,job_id,with_for_update=True)
        if not job or job.state!='queued':return None
        moved=s.execute(update(ComparisonJob).where(ComparisonJob.id==job_id,
            ComparisonJob.state=='queued').values(state='running',attempts=ComparisonJob.attempts+1,
            started_at=stamp())).rowcount
        if moved!=1:return None
        return job.sample_id,job.batch_id

def stale(s,job,reason):
    job.state='stale';job.finished_at=stamp();job.result_status='';job.detail=reason

def run(job_id):
    claimed=claim(job_id)
    if not claimed:return
    sample_id,batch_id=claimed
    # Lock order matches every other path: Tank -> batch -> sample -> job.
    with Session.begin() as s:
        job=s.get(ComparisonJob,job_id)
        peek=s.get(FuelSample,job.sample_id)if job else None
        tank=s.scalar(select(Tank).where(Tank.id==peek.tank_id).with_for_update().execution_options(populate_existing=True))if peek else None
        job=s.scalar(select(ComparisonJob).where(ComparisonJob.id==job_id).with_for_update().execution_options(populate_existing=True))
        if not job or job.state!='running':
            return
        batch=s.scalar(select(TankBatch).where(TankBatch.id==batch_id).with_for_update().execution_options(populate_existing=True))
        sample=s.scalar(select(FuelSample).where(FuelSample.id==sample_id).with_for_update().execution_options(populate_existing=True))
        if not sample or not batch or not tank:
            return stale(s,job,'油样或取样批次不存在，比对结果不生效')
        if sample.status!='queued':
            return stale(s,job,'油样状态已变化，迟到结果不生效')
        if sample.status=='invalidated' or batch.state!='active':
            return stale(s,job,f'取样批次 {batch.code} 已失效，迟到结果不生效')
        active=s.scalar(select(TankBatch).where(TankBatch.tank_id==batch.tank_id,
            TankBatch.state=='active').with_for_update().execution_options(populate_existing=True))
        if not active or active.id!=batch.id:
            return stale(s,job,f'取样批次 {batch.code} 已被更新批次取代，迟到结果不生效')
        score,threshold,passed,ev=compare(s,sample)
        sample.log_density,sample.threshold=score,threshold
        sample.status='qualified'if passed else'anomaly'
        sample.evidence=json.dumps(ev,ensure_ascii=False)
        job.state='completed';job.result_status=sample.status;job.finished_at=stamp()
        job.detail=f'批次 {batch.code} 完成比对'
        save_evidence(sample_id,sample.evidence)

def fail(job_id,error):
    with Session.begin() as s:
        job=s.get(ComparisonJob,job_id,with_for_update=True)
        sample=s.get(FuelSample,job.sample_id)if job else None
        if job and job.state=='running':
            job.state='failed';job.finished_at=stamp();job.detail=str(error)[:600]
        if sample and sample.status=='queued':sample.status='failed';sample.evidence=str(error)

def recover():
    """Re-notify jobs left 'running' by an interrupted worker. The conditional UPDATE only
    moves a job that is still running, so an already committed result is never repeated."""
    ids=[]
    with Session.begin() as s:
        for job in s.scalars(select(ComparisonJob).where(ComparisonJob.state=='running',
                ComparisonJob.started_at.is_not(None)).order_by(ComparisonJob.id)).all():
            moved=s.execute(update(ComparisonJob).where(ComparisonJob.id==job.id,
                ComparisonJob.state=='running').values(state='queued',
                detail='处理进程中断，恢复待办比对')).rowcount
            if moved:ids.append(job.id)
    for job_id in ids:r.rpush('density-check',job_id)

def main():
    while True:
        heartbeat()
        item=r.blpop('density-check',timeout=2)
        if not item:continue
        try:job_id=int(item[1])
        except ValueError:continue
        try:run(job_id)
        except Exception as e:fail(job_id,e)

if __name__=='__main__':
    try:recover()
    except Exception:pass
    main()
