"""Stored-oil batches: every arrival receipt opens a new batch for the tank.

Invariants
- at most one active TankBatch per tank (partial unique index);
- every business FuelSample is pinned to the batch that existed at intake;
- every Transfer freezes its batch as evidence at request time;
- when a receipt supersedes a batch, that batch's samples, their still-open comparison
  jobs and unreleased (requested) transfers are invalidated in the same transaction;
- released transfers are never mutated: their frozen evidence keeps the original basis.

Lock order for anything touching a batch is Tank -> batch -> samples -> Transfer/job. Until
the Tank lock is held, pins and ids are only read without locking (``s.get`` peeks) so we
never take a lower-level lock before the Tank lock. Calls here assume the caller already
holds the Tank row lock.
"""
from datetime import datetime,timezone
from decimal import Decimal
from fastapi import HTTPException
from sqlalchemy import select
from .models import Tank,FuelSample,Transfer
from .receipt_models import TankCapacity
from .batch_models import TankBatch,SampleBatch,TransferBatchEvidence,BatchInvalidation,ComparisonJob
from .constants import GRADES

ACTIVE='active';SUPERSEDED='superseded'
OPEN_JOB_STATES=('queued','running')

def now():return datetime.now(timezone.utc).isoformat()
def batch_code(tank,seq):return f'{tank.code}-B{seq:02d}'
def row(x):return {c.name:float(getattr(x,c.name))if isinstance(getattr(x,c.name),Decimal)else getattr(x,c.name)for c in x.__table__.columns}

def legacy_density(s,tank):
    cap=s.scalar(select(TankCapacity).where(TankCapacity.tank_id==tank.id))
    if cap:return cap.density_15
    sample=s.scalar(select(FuelSample).where(FuelSample.tank_id==tank.id,FuelSample.reference==False).order_by(FuelSample.id.desc()))
    if sample:return sample.density_15
    g=GRADES[tank.grade];return Decimal(str((g['density_min']+g['density_max'])/2)).quantize(Decimal('.001'))

def active_batch(s,tank_id,lock=False):
    q=select(TankBatch).where(TankBatch.tank_id==tank_id,TankBatch.state==ACTIVE)
    if lock:q=q.with_for_update().execution_options(populate_existing=True)
    return s.scalar(q)

def next_batch(s,tank,density,liters,source,receipt_id,note):
    last=s.scalar(select(TankBatch).where(TankBatch.tank_id==tank.id).order_by(TankBatch.seq.desc()).limit(1))
    seq=(last.seq+1)if last else 1
    x=TankBatch(tank_id=tank.id,code=batch_code(tank,seq),seq=seq,source=source,receipt_id=receipt_id,
               liters=liters,density_15=density,state=ACTIVE,note=note,created_at=now())
    s.add(x);s.flush();return x

def ensure_active_batch(s,tank,lock=False):
    b=active_batch(s,tank.id,lock)
    if b:return b
    return next_batch(s,tank,legacy_density(s,tank),tank.liters,'legacy',None,'升级或新罐建立的初始存油批次')

def pinned_batch(s,sample):
    sb=s.get(SampleBatch,sample.id)
    return s.get(TankBatch,sb.batch_id)if sb else None

def pin_sample(s,sample,tank):
    b=ensure_active_batch(s,tank,lock=True)
    if not s.get(SampleBatch,sample.id):
        s.add(SampleBatch(sample_id=sample.id,batch_id=b.id));s.flush()
    return b

def add_invalidation(s,entity_type,entity_id,batch_id,reason):
    s.add(BatchInvalidation(entity_type=entity_type,entity_id=entity_id,batch_id=batch_id,reason=reason,at=now()))

def invalid_reason(s,entity_type,entity_id):
    return s.scalar(select(BatchInvalidation.reason).where(
        BatchInvalidation.entity_type==entity_type,BatchInvalidation.entity_id==entity_id).order_by(BatchInvalidation.id.desc()).limit(1))

def batch_view(s,x):
    tank=s.get(Tank,x.tank_id);by=s.get(TankBatch,x.superseded_by_id)if x.superseded_by_id else None
    return {**row(x),'tank_code':tank.code if tank else '','superseded_by_code':by.code if by else ''}

def comparison_progress(s,sample_id):
    j=s.scalar(select(ComparisonJob).where(ComparisonJob.sample_id==sample_id).order_by(ComparisonJob.id.desc()).limit(1))
    if not j:return None
    b=s.get(TankBatch,j.batch_id)
    return {'id':j.id,'state':j.state,'attempts':j.attempts,'batch_code':b.code if b else '',
            'batch_id':j.batch_id,'queued_at':j.queued_at,'started_at':j.started_at,
            'finished_at':j.finished_at,'result_status':j.result_status,'detail':j.detail}

def open_job(s,sample_id):
    return s.scalar(select(ComparisonJob).where(ComparisonJob.sample_id==sample_id,
        ComparisonJob.state.in_(OPEN_JOB_STATES)).order_by(ComparisonJob.id.desc()).limit(1))

def lock_sample(s,sample_id):
    """Canonical lock order Tank -> stored-oil batch -> sample -> job, matching receipts,
    releases and the worker. Reading the pin without a lock lets us take the batch before
    the sample instead of inverting the order and risking a deadlock."""
    peek=s.get(FuelSample,sample_id)
    if not peek:raise HTTPException(404,'记录不存在')
    tank=s.scalar(select(Tank).where(Tank.id==peek.tank_id).with_for_update().execution_options(populate_existing=True))
    if not tank:raise HTTPException(404,'记录不存在')
    sb=s.get(SampleBatch,sample_id)
    batch=s.scalar(select(TankBatch).where(TankBatch.id==sb.batch_id).with_for_update().execution_options(populate_existing=True))if sb else None
    sample=s.scalar(select(FuelSample).where(FuelSample.id==sample_id).with_for_update().execution_options(populate_existing=True))
    return tank,sample,batch

def queue_for_sample(s,sample_id):
    """Validate and queue one comparison job; returns the job id."""
    tank,sample,batch=lock_sample(s,sample_id)
    if sample.reference:raise HTTPException(409,'参考样不参与业务比对')
    if sample.status=='invalidated':raise HTTPException(409,invalid_reason(s,'sample',sample.id)or'油样已随上游补罐失效，请按新存油批次重新取样')
    if open_job(s,sample.id):raise HTTPException(409,'该油样已有待处理比对，不能重复排队')
    active=active_batch(s,tank.id,True)
    if not active:raise HTTPException(409,'储罐尚无有效存油批次')
    if not batch:
        s.add(SampleBatch(sample_id=sample.id,batch_id=active.id));s.flush();batch=active
    elif batch.id!=active.id or batch.state!='active':
        raise HTTPException(409,invalid_reason(s,'sample',sample.id)or'油样批次已过期，请重新取样')
    job=queue_comparison(s,sample,batch)
    return job.id,batch.code

def queue_comparison(s,sample,batch):
    """Queue one durable job. Callers first reject an existing open job so a retry cannot
    create a second effective result."""
    job=ComparisonJob(sample_id=sample.id,batch_id=batch.id,state='queued',attempts=0,queued_at=now())
    s.add(job);sample.status='queued';s.flush();return job

def sample_batch_view(s,sample):
    sb=s.get(SampleBatch,sample.id);b=s.get(TankBatch,sb.batch_id)if sb else None
    if not b:return None
    return {'batch_id':b.id,'batch_code':b.code,'batch_state':b.state,'batch_source':b.source,
            'batch_liters':float(b.liters),'batch_density_15':float(b.density_15),'batch_note':b.note,
            'invalidated_reason':invalid_reason(s,'sample',sample.id)}

def freeze_transfer_batch(s,tr,sample,batch):
    if s.get(TransferBatchEvidence,tr.id):return
    s.add(TransferBatchEvidence(transfer_id=tr.id,batch_id=batch.id,batch_code=batch.code,
        batch_source=batch.source,batch_density_15=batch.density_15,sample_code=sample.code,captured_at=now()))
    s.flush()

def transfer_batch_view(s,tr):
    ev=s.get(TransferBatchEvidence,tr.id)
    if not ev:return None
    b=s.get(TankBatch,ev.batch_id)
    return {'batch_id':ev.batch_id,'batch_code':ev.batch_code,'batch_source':ev.batch_source,
            'batch_density_15':float(ev.batch_density_15),'sample_code':ev.sample_code,
            'captured_at':ev.captured_at,'batch_state':b.state if b else '',
            'batch_active':bool(b and b.state==ACTIVE)}

def invalidate_batch(s,old,new):
    reason=f'上游到港补罐已形成新存油批次 {new.code}，旧批次 {old.code} 的油样与待放行调拨失效，请按新批次重新取样比对'
    old.state=SUPERSEDED;old.superseded_at=now();old.superseded_by_id=new.id;old.superseded_reason=reason
    s.flush()
    samples=s.scalars(select(FuelSample).join(SampleBatch,SampleBatch.sample_id==FuelSample.id).where(
        SampleBatch.batch_id==old.id,FuelSample.reference==False).with_for_update()).all()
    for x in samples:
        if x.status!='invalidated':
            x.status='invalidated'
            add_invalidation(s,'sample',x.id,old.id,reason)
    stale=f'取样批次 {old.code} 已被到港补罐 {new.code} 更新，迟到比对结果不生效'
    for j in s.scalars(select(ComparisonJob).where(ComparisonJob.batch_id==old.id,
            ComparisonJob.state.in_(OPEN_JOB_STATES)).with_for_update()).all():
        j.state='stale';j.finished_at=now();j.result_status='';j.detail=stale
    transfers=s.scalars(select(Transfer).join(TransferBatchEvidence,TransferBatchEvidence.transfer_id==Transfer.id).where(
        TransferBatchEvidence.batch_id==old.id,Transfer.status=='requested').with_for_update()).all()
    seen={t.id for t in transfers}
    if samples:
        ids=[x.id for x in samples]
        for t in s.scalars(select(Transfer).where(Transfer.status=='requested',Transfer.sample_id.in_(ids)).with_for_update()).all():
            if t.id not in seen:transfers.append(t);seen.add(t.id)
    for t in transfers:
        t.status='invalidated'
        add_invalidation(s,'transfer',t.id,old.id,reason)
    s.flush()
    return reason

def apply_receipt_batch(s,tank,receipt,after,weighted):
    """Supersede the active batch and open the receipt's new batch. Tank lock held."""
    old=active_batch(s,tank.id,lock=True)
    note=f'到港补罐 {receipt.code} 入罐 {receipt.liters}L，罐内存油形成新批次并按体积重算15℃密度'
    if old:
        old.state=SUPERSEDED;old.superseded_at=now();old.superseded_reason=note
        s.flush()
    new=next_batch(s,tank,weighted,after,'receipt',receipt.id,note)
    if old:
        old.superseded_by_id=new.id
        invalidate_batch(s,old,new)
    return new,old

def migrate():
    """Idempotent upgrade for old databases: legacy tanks get an initial batch, old samples
    and transfers are pinned to it, interrupted queued samples regain a durable job."""
    from .database import Session
    with Session.begin()as s:
        for tank in s.scalars(select(Tank).order_by(Tank.id).with_for_update()).all():
            b=ensure_active_batch(s,tank,lock=True)
            samples=s.scalars(select(FuelSample).where(FuelSample.tank_id==tank.id,FuelSample.reference==False).with_for_update()).all()
            for x in samples:
                if not s.get(SampleBatch,x.id):s.add(SampleBatch(sample_id=x.id,batch_id=b.id))
                if x.status=='queued' and not open_job(s,x.id):
                    s.add(ComparisonJob(sample_id=x.id,batch_id=b.id,state='queued',attempts=0,
                        queued_at=now(),detail='升级后恢复待办比对'))
                if x.status=='invalidated' and not invalid_reason(s,'sample',x.id):
                    add_invalidation(s,'sample',x.id,b.id,'升级前油样已随旧批次失效')
            s.flush()
        for tr in s.scalars(select(Transfer).order_by(Transfer.id).with_for_update()).all():
            if s.get(TransferBatchEvidence,tr.id):continue
            sample=s.get(FuelSample,tr.sample_id)
            b=pinned_batch(s,sample)
            if not b:
                tank=s.get(Tank,sample.tank_id)
                b=ensure_active_batch(s,tank,lock=True)
                s.add(SampleBatch(sample_id=sample.id,batch_id=b.id));s.flush()
            freeze_transfer_batch(s,tr,sample,b)
        s.flush()
