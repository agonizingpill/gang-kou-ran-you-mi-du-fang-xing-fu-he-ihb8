from datetime import datetime,timezone
from decimal import Decimal
from fastapi import HTTPException
from sqlalchemy import select
from .models import Tank,FuelSample,Transfer,Audit
from .density_model import within_spec
from .batch_models import TankBatch,SampleBatch,TransferBatchEvidence

def obj(x):return {c.name:float(getattr(x,c.name))if isinstance(getattr(x,c.name),Decimal)else getattr(x,c.name)for c in x.__table__.columns}
def get(s,model,id,lock=False):
    q=select(model).where(model.id==id)
    if lock:q=q.with_for_update().execution_options(populate_existing=True)
    x=s.scalar(q)
    if not x:raise HTTPException(404,'记录不存在')
    return x
def audit(s,u,action,detail):s.add(Audit(actor=u['email'],action=action,detail=str(detail),at=datetime.now(timezone.utc).isoformat()))
def sample_view(s,x):
    from .batch_service import sample_batch_view,comparison_progress
    return {**obj(x),'tank_code':s.get(Tank,x.tank_id).code,'grade':s.get(Tank,x.tank_id).grade,
            'batch':sample_batch_view(s,x),'comparison':comparison_progress(s,x.id)}
def transfer_view(s,x):
    from .batch_service import transfer_batch_view,invalid_reason
    sample=s.get(FuelSample,x.sample_id);tank=s.get(Tank,sample.tank_id)
    return {**obj(x),'sample_code':sample.code,'tank_code':tank.code,'tank_liters':float(tank.liters),
            'batch':transfer_batch_view(s,x),'invalidated_reason':invalid_reason(s,'transfer',x.id)}

def lock_context(s,sample_id):
    """Canonical lock order shared with arrival receipts: Tank -> stored-oil batch -> sample.
    A receipt always locks the Tank before superseding a batch, so this lock order is what
    makes a concurrent release and receipt serialise instead of deadlocking."""
    peek=s.get(FuelSample,sample_id)
    if not peek:raise HTTPException(404,'记录不存在')
    tank=get(s,Tank,peek.tank_id,True)
    sb=s.get(SampleBatch,sample_id)
    batch=None
    if sb:batch=s.scalar(select(TankBatch).where(TankBatch.id==sb.batch_id).with_for_update().execution_options(populate_existing=True))
    sample=get(s,FuelSample,sample_id,True)
    return tank,sample,batch

def check_batch(s,x,batch):
    from .batch_service import active_batch,invalid_reason
    if not batch:raise HTTPException(409,'油样未固定存油批次，请重新收样')
    active=active_batch(s,x.tank_id,True)
    if batch.state!='active' or not active or active.id!=batch.id:
        raise HTTPException(409,invalid_reason(s,'sample',x.id)or f'油样所属存油批次 {batch.code} 已被上游补罐更新而失效，请按新批次重新取样比对')
    return batch

def eligible(s,x,batch):
    if x.reference:raise HTTPException(409,'参考样不能用于实船调拨')
    if x.status=='invalidated':raise HTTPException(409,'油样已随上游补罐失效，请按新存油批次重新取样比对')
    if x.status not in ['qualified','retested']:raise HTTPException(409,'油样未合格或异常尚未通过复检，不能申请调拨')
    check_batch(s,x,batch)
    t=get(s,Tank,x.tank_id)
    density=x.retest_density if x.retest_density is not None else x.density_15
    if not within_spec(t.grade,density,x.viscosity,x.sulfur):raise HTTPException(409,'复检或原样仍超出本等级密度/黏度/硫规格')
    return t,density,batch

def retest(s,sample_id,d,u):
    t,x,batch=lock_context(s,sample_id)
    if x.reference or x.status not in ['anomaly','rejected','retested']:raise HTTPException(409,'仅异常待复检或未通过样可复检')
    if s.scalar(select(Transfer.id).where(Transfer.sample_id==x.id).limit(1)):raise HTTPException(409,'油样已被调拨引用，化验快照冻结')
    check_batch(s,x,batch)
    x.retest_density=d.density;x.retest_note=d.note
    x.status='retested'if within_spec(t.grade,d.density,x.viscosity,x.sulfur)else'rejected';audit(s,u,'sample_retested',f'{x.code}:{x.status}');return sample_view(s,x)

def request_transfer(s,sample_id,d,u):
    from .batch_service import freeze_transfer_batch
    t,x,batch=lock_context(s,sample_id)
    t,density,batch=eligible(s,x,batch)
    if t.status!='active':raise HTTPException(409,'储罐处于暂停调拨状态')
    if d.liters>t.liters:raise HTTPException(409,'申请调拨量超过罐内可用量，未扣减')
    tr=Transfer(**d.model_dump(),status='requested',grade_snapshot=t.grade,density_snapshot=density,viscosity_snapshot=x.viscosity,sulfur_snapshot=x.sulfur,released_by='');s.add(tr);s.flush()
    freeze_transfer_batch(s,tr,x,batch)
    audit(s,u,'transfer_requested',tr.code+' / '+batch.code);return transfer_view(s,tr)

def release(s,transfer_id,u):
    """Locks in the canonical order Tank -> stored-oil batch -> sample -> Transfer, then the
    harbour resources, all inside the request transaction. A concurrent receipt uses the
    same order, so the two serialise: either the receipt commits first and this release
    refuses the stale batch, or this release commits first and the receipt supersedes only
    future requests while the released record keeps its frozen evidence. Volume deduction,
    line occupation and berth occupation either all commit or all roll back."""
    peek=s.get(Transfer,transfer_id)
    if not peek:raise HTTPException(404,'记录不存在')
    t,sample,batch=lock_context(s,peek.sample_id)
    tr=get(s,Transfer,transfer_id,True)
    if tr.status=='invalidated':
        from .batch_service import invalid_reason
        raise HTTPException(409,invalid_reason(s,'transfer',tr.id)or'调拨申请已随上游补罐失效，请重新取样比对后申请')
    if tr.status!='requested':raise HTTPException(409,'调拨单已放行，不能重复扣减')
    if sample.status=='invalidated':raise HTTPException(409,'油样已随上游补罐失效，调拨申请不能放行')
    check_batch(s,sample,batch)
    ev=s.get(TransferBatchEvidence,tr.id)
    if ev and ev.batch_id!=batch.id:raise HTTPException(409,'调拨冻结的存油批次已被上游补罐更新，须按新批次重新取样比对后申请')
    if t.status!='active':raise HTTPException(409,'储罐暂停中，不能放行')
    if not within_spec(tr.grade_snapshot,tr.density_snapshot,tr.viscosity_snapshot,tr.sulfur_snapshot):raise HTTPException(409,'调拨化验快照不满足等级规格')
    if tr.liters>t.liters:raise HTTPException(409,'罐内可用量不足，调拨未扣减')
    from .harbor_service import occupy_for_release
    occupy_for_release(s,tr,u)
    t.liters-=tr.liters;tr.status='released';tr.released_by=u['email'];audit(s,u,'transfer_released',f'{tr.code}:{tr.liters}L / {batch.code}');s.flush();return transfer_view(s,tr)
