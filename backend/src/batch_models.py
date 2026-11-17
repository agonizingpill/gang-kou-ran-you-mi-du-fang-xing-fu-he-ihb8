"""Stored-oil batches, sample batch pins, durable comparison jobs and invalidation evidence.

These are additive tables: existing tank/sample/transfer columns stay untouched so an
old database upgrades by ``create_all`` alone (no destructive column migration).
"""
from decimal import Decimal
from sqlalchemy import String,Numeric,Integer,ForeignKey,CheckConstraint,Index,text
from sqlalchemy.orm import Mapped,mapped_column
from .database import Base

class TankBatch(Base):
    __tablename__='tank_batches'
    __table_args__=(
        CheckConstraint('liters >= 0 AND density_15 > 0'),
        CheckConstraint("state IN ('active','superseded')"),
        Index('uq_tank_batches_active','tank_id',unique=True,
              postgresql_where=text("state = 'active'"),sqlite_where=text("state = 'active'")),
        Index('uq_tank_batches_code','code',unique=True),
        Index('uq_tank_batches_receipt','receipt_id',unique=True),
    )
    id:Mapped[int]=mapped_column(primary_key=True)
    tank_id:Mapped[int]=mapped_column(ForeignKey('tanks.id'))
    code:Mapped[str]=mapped_column(String(90))
    seq:Mapped[int]=mapped_column(Integer)
    source:Mapped[str]=mapped_column(String(20),default='legacy')
    receipt_id:Mapped[int|None]=mapped_column(ForeignKey('tank_receipts.id'),nullable=True)
    liters:Mapped[Decimal]=mapped_column(Numeric(14,3))
    density_15:Mapped[Decimal]=mapped_column(Numeric(10,3))
    state:Mapped[str]=mapped_column(String(20),default='active')
    note:Mapped[str]=mapped_column(String(600),default='')
    created_at:Mapped[str]=mapped_column(String(40))
    superseded_at:Mapped[str|None]=mapped_column(String(40),nullable=True)
    superseded_by_id:Mapped[int|None]=mapped_column(ForeignKey('tank_batches.id'),nullable=True)
    superseded_reason:Mapped[str]=mapped_column(String(600),default='')

class SampleBatch(Base):
    """One sample belongs to exactly one stored-oil batch, fixed at intake."""
    __tablename__='sample_batches'
    sample_id:Mapped[int]=mapped_column(ForeignKey('fuel_samples.id'),primary_key=True)
    batch_id:Mapped[int]=mapped_column(ForeignKey('tank_batches.id'))

class TransferBatchEvidence(Base):
    """Frozen batch basis captured when a transfer is requested/released."""
    __tablename__='transfer_batch_evidence'
    transfer_id:Mapped[int]=mapped_column(ForeignKey('transfers.id'),primary_key=True)
    batch_id:Mapped[int]=mapped_column(ForeignKey('tank_batches.id'))
    batch_code:Mapped[str]=mapped_column(String(90))
    batch_source:Mapped[str]=mapped_column(String(20))
    batch_density_15:Mapped[Decimal]=mapped_column(Numeric(10,3))
    sample_code:Mapped[str]=mapped_column(String(70))
    captured_at:Mapped[str]=mapped_column(String(40))

class BatchInvalidation(Base):
    """Latest reason is shown in the UI; rows are append-only evidence."""
    __tablename__='batch_invalidations'
    __table_args__=(Index('ix_batch_invalidations_entity','entity_type','entity_id'),)
    id:Mapped[int]=mapped_column(primary_key=True)
    entity_type:Mapped[str]=mapped_column(String(20))
    entity_id:Mapped[int]=mapped_column(Integer)
    batch_id:Mapped[int]=mapped_column(ForeignKey('tank_batches.id'))
    reason:Mapped[str]=mapped_column(String(600))
    at:Mapped[str]=mapped_column(String(40))

class ComparisonJob(Base):
    """Durable density-comparison job. Redis is only a notification channel."""
    __tablename__='comparison_jobs'
    __table_args__=(
        CheckConstraint("state IN ('queued','running','completed','stale','failed')"),
        Index('ix_comparison_jobs_state','state','id'),
        Index('ix_comparison_jobs_sample','sample_id','id'),
    )
    id:Mapped[int]=mapped_column(primary_key=True)
    sample_id:Mapped[int]=mapped_column(ForeignKey('fuel_samples.id'))
    batch_id:Mapped[int]=mapped_column(ForeignKey('tank_batches.id'))
    state:Mapped[str]=mapped_column(String(20),default='queued')
    attempts:Mapped[int]=mapped_column(Integer,default=0)
    queued_at:Mapped[str]=mapped_column(String(40))
    started_at:Mapped[str|None]=mapped_column(String(40),nullable=True)
    finished_at:Mapped[str|None]=mapped_column(String(40),nullable=True)
    result_status:Mapped[str]=mapped_column(String(20),default='')
    detail:Mapped[str]=mapped_column(String(600),default='')
