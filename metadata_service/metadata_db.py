import os
import json
import uuid
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Tuple
from sqlalchemy import (
    create_engine, Column, Integer, BigInteger, String, Text, DateTime,
    UniqueConstraint, func
)
from sqlalchemy.orm import declarative_base, sessionmaker, Session

Base = declarative_base()

class StoredBucket(Base):
    __tablename__ = 'buckets'

    name = Column(String(64), primary_key=True)
    policy = Column(String(32), nullable=False, default="replication") # "replication" or "erasure_coding"
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "policy": self.policy,
            "created_at": self.created_at.isoformat() if self.created_at else None
        }

class StoredObject(Base):
    __tablename__ = 'objects'

    id = Column(Integer, primary_key=True, autoincrement=True)
    bucket = Column(String(64), nullable=False, index=True)
    key = Column(String(256), nullable=False, index=True)
    version = Column(Integer, nullable=False, default=1)
    size = Column(BigInteger, nullable=False)
    chunk_hashes = Column(Text, nullable=False)  # JSON-encoded list of SHA-256 strings
    tags = Column(Text, nullable=False, default="{}") # JSON-encoded dict of user tags
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint('bucket', 'key', 'version', name='uq_bucket_key_version'),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "bucket": self.bucket,
            "key": self.key,
            "version": self.version,
            "size": self.size,
            "chunk_hashes": json.loads(self.chunk_hashes),
            "tags": json.loads(self.tags) if self.tags else {},
            "created_at": self.created_at.isoformat() if self.created_at else None
        }

class StoredChunk(Base):
    __tablename__ = 'chunks'

    chunk_hash = Column(String(64), primary_key=True) # SHA-256
    size = Column(Integer, nullable=False)
    ref_count = Column(Integer, nullable=False, default=1) # Deduplication Reference Count
    target_nodes = Column(Text, nullable=False) # JSON list of storage node IDs
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "chunk_hash": self.chunk_hash,
            "size": self.size,
            "ref_count": self.ref_count,
            "target_nodes": json.loads(self.target_nodes),
            "created_at": self.created_at.isoformat() if self.created_at else None
        }

class StoredMultipart(Base):
    __tablename__ = 'multipart_uploads'

    upload_id = Column(String(64), primary_key=True)
    bucket = Column(String(64), nullable=False)
    key = Column(String(256), nullable=False)
    parts = Column(Text, nullable=False, default="{}") # JSON mapping part_num -> list[chunk_hash]
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

class MetadataDatabase:
    """
    Metadata Database Manager supporting PostgreSQL and SQLite.
    Implements buckets, object cataloging, chunk deduplication, multipart uploads, and search.
    """
    def __init__(self, db_url: Optional[str] = None):
        if not db_url:
            db_url = os.getenv(
                "DATABASE_URL",
                f"sqlite:///{os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'metadata.db'))}"
            )
        
        if db_url.startswith("sqlite:///"):
            sqlite_path = db_url.replace("sqlite:///", "")
            os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)

        self.engine = create_engine(db_url, echo=False)
        Base.metadata.create_all(self.engine)
        self.SessionLocal = sessionmaker(bind=self.engine)

    # --- Buckets Management ---
    def create_bucket(self, name: str, policy: str = "replication") -> Dict[str, Any]:
        session: Session = self.SessionLocal()
        try:
            existing = session.query(StoredBucket).filter_by(name=name).first()
            if existing:
                return existing.to_dict()
            b = StoredBucket(name=name, policy=policy)
            session.add(b)
            session.commit()
            return b.to_dict()
        finally:
            session.close()

    def list_buckets(self) -> List[Dict[str, Any]]:
        session: Session = self.SessionLocal()
        try:
            return [b.to_dict() for b in session.query(StoredBucket).all()]
        finally:
            session.close()

    # --- Object Registration & Deduplication ---
    def register_object(
        self,
        bucket: str,
        key: str,
        size: int,
        chunks: List[Dict[str, Any]],
        tags: Optional[Dict[str, str]] = None,
        version: Optional[int] = None
    ) -> Dict[str, Any]:
        session: Session = self.SessionLocal()
        try:
            if version is None:
                max_v = session.query(func.max(StoredObject.version))\
                    .filter_by(bucket=bucket, key=key).scalar()
                version = (max_v or 0) + 1

            chunk_hashes = []
            new_chunks_count = 0
            dedup_chunks_count = 0
            bytes_saved_by_dedup = 0

            for c in chunks:
                c_hash = c["hash"]
                c_size = c["size"]
                c_nodes = c.get("target_nodes", [])
                chunk_hashes.append(c_hash)

                existing_chunk = session.query(StoredChunk).filter_by(chunk_hash=c_hash).first()
                if existing_chunk:
                    existing_chunk.ref_count += 1
                    dedup_chunks_count += 1
                    bytes_saved_by_dedup += c_size
                else:
                    new_chunk = StoredChunk(
                        chunk_hash=c_hash,
                        size=c_size,
                        ref_count=1,
                        target_nodes=json.dumps(c_nodes)
                    )
                    session.add(new_chunk)
                    new_chunks_count += 1

            obj = StoredObject(
                bucket=bucket,
                key=key,
                version=version,
                size=size,
                chunk_hashes=json.dumps(chunk_hashes),
                tags=json.dumps(tags or {})
            )
            session.add(obj)
            session.commit()

            return {
                "bucket": bucket,
                "key": key,
                "version": version,
                "size": size,
                "total_chunks": len(chunks),
                "new_chunks_stored": new_chunks_count,
                "deduplicated_chunks": dedup_chunks_count,
                "bytes_saved_by_dedup": bytes_saved_by_dedup,
                "dedup_ratio_pct": round((dedup_chunks_count / max(len(chunks), 1)) * 100.0, 2),
                "chunk_hashes": chunk_hashes,
                "tags": tags or {}
            }
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def get_object_metadata(
        self,
        bucket: str,
        key: str,
        version: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        session: Session = self.SessionLocal()
        try:
            query = session.query(StoredObject).filter_by(bucket=bucket, key=key)
            if version is not None:
                obj = query.filter_by(version=version).first()
            else:
                obj = query.order_by(StoredObject.version.desc()).first()
            return obj.to_dict() if obj else None
        finally:
            session.close()

    def delete_object(
        self,
        bucket: str,
        key: str,
        version: Optional[int] = None
    ) -> Dict[str, Any]:
        session: Session = self.SessionLocal()
        try:
            query = session.query(StoredObject).filter_by(bucket=bucket, key=key)
            if version is not None:
                obj = query.filter_by(version=version).first()
            else:
                obj = query.order_by(StoredObject.version.desc()).first()

            if not obj:
                return {"status": "not_found", "message": f"Object {bucket}/{key} not found"}

            chunk_hashes = json.loads(obj.chunk_hashes)
            chunks_decremented = 0
            chunks_orphaned_for_deletion = []

            for c_hash in chunk_hashes:
                c_record = session.query(StoredChunk).filter_by(chunk_hash=c_hash).first()
                if c_record:
                    c_record.ref_count -= 1
                    chunks_decremented += 1
                    if c_record.ref_count <= 0:
                        chunks_orphaned_for_deletion.append({
                            "chunk_hash": c_record.chunk_hash,
                            "target_nodes": json.loads(c_record.target_nodes)
                        })
                        session.delete(c_record)

            session.delete(obj)
            session.commit()

            return {
                "status": "deleted",
                "bucket": bucket,
                "key": key,
                "version": obj.version,
                "chunks_decremented": chunks_decremented,
                "chunks_orphaned_for_deletion": chunks_orphaned_for_deletion,
                "orphaned_count": len(chunks_orphaned_for_deletion)
            }
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def get_chunk(self, chunk_hash: str) -> Optional[Dict[str, Any]]:
        session: Session = self.SessionLocal()
        try:
            c = session.query(StoredChunk).filter_by(chunk_hash=chunk_hash).first()
            return c.to_dict() if c else None
        finally:
            session.close()

    # --- Search & Query Filter ---
    def search_objects(
        self,
        bucket: Optional[str] = None,
        prefix: Optional[str] = None,
        min_size: Optional[int] = None,
        max_size: Optional[int] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        tag_key: Optional[str] = None,
        tag_value: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        session: Session = self.SessionLocal()
        try:
            query = session.query(StoredObject)
            if bucket:
                query = query.filter(StoredObject.bucket == bucket)
            if prefix:
                query = query.filter(StoredObject.key.like(f"{prefix}%"))
            if min_size is not None:
                query = query.filter(StoredObject.size >= min_size)
            if max_size is not None:
                query = query.filter(StoredObject.size <= max_size)

            results = [obj.to_dict() for obj in query.all()]

            # Apply tag filter if requested
            if tag_key:
                filtered = []
                for r in results:
                    obj_tags = r.get("tags", {})
                    if tag_key in obj_tags:
                        if tag_value is None or obj_tags[tag_key] == tag_value:
                            filtered.append(r)
                return filtered

            return results
        finally:
            session.close()

    # --- Multipart Upload Support ---
    def initiate_multipart(self, bucket: str, key: str) -> str:
        session: Session = self.SessionLocal()
        try:
            upload_id = str(uuid.uuid4())
            rec = StoredMultipart(upload_id=upload_id, bucket=bucket, key=key, parts="{}")
            session.add(rec)
            session.commit()
            return upload_id
        finally:
            session.close()

    def record_multipart_part(self, upload_id: str, part_number: int, chunk_specs: List[Dict[str, Any]]) -> Dict[str, Any]:
        session: Session = self.SessionLocal()
        try:
            rec = session.query(StoredMultipart).filter_by(upload_id=upload_id).first()
            if not rec:
                raise ValueError(f"Upload ID {upload_id} not found")
            parts = json.loads(rec.parts)
            parts[str(part_number)] = chunk_specs
            rec.parts = json.dumps(parts)
            session.commit()
            return {"upload_id": upload_id, "part_number": part_number, "chunks_in_part": len(chunk_specs)}
        finally:
            session.close()

    def complete_multipart(self, upload_id: str, tags: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        session: Session = self.SessionLocal()
        try:
            rec = session.query(StoredMultipart).filter_by(upload_id=upload_id).first()
            if not rec:
                raise ValueError(f"Upload ID {upload_id} not found")
            parts = json.loads(rec.parts)
            sorted_part_nums = sorted([int(k) for k in parts.keys()])
            all_chunks = []
            total_size = 0
            for p_num in sorted_part_nums:
                p_chunks = parts[str(p_num)]
                for c in p_chunks:
                    all_chunks.append(c)
                    total_size += c["size"]

            bucket = rec.bucket
            key = rec.key
            session.delete(rec)
            session.commit()

            return self.register_object(bucket=bucket, key=key, size=total_size, chunks=all_chunks, tags=tags)
        finally:
            session.close()

    def get_dedup_statistics(self) -> Dict[str, Any]:
        session: Session = self.SessionLocal()
        try:
            total_unique_chunks = session.query(StoredChunk).count()
            total_referenced_chunks = session.query(func.sum(StoredChunk.ref_count)).scalar() or 0
            total_unique_bytes = session.query(func.sum(StoredChunk.size)).scalar() or 0
            total_virtual_bytes = session.query(
                func.sum(StoredChunk.size * StoredChunk.ref_count)
            ).scalar() or 0

            saved_bytes = total_virtual_bytes - total_unique_bytes
            savings_pct = (saved_bytes / max(total_virtual_bytes, 1)) * 100.0

            return {
                "unique_chunks_stored": total_unique_chunks,
                "total_chunk_references": total_referenced_chunks,
                "physical_storage_bytes": total_unique_bytes,
                "logical_storage_bytes": total_virtual_bytes,
                "bytes_saved_by_dedup": saved_bytes,
                "storage_reduction_pct": round(savings_pct, 2)
            }
        finally:
            session.close()
