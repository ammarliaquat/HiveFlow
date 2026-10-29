# src/metadata/recommendation_service.py
"""
RecommendationService — Decoupled Metadata OS Core Governance Layer.
Manages recommendation observations, steward review actions (Approve/Override),
and active version invariants (is_active_version).
Never mixes execution runtime properties or generic tuning into table_recommendations.
"""
import json
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime
from dataclasses import dataclass
import psycopg2.extras

logger = logging.getLogger("RecommendationService")


@dataclass
class RecommendationResult:
    allowed: bool
    cdc_column: Optional[str]
    cdc_type: Optional[str]
    strategy: Optional[str]
    reason: str
    recommendation_id: Optional[int] = None
    table_id: Optional[int] = None
    version_num: Optional[int] = None
    is_active_version: bool = True
    confidence_level: Optional[str] = None
    confidence_reasons: Any = None
    performance_risk: Optional[str] = None
    correctness_risk: Optional[str] = None
    recommendation_status: Optional[str] = None
    steward_user: Optional[str] = None
    steward_notes: Optional[str] = None
    reviewed_at: Optional[datetime] = None
    last_verified_at: Optional[datetime] = None
    generated_at: Optional[datetime] = None

    def __getitem__(self, item):
        return getattr(self, item)

    def get(self, item, default=None):
        return getattr(self, item, default)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "allowed": self.allowed,
            "cdc_column": self.cdc_column,
            "cdc_type": self.cdc_type,
            "strategy": self.strategy,
            "reason": self.reason,
            "recommendation_id": self.recommendation_id,
            "table_id": self.table_id,
            "version_num": self.version_num,
            "is_active_version": self.is_active_version,
            "confidence_level": self.confidence_level,
            "confidence_reasons": self.confidence_reasons,
            "performance_risk": self.performance_risk,
            "correctness_risk": self.correctness_risk,
            "recommendation_status": self.recommendation_status,
            "steward_user": self.steward_user,
            "steward_notes": self.steward_notes,
            "reviewed_at": self.reviewed_at,
            "last_verified_at": self.last_verified_at,
            "generated_at": self.generated_at
        }


class RecommendationService:
    """
    Decoupled service repository for querying and reviewing table recommendations.
    Bounded exclusively to CDC strategies and table classification observations.
    All public methods accept a live database connection (psycopg2 connection object).
    """

    @classmethod
    def _build_result(cls, row: Optional[Dict[str, Any]]) -> RecommendationResult:
        if not row:
            return RecommendationResult(
                allowed=False,
                cdc_column=None,
                cdc_type=None,
                strategy=None,
                reason="Table has no active recommendation observation (UNOBSERVED)."
            )

        status = (row.get("recommendation_status") or "UNOBSERVED").upper()
        allowed = status in ("APPROVED", "OVERRIDDEN")
        if allowed:
            reason = f"Governance {status} (v{row.get('version_num', 1)})"
        else:
            reason = f"Table requires Data Steward approval (status={status})."

        return RecommendationResult(
            allowed=allowed,
            cdc_column=row.get("recommended_value"),
            cdc_type=row.get("recommended_type"),
            strategy=row.get("recommended_type"),
            reason=reason,
            recommendation_id=row.get("recommendation_id"),
            table_id=row.get("table_id"),
            version_num=row.get("version_num"),
            is_active_version=bool(row.get("is_active_version", True)),
            confidence_level=row.get("confidence_level"),
            confidence_reasons=row.get("confidence_reasons"),
            performance_risk=row.get("performance_risk"),
            correctness_risk=row.get("correctness_risk"),
            recommendation_status=status,
            steward_user=row.get("steward_user"),
            steward_notes=row.get("steward_notes"),
            reviewed_at=row.get("reviewed_at"),
            last_verified_at=row.get("last_verified_at"),
            generated_at=row.get("generated_at")
        )

    @classmethod
    def get_effective_recommendation(cls, conn, table_id: int) -> Optional[RecommendationResult]:
        """
        Retrieves the active recommendation (is_active_version = TRUE) for a table_id.
        """
        query = """
            SELECT recommendation_id, table_id, version_num, is_active_version,
                   recommendation_type, recommended_value, recommended_type,
                   confidence_level, confidence_reasons, performance_risk, correctness_risk,
                   recommendation_status, steward_user, steward_notes, reviewed_at, last_verified_at,
                   generated_at
            FROM metadata.table_recommendations
            WHERE table_id = %s AND is_active_version = TRUE
            ORDER BY version_num DESC LIMIT 1
        """
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, [table_id])
            row = cur.fetchone()
            if row and isinstance(row.get("confidence_reasons"), str):
                try:
                    row["confidence_reasons"] = json.loads(row["confidence_reasons"])
                except Exception:
                    pass
            return cls._build_result(row) if row else None

    @classmethod
    def get_effective_recommendation_by_tag_table(cls, conn, source_tag: str, table_name: str) -> Optional[RecommendationResult]:
        """
        Retrieves the active recommendation by joining db_tables on source_tag and table_name.
        """
        query = """
            SELECT r.recommendation_id, r.table_id, r.version_num, r.is_active_version,
                   r.recommendation_type, r.recommended_value, r.recommended_type,
                   r.confidence_level, r.confidence_reasons, r.performance_risk, r.correctness_risk,
                   r.recommendation_status, r.steward_user, r.steward_notes, r.reviewed_at, r.last_verified_at,
                   r.generated_at
            FROM metadata.table_recommendations r
            JOIN metadata.db_tables t ON t.table_master_key = r.table_id
            WHERE t.source_tag = %s AND t.table_name = %s AND r.is_active_version = TRUE
            ORDER BY r.version_num DESC LIMIT 1
        """
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, [source_tag, table_name])
            row = cur.fetchone()
            if row and isinstance(row.get("confidence_reasons"), str):
                try:
                    row["confidence_reasons"] = json.loads(row["confidence_reasons"])
                except Exception:
                    pass
            return cls._build_result(row) if row else None

    @classmethod
    def get_all_versions(cls, conn, table_id: int) -> List[Dict[str, Any]]:
        """
        Retrieves the complete audit version history for a table_id, ordered newest first.
        """
        query = """
            SELECT recommendation_id, table_id, version_num, is_active_version,
                   recommendation_type, recommended_value, recommended_type,
                   confidence_level, confidence_reasons, performance_risk, correctness_risk,
                   recommendation_status, steward_user, steward_notes, reviewed_at, last_verified_at,
                   generated_at
            FROM metadata.table_recommendations
            WHERE table_id = %s
            ORDER BY version_num DESC
        """
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, [table_id])
            rows = cur.fetchall()
            for r in rows:
                if isinstance(r.get("confidence_reasons"), str):
                    try:
                        r["confidence_reasons"] = json.loads(r["confidence_reasons"])
                    except Exception:
                        pass
            return rows

    @classmethod
    def get_governance_queue(cls, conn, status_filter: Optional[str] = "PENDING") -> List[Dict[str, Any]]:
        """
        Retrieves table catalog items along with their active recommendation for steward review.
        """
        query = """
            SELECT t.table_master_key AS table_id, t.source_tag, t.schema_name, t.table_name,
                   COALESCE(t.table_row_count, 0) AS table_row_count,
                   r.recommendation_id, r.version_num, r.is_active_version,
                   r.recommendation_type, r.recommended_value, r.recommended_type,
                   r.confidence_level, r.confidence_reasons, r.performance_risk, r.correctness_risk,
                   COALESCE(r.recommendation_status, 'UNOBSERVED') AS recommendation_status,
                   r.steward_user, r.steward_notes, r.reviewed_at, r.last_verified_at
            FROM metadata.db_tables t
            LEFT JOIN metadata.table_recommendations r 
                   ON r.table_id = t.table_master_key AND r.is_active_version = TRUE
            WHERE (%s IS NULL OR COALESCE(r.recommendation_status, 'UNOBSERVED') = %s)
            ORDER BY t.source_tag, t.table_name
        """
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, [status_filter, status_filter])
            rows = cur.fetchall()
            for r in rows:
                if isinstance(r.get("confidence_reasons"), str):
                    try:
                        r["confidence_reasons"] = json.loads(r["confidence_reasons"])
                    except Exception:
                        pass
            return rows

    @classmethod
    def record_candidate(cls, conn, table_id: int, strategy: str, value: str, recommended_type: str,
                         confidence_level: str, confidence_reasons: List[str],
                         performance_risk: str = "LOW", correctness_risk: str = "LOW") -> int:
        """
        Records a discovery observation. Discovery is pure observation and NEVER deactivates approved versions.
        If an approved/overridden V1 exists and discovery observes a drift/change, inserts V2 as inactive (is_active_version = FALSE).
        Only steward actions (approve/override) transition is_active_version.
        """
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Look up existing active recommendation and highest version number
            cur.execute("""
                SELECT recommendation_id, version_num, is_active_version, recommendation_status,
                       recommended_value, recommended_type
                FROM metadata.table_recommendations
                WHERE table_id = %s AND is_active_version = TRUE
                ORDER BY version_num DESC LIMIT 1
            """, [table_id])
            active = cur.fetchone()

            cur.execute("""
                SELECT COALESCE(MAX(version_num), 0) FROM metadata.table_recommendations
                WHERE table_id = %s
            """, [table_id])
            max_ver = cur.fetchone()[0] if isinstance(cur.fetchone(), (list, tuple)) else (cur.fetchone() or {"coalesce": 0}).get("coalesce", 0)
            # Re-run simple scalar max if dict cursor returned dict
            cur.execute("SELECT COALESCE(MAX(version_num), 0) AS max_v FROM metadata.table_recommendations WHERE table_id = %s", [table_id])
            max_ver = cur.fetchone()["max_v"]

            reasons_json = json.dumps(confidence_reasons)

            if active:
                status = (active["recommendation_status"] or "").upper()
                is_same = (value == active["recommended_value"] and recommended_type == active["recommended_type"])

                if status in ("APPROVED", "OVERRIDDEN"):
                    if is_same:
                        # Observation confirms active approved strategy -> touch last_verified_at
                        cur.execute("""
                            UPDATE metadata.table_recommendations
                            SET last_verified_at = CURRENT_TIMESTAMP
                            WHERE recommendation_id = %s
                        """, [active["recommendation_id"]])
                        conn.commit()
                        return active["recommendation_id"]
                    else:
                        # Discovery observed drift from approved V1 -> insert V2 (PENDING / GENERATED) as INACTIVE!
                        # Do NOT touch V1 is_active_version!
                        next_ver = max_ver + 1
                        cur.execute("""
                            INSERT INTO metadata.table_recommendations
                                (table_id, version_num, is_active_version, recommendation_type,
                                 recommended_value, recommended_type, confidence_level, confidence_reasons,
                                 performance_risk, correctness_risk, recommendation_status, last_verified_at)
                            VALUES
                                (%s, %s, FALSE, 'CDC_STRATEGY',
                                 %s, %s, %s, %s,
                                 %s, %s, 'GENERATED', CURRENT_TIMESTAMP)
                            RETURNING recommendation_id
                        """, [table_id, next_ver, value, recommended_type, confidence_level, reasons_json,
                              performance_risk, correctness_risk])
                        new_id = cur.fetchone()["recommendation_id"]
                        conn.commit()
                        logger.info(f"Discovery observed change for table_id={table_id}. Inserted inactive candidate v{next_ver} while keeping v{active['version_num']} active.")
                        return new_id
                else:
                    # Existing active observation is still unapproved (GENERATED/PENDING)
                    if is_same:
                        cur.execute("""
                            UPDATE metadata.table_recommendations
                            SET last_verified_at = CURRENT_TIMESTAMP,
                                confidence_level = %s,
                                confidence_reasons = %s,
                                performance_risk = %s,
                                correctness_risk = %s
                            WHERE recommendation_id = %s
                        """, [confidence_level, reasons_json, performance_risk, correctness_risk, active["recommendation_id"]])
                        conn.commit()
                        return active["recommendation_id"]
                    else:
                        # Unapproved candidate changed -> mark old unapproved candidate inactive and insert new candidate as active
                        cur.execute("""
                            UPDATE metadata.table_recommendations
                            SET is_active_version = FALSE
                            WHERE recommendation_id = %s
                        """, [active["recommendation_id"]])
                        next_ver = max_ver + 1
                        cur.execute("""
                            INSERT INTO metadata.table_recommendations
                                (table_id, version_num, is_active_version, recommendation_type,
                                 recommended_value, recommended_type, confidence_level, confidence_reasons,
                                 performance_risk, correctness_risk, recommendation_status, last_verified_at)
                            VALUES
                                (%s, %s, TRUE, 'CDC_STRATEGY',
                                 %s, %s, %s, %s,
                                 %s, %s, 'GENERATED', CURRENT_TIMESTAMP)
                            RETURNING recommendation_id
                        """, [table_id, next_ver, value, recommended_type, confidence_level, reasons_json,
                              performance_risk, correctness_risk])
                        new_id = cur.fetchone()["recommendation_id"]
                        conn.commit()
                        return new_id
            else:
                # No existing active version -> insert initial candidate as active
                next_ver = max_ver + 1
                cur.execute("""
                    INSERT INTO metadata.table_recommendations
                        (table_id, version_num, is_active_version, recommendation_type,
                         recommended_value, recommended_type, confidence_level, confidence_reasons,
                         performance_risk, correctness_risk, recommendation_status, last_verified_at)
                    VALUES
                        (%s, %s, TRUE, 'CDC_STRATEGY',
                         %s, %s, %s, %s,
                         %s, %s, 'GENERATED', CURRENT_TIMESTAMP)
                    RETURNING recommendation_id
                """, [table_id, next_ver, value, recommended_type, confidence_level, reasons_json,
                      performance_risk, correctness_risk])
                new_id = cur.fetchone()["recommendation_id"]
                conn.commit()
                return new_id

    @classmethod
    def approve_recommendation(cls, conn, table_id: int, steward_user: str, notes: Optional[str] = None) -> bool:
        """
        Approve a recommendation for a table_id.
        Enforces audit provenance and sets is_active_version = TRUE exclusively.
        Stewards edit steward_notes, but cannot modify confidence_reasons (which remain immutable).
        """
        if not steward_user or not steward_user.strip():
            raise ValueError("steward_user is required to approve a recommendation.")

        with conn.cursor() as cur:
            # Find target recommendation: highest version_num (either inactive candidate V2 or current active)
            cur.execute("""
                SELECT recommendation_id FROM metadata.table_recommendations
                WHERE table_id = %s
                ORDER BY version_num DESC LIMIT 1
            """, [table_id])
            target = cur.fetchone()
            if not target:
                logger.error(f"Cannot approve: No recommendation found for table_id={table_id}")
                return False
            
            target_id = target[0] if isinstance(target, tuple) else target["recommendation_id"]

            # Flip other versions to FALSE so partial unique index invariant holds cleanly
            cur.execute("""
                UPDATE metadata.table_recommendations
                SET is_active_version = FALSE
                WHERE table_id = %s AND recommendation_id != %s
            """, [table_id, target_id])

            # Update target to APPROVED with audit provenance
            cur.execute("""
                UPDATE metadata.table_recommendations
                SET recommendation_status = 'APPROVED',
                    steward_user = %s,
                    steward_notes = COALESCE(%s, steward_notes),
                    reviewed_at = CURRENT_TIMESTAMP,
                    last_verified_at = CURRENT_TIMESTAMP,
                    is_active_version = TRUE
                WHERE recommendation_id = %s
            """, [steward_user.strip(), notes, target_id])

        conn.commit()
        logger.info(f"Recommendation {target_id} approved for table_id={table_id} by {steward_user}")
        return True

    @classmethod
    def override_recommendation(cls, conn, table_id: int, custom_strategy: str, custom_column: Optional[str],
                                steward_user: str, notes: Optional[str] = None) -> bool:
        """
        Steward overrides recommendation with a custom policy.
        Inserts version_num = MAX + 1 with status = 'OVERRIDDEN' and is_active_version = TRUE.
        Stewards edit steward_notes, while confidence_reasons records immutable override audit justification.
        """
        if not steward_user or not steward_user.strip():
            raise ValueError("steward_user is required to override a recommendation.")

        with conn.cursor() as cur:
            # Flip all existing versions to FALSE
            cur.execute("""
                UPDATE metadata.table_recommendations
                SET is_active_version = FALSE
                WHERE table_id = %s
            """, [table_id])

            # Get next version_num
            cur.execute("""
                SELECT COALESCE(MAX(version_num), 0) + 1 FROM metadata.table_recommendations
                WHERE table_id = %s
            """, [table_id])
            next_ver = cur.fetchone()[0]

            # Insert new OVERRIDDEN version
            cur.execute("""
                INSERT INTO metadata.table_recommendations
                    (table_id, version_num, is_active_version, recommendation_type,
                     recommended_value, recommended_type, confidence_level, confidence_reasons,
                     performance_risk, correctness_risk, recommendation_status,
                     steward_user, steward_notes, reviewed_at, last_verified_at)
                VALUES
                    (%s, %s, TRUE, 'CDC_STRATEGY',
                     %s, %s, 'HIGH', %s,
                     'LOW', 'LOW', 'OVERRIDDEN',
                     %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """, [
                table_id,
                next_ver,
                custom_column or "",
                custom_strategy,
                json.dumps(["Steward override policy assigned directly"]),
                steward_user.strip(),
                notes
            ])

        conn.commit()
        logger.info(f"Steward override version #{next_ver} created for table_id={table_id} by {steward_user}")
        return True
