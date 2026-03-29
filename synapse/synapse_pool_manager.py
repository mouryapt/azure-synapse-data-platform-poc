from enum import Enum
from dataclasses import dataclass, field
from typing import List, Dict, Optional


class PoolType(Enum):
    DEDICATED_SQL = "dedicated_sql"
    SERVERLESS_SQL = "serverless_sql"
    SPARK = "spark"
    DATA_EXPLORER = "data_explorer"


class DedicatedSqlTier(Enum):
    DW100c = "DW100c"
    DW200c = "DW200c"
    DW300c = "DW300c"
    DW500c = "DW500c"
    DW1000c = "DW1000c"
    DW2000c = "DW2000c"


class PoolState(Enum):
    ONLINE = "online"
    PAUSED = "paused"
    SCALING = "scaling"
    RESUMING = "resuming"
    PAUSING = "pausing"
    PROVISIONING = "provisioning"


class AutoPauseMode(Enum):
    ENABLED = "enabled"
    DISABLED = "disabled"
    SCHEDULED = "scheduled"


@dataclass
class DwuConfig:
    tier: DedicatedSqlTier
    auto_pause: AutoPauseMode = AutoPauseMode.DISABLED
    pause_delay_minutes: int = 30

    def _get_credits_map(self):
        return {
            DedicatedSqlTier.DW100c: 1,
            DedicatedSqlTier.DW200c: 2,
            DedicatedSqlTier.DW300c: 3,
            DedicatedSqlTier.DW500c: 5,
            DedicatedSqlTier.DW1000c: 10,
            DedicatedSqlTier.DW2000c: 20,
        }

    def credits_per_hour(self) -> int:
        return self._get_credits_map()[self.tier]

    def is_auto_pause_enabled(self) -> bool:
        return self.auto_pause in (AutoPauseMode.ENABLED, AutoPauseMode.SCHEDULED)

    def is_large_pool(self) -> bool:
        return self.tier in (DedicatedSqlTier.DW1000c, DedicatedSqlTier.DW2000c)


@dataclass
class SynapsePool:
    pool_id: str
    pool_name: str
    pool_type: PoolType
    state: PoolState
    dwu_config: Optional[DwuConfig] = None

    def is_dedicated_sql(self) -> bool:
        return self.pool_type == PoolType.DEDICATED_SQL

    def is_spark(self) -> bool:
        return self.pool_type == PoolType.SPARK

    def is_serverless(self) -> bool:
        return self.pool_type == PoolType.SERVERLESS_SQL

    def is_online(self) -> bool:
        return self.state == PoolState.ONLINE

    def is_paused(self) -> bool:
        return self.state == PoolState.PAUSED

    def is_scaling(self) -> bool:
        return self.state == PoolState.SCALING


@dataclass
class SynapseWorkspace:
    workspace_id: str
    workspace_name: str
    region: str
    _pools: Dict[str, SynapsePool] = field(default_factory=dict)

    def add_pool(self, pool: SynapsePool) -> None:
        if pool.pool_id in self._pools:
            raise ValueError(f"Pool {pool.pool_id} already exists in workspace")
        self._pools[pool.pool_id] = pool

    def get_pool(self, pool_id: str) -> Optional[SynapsePool]:
        return self._pools.get(pool_id)

    @property
    def pools(self) -> List[SynapsePool]:
        return list(self._pools.values())

    @property
    def online_pools(self) -> List[SynapsePool]:
        return [p for p in self._pools.values() if p.is_online()]

    @property
    def paused_pools(self) -> List[SynapsePool]:
        return [p for p in self._pools.values() if p.is_paused()]

    @property
    def dedicated_sql_pools(self) -> List[SynapsePool]:
        return [p for p in self._pools.values() if p.is_dedicated_sql()]

    @property
    def spark_pools(self) -> List[SynapsePool]:
        return [p for p in self._pools.values() if p.is_spark()]

    def total_dws_credits_per_hour(self) -> int:
        total = 0
        for p in self._pools.values():
            if p.is_dedicated_sql() and p.is_online() and p.dwu_config:
                total += p.dwu_config.credits_per_hour()
        return total

    def summary(self) -> dict:
        return {
            "workspace_id": self.workspace_id,
            "workspace_name": self.workspace_name,
            "region": self.region,
            "total_pools": len(self._pools),
            "online_pools": len(self.online_pools),
            "paused_pools": len(self.paused_pools),
            "dedicated_sql_pools": len(self.dedicated_sql_pools),
            "spark_pools": len(self.spark_pools),
            "total_dws_credits_per_hour": self.total_dws_credits_per_hour(),
        }
