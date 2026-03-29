import pytest
from synapse.synapse_pool_manager import (
    PoolType, DedicatedSqlTier, PoolState, AutoPauseMode,
    DwuConfig, SynapsePool, SynapseWorkspace
)


# ── DwuConfig tests ──────────────────────────────────────────────────────────

def test_dwuconfig_credits_dw100c():
    cfg = DwuConfig(tier=DedicatedSqlTier.DW100c)
    assert cfg.credits_per_hour() == 1

def test_dwuconfig_credits_dw200c():
    cfg = DwuConfig(tier=DedicatedSqlTier.DW200c)
    assert cfg.credits_per_hour() == 2

def test_dwuconfig_credits_dw300c():
    cfg = DwuConfig(tier=DedicatedSqlTier.DW300c)
    assert cfg.credits_per_hour() == 3

def test_dwuconfig_credits_dw500c():
    cfg = DwuConfig(tier=DedicatedSqlTier.DW500c)
    assert cfg.credits_per_hour() == 5

def test_dwuconfig_credits_dw1000c():
    cfg = DwuConfig(tier=DedicatedSqlTier.DW1000c)
    assert cfg.credits_per_hour() == 10

def test_dwuconfig_credits_dw2000c():
    cfg = DwuConfig(tier=DedicatedSqlTier.DW2000c)
    assert cfg.credits_per_hour() == 20

def test_dwuconfig_auto_pause_disabled_by_default():
    cfg = DwuConfig(tier=DedicatedSqlTier.DW100c)
    assert not cfg.is_auto_pause_enabled()

def test_dwuconfig_auto_pause_enabled():
    cfg = DwuConfig(tier=DedicatedSqlTier.DW100c, auto_pause=AutoPauseMode.ENABLED)
    assert cfg.is_auto_pause_enabled()

def test_dwuconfig_auto_pause_scheduled():
    cfg = DwuConfig(tier=DedicatedSqlTier.DW100c, auto_pause=AutoPauseMode.SCHEDULED)
    assert cfg.is_auto_pause_enabled()

def test_dwuconfig_not_large_pool_dw100c():
    cfg = DwuConfig(tier=DedicatedSqlTier.DW100c)
    assert not cfg.is_large_pool()

def test_dwuconfig_not_large_pool_dw500c():
    cfg = DwuConfig(tier=DedicatedSqlTier.DW500c)
    assert not cfg.is_large_pool()

def test_dwuconfig_large_pool_dw1000c():
    cfg = DwuConfig(tier=DedicatedSqlTier.DW1000c)
    assert cfg.is_large_pool()

def test_dwuconfig_large_pool_dw2000c():
    cfg = DwuConfig(tier=DedicatedSqlTier.DW2000c)
    assert cfg.is_large_pool()

def test_dwuconfig_pause_delay_default():
    cfg = DwuConfig(tier=DedicatedSqlTier.DW100c)
    assert cfg.pause_delay_minutes == 30

def test_dwuconfig_pause_delay_custom():
    cfg = DwuConfig(tier=DedicatedSqlTier.DW200c, pause_delay_minutes=60)
    assert cfg.pause_delay_minutes == 60


# ── SynapsePool tests ────────────────────────────────────────────────────────

def test_pool_is_dedicated_sql():
    pool = SynapsePool("p1", "sql-pool", PoolType.DEDICATED_SQL, PoolState.ONLINE)
    assert pool.is_dedicated_sql()
    assert not pool.is_spark()
    assert not pool.is_serverless()

def test_pool_is_spark():
    pool = SynapsePool("p2", "spark-pool", PoolType.SPARK, PoolState.ONLINE)
    assert pool.is_spark()
    assert not pool.is_dedicated_sql()

def test_pool_is_serverless():
    pool = SynapsePool("p3", "serverless", PoolType.SERVERLESS_SQL, PoolState.ONLINE)
    assert pool.is_serverless()

def test_pool_is_online():
    pool = SynapsePool("p4", "pool", PoolType.DEDICATED_SQL, PoolState.ONLINE)
    assert pool.is_online()
    assert not pool.is_paused()

def test_pool_is_paused():
    pool = SynapsePool("p5", "pool", PoolType.DEDICATED_SQL, PoolState.PAUSED)
    assert pool.is_paused()
    assert not pool.is_online()

def test_pool_is_scaling():
    pool = SynapsePool("p6", "pool", PoolType.DEDICATED_SQL, PoolState.SCALING)
    assert pool.is_scaling()
    assert not pool.is_online()

def test_pool_data_explorer_type():
    pool = SynapsePool("p7", "de-pool", PoolType.DATA_EXPLORER, PoolState.ONLINE)
    assert not pool.is_dedicated_sql()
    assert not pool.is_spark()
    assert not pool.is_serverless()

def test_pool_provisioning_state():
    pool = SynapsePool("p8", "pool", PoolType.DEDICATED_SQL, PoolState.PROVISIONING)
    assert not pool.is_online()
    assert not pool.is_paused()


# ── SynapseWorkspace tests ───────────────────────────────────────────────────

def make_workspace():
    return SynapseWorkspace("ws1", "analytics-ws", "uksouth")

def test_workspace_add_and_get_pool():
    ws = make_workspace()
    pool = SynapsePool("p1", "sql", PoolType.DEDICATED_SQL, PoolState.ONLINE)
    ws.add_pool(pool)
    assert ws.get_pool("p1") is pool

def test_workspace_duplicate_pool_raises():
    ws = make_workspace()
    pool = SynapsePool("p1", "sql", PoolType.DEDICATED_SQL, PoolState.ONLINE)
    ws.add_pool(pool)
    with pytest.raises(ValueError):
        ws.add_pool(pool)

def test_workspace_get_nonexistent_pool():
    ws = make_workspace()
    assert ws.get_pool("nonexistent") is None

def test_workspace_pools_property():
    ws = make_workspace()
    ws.add_pool(SynapsePool("p1", "sql", PoolType.DEDICATED_SQL, PoolState.ONLINE))
    ws.add_pool(SynapsePool("p2", "spark", PoolType.SPARK, PoolState.ONLINE))
    assert len(ws.pools) == 2

def test_workspace_online_pools():
    ws = make_workspace()
    ws.add_pool(SynapsePool("p1", "sql", PoolType.DEDICATED_SQL, PoolState.ONLINE))
    ws.add_pool(SynapsePool("p2", "sql2", PoolType.DEDICATED_SQL, PoolState.PAUSED))
    assert len(ws.online_pools) == 1

def test_workspace_paused_pools():
    ws = make_workspace()
    ws.add_pool(SynapsePool("p1", "sql", PoolType.DEDICATED_SQL, PoolState.PAUSED))
    ws.add_pool(SynapsePool("p2", "spark", PoolType.SPARK, PoolState.ONLINE))
    assert len(ws.paused_pools) == 1

def test_workspace_dedicated_sql_pools():
    ws = make_workspace()
    ws.add_pool(SynapsePool("p1", "sql", PoolType.DEDICATED_SQL, PoolState.ONLINE))
    ws.add_pool(SynapsePool("p2", "spark", PoolType.SPARK, PoolState.ONLINE))
    ws.add_pool(SynapsePool("p3", "serverless", PoolType.SERVERLESS_SQL, PoolState.ONLINE))
    assert len(ws.dedicated_sql_pools) == 1

def test_workspace_spark_pools():
    ws = make_workspace()
    ws.add_pool(SynapsePool("p1", "spark1", PoolType.SPARK, PoolState.ONLINE))
    ws.add_pool(SynapsePool("p2", "spark2", PoolType.SPARK, PoolState.PAUSED))
    ws.add_pool(SynapsePool("p3", "sql", PoolType.DEDICATED_SQL, PoolState.ONLINE))
    assert len(ws.spark_pools) == 2

def test_workspace_total_credits_online_only():
    ws = make_workspace()
    cfg1 = DwuConfig(tier=DedicatedSqlTier.DW500c)
    cfg2 = DwuConfig(tier=DedicatedSqlTier.DW1000c)
    ws.add_pool(SynapsePool("p1", "sql1", PoolType.DEDICATED_SQL, PoolState.ONLINE, dwu_config=cfg1))
    ws.add_pool(SynapsePool("p2", "sql2", PoolType.DEDICATED_SQL, PoolState.PAUSED, dwu_config=cfg2))
    # Only online pools count
    assert ws.total_dws_credits_per_hour() == 5

def test_workspace_total_credits_no_pools():
    ws = make_workspace()
    assert ws.total_dws_credits_per_hour() == 0

def test_workspace_total_credits_multiple_online():
    ws = make_workspace()
    cfg1 = DwuConfig(tier=DedicatedSqlTier.DW200c)
    cfg2 = DwuConfig(tier=DedicatedSqlTier.DW300c)
    ws.add_pool(SynapsePool("p1", "sql1", PoolType.DEDICATED_SQL, PoolState.ONLINE, dwu_config=cfg1))
    ws.add_pool(SynapsePool("p2", "sql2", PoolType.DEDICATED_SQL, PoolState.ONLINE, dwu_config=cfg2))
    assert ws.total_dws_credits_per_hour() == 5

def test_workspace_spark_pool_not_counted_in_credits():
    ws = make_workspace()
    ws.add_pool(SynapsePool("p1", "spark", PoolType.SPARK, PoolState.ONLINE))
    assert ws.total_dws_credits_per_hour() == 0

def test_workspace_summary_keys():
    ws = make_workspace()
    s = ws.summary()
    assert "workspace_id" in s
    assert "total_pools" in s
    assert "online_pools" in s
    assert "total_dws_credits_per_hour" in s

def test_workspace_summary_values():
    ws = make_workspace()
    cfg = DwuConfig(tier=DedicatedSqlTier.DW100c)
    ws.add_pool(SynapsePool("p1", "sql", PoolType.DEDICATED_SQL, PoolState.ONLINE, dwu_config=cfg))
    s = ws.summary()
    assert s["total_pools"] == 1
    assert s["online_pools"] == 1
    assert s["total_dws_credits_per_hour"] == 1

def test_workspace_empty_summary():
    ws = make_workspace()
    s = ws.summary()
    assert s["total_pools"] == 0
    assert s["online_pools"] == 0
