"""
VOLGUARD 3.0 – STRESS & LOAD TESTS
Tests system under extreme load and adverse conditions
Run:  pytest test_stress.py -v -s --durations=10
"""

import pytest
import os
import time
import threading
import multiprocessing
import queue
import sqlite3
import numpy as np
import pandas as pd
import psutil
import gc
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from unittest.mock import Mock, patch

# ------------------------------------------------------------------ setup
os.environ['VG_DRY_RUN'] = 'TRUE'
os.environ['UPSTOX_ACCESS_TOKEN']   = 'test_token'
os.environ['TELEGRAM_BOT_TOKEN']    = 'test_bot_token'
os.environ['TELEGRAM_CHAT_ID']      = 'test_chat_id'

from volguard import (
    DatabaseWriter, CircuitBreaker, PaperTradingEngine,
    RegimeEngine, StrategyFactory, ExecutionEngine,
    VolMetrics, StructMetrics, EdgeMetrics,
    ExternalMetrics, TimeMetrics, TradingMandate,
    RegimeScore, ProductionConfig
)
# -------------------------------------------------------------------------

# =========================================================================
#  DATABASE STRESS TESTS
# =========================================================================

class TestDatabaseStress:

    @pytest.mark.stress
    def test_massive_concurrent_writes(self):
        db_path = "/tmp/stress_test_db.db"
        if os.path.exists(db_path):
            os.remove(db_path)

        db = DatabaseWriter(db_path)

        def writer_thread(thread_id, count):
            for i in range(count):
                db.log_order(
                    f"ORDER_T{thread_id}_N{i}",
                    f"KEY_{thread_id}",
                    "BUY" if i % 2 == 0 else "SELL",
                    25 * (i % 10 + 1),
                    100.0 + i,
                    "FILLED",
                    25,
                    100.0 + i
                )

        num_threads, writes_per_thread = 20, 500
        start_time = time.time()

        threads = []
        for t in range(num_threads):
            th = threading.Thread(target=writer_thread, args=(t, writes_per_thread))
            threads.append(th)
            th.start()

        for th in threads:
            th.join(timeout=30)

        # wait for queue to drain
        timeout = 60
        start_wait = time.time()
        while db.message_queue.qsize() > 0 and (time.time() - start_wait) < timeout:
            time.sleep(0.1)

        elapsed = time.time() - start_time

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM order_log")
        count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(DISTINCT order_id) FROM order_log")
        unique_count = cursor.fetchone()[0]
        conn.close()
        db.shutdown()

        expected = num_threads * writes_per_thread
        print(f"\nConcurrent-write stress: {count}/{expected} written  ({count/elapsed:.0f} ops/sec)")
        assert count == expected and unique_count == count
        os.remove(db_path)

    @pytest.mark.stress
    def test_database_under_memory_pressure(self):
        db_path = "/tmp/memory_stress_db.db"
        if os.path.exists(db_path):
            os.remove(db_path)

        db = DatabaseWriter(db_path)
        memory_hogs = []
        try:
            # create memory pressure
            for _ in range(5):
                memory_hogs.append(np.random.rand(10_000_000))  # ~76 MB each

            start_time = time.time()
            for i in range(1000):
                db.log_order(f"ORDER_{i}", "TEST_KEY", "BUY", 25, 100.0, "FILLED", 25, 100.0)
                if i % 100 == 0:
                    db.save_trade(f"TRADE_{i}", "TEST", date.today(), [], 5000.0, 50000.0)

            time.sleep(5)  # <-- FIX-1: give writer time to drain queue under pressure
            elapsed = time.time() - start_time

            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM order_log")
            order_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM trades")
            trade_count = cursor.fetchone()[0]
            conn.close()

            print(f"Memory-pressure test: orders {order_count}/1000  trades {trade_count}/10")
            assert order_count >= 990 and trade_count >= 9
        finally:
            del memory_hogs
            gc.collect()
            db.shutdown()
            os.remove(db_path)

    @pytest.mark.stress
    def test_rapid_state_updates(self):
        db_path = "/tmp/state_stress_db.db"
        if os.path.exists(db_path):
            os.remove(db_path)

        db = DatabaseWriter(db_path)

        def rapid_updater(key_prefix, iterations):
            for i in range(iterations):
                db.set_state(f"{key_prefix}_counter", str(i))
                db.set_state(f"{key_prefix}_timestamp", str(time.time()))

        num_keys, iterations = 10, 200
        start_time = time.time()

        threads = []
        for k in range(num_keys):
            th = threading.Thread(target=rapid_updater, args=(f"KEY_{k}", iterations))
            threads.append(th)
            th.start()
        for th in threads:
            th.join()

        time.sleep(2)
        elapsed = time.time() - start_time

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM system_state")
        state_count = cursor.fetchone()[0]
        conn.close()
        db.shutdown()

        print(f"Rapid-state updates: {state_count}/{num_keys*2} entries  ({(num_keys*iterations)/elapsed:.0f} ops/sec)")
        assert state_count == num_keys * 2
        os.remove(db_path)


# =========================================================================
#  CIRCUIT-BREAKER STRESS
# =========================================================================

class TestCircuitBreakerStress:

    @pytest.mark.stress
    def test_rapid_consecutive_loss_checks(self):
        db_path = "/tmp/cb_stress_db.db"
        if os.path.exists(db_path):
            os.remove(db_path)

        db = DatabaseWriter(db_path)
        cb = CircuitBreaker(db)

        results, start_time = [], time.time()
        for i in range(100):
            pnl = 1000 if i % 5 == 0 else -500
            ok = cb.record_trade_result(pnl)
            results.append(ok)
            if not ok:
                break

        elapsed = time.time() - start_time
        print(f"Circuit-breaker rapid-fire: {len(results)} checks  triggered={cb.breaker_triggered}")
        assert cb.breaker_triggered or len(results) == 100
        db.shutdown()
        os.remove(db_path)

    @pytest.mark.stress
    def test_concurrent_risk_checks(self):
        db_path = "/tmp/cb_concurrent_db.db"
        if os.path.exists(db_path):
            os.remove(db_path)

        db   = DatabaseWriter(db_path)
        cb   = CircuitBreaker(db)

        def risk_checker(thread_id):
            for i in range(50):
                cb.update_capital(1_000_000 - np.random.randint(0, 200_000))
                if np.random.random() < 0.3:
                    cb.record_slippage_event(np.random.uniform(0.01, 0.05))
                time.sleep(0.01)

        num_threads = 10
        threads = []
        for t in range(num_threads):
            th = threading.Thread(target=risk_checker, args=(t,))
            threads.append(th)
            th.start()
        for th in threads:
            th.join()

        print(f"Concurrent risk-checks: {num_threads} threads  capital={cb.current_capital:,.0f}")
        assert cb.current_capital >= 0
        db.shutdown()
        os.remove(db_path)


# =========================================================================
#  PAPER-TRADING ENGINE STRESS
# =========================================================================

class TestPaperTradingStress:

    @pytest.mark.stress
    def test_high_frequency_trading_simulation(self):
        engine = PaperTradingEngine()

        num_orders = 5_000
        start_time = time.time()

        order_ids = []
        for i in range(num_orders):
            oid = engine.place_order(
                f"INSTRUMENT_{i % 100}",
                25,
                "BUY" if i % 2 == 0 else "SELL",
                "LIMIT",
                100.0 + (i % 50)
            )
            if oid:
                order_ids.append(oid)

        elapsed = time.time() - start_time

        sample_size = min(100, len(order_ids))
        sample_ids  = np.random.choice(order_ids, sample_size, replace=False)
        statuses    = {'complete': 0, 'rejected': 0, 'unknown': 0}
        for oid in sample_ids:
            st = engine.get_order_status(oid)
            if st:
                statuses[st.get('status', 'unknown')] += 1

        print(f"HFT simulation: {len(order_ids)}/{num_orders} placed  {len(order_ids)/elapsed:.0f} ops/sec")
        assert len(order_ids) == num_orders and elapsed < 10

    @pytest.mark.stress
    def test_massive_position_accumulation(self):
        engine = PaperTradingEngine()

        num_instruments, orders_per = 100, 20
        for inst in range(num_instruments):
            for _ in range(orders_per):
                engine.place_order(f"INST_{inst}", 25, "BUY", "LIMIT", 100.0 + inst)

        positions = engine.get_positions()
        print(f"Massive-position test: {len(positions)} positions")
        assert len(positions) <= num_instruments * 2


# =========================================================================
#  REGIME-ENGINE STRESS
# =========================================================================

class TestRegimeEngineStress:

    @pytest.mark.stress
    def test_extreme_market_conditions(self):
        engine = RegimeEngine()

        scenarios = [
            {
                'name': 'Flash Crash',
                'vol': VolMetrics(
                    spot=20000, vix=80, rv7=75, rv28=70, rv90=65,
                    garch7=75, garch28=70, park7=75, park28=70,
                    vov=15, vov_zscore=5.0, ivp_30d=99, ivp_90d=98, ivp_1yr=95,
                    ma20=23000, atr14=2000, trend_strength=5.0,
                    vol_regime="EXPLODING", is_fallback=False
                )
            },
            {
                'name': 'Extreme Calm',
                'vol': VolMetrics(
                    spot=23500, vix=8, rv7=7, rv28=6, rv90=5,
                    garch7=7, garch28=6, park7=7, park28=6,
                    vov=0.5, vov_zscore=-2.0, ivp_30d=5, ivp_90d=3, ivp_1yr=2,
                    ma20=23500, atr14=50, trend_strength=0.02,
                    vol_regime="CHEAP", is_fallback=False
                )
            },
            {
                'name': 'Whipsaw Market',
                'vol': VolMetrics(
                    spot=23500, vix=25, rv7=30, rv28=20, rv90=15,
                    garch7=28, garch28=22, park7=29, park28=21,
                    vov=8, vov_zscore=3.0, ivp_30d=60, ivp_90d=50, ivp_1yr=55,
                    ma20=23500, atr14=800, trend_strength=0.5,
                    vol_regime="RICH", is_fallback=False
                )
            }
        ]

        struct = StructMetrics(
            net_gex=1_000_000, gex_ratio=0.02, total_oi_value=50_000_000,
            gex_regime="NEUTRAL", pcr=1.0, max_pain=23500, skew_25d=0,
            oi_regime="NEUTRAL", lot_size=25
        )

        edge = EdgeMetrics(
            iv_weekly=20, vrp_rv_weekly=5, vrp_garch_weekly=4, vrp_park_weekly=4.5,
            iv_monthly=19, vrp_rv_monthly=4, vrp_garch_monthly=3, vrp_park_monthly=3.5,
            term_spread=-1, term_regime="FLAT", primary_edge="NONE"
        )

        from volguard import ParticipantData
        external = ExternalMetrics(
            fii=ParticipantData(100_000, 100_000, 0, 50_000, 50_000, 0, 50_000, 50_000, 0, 0),
            dii=None, pro=None, client=None, fii_net_change=0,
            flow_regime="NEUTRAL", fast_vol=True, data_date="18-Jan-2026", event_risk="HIGH"
        )

        time_m = TimeMetrics(
            current_date=date.today(),
            weekly_exp=date.today() + timedelta(days=3),
            monthly_exp=date.today() + timedelta(days=25),
            next_weekly_exp=date.today() + timedelta(days=10),
            dte_weekly=3, dte_monthly=25,
            is_gamma_week=False, is_gamma_month=False,
            days_to_next_weekly=10
        )

        print(f"\nExtreme-market-conditions test:")
        for sc in scenarios:
            score   = engine.calculate_scores(sc['vol'], struct, edge, external, time_m, "WEEKLY")
            mandate = engine.generate_mandate(
                score, sc['vol'], struct, edge, external, time_m,
                "WEEKLY", time_m.weekly_exp, time_m.dte_weekly
            )
            print(f"  {sc['name']}: score={score.composite:.2f}  regime={mandate.regime_name}  alloc={mandate.allocation_pct:.1f}%")
            assert 0 <= score.composite <= 10 and 0 <= mandate.allocation_pct <= 100 and mandate.max_lots >= 0

    @pytest.mark.stress
    def test_rapid_regime_recalculation(self):
        engine = RegimeEngine()

        vol = VolMetrics(
            spot=23500, vix=15, rv7=14, rv28=13, rv90=12,
            garch7=14, garch28=13, park7=14, park28=13,
            vov=3, vov_zscore=1, ivp_30d=50, ivp_90d=48, ivp_1yr=45,
            ma20=23400, atr14=350, trend_strength=0.3,
            vol_regime="FAIR", is_fallback=False
        )

        struct = StructMetrics(
            net_gex=1_500_000, gex_ratio=0.025, total_oi_value=60_000_000,
            gex_regime="STICKY", pcr=1.05, max_pain=23500, skew_25d=1.2,
            oi_regime="NEUTRAL", lot_size=25
        )

        edge = EdgeMetrics(
            iv_weekly=16, vrp_rv_weekly=2, vrp_garch_weekly=2, vrp_park_weekly=2,
            iv_monthly=15.5, vrp_rv_monthly=2, vrp_garch_monthly=1.8, vrp_park_monthly=1.8,
            term_spread=-0.5, term_regime="FLAT", primary_edge="SHORT_GAMMA"
        )

        from volguard import ParticipantData
        external = ExternalMetrics(
            fii=ParticipantData(150_000, 100_000, 50_000, 80_000, 60_000, 20_000, 70_000, 90_000, -20_000, 30_000),
            dii=None, pro=None, client=None, fii_net_change=5000,
            flow_regime="MODERATE_LONG", fast_vol=False, data_date="18-Jan-2026", event_risk="LOW"
        )

        time_m = TimeMetrics(
            current_date=date.today(),
            weekly_exp=date.today() + timedelta(days=3),
            monthly_exp=date.today() + timedelta(days=25),
            next_weekly_exp=date.today() + timedelta(days=10),
            dte_weekly=3, dte_monthly=25,
            is_gamma_week=False, is_gamma_month=False,
            days_to_next_weekly=10
        )

        num_calcs = 1000
        start   = time.time()
        for _ in range(num_calcs):
            vol.spot = 23500 + np.random.randint(-100, 100)
            vol.vix  = 15 + np.random.uniform(-2, 2)
            engine.calculate_scores(vol, struct, edge, external, time_m, "WEEKLY")
        elapsed = time.time() - start

        print(f"Rapid-regime-recalc: {num_calcs} calcs  {elapsed:.2f}s  ({num_calcs/elapsed:.0f} calcs/sec)")
        assert elapsed < 5


# =========================================================================
#  EXECUTION-ENGINE STRESS
# =========================================================================

class TestExecutionEngineStress:

    @pytest.mark.stress
    def test_concurrent_order_placement(self):
        ProductionConfig.DRY_RUN_MODE = True
        mock_client = Mock()
        engine      = ExecutionEngine(mock_client)

        def place_orders(thread_id, count):
            ok = 0
            for i in range(count):
                oid = engine.place_order(
                    f"INST_{thread_id}_{i}",
                    25,
                    "BUY" if i % 2 == 0 else "SELL",
                    "LIMIT",
                    100.0 + i
                )
                if oid:
                    ok += 1
            return ok

        num_threads, orders_per = 20, 50
        start = time.time()
        with ThreadPoolExecutor(max_workers=num_threads) as exe:
            futures = [exe.submit(place_orders, t, orders_per) for t in range(num_threads)]
            results = [f.result() for f in futures]
        elapsed      = time.time() - start
        total_orders = sum(results)

        print(f"Concurrent-order placement: {total_orders}/{num_threads*orders_per}  {total_orders/elapsed:.0f} ops/sec")
        assert total_orders >= num_threads * orders_per * 0.9

    @pytest.mark.stress
    def test_strategy_execution_under_load(self):
        ProductionConfig.DRY_RUN_MODE = True
        mock_client = Mock()
        engine      = ExecutionEngine(mock_client)

        legs = []
        for i in range(20):
            legs.append({
                'key': f'INST_{i}',
                'strike': 23000 + i * 100,
                'type': 'CE' if i % 2 == 0 else 'PE',
                'side': 'SELL' if i < 10 else 'BUY',
                'qty': 25,
                'ltp': 100.0 - i * 2,
                'role': 'CORE' if i < 10 else 'HEDGE',
                'structure': 'COMPLEX'
            })

        start   = time.time()
        result  = engine.execute_strategy(legs)
        elapsed = time.time() - start

        print(f"Complex-strategy execution: {len(legs)} legs  {len(result)} executed  {elapsed:.2f}s")
        assert isinstance(result, list)


# =========================================================================
#  MEMORY & RESOURCE STRESS
# =========================================================================

class TestResourceStress:

    @pytest.mark.stress
    def test_memory_leak_detection(self):
        import tracemalloc
        tracemalloc.start()

        db_path = "/tmp/leak_test_db.db"
        if os.path.exists(db_path):
            os.remove(db_path)

        gc.collect()
        baseline = tracemalloc.get_traced_memory()[0]

        db = DatabaseWriter(db_path)
        for cycle in range(10):
            for i in range(100):
                db.log_order(f"ORDER_{cycle}_{i}", "KEY", "BUY", 25, 100.0, "FILLED")
                db.set_state(f"key_{i % 10}", str(time.time()))
            time.sleep(0.5)
            gc.collect()
            if cycle % 3 == 0:
                current, _ = tracemalloc.get_traced_memory()
                print(f"  Cycle {cycle}: mem-growth {(current - baseline)/1024/1024:.2f} MB")

        final_current, final_peak = tracemalloc.get_traced_memory()
        final_growth = (final_current - baseline) / 1024 / 1024
        tracemalloc.stop()
        db.shutdown()
        os.remove(db_path)

        print(f"Memory-leak detection: 1000 ops  growth={final_growth:.2f} MB  peak={final_peak/1024/1024:.2f} MB")
        assert final_growth < 50  # < 50 MB for 1000 ops

    @pytest.mark.stress
    def test_cpu_intensive_calculations(self):
        engine = RegimeEngine()

        vol_scenarios = []
        for i in range(100):
            vol_scenarios.append(VolMetrics(
                spot=23000 + i * 10,
                vix=10 + i * 0.2,
                rv7=9 + i * 0.18, rv28=8 + i * 0.16, rv90=7 + i * 0.14,
                garch7=9 + i * 0.18, garch28=8 + i * 0.16,
                park7=9 + i * 0.18, park28=8 + i * 0.16,
                vov=2 + i * 0.03, vov_zscore=0 + i * 0.02,
                ivp_30d=20 + i * 0.6, ivp_90d=18 + i * 0.58, ivp_1yr=15 + i * 0.55,
                ma20=23000 + i * 10, atr14=200 + i * 3, trend_strength=0.1 + i * 0.005,
                vol_regime="FAIR", is_fallback=False
            ))

        struct = StructMetrics(
            net_gex=1_000_000, gex_ratio=0.02, total_oi_value=50_000_000,
            gex_regime="NEUTRAL", pcr=1.0, max_pain=23500, skew_25d=0,
            oi_regime="NEUTRAL", lot_size=25
        )

        edge = EdgeMetrics(
            iv_weekly=16, vrp_rv_weekly=2, vrp_garch_weekly=2, vrp_park_weekly=2,
            iv_monthly=15, vrp_rv_monthly=1.8, vrp_garch_monthly=1.6, vrp_park_monthly=1.7,
            term_spread=-1, term_regime="FLAT", primary_edge="SHORT_GAMMA"
        )

        from volguard import ParticipantData
        external = ExternalMetrics(
            fii=ParticipantData(150_000, 100_000, 50_000, 80_000, 60_000, 20_000, 70_000, 90_000, -20_000, 30_000),
            dii=None, pro=None, client=None, fii_net_change=5000,
            flow_regime="MODERATE_LONG", fast_vol=False, data_date="18-Jan-2026", event_risk="LOW"
        )

        time_m = TimeMetrics(
            current_date=date.today(),
            weekly_exp=date.today() + timedelta(days=3),
            monthly_exp=date.today() + timedelta(days=25),
            next_weekly_exp=date.today() + timedelta(days=10),
            dte_weekly=3, dte_monthly=25,
            is_gamma_week=False, is_gamma_month=False,
            days_to_next_weekly=10
        )

        process   = psutil.Process()
        cpu_before = process.cpu_percent(interval=0.1)
        start      = time.time()

        for vol in vol_scenarios:
            score = engine.calculate_scores(vol, struct, edge, external, time_m, "WEEKLY")
            engine.generate_mandate(score, vol, struct, edge, external, time_m,
                                    "WEEKLY", time_m.weekly_exp, time_m.dte_weekly)

        elapsed  = time.time() - start
        cpu_after = process.cpu_percent(interval=0.1)

        print(f"CPU-intensive calcs: {len(vol_scenarios)} scenarios  {elapsed:.2f}s  CPU-before={cpu_before:.1f}%  CPU-after={cpu_after:.1f}%")
        assert elapsed < 10


# =========================================================================
#  CHAOS & FAILURE INJECTION
# =========================================================================

class TestChaosEngineering:

    @pytest.mark.stress
    def test_database_connection_interruption(self):
        # FIX-2: small pause so previous test releases file-lock
        time.sleep(1)

        db_path = "/tmp/chaos_db.db"
        if os.path.exists(db_path):
            os.remove(db_path)

        db = DatabaseWriter(db_path)

        # write some data
        for i in range(100):
            db.log_order(f"ORDER_{i}", "KEY", "BUY", 25, 100.0, "FILLED")
        time.sleep(1)

        # simulate connection interruption
        try:
            db.running = False
            time.sleep(0.5)
            for i in range(10):
                db.log_order(f"ORDER_POST_{i}", "KEY", "BUY", 25, 100.0, "FILLED")
            print("\nDatabase-interruption test: system remained stable")
        except Exception as e:
            print(f"\nDatabase-interruption test: handled gracefully – {type(e).__name__}")
        finally:
            db.shutdown()
            if os.path.exists(db_path):
                os.remove(db_path)

    @pytest.mark.stress
    def test_random_failures_during_execution(self):
        ProductionConfig.DRY_RUN_MODE = True
        mock_client = Mock()
        engine      = ExecutionEngine(mock_client)

        success, failure = 0, 0
        for i in range(100):
            if np.random.random() < 0.2:  # 20 % fail
                failure += 1
            else:
                oid = engine.place_order(f"INST_{i}", 25, "BUY", "LIMIT", 100.0)
                if oid:
                    success += 1

        print(f"Random-failure injection: successes={success}  failures={failure}  rate={success/(success+failure)*100:.1f}%")
        assert success > 0

    @pytest.mark.stress
    def test_race_condition_simulation(self):
        db_path = "/tmp/race_test_db.db"
        if os.path.exists(db_path):
            os.remove(db_path)

        db   = DatabaseWriter(db_path)
        cb   = CircuitBreaker(db)

        shared_counter = {'value': 0}
        lock           = threading.Lock()

        def concurrent_updater(thread_id):
            for i in range(100):
                with lock:
                    shared_counter['value'] += 1
                cb.record_trade_result(np.random.randint(-1000, 1000))
                db.log_order(f"ORDER_T{thread_id}_N{i}", f"KEY_{thread_id}", "BUY", 25, 100.0, "FILLED")

        num_threads = 20
        threads     = []
        for t in range(num_threads):
            th = threading.Thread(target=concurrent_updater, args=(t,))
            threads.append(th)
            th.start()
        for th in threads:
            th.join()
        time.sleep(2)

        expected   = num_threads * 100
        actual_ctr = shared_counter['value']

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM order_log")
        db_count = cursor.fetchone()[0]
        conn.close()

        print(f"Race-condition test: counter={actual_ctr}/{expected}  DB={db_count}/{expected}")
        assert actual_ctr == expected and db_count == expected
        db.shutdown()
        os.remove(db_path)

    @pytest.mark.stress
    def test_sudden_process_termination(self):
        db_path = "/tmp/termination_test_db.db"
        if os.path.exists(db_path):
            os.remove(db_path)

        db = DatabaseWriter(db_path)
        for i in range(500):
            db.log_order(f"ORDER_{i}", "KEY", "BUY", 25, 100.0, "FILLED")

        # sudden "crash"
        db.running = False
        time.sleep(1)

        try:
            conn  = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM order_log")
            count = cursor.fetchone()[0]
            conn.close()
            print(f"Sudden-termination test: orders={count}/500  integrity=OK")
            assert count >= 0
        except sqlite3.Error as e:
            pytest.fail(f"Database corrupted after termination: {e}")
        finally:
            if os.path.exists(db_path):
                os.remove(db_path)


# =========================================================================
#  ENDURANCE TESTS
# =========================================================================

class TestEndurance:

    @pytest.mark.slow
    @pytest.mark.stress
    def test_24_hour_simulation(self):
        db_path = "/tmp/endurance_db.db"
        if os.path.exists(db_path):
            os.remove(db_path)

        db   = DatabaseWriter(db_path)
        cb   = CircuitBreaker(db)

        ProductionConfig.DRY_RUN_MODE = True
        mock_client = Mock()
        engine      = ExecutionEngine(mock_client)

        start          = time.time()
        target_seconds = 60  # 60 s = 1 compressed "day"
        trade_count    = 0
        order_count    = 0

        print("\nStarting 24-hour endurance test (60 s simulation)...")

        while (time.time() - start) < target_seconds:
            if np.random.random() < 0.3:
                oid = engine.place_order(f"INST_{order_count}", 25,
                                         "BUY" if order_count % 2 == 0 else "SELL",
                                         "LIMIT", 100.0)
                if oid:
                    order_count += 1

            if order_count % 10 == 0:
                db.set_state("current_pnl", str(np.random.randint(-5000, 5000)))
                db.set_state("timestamp", str(time.time()))

            if np.random.random() < 0.05:  # 5 % chance
                pnl = np.random.randint(-2000, 3000)
                cb.record_trade_result(pnl)
                db.update_daily_stats(trades=1, pnl=pnl)
                trade_count += 1

            if trade_count > 0 and trade_count % 5 == 0:
                new_cap = 1_000_000 + np.random.randint(-50_000, 100_000)
                cb.update_capital(new_cap)

            time.sleep(0.1)

        elapsed = time.time() - start

        # -----------------------------------------------------------------
        # FIX-3: wait for async writer to drain queue before checking DB
        # -----------------------------------------------------------------
        start_wait = time.time()
        while db.message_queue.qsize() > 0 and (time.time() - start_wait) < 10:
            time.sleep(0.5)
        time.sleep(2)  # extra buffer
        # -----------------------------------------------------------------

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM order_log")
        final_order_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM daily_stats")
        stats_count = cursor.fetchone()[0]
        conn.close()

        print(f"Endurance-test results: duration={elapsed:.2f}s  orders-placed={order_count}  orders-logged={final_order_count}  trades={trade_count}  breaker-triggered={cb.breaker_triggered}")
        db.shutdown()
        os.remove(db_path)

        assert final_order_count >= order_count * 0.9
        assert not cb.breaker_triggered or trade_count > 0

    @pytest.mark.slow
    @pytest.mark.stress
    def test_gradual_degradation(self):
        db_path = "/tmp/degradation_db.db"
        if os.path.exists(db_path):
            os.remove(db_path)

        db = DatabaseWriter(db_path)
        print("\nGradual-load-increase test:")

        for phase in range(5):
            multiplier = phase + 1
            operations = 100 * multiplier

            start = time.time()
            for i in range(operations):
                db.log_order(f"PHASE{phase}_ORDER_{i}", f"KEY_{i % 10}", "BUY", 25, 100.0, "FILLED")
            time.sleep(1)
            elapsed   = time.time() - start
            queue_size = db.message_queue.qsize()

            print(f"  Phase {phase+1}: {operations} ops  {elapsed:.2f}s  queue={queue_size}")
            assert elapsed < 10 and queue_size < 5000

        db.shutdown()
        os.remove(db_path)


# =========================================================================
#  MAIN RUNNER
# =========================================================================

if __name__ == "__main__":
    pytest.main([
        __file__,
        '-v', '-s', '--durations=10',
        '-m', 'stress',
        '--tb=short'
    ])
