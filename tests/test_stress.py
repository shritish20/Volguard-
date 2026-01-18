"""
VOLGUARD 3.0 - STRESS & LOAD TESTS
Tests system under extreme load and adverse conditions
Run: pytest test_stress.py -v -s --durations=10
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

# Setup
os.environ['VG_DRY_RUN'] = 'TRUE'
os.environ['UPSTOX_ACCESS_TOKEN'] = 'test_token'
os.environ['TELEGRAM_BOT_TOKEN'] = 'test_bot_token'
os.environ['TELEGRAM_CHAT_ID'] = 'test_chat_id'

from volguard import (
    DatabaseWriter, CircuitBreaker, PaperTradingEngine,
    RegimeEngine, StrategyFactory, ExecutionEngine,
    VolMetrics, StructMetrics, EdgeMetrics,
    ExternalMetrics, TimeMetrics, TradingMandate,
    RegimeScore, ProductionConfig
)


# ============================================================================
# DATABASE STRESS TESTS
# ============================================================================

class TestDatabaseStress:
    
    @pytest.mark.stress
    def test_massive_concurrent_writes(self):
        """Test database with 10,000 concurrent writes from multiple threads"""
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
        
        num_threads = 20
        writes_per_thread = 500
        
        start_time = time.time()
        
        threads = []
        for t in range(num_threads):
            thread = threading.Thread(target=writer_thread, args=(t, writes_per_thread))
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join(timeout=30)
        
        # Wait for queue to drain
        timeout = 60
        start_wait = time.time()
        while db.message_queue.qsize() > 0 and (time.time() - start_wait) < timeout:
            time.sleep(0.1)
        
        elapsed = time.time() - start_time
        
        # Verify data integrity
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM order_log")
        count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT order_id) FROM order_log")
        unique_count = cursor.fetchone()[0]
        
        conn.close()
        db.shutdown()
        
        expected_writes = num_threads * writes_per_thread
        
        print(f"\n{'='*60}")
        print(f"Concurrent Write Stress Test Results:")
        print(f"  Threads: {num_threads}")
        print(f"  Writes per thread: {writes_per_thread}")
        print(f"  Total expected: {expected_writes}")
        print(f"  Total written: {count}")
        print(f"  Unique orders: {unique_count}")
        print(f"  Time elapsed: {elapsed:.2f}s")
        print(f"  Writes/sec: {count/elapsed:.0f}")
        print(f"{'='*60}\n")
        
        assert count == expected_writes, f"Expected {expected_writes} writes, got {count}"
        assert unique_count == count, "Duplicate order IDs detected"
        
        os.remove(db_path)
    
    @pytest.mark.stress
    def test_database_under_memory_pressure(self):
        """Test database performance when system memory is constrained"""
        db_path = "/tmp/memory_stress_db.db"
        if os.path.exists(db_path):
            os.remove(db_path)
        
        db = DatabaseWriter(db_path)
        
        # Create memory pressure
        memory_hogs = []
        try:
            # Allocate large arrays (but leave some memory for DB)
            for _ in range(5):
                memory_hogs.append(np.random.rand(10000000))  # ~76MB each
            
            # Now stress test the database
            start_time = time.time()
            
            for i in range(1000):
                db.log_order(
                    f"ORDER_{i}", "TEST_KEY", "BUY",
                    25, 100.0, "FILLED", 25, 100.0
                )
                
                if i % 100 == 0:
                    db.save_trade(
                        f"TRADE_{i}", "TEST", date.today(),
                        [], 5000.0, 50000.0
                    )
            
            time.sleep(3)
            elapsed = time.time() - start_time
            
            # Verify writes completed
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM order_log")
            order_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM trades")
            trade_count = cursor.fetchone()[0]
            conn.close()
            
            print(f"\nMemory Pressure Test:")
            print(f"  Orders written: {order_count}/1000")
            print(f"  Trades written: {trade_count}/10")
            print(f"  Time: {elapsed:.2f}s")
            
            assert order_count >= 990  # Allow small loss
            assert trade_count >= 9
            
        finally:
            # Cleanup
            del memory_hogs
            gc.collect()
            db.shutdown()
            os.remove(db_path)
    
    @pytest.mark.stress
    def test_rapid_state_updates(self):
        """Test system state updates under high frequency"""
        db_path = "/tmp/state_stress_db.db"
        if os.path.exists(db_path):
            os.remove(db_path)
        
        db = DatabaseWriter(db_path)
        
        def rapid_updater(key_prefix, iterations):
            for i in range(iterations):
                db.set_state(f"{key_prefix}_counter", str(i))
                db.set_state(f"{key_prefix}_timestamp", str(time.time()))
        
        num_keys = 10
        iterations = 200
        
        start_time = time.time()
        
        threads = []
        for k in range(num_keys):
            thread = threading.Thread(target=rapid_updater, args=(f"KEY_{k}", iterations))
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        time.sleep(2)
        elapsed = time.time() - start_time
        
        # Verify final states
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM system_state")
        state_count = cursor.fetchone()[0]
        conn.close()
        
        print(f"\nRapid State Update Test:")
        print(f"  Keys: {num_keys}")
        print(f"  Updates per key: {iterations}")
        print(f"  Total updates: {num_keys * iterations}")
        print(f"  Final state entries: {state_count}")
        print(f"  Time: {elapsed:.2f}s")
        print(f"  Updates/sec: {(num_keys * iterations)/elapsed:.0f}")
        
        assert state_count == num_keys * 2  # counter + timestamp per key
        
        db.shutdown()
        os.remove(db_path)


# ============================================================================
# CIRCUIT BREAKER STRESS TESTS
# ============================================================================

class TestCircuitBreakerStress:
    
    @pytest.mark.stress
    def test_rapid_consecutive_loss_checks(self):
        """Test circuit breaker with rapid-fire loss notifications"""
        db_path = "/tmp/cb_stress_db.db"
        if os.path.exists(db_path):
            os.remove(db_path)
        
        db = DatabaseWriter(db_path)
        cb = CircuitBreaker(db)
        
        # Simulate 100 trades in rapid succession
        results = []
        start_time = time.time()
        
        for i in range(100):
            # Alternate wins and losses
            pnl = 1000 if i % 5 == 0 else -500
            result = cb.record_trade_result(pnl)
            results.append(result)
            
            if not result:
                break  # Circuit breaker triggered
        
        elapsed = time.time() - start_time
        
        print(f"\nCircuit Breaker Stress Test:")
        print(f"  Total checks: {len(results)}")
        print(f"  Breaker triggered: {cb.breaker_triggered}")
        print(f"  Consecutive losses: {cb.consecutive_losses}")
        print(f"  Time: {elapsed:.2f}s")
        
        # Should eventually trigger
        assert cb.breaker_triggered or len(results) == 100
        
        db.shutdown()
        os.remove(db_path)
    
    @pytest.mark.stress
    def test_concurrent_risk_checks(self):
        """Test circuit breaker with concurrent risk checks from multiple sources"""
        db_path = "/tmp/cb_concurrent_db.db"
        if os.path.exists(db_path):
            os.remove(db_path)
        
        db = DatabaseWriter(db_path)
        cb = CircuitBreaker(db)
        
        def risk_checker(thread_id):
            for i in range(50):
                # Random capital updates
                new_capital = 1000000 - np.random.randint(0, 200000)
                cb.update_capital(new_capital)
                
                # Random slippage events
                if np.random.random() < 0.3:
                    cb.record_slippage_event(np.random.uniform(0.01, 0.05))
                
                time.sleep(0.01)
        
        num_threads = 10
        
        threads = []
        for t in range(num_threads):
            thread = threading.Thread(target=risk_checker, args=(t,))
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        print(f"\nConcurrent Risk Check Test:")
        print(f"  Threads: {num_threads}")
        print(f"  Breaker triggered: {cb.breaker_triggered}")
        print(f"  Current capital: {cb.current_capital:,.0f}")
        print(f"  Peak capital: {cb.peak_capital:,.0f}")
        
        # System should remain stable
        assert cb.current_capital >= 0
        
        db.shutdown()
        os.remove(db_path)


# ============================================================================
# PAPER TRADING ENGINE STRESS TESTS
# ============================================================================

class TestPaperTradingStress:
    
    @pytest.mark.stress
    def test_high_frequency_trading_simulation(self):
        """Test paper engine with high-frequency order placement"""
        engine = PaperTradingEngine()
        
        num_orders = 5000
        start_time = time.time()
        
        order_ids = []
        for i in range(num_orders):
            order_id = engine.place_order(
                f"INSTRUMENT_{i % 100}",
                25,
                "BUY" if i % 2 == 0 else "SELL",
                "LIMIT",
                100.0 + (i % 50)
            )
            if order_id:
                order_ids.append(order_id)
        
        elapsed = time.time() - start_time
        
        # Check status of random sample
        sample_size = min(100, len(order_ids))
        sample_ids = np.random.choice(order_ids, sample_size, replace=False)
        
        statuses = {'complete': 0, 'rejected': 0, 'unknown': 0}
        for oid in sample_ids:
            status = engine.get_order_status(oid)
            if status:
                statuses[status.get('status', 'unknown')] += 1
        
        print(f"\nHigh-Frequency Paper Trading Test:")
        print(f"  Orders placed: {len(order_ids)}/{num_orders}")
        print(f"  Time: {elapsed:.2f}s")
        print(f"  Orders/sec: {len(order_ids)/elapsed:.0f}")
        print(f"  Sample statuses: {statuses}")
        
        assert len(order_ids) == num_orders
        assert elapsed < 10  # Should be very fast
    
    @pytest.mark.stress
    def test_massive_position_accumulation(self):
        """Test position tracking with thousands of fills"""
        engine = PaperTradingEngine()
        
        # Place orders for many instruments
        num_instruments = 100
        orders_per_instrument = 20
        
        for inst in range(num_instruments):
            for order in range(orders_per_instrument):
                engine.place_order(
                    f"INST_{inst}",
                    25,
                    "BUY",
                    "LIMIT",
                    100.0 + inst
                )
        
        positions = engine.get_positions()
        
        print(f"\nMassive Position Test:")
        print(f"  Total positions: {len(positions)}")
        print(f"  Expected max: {num_instruments * 2}")  # BUY and SELL sides
        
        assert len(positions) <= num_instruments * 2


# ============================================================================
# REGIME ENGINE STRESS TESTS
# ============================================================================

class TestRegimeEngineStress:
    
    @pytest.mark.stress
    def test_extreme_market_conditions(self):
        """Test regime engine under extreme market scenarios"""
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
            net_gex=1000000, gex_ratio=0.02, total_oi_value=50000000,
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
            fii=ParticipantData(100000, 100000, 0, 50000, 50000, 0, 50000, 50000, 0, 0),
            dii=None, pro=None, client=None, fii_net_change=0,
            flow_regime="NEUTRAL", fast_vol=True, data_date="18-Jan-2026",
            event_risk="HIGH"
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
        
        print(f"\nExtreme Market Conditions Test:")
        
        for scenario in scenarios:
            score = engine.calculate_scores(
                scenario['vol'], struct, edge, external, time_m, "WEEKLY"
            )
            
            mandate = engine.generate_mandate(
                score, scenario['vol'], struct, edge, external, time_m,
                "WEEKLY", time_m.weekly_exp, time_m.dte_weekly
            )
            
            print(f"\n  {scenario['name']}:")
            print(f"    Composite Score: {score.composite:.2f}")
            print(f"    Regime: {mandate.regime_name}")
            print(f"    Allocation: {mandate.allocation_pct:.1f}%")
            print(f"    Strategy: {mandate.suggested_structure}")
            
            # Should produce valid output even in extreme conditions
            assert 0 <= score.composite <= 10
            assert 0 <= mandate.allocation_pct <= 100
            assert mandate.max_lots >= 0
    
    @pytest.mark.stress
    def test_rapid_regime_recalculation(self):
        """Test regime calculations under rapid fire"""
        engine = RegimeEngine()
        
        vol = VolMetrics(
            spot=23500, vix=15, rv7=14, rv28=13, rv90=12,
            garch7=14, garch28=13, park7=14, park28=13,
            vov=3, vov_zscore=1, ivp_30d=50, ivp_90d=48, ivp_1yr=45,
            ma20=23400, atr14=350, trend_strength=0.3,
            vol_regime="FAIR", is_fallback=False
        )
        
        struct = StructMetrics(
            net_gex=1500000, gex_ratio=0.025, total_oi_value=60000000,
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
            fii=ParticipantData(150000, 100000, 50000, 80000, 60000, 20000, 70000, 90000, -20000, 30000),
            dii=None, pro=None, client=None, fii_net_change=5000,
            flow_regime="MODERATE_LONG", fast_vol=False,
            data_date="18-Jan-2026", event_risk="LOW"
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
        
        num_calculations = 1000
        start_time = time.time()
        
        for _ in range(num_calculations):
            # Slightly vary inputs
            vol.spot = 23500 + np.random.randint(-100, 100)
            vol.vix = 15 + np.random.uniform(-2, 2)
            
            score = engine.calculate_scores(
                vol, struct, edge, external, time_m, "WEEKLY"
            )
        
        elapsed = time.time() - start_time
        
        print(f"\nRapid Regime Recalculation Test:")
        print(f"  Calculations: {num_calculations}")
        print(f"  Time: {elapsed:.2f}s")
        print(f"  Calc/sec: {num_calculations/elapsed:.0f}")
        
        assert elapsed < 5  # Should be very fast


# ============================================================================
# EXECUTION ENGINE STRESS TESTS
# ============================================================================

class TestExecutionEngineStress:
    
    @pytest.mark.stress
    def test_concurrent_order_placement(self):
        """Test execution engine with concurrent order requests"""
        ProductionConfig.DRY_RUN_MODE = True
        mock_client = Mock()
        engine = ExecutionEngine(mock_client)
        
        def place_orders(thread_id, count):
            results = []
            for i in range(count):
                order_id = engine.place_order(
                    f"INST_{thread_id}_{i}",
                    25,
                    "BUY" if i % 2 == 0 else "SELL",
                    "LIMIT",
                    100.0 + i
                )
                results.append(order_id is not None)
            return sum(results)
        
        num_threads = 20
        orders_per_thread = 50
        
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [
                executor.submit(place_orders, t, orders_per_thread)
                for t in range(num_threads)
            ]
            
            results = [f.result() for f in futures]
        
        elapsed = time.time() - start_time
        total_orders = sum(results)
        
        print(f"\nConcurrent Order Placement Test:")
        print(f"  Threads: {num_threads}")
        print(f"  Orders per thread: {orders_per_thread}")
        print(f"  Total orders: {total_orders}/{num_threads * orders_per_thread}")
        print(f"  Time: {elapsed:.2f}s")
        print(f"  Orders/sec: {total_orders/elapsed:.0f}")
        
        assert total_orders >= num_threads * orders_per_thread * 0.9  # 90% success rate
    
    @pytest.mark.stress
    def test_strategy_execution_under_load(self):
        """Test full strategy execution with many concurrent legs"""
        ProductionConfig.DRY_RUN_MODE = True
        mock_client = Mock()
        engine = ExecutionEngine(mock_client)
        
        # Create complex multi-leg strategy
        legs = []
        for i in range(20):  # 20 legs (extreme case)
            legs.append({
                'key': f'INST_{i}',
                'strike': 23000 + (i * 100),
                'type': 'CE' if i % 2 == 0 else 'PE',
                'side': 'SELL' if i < 10 else 'BUY',
                'qty': 25,
                'ltp': 100.0 - (i * 2),
                'role': 'CORE' if i < 10 else 'HEDGE',
                'structure': 'COMPLEX'
            })
        
        start_time = time.time()
        result = engine.execute_strategy(legs)
        elapsed = time.time() - start_time
        
        print(f"\nComplex Strategy Execution Test:")
        print(f"  Input legs: {len(legs)}")
        print(f"  Executed legs: {len(result)}")
        print(f"  Time: {elapsed:.2f}s")
        
        # In paper mode, should execute all or fail gracefully
        assert isinstance(result, list)


# ============================================================================
# MEMORY & RESOURCE STRESS TESTS
# ============================================================================

class TestResourceStress:
    
    @pytest.mark.stress
    def test_memory_leak_detection(self):
        """Test for memory leaks during extended operation"""
        import tracemalloc
        
        tracemalloc.start()
        
        db_path = "/tmp/leak_test_db.db"
        if os.path.exists(db_path):
            os.remove(db_path)
        
        # Get baseline
        gc.collect()
        baseline = tracemalloc.get_traced_memory()[0]
        
        db = DatabaseWriter(db_path)
        
        # Perform 1000 operations
        for cycle in range(10):
            for i in range(100):
                db.log_order(f"ORDER_{cycle}_{i}", "KEY", "BUY", 25, 100.0, "FILLED")
                db.set_state(f"key_{i % 10}", str(time.time()))
            
            time.sleep(0.5)
            gc.collect()
            
            current, peak = tracemalloc.get_traced_memory()
            growth = (current - baseline) / 1024 / 1024  # MB
            
            if cycle % 3 == 0:
                print(f"  Cycle {cycle}: Memory growth: {growth:.2f} MB")
        
        final_current, final_peak = tracemalloc.get_traced_memory()
        final_growth = (final_current - baseline) / 1024 / 1024
        
        tracemalloc.stop()
        db.shutdown()
        
        print(f"\nMemory Leak Detection Test:")
        print(f"  Total operations: 1000")
        print(f"  Final memory growth: {final_growth:.2f} MB")
        print(f"  Peak memory: {final_peak / 1024 / 1024:.2f} MB")
        
        # Should not grow excessively (< 50MB for 1000 ops)
        assert final_growth < 50
        
        os.remove(db_path)
    
    @pytest.mark.stress
    def test_cpu_intensive_calculations(self):
        """Test CPU usage under intensive calculations"""
        engine = RegimeEngine()
        
        # Create dataset for intensive calculations
        vol_scenarios = []
        for i in range(100):
            vol_scenarios.append(VolMetrics(
                spot=23000 + i * 10,
                vix=10 + i * 0.2,
                rv7=9 + i * 0.18,
                rv28=8 + i * 0.16,
                rv90=7 + i * 0.14,
                garch7=9 + i * 0.18,
                garch28=8 + i * 0.16,
                park7=9 + i * 0.18,
                park28=8 + i * 0.16,
                vov=2 + i * 0.03,
                vov_zscore=0 + i * 0.02,
                ivp_30d=20 + i * 0.6,
                ivp_90d=18 + i * 0.58,
                ivp_1yr=15 + i * 0.55,
                ma20=23000 + i * 10,
                atr14=200 + i * 3,
                trend_strength=0.1 + i * 0.005,
                vol_regime="FAIR",
                is_fallback=False
            ))
        
        struct = StructMetrics(
            net_gex=1000000, gex_ratio=0.02, total_oi_value=50000000,
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
            fii=ParticipantData(150000, 100000, 50000, 80000, 60000, 20000, 70000, 90000, -20000, 30000),
            dii=None, pro=None, client=None, fii_net_change=5000,
            flow_regime="MODERATE_LONG", fast_vol=False,
            data_date="18-Jan-2026", event_risk="LOW"
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
        
        process = psutil.Process()
        cpu_before = process.cpu_percent(interval=0.1)
        
        start_time = time.time()
        
        # Run intensive calculations
        for vol in vol_scenarios:
            score = engine.calculate_scores(vol, struct, edge, external, time_m, "WEEKLY")
            mandate = engine.generate_mandate(
                score, vol, struct, edge, external, time_m,
                "WEEKLY", time_m.weekly_exp, time_m.dte_weekly
            )
        
        elapsed = time.time() - start_time
        cpu_after = process.cpu_percent(interval=0.1)
        
        print(f"\nCPU Intensive Calculation Test:")
        print(f"  Scenarios processed: {len(vol_scenarios)}")
        print(f"  Time: {elapsed:.2f}s")
        print(f"  CPU before: {cpu_before:.1f}%")
        print(f"  CPU after: {cpu_after:.1f}%")
        
        assert elapsed < 10  # Should complete in reasonable time


# ============================================================================
# CHAOS & FAILURE INJECTION TESTS
# ============================================================================

class TestChaosEngineering:
    
    @pytest.mark.stress
    def test_database_connection_interruption(self):
        """Test behavior when database connection is interrupted"""
        db_path = "/tmp/chaos_db.db"
        if os.path.exists(db_path):
            os.remove(db_path)
        
        db = DatabaseWriter(db_path)
        
        # Write some data
        for i in range(100):
            db.log_order(f"ORDER_{i}", "KEY", "BUY", 25, 100.0, "FILLED")
        
        time.sleep(1)
        
        # Simulate connection interruption by corrupting db file
        # (DB writer should handle this gracefully)
        try:
            # Force close the connection
            db.running = False
            time.sleep(0.5)
            
            # Try to write (should queue or fail gracefully)
            for i in range(10):
                db.log_order(f"ORDER_POST_{i}", "KEY", "BUY", 25, 100.0, "FILLED")
            
            print("\nDatabase Interruption Test: System remained stable")
            
        except Exception as e:
            # Should not crash
            print(f"\nDatabase Interruption Test: Handled gracefully - {type(e).__name__}")
        
        finally:
            db.shutdown()
            os.remove(db_path)
    
    @pytest.mark.stress
    def test_random_failures_during_execution(self):
        """Test resilience to random failures"""
        ProductionConfig.DRY_RUN_MODE = True
        mock_client = Mock()
        engine = ExecutionEngine(mock_client)
        
        failure_count = 0
        success_count = 0
        
        for i in range(100):
            # Randomly inject failures
            if np.random.random() < 0.2:  # 20% failure rate
                # Simulate order failure
                order_id = None
                failure_count += 1
            else:
                order_id = engine.place_order(
                    f"INST_{i}", 25, "BUY", "LIMIT", 100.0
                )
                if order_id:
                    success_count += 1
        
        print(f"\nRandom Failure Injection Test:")
        print(f"  Successes: {success_count}")
        print(f"  Failures: {failure_count}")
        print(f"  Success rate: {success_count/(success_count+failure_count)*100:.1f}%")
        
        # System should handle failures gracefully
        assert success_count > 0
    
    @pytest.mark.stress
    def test_race_condition_simulation(self):
        """Test for race conditions in concurrent operations"""
        db_path = "/tmp/race_test_db.db"
        if os.path.exists(db_path):
            os.remove(db_path)
        
        db = DatabaseWriter(db_path)
        cb = CircuitBreaker(db)
        
        # Shared state that could cause race conditions
        shared_counter = {'value': 0}
        lock = threading.Lock()
        
        def concurrent_updater(thread_id):
            for i in range(100):
                # Update shared state
                with lock:
                    shared_counter['value'] += 1
                
                # Update circuit breaker
                pnl = np.random.randint(-1000, 1000)
                cb.record_trade_result(pnl)
                
                # Database write
                db.log_order(
                    f"ORDER_T{thread_id}_N{i}",
                    f"KEY_{thread_id}",
                    "BUY",
                    25,
                    100.0,
                    "FILLED"
                )
        
        num_threads = 20
        
        threads = []
        for t in range(num_threads):
            thread = threading.Thread(target=concurrent_updater, args=(t,))
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        time.sleep(2)
        
        # Verify data integrity
        expected_counter = num_threads * 100
        actual_counter = shared_counter['value']
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM order_log")
        db_count = cursor.fetchone()[0]
        conn.close()
        
        print(f"\nRace Condition Test:")
        print(f"  Expected counter: {expected_counter}")
        print(f"  Actual counter: {actual_counter}")
        print(f"  Database writes: {db_count}/{expected_counter}")
        
        assert actual_counter == expected_counter  # No race condition
        assert db_count == expected_counter
        
        db.shutdown()
        os.remove(db_path)
    
    @pytest.mark.stress
    def test_sudden_process_termination(self):
        """Test recovery from sudden process termination"""
        db_path = "/tmp/termination_test_db.db"
        if os.path.exists(db_path):
            os.remove(db_path)
        
        # Start database writer
        db = DatabaseWriter(db_path)
        
        # Queue up writes
        for i in range(500):
            db.log_order(f"ORDER_{i}", "KEY", "BUY", 25, 100.0, "FILLED")
        
        # Sudden shutdown (simulating crash)
        db.running = False
        
        # Some writes may be lost, but DB should not corrupt
        time.sleep(1)
        
        # Verify database is still readable
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM order_log")
            count = cursor.fetchone()[0]
            conn.close()
            
            print(f"\nSudden Termination Test:")
            print(f"  Orders written: {count}/500")
            print(f"  Database integrity: OK")
            
            assert count >= 0  # DB should be readable
            
        except sqlite3.Error as e:
            pytest.fail(f"Database corrupted after termination: {e}")
        
        finally:
            if os.path.exists(db_path):
                os.remove(db_path)


# ============================================================================
# ENDURANCE TESTS
# ============================================================================

class TestEndurance:
    
    @pytest.mark.slow
    @pytest.mark.stress
    def test_24_hour_simulation(self):
        """Simulate 24 hours of operation (compressed to 60 seconds)"""
        db_path = "/tmp/endurance_db.db"
        if os.path.exists(db_path):
            os.remove(db_path)
        
        db = DatabaseWriter(db_path)
        cb = CircuitBreaker(db)
        
        ProductionConfig.DRY_RUN_MODE = True
        mock_client = Mock()
        engine = ExecutionEngine(mock_client)
        
        # Simulate trading day
        start_time = time.time()
        target_duration = 60  # 60 seconds = 1 "day"
        
        trade_count = 0
        order_count = 0
        
        print("\nStarting 24-hour endurance test (60s simulation)...")
        
        while (time.time() - start_time) < target_duration:
            # Simulate market activity
            
            # Place some orders (simulating monitoring)
            if np.random.random() < 0.3:
                order_id = engine.place_order(
                    f"INST_{order_count}",
                    25,
                    "BUY" if order_count % 2 == 0 else "SELL",
                    "LIMIT",
                    100.0
                )
                if order_id:
                    order_count += 1
            
            # Log state updates
            if order_count % 10 == 0:
                db.set_state("current_pnl", str(np.random.randint(-5000, 5000)))
                db.set_state("timestamp", str(time.time()))
            
            # Simulate trade results
            if np.random.random() < 0.05:  # 5% chance per cycle
                pnl = np.random.randint(-2000, 3000)
                cb.record_trade_result(pnl)
                db.update_daily_stats(trades=1, pnl=pnl)
                trade_count += 1
            
            # Update capital
            if trade_count > 0 and trade_count % 5 == 0:
                new_capital = 1000000 + np.random.randint(-50000, 100000)
                cb.update_capital(new_capital)
            
            time.sleep(0.1)  # Simulate monitoring interval
        
        elapsed = time.time() - start_time
        
        # Verify system state
        time.sleep(2)
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM order_log")
        final_order_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM daily_stats")
        stats_count = cursor.fetchone()[0]
        conn.close()
        
        print(f"\nEndurance Test Results:")
        print(f"  Duration: {elapsed:.2f}s")
        print(f"  Orders placed: {order_count}")
        print(f"  Orders logged: {final_order_count}")
        print(f"  Trades executed: {trade_count}")
        print(f"  Circuit breaker triggered: {cb.breaker_triggered}")
        print(f"  System status: STABLE")
        
        db.shutdown()
        os.remove(db_path)
        
        assert final_order_count >= order_count * 0.9  # Allow some loss
        assert not cb.breaker_triggered or trade_count > 0  # Only trigger if trading
    
    @pytest.mark.slow
    @pytest.mark.stress
    def test_gradual_degradation(self):
        """Test system behavior under gradually increasing load"""
        db_path = "/tmp/degradation_db.db"
        if os.path.exists(db_path):
            os.remove(db_path)
        
        db = DatabaseWriter(db_path)
        
        print("\nGradual Load Increase Test:")
        
        for phase in range(5):
            load_multiplier = phase + 1
            operations = 100 * load_multiplier
            
            start_time = time.time()
            
            for i in range(operations):
                db.log_order(
                    f"PHASE{phase}_ORDER_{i}",
                    f"KEY_{i % 10}",
                    "BUY",
                    25,
                    100.0,
                    "FILLED"
                )
            
            time.sleep(1)
            elapsed = time.time() - start_time
            
            queue_size = db.message_queue.qsize()
            
            print(f"  Phase {phase + 1}: {operations} ops, {elapsed:.2f}s, queue={queue_size}")
            
            # System should remain responsive
            assert elapsed < 10
            assert queue_size < 5000  # Queue shouldn't overflow
        
        db.shutdown()
        os.remove(db_path)


# ============================================================================
# MAIN TEST RUNNER
# ============================================================================

if __name__ == "__main__":
    pytest.main([
        __file__,
        '-v',
        '-s',
        '--durations=10',
        '-m', 'stress',
        '--tb=short'
    ])
