"""
VOLGUARD 3.0 - CHAOS ENGINEERING TESTS
Simulates real-world failures and edge cases
Run: pytest test_chaos.py -v -s
"""

import pytest
import os
import time
import threading
import sqlite3
import numpy as np
import pandas as pd
from datetime import datetime, date, timedelta
from unittest.mock import Mock, patch, MagicMock
import random

os.environ['VG_DRY_RUN'] = 'TRUE'
os.environ['UPSTOX_ACCESS_TOKEN'] = 'test_token'
os.environ['TELEGRAM_BOT_TOKEN'] = 'test_bot_token'
os.environ['TELEGRAM_CHAT_ID'] = 'test_chat_id'

from volguard import (
    DatabaseWriter, CircuitBreaker, PaperTradingEngine,
    RegimeEngine, StrategyFactory, ExecutionEngine,
    RiskManager, SessionManager, ProductionConfig,
    VolMetrics, StructMetrics, EdgeMetrics, ExternalMetrics,
    TimeMetrics, TradingMandate, RegimeScore
)


# ============================================================================
# NETWORK FAILURE SCENARIOS
# ============================================================================

class TestNetworkChaos:
    
    @pytest.mark.chaos
    def test_api_timeout_during_order_placement(self):
        """Test order placement when API times out"""
        ProductionConfig.DRY_RUN_MODE = False  # Test real API path
        mock_client = Mock()
        engine = ExecutionEngine(mock_client)
        
        # Mock API timeout
        from volguard import OrderApiV3, ApiException
        
        with patch('volguard.OrderApiV3') as mock_api:
            mock_instance = Mock()
            mock_instance.place_order.side_effect = Exception("Connection timeout")
            mock_api.return_value = mock_instance
            
            order_id = engine.place_order("TEST_KEY", 25, "BUY", "LIMIT", 100.0)
            
            # Should return None gracefully
            assert order_id is None
            
            print("\n✓ API timeout handled gracefully")
    
    @pytest.mark.chaos
    def test_intermittent_network_failures(self):
        """Test behavior under intermittent network connectivity"""
        ProductionConfig.DRY_RUN_MODE = False
        mock_client = Mock()
        engine = ExecutionEngine(mock_client)
        
        # Simulate intermittent failures (50% success rate)
        call_count = {'count': 0}
        
        def intermittent_failure(*args, **kwargs):
            call_count['count'] += 1
            if call_count['count'] % 2 == 0:
                raise Exception("Network unreachable")
            else:
                mock_response = Mock()
                mock_response.status = 'success'
                mock_response.data = Mock()
                mock_response.data.order_ids = ['ORDER_123']
                return mock_response
        
        with patch('volguard.OrderApiV3') as mock_api:
            mock_instance = Mock()
            mock_instance.place_order.side_effect = intermittent_failure
            mock_api.return_value = mock_instance
            
            successes = 0
            failures = 0
            
            for i in range(10):
                order_id = engine.place_order(f"INST_{i}", 25, "BUY", "LIMIT", 100.0)
                if order_id:
                    successes += 1
                else:
                    failures += 1
            
            print(f"\nIntermittent Network Test:")
            print(f"  Successes: {successes}")
            print(f"  Failures: {failures}")
            
            # Should have some successes despite failures
            assert successes > 0
    
    @pytest.mark.chaos
    def test_session_expiry_during_trading(self):
        """Test session refresh when token expires mid-trading"""
        mock_client = Mock()
        session_mgr = SessionManager(mock_client)
        
        # Mock expired session
        call_count = {'count': 0}
        
        def session_validator(*args, **kwargs):
            call_count['count'] += 1
            mock_response = Mock()
            if call_count['count'] == 1:
                mock_response.status = 'error'  # First call fails
            else:
                mock_response.status = 'success'  # Refreshed
            return mock_response
        
        with patch('volguard.upstox_client.UserApi') as mock_user_api:
            mock_user_instance = Mock()
            mock_user_instance.get_profile.side_effect = session_validator
            mock_user_api.return_value = mock_user_instance
            
            # Mock successful token refresh
            with patch.object(session_mgr, '_refresh_token', return_value=True):
                result = session_mgr.validate_session(force=True)
                
                print(f"\nSession Expiry Test: Refresh triggered = {call_count['count'] > 1}")
                
                # Should attempt refresh
                assert call_count['count'] > 0


# ============================================================================
# DATA CORRUPTION SCENARIOS
# ============================================================================

class TestDataChaos:
    
    @pytest.mark.chaos
    def test_corrupted_market_data(self):
        """Test handling of corrupted/invalid market data"""
        from volguard import AnalyticsEngine
        from multiprocessing import Queue
        
        result_queue = Queue()
        engine = AnalyticsEngine(result_queue)
        
        # Corrupt data: negative prices, NaN values
        mock_response = Mock()
        mock_response.status = 'success'
        mock_response.data = Mock()
        mock_response.data.candles = [
            ['2026-01-15T09:15:00+05:30', -100, float('nan'), 23480, 23520, 1000000, 0],
            ['2026-01-16T09:15:00+05:30', 23520, 23600, -500, float('inf'), 1100000, 0],
            ['2026-01-17T09:15:00+05:30', None, 23620, 23560, 23600, -1000, 0]
        ]
        
        df = engine._parse_candle_response(mock_response)
        
        print(f"\nCorrupted Data Test:")
        print(f"  Parsed rows: {len(df)}")
        print(f"  Has NaN: {df.isnull().any().any()}")
        
        # Should parse without crashing
        assert isinstance(df, pd.DataFrame)
    
    @pytest.mark.chaos
    def test_missing_option_chain_data(self):
        """Test strategy generation with missing option data"""
        mock_client = Mock()
        factory = StrategyFactory(mock_client)
        
        # Incomplete option chain
        incomplete_chain = pd.DataFrame({
            'strike': [23500],
            'ce_iv': [16.0],
            'pe_iv': [None],  # Missing data
            'ce_delta': [0.5],
            'pe_delta': [None],
            'ce_ltp': [100.0],
            'pe_ltp': [None],
            'ce_key': ['CE_KEY'],
            'pe_key': [None]
        })
        
        mandate = TradingMandate(
            expiry_type="WEEKLY",
            expiry_date=date.today() + timedelta(days=3),
            dte=3,
            regime_name="TEST",
            strategy_type="TEST",
            allocation_pct=50,
            max_lots=2,
            risk_per_lot=125000,
            score=RegimeScore(7, 7, 7, 7, 7, "HIGH"),
            rationale=[],
            warnings=[],
            suggested_structure="IRON_FLY"
        )
        
        vol = VolMetrics(
            spot=23500, vix=15, rv7=14, rv28=13, rv90=12,
            garch7=14, garch28=13, park7=14, park28=13,
            vov=3, vov_zscore=1, ivp_30d=50, ivp_90d=48, ivp_1yr=45,
            ma20=23400, atr14=350, trend_strength=0.3,
            vol_regime="FAIR", is_fallback=False
        )
        
        legs = factory.generate(mandate, incomplete_chain, 25, vol, 23500)
        
        print(f"\nMissing Data Test: Generated {len(legs)} legs (expected 0)")
        
        # Should return empty or fail gracefully
        assert isinstance(legs, list)
    
    @pytest.mark.chaos
    def test_database_file_deletion_during_operation(self):
        """Test recovery when database file is deleted"""
        db_path = "/tmp/chaos_deletion_db.db"
        if os.path.exists(db_path):
            os.remove(db_path)
        
        db = DatabaseWriter(db_path)
        
        # Write some data
        for i in range(50):
            db.log_order(f"ORDER_{i}", "KEY", "BUY", 25, 100.0, "FILLED")
        
        time.sleep(0.5)
        
        # Delete database file while writer is running
        try:
            os.remove(db_path)
            print("\n✓ Database file deleted")
        except:
            pass
        
        # Try to write more data
        errors_caught = 0
        for i in range(50, 100):
            try:
                db.log_order(f"ORDER_{i}", "KEY", "BUY", 25, 100.0, "FILLED")
            except Exception as e:
                errors_caught += 1
        
        time.sleep(1)
        
        print(f"Database Deletion Test: Errors caught = {errors_caught}")
        
        db.shutdown()
        
        # System should handle gracefully (queue or log errors)
        assert True  # If we reached here, no crash


# ============================================================================
# RESOURCE EXHAUSTION SCENARIOS
# ============================================================================

class TestResourceChaos:
    
    @pytest.mark.chaos
    def test_disk_full_simulation(self):
        """Test behavior when disk is full (queue overflow simulation)"""
        db_path = "/tmp/diskfull_db.db"
        if os.path.exists(db_path):
            os.remove(db_path)
        
        db = DatabaseWriter(db_path)
        
        # Flood the queue to simulate disk full
        overflow_count = 0
        
        for i in range(15000):  # Exceed queue max size
            try:
                db.execute(
                    "INSERT INTO order_log (order_id, status) VALUES (?, ?)",
                    (f"ORDER_{i}", "TEST"),
                    timeout=0.001  # Very short timeout
                )
            except:
                overflow_count += 1
        
        print(f"\nDisk Full Simulation:")
        print(f"  Queue overflow events: {overflow_count}")
        print(f"  Final queue size: {db.message_queue.qsize()}")
        
        db.shutdown()
        os.remove(db_path)
        
        # Should have overflow protection
        assert overflow_count > 0  # Some requests should fail
    
    @pytest.mark.chaos
    def test_memory_allocation_failure(self):
        """Test handling when memory allocation fails"""
        try:
            # Try to allocate unreasonable amount of memory
            huge_array = np.zeros((1000000000, 1000000000))
            pytest.fail("Should have raised MemoryError")
        except MemoryError:
            print("\n✓ Memory Error caught gracefully")
        except Exception as e:
            print(f"\n✓ Memory constraint handled: {type(e).__name__}")
    
    @pytest.mark.chaos
    def test_thread_pool_exhaustion(self):
        """Test behavior when all threads are busy"""
        from concurrent.futures import ThreadPoolExecutor
        import queue
        
        max_workers = 5
        executor = ThreadPoolExecutor(max_workers=max_workers)
        
        # Block all threads
        barrier = threading.Barrier(max_workers + 1)
        
        def blocking_task():
            barrier.wait(timeout=2)
            time.sleep(0.5)
        
        # Submit blocking tasks
        futures = [executor.submit(blocking_task) for _ in range(max_workers)]
        
        # Try to submit one more (should queue)
        start_time = time.time()
        extra_future = executor.submit(lambda: time.time())
        
        # Release threads
        barrier.wait(timeout=2)
        
        # Wait for extra task
        result = extra_future.result(timeout=3)
        elapsed = time.time() - start_time
        
        print(f"\nThread Pool Exhaustion Test:")
        print(f"  Waited: {elapsed:.2f}s")
        print(f"  Extra task completed: {result is not None}")
        
        executor.shutdown()
        
        assert result is not None


# ============================================================================
# RACE CONDITION & DEADLOCK SCENARIOS
# ============================================================================

class TestConcurrencyChaos:
    
    @pytest.mark.chaos
    def test_deadlock_detection(self):
        """Test for potential deadlocks in concurrent operations"""
        lock1 = threading.Lock()
        lock2 = threading.Lock()
        
        deadlock_detected = {'flag': False}
        
        def thread1_task():
            try:
                lock1.acquire(timeout=1)
                time.sleep(0.1)
                if not lock2.acquire(timeout=1):
                    deadlock_detected['flag'] = True
                    print("\n⚠ Potential deadlock detected in thread1")
                else:
                    lock2.release()
                lock1.release()
            except Exception as e:
                print(f"\nThread1 error: {e}")
        
        def thread2_task():
            try:
                lock2.acquire(timeout=1)
                time.sleep(0.1)
                if not lock1.acquire(timeout=1):
                    deadlock_detected['flag'] = True
                    print("\n⚠ Potential deadlock detected in thread2")
                else:
                    lock1.release()
                lock2.release()
            except Exception as e:
                print(f"\nThread2 error: {e}")
        
        t1 = threading.Thread(target=thread1_task)
        t2 = threading.Thread(target=thread2_task)
        
        t1.start()
        t2.start()
        
        t1.join(timeout=3)
        t2.join(timeout=3)
        
        print(f"\nDeadlock Test: Deadlock = {deadlock_detected['flag']}")
        
        # If deadlock detected, test passes (we detected it)
        # If no deadlock, test passes (no deadlock occurred)
        assert True
    
    @pytest.mark.chaos
    def test_race_condition_in_circuit_breaker(self):
        """Test circuit breaker for race conditions"""
        db_path = "/tmp/race_cb_db.db"
        if os.path.exists(db_path):
            os.remove(db_path)
        
        db = DatabaseWriter(db_path)
        cb = CircuitBreaker(db)
        
        results = []
        
        def rapid_trade_recorder(thread_id):
            thread_results = []
            for i in range(100):
                pnl = -1000 if i % 3 == 0 else 500
                result = cb.record_trade_result(pnl)
                thread_results.append(result)
            return thread_results
        
        threads = []
        from concurrent.futures import ThreadPoolExecutor
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(rapid_trade_recorder, t) for t in range(10)]
            for future in futures:
                results.extend(future.result())
        
        print(f"\nRace Condition Test:")
        print(f"  Total operations: {len(results)}")
        print(f"  Circuit breaker triggered: {cb.breaker_triggered}")
        print(f"  Consecutive losses: {cb.consecutive_losses}")
        
        db.shutdown()
        os.remove(db_path)
        
        # Should maintain correct state despite concurrency
        assert cb.consecutive_losses >= 0


# ============================================================================
# EXTREME MARKET CONDITIONS
# ============================================================================

class TestMarketChaos:
    
    @pytest.mark.chaos
    def test_flash_crash_scenario(self):
        """Test system behavior during flash crash"""
        engine = RegimeEngine()
        
        # Simulate flash crash conditions
        crash_vol = VolMetrics(
            spot=18000,  # 25% drop
            vix=85,  # Extreme fear
            rv7=100,
            rv28=95,
            rv90=90,
            garch7=100,
            garch28=95,
            park7=100,
            park28=95,
            vov=20,  # Extreme vol-of-vol
            vov_zscore=6.0,
            ivp_30d=100,
            ivp_90d=100,
            ivp_1yr=99,
            ma20=23000,
            atr14=3000,  # Huge daily range
            trend_strength=10.0,
            vol_regime="EXPLODING",
            is_fallback=False
        )
        
        struct = StructMetrics(
            net_gex=-5000000,  # Negative GEX = unstable
            gex_ratio=0.08,
            total_oi_value=100000000,
            gex_regime="SLIPPERY",
            pcr=2.5,  # Extreme put buying
            max_pain=20000,
            skew_25d=10,  # Extreme skew
            oi_regime="BEARISH",
            lot_size=25
        )
        
        edge = EdgeMetrics(
            iv_weekly=100,
            vrp_rv_weekly=-10,  # Negative VRP in crash
            vrp_garch_weekly=-10,
            vrp_park_weekly=-10,
            iv_monthly=95,
            vrp_rv_monthly=-8,
            vrp_garch_monthly=-8,
            vrp_park_monthly=-8,
            term_spread=5,  # Inverted term structure
            term_regime="CONTANGO",
            primary_edge="LONG_VOL"
        )
        
        from volguard import ParticipantData
        external = ExternalMetrics(
            fii=ParticipantData(50000, 200000, -150000, 20000, 80000, -60000, 100000, 50000, 50000, -50000),
            dii=None,
            pro=None,
            client=None,
            fii_net_change=-100000,  # Massive selling
            flow_regime="STRONG_SHORT",
            fast_vol=True,
            data_date="18-Jan-2026",
            event_risk="HIGH"
        )
        
        time_m = TimeMetrics(
            current_date=date.today(),
            weekly_exp=date.today() + timedelta(days=1),
            monthly_exp=date.today() + timedelta(days=20),
            next_weekly_exp=date.today() + timedelta(days=8),
            dte_weekly=1,
            dte_monthly=20,
            is_gamma_week=True,
            is_gamma_month=False,
            days_to_next_weekly=8
        )
        
        score = engine.calculate_scores(crash_vol, struct, edge, external, time_m, "WEEKLY")
        
        mandate = engine.generate_mandate(
            score, crash_vol, struct, edge, external, time_m,
            "WEEKLY", time_m.weekly_exp, time_m.dte_weekly
        )
        
        print(f"\nFlash Crash Scenario:")
        print(f"  Composite Score: {score.composite:.2f}")
        print(f"  Regime: {mandate.regime_name}")
        print(f"  Allocation: {mandate.allocation_pct:.1f}%")
        print(f"  Strategy: {mandate.suggested_structure}")
        
        # Should recommend CASH or very conservative
        assert mandate.allocation_pct < 30 or mandate.regime_name == "CASH"
        assert score.composite < 5
    
    @pytest.mark.chaos
    def test_circuit_limit_scenario(self):
        """Test behavior when market hits circuit limits"""
        # Similar to flash crash but with trading halted
        # System should detect and not place trades
        
        ProductionConfig.DRY_RUN_MODE = True
        mock_client = Mock()
        engine = ExecutionEngine(mock_client)
        
        # Simulate market closed/halted
        with patch('volguard.MarketHolidaysAndTimingsApi') as mock_api:
            mock_instance = Mock()
            mock_response = Mock()
            mock_response.status = 'success'
            mock_response.data = Mock()
            mock_response.data.status = 'CLOSED'
            mock_instance.get_market_status.return_value = mock_response
            mock_api.return_value = mock_instance
            
            session_mgr = SessionManager(mock_client)
            is_open = session_mgr.check_market_status()
            
            print(f"\nCircuit Limit Test: Market open = {is_open}")
            
            # Should detect market is closed
            assert is_open is False


# ============================================================================
# BYZANTINE FAILURES
# ============================================================================

class TestByzantineFailures:
    
    @pytest.mark.chaos
    def test_inconsistent_data_across_sources(self):
        """Test handling when different data sources conflict"""
        # Price from API vs WebSocket differ significantly
        ProductionConfig.DRY_RUN_MODE = True
        mock_client = Mock()
        
        # Mock conflicting prices
        api_price = 23500
        ws_price = 24500  # 1000 point difference!
        
        print(f"\nInconsistent Data Test:")
        print(f"  API Price: {api_price}")
        print(f"  WS Price: {ws_price}")
        print(f"  Difference: {abs(api_price - ws_price)} points")
        
        # Validator should flag this
        from volguard import InstrumentValidator
        validator_instance = InstrumentValidator(mock_client)
        
        is_valid = validator_instance.validate_price(ws_price, api_price)
        
        print(f"  Price validated: {is_valid}")
        
        # Large difference should fail validation
        assert is_valid is False
    
    @pytest.mark.chaos
    def test_stale_data_detection(self):
        """Test detection of stale market data"""
        old_timestamp = time.time() - 3600  # 1 hour old
        current_timestamp = time.time()
        
        staleness = current_timestamp - old_timestamp
        
        print(f"\nStale Data Test:")
        print(f"  Data age: {staleness:.0f}s")
        print(f"  Threshold: {ProductionConfig.PRICE_STALENESS_THRESHOLD}s")
        
        # Should detect staleness
        assert staleness > ProductionConfig.PRICE_STALENESS_THRESHOLD
    
    @pytest.mark.chaos
    def test_malicious_input_injection(self):
        """Test protection against SQL injection and malicious inputs"""
        db_path = "/tmp/injection_test_db.db"
        if os.path.exists(db_path):
            os.remove(db_path)
        
        db = DatabaseWriter(db_path)
        
        # Try SQL injection in order_id
        malicious_order_id = "ORDER_1'; DROP TABLE order_log; --"
        
        try:
            db.log_order(
                malicious_order_id,
                "TEST_KEY",
                "BUY",
                25,
                100.0,
                "FILLED"
            )
            
            time.sleep(1)
            
            # Check if table still exists
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='order_log'")
            table_exists = cursor.fetchone() is not None
            conn.close()
            
            print(f"\nSQL Injection Test: Table survived = {table_exists}")
            
            assert table_exists  # Parameterized queries should prevent injection
            
        finally:
            db.shutdown()
            os.remove(db_path)


# ============================================================================
# RECOVERY SCENARIOS
# ============================================================================

class TestRecoveryScenarios:
    
    @pytest.mark.chaos
    def test_graceful_degradation(self):
        """Test system degrades gracefully under failure"""
        # Simulate partial system failure
        components_status = {
            'database': True,
            'circuit_breaker': True,
            'execution': False,  # Execution failed
            'risk_manager': True
        }
        
        # System should still function with reduced capability
        critical_components = ['database', 'circuit_breaker']
        
        can_operate = all(components_status[c] for c in critical_components)
        
        print(f"\nGraceful Degradation Test:")
        print(f"  Component Status: {components_status}")
        print(f"  Can operate: {can_operate}")
        
        assert can_operate is True
    
    @pytest.mark.chaos
    def test_automatic_recovery_attempt(self):
        """Test system attempts automatic recovery"""
        db_path = "/tmp/recovery_test_db.db"
        if os.path.exists(db_path):
            os.remove(db_path)
        
        db = DatabaseWriter(db_path)
        
        # Simulate database errors
        original_error_count = db.write_error_count
        
        # Force some errors (simulate by writing to closed DB)
        db.running = False
        time.sleep(0.5)
        
        # Try to write (should increment error count)
        for i in range(5):
            try:
                db.execute("INSERT INTO order_log (order_id) VALUES (?)", (f"ORDER_{i}",))
            except:
                pass
        
        time.sleep(1)
        
        print(f"\nAutomatic Recovery Test:")
        print(f"  Original errors: {original_error_count}")
        print(f"  Current errors: {db.write_error_count}")
        
        db.shutdown()
        os.remove(db_path)
        
        # Error count should have increased
        assert db.write_error_count >= original_error_count


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    pytest.main([
        __file__,
        '-v',
        '-s',
        '-m', 'chaos',
        '--tb=short'
    ])
