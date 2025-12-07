# DAG Processor Crashes on MySQL Connection Failure During Import Error Recording

## Summary

The DAG processor crashes and enters a restart loop when MySQL connection fails while recording DAG import errors to the database. This is due to missing exception handling for `session.flush()` after caught exceptions leave the session in an invalid state.

## Airflow Version

- **Affected Version**: 3.1.3 (likely affects 3.0.x and 3.1.x series)
- **Component**: DAG Processor (`airflow dag-processor`)
- **Database**: MySQL (also potentially affects PostgreSQL)

## Environment

- **Deployment**: Kubernetes (OKE on OCI), Docker Compose (local reproduction)
- **Executor**: KubernetesExecutor
- **Database**: MySQL 8.0
- **Observed**: Production environment with remote MySQL

## Problem Description

### Expected Behavior

When a DAG has import errors (e.g., `ModuleNotFoundError`):
1. ✅ Import error should be caught during parsing
2. ✅ Error should be stored in memory
3. ✅ Error should be recorded to the database
4. ✅ If database operation fails, it should be retried or logged
5. ✅ DAG processor should continue processing other DAGs
6. ✅ Import errors should eventually appear in the Airflow UI

### Actual Behavior

When MySQL connection fails during import error recording:
1. ✅ Import error is caught during parsing
2. ✅ Error is stored in memory
3. ❌ Database operation fails (connection timeout, pool exhaustion, network hiccup)
4. ❌ Exception is caught but **session is left in invalid state**
5. ❌ `session.flush()` attempts to flush the invalid session
6. ❌ New exception is raised (OperationalError or PendingRollbackError)
7. ❌ **DAG processor process crashes with exit code 1**
8. ❌ In production with restart policy: **continuous restart loop**

### Impact

**Production Environment:**
- DAG processor pod restarted **1,259 times in 4 days** (~13 restarts/hour)
- Each restart creates new database connections
- Connection pool exhaustion
- Cascading failures across Airflow components
- System instability

**Symptoms:**
- Import errors not visible in UI (DB write fails before crash)
- Liveness probe failures
- Excessive MySQL connection usage
- API server errors (`PendingRollbackError` when trying to reuse connections)

## Root Cause Analysis

### Location 1: Missing Session Rollback

**File**: `airflow/dag_processing/collection.py`  
**Lines**: ~439-448

```python
# Line 423: Import errors recording - NO RETRIES by design
try:
    _update_import_errors(
        files_parsed=good_dag_filelocs,
        bundle_name=bundle_name,
        import_errors=import_errors,
        session=session,
    )
except Exception:
    log.exception("Error logging import errors!")
    # ❌ MISSING: session.rollback() here!

# Line 442: DAG warnings - same issue
try:
    _update_dag_warnings([dag.dag_id for dag in dags], warnings, warning_types, session)
except Exception:
    log.exception("Error logging DAG warnings.")
    # ❌ MISSING: session.rollback() here!

# Line 448: Session flush - NO ERROR HANDLING
session.flush()  # ❌ CRASHES if session is in invalid state
```

### Why This Crashes

1. MySQL connection fails during `_update_import_errors()`
2. SQLAlchemy exception is raised and **caught** (line 439)
3. Session now has uncommitted/failed transaction
4. Session is in **invalid state** but not rolled back
5. `session.flush()` (line 448) tries to flush invalid session
6. New exception: `OperationalError` or `PendingRollbackError`
7. This exception is **NOT caught** → process exits

### Design Inconsistency

The code shows inconsistent error handling:

```python
# DAG serialization: HAS retries (line 394)
for attempt in run_with_db_retries(logger=log):  # 3 retries with exponential backoff
    with attempt:
        SerializedDAG.bulk_write_to_db(...)

# Import errors: NO retries (line 423)
# Developer comment: "we don't retry on this one as it's not as critical"
try:
    _update_import_errors(...)
except Exception:
    log.exception(...)  # Just log, no retry, no session cleanup
```

**The contradiction**: If import errors are "not critical," why does failure crash the entire DAG processor?

## Reproduction Steps

### Prerequisites
- Docker and Docker Compose
- Airflow 3.1.x with MySQL backend
- DAGs with import errors

### Steps to Reproduce

1. **Create test DAGs with import errors**:
```python
# test_import_error.py
from datetime import datetime
from airflow import DAG
from nonexistent_module import some_function  # Will fail

with DAG('test_error', start_date=datetime(2025,1,1), schedule=None) as dag:
    pass
```

2. **Start Airflow with MySQL**:
```bash
docker compose up -d
# Wait for DAG processor to parse the DAG with import error
```

3. **Simulate connection failure**:
```bash
# Restart MySQL while DAG processor is running
docker compose restart mysql
```

4. **Observe the crash**:
```bash
docker compose logs -f airflow-dag-processor
# Will show: sqlalchemy.exc.OperationalError
# Container exits with code 1
```

### Reproduction Environment

Complete reproduction available at: [Link to test branch if made public]

**Docker Compose config** that reproduces the bug:
- MySQL with low connection limits (20)
- Small SQLAlchemy pool (size: 2, overflow: 3)
- Short pool timeout (10 seconds)
- DAG processor service
- Multiple DAGs with import errors

**Success Rate**: 100% reproduction when MySQL is restarted during DAG processing

## Error Messages

### Error 1: Lost Connection (Transient)
```
sqlalchemy.exc.OperationalError: (pymysql.err.OperationalError) 
(2013, 'Lost connection to MySQL server during query')
```

### Error 2: Connection Refused (MySQL Down)
```
sqlalchemy.exc.OperationalError: (pymysql.err.OperationalError) 
(2003, "Can't connect to MySQL server on 'mysql' ([Errno 111] Connection refused)")
```

### Error 3: Invalid Session State (After Caught Exception)
```
sqlalchemy.exc.PendingRollbackError: 
Can't reconnect until invalid transaction is rolled back. 
Please rollback() fully before proceeding
```

### Error 4: Connection Pool Timeout
```
sqlalchemy.exc.TimeoutError: 
QueuePool limit of size 5 overflow 10 reached, connection timed out
```

All these errors lead to the same crash due to missing error handling in `session.flush()`.

## Stack Trace

```
Traceback (most recent call last):
  File "airflow/dag_processing/collection.py", line 433, in update_dag_parsing_results_in_db
    _update_import_errors(...)
  File "airflow/dag_processing/collection.py", line 291, in _update_import_errors
    session.execute(...)
  [SQLAlchemy connection failure]
  
pymysql.err.OperationalError: (2013, 'Lost connection to MySQL server during query')

# Exception is caught here ✅ but session not cleaned up ❌

# Later...
  File "airflow/dag_processing/collection.py", line 448, in update_dag_parsing_results_in_db
    session.flush()  # ❌ This line crashes because session is invalid

sqlalchemy.exc.PendingRollbackError: Can't reconnect until invalid transaction is rolled back
```

## Proposed Solution

### Solution 1: Add Session Rollback (Minimal Fix)

**File**: `airflow/dag_processing/collection.py`  
**Lines**: ~439-448

```python
# Current (broken)
try:
    _update_import_errors(...)
except Exception:
    log.exception("Error logging import errors!")
    # ❌ Missing session cleanup

try:
    _update_dag_warnings(...)
except Exception:
    log.exception("Error logging DAG warnings.")
    # ❌ Missing session cleanup

session.flush()  # ❌ No error handling
```

**Proposed fix**:
```python
try:
    _update_import_errors(
        files_parsed=good_dag_filelocs,
        bundle_name=bundle_name,
        import_errors=import_errors,
        session=session,
    )
except Exception:
    log.exception("Error logging import errors!")
    session.rollback()  # ✅ Clean up invalid session state

try:
    _update_dag_warnings([dag.dag_id for dag in dags], warnings, warning_types, session)
except Exception:
    log.exception("Error logging DAG warnings.")
    session.rollback()  # ✅ Clean up invalid session state

try:
    session.flush()  # ✅ Now wrapped in error handling
except Exception:
    log.exception("Error flushing session after parsing results")
    session.rollback()  # ✅ Don't crash - log and continue
```

### Solution 2: Add Retries (Better Fix)

Make import error recording consistent with DAG serialization:

```python
# Use the same retry logic as DAG serialization
for attempt in run_with_db_retries(logger=log):
    with attempt:
        _update_import_errors(
            files_parsed=good_dag_filelocs,
            bundle_name=bundle_name,
            import_errors=import_errors,
            session=session,
        )
```

### Solution 3: Defensive Session Management (Best Fix)

Wrap the entire database operation section:

```python
try:
    # All database operations here with retries
    for attempt in run_with_db_retries(logger=log):
        with attempt:
            _update_import_errors(...)
            _update_dag_warnings(...)
            session.flush()
except Exception:
    log.exception("Critical error updating parsing results in database")
    session.rollback()
    # Continue processing - don't crash the DAG processor
```

## Why This is Important

### Impact on Distributed Systems

Airflow is designed for distributed, production environments where:
- Network hiccups are **normal, expected events**
- Database timeouts happen under load
- Connection pools get exhausted temporarily
- Systems should **retry transient failures** automatically

**Current behavior violates distributed systems best practices** by:
- Not retrying transient errors
- Not cleaning up failed transactions
- Crashing on recoverable errors
- Creating restart cascades

### Comparison to Other Components

Other Airflow components handle database failures gracefully:
- **Scheduler**: Has retry logic, handles connection failures
- **Triggerer**: Retries database operations
- **Task execution**: Retries on transient failures

**Only the DAG processor lacks proper error recovery** for the import error recording path.

## Related Issues

Possibly related to:
- #50708 - DAG processor memory leak (Airflow 3.x)
- #49650 - DAG processor RuntimeError (Airflow 3.0.0)
- #49887 - File descriptor exhaustion in dag-processor
- Reports of intermittent dag-processor failures with MySQL in Kubernetes (mailing list)

## Testing

### Verification Before Fix

1. Start Airflow with MySQL
2. Add DAGs with import errors
3. Restart MySQL or cause connection timeout
4. DAG processor crashes with `OperationalError` or `PendingRollbackError`

### Verification After Fix

1. Start Airflow with MySQL
2. Add DAGs with import errors
3. Restart MySQL or cause connection timeout
4. DAG processor logs error but **continues running**
5. Import errors eventually saved when connection recovers
6. No crash, no restart loop

## Additional Context

### Production Metrics
- **Environment**: Kubernetes (Oracle Cloud Infrastructure)
- **DAG Count**: 90+ DAGs
- **Restart Count**: 1,259 restarts in 4 days (~13 restarts/hour)
- **Error Frequency**: Crashes within minutes of startup under load
- **MySQL Connection Pool**: size=5, max_overflow=10

### Configuration Attempted
Multiple configuration tuning attempts have been made:
- Increased `sql_alchemy_pool_size` from 4 to 5
- Increased `sql_alchemy_max_overflow` from 4 to 10
- Increased `sql_alchemy_pool_timeout` from 20 to 60 seconds
- Enabled `sql_alchemy_pool_pre_ping`
- Set `sql_alchemy_pool_recycle` to 60 seconds

**None of these fixes addressed the root cause** - the missing exception handling in the code.

### Why Configuration Cannot Fix This

The bug is in the **code logic**, not configuration:
1. Exception is caught ✅
2. But session is not rolled back ❌
3. `session.flush()` has no error handling ❌

No amount of connection pool tuning can prevent:
- Network hiccups
- MySQL restarts
- Connection timeouts under load
- Transaction errors

**The code must handle these gracefully** - that's fundamental distributed systems design.

## Workarounds (Temporary)

While waiting for a fix:

### 1. Increase Connection Pool Settings
```yaml
sql_alchemy_pool_size: 10
sql_alchemy_max_overflow: 20
sql_alchemy_pool_timeout: 60
```

**Effectiveness**: Reduces frequency but doesn't eliminate crashes

### 2. Reduce DAG Processor Resource Limits
```yaml
dagProcessor:
  resources:
    limits:
      memory: "1Gi"
      cpu: "500m"
```

**Effectiveness**: Faster restarts but doesn't prevent crashes

### 3. Increase Liveness Probe Timeout
```yaml
livenessProbe:
  initialDelaySeconds: 30
  timeoutSeconds: 30
  periodSeconds: 60
  failureThreshold: 5
```

**Effectiveness**: Tolerates temporary hangs but doesn't prevent crashes

### 4. Set Up Alerting
Monitor DAG processor restarts and alert when > 10/hour

**Effectiveness**: Doesn't fix the issue but helps with detection

**None of these are real solutions** - the code needs to be fixed.

## Files Affected

### Primary Issue
- `airflow/dag_processing/collection.py` - Line ~448 (`session.flush()`)
- `airflow/dag_processing/collection.py` - Lines ~439, ~445 (missing `session.rollback()`)

### Secondary Issues (Related Session Management)
- `airflow/dag_processing/manager.py` - Line ~842 (`process_parse_results()` call site)
- Session lifecycle management in DAG processor

## Proposed Changes

### Change 1: Add Session Rollback After Caught Exceptions

**File**: `airflow/dag_processing/collection.py`

```diff
     try:
         _update_import_errors(
             files_parsed=good_dag_filelocs,
             bundle_name=bundle_name,
             import_errors=import_errors,
             session=session,
         )
     except Exception:
         log.exception("Error logging import errors!")
+        session.rollback()  # Clean up invalid session state
 
     # Record DAG warnings in the metadatabase.
     try:
         _update_dag_warnings([dag.dag_id for dag in dags], warnings, warning_types, session)
     except Exception:
         log.exception("Error logging DAG warnings.")
+        session.rollback()  # Clean up invalid session state
 
-    session.flush()
+    try:
+        session.flush()
+    except Exception:
+        log.exception("Error flushing session after parsing results")
+        session.rollback()
+        # Don't crash - continue processing other DAGs
```

### Change 2: Add Retries for Import Error Recording (Recommended)

Make import error recording consistent with DAG serialization:

```diff
-    # Record import errors into the ORM - we don't retry on this one as it's not as critical that it works
-    try:
-        _update_import_errors(...)
-    except Exception:
-        log.exception("Error logging import errors!")
+    # Record import errors into the ORM with retries for transient failures
+    for attempt in run_with_db_retries(logger=log):
+        with attempt:
+            _update_import_errors(
+                files_parsed=good_dag_filelocs,
+                bundle_name=bundle_name,
+                import_errors=import_errors,
+                session=session,
+            )
+            break  # Success - exit retry loop
```

## Benefits of Fix

1. **Graceful degradation**: DAG processor continues running during database issues
2. **Automatic recovery**: Import errors saved when connection recovers
3. **No restart loops**: Eliminates cascading failures
4. **Better resilience**: Handles network hiccups and transient errors
5. **Consistent behavior**: Matches retry logic used for DAG serialization
6. **Production stability**: Prevents connection pool exhaustion from restart storms

## Testing Recommendation

### Unit Test
```python
def test_dag_processor_handles_db_failure_during_import_error_recording():
    """Test that DAG processor doesn't crash when DB fails during import error recording."""
    # Mock MySQL connection failure
    # Verify exception is logged
    # Verify DAG processor continues
    # Verify session is rolled back
```

### Integration Test
```python
def test_dag_processor_resilience_to_mysql_restart():
    """Test DAG processor handles MySQL restart during operation."""
    # Start DAG processor with DAGs containing import errors
    # Restart MySQL
    # Verify DAG processor doesn't crash
    # Verify import errors eventually recorded
```

## Additional Observations

### API Server Has Similar Issue

The API server shows `PendingRollbackError` when trying to load users after database connection issues:

```
sqlalchemy.exc.PendingRollbackError: Can't reconnect until invalid transaction 
is rolled back. Please rollback() fully before proceeding
```

This suggests a broader pattern of inadequate session state management across Airflow components.

**Location**: Flask-Login authentication flow in `airflow.providers.fab.auth_manager`

**Recommendation**: Add session rollback middleware or error handlers for Flask routes.

## Severity

**Critical** - Causes complete DAG processor failure and restart loops in production environments with remote databases.

## Reproducibility

**100%** reproducible when:
- DAGs have import errors (normal situation)
- MySQL connection fails (network issue, restart, timeout)
- Running as continuous service (not one-shot CLI commands)

## Request

Please review and consider:
1. Adding session rollback after caught exceptions in `collection.py`
2. Adding error handling for `session.flush()`
3. Optionally: Adding retries for import error recording
4. Reviewing session lifecycle management in other components

The fix is straightforward and would significantly improve Airflow stability in production environments with remote databases.

## Contact

- Reproduced by: Kueez Infrastructure Team
- Production Environment: Oracle Cloud Infrastructure (OKE)
- Available for testing: Yes
- Can provide reproduction environment: Yes

---

**Note**: This issue has been fully reproduced in both Docker Compose and Kubernetes environments with detailed stack traces, logs, and reproduction steps available.
