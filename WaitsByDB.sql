--============================================================
-- WAITS FOR A SPECIFIC DATABASE'S SESSIONS
--============================================================
IF NOT EXISTS(SELECT 1 FROM tempdb.sys.objects WHERE name LIKE '#WAITS_%')
        BEGIN
                CREATE TABLE #WAITS
                        (
                                database_id sysname
                                , wait_type nvarchar(60)
                                , waiting_tasks_count bigint
                                , wait_time_ms bigint
                                , max_wait_time_ms bigint
                                , signal_wait_time_ms bigint
                        ) 
        END
ELSE 
        BEGIN
                TRUNCATE TABLE #WAITS;
        END

INSERT INTO #WAITS
        (
                database_id
                , wait_type
                , waiting_tasks_count
                , wait_time_ms 
                , signal_wait_time_ms
        )
SELECT eS.database_id
        , SWS.wait_type
        , SUM(SWS.waiting_tasks_count) AS waiting_tasks_count
        , SUM(SWS.wait_time_ms) AS wait_time_ms
        , SUM(SWS.signal_wait_time_ms) AS signal_wait_time_ms
FROM sys.dm_exec_session_wait_stats SWS
        INNER JOIN sys.dm_exec_sessions eS
                ON SWS.session_id = eS.session_id
WHERE SWS.wait_type 
        NOT IN (-- filter out additional irrelevant waits
                        'BROKER_TASK_STOP', 'BROKER_RECEIVE_WAITFOR'
                        , 'BROKER_TO_FLUSH', 'BROKER_TRANSMITTER', 'CHECKPOINT_QUEUE'
                        , 'CHKPT', 'DISPATCHER_QUEUE_SEMAPHORE', 'CLR_AUTO_EVENT'
                        , 'CLR_MANUAL_EVENT','FT_IFTS_SCHEDULER_IDLE_WAIT', 'KSOURCE_WAKEUP' 
                        , 'LAZYWRITER_SLEEP', 'LOGMGR_QUEUE', 'MISCELLANEOUS', 'ONDEMAND_TASK_QUEUE'
                        , 'REQUEST_FOR_DEADLOCK_SEARCH', 'SLEEP_TASK', 'TRACEWRITE'
                        , 'SQLTRACE_BUFFER_FLUSH', 'XE_DISPATCHER_WAIT', 'XE_TIMER_EVENT'
                        , 'DIRTY_PAGE_POLL', 'SQLTRACE_INCREMENTAL_FLUSH_SLEEP'
                        , 'BROKER_EVENTHANDLER', 'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP'
                        , 'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP', 'SP_SERVER_DIAGNOSTICS_SLEEP'
                        , 'HADR_FILESTREAM_IOMGR_IOCOMPLETION', 'HADR_WORK_QUEUE', 'HADR_NOTIFICATION_DEQUEUE'
                        , 'HADR_LOGCAPTURE_WAIT', 'HADR_CLUSAPI_CALL', 'HADR_TIMER_TASK', 'HADR_SYNC_COMMIT'
                        , 'PREEMPTIVE_SP_SERVER_DIAGNOSTICS', 'PREEMPTIVE_HADR_LEASE_MECHANISM'
                        ,'PREEMPTIVE_OS_GETFILEATTRIBUTES', 'PREEMPTIVE_OS_CREATEFILE', 'PREEMPTIVE_OS_FILEOPS'
                        , 'XE_LIVE_TARGET_TVF') 
        AND DB_NAME(es.database_id) = '<database_name, ,>'
        AND eS.is_user_process = 1
GROUP BY eS.database_id
        , SWS.wait_type
OPTION (MAXDOP 1);

WITH Waits AS
        (
        SELECT SWS.database_id
                , SWS.wait_type
                , SWS.wait_time_ms
                , SWS.signal_wait_time_ms 
                , (SWS.wait_time_ms - SWS.signal_wait_time_ms) AS resource_wait_time_ms
                , SWS.waiting_tasks_count 
                , CASE SWS.waiting_tasks_count
                        WHEN 0 THEN 0
                        ELSE SWS.wait_time_ms/SWS.waiting_tasks_count
                END AS avg_wait_time_ms
                , (100. * SWS.wait_time_ms) / SUM(SWS.wait_time_ms) OVER() AS pct
                , ROW_NUMBER() OVER(ORDER BY (SWS.wait_time_ms - SWS.signal_wait_time_ms) DESC) AS row_num
        FROM #WAITS SWS
                )
                                                
SELECT TOP 3 DB_NAME(W.database_id) AS the_database
        , W.wait_type
        , CAST(W.wait_time_ms AS DECIMAL(20, 0)) AS wait_time_ms
        , CAST(W.signal_wait_time_ms AS DECIMAL(20, 0)) AS signal_wait_time_ms
        , CAST(W.resource_wait_time_ms AS DECIMAL(20, 0)) AS resource_wait_time_ms
        , W.waiting_tasks_count
        , W.avg_wait_time_ms
        , CAST(W.pct AS DECIMAL(5, 2)) AS pct
        , CAST(SUM(W.pct) OVER(ORDER BY (W.resource_wait_time_ms) DESC)AS DECIMAL(5,2)) AS running_pct
FROM Waits AS W
GROUP BY W.database_id
        , W.row_num 
        , W.wait_type 
        , W.wait_time_ms
        , W.signal_wait_time_ms
        , W.resource_wait_time_ms
        , W.waiting_tasks_count
        , W.avg_wait_time_ms 
        , W.pct
ORDER BY W.pct DESC 
OPTION (MAXDOP 1);

DROP TABLE #WAITS;
GO