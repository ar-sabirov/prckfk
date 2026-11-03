-- Run with: docker-compose run --rm --no-deps ksqldb-check
-- Wait for asynchronous aggregates to settle, then compare with the README:
--                 Bob messages/recipients | Степан messages/recipients | global
-- After m1-m3:           2/1               |          1/2               | 3/3
-- After m4 (restart):    3/1               |          1/2               | 4/3
-- After m5 (unblock):    4/2               |          1/2               | 5/3
-- Alice has no row because she has not sent any messages.
-- A fresh startup has no messages and therefore no statistics rows yet.
SHOW QUERIES;
SELECT * FROM USER_STATISTICS;
SELECT * FROM GLOBAL_STATISTICS WHERE STATISTICS_KEY = 'all';
