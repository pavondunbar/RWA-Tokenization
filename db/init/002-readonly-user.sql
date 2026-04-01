-- -----------------------------------------------
-- Read-only user for the outbox publisher.
-- The publisher needs SELECT on outbox tables
-- and INSERT on outbox_published to mark events
-- as delivered (append-only — no UPDATE allowed).
-- -----------------------------------------------

CREATE USER readonly_user WITH PASSWORD 'readonly_pass';

GRANT CONNECT ON DATABASE rwa TO readonly_user;
GRANT USAGE ON SCHEMA public TO readonly_user;

GRANT SELECT, UPDATE ON outbox_events TO readonly_user;
GRANT SELECT ON outbox_published TO readonly_user;
GRANT INSERT ON outbox_published TO readonly_user;

-- The publisher reads the unpublished-events view
GRANT SELECT ON v_unpublished_events TO readonly_user;

-- DLQ tables: publisher records failed attempts and dead-letters
GRANT SELECT, INSERT ON outbox_publish_attempts TO readonly_user;
GRANT SELECT, INSERT ON outbox_dlq TO readonly_user;
