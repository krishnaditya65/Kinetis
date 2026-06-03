-- V10: webhook callback URLs on jobs and workflows.
-- When a job or workflow reaches a terminal state, Kinetis POSTs a JSON payload
-- to the configured URL. NULL means no callback.

ALTER TABLE jobs      ADD COLUMN callback_url TEXT;
ALTER TABLE workflows ADD COLUMN callback_url TEXT;
