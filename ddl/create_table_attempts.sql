CREATE TABLE IF NOT EXISTS attempts (
	id SERIAL PRIMARY KEY,
    user_id VARCHAR(32) NOT NULL,
    oauth_consumer_key TEXT,
    lis_result_sourcedid TEXT,
    lis_outcome_service_url TEXT,
    is_correct BOOLEAN,
    attempt_type VARCHAR(10) NOT NULL CHECK (attempt_type IN ('run', 'submit')),
    created_at TIMESTAMP NOT NULL,
    CONSTRAINT unique_attempt UNIQUE (user_id, attempt_type, created_at)
);