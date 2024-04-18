CREATE TABLE lease(
  name VARCHAR(255) PRIMARY KEY,
  version BIGINT NOT NULL,
  acquired_at TIMESTAMP WITH TIME ZONE NOT NULL,
  renewed_at TIMESTAMP WITH TIME ZONE NOT NULL,
  timeout BIGINT NOT NULL,
  holder_name varchar(255) NOT NULL,
  expiry_datetime TIMESTAMP WITH TIME ZONE NOT NULL
);