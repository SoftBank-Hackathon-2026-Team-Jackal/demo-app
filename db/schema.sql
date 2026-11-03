-- 데모 채팅 앱 스키마. pg-a에만 만들고, pg-b에는 이전 스크립트(01_copy_schema.sh)가 복사한다
SET ROLE app;

CREATE TABLE chat_messages (
  id            bigserial PRIMARY KEY,
  -- 클라이언트가 만든 ID. 재전송해도 한 번만 저장되고, 유실 검증의 기준이 된다
  client_msg_id text NOT NULL UNIQUE CHECK (length(client_msg_id) BETWEEN 8 AND 64),
  author        text NOT NULL,
  body          text NOT NULL,
  -- 이 메시지를 처리한 앱의 환경 (env-a, env-b)
  app_env       text NOT NULL,
  -- 이 메시지가 처음 저장된 DB. 기본값은 쓰기를 받은 서버에서 계산되고,
  -- 논리 복제는 값 자체를 옮기므로 복제된 행에서도 원래 값이 유지된다
  db_node       text NOT NULL DEFAULT coalesce(current_setting('app.node_name', true), 'unknown'),
  created_at    timestamptz NOT NULL DEFAULT now()
);

RESET ROLE;
-- 논리 복제의 초기 복사에 필요하다 (schema.sql의 GRANT 이후에 만든 테이블이므로 다시 부여)
GRANT SELECT ON chat_messages TO replicator;
