-- 서버마다 다른 노드 이름을 DB 설정으로 남긴다. 앱과 db_node 기본값이 current_setting('app.node_name')으로 읽는다.
-- ALTER DATABASE 설정은 pg_dump --schema-only와 논리 복제로 옮겨지지 않으므로 서버마다 고유하게 유지된다.
\getenv node_name NODE_NAME
ALTER DATABASE app SET app.node_name = :'node_name';
