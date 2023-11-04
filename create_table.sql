create table outbox (
    id uuid not null primary key default gen_random_uuid(),
    exchange text not null,
    exchange_kind text not null default 'direct',
    routing_key text,
    headers json not null default '{}',
    content_type text not null default 'application/json',
    content_encoding text not null default 'utf-8',
    delivery_mode text not null default 'persistent',
    body json
);