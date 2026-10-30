-- Servicio de PEDIDOS
create table if not exists orders (
    created_seq bigint auto_increment,
    id          varchar(20)    primary key,
    mode        varchar(15)    not null,
    customer    varchar(40)    not null,
    sku         varchar(40)    not null,
    quantity    int            not null,
    amount      decimal(14, 2) not null,
    address     varchar(200),
    status      varchar(12)    not null,
    reason      varchar(300)
);

-- Servicio de PAGOS
create table if not exists accounts (
    customer varchar(40)     primary key,
    balance  decimal(14, 2)  not null check (balance >= 0)
);
create table if not exists payments (
    order_id varchar(20)     primary key,        -- clave de idempotencia: un solo cobro por pedido
    customer varchar(40)     not null,
    amount   decimal(14, 2)  not null,
    status   varchar(12)     not null,           -- PROCESSING, CHARGED, FAILED, REFUNDED
    reason   varchar(200)
);

-- Servicio de INVENTARIO
create table if not exists products (
    sku   varchar(40)     primary key,
    price decimal(14, 2)  not null,
    stock int             not null check (stock >= 0)
);
create table if not exists reservations (
    order_id varchar(20) primary key,            -- clave de idempotencia
    sku      varchar(40) not null,
    quantity int         not null,
    status   varchar(12) not null                -- PROCESSING, RESERVED, REJECTED, RELEASED
);

-- Servicio de ENVIOS
create table if not exists shipments (
    order_id varchar(20)  primary key,
    address  varchar(200) not null,
    status   varchar(12)  not null               -- SCHEDULED, FAILED
);

-- ORQUESTADOR: estado de cada saga
create table if not exists sagas (
    order_id   varchar(20) primary key,
    state      varchar(14) not null,
    step_index int         not null,
    deadline   bigint      not null,
    attempts   int         not null
);

-- Bitacora de todo lo que ocurre en una saga
create table if not exists saga_log (
    id       bigint auto_increment primary key,
    order_id varchar(20)  not null,
    source   varchar(15)  not null,
    event    varchar(40)  not null,
    detail   varchar(300)
);
create index if not exists idx_saga_log_order on saga_log (order_id);
