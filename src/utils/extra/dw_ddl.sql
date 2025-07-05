-- Table: public.dim_cliente

-- DROP TABLE IF EXISTS public.dim_cliente;

CREATE TABLE IF NOT EXISTS public.dim_cliente
(
    cliente_key integer NOT NULL,
    cliente_id integer NOT NULL,
    nit character varying(20) COLLATE pg_catalog."default",
    nombre character varying(120) COLLATE pg_catalog."default" NOT NULL,
    email character varying(120) COLLATE pg_catalog."default",
    direccion character varying(250) COLLATE pg_catalog."default",
    telefono character varying(100) COLLATE pg_catalog."default",
    ciudad character varying(120) COLLATE pg_catalog."default",
    departamento character varying(120) COLLATE pg_catalog."default",
    sector character varying(50) COLLATE pg_catalog."default",
    activo boolean,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT dim_cliente_pkey PRIMARY KEY (cliente_key),
    CONSTRAINT uk_cliente_id UNIQUE (cliente_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.dim_cliente
    OWNER to postgres;

--------------------------------------------------------------------

-- Table: public.dim_estado_servicio
CREATE TABLE IF NOT EXISTS public.dim_estado_servicio
(
    estado_servicio_key integer NOT NULL,
    estado_servicio_id integer,
    nombre character varying(75) COLLATE pg_catalog."default" NOT NULL,
    descripcion character varying(500) COLLATE pg_catalog."default" NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT dim_estado_servicio_pkey PRIMARY KEY (estado_servicio_key),
    CONSTRAINT uk_estado_servicio_id UNIQUE (estado_servicio_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.dim_estado_servicio
    OWNER to postgres;

--------------------------------------------------------------------

-- Table: public.dim_fecha

CREATE TABLE IF NOT EXISTS public.dim_fecha
(
    fecha_key integer NOT NULL,
    fecha date NOT NULL,
    anio integer NOT NULL,
    mes integer NOT NULL,
    dia integer NOT NULL,
    trimestre integer NOT NULL,
    nombre_mes character varying(20) COLLATE pg_catalog."default" NOT NULL,
    dia_semana character varying(20) COLLATE pg_catalog."default" NOT NULL,
    es_festivo boolean NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT dim_fecha_pkey PRIMARY KEY (fecha_key),
    CONSTRAINT uk_fecha UNIQUE (fecha)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.dim_fecha
    OWNER to postgres;

--------------------------------------------------------------------

-- Table: public.dim_fecha

CREATE TABLE IF NOT EXISTS public.dim_fecha
(
    fecha_key integer NOT NULL,
    fecha date NOT NULL,
    anio integer NOT NULL,
    mes integer NOT NULL,
    dia integer NOT NULL,
    trimestre integer NOT NULL,
    nombre_mes character varying(20) COLLATE pg_catalog."default" NOT NULL,
    dia_semana character varying(20) COLLATE pg_catalog."default" NOT NULL,
    es_festivo boolean NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT dim_fecha_pkey PRIMARY KEY (fecha_key),
    CONSTRAINT uk_fecha UNIQUE (fecha)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.dim_fecha
    OWNER to postgres;

--------------------------------------------------------------------
-- Table: public.dim_mensajero

CREATE TABLE IF NOT EXISTS public.dim_mensajero
(
    mensajero_key integer NOT NULL,
    mensajero_id integer,
    nombre_usuario character varying(150) COLLATE pg_catalog."default",
    nombre character varying(120) COLLATE pg_catalog."default",
    apellido character varying(120) COLLATE pg_catalog."default",
    telefono character varying(15) COLLATE pg_catalog."default",
    ciudad_operacion character varying(120) COLLATE pg_catalog."default",
    departamento_operacion character varying(120) COLLATE pg_catalog."default",
    activo boolean,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT dim_mensajero_pkey PRIMARY KEY (mensajero_key),
    CONSTRAINT uk_mensajero_id UNIQUE (mensajero_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.dim_mensajero
    OWNER to postgres;

--------------------------------------------------------------------
-- Table: public.dim_novedad

CREATE TABLE IF NOT EXISTS public.dim_novedad
(
    novedad_key integer NOT NULL,
    novedad_id integer,
    nombre character varying(30) COLLATE pg_catalog."default" NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT dim_novedad_pkey PRIMARY KEY (novedad_key),
    CONSTRAINT uk_novedad_id UNIQUE (novedad_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.dim_novedad
    OWNER to postgres;

--------------------------------------------------------------------
-- Table: public.dim_sede

CREATE TABLE IF NOT EXISTS public.dim_sede
(
    sede_key integer NOT NULL,
    sede_id integer NOT NULL,
    nombre character varying(120) COLLATE pg_catalog."default" NOT NULL,
    direccion character varying(250) COLLATE pg_catalog."default",
    ciudad character varying(120) COLLATE pg_catalog."default" NOT NULL,
    departamento character varying(120) COLLATE pg_catalog."default" NOT NULL,
    cliente_id integer NOT NULL,
    nit_cliente character varying(50) COLLATE pg_catalog."default" NOT NULL,
    nombre_cliente character varying(120) COLLATE pg_catalog."default" NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT dim_sede_pkey PRIMARY KEY (sede_key),
    CONSTRAINT uk_sede_id UNIQUE (sede_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.dim_sede
    OWNER to postgres;

--------------------------------------------------------------------

-- Table: public.dim_tipo_servicio

CREATE TABLE IF NOT EXISTS public.dim_tipo_servicio
(
    tipo_servicio_key integer NOT NULL,
    tipo_servicio_id integer,
    nombre character varying(75) COLLATE pg_catalog."default" NOT NULL,
    descripcion character varying(500) COLLATE pg_catalog."default" NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT dim_tipo_servicio_pkey PRIMARY KEY (tipo_servicio_key),
    CONSTRAINT uk_tipo_servicio_id UNIQUE (tipo_servicio_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.dim_tipo_servicio
    OWNER to postgres;


--------------------------------------------------------------------

-- Table: public.fact_estado_servicio_transaccional

CREATE TABLE IF NOT EXISTS public.fact_estado_servicio_transaccional
(
    estado_transaccional_key integer NOT NULL,
    servicio_key integer NOT NULL,
    estado_anterior_key integer,
    estado_nuevo_key integer,
    fecha_cambio_key integer NOT NULL,
    hora_cambio_key integer NOT NULL,
    duracion_estado_minutos integer,
    CONSTRAINT fact_estado_servicio_transaccional_pkey PRIMARY KEY (estado_transaccional_key),
    CONSTRAINT fk_estado_anterior FOREIGN KEY (estado_anterior_key)
        REFERENCES public.dim_estado_servicio (estado_servicio_key) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_estado_nuevo FOREIGN KEY (estado_nuevo_key)
        REFERENCES public.dim_estado_servicio (estado_servicio_key) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_fecha_cambio FOREIGN KEY (fecha_cambio_key)
        REFERENCES public.dim_fecha (fecha_key) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_hora_cambio FOREIGN KEY (hora_cambio_key)
        REFERENCES public.dim_hora (hora_key) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_servicio FOREIGN KEY (servicio_key)
        REFERENCES public.fact_servicio (servicio_key) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.fact_estado_servicio_transaccional
    OWNER to postgres;

--------------------------------------------------------------------
-- Table: public.fact_novedad_servicio_transaccional

CREATE TABLE IF NOT EXISTS public.fact_novedad_servicio_transaccional
(
    novedad_transaccional_key integer NOT NULL,
    servicio_key integer NOT NULL,
    novedad_key integer NOT NULL,
    mensajero_key integer,
    descripcion character varying(700) COLLATE pg_catalog."default",
    fecha_novedad_key integer NOT NULL,
    hora_novedad_key integer NOT NULL,
    CONSTRAINT fact_novedad_servicio_transaccional_pkey PRIMARY KEY (novedad_transaccional_key),
    CONSTRAINT fk_fecha_novedad FOREIGN KEY (fecha_novedad_key)
        REFERENCES public.dim_fecha (fecha_key) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_hora_novedad FOREIGN KEY (hora_novedad_key)
        REFERENCES public.dim_hora (hora_key) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_mensajero FOREIGN KEY (mensajero_key)
        REFERENCES public.dim_mensajero (mensajero_key) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_novedad FOREIGN KEY (novedad_key)
        REFERENCES public.dim_novedad (novedad_key) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_servicio FOREIGN KEY (servicio_key)
        REFERENCES public.fact_servicio (servicio_key) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.fact_novedad_servicio_transaccional
    OWNER to postgres;

--------------------------------------------------------------------
-- Table: public.fact_servicio

CREATE TABLE IF NOT EXISTS public.fact_servicio
(
    servicio_key integer NOT NULL,
    servicio_id integer,
    cliente_key integer,
    mensajero_key integer,
    sede_key integer,
    tipo_servicio_key integer,
    estado_servicio_final_key integer,
    prioridad character varying(50) COLLATE pg_catalog."default",
    ciudad_origen character varying(50) COLLATE pg_catalog."default",
    departamento_origen character varying(50) COLLATE pg_catalog."default",
    ciudad_destino character varying(50) COLLATE pg_catalog."default",
    departamento_destino character varying(50) COLLATE pg_catalog."default",
    fecha_solicitud_key integer,
    hora_solicitud_key integer,
    fecha_iniciado_key integer,
    hora_iniciado_key integer,
    fecha_asignacion_key integer,
    hora_asignacion_key integer,
    fecha_recogida_key integer,
    hora_recogida_key integer,
    fecha_entrega_key integer,
    hora_entrega_key integer,
    tiempo_total_entrega_min integer,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fact_servicio_pkey PRIMARY KEY (servicio_key),
    CONSTRAINT uk_fact_servicio_id UNIQUE (servicio_id),
    CONSTRAINT fk_cliente FOREIGN KEY (cliente_key)
        REFERENCES public.dim_cliente (cliente_key) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_estado_servicio_final FOREIGN KEY (estado_servicio_final_key)
        REFERENCES public.dim_estado_servicio (estado_servicio_key) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_fecha_asignacion FOREIGN KEY (fecha_asignacion_key)
        REFERENCES public.dim_fecha (fecha_key) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_fecha_entrega FOREIGN KEY (fecha_entrega_key)
        REFERENCES public.dim_fecha (fecha_key) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_fecha_iniciado FOREIGN KEY (fecha_iniciado_key)
        REFERENCES public.dim_fecha (fecha_key) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_fecha_recogida FOREIGN KEY (fecha_recogida_key)
        REFERENCES public.dim_fecha (fecha_key) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_fecha_solicitud FOREIGN KEY (fecha_solicitud_key)
        REFERENCES public.dim_fecha (fecha_key) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_hora_asignacion FOREIGN KEY (hora_asignacion_key)
        REFERENCES public.dim_hora (hora_key) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_hora_entrega FOREIGN KEY (hora_entrega_key)
        REFERENCES public.dim_hora (hora_key) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_hora_iniciado FOREIGN KEY (hora_iniciado_key)
        REFERENCES public.dim_hora (hora_key) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_hora_recogida FOREIGN KEY (hora_recogida_key)
        REFERENCES public.dim_hora (hora_key) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_hora_solicitud FOREIGN KEY (hora_solicitud_key)
        REFERENCES public.dim_hora (hora_key) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_mensajero FOREIGN KEY (mensajero_key)
        REFERENCES public.dim_mensajero (mensajero_key) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_sede FOREIGN KEY (sede_key)
        REFERENCES public.dim_sede (sede_key) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_tipo_servicio FOREIGN KEY (tipo_servicio_key)
        REFERENCES public.dim_tipo_servicio (tipo_servicio_key) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.fact_servicio
    OWNER to postgres;