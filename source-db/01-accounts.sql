-- Только учебные данные. Сначала создаём текущую запись, затем меняем её вручную.
CREATE TABLE public.accounts (
    account_id integer PRIMARY KEY,
    owner_name text NOT NULL,
    status text NOT NULL CHECK (status IN ('active', 'blocked', 'closed')),
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
);
-- Для CDC полезно передавать полную старую запись при UPDATE и DELETE.
ALTER TABLE public.accounts REPLICA IDENTITY FULL;
INSERT INTO public.accounts (account_id, owner_name, status)
VALUES (101, 'Анна Орлова', 'active'), (102, 'Мария Соколова', 'active');
