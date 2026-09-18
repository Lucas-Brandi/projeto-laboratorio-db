-- Script de criação de schema do banco.
-- Rode este arquivo contra o banco do Aiven (via psql, DBeaver, ou o script
-- Python init_db.py) sempre que precisar recriar a estrutura do zero.

-- Exemplo de tabela — ajustem para o schema real do trabalho.
CREATE TABLE IF NOT EXISTS exemplo (
    id SERIAL PRIMARY KEY,
    nome VARCHAR(255) NOT NULL,
    criado_em TIMESTAMP NOT NULL DEFAULT NOW()
);
