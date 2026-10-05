"""
Validador do Data Warehouse (star schema em dw_techpop) e do projeto.

O que ele faz (tudo SOMENTE LEITURA: a sessao e aberta como read-only):
  1. Projeto   : arquivos obrigatorios, .env, dependencias, cobertura do ETL, segredos expostos.
  2. Estrutura : schema, tabelas, colunas/tipos, PKs e FKs do DW (contra o que o 004 define).
  3. Volumetria: linhas do DW x linhas esperadas da origem (public).
  4. Integridade: chaves nulas/orfas na fato, dominio de regiao.
  5. Dimensoes : conteudo de cada dimensao recalculado a partir da origem.
  6. Fato      : conferencia linha a linha contra a origem (ETL independente).
  7. Regras    : line_total = qty * unit_price, commission_total, quantidades, comissao 0-100.
  8. Totais    : somas de quantidade/receita/comissao origem x DW.
  9. Dashboard : executa cada consulta de sql/005 e confere consistencia entre elas.
 10. Origem    : problemas de qualidade nos dados de origem (avisos, nao sao falha do ETL).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import asdict, dataclass
from decimal import Decimal
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
SQL_DIR = BASE_DIR / "sql"
SRC_DIR = BASE_DIR / "src"

PASS, FAIL, WARN, INFO, SKIP = "PASS", "FAIL", "WARN", "INFO", "SKIP"

SOURCE_TABLES = [
    "categories",
    "customers",
    "sellers",
    "suppliers",
    "products",
    "sales",
    "sales_items",
]
ENV_KEYS = ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD", "DB_SSLMODE"]

# Regra de regiao do ETL (src/etl_pyspark.py -> regiao()). Mantida aqui como "contrato".
REGION_MAP = {"WEST": ("CA", "WA"), "NORTHEAST": ("NY",), "SOUTH": ("TX", "FL")}
REGION_DEFAULT = "OTHER"
VALID_REGIONS = list(REGION_MAP) + [REGION_DEFAULT]

# Tipos esperados: (udt_name, extra) -> extra = tamanho (varchar) ou (precisao, escala) (numeric)
I4 = ("int4", None)
BOOL = ("bool", None)
DATE = ("date", None)


def vc(n):
    return ("varchar", n)


def num(p, s):
    return ("numeric", (p, s))


DW_SPEC = {
    "dim_date": {
        "cols": {
            "date_key": I4,
            "full_date": DATE,
            "year": I4,
            "quarter": I4,
            "month": I4,
            "day": I4,
            "day_of_week": I4,
            "is_weekend": BOOL,
        },
        "pk": ["date_key"],
        "not_null": [
            "date_key",
            "full_date",
            "year",
            "quarter",
            "month",
            "day",
            "day_of_week",
            "is_weekend",
        ],
    },
    "dim_customer": {
        "cols": {
            "customer_id": I4,
            "customer_name": vc(100),
            "email": vc(100),
            "state": vc(2),
            "region": vc(30),
        },
        "pk": ["customer_id"],
        "not_null": ["customer_id"],
    },
    "dim_seller": {
        "cols": {
            "seller_id": I4,
            "seller_name": vc(100),
            "email": vc(100),
            "state": vc(2),
            "region": vc(30),
            "tx_commission": I4,
        },
        "pk": ["seller_id"],
        "not_null": ["seller_id"],
    },
    "dim_product": {
        "cols": {
            "product_id": I4,
            "product_name": vc(100),
            "list_price": num(12, 2),
            "category_id": I4,
            "category_name": vc(100),
            "supplier_id": I4,
            "supplier_name": vc(100),
            "supplier_state": vc(50),
            "supplier_region": vc(30),
        },
        "pk": ["product_id"],
        "not_null": ["product_id"],
    },
    "fact_sales_items": {
        "cols": {
            "sales_id": I4,
            "item_id": I4,
            "date_key": I4,
            "customer_id": I4,
            "seller_id": I4,
            "product_id": I4,
            "quantity": I4,
            "unit_price": num(12, 2),
            "line_total": num(14, 2),
            "commission_total": num(14, 2),
        },
        "pk": ["sales_id", "item_id"],
        "not_null": ["sales_id", "item_id"],
    },
}

# (tabela, coluna, tabela_referenciada, coluna_referenciada)
DW_FKS = [
    ("fact_sales_items", "date_key", "dim_date", "date_key"),
    ("fact_sales_items", "customer_id", "dim_customer", "customer_id"),
    ("fact_sales_items", "seller_id", "dim_seller", "seller_id"),
    ("fact_sales_items", "product_id", "dim_product", "product_id"),
]

IDENT_RE = re.compile(r"^[a-z_][a-z0-9_]*$")


# --------------------------------------------------------------------------- infraestrutura


@dataclass
class Result:
    category: str
    name: str
    status: str
    detail: str = ""


class Ctx:
    def __init__(self, dw: str, src: str, dashboard_sql: Path):
        self.dw = dw
        self.src = src
        self.dashboard_sql = dashboard_sql
        self.conn = None
        self.results: list[Result] = []
        self.ready = False  # True quando origem e DW tem todas as tabelas

    def add(self, category, name, status, detail=""):
        self.results.append(Result(category, name, status, str(detail)))

    def q(self, sql, params=None):
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()

    def scalar(self, sql, params=None):
        rows = self.q(sql, params)
        return rows[0][0] if rows else None

    def zero_check(
        self, cat, name, sql, fail_detail, bad_status=FAIL, ok_detail="0 ocorrencias"
    ):
        """PASS se a consulta retornar 0; senao bad_status com a contagem."""
        n = self.scalar(sql) or 0
        if n == 0:
            self.add(cat, name, PASS, ok_detail)
        else:
            self.add(cat, name, bad_status, f"{n} {fail_detail}")
        return n


def fmt_rows(rows, limit=5):
    out = []
    for r in rows[:limit]:
        out.append("(" + ", ".join("NULL" if v is None else str(v) for v in r) + ")")
    return "; ".join(out)


def region_sql(col):
    whens = " ".join(
        f"WHEN {col} IN ({', '.join(repr(s) for s in states)}) THEN '{region}'"
        for region, states in REGION_MAP.items()
    )
    return f"CASE {whens} ELSE '{REGION_DEFAULT}' END"


def money_diff(a, b, tol="0.01"):
    """Expressao SQL: verdadeira quando a e b diferem (NULL x valor, ou |a-b| > tol)."""
    return f"(({a} IS NULL) <> ({b} IS NULL) OR abs({a} - {b}) > {tol})"


# --------------------------------------------------------------------------- 1. projeto (offline)


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace") if path.exists() else ""


def parse_env(path: Path):
    """Retorna lista de (linha, chave, valor) ignorando comentarios."""
    items = []
    for i, line in enumerate(read_text(path).splitlines(), 1):
        m = re.match(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$", line)
        if m and not line.lstrip().startswith("#"):
            items.append((i, m.group(1), m.group(2).strip().strip('"').strip("'")))
    return items


def check_project(ctx: Ctx):
    cat = "1. Projeto"

    # arquivos obrigatorios
    required = [
        "sql/001_create_tables.sql",
        "sql/002_insert_data.sql",
        "sql/003_create_dw.sql",
        "sql/004_create_dw_entrega.sql",
        "sql/005_dashboard_queries.sql",
        "src/etl_pyspark.py",
        "src/init_db.py",
        "requirements.txt",
        ".env.example",
        ".gitignore",
        "README.md",
    ]
    missing = [f for f in required if not (BASE_DIR / f).exists()]
    if missing:
        ctx.add(cat, "Arquivos obrigatorios", FAIL, "faltando: " + ", ".join(missing))
    else:
        ctx.add(
            cat, "Arquivos obrigatorios", PASS, f"{len(required)} arquivos presentes"
        )

    # ETL
    etl_path = SRC_DIR / "etl_pyspark.py"
    etl = read_text(etl_path)
    if etl:
        jar = re.search(r'"(postgresql[-\w.]*\.jar)"', etl)
        if jar and (BASE_DIR / "jars" / jar.group(1)).exists():
            ctx.add(cat, "Driver JDBC referenciado no ETL", PASS, jar.group(1))
        else:
            ctx.add(
                cat,
                "Driver JDBC referenciado no ETL",
                FAIL,
                f"jar nao encontrado em jars/: {jar.group(1) if jar else 'referencia nao localizada'}",
            )

        read_tables = set(
            re.findall(r'ler_tabela\(\s*spark\s*,\s*"public\.(\w+)"', etl)
        )
        gap = [t for t in SOURCE_TABLES if t not in read_tables]
        ctx.add(
            cat,
            "ETL le todas as tabelas de origem",
            FAIL if gap else PASS,
            (
                ("nao lidas: " + ", ".join(gap))
                if gap
                else f"{len(SOURCE_TABLES)} tabelas"
            ),
        )

        saved = set(re.findall(r'salvar\(\s*\w+\s*,\s*"(\w+)"', etl))
        gap = [t for t in DW_SPEC if t not in saved]
        extra = [t for t in saved if t not in DW_SPEC]
        if gap or extra:
            ctx.add(
                cat,
                "ETL grava todas as tabelas do DW",
                FAIL,
                f"sem salvar(): {gap or '-'} | desconhecidas: {extra or '-'}",
            )
        else:
            ctx.add(
                cat, "ETL grava todas as tabelas do DW", PASS, f"{len(saved)} tabelas"
            )

        i = etl.find("TRUNCATE")
        window = etl[i : i + 500] if i >= 0 else ""
        gap = [t for t in DW_SPEC if t not in window]
        if i < 0:
            ctx.add(
                cat,
                "ETL limpa o DW antes de carregar (re-execucao)",
                WARN,
                "nenhum TRUNCATE: rodar o ETL 2x duplicaria dados / violaria PK",
            )
        else:
            ctx.add(
                cat,
                "ETL limpa o DW antes de carregar (re-execucao)",
                FAIL if gap else PASS,
                (
                    ("TRUNCATE nao cobre: " + ", ".join(gap))
                    if gap
                    else "TRUNCATE cobre as 5 tabelas"
                ),
            )

        m = re.search(r'DW_SCHEMA\s*=\s*"(\w+)"', etl)
        if m and m.group(1) != ctx.dw:
            ctx.add(
                cat,
                "Schema do ETL x schema validado",
                WARN,
                f"ETL usa '{m.group(1)}', validador usa '{ctx.dw}'",
            )

        m = re.search(r'SQL_FILE\s*=\s*BASE_DIR\s*/\s*"sql"\s*/\s*"([^"]+)"', etl)
        if m:
            ddl = read_text(SQL_DIR / m.group(1))
            found = set(re.findall(r"CREATE TABLE IF NOT EXISTS\s+(\w+)\.(\w+)", ddl))
            tables = {t for _, t in found}
            schemas = {s for s, _ in found}
            ok = tables == set(DW_SPEC) and schemas == {ctx.dw}
            ctx.add(
                cat,
                f"DDL do ETL ({m.group(1)}) declara o DW esperado",
                PASS if ok else FAIL,
                (
                    f"{len(tables)} tabelas no schema {sorted(schemas)}"
                    if ok
                    else f"tabelas={sorted(tables)} schemas={sorted(schemas)}"
                ),
            )

    # requirements
    req = read_text(BASE_DIR / "requirements.txt")
    lines = [l.strip() for l in req.splitlines() if l.strip() and not l.startswith("#")]
    unpinned = [l for l in lines if "==" not in l]
    names = {re.split(r"[=<>~!]", l)[0].lower() for l in lines}
    need = {"pyspark", "psycopg2-binary", "python-dotenv"}
    if need - names:
        ctx.add(
            cat,
            "requirements.txt",
            FAIL,
            "faltando: " + ", ".join(sorted(need - names)),
        )
    elif unpinned:
        ctx.add(
            cat, "requirements.txt", WARN, "sem versao fixada: " + ", ".join(unpinned)
        )
    else:
        ctx.add(cat, "requirements.txt", PASS, f"{len(lines)} dependencias fixadas")

    # .env / .env.example
    ex_keys = {k for _, k, _ in parse_env(BASE_DIR / ".env.example")}
    gap = [k for k in ENV_KEYS if k not in ex_keys]
    ctx.add(
        cat,
        ".env.example documenta todas as variaveis",
        FAIL if gap else PASS,
        ("faltando: " + ", ".join(gap)) if gap else ", ".join(ENV_KEYS),
    )

    env_path = BASE_DIR / ".env"
    if not env_path.exists():
        ctx.add(cat, ".env", WARN, "nao existe (necessario para a validacao online)")
    else:
        items = parse_env(env_path)
        values = {}
        for _, k, v in items:
            values[k] = v
        empty = [k for k in ENV_KEYS if k != "DB_SSLMODE" and not values.get(k)]
        ctx.add(
            cat,
            ".env preenchido",
            FAIL if empty else PASS,
            (
                ("vazias/ausentes: " + ", ".join(empty))
                if empty
                else "todas as variaveis obrigatorias"
            ),
        )
        seen = {}
        for _, k, v in items:
            seen.setdefault(k, []).append(v)
        dups = {k: vs for k, vs in seen.items() if len(vs) > 1}
        if dups:
            diff = [k for k, vs in dups.items() if len(set(vs)) > 1]
            ctx.add(
                cat,
                ".env sem chaves duplicadas",
                WARN,
                "duplicadas: "
                + ", ".join(dups)
                + (
                    " (com VALORES DIFERENTES: " + ", ".join(diff) + ")"
                    if diff
                    else " (valores iguais; vale a ultima)"
                ),
            )

    gi = read_text(BASE_DIR / ".gitignore")
    ignored = any(l.strip() in (".env", "*.env") for l in gi.splitlines())
    ctx.add(
        cat,
        ".gitignore protege o .env",
        PASS if ignored else FAIL,
        "" if ignored else ".env nao esta ignorado",
    )

    # segredos em texto
    cred = re.compile(r"\b[a-zA-Z][a-zA-Z0-9+.-]*://[^\s:/@]+:[^\s@]+@")
    candidates = [BASE_DIR / ".env", BASE_DIR / ".env.example", BASE_DIR / "README.md"]
    candidates += list(SQL_DIR.glob("*.sql")) + list(SRC_DIR.glob("*.py"))
    hits = []
    for p in candidates:
        for n, line in enumerate(read_text(p).splitlines(), 1):
            if cred.search(line):
                hits.append((p.relative_to(BASE_DIR).as_posix(), n))
    if not hits:
        ctx.add(cat, "Credenciais em texto (URI com senha)", PASS, "nenhuma encontrada")
    else:
        only_env = all(f == ".env" for f, _ in hits)
        where = ", ".join(f"{f}:{n}" for f, n in hits)
        ctx.add(
            cat,
            "Credenciais em texto (URI com senha)",
            WARN if only_env else FAIL,
            f"URI com senha em {where}. "
            + (
                "Esta so no .env (ignorado pelo git), mas vaza se o projeto for zipado/compartilhado: "
                "remova a linha e troque a senha no Aiven."
                if only_env
                else "Remova e troque a senha."
            ),
        )

    # documentacao / idempotencia do init_db
    readme = read_text(BASE_DIR / "README.md")
    if "etl_pyspark" not in readme:
        ctx.add(
            cat,
            "README documenta o ETL",
            WARN,
            "etl_pyspark.py e os scripts 003-005 nao aparecem no README",
        )
    else:
        ctx.add(cat, "README documenta o ETL", PASS)

    init_db = read_text(SRC_DIR / "init_db.py")
    if 'glob("*.sql")' in init_db:
        risky = []
        if re.search(
            r"CREATE TABLE\s+(?!IF NOT EXISTS)",
            read_text(SQL_DIR / "001_create_tables.sql"),
        ):
            risky.append("001 (CREATE TABLE sem IF NOT EXISTS)")
        if "INSERT INTO" in read_text(SQL_DIR / "002_insert_data.sql"):
            risky.append("002 (INSERTs duplicariam dados)")
        if risky:
            ctx.add(
                cat,
                "init_db.py e idempotente",
                WARN,
                "sem argumentos roda TODOS os .sql; nao e seguro re-executar: "
                + "; ".join(risky),
            )


# --------------------------------------------------------------------------- conexao


def connect(ctx: Ctx):
    cat = "Conexao"
    try:
        import psycopg2
    except ImportError:
        ctx.add(cat, "Dependencia psycopg2", FAIL, "pip install -r requirements.txt")
        return False
    try:
        from dotenv import load_dotenv

        load_dotenv(BASE_DIR / ".env")
    except ImportError:
        pass
    missing = [
        k for k in ENV_KEYS if k != "DB_SSLMODE" and not os.environ.get(k, "").strip()
    ]
    if missing:
        ctx.add(cat, "Variaveis de ambiente", FAIL, "ausentes: " + ", ".join(missing))
        return False
    try:
        ctx.conn = psycopg2.connect(
            host=os.environ["DB_HOST"].strip(),
            port=os.environ["DB_PORT"].strip(),
            dbname=os.environ["DB_NAME"].strip(),
            user=os.environ["DB_USER"].strip(),
            password=os.environ["DB_PASSWORD"].strip(),
            sslmode=os.environ.get("DB_SSLMODE", "require").strip(),
            connect_timeout=15,
        )
        ctx.conn.set_session(
            readonly=True, autocommit=True
        )  # garante que nada sera alterado
    except Exception as e:  # noqa: BLE001
        ctx.add(cat, "Conexao com o banco", FAIL, str(e).strip().splitlines()[0])
        return False
    ctx.add(
        cat,
        "Conexao com o banco (sessao somente leitura)",
        PASS,
        os.environ["DB_NAME"].strip(),
    )
    return True


# --------------------------------------------------------------------------- 2. estrutura


def check_structure(ctx: Ctx):
    cat = "2. Estrutura"
    S, D = ctx.src, ctx.dw

    existing = {}
    for schema in (S, D):
        existing[schema] = {
            r[0]
            for r in ctx.q(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = %s",
                (schema,),
            )
        }

    gap = [t for t in SOURCE_TABLES if t not in existing[S]]
    ctx.add(
        cat,
        f"Tabelas de origem em {S}",
        FAIL if gap else PASS,
        ("faltando: " + ", ".join(gap)) if gap else f"{len(SOURCE_TABLES)} tabelas",
    )
    gap_dw = [t for t in DW_SPEC if t not in existing[D]]
    ctx.add(
        cat,
        f"Tabelas do DW em {D}",
        FAIL if gap_dw else PASS,
        ("faltando: " + ", ".join(gap_dw)) if gap_dw else f"{len(DW_SPEC)} tabelas",
    )
    ctx.ready = not gap and not gap_dw
    if gap_dw:
        return

    cols = {}
    for t, c, udt, clen, nprec, nscale, nullable in ctx.q(
        """SELECT table_name, column_name, udt_name, character_maximum_length,
                  numeric_precision, numeric_scale, is_nullable
           FROM information_schema.columns WHERE table_schema = %s""",
        (D,),
    ):
        cols[(t, c)] = (udt, clen, nprec, nscale, nullable)

    for table, spec in DW_SPEC.items():
        problems = []
        for col, (udt_exp, extra) in spec["cols"].items():
            got = cols.get((table, col))
            if got is None:
                problems.append(f"coluna ausente: {col}")
                continue
            udt, clen, nprec, nscale, nullable = got
            if udt != udt_exp:
                problems.append(f"{col}: tipo {udt} (esperado {udt_exp})")
            elif udt_exp == "varchar" and clen != extra:
                problems.append(f"{col}: varchar({clen}) (esperado {extra})")
            elif udt_exp == "numeric" and (nprec, nscale) != extra:
                problems.append(f"{col}: numeric({nprec},{nscale}) (esperado {extra})")
            if col in spec["not_null"] and nullable != "NO":
                problems.append(f"{col}: deveria ser NOT NULL")
        extra_cols = sorted({c for (t, c) in cols if t == table} - set(spec["cols"]))
        if problems:
            ctx.add(cat, f"Colunas de {table}", FAIL, "; ".join(problems))
        elif extra_cols:
            ctx.add(
                cat,
                f"Colunas de {table}",
                WARN,
                "colunas nao previstas: " + ", ".join(extra_cols),
            )
        else:
            ctx.add(
                cat,
                f"Colunas de {table}",
                PASS,
                f"{len(spec['cols'])} colunas / tipos OK",
            )

    pks = {}
    for t, c in ctx.q(
        """SELECT tc.table_name, kcu.column_name
           FROM information_schema.table_constraints tc
           JOIN information_schema.key_column_usage kcu
             ON kcu.constraint_name = tc.constraint_name
            AND kcu.table_schema = tc.table_schema AND kcu.table_name = tc.table_name
           WHERE tc.table_schema = %s AND tc.constraint_type = 'PRIMARY KEY'""",
        (D,),
    ):
        pks.setdefault(t, set()).add(c)
    bad = [
        f"{t}: {sorted(pks.get(t, set())) or 'sem PK'} (esperado {s['pk']})"
        for t, s in DW_SPEC.items()
        if pks.get(t, set()) != set(s["pk"])
    ]
    ctx.add(
        cat,
        "Chaves primarias",
        FAIL if bad else PASS,
        "; ".join(bad) if bad else "5 PKs corretas",
    )

    fks = {
        (t, c, rt, rc)
        for t, c, rt, rc in ctx.q(
            """SELECT cl.relname, a.attname, rcl.relname, af.attname
           FROM pg_constraint c
           JOIN pg_class cl ON cl.oid = c.conrelid
           JOIN pg_class rcl ON rcl.oid = c.confrelid
           JOIN pg_namespace n ON n.oid = cl.relnamespace
           JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = c.conkey[1]
           JOIN pg_attribute af ON af.attrelid = c.confrelid AND af.attnum = c.confkey[1]
           WHERE c.contype = 'f' AND n.nspname = %s""",
            (D,),
        )
    }
    gap = [
        f"{t}.{c} -> {rt}.{rc}" for t, c, rt, rc in DW_FKS if (t, c, rt, rc) not in fks
    ]
    ctx.add(
        cat,
        "Chaves estrangeiras da fato",
        FAIL if gap else PASS,
        ("faltando: " + "; ".join(gap)) if gap else f"{len(DW_FKS)} FKs corretas",
    )

    # schema legado dw (003)
    if (
        D != "dw"
        and ctx.scalar("SELECT to_regclass('dw.fact_sales_items')") is not None
    ):
        n = ctx.scalar("SELECT count(*) FROM dw.fact_sales_items")
        ctx.add(
            cat,
            "Schema legado 'dw' (003_create_dw.sql)",
            INFO,
            f"existe com {n} linhas na fato; o ETL atual so carrega {D}. "
            "Se for apenas rascunho, considere remover para evitar confusao.",
        )


# --------------------------------------------------------------------------- 3. volumetria


def check_volume(ctx: Ctx):
    cat = "3. Volumetria"
    S, D = ctx.src, ctx.dw

    def compare(name, src_sql, dw_table, explain=None):
        exp = ctx.scalar(src_sql)
        got = ctx.scalar(f"SELECT count(*) FROM {D}.{dw_table}")
        if exp == got:
            ctx.add(cat, name, PASS, f"{got} linhas")
        else:
            ctx.add(
                cat,
                name,
                FAIL,
                f"DW={got} esperado={exp}" + (f". {explain}" if explain else ""),
            )
        return exp, got

    compare(
        "dim_customer = customers",
        f"SELECT count(*) FROM {S}.customers",
        "dim_customer",
    )
    compare("dim_seller = sellers", f"SELECT count(*) FROM {S}.sellers", "dim_seller")
    compare(
        "dim_product = products",
        f"SELECT count(*) FROM {S}.products",
        "dim_product",
        "o ETL usa INNER JOIN com categories/suppliers: produto sem categoria/fornecedor e descartado",
    )
    compare(
        "dim_date = datas distintas de sales",
        f'SELECT count(DISTINCT "date") FROM {S}.sales',
        "dim_date",
    )
    compare(
        "fact_sales_items = sales_items",
        f"SELECT count(*) FROM {S}.sales_items",
        "fact_sales_items",
        "o ETL usa INNER JOIN com sales/sellers: item de venda sem vendedor e descartado",
    )

    n = ctx.scalar(
        f"SELECT count(*) FROM {S}.sales_items i LEFT JOIN {S}.sales s ON s.sales_id = i.sales_id "
        f"LEFT JOIN {S}.sellers l ON l.seller_id = s.seller_id WHERE l.seller_id IS NULL"
    )
    if n:
        ctx.add(
            cat,
            "Itens sem venda/vendedor na origem",
            FAIL,
            f"{n} itens nao chegam a fato",
        )


# --------------------------------------------------------------------------- 4. integridade


def check_integrity(ctx: Ctx):
    cat = "4. Integridade"
    D = ctx.dw
    F = f"{D}.fact_sales_items"

    for col in ("date_key", "customer_id", "seller_id", "product_id"):
        ctx.zero_check(
            cat,
            f"fato.{col} sem NULL",
            f"SELECT count(*) FROM {F} WHERE {col} IS NULL",
            "linhas com chave nula",
        )
    for col, dim in (
        ("date_key", "dim_date"),
        ("customer_id", "dim_customer"),
        ("seller_id", "dim_seller"),
        ("product_id", "dim_product"),
    ):
        ctx.zero_check(
            cat,
            f"fato.{col} sem orfaos em {dim}",
            f"SELECT count(*) FROM {F} f WHERE f.{col} IS NOT NULL AND NOT EXISTS "
            f"(SELECT 1 FROM {D}.{dim} d WHERE d.{col} = f.{col})",
            "linhas orfas",
        )

    for table, col in (
        ("dim_customer", "region"),
        ("dim_seller", "region"),
        ("dim_product", "supplier_region"),
    ):
        regions = ", ".join(f"'{r}'" for r in VALID_REGIONS)
        ctx.zero_check(
            cat,
            f"{table}.{col} com dominio valido",
            f"SELECT count(*) FROM {D}.{table} WHERE {col} IS NULL OR {col} NOT IN ({regions})",
            f"linhas com regiao invalida (validas: {', '.join(VALID_REGIONS)})",
        )
        ctx.zero_check(
            cat,
            f"{table}.{col} sem 'OTHER'",
            f"SELECT count(*) FROM {D}.{table} WHERE {col} = '{REGION_DEFAULT}'",
            "linhas com estado nao mapeado (caem em OTHER)",
            bad_status=WARN,
        )

    for table, key, label in (
        ("dim_customer", "customer_id", "clientes"),
        ("dim_seller", "seller_id", "vendedores"),
        ("dim_product", "product_id", "produtos"),
    ):
        n = ctx.scalar(
            f"SELECT count(*) FROM {D}.{table} d WHERE NOT EXISTS "
            f"(SELECT 1 FROM {F} f WHERE f.{key} = d.{key})"
        )
        ctx.add(
            cat,
            f"{label.capitalize()} sem nenhuma venda",
            INFO if n else PASS,
            f"{n} {label} nunca aparecem na fato" if n else "todos tem vendas",
        )

    for table in ("dim_customer", "dim_seller"):
        ctx.zero_check(
            cat,
            f"{table} sem nome/email/estado nulos",
            f"SELECT count(*) FROM {D}.{table} WHERE state IS NULL OR "
            f"{table.split('_')[1]}_name IS NULL OR email IS NULL",
            "linhas com campo descritivo nulo",
            bad_status=WARN,
        )


# --------------------------------------------------------------------------- 5. dimensoes


def diff_check(ctx: Ctx, cat, name, src_sql, dw_sql):
    """Compara conjuntos (EXCEPT nos dois sentidos); NULLs sao tratados como iguais."""
    missing = ctx.scalar(f"SELECT count(*) FROM (({src_sql}) EXCEPT ({dw_sql})) x") or 0
    extra = ctx.scalar(f"SELECT count(*) FROM (({dw_sql}) EXCEPT ({src_sql})) x") or 0
    if missing == 0 and extra == 0:
        ctx.add(cat, name, PASS, "identica ao esperado a partir da origem")
        return
    parts = [
        f"{missing} linhas esperadas ausentes/diferentes no DW",
        f"{extra} linhas no DW que nao deveriam existir",
    ]
    sample = ""
    if missing:
        sample += " | esperado e nao encontrado: " + fmt_rows(
            ctx.q(f"({src_sql}) EXCEPT ({dw_sql}) LIMIT 3")
        )
    if extra:
        sample += " | encontrado e inesperado: " + fmt_rows(
            ctx.q(f"({dw_sql}) EXCEPT ({src_sql}) LIMIT 3")
        )
    ctx.add(cat, name, FAIL, "; ".join(parts) + sample)


def check_dimensions(ctx: Ctx):
    cat = "5. Dimensoes"
    S, D = ctx.src, ctx.dw

    diff_check(
        ctx,
        cat,
        "dim_customer (upper, regiao)",
        f"SELECT customer_id, upper(customer_name), upper(email), state, {region_sql('state')} "
        f"FROM {S}.customers",
        f"SELECT customer_id, customer_name, email, state, region FROM {D}.dim_customer",
    )

    diff_check(
        ctx,
        cat,
        "dim_seller (upper, regiao, comissao)",
        f"SELECT seller_id, upper(seller_name), upper(email), state, {region_sql('state')}, tx_commission "
        f"FROM {S}.sellers",
        f"SELECT seller_id, seller_name, email, state, region, tx_commission FROM {D}.dim_seller",
    )

    diff_check(
        ctx,
        cat,
        "dim_product (categoria, fornecedor, regiao)",
        f"SELECT p.product_id, upper(p.product_name), round(p.price, 2), p.category_id, "
        f"upper(c.category_name), p.supplier_id, upper(s.supplier_name), s.state, {region_sql('s.state')} "
        f"FROM {S}.products p JOIN {S}.categories c ON c.category_id = p.category_id "
        f"JOIN {S}.suppliers s ON s.supplier_id = p.supplier_id",
        f"SELECT product_id, product_name, list_price, category_id, category_name, supplier_id, "
        f"supplier_name, supplier_state, supplier_region FROM {D}.dim_product",
    )

    # dim_date: day_of_week no padrao do Spark (1 = domingo ... 7 = sabado) = dow do Postgres + 1
    diff_check(
        ctx,
        cat,
        "dim_date (ano, trimestre, mes, dia, dia da semana, fim de semana)",
        f"SELECT DISTINCT to_char(d, 'YYYYMMDD')::int, d, EXTRACT(year FROM d)::int, "
        f"EXTRACT(quarter FROM d)::int, EXTRACT(month FROM d)::int, EXTRACT(day FROM d)::int, "
        f"EXTRACT(dow FROM d)::int + 1, EXTRACT(dow FROM d)::int IN (0, 6) "
        f'FROM (SELECT "date" AS d FROM {S}.sales WHERE "date" IS NOT NULL) t',
        f"SELECT date_key, full_date, year, quarter, month, day, day_of_week, is_weekend "
        f"FROM {D}.dim_date",
    )


# --------------------------------------------------------------------------- 6. fato

FACT_FLAGS = {
    "date_key": "s.date_key IS DISTINCT FROM d.date_key",
    "customer_id": "s.customer_id IS DISTINCT FROM d.customer_id",
    "seller_id": "s.seller_id IS DISTINCT FROM d.seller_id",
    "product_id": "s.product_id IS DISTINCT FROM d.product_id",
    "quantity": "s.quantity IS DISTINCT FROM d.quantity",
    "unit_price": money_diff("s.unit_price", "d.unit_price"),
    "line_total": money_diff("s.line_total", "d.line_total"),
    "commission_total": money_diff("s.commission_total", "d.commission_total"),
}


def check_fact(ctx: Ctx):
    cat = "6. Fato"
    S, D = ctx.src, ctx.dw
    src = f"""
        SELECT s.sales_id, i.item_id,
               to_char(s."date", 'YYYYMMDD')::int AS date_key,
               s.customer_id, s.seller_id, i.product_id, i.quantity,
               round(i.price, 2) AS unit_price,
               round(i.quantity * i.price, 2) AS line_total,
               round(i.quantity * i.price * l.tx_commission / 100, 2) AS commission_total
        FROM {S}.sales_items i
        JOIN {S}.sales s ON s.sales_id = i.sales_id
        JOIN {S}.sellers l ON l.seller_id = s.seller_id"""
    flag_cols = ", ".join(f"({expr}) AS f_{name}" for name, expr in FACT_FLAGS.items())
    cte = f"""
        WITH src AS ({src}),
        j AS (
            SELECT s.sales_id AS s_id, s.item_id AS s_item, d.sales_id AS d_id, d.item_id AS d_item, {flag_cols}
            FROM src s
            FULL OUTER JOIN {D}.fact_sales_items d ON d.sales_id = s.sales_id AND d.item_id = s.item_id
        )"""
    both = "s_id IS NOT NULL AND d_id IS NOT NULL"
    counts = ", ".join(f"count(*) FILTER (WHERE {both} AND f_{n})" for n in FACT_FLAGS)
    row = ctx.q(
        f"{cte} SELECT count(*) FILTER (WHERE d_id IS NULL), count(*) FILTER (WHERE s_id IS NULL), "
        f"{counts} FROM j"
    )[0]
    missing, extra, per_col = row[0], row[1], dict(zip(FACT_FLAGS, row[2:]))

    any_bad = " OR ".join(
        ["d_id IS NULL", "s_id IS NULL"] + [f"({both} AND f_{n})" for n in FACT_FLAGS]
    )
    sample_sql = f"{cte} SELECT coalesce(s_id, d_id), coalesce(s_item, d_item) FROM j WHERE {any_bad} LIMIT 5"

    ctx.add(
        cat,
        "Itens da origem presentes na fato",
        FAIL if missing else PASS,
        (
            f"{missing} itens esperados nao estao na fato"
            if missing
            else "nenhum item perdido"
        ),
    )
    ctx.add(
        cat,
        "Fato sem linhas fantasma",
        FAIL if extra else PASS,
        (
            f"{extra} linhas na fato sem correspondente na origem"
            if extra
            else "nenhuma linha extra"
        ),
    )
    bad_cols = {c: n for c, n in per_col.items() if n}
    if bad_cols:
        keys = fmt_rows(ctx.q(sample_sql))
        for c, n in bad_cols.items():
            ctx.add(
                cat,
                f"fato.{c} confere com a origem",
                FAIL,
                f"{n} linhas divergentes (ex. sales_id,item_id: {keys})",
            )
    for c in per_col:
        if c not in bad_cols:
            ctx.add(cat, f"fato.{c} confere com a origem", PASS, "todas as linhas")


# --------------------------------------------------------------------------- 7. regras de negocio


def check_rules(ctx: Ctx):
    cat = "7. Regras de negocio"
    D = ctx.dw
    F = f"{D}.fact_sales_items"

    ctx.zero_check(
        cat,
        "quantity > 0",
        f"SELECT count(*) FROM {F} WHERE quantity IS NULL OR quantity <= 0",
        "linhas com quantidade nula/zero/negativa",
    )
    ctx.zero_check(
        cat,
        "unit_price >= 0",
        f"SELECT count(*) FROM {F} WHERE unit_price IS NULL OR unit_price < 0",
        "linhas com preco nulo/negativo",
    )
    ctx.zero_check(
        cat,
        "line_total = quantity * unit_price",
        f"SELECT count(*) FROM {F} WHERE "
        + money_diff("line_total", "round(quantity * unit_price, 2)"),
        "linhas com line_total incoerente",
    )
    ctx.zero_check(
        cat,
        "tx_commission entre 0 e 100",
        f"SELECT count(*) FROM {D}.dim_seller WHERE tx_commission IS NULL OR tx_commission NOT BETWEEN 0 AND 100",
        "vendedores com comissao nula ou fora de 0-100",
    )
    ctx.zero_check(
        cat,
        "commission_total = line_total * tx_commission / 100",
        f"SELECT count(*) FROM {F} f JOIN {D}.dim_seller s ON s.seller_id = f.seller_id WHERE "
        + money_diff(
            "f.commission_total", "round(f.line_total * s.tx_commission / 100, 2)"
        ),
        "linhas com comissao incoerente",
    )


# --------------------------------------------------------------------------- 8. totais


def check_totals(ctx: Ctx):
    cat = "8. Totais origem x DW"
    S, D = ctx.src, ctx.dw
    tol = Decimal("0.01")

    def cmp(name, src_sql, dw_sql):
        a = ctx.scalar(src_sql) or 0
        b = ctx.scalar(dw_sql) or 0
        a, b = Decimal(a), Decimal(b)
        if abs(a - b) <= tol:
            ctx.add(cat, name, PASS, f"{b}")
        else:
            ctx.add(cat, name, FAIL, f"origem={a} DW={b} diferenca={b - a}")

    cmp(
        "Quantidade total vendida",
        f"SELECT sum(quantity) FROM {S}.sales_items",
        f"SELECT sum(quantity) FROM {D}.fact_sales_items",
    )
    cmp(
        "Receita total (quantity * price)",
        f"SELECT sum(quantity * price) FROM {S}.sales_items",
        f"SELECT sum(line_total) FROM {D}.fact_sales_items",
    )
    cmp(
        "Comissao total",
        f"SELECT sum(i.quantity * i.price * l.tx_commission / 100) FROM {S}.sales_items i "
        f"JOIN {S}.sales s ON s.sales_id = i.sales_id JOIN {S}.sellers l ON l.seller_id = s.seller_id",
        f"SELECT sum(commission_total) FROM {D}.fact_sales_items",
    )
    cmp(
        "Vendas distintas",
        f"SELECT count(DISTINCT sales_id) FROM {S}.sales_items",
        f"SELECT count(DISTINCT sales_id) FROM {D}.fact_sales_items",
    )


# --------------------------------------------------------------------------- 9. dashboard


def check_dashboard(ctx: Ctx):
    cat = "9. Dashboard (sql/005)"
    D = ctx.dw
    F = f"{D}.fact_sales_items"

    total = Decimal(ctx.scalar(f"SELECT coalesce(sum(line_total), 0) FROM {F}"))
    for dim, key in (
        ("dim_product", "product_id"),
        ("dim_date", "date_key"),
        ("dim_customer", "customer_id"),
        ("dim_seller", "seller_id"),
    ):
        via = Decimal(
            ctx.scalar(
                f"SELECT coalesce(sum(f.line_total), 0) FROM {F} f "
                f"JOIN {D}.{dim} d ON d.{key} = f.{key}"
            )
        )
        ctx.add(
            cat,
            f"Receita agrupada via {dim} = receita total",
            PASS if via == total else FAIL,
            f"{via}" if via == total else f"via join={via} total={total}",
        )
    com = Decimal(ctx.scalar(f"SELECT coalesce(sum(commission_total), 0) FROM {F}"))
    com_via = Decimal(
        ctx.scalar(
            f"SELECT coalesce(sum(f.commission_total), 0) FROM {F} f "
            f"JOIN {D}.dim_seller s ON s.seller_id = f.seller_id"
        )
    )
    ctx.add(
        cat,
        "Comissao agrupada por vendedor = comissao total",
        PASS if com == com_via else FAIL,
        f"{com_via}" if com == com_via else f"via join={com_via} total={com}",
    )

    if not ctx.dashboard_sql.exists():
        ctx.add(
            cat, "Arquivo de consultas", FAIL, f"{ctx.dashboard_sql} nao encontrado"
        )
        return
    text = "\n".join(
        l
        for l in ctx.dashboard_sql.read_text(encoding="utf-8").splitlines()
        if not l.strip().startswith("--")
    )
    text = text.replace("dw_techpop.", f"{D}.")
    statements = [s.strip() for s in text.split(";") if s.strip()]
    if not statements:
        ctx.add(cat, "Arquivo de consultas", FAIL, "nenhuma consulta encontrada")
        return
    for n, stmt in enumerate(statements, 1):
        label = (
            f"Consulta {n}/{len(statements)}: " + re.sub(r"\s+", " ", stmt)[:60] + "..."
        )
        try:
            rows = ctx.q(stmt)
        except Exception as e:  # noqa: BLE001
            ctx.add(
                cat, label, FAIL, "erro ao executar: " + str(e).strip().splitlines()[0]
            )
            continue
        if not rows:
            ctx.add(
                cat,
                label,
                FAIL,
                "executou mas nao retornou linhas (dashboard ficaria vazio)",
            )
        elif all(v is None for r in rows for v in r):
            ctx.add(cat, label, FAIL, "retornou apenas NULL")
        else:
            ctx.add(cat, label, PASS, f"{len(rows)} linhas")


# --------------------------------------------------------------------------- 10. qualidade da origem


def check_source_quality(ctx: Ctx):
    cat = "10. Qualidade da origem"
    S = ctx.src

    n_sales = ctx.scalar(f"SELECT count(*) FROM {S}.sales")
    n = ctx.scalar(
        f"""SELECT count(*) FROM {S}.sales s
                       JOIN (SELECT sales_id, sum(quantity * price) AS t FROM {S}.sales_items GROUP BY sales_id) i
                         ON i.sales_id = s.sales_id
                       WHERE abs(coalesce(s.total_price, 0) - i.t) > 0.01"""
    )
    ctx.add(
        cat,
        "sales.total_price = soma dos itens",
        WARN if n else PASS,
        (
            f"{n} de {n_sales} vendas divergem. O DW ignora sales.total_price (usa quantity*price, "
            f"decisao documentada no 003); so confirme que isso e intencional."
            if n
            else "todas conferem"
        ),
    )

    n = ctx.scalar(
        f"SELECT count(*) FROM {S}.sales_items i JOIN {S}.products p ON p.product_id = i.product_id "
        f"WHERE i.price <> p.price"
    )
    n_items = ctx.scalar(f"SELECT count(*) FROM {S}.sales_items")
    ctx.add(
        cat,
        "Preco do item = preco de tabela do produto",
        INFO if n else PASS,
        (
            f"{n} de {n_items} itens com preco diferente do list_price (normal: preco praticado)"
            if n
            else "todos iguais"
        ),
    )

    ctx.zero_check(
        cat,
        "Vendas sem itens",
        f"SELECT count(*) FROM {S}.sales s WHERE NOT EXISTS (SELECT 1 FROM {S}.sales_items i WHERE i.sales_id = s.sales_id)",
        "vendas sem itens (nao aparecem na fato)",
        bad_status=WARN,
    )
    ctx.zero_check(
        cat,
        "Vendas sem cliente/vendedor/data",
        f'SELECT count(*) FROM {S}.sales WHERE customer_id IS NULL OR seller_id IS NULL OR "date" IS NULL',
        "vendas com chave nula",
        bad_status=WARN,
    )
    ctx.zero_check(
        cat,
        "Itens sem produto/quantidade/preco",
        f"SELECT count(*) FROM {S}.sales_items WHERE product_id IS NULL OR quantity IS NULL OR price IS NULL",
        "itens com campo nulo",
        bad_status=WARN,
    )

    mapped = ", ".join(repr(s) for states in REGION_MAP.values() for s in states)
    rows = ctx.q(
        f"""SELECT DISTINCT st FROM (
                         SELECT state AS st FROM {S}.customers UNION ALL
                         SELECT state FROM {S}.sellers UNION ALL
                         SELECT state FROM {S}.suppliers) u
                     WHERE st IS NULL OR st NOT IN ({mapped})"""
    )
    if rows:
        ctx.add(
            cat,
            "Estados mapeados em regiao",
            WARN,
            "estados sem regiao definida (viram OTHER): "
            + ", ".join(str(r[0]) for r in rows),
        )
    else:
        ctx.add(
            cat,
            "Estados mapeados em regiao",
            PASS,
            "todos os estados da origem tem regiao",
        )


# --------------------------------------------------------------------------- relatorio


def render(results, color):
    tags = {
        PASS: ("[ OK  ]", "32"),
        FAIL: ("[FALHA]", "31"),
        WARN: ("[AVISO]", "33"),
        INFO: ("[INFO ]", "36"),
        SKIP: ("[PULOU]", "90"),
    }

    def paint(text, code):
        return f"\033[{code}m{text}\033[0m" if color else text

    last = None
    for r in results:
        if r.category != last:
            print("\n" + paint(r.category, "1"))
            last = r.category
        tag, code = tags[r.status]
        print(
            f"  {paint(tag, code)} {r.name}" + (f"  -> {r.detail}" if r.detail else "")
        )

    count = {s: sum(1 for r in results if r.status == s) for s in tags}
    print("\n" + "=" * 70)
    print(
        "Resumo: "
        + " | ".join(f"{count[s]} {tags[s][0].strip('[] ').lower()}" for s in tags)
    )
    if count[FAIL]:
        print(paint("RESULTADO: REPROVADO - ha falhas a corrigir.", "31"))
    elif count[WARN]:
        print(paint("RESULTADO: APROVADO COM AVISOS.", "33"))
    else:
        print(paint("RESULTADO: APROVADO.", "32"))
    return count


def run_group(ctx, name, fn):
    try:
        fn(ctx)
    except Exception as e:  # noqa: BLE001
        msg = (getattr(e, "pgerror", None) or str(e)).strip().splitlines()
        ctx.add(
            name,
            "Erro inesperado ao executar o grupo",
            FAIL,
            f"{type(e).__name__}: {msg[0] if msg else ''}",
        )


def main():
    ap = argparse.ArgumentParser(description="Valida o Data Warehouse e o projeto.")
    ap.add_argument(
        "--schema", default="dw_techpop", help="schema do DW (padrao: dw_techpop)"
    )
    ap.add_argument(
        "--source-schema", default="public", help="schema de origem (padrao: public)"
    )
    ap.add_argument(
        "--sql-file",
        default=str(SQL_DIR / "005_dashboard_queries.sql"),
        help="arquivo com as consultas do dashboard",
    )
    ap.add_argument(
        "--offline",
        action="store_true",
        help="roda so os checks do projeto (sem banco)",
    )
    ap.add_argument(
        "--strict", action="store_true", help="avisos tambem reprovam (exit code 1)"
    )
    ap.add_argument("--json", metavar="ARQUIVO", help="grava o resultado em JSON")
    ap.add_argument("--no-color", action="store_true")
    args = ap.parse_args()

    for ident in (args.schema, args.source_schema):
        if not IDENT_RE.match(ident):
            print(f"Nome de schema invalido: {ident!r}")
            return 2

    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass

    ctx = Ctx(args.schema, args.source_schema, Path(args.sql_file))
    run_group(ctx, "1. Projeto", check_project)

    if not args.offline:
        if connect(ctx):
            run_group(ctx, "2. Estrutura", check_structure)
            if ctx.ready:
                for name, fn in (
                    ("3. Volumetria", check_volume),
                    ("4. Integridade", check_integrity),
                    ("5. Dimensoes", check_dimensions),
                    ("6. Fato", check_fact),
                    ("7. Regras de negocio", check_rules),
                    ("8. Totais origem x DW", check_totals),
                    ("9. Dashboard (sql/005)", check_dashboard),
                    ("10. Qualidade da origem", check_source_quality),
                ):
                    run_group(ctx, name, fn)
            else:
                ctx.add(
                    "3-10",
                    "Checks de conteudo",
                    SKIP,
                    "estrutura incompleta: corrija os itens da secao 2 e rode de novo",
                )
            ctx.conn.close()
        else:
            ctx.add("3-10", "Checks de conteudo", SKIP, "sem conexao com o banco")

    color = (
        sys.stdout.isatty()
        and not args.no_color
        and not os.environ.get("NO_COLOR")
        and (os.name != "nt" or bool(os.environ.get("WT_SESSION")))
    )
    print(
        f"Validacao do DW  |  schema={ctx.dw}  origem={ctx.src}  |  modo={'offline' if args.offline else 'completo'}"
    )
    count = render(ctx.results, color)

    if args.json:
        Path(args.json).write_text(
            json.dumps(
                {"summary": count, "results": [asdict(r) for r in ctx.results]},
                ensure_ascii=False,
                indent=2,
                default=str,
            ),
            encoding="utf-8",
        )
        print(f"JSON gravado em {args.json}")

    return 1 if count[FAIL] or (args.strict and count[WARN]) else 0


if __name__ == "__main__":
    sys.exit(main())
