"""
Pipeline de Precificação por Categoria — ShopBrasil
====================================================

Pipeline Apache Airflow (TaskFlow API) que aposenta o antigo script agendado
via cron. Todos os dias às **06:00 (horário de Brasília)** o pipeline:

1.  **Ingestão**   : busca os produtos na FakeStore API de forma resiliente
    (retry + *exponential backoff*) e valida o schema com um operador
    customizado.
2.  **Análise**    : agrupa os produtos por categoria e calcula as métricas
    (preço médio, mínimo, máximo e quantidade) usando *Dynamic Task Mapping*
    (fan-out), consolidando o resultado em seguida (fan-in).
3.  **Persistência**: grava o snapshot do dia de forma **idempotente** (UPSERT)
    e mantém uma tabela de **histórico** (append) para acompanhar a evolução
    dos preços.

Topologias presentes (todas identificáveis no grafo):
    * **Linear**  : ``criar_tabelas -> buscar_produtos -> validar_produtos``
    * **Fan-out** : ``calcular_metricas.expand(...)`` (uma task por categoria)
    * **Fan-in**  : ``consolidar_metricas`` (junta o resultado de todas elas)
"""
from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
import requests
from airflow.decorators import dag, task, task_group
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

from operators.validar_produtos_operator import ValidarProdutosOperator

log = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Configurações                                                               #
# --------------------------------------------------------------------------- #
FAKESTORE_URL = "https://fakestoreapi.com/products"
POSTGRES_CONN_ID = "postgres_analytics"      # Connection do Airflow (env var)
ECOMMERCE_POOL = "ecommerce_pool"            # Pool com 2 slots (criado no init)
LOCAL_TZ = pendulum.timezone("America/Sao_Paulo")

TABELA_SNAPSHOT = "precos_por_categoria"             # snapshot idempotente
TABELA_HISTORICO = "precos_por_categoria_historico"  # histórico (append)


# --------------------------------------------------------------------------- #
# Callbacks de ciclo de vida (simulam o envio de um alerta)                   #
# --------------------------------------------------------------------------- #
def _descrever(context) -> str:
    ti = context.get("task_instance") or context.get("ti")
    return (
        f"dag={getattr(ti, 'dag_id', '?')} "
        f"task={getattr(ti, 'task_id', '?')} "
        f"run={getattr(ti, 'run_id', '?')} "
        f"tentativa={getattr(ti, 'try_number', '?')}"
    )


def notificar_falha(context) -> None:
    """on_failure_callback — simula o disparo de um alerta (Slack/e-mail/PagerDuty)."""
    log.error("🚨 [ALERTA-FALHA] %s | erro=%s", _descrever(context), context.get("exception"))
    log.error("🚨 [ALERTA-FALHA] >> Aqui notificaríamos o on-call do time de dados.")


def notificar_retry(context) -> None:
    """on_retry_callback — registra que a task entrará em nova tentativa."""
    log.warning("🔁 [RETRY] %s | aguardando o backoff antes de tentar novamente.", _descrever(context))


def notificar_sucesso(context) -> None:
    """on_success_callback — confirma a coleta bem-sucedida."""
    log.info("✅ [SUCESSO] %s | produtos coletados com sucesso.", _descrever(context))


def alerta_sla(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    """sla_miss_callback — simula um alerta quando uma task estoura o SLA."""
    log.error("⏰ [SLA-MISS] dag=%s | tasks que estouraram o SLA=%s", dag.dag_id, task_list)


# --------------------------------------------------------------------------- #
# Definição do DAG (TaskFlow API)                                            #
# --------------------------------------------------------------------------- #
@dag(
    dag_id="ecommerce_pricing_pipeline",
    description="Coleta produtos da FakeStore, calcula métricas por categoria e grava no PostgreSQL.",
    schedule="0 6 * * *",                                  # todo dia às 06:00
    start_date=pendulum.datetime(2024, 1, 1, tz=LOCAL_TZ),  # timezone ancorado
    catchup=False,                                          # sem backfill histórico
    max_active_runs=1,
    default_args={
        "owner": "data-team",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(seconds=30),
    },
    sla_miss_callback=alerta_sla,
    tags=["ecommerce", "pricing", "shopbrasil", "fakestore"],
    doc_md=__doc__,
)
def ecommerce_pricing_pipeline():

    # ===================================================================== #
    # TaskGroup 1 — INGESTÃO                                                 #
    # ===================================================================== #
    @task_group(group_id="ingestao", tooltip="Cria as tabelas, busca e valida os produtos.")
    def ingestao():

        @task
        def criar_tabelas() -> None:
            """Cria (idempotentemente) as tabelas analíticas no PostgreSQL."""
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            hook.run(
                f"""
                CREATE TABLE IF NOT EXISTS {TABELA_SNAPSHOT} (
                    data_referencia DATE          NOT NULL,
                    categoria       TEXT          NOT NULL,
                    qtd_produtos    INTEGER       NOT NULL,
                    preco_medio     NUMERIC(12,2) NOT NULL,
                    preco_minimo    NUMERIC(12,2) NOT NULL,
                    preco_maximo    NUMERIC(12,2) NOT NULL,
                    atualizado_em   TIMESTAMPTZ   NOT NULL DEFAULT now(),
                    PRIMARY KEY (data_referencia, categoria)
                );

                CREATE TABLE IF NOT EXISTS {TABELA_HISTORICO} (
                    id              BIGSERIAL     PRIMARY KEY,
                    run_id          TEXT          NOT NULL,
                    data_referencia DATE          NOT NULL,
                    executado_em    TIMESTAMPTZ   NOT NULL,
                    categoria       TEXT          NOT NULL,
                    qtd_produtos    INTEGER       NOT NULL,
                    preco_medio     NUMERIC(12,2) NOT NULL,
                    preco_minimo    NUMERIC(12,2) NOT NULL,
                    preco_maximo    NUMERIC(12,2) NOT NULL
                );
                """
            )
            log.info("Tabelas '%s' e '%s' garantidas.", TABELA_SNAPSHOT, TABELA_HISTORICO)

        @task(
            retries=5,
            retry_delay=timedelta(seconds=5),
            retry_exponential_backoff=True,                 # backoff exponencial
            max_retry_delay=timedelta(minutes=3),
            sla=timedelta(minutes=10),                      # SLA da task crítica
            on_failure_callback=notificar_falha,
            on_retry_callback=notificar_retry,
            on_success_callback=notificar_sucesso,
        )
        def buscar_produtos() -> list[dict]:
            """Task **crítica**: coleta os produtos da FakeStore API.

            Resiliente a instabilidades da API: qualquer erro dispara ``raise``
            para acionar o *retry* com *exponential backoff*, sem derrubar a
            execução inteira. Retorna apenas os campos necessários (payload
            pequeno trafegado via XCom)."""
            try:
                resposta = requests.get(FAKESTORE_URL, timeout=30)
                resposta.raise_for_status()
                produtos = resposta.json()
            except Exception as erro:  # noqa: BLE001
                log.error("Falha ao consultar a FakeStore API: %s", erro)
                raise  # re-levanta para acionar o retry do Airflow

            if not isinstance(produtos, list) or not produtos:
                raise ValueError("A API retornou um payload vazio ou inválido.")

            enxutos = [
                {
                    "id": p.get("id"),
                    "title": p.get("title"),
                    "price": p.get("price"),
                    "category": p.get("category"),
                }
                for p in produtos
            ]
            log.info("Coletados %d produtos da API.", len(enxutos))
            return enxutos

        # --- Topologia LINEAR: criar_tabelas -> buscar_produtos -> validar --- #
        tabelas = criar_tabelas()
        produtos = buscar_produtos()

        produtos_validos = ValidarProdutosOperator(
            task_id="validar_produtos",
            produtos=produtos,  # XCom de buscar_produtos (resolvido em runtime)
            campos_obrigatorios=["id", "title", "price", "category"],
        )

        tabelas >> produtos  # encadeamento linear explícito

        return produtos_validos.output

    # ===================================================================== #
    # TaskGroup 2 — ANÁLISE                                                  #
    # ===================================================================== #
    @task_group(group_id="analise", tooltip="Agrupa por categoria e calcula métricas (fan-out/fan-in).")
    def analise(produtos: list[dict]):

        @task
        def agrupar_por_categoria(produtos: list[dict]) -> list[dict]:
            """Prepara a lista que alimenta o Dynamic Task Mapping.

            O pipeline **escala sozinho**: cada categoria nova que surgir vira
            automaticamente uma task mapeada, sem precisar editar o código."""
            from collections import defaultdict

            agrupado: dict[str, list[float]] = defaultdict(list)
            for p in produtos:
                agrupado[p["category"]].append(float(p["price"]))

            grupos = [{"categoria": c, "precos": precos} for c, precos in agrupado.items()]
            log.info("Categorias encontradas (%d): %s", len(grupos), [g["categoria"] for g in grupos])
            return grupos

        @task(pool=ECOMMERCE_POOL)  # Pool limita a concorrência a 2 slots
        def calcular_metricas(grupo: dict) -> dict:
            """Calcula as métricas de UMA categoria (task mapeada / fan-out)."""
            precos = grupo["precos"]
            return {
                "categoria": grupo["categoria"],
                "qtd_produtos": len(precos),
                "preco_medio": round(sum(precos) / len(precos), 2),
                "preco_minimo": round(min(precos), 2),
                "preco_maximo": round(max(precos), 2),
            }

        @task
        def consolidar_metricas(metricas: list[dict]) -> list[dict]:
            """Junta o resultado de todas as categorias (fan-in)."""
            consolidado = sorted(metricas, key=lambda m: m["categoria"])
            log.info("Métricas consolidadas para %d categorias.", len(consolidado))
            return consolidado

        grupos = agrupar_por_categoria(produtos)            # linear
        metricas = calcular_metricas.expand(grupo=grupos)   # FAN-OUT (mapeamento)
        return consolidar_metricas(metricas)                # FAN-IN (consolidação)

    # ===================================================================== #
    # TaskGroup 3 — PERSISTÊNCIA                                             #
    # ===================================================================== #
    @task_group(group_id="persistencia", tooltip="Grava o snapshot idempotente e o histórico.")
    def persistencia(metricas: list[dict]):

        @task
        def salvar_snapshot(metricas: list[dict]) -> int:
            """Grava o snapshot do dia de forma **IDEMPOTENTE** (UPSERT).

            Re-rodar a mesma run não duplica linhas: o conflito na PK
            ``(data_referencia, categoria)`` atualiza o registro existente."""
            ctx = get_current_context()
            data_ref = ctx["logical_date"].in_timezone(LOCAL_TZ).date()

            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            sql = f"""
                INSERT INTO {TABELA_SNAPSHOT}
                    (data_referencia, categoria, qtd_produtos,
                     preco_medio, preco_minimo, preco_maximo, atualizado_em)
                VALUES (%s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (data_referencia, categoria) DO UPDATE SET
                    qtd_produtos  = EXCLUDED.qtd_produtos,
                    preco_medio   = EXCLUDED.preco_medio,
                    preco_minimo  = EXCLUDED.preco_minimo,
                    preco_maximo  = EXCLUDED.preco_maximo,
                    atualizado_em = now();
            """
            params = [
                (data_ref, m["categoria"], m["qtd_produtos"],
                 m["preco_medio"], m["preco_minimo"], m["preco_maximo"])
                for m in metricas
            ]
            conn = hook.get_conn()
            try:
                with conn.cursor() as cur:
                    cur.executemany(sql, params)
                conn.commit()
            finally:
                conn.close()
            log.info("Snapshot gravado/atualizado: %d categorias (data_ref=%s).", len(params), data_ref)
            return len(params)

        @task
        def salvar_historico(metricas: list[dict]) -> int:
            """Tabela de **HISTÓRICO** (append) para acompanhar a evolução dos preços.

            Idempotente por run: remove as linhas da mesma run antes de inserir,
            preservando o histórico entre execuções de dias diferentes."""
            ctx = get_current_context()
            run_id = ctx["run_id"]
            data_ref = ctx["logical_date"].in_timezone(LOCAL_TZ).date()
            executado_em = pendulum.now(LOCAL_TZ)

            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            conn = hook.get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(f"DELETE FROM {TABELA_HISTORICO} WHERE run_id = %s;", (run_id,))
                    cur.executemany(
                        f"""
                        INSERT INTO {TABELA_HISTORICO}
                            (run_id, data_referencia, executado_em, categoria,
                             qtd_produtos, preco_medio, preco_minimo, preco_maximo)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                        """,
                        [
                            (run_id, data_ref, executado_em, m["categoria"],
                             m["qtd_produtos"], m["preco_medio"],
                             m["preco_minimo"], m["preco_maximo"])
                            for m in metricas
                        ],
                    )
                conn.commit()
            finally:
                conn.close()
            log.info("Histórico registrado (run_id=%s, %d categorias).", run_id, len(metricas))
            return len(metricas)

        # Ambas dependem das métricas consolidadas (fan-out a partir do fan-in).
        salvar_snapshot(metricas)
        salvar_historico(metricas)

    # --------------------------------------------------------------------- #
    # Orquestração de alto nível — dependências saem da chamada das funções #
    # --------------------------------------------------------------------- #
    produtos_validos = ingestao()
    metricas = analise(produtos_validos)
    persistencia(metricas)


dag = ecommerce_pricing_pipeline()
