# ShopBrasil — Pipeline de Precificação por Categoria (Apache Airflow)

Pipeline de dados em **Apache Airflow** que aposenta o antigo script `cron` da
ShopBrasil. Todos os dias às **06:00 (horário de Brasília)** ele coleta os
produtos da [FakeStore API](https://fakestoreapi.com/docs), calcula as métricas
de preço por categoria (médio, mínimo, máximo e quantidade) e grava o resultado
em uma base analítica **PostgreSQL** — de forma resiliente, escalável e
**idempotente**.

---

## 🏛️ Arquitetura

```
                          ┌────────────────────────── DAG: ecommerce_pricing_pipeline ──────────────────────────┐
                          │                                                                                      │
  TaskGroup: ingestao     │   criar_tabelas ─▶ buscar_produtos ─▶ validar_produtos            (TOPOLOGIA LINEAR) │
                          │                         │  (retry+backoff, callbacks, SLA)   (ValidarProdutosOperator)│
                          │                         ▼                                                            │
  TaskGroup: analise      │   agrupar_por_categoria ─▶ calcular_metricas.expand(...)  ─▶ consolidar_metricas     │
                          │                              (FAN-OUT / pool 2 slots)         (FAN-IN)               │
                          │                         ▼                                                            │
  TaskGroup: persistencia │   salvar_snapshot (UPSERT idempotente)   +   salvar_historico (append)              │
                          └──────────────────────────────────────────────────────────────────────────────────┘
                                                          │
                                                          ▼
                                          PostgreSQL  (banco: ecommerce_analytics)
                                          ├─ precos_por_categoria             (snapshot do dia, idempotente)
                                          └─ precos_por_categoria_historico   (histórico append, evolução)
```

### Serviços (Docker)
| Serviço | Função |
|---|---|
| `postgres` | Banco de metadados do Airflow **+** banco analítico `ecommerce_analytics` |
| `airflow-init` | `db migrate`, cria o usuário admin e o **pool `ecommerce_pool` (2 slots)** |
| `airflow-webserver` | UI em http://localhost:8080 |
| `airflow-scheduler` | Agenda e executa as tasks (LocalExecutor) |

---

## 🚀 Como executar

> Pré-requisitos: **Docker** e **Docker Compose** instalados.

```bash
# 1) Subir todo o ambiente (build da imagem + bancos + airflow)
docker compose up -d --build

# 2) Acompanhar a inicialização (opcional)
docker compose logs -f airflow-init

# 3) Acessar a UI
#    http://localhost:8080   ->   usuário: airflow / senha: airflow
```

Na UI, ative (toggle) o DAG **`ecommerce_pricing_pipeline`** e dispare uma run
manual no botão ▶️ (*Trigger DAG*) para testar imediatamente — ele também
rodará sozinho todo dia às 06:00.

### Conferindo os dados gravados
```bash
docker compose exec postgres psql -U airflow -d ecommerce_analytics \
  -c "SELECT * FROM precos_por_categoria ORDER BY categoria;"

docker compose exec postgres psql -U airflow -d ecommerce_analytics \
  -c "SELECT data_referencia, categoria, preco_medio, executado_em
        FROM precos_por_categoria_historico ORDER BY executado_em DESC;"
```

### Encerrar
```bash
docker compose down          # para os containers
docker compose down -v       # para e apaga também os dados (volume do postgres)
```

---

## ✅ Mapa dos requisitos × implementação

### Requisitos obrigatórios
| Requisito | Onde / Como |
|---|---|
| **FakeStore API** como fonte | `buscar_produtos` → `GET https://fakestoreapi.com/products` |
| **TaskFlow API** (`@dag`/`@task`), deps pela chamada das funções | Todo o `dags/ecommerce_pricing_pipeline.py` |
| **XComs via `return`** (dados pequenos) | Tasks retornam só os campos necessários (id, title, price, category / métricas) |
| Topologia **linear** | `criar_tabelas >> buscar_produtos >> validar_produtos` |
| Topologia **fan-out** (mapeamento por categoria) | `calcular_metricas.expand(grupo=grupos)` |
| Topologia **fan-in** (consolidação) | `consolidar_metricas(metricas)` |
| **Timezone `America/Sao_Paulo`** (pendulum) + `start_date` + `catchup=False` | Decorador `@dag(...)` |
| Roda **todo dia às 06:00** | `schedule="0 6 * * *"` com `start_date` tz-aware |
| **Retry + exponential backoff** na task "Buscar Produtos" | `retries=5, retry_exponential_backoff=True, max_retry_delay=...` |
| **try/except + raise** (aciona o retry) | `buscar_produtos` |
| **Callbacks** `on_failure` / `on_retry` / `on_success` na task crítica | `notificar_falha` / `notificar_retry` / `notificar_sucesso` |
| **Dynamic Task Mapping** (`.expand`) | `calcular_metricas.expand(...)` |
| **Pool `ecommerce_pool` (2 slots)** nas tasks mapeadas | `@task(pool="ecommerce_pool")` + criado no `airflow-init` |
| **≥ 2 TaskGroups** | `ingestao`, `analise`, `persistencia` (3 grupos) |
| **PostgresHook + Connection do Airflow** | `PostgresHook("postgres_analytics")` (conn via env var) |
| **Gravação idempotente** (re-run não duplica) | `INSERT ... ON CONFLICT (data_referencia, categoria) DO UPDATE` |

### Requisitos opcionais (todos implementados)
| Requisito | Onde / Como |
|---|---|
| **Operador customizado** `ValidarProdutosOperator(BaseOperator)` | `plugins/operators/validar_produtos_operator.py` |
| **Tabela de histórico** (append, com data da execução) | `precos_por_categoria_historico` em `salvar_historico` |
| **SLA / alerta** | `sla=timedelta(minutes=10)` na task crítica + `sla_miss_callback` + `on_failure_callback` que simula o alerta |

---

## 🧠 Decisões de projeto

- **Escala sozinho:** as categorias são derivadas dinamicamente dos produtos em
  `agrupar_por_categoria`; cada categoria nova vira automaticamente uma task
  mapeada — sem editar código.
- **Idempotência:** o *snapshot* usa `ON CONFLICT` sobre a PK
  `(data_referencia, categoria)`. O *histórico* remove as linhas da mesma
  `run_id` antes de inserir, então reprocessar a mesma run nunca duplica, mas o
  histórico entre dias diferentes é preservado.
- **Resiliência:** falhas da API levantam exceção, que o Airflow trata com
  *retry* + *exponential backoff*, sem derrubar a run inteira.
- **Connection sem clicar na UI:** a conexão `postgres_analytics` é injetada via
  variável de ambiente `AIRFLOW_CONN_POSTGRES_ANALYTICS` no `docker-compose.yml`.

---

## 📁 Estrutura do projeto

```
.
├── dags/
│   └── ecommerce_pricing_pipeline.py     # DAG principal (TaskFlow)
├── plugins/
│   └── operators/
│       └── validar_produtos_operator.py  # Operador customizado (opcional)
├── scripts/
│   └── init-analytics-db.sql             # Cria o banco ecommerce_analytics
├── config/                               # Config do Airflow (montado no container)
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

---

## 📦 Entrega (repositório)

```bash
git init
git add .
git commit -m "Pipeline Airflow de precificacao por categoria - ShopBrasil"
git branch -M main
git remote add origin <URL_DO_SEU_REPOSITORIO>
git push -u origin main
```
Depois, libere o acesso de visualização do repositório e poste o link.

---

## 🩺 Notas / Troubleshooting

- **Logs em volume nomeado:** os logs do Airflow ficam no volume Docker
  `airflow-logs` (e não num bind-mount do host). Isso evita o erro
  `PermissionError: [Errno 13] ... /opt/airflow/logs/...` comum no
  Docker Desktop (Windows), em que o uid do container não consegue escrever na
  pasta do host. Para inspecionar os logs:
  `docker compose exec airflow-scheduler bash -c 'ls -R /opt/airflow/logs'`.
- **O DAG não aparece na UI?** Aguarde alguns segundos após o `up` (o scheduler
  precisa serializar o DAG) e atualize a página. Confira erros com
  `docker compose exec airflow-scheduler airflow dags list-import-errors`.
- O `airflow-init` roda com o mesmo uid do Airflow (`50000:0`) justamente para
  não criar diretórios de log pertencentes a `root` no volume compartilhado.
