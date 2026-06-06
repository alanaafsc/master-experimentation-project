# XAI-FinOps Framework — Repositório de Experimentos

Artefato de implementação da dissertação de mestrado:

> **Alana Ingrid Farias Ferreira Costa**
> *Framework XAI-FinOps: Gerenciamento Autônomo e Explicável de Recursos em Kubernetes com MAPE-K e Inteligência Artificial Explicável*
> Programa de Pós-Graduação em Ciência da Computação — CIn/UFPE, 2026

---

## Visão Geral

O XAI-FinOps é um sistema autônomo de gerenciamento de recursos para clusters Kubernetes que integra o ciclo de controle MAPE-K com Inteligência Artificial Explicável sob o princípio de **causalidade antes de reatividade**: a causa raiz de uma degradação de latência é identificada antes de qualquer ação de escalonamento.

Componentes principais:

| Módulo | Fase MAPE-K | Responsabilidade |
|---|---|---|
| `monitor.py` | Monitor | Coleta métricas P95 via Prometheus/Istio; extrai grafo de dependências |
| `analyzer_xai.py` | Analyze | Detecta anomalias (Regra 3σ); identifica causa raiz (Pearson topológico) |
| `scaler.py` | Plan | Confirma tendência persistente via ARIMA(2,1,0) antes de escalar |
| `executor.py` | Execute | Aplica PATCH no Deployment Kubernetes do serviço causa raiz |
| `xai_reporter.py` | XAI | Gera heatmap de correlação, série temporal 3σ e relatório JSON auditável |

---

## Pré-requisitos

- Python 3.11+
- Kubernetes 1.28+ com `kubectl` configurado
- Istio 1.20+ com sidecar injection habilitado no namespace `default`
- Prometheus 2.47+ (recomendado via `kube-prometheus-stack` no Helm)
- Google Online Boutique v0.8.0 implantado no namespace `default`
- Locust 2.20+ para geração de carga sintética

**Ambiente experimental utilizado:** cluster `kind` (Kubernetes IN Docker) em notebook Intel Core i7-13650HX (2,60 GHz), 16 GB RAM DDR5, Windows 11 + Docker Desktop.

---

## Instalação

```bash
git clone https://github.com/alanaafsc/master-experimentation-project.git
cd master-experimentation-project
python -m venv venv
# Windows:
venv\Scripts\activate
# Linux/macOS:
source venv/bin/activate
pip install -r requirements.txt
```

---

## Configuração

Todos os parâmetros estão centralizados em `scripts/config.py`:

| Parâmetro | Valor | Descrição |
|---|---|---|
| `BASELINE_WINDOW` | 20 | Ciclos mínimos para baseline estatístico |
| `ANOMALY_K` | 3.0 | Fator k da Regra 3σ — P(falso positivo) ≈ 0,27% |
| `PEARSON_THRESHOLD` | 0.7 | Limiar τ de correlação forte para RCA |
| `LATENCY_SLA_MS` | 500 ms | SLA de latência P95 |
| `ARIMA_ORDER` | (2,1,0) | d=1: diferenciação; p=2: AR curto prazo; q=0: sem MA |
| `FORECAST_STEPS` | 3 | Horizonte h=3 passos (90 s) |
| `SCALE_UP_REPLICAS` | 3 | Réplicas-alvo ao escalar (fixo; não baseado em CPU/memória) |
| `COOLDOWN_CYCLES` | 5 | Ciclos de resfriamento pós-escalonamento (~2,5 min) para evitar thrashing |
| `SCRAPE_INTERVAL_S` | 30 | Intervalo de coleta (segundos) |
| `BASELINE_PHASE_CYCLES` | 40 | Duração da Fase 1 (~20 min) |
| `FAULT_PHASE_CYCLES` | 20 | Duração da Fase 2 (~10 min) |

Para usar um diretório de resultados diferente sem modificar o código:

```bash
XAI_RESULTS_DIR=results_cenarioB python scripts/run_experiment.py
```

---

## Executando os Experimentos

### Experimento principal — Cenário A (falha em `productcatalogservice`)

```bash
cd scripts
python run_experiment.py
# Resultados em: scripts/results/
```

### Cenário B — falha em `cartservice` (nó folha, propagação indireta)

```bash
XAI_RESULTS_DIR=results_cenarioB python scripts/run_experiment.py
# Resultados em: scripts/results_cenarioB/
```

### Cenário C — falha em `checkoutservice` (nó intermediário)

```bash
XAI_RESULTS_DIR=results_cenarioC python scripts/run_experiment.py
# Resultados em: scripts/results_cenarioC/
```

### Multi-execução (n=5 por cenário)

```bash
python scripts/run_all_scenarios.py
# Resultados em: scripts/results_cenarioA_run{1..5}/  (e B, C)
# Sumário: scripts/results_multi_run_summary.csv
```

### Grupo de controle — HPA nativo

```bash
python scripts/hpa_baseline.py
# Resultados em: results_hpa/
```

### Grupo de controle — KEDA (n=5)

```bash
python scripts/run_keda_multirun.py
# Resultados em: scripts/results_keda_run{1..5}/
# Sumário: scripts/keda_multirun_summary.csv
```

---

## Estrutura do Repositório

```
master-experimentation-project/
├── .gitignore
├── README.md
├── requirements.txt
├── infrastructure/                      # Manifestos Kubernetes e Istio
│   ├── rbac-config.yaml                 # RBAC para executor.py (permissão de patch em Deployments)
│   └── fault-injection/                 # VirtualService com fixedDelay para injeção de falha
├── scripts/                             # Código-fonte do framework XAI-FinOps
│   ├── config.py                        # Parâmetros centralizados (único arquivo a editar)
│   ├── monitor.py                       # Módulo Monitor
│   ├── analyzer_xai.py                  # Módulo Analyze (v2 com restrição topológica)
│   ├── scaler.py                        # Módulo Plan (ARIMA)
│   ├── executor.py                      # Módulo Execute
│   ├── xai_reporter.py                  # Geração de evidências XAI
│   ├── run_experiment.py                # Orquestrador principal
│   ├── run_all_scenarios.py             # Multi-cenário automatizado
│   ├── run_keda_multirun.py             # Multi-run KEDA
│   ├── hpa_baseline.py                  # Baseline HPA
│   ├── keda_scenario.py                 # Baseline KEDA (execução única)
│   ├── analyze_results.py               # Análise pós-experimento e geração de figuras
│   ├── locustfile.py                    # Script de carga sintética (Locust)
│   ├── results/                         # Cenário A — execução principal (2026-05-23)
│   ├── results_cenarioB/                # Cenário B — execução principal (2026-05-27)
│   ├── results_cenarioA_run{1..6}/      # Cenário A — 6 execuções independentes
│   ├── results_cenarioB_run{1..5}/      # Cenário B — 5 execuções independentes
│   ├── results_cenarioC_run{1..5}/      # Cenário C — 5 execuções independentes
│   ├── results_keda_run{1..5}/          # KEDA — 5 execuções independentes
│   ├── results_multi_run_summary.csv    # Sumário A+B+C: média ± DP, IC 95%
│   └── keda_multirun_summary.csv        # Sumário KEDA: Precisão, IC 95%
├── results/                             # Cenário A — métricas brutas (raiz)
├── results_hpa/                         # HPA — métricas brutas
├── results_keda/                        # KEDA — métricas brutas
├── results_pilot_20260521/              # Experimento piloto (identificou necessidade de v2)
├── logs_cenarioA_run{1..6}_stdout.txt   # Log completo de cada run — Cenário A
├── logs_cenarioB_run{1..5}_stdout.txt   # Log completo — Cenário B
├── logs_cenarioC_run{1..5}_stdout.txt   # Log completo — Cenário C
└── logs_keda_run{1..5}_stdout.txt       # Log completo — KEDA
```

---

## Estrutura dos Resultados por Diretório

Cada diretório `results_*/` contém:

```
results_*/
├── metrics_raw.csv                      # Série temporal completa de P95, throughput e CPU
├── experiment_meta.json                 # Metadados: timestamps, parâmetros, lista de eventos
├── reports/
│   ├── experiment_summary.json          # Métricas: Precisão, MTTR, supressão, FinOps
│   └── event_YYYYMMDD_HHMMSS.json       # Relatório XAI por evento de escalonamento
└── plots/
    ├── heatmap_*.png                    # Heatmap de correlação de Pearson no ciclo do evento
    └── latency_*.png                    # Série temporal de P95 com banda 3σ
```

### Campos principais de `experiment_summary.json`

- `precision` — Precisão ciclo a ciclo (VP / (VP + FP))
- `functional_recall` — Revocação Funcional (SCALE_UPs corretos / total de eventos de falha)
- `mttr_seconds` — Mean Time To Repair após remoção da falha
- `suppressed_count` — Eventos de anomalia detectados mas não escalados (supressão FinOps)
- `scale_up_events` — Lista de ciclos com SCALE_UP, serviço escalonado e score Pearson

### `results_multi_run_summary.csv`

Uma linha por run × cenário. Colunas: `cenario`, `run`, `precisao`, `supressao_pct`, `mttr_s`, `pearson_medio`. Usado para calcular média ± DP e IC 95% reportados no Capítulo 6 da dissertação.

---

## Resultados dos Experimentos

### Cenário A — falha direta em `productcatalogservice` (atraso 2,5 s via Istio)

| Métrica | Valor |
|---|---|
| Precisão (ciclo-a-ciclo) | 83,3% (VP=5, FP=1, FN=14) |
| Revocação Funcional | 100% (único SCALE_UP correto) |
| Tempo até primeira ação | ~90 s (ciclo 43, a partir da injeção no ciclo 41) |
| MTTR após remoção da falha | ~61 s |
| Taxa de supressão (n=5, média ± DP) | 75,2 ± 15,6% |
| Redução de custo FinOps | 67% vs. escalonamento sem RCA |
| Comparativo KEDA | 9,4× mais rápido; Precisão 83,3% vs. 17,1% do KEDA |

### Cenário B — falha em `cartservice` (nó folha, propagação via `checkoutservice`)

| Métrica | Valor |
|---|---|
| Acurácia de identificação da causa raiz | 100% |
| Taxa de supressão (n=5, média ± DP) | 69,7 ± 6,8% (IC 95%: [61,3%; 78,2%]) |

### Cenário C — falha em `checkoutservice` (nó intermediário)

| Métrica | Valor |
|---|---|
| Taxa de supressão (n=5, média ± DP) | 94,3 ± 12,8% |
| Observação | Identificação causal falha quando variância no baseline é nula (fronteira de validade do método) |

### Grupo de controle — KEDA (n=5, mesmo cenário do A)

| Métrica | Valor |
|---|---|
| Precisão | 17,1% (IC 95%: [9,1%; 33,7%]) |
| Tempo de resposta | ~843 s (9,4× mais lento que XAI-FinOps) |
| Causa | Ausência de restrição topológica → over-provisioning em cascata |

---

## Referência

Costa, A. I. F. F. *Framework XAI-FinOps: Gerenciamento Autônomo e Explicável de Recursos em Kubernetes com MAPE-K e Inteligência Artificial Explicável.* Dissertação (Mestrado em Ciência da Computação) — Centro de Informática, Universidade Federal de Pernambuco, Recife, 2026.
