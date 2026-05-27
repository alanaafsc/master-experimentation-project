"""
config.py — Configuração centralizada do experimento XAI-FinOps.

Todos os parâmetros do experimento ficam aqui para permitir reprodutibilidade
e rastreabilidade científica. Altere apenas este arquivo para ajustar o
comportamento do framework sem modificar a lógica dos módulos.
"""

import os

# ── Infraestrutura ──────────────────────────────────────────────────────────────
PROMETHEUS_URL: str = os.getenv("PROMETHEUS_URL", "http://localhost:9090")
KUBERNETES_NAMESPACE: str = os.getenv("K8S_NAMESPACE", "default")

# ── Serviços monitorados (Google Online Boutique / Hipster-shop) ───────────────
# Mantém apenas os serviços com dependências relevantes para o grafo causal.
SERVICES: list[str] = [
    "frontend",
    "productcatalogservice",
    "checkoutservice",
    "cartservice",
    "recommendationservice",
]

# Pares de dependência conhecidos (source → destination) para guiar o Random Walk.
# Baseado na topologia oficial do Google Online Boutique.
SERVICE_DEPENDENCIES: list[tuple[str, str]] = [
    ("frontend", "productcatalogservice"),
    ("frontend", "cartservice"),
    ("frontend", "checkoutservice"),
    ("frontend", "recommendationservice"),
    ("checkoutservice", "productcatalogservice"),
    ("checkoutservice", "cartservice"),
    ("recommendationservice", "productcatalogservice"),
]

# ── Parâmetros estatísticos — Regra k-sigma (Seção III.A) ──────────────────────
BASELINE_WINDOW: int = 20          # N mínimo de observações para estabelecer baseline
ANOMALY_K: float = 3.0             # k da Regra k-σ  →  P(falso positivo) ≈ 0,27%
PEARSON_THRESHOLD: float = 0.7     # Limiar de correlação para identificar causa raiz
LATENCY_SLA_MS: float = 500.0      # SLA de latência P95 aceito pela aplicação (ms)

# ── Modelo preditivo ARIMA (Seção III.C) ───────────────────────────────────────
# Ordem (2,1,0) escolhida por:
#   d=1 → primeira diferenciação remove tendência linear (garante estacionariedade)
#   p=2 → dois termos AR capturam autocorrelação de curto prazo (intervalo 30s)
#   q=0 → sem MA; série de latência não apresenta correlação de erro residual
ARIMA_ORDER: tuple[int, int, int] = (2, 1, 0)
ARIMA_MIN_POINTS: int = 15         # Mínimo de pontos para ajuste do modelo
FORECAST_STEPS: int = 3            # Horizonte de previsão (= 3 × intervalo de coleta)

# ── Controle de escalonamento ──────────────────────────────────────────────────
SCALE_UP_REPLICAS: int = 3         # Número de réplicas ao escalar para cima
SCALE_DOWN_REPLICAS: int = 1       # Número de réplicas ao escalar para baixo
COOLDOWN_CYCLES: int = 5           # Ciclos de cooldown após ação de escalonamento

# ── Coleta e loop ──────────────────────────────────────────────────────────────
SCRAPE_INTERVAL_S: int = 30        # Intervalo de coleta em segundos
BASELINE_PHASE_CYCLES: int = 40    # Ciclos da Fase 1 (~20 min a 30s/ciclo)
FAULT_PHASE_CYCLES: int = 20       # Ciclos da Fase 2 (~10 min de observação pós-falha)

# ── Diretórios de saída ─────────────────────────────────────────────────────────
# Lidos via env var para permitir cenários múltiplos sem alterar o código.
# Ex.: XAI_RESULTS_DIR=results_cenarioB python run_experiment.py
LOG_DIR: str = os.getenv("XAI_LOG_DIR", "logs")
RESULTS_DIR: str = os.getenv("XAI_RESULTS_DIR", "results")
