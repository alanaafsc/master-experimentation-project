"""
run_experiment.py — Orquestrador do Experimento em Duas Fases

Gerencia o fluxo completo do experimento empírico descrito na Seção de
Metodologia da dissertação, dividido em:

  Fase 1 — Baseline (~20 min):
    O sistema opera sob carga estável gerada pelo Locust. O framework
    coleta BASELINE_PHASE_CYCLES ciclos de dados para construir o perfil
    estatístico normal (μ, σ) de cada serviço. Nenhum escalonamento é
    esperado nesta fase.

  Fase 2 — Injeção de Falhas (~10 min):
    Aplica VirtualService Istio com delay fixo de 2,5s em 100% das
    requisições ao productcatalogservice. Observa-se a capacidade do
    framework de:
      (a) detectar a anomalia via Regra 3σ
      (b) identificar productcatalogservice como causa raiz (Pearson)
      (c) confirmar tendência via ARIMA antes de escalonar
      (d) suprimir escalonamentos de frontend (serviço sintomático)

  Fase 3 — Recuperação (~5 min):
    Remove a injeção de falhas e observa o retorno às métricas de baseline
    (cálculo do MTTR — Mean Time To Repair).

Resultados salvos em results/:
  metrics_raw.csv          — série temporal bruta de todas as métricas
  reports/event_*.json     — evidências XAI por evento de anomalia
  plots/heatmap_*.png      — heatmaps de correlação por evento
  plots/timeseries_*.png   — séries temporais com anomalia e previsão
  experiment_summary.json  — métricas agregadas do experimento
  experiment_meta.json     — metadados e cronograma das fases
"""

import argparse
import os

# ── Configurações por cenário ─────────────────────────────────────────────────
_SCENARIOS = {
    "A": {
        "manifest":    "fault-injection.yaml",
        "service":     "productcatalogservice",
        "delay":       "2,5s",
        "results_dir": "results",
        "log_dir":     "logs",
    },
    "B": {
        "manifest":    "fault-cartservice.yaml",
        "service":     "cartservice",
        "delay":       "2,5s",
        "results_dir": "results_cenarioB",
        "log_dir":     "logs_cenarioB",
    },
    "C": {
        "manifest":    "fault-checkoutservice.yaml",
        "service":     "checkoutservice",
        "delay":       "1,5s",
        "results_dir": "results_cenarioC",
        "log_dir":     "logs_cenarioC",
    },
}


def _setup():
    """Parseia --scenario e configura env vars ANTES de importar config/XAIAnalyzer."""
    parser = argparse.ArgumentParser(
        description="XAI-FinOps Experiment Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Cenários disponíveis:\n"
            "  A — productcatalogservice, delay 2,5s (padrão)\n"
            "  B — cartservice,           delay 2,5s\n"
            "  C — checkoutservice,       delay 1,5s\n"
        ),
    )
    parser.add_argument(
        "--scenario", choices=["A", "B", "C"], default="A",
        help="Cenário de injeção de falha (default: A)",
    )
    args = parser.parse_args()
    sc = _SCENARIOS[args.scenario]
    # Propaga para config.py antes que seja importado
    os.environ["XAI_RESULTS_DIR"] = sc["results_dir"]
    os.environ["XAI_LOG_DIR"]     = sc["log_dir"]
    fault_manifest = os.path.join(
        os.path.dirname(__file__), "..", "infrastructure", sc["manifest"]
    )
    return args.scenario, fault_manifest, sc["service"], sc["delay"]


# Executa setup ANTES de qualquer import que leia config.py
SCENARIO_LABEL, FAULT_MANIFEST, FAULT_SERVICE, FAULT_DELAY = _setup()

# ── Agora importa o resto (config já lê as env vars corretas) ─────────────────
import json
import logging
import subprocess
import time
from datetime import datetime

from config import (
    BASELINE_PHASE_CYCLES,
    FAULT_PHASE_CYCLES,
    SCRAPE_INTERVAL_S,
    RESULTS_DIR,
    LOG_DIR,
)
from analyzer_xai import XAIAnalyzer

# ── Logging ──────────────────────────────────────────────────────────────────
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(RESULTS_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "experiment.log"), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("ExperimentRunner")

RECOVERY_CYCLES = 10   # ciclos para medir MTTR após remoção da falha


# ─────────────────────────────────────────────────────────────────────────────
# Utilitários kubectl
# ─────────────────────────────────────────────────────────────────────────────

def kubectl(action: str, manifest: str) -> bool:
    """Aplica (apply) ou remove (delete) um manifesto Kubernetes."""
    cmd = ["kubectl", action, "-f", manifest]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error("kubectl %s falhou:\n%s", action, result.stderr.strip())
        return False
    logger.info("kubectl %s %s — OK", action, os.path.basename(manifest))
    return True


def _run_phase(
    analyzer: XAIAnalyzer,
    label: str,
    cycles: int,
) -> dict:
    """
    Executa um número fixo de ciclos MAPE-K e retorna metadados da fase.
    """
    logger.info("┌─ FASE: %s (%d ciclos × %ds = ~%d min) ─────────────────",
                label, cycles, SCRAPE_INTERVAL_S, (cycles * SCRAPE_INTERVAL_S) // 60)

    start = datetime.utcnow()
    for i in range(cycles):
        logger.info("│  Ciclo %d/%d [%s]", i + 1, cycles, label)
        analyzer._collect_cycle()
        if i < cycles - 1:              # sem sleep no último ciclo
            time.sleep(SCRAPE_INTERVAL_S)

    end = datetime.utcnow()
    duration_s = (end - start).total_seconds()

    phase_events = [
        e for e in analyzer.event_log
        if e["timestamp"] >= start.isoformat()
    ]
    scale_count = sum(1 for e in phase_events if "SCALE_UP" in e.get("decision", ""))

    logger.info("└─ FIM %s | Duração: %.0fs | Anomalias: %d | Escalonamentos: %d",
                label, duration_s, len(phase_events), scale_count)

    return {
        "name": label,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "duration_s": round(duration_s, 1),
        "cycles": cycles,
        "anomaly_events": len(phase_events),
        "scale_events": scale_count,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Experimento principal
# ─────────────────────────────────────────────────────────────────────────────

def run_experiment():
    """
    Executa o experimento completo em três fases (Baseline, Fault, Recovery).
    """
    logger.info("=" * 65)
    logger.info("  XAI-FinOps — Experimento Empírico (Design Science Research)")
    logger.info("  Cenário %s: %s (delay=%s)", SCENARIO_LABEL, FAULT_SERVICE, FAULT_DELAY)
    logger.info("  Iniciado em: %s", datetime.utcnow().isoformat())
    logger.info("=" * 65)

    analyzer = XAIAnalyzer()
    meta = {
        "experiment_id": datetime.utcnow().strftime("%Y%m%d_%H%M%S"),
        "start_time": datetime.utcnow().isoformat(),
        "config": {
            "scrape_interval_s": SCRAPE_INTERVAL_S,
            "baseline_cycles": BASELINE_PHASE_CYCLES,
            "fault_cycles": FAULT_PHASE_CYCLES,
            "recovery_cycles": RECOVERY_CYCLES,
        },
        "phases": [],
    }

    # ── FASE 1: BASELINE ─────────────────────────────────────────────────────
    phase1 = _run_phase(analyzer, "BASELINE", BASELINE_PHASE_CYCLES)
    meta["phases"].append(phase1)

    # Verificação: baseline deve ter zero anomalias para validade do experimento
    if phase1["anomaly_events"] > 0:
        logger.warning(
            "ATENÇÃO: %d anomalias detectadas na fase de baseline. "
            "Verifique se o sistema está estável antes de prosseguir.",
            phase1["anomaly_events"],
        )

    # ── FASE 2: INJEÇÃO DE FALHAS ─────────────────────────────────────────────
    logger.info("")
    logger.info("Aplicando injeção de falha Istio → %s (delay=%s, 100%%)...", FAULT_SERVICE, FAULT_DELAY)
    fault_applied = kubectl("apply", FAULT_MANIFEST)

    if fault_applied:
        phase2 = _run_phase(analyzer, "FAULT_INJECTION", FAULT_PHASE_CYCLES)
    else:
        logger.error("Falha ao aplicar manifesto Istio. Pulando Fase 2.")
        phase2 = {"name": "FAULT_INJECTION", "status": "SKIPPED"}

    meta["phases"].append(phase2)

    # ── FASE 3: RECUPERAÇÃO (MTTR) ────────────────────────────────────────────
    logger.info("")
    logger.info("Removendo injeção de falha. Iniciando medição de MTTR...")
    fault_removed_at = datetime.utcnow()
    kubectl("delete", FAULT_MANIFEST)

    phase3 = _run_phase(analyzer, "RECOVERY", RECOVERY_CYCLES)
    meta["phases"].append(phase3)

    # ── MÉTRICAS FINAIS ────────────────────────────────────────────────────────
    summary = analyzer.reporter.generate_summary(analyzer.event_log)

    meta["end_time"] = datetime.utcnow().isoformat()
    meta["fault_removed_at"] = fault_removed_at.isoformat()
    meta["results"] = {
        "total_anomaly_events": summary["total_anomaly_events"],
        "scale_up_events": summary["scale_up_events"],
        "suppressed_events": summary["suppressed_events"],
        "suppression_rate": summary["suppression_rate"],
        "avg_pearson_score": summary["avg_pearson_score"],
    }

    meta_path = os.path.join(RESULTS_DIR, "experiment_meta.json")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)

    logger.info("")
    logger.info("=" * 65)
    logger.info("  EXPERIMENTO CONCLUÍDO")
    logger.info("  Anomalias detectadas  : %d", summary["total_anomaly_events"])
    logger.info("  Escalonamentos        : %d", summary["scale_up_events"])
    logger.info("  Supressões (FP evit.) : %d", summary["suppressed_events"])
    logger.info("  Taxa de supressão     : %.1f%%", summary["suppression_rate"] * 100)
    logger.info("  Resultados em         : %s/", RESULTS_DIR)
    logger.info("=" * 65)


if __name__ == "__main__":
    run_experiment()
