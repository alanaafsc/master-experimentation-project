"""
hpa_baseline.py — Experimento de linha de base com Kubernetes HPA (Grupo de Controle).

Executa o mesmo protocolo de 3 fases do run_experiment.py, substituindo o
framework XAI-FinOps pelo Horizontal Pod Autoscaler (HPA) padrão do Kubernetes.

Função científica (comparação para o Cap 6):
  O HPA reage a métricas de CPU/memória do serviço LOCAL (sintomático).
  Durante uma falha em productcatalogservice, o frontend sente a latência
  aumentar e, indiretamente, seu uso de CPU sobe — então o HPA pode escalar
  o FRONTEND (errado), não o productcatalogservice (causador).

  O XAI-FinOps, via Pearson, identifica o causador e escala o serviço correto.
  Essa diferença é o argumento central da dissertação.

Saídas em results_hpa/:
  metrics_raw.csv       — série temporal de P95, throughput e CPU por serviço
  hpa_events.csv        — log de mudanças de réplicas detectadas a cada ciclo
  experiment_meta.json  — cronograma das fases e configuração do HPA

Uso:
    cd master-experimentation-project/
    python scripts/hpa_baseline.py

Pré-requisito: kubectl configurado e apontando para o cluster com Online Boutique.
"""

import csv
import json
import logging
import os
import subprocess
import time
from datetime import datetime

from config import (
    BASELINE_PHASE_CYCLES,
    FAULT_PHASE_CYCLES,
    KUBERNETES_NAMESPACE,
    LOG_DIR,
    SCRAPE_INTERVAL_S,
    SERVICES,
)
from monitor import MonitoringModule

# ── Diretórios de saída ───────────────────────────────────────────────────────
HPA_RESULTS_DIR = "results_hpa"
HPA_LOG_DIR = os.path.join(LOG_DIR, "hpa")

os.makedirs(HPA_RESULTS_DIR, exist_ok=True)
os.makedirs(HPA_LOG_DIR, exist_ok=True)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.FileHandler(
            os.path.join(HPA_LOG_DIR, "hpa_baseline.log"), encoding="utf-8"
        ),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("HPABaseline")

# ── Parâmetros HPA ────────────────────────────────────────────────────────────
HPA_CPU_TARGET_PERCENT = 60    # Limiar de CPU para escalonamento reativo
HPA_MIN_REPLICAS = 1
HPA_MAX_REPLICAS = 5
RECOVERY_CYCLES = 10

# Serviços onde o HPA será instalado (os mais prováveis de serem escalados)
HPA_SERVICES = ["frontend", "productcatalogservice", "checkoutservice", "cartservice"]

FAULT_MANIFEST = os.path.join(
    os.path.dirname(__file__), "..", "infrastructure", "fault-injection.yaml"
)


# ─────────────────────────────────────────────────────────────────────────────
# Utilitários kubectl
# ─────────────────────────────────────────────────────────────────────────────

def _kubectl(*args) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["kubectl"] + list(args),
        capture_output=True, text=True,
    )


def kubectl_apply(manifest: str) -> bool:
    r = _kubectl("apply", "-f", manifest)
    if r.returncode != 0:
        logger.error("kubectl apply falhou:\n%s", r.stderr.strip())
        return False
    logger.info("kubectl apply %s — OK", os.path.basename(manifest))
    return True


def kubectl_delete(manifest: str) -> bool:
    r = _kubectl("delete", "-f", manifest, "--ignore-not-found")
    if r.returncode != 0:
        logger.error("kubectl delete falhou:\n%s", r.stderr.strip())
        return False
    logger.info("kubectl delete %s — OK", os.path.basename(manifest))
    return True


def get_replicas(service: str) -> int:
    """Retorna o número atual de réplicas de um Deployment (ou -1 se erro)."""
    r = _kubectl(
        "get", "deployment", service,
        "-n", KUBERNETES_NAMESPACE,
        "-o", "jsonpath={.spec.replicas}",
    )
    try:
        return int(r.stdout.strip()) if r.stdout.strip() else -1
    except ValueError:
        return -1


# ─────────────────────────────────────────────────────────────────────────────
# Gerenciamento do HPA
# ─────────────────────────────────────────────────────────────────────────────

def apply_hpa():
    """Cria HPAs de CPU para cada serviço em HPA_SERVICES."""
    logger.info("Instalando HPAs (CPU target=%d%%)...", HPA_CPU_TARGET_PERCENT)
    for svc in HPA_SERVICES:
        r = _kubectl(
            "autoscale", "deployment", svc,
            f"--cpu-percent={HPA_CPU_TARGET_PERCENT}",
            f"--min={HPA_MIN_REPLICAS}",
            f"--max={HPA_MAX_REPLICAS}",
            "-n", KUBERNETES_NAMESPACE,
        )
        if r.returncode == 0:
            logger.info("  HPA criado: %s", svc)
        elif "already exists" in r.stderr:
            logger.info("  HPA já existe: %s (mantido)", svc)
        else:
            logger.error("  Falha ao criar HPA para %s: %s", svc, r.stderr.strip())


def remove_hpa():
    """Remove os HPAs criados pelo experimento."""
    logger.info("Removendo HPAs...")
    for svc in HPA_SERVICES:
        _kubectl(
            "delete", "hpa", svc,
            "-n", KUBERNETES_NAMESPACE,
            "--ignore-not-found",
        )
        logger.info("  HPA removido: %s", svc)


# ─────────────────────────────────────────────────────────────────────────────
# Coletor de métricas (sem intervenção no escalonamento)
# ─────────────────────────────────────────────────────────────────────────────

class HPAMetricsCollector:
    """
    Coleta métricas de todos os serviços e detecta mudanças de réplicas
    causadas pelo HPA. Não toma nenhuma decisão de escalonamento.
    """

    def __init__(self):
        self.monitor = MonitoringModule()
        self._cycle_count = 0
        self._prev_replicas: dict[str, int] = {svc: get_replicas(svc) for svc in HPA_SERVICES}

        self._metrics_path = os.path.join(HPA_RESULTS_DIR, "metrics_raw.csv")
        self._events_path = os.path.join(HPA_RESULTS_DIR, "hpa_events.csv")
        self._init_files()

        logger.info(
            "HPAMetricsCollector iniciado. Réplicas iniciais: %s",
            self._prev_replicas,
        )

    def _init_files(self):
        with open(self._metrics_path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                "cycle", "timestamp", "service",
                "p95_ms", "throughput_rps", "cpu_cores",
            ])
        with open(self._events_path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                "cycle", "timestamp", "service",
                "from_replicas", "to_replicas", "direction",
            ])

    def _check_hpa_scaling(self, cycle: int, ts: str):
        """Compara réplicas atuais vs. anteriores para detectar ação do HPA."""
        with open(self._events_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            for svc in HPA_SERVICES:
                current = get_replicas(svc)
                prev = self._prev_replicas.get(svc, -1)
                if current != prev and current != -1 and prev != -1:
                    direction = "SCALE_UP" if current > prev else "SCALE_DOWN"
                    writer.writerow([cycle, ts, svc, prev, current, direction])
                    logger.warning(
                        "[HPA] %s escalado: %d → %d réplicas (%s)",
                        svc, prev, current, direction,
                    )
                self._prev_replicas[svc] = current

    def collect_cycle(self) -> dict:
        """Coleta um ciclo de métricas de todos os serviços."""
        self._cycle_count += 1
        ts = datetime.utcnow().isoformat()
        cycle_data = {
            "cycle": self._cycle_count,
            "timestamp": ts,
            "services": {},
        }

        with open(self._metrics_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            for svc in SERVICES:
                try:
                    p95, tput, cpu = self.monitor.get_metrics(svc)
                    writer.writerow([
                        self._cycle_count, ts, svc,
                        round(p95, 2), round(tput, 4), round(cpu, 4),
                    ])
                    cycle_data["services"][svc] = {
                        "p95_ms": round(p95, 2),
                        "throughput_rps": round(tput, 4),
                        "cpu_cores": round(cpu, 4),
                    }
                    logger.debug("[%s] P95=%.2fms", svc, p95)
                except Exception as exc:
                    logger.error("Erro ao coletar %s: %s", svc, exc, exc_info=True)

        self._check_hpa_scaling(self._cycle_count, ts)
        return cycle_data


# ─────────────────────────────────────────────────────────────────────────────
# Fases do experimento
# ─────────────────────────────────────────────────────────────────────────────

def _run_phase(collector: HPAMetricsCollector, label: str, cycles: int) -> dict:
    """Executa N ciclos de coleta e retorna metadados da fase."""
    logger.info(
        "┌─ FASE HPA: %s (%d ciclos × %ds = ~%d min) ──────────────────",
        label, cycles, SCRAPE_INTERVAL_S, (cycles * SCRAPE_INTERVAL_S) // 60,
    )
    start = datetime.utcnow()

    for i in range(cycles):
        logger.info("│  Ciclo %d/%d [%s]", i + 1, cycles, label)
        collector.collect_cycle()
        if i < cycles - 1:
            time.sleep(SCRAPE_INTERVAL_S)

    end = datetime.utcnow()
    duration_s = (end - start).total_seconds()
    logger.info("└─ FIM %s | Duração: %.0fs", label, duration_s)

    return {
        "name": label,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "duration_s": round(duration_s, 1),
        "cycles": cycles,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Experimento principal
# ─────────────────────────────────────────────────────────────────────────────

def run_hpa_baseline():
    """Executa o experimento de linha de base com HPA nas 3 fases."""
    logger.info("=" * 65)
    logger.info("  XAI-FinOps — Grupo de Controle: Kubernetes HPA")
    logger.info("  Iniciado em: %s", datetime.utcnow().isoformat())
    logger.info("=" * 65)

    apply_hpa()
    logger.info("Aguardando HPA estabilizar (30s)...")
    time.sleep(30)

    collector = HPAMetricsCollector()

    meta = {
        "experiment_id": datetime.utcnow().strftime("%Y%m%d_%H%M%S"),
        "type": "HPA_BASELINE",
        "start_time": datetime.utcnow().isoformat(),
        "hpa_config": {
            "cpu_target_percent": HPA_CPU_TARGET_PERCENT,
            "min_replicas": HPA_MIN_REPLICAS,
            "max_replicas": HPA_MAX_REPLICAS,
            "services": HPA_SERVICES,
        },
        "config": {
            "scrape_interval_s": SCRAPE_INTERVAL_S,
            "baseline_cycles": BASELINE_PHASE_CYCLES,
            "fault_cycles": FAULT_PHASE_CYCLES,
            "recovery_cycles": RECOVERY_CYCLES,
        },
        "phases": [],
    }

    # ── FASE 1: BASELINE ─────────────────────────────────────────────────────
    phase1 = _run_phase(collector, "BASELINE", BASELINE_PHASE_CYCLES)
    meta["phases"].append(phase1)

    if phase1.get("hpa_scale_events", 0) > 0:
        logger.warning(
            "ATENÇÃO: HPA escalou durante o baseline (%d eventos). "
            "O sistema pode não estar estável.",
            phase1.get("hpa_scale_events", 0),
        )

    # ── FASE 2: INJEÇÃO DE FALHAS ─────────────────────────────────────────────
    logger.info("")
    logger.info(
        "Aplicando falha Istio → productcatalogservice (delay=2,5s, 100%%)..."
    )
    fault_applied = kubectl_apply(FAULT_MANIFEST)

    if fault_applied:
        phase2 = _run_phase(collector, "FAULT_INJECTION", FAULT_PHASE_CYCLES)
    else:
        logger.error("Falha ao aplicar manifesto. Pulando Fase 2.")
        phase2 = {"name": "FAULT_INJECTION", "status": "SKIPPED"}

    meta["phases"].append(phase2)

    # ── FASE 3: RECUPERAÇÃO ───────────────────────────────────────────────────
    logger.info("")
    logger.info("Removendo falha. Observando recuperação com HPA...")
    fault_removed_at = datetime.utcnow()
    kubectl_delete(FAULT_MANIFEST)

    phase3 = _run_phase(collector, "RECOVERY", RECOVERY_CYCLES)
    meta["phases"].append(phase3)

    # ── Finalização ────────────────────────────────────────────────────────────
    remove_hpa()

    meta["end_time"] = datetime.utcnow().isoformat()
    meta["fault_removed_at"] = fault_removed_at.isoformat()

    meta_path = os.path.join(HPA_RESULTS_DIR, "experiment_meta.json")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)

    logger.info("")
    logger.info("=" * 65)
    logger.info("  BASELINE HPA CONCLUÍDO")
    logger.info("  Resultados em: %s/", HPA_RESULTS_DIR)
    logger.info("=" * 65)


if __name__ == "__main__":
    run_hpa_baseline()
