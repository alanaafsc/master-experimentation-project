"""
keda_scenario.py — Cenário B: KEDA com scaler de P95 via Prometheus.

Executa o mesmo protocolo de 3 fases (Baseline/Falha/Recuperação) do
Cenário A (XAI-FinOps), substituindo o framework pelo KEDA com ScaledObjects
baseados no P95 de latência Istio.

Função científica (§6.8 da dissertação):
  KEDA detecta degradação via P95 e escala TODOS os serviços afetados,
  sem identificação de causa raiz. Contrasta com o XAI-FinOps, que usa
  correlação de Pearson + restrição topológica para escalar apenas o
  serviço causador (productcatalogservice).

  Hipótese experimental:
    - KEDA detecta a falha (P95 > 500ms) ✓
    - KEDA escala frontend + productcatalogservice + recommendationservice
      (escalamento excessivo — sem RCA) ✗
    - KEDA não identifica a causa raiz ✗
    - Custo FinOps maior que XAI-FinOps (mais réplicas desnecessárias)

Saídas em results_keda/:
  metrics_raw.csv       — série temporal de P95, throughput e CPU por serviço
  keda_events.csv       — log de mudanças de réplicas detectadas a cada ciclo
  experiment_meta.json  — cronograma das fases e configuração dos ScaledObjects

Pré-requisitos:
  - KEDA instalado (kubectl get pods -n keda — todos Running)
  - Prometheus port-forward ativo: kubectl port-forward svc/prometheus -n istio-system 9090:9090
  - Istio + Online Boutique em execução no namespace default
  - Locust gerando carga (50 usuários, spawn rate 5)

Uso:
    cd master-experimentation-project/
    python scripts/keda_scenario.py
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
KEDA_RESULTS_DIR = "results_keda"
KEDA_LOG_DIR = os.path.join(LOG_DIR, "keda")

os.makedirs(KEDA_RESULTS_DIR, exist_ok=True)
os.makedirs(KEDA_LOG_DIR, exist_ok=True)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.FileHandler(
            os.path.join(KEDA_LOG_DIR, "keda_scenario.log"), encoding="utf-8"
        ),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("KEDAScenario")

# ── Parâmetros KEDA ───────────────────────────────────────────────────────────
KEDA_P95_THRESHOLD_MS = 500          # Limiar de P95 (ms) — mesmo SLA do config.py
KEDA_MIN_REPLICAS = 1
KEDA_MAX_REPLICAS = 5
KEDA_POLLING_INTERVAL = 30           # Alinhado ao SCRAPE_INTERVAL_S
KEDA_COOLDOWN_PERIOD = 60            # 2 ciclos — permite observar scale-down na recuperação
RECOVERY_CYCLES = 10

# URL do Prometheus acessível de dentro do cluster (usada pelo operador KEDA)
PROMETHEUS_IN_CLUSTER = "http://prometheus.istio-system.svc.cluster.local:9090"

# Serviços que receberão ScaledObjects — todos os monitorados
KEDA_SERVICES = [
    "frontend",
    "productcatalogservice",
    "checkoutservice",
    "recommendationservice",
    "cartservice",
]

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


def _kubectl_stdin(yaml_content: str, *args) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["kubectl"] + list(args) + ["-f", "-"],
        input=yaml_content,
        capture_output=True, text=True,
    )


def kubectl_apply_file(manifest: str) -> bool:
    r = _kubectl("apply", "-f", manifest)
    if r.returncode != 0:
        logger.error("kubectl apply falhou:\n%s", r.stderr.strip())
        return False
    logger.info("kubectl apply %s — OK", os.path.basename(manifest))
    return True


def kubectl_delete_file(manifest: str) -> bool:
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
# Geração e aplicação dos ScaledObjects KEDA
# ─────────────────────────────────────────────────────────────────────────────

def _scaled_object_yaml(service: str) -> str:
    """
    Gera o YAML de um ScaledObject KEDA com scaler Prometheus de P95.

    A query PromQL retorna o P95 de latência em milissegundos do serviço.
    O KEDA compara esse valor com o threshold e escala quando P95 > threshold.
    """
    query = (
        f"histogram_quantile(0.95, sum(rate("
        f"istio_request_duration_milliseconds_bucket{{"
        f"destination_service_name=\\\"{service}\\\""
        f"}}[2m])) by (le))"
    )
    return f"""apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: keda-p95-{service}
  namespace: {KUBERNETES_NAMESPACE}
spec:
  scaleTargetRef:
    name: {service}
  pollingInterval: {KEDA_POLLING_INTERVAL}
  cooldownPeriod: {KEDA_COOLDOWN_PERIOD}
  minReplicaCount: {KEDA_MIN_REPLICAS}
  maxReplicaCount: {KEDA_MAX_REPLICAS}
  triggers:
  - type: prometheus
    metadata:
      serverAddress: {PROMETHEUS_IN_CLUSTER}
      metricName: p95_latency_ms_{service}
      threshold: "{KEDA_P95_THRESHOLD_MS}"
      query: >-
        histogram_quantile(0.95, sum(rate(istio_request_duration_milliseconds_bucket{{destination_service_name="{service}"}}[2m])) by (le))
"""


def apply_keda_scaled_objects():
    logger.info(
        "Instalando KEDA ScaledObjects (P95 threshold=%dms)...",
        KEDA_P95_THRESHOLD_MS,
    )
    for svc in KEDA_SERVICES:
        yaml = _scaled_object_yaml(svc)
        r = _kubectl_stdin(yaml, "apply")
        if r.returncode == 0:
            logger.info("  ScaledObject criado: keda-p95-%s", svc)
        elif "already exists" in r.stderr or "configured" in r.stdout:
            logger.info("  ScaledObject já existe: keda-p95-%s (atualizado)", svc)
        else:
            logger.error(
                "  Falha ao criar ScaledObject para %s:\n%s", svc, r.stderr.strip()
            )


def remove_keda_scaled_objects():
    logger.info("Removendo KEDA ScaledObjects...")
    for svc in KEDA_SERVICES:
        r = _kubectl(
            "delete", "scaledobject", f"keda-p95-{svc}",
            "-n", KUBERNETES_NAMESPACE,
            "--ignore-not-found",
        )
        if r.returncode == 0:
            logger.info("  ScaledObject removido: keda-p95-%s", svc)
        else:
            logger.warning(
                "  Falha ao remover keda-p95-%s: %s", svc, r.stderr.strip()
            )


# ─────────────────────────────────────────────────────────────────────────────
# Coletor de métricas
# ─────────────────────────────────────────────────────────────────────────────

class KEDAMetricsCollector:
    """
    Coleta métricas de latência P95 e detecta mudanças de réplicas causadas
    pelo KEDA. Não interfere no escalonamento — observação passiva.
    """

    def __init__(self):
        self.monitor = MonitoringModule()
        self._cycle_count = 0
        self._prev_replicas: dict[str, int] = {
            svc: get_replicas(svc) for svc in KEDA_SERVICES
        }

        self._metrics_path = os.path.join(KEDA_RESULTS_DIR, "metrics_raw.csv")
        self._events_path = os.path.join(KEDA_RESULTS_DIR, "keda_events.csv")
        self._init_files()

        logger.info(
            "KEDAMetricsCollector iniciado. Réplicas iniciais: %s",
            self._prev_replicas,
        )

    def _init_files(self):
        with open(self._metrics_path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                "cycle", "timestamp", "phase", "service",
                "p95_ms", "throughput_rps", "cpu_cores", "replicas",
            ])
        with open(self._events_path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                "cycle", "timestamp", "phase", "service",
                "from_replicas", "to_replicas", "direction",
            ])

    def _check_keda_scaling(self, cycle: int, ts: str, phase: str):
        """Compara réplicas atuais vs. anteriores para detectar ação do KEDA."""
        with open(self._events_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            for svc in KEDA_SERVICES:
                current = get_replicas(svc)
                prev = self._prev_replicas.get(svc, -1)
                if current != prev and current != -1 and prev != -1:
                    direction = "SCALE_UP" if current > prev else "SCALE_DOWN"
                    writer.writerow([cycle, ts, phase, svc, prev, current, direction])
                    logger.warning(
                        "[KEDA] %s escalado: %d → %d réplicas (%s)",
                        svc, prev, current, direction,
                    )
                self._prev_replicas[svc] = current

    def collect_cycle(self, phase: str) -> dict:
        """Coleta um ciclo de métricas de todos os serviços."""
        self._cycle_count += 1
        ts = datetime.utcnow().isoformat()
        cycle_data = {"cycle": self._cycle_count, "timestamp": ts, "services": {}}

        with open(self._metrics_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            for svc in SERVICES:
                replicas = get_replicas(svc) if svc in KEDA_SERVICES else -1
                try:
                    p95, tput, cpu = self.monitor.get_metrics(svc)
                    writer.writerow([
                        self._cycle_count, ts, phase, svc,
                        round(p95, 2), round(tput, 4), round(cpu, 4), replicas,
                    ])
                    cycle_data["services"][svc] = {
                        "p95_ms": round(p95, 2),
                        "throughput_rps": round(tput, 4),
                        "cpu_cores": round(cpu, 4),
                        "replicas": replicas,
                    }
                    logger.debug("[%s] P95=%.2fms replicas=%d", svc, p95, replicas)
                except Exception as exc:
                    logger.error("Erro ao coletar %s: %s", svc, exc, exc_info=True)

        self._check_keda_scaling(self._cycle_count, ts, phase)
        return cycle_data


# ─────────────────────────────────────────────────────────────────────────────
# Fases do experimento
# ─────────────────────────────────────────────────────────────────────────────

def _run_phase(
    collector: KEDAMetricsCollector, label: str, cycles: int
) -> dict:
    logger.info(
        "┌─ FASE KEDA: %s (%d ciclos × %ds = ~%d min) ─────────────────",
        label, cycles, SCRAPE_INTERVAL_S, (cycles * SCRAPE_INTERVAL_S) // 60,
    )
    start = datetime.utcnow()

    for i in range(cycles):
        logger.info("│  Ciclo %d/%d [%s]", i + 1, cycles, label)
        collector.collect_cycle(phase=label)
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

def run_keda_scenario():
    logger.info("=" * 65)
    logger.info("  XAI-FinOps — Cenário B: KEDA + Prometheus P95")
    logger.info("  Iniciado em: %s", datetime.utcnow().isoformat())
    logger.info("=" * 65)

    # Garante réplicas iniciais = 1 em todos os serviços
    logger.info("Resetando réplicas para 1 em todos os serviços...")
    for svc in KEDA_SERVICES:
        _kubectl(
            "scale", "deployment", svc,
            "--replicas=1",
            "-n", KUBERNETES_NAMESPACE,
        )
        logger.info("  %s → 1 réplica", svc)

    apply_keda_scaled_objects()
    logger.info("Aguardando KEDA estabilizar (60s)...")
    time.sleep(60)

    collector = KEDAMetricsCollector()

    meta = {
        "experiment_id": datetime.utcnow().strftime("%Y%m%d_%H%M%S"),
        "type": "KEDA_P95_SCENARIO",
        "start_time": datetime.utcnow().isoformat(),
        "keda_config": {
            "p95_threshold_ms": KEDA_P95_THRESHOLD_MS,
            "min_replicas": KEDA_MIN_REPLICAS,
            "max_replicas": KEDA_MAX_REPLICAS,
            "polling_interval_s": KEDA_POLLING_INTERVAL,
            "cooldown_period_s": KEDA_COOLDOWN_PERIOD,
            "prometheus_address": PROMETHEUS_IN_CLUSTER,
            "services": KEDA_SERVICES,
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

    # ── FASE 2: INJEÇÃO DE FALHA ──────────────────────────────────────────────
    logger.info("")
    logger.info(
        "Aplicando falha Istio → productcatalogservice (delay=2.5s, 100%%)..."
    )
    fault_applied = kubectl_apply_file(FAULT_MANIFEST)

    if fault_applied:
        phase2 = _run_phase(collector, "FAULT_INJECTION", FAULT_PHASE_CYCLES)
    else:
        logger.error("Falha ao aplicar manifesto. Pulando Fase 2.")
        phase2 = {"name": "FAULT_INJECTION", "status": "SKIPPED"}

    meta["phases"].append(phase2)
    meta["fault_injected_at"] = phase2.get("start", "")

    # ── FASE 3: RECUPERAÇÃO ───────────────────────────────────────────────────
    logger.info("")
    logger.info("Removendo falha. Observando recuperação com KEDA...")
    fault_removed_at = datetime.utcnow()
    kubectl_delete_file(FAULT_MANIFEST)

    phase3 = _run_phase(collector, "RECOVERY", RECOVERY_CYCLES)
    meta["phases"].append(phase3)

    # ── Limpeza e finalização ──────────────────────────────────────────────────
    remove_keda_scaled_objects()

    meta["end_time"] = datetime.utcnow().isoformat()
    meta["fault_removed_at"] = fault_removed_at.isoformat()

    meta_path = os.path.join(KEDA_RESULTS_DIR, "experiment_meta.json")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)

    logger.info("")
    logger.info("=" * 65)
    logger.info("  CENÁRIO B (KEDA) CONCLUÍDO")
    logger.info("  Resultados em: %s/", KEDA_RESULTS_DIR)
    logger.info("=" * 65)


if __name__ == "__main__":
    run_keda_scenario()
