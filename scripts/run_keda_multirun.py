"""
run_keda_multirun.py — Executor de N Runs Independentes do Cenário KEDA

Roda N repetições do cenário KEDA (Cenário A: productcatalogservice, 2,5s),
salvando resultados em diretórios separados (results_keda_run1/, results_keda_run2/, ...).
Gera CSV consolidado keda_multirun_summary.csv ao final.

Uso:
    cd master-experimentation-project/scripts/
    python run_keda_multirun.py [--runs N] [--cooldown S]

Exemplos:
    python run_keda_multirun.py --runs 5
    python run_keda_multirun.py --runs 5 --cooldown 60
    python run_keda_multirun.py --runs 5 --start-run 3   # retoma do run 3
"""

import argparse
import csv
import json
import logging
import os
import subprocess
import sys
import time
import urllib.request
from datetime import datetime
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.resolve()
BASE_DIR    = SCRIPTS_DIR.parent.resolve()

LOG_FILE = BASE_DIR / "keda_multirun.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(LOG_FILE), mode="a", encoding="utf-8"),
    ],
)
logger = logging.getLogger("KEDAMultiRun")

SERVICES = [
    "frontend",
    "productcatalogservice",
    "checkoutservice",
    "cartservice",
    "recommendationservice",
]


# ── Utilitários ───────────────────────────────────────────────────────────────

def check_prometheus(url: str = "http://localhost:9090") -> bool:
    try:
        urllib.request.urlopen(f"{url}/-/healthy", timeout=5)
        return True
    except Exception:
        return False


def check_keda_operator() -> bool:
    r = subprocess.run(
        ["kubectl", "get", "pods", "-n", "keda",
         "-l", "app=keda-operator",
         "-o", "jsonpath={.items[0].status.phase}"],
        capture_output=True, text=True,
    )
    return r.stdout.strip() == "Running"


def reset_replicas(namespace: str = "default") -> None:
    logger.info("  Resetando réplicas para 1 em todos os serviços...")
    for svc in SERVICES:
        subprocess.run(
            ["kubectl", "scale", "deploy", svc,
             "-n", namespace, "--replicas=1"],
            capture_output=True, text=True,
        )


def remove_scaled_objects(namespace: str = "default") -> None:
    """Remove ScaledObjects KEDA residuais de runs anteriores."""
    for svc in SERVICES:
        subprocess.run(
            ["kubectl", "delete", "scaledobject",
             f"keda-p95-{svc}", "-n", namespace, "--ignore-not-found"],
            capture_output=True, text=True,
        )
    logger.info("  ScaledObjects KEDA removidos.")


def remove_fault_manifests() -> None:
    infra = BASE_DIR / "infrastructure"
    for manifest in ["fault-injection.yaml", "fault-cartservice.yaml",
                     "fault-checkoutservice.yaml"]:
        path = infra / manifest
        if path.exists():
            subprocess.run(
                ["kubectl", "delete", "-f", str(path), "--ignore-not-found"],
                capture_output=True, text=True,
            )


# ── Execução de um run ────────────────────────────────────────────────────────

def run_single_keda(run_idx: int, cooldown_s: int, namespace: str) -> dict:
    """Executa uma repetição do cenário KEDA, salvando em results_keda_run{N}/."""
    results_dir = str(SCRIPTS_DIR / f"results_keda_run{run_idx}")
    stdout_log  = BASE_DIR / f"logs_keda_run{run_idx}_stdout.txt"

    env = os.environ.copy()
    env["KEDA_RESULTS_DIR"] = results_dir   # keda_scenario.py lê esta variável

    logger.info("┌─ KEDA Run %d ─────────────────────────────────────────────", run_idx)
    logger.info("│  Resultados → %s", results_dir)
    logger.info("│  Log stdout → %s", stdout_log)

    start = datetime.utcnow()

    with open(stdout_log, "w", encoding="utf-8") as f:
        proc = subprocess.run(
            [sys.executable, str(SCRIPTS_DIR / "keda_scenario.py")],
            env=env,
            cwd=str(SCRIPTS_DIR),
            stdout=f,
            stderr=subprocess.STDOUT,
        )

    duration_s = (datetime.utcnow() - start).total_seconds()

    result = {
        "run":        run_idx,
        "timestamp":  start.isoformat(),
        "duration_s": round(duration_s, 1),
        "exit_code":  proc.returncode,
        "results_dir": results_dir,
    }

    if proc.returncode != 0:
        logger.error("└─ FALHOU (exit %d). Log: %s", proc.returncode, stdout_log)
        result["status"] = "FAILED"
        return result

    # Extrai eventos do keda_events.csv gerado
    events_path = Path(results_dir) / "keda_events.csv"
    meta_path   = Path(results_dir) / "experiment_meta.json"

    scale_ups_total   = 0
    scale_ups_correct = 0   # eventos em productcatalogservice durante FAULT
    false_positives   = 0   # scale-ups durante BASELINE ou em serviço errado

    if events_path.exists():
        with open(events_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("direction") != "SCALE_UP":
                    continue
                scale_ups_total += 1
                svc   = row.get("service", "")
                phase = row.get("phase", "")
                if phase == "FAULT_INJECTION" and svc == "productcatalogservice":
                    scale_ups_correct += 1
                else:
                    false_positives += 1

    result.update({
        "status":             "OK",
        "scale_up_events":    scale_ups_total,
        "correct_scale_ups":  scale_ups_correct,   # TP
        "false_positives":    false_positives,      # FP
        "true_negatives":     1 if scale_ups_total == 0 else 0,
    })

    logger.info(
        "└─ OK em %.0fs | scale_ups=%d correct=%d FP=%d",
        duration_s, scale_ups_total, scale_ups_correct, false_positives,
    )

    if cooldown_s > 0:
        logger.info("   Cooldown %ds antes do próximo run...", cooldown_s)
        time.sleep(cooldown_s)

    return result


# ── CSV de sumário ─────────────────────────────────────────────────────────────

SUMMARY_FIELDS = [
    "run", "timestamp", "duration_s", "exit_code", "status",
    "scale_up_events", "correct_scale_ups", "false_positives", "results_dir",
]


def save_summary(results: list[dict], path: Path) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SUMMARY_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="KEDA multi-run executor")
    parser.add_argument("--runs",      type=int, default=5)
    parser.add_argument("--cooldown",  type=int, default=120,
                        help="Segundos entre runs (default 120)")
    parser.add_argument("--start-run", type=int, default=1,
                        help="Run inicial (para retomada)")
    parser.add_argument("--namespace", default="default")
    args = parser.parse_args()

    runs_to_do = list(range(args.start_run, args.runs + 1))
    est_min    = len(runs_to_do) * 35 + (len(runs_to_do) - 1) * (args.cooldown // 60)

    logger.info("=" * 65)
    logger.info("  KEDA Multi-Run — Cenário A (productcatalogservice 2,5s)")
    logger.info("  Runs      : %s", runs_to_do)
    logger.info("  Cooldown  : %ds entre runs", args.cooldown)
    logger.info("  Estimativa: ~%dh%02dmin", est_min // 60, est_min % 60)
    logger.info("=" * 65)

    # Pré-requisitos
    if not check_prometheus():
        logger.error("Prometheus não acessível em localhost:9090. Execute port-forward.")
        sys.exit(1)
    if not check_keda_operator():
        logger.error("keda-operator não está Running. Verifique: kubectl get pods -n keda")
        sys.exit(1)
    logger.info("Prometheus OK | KEDA operator OK\n")

    all_results: list[dict] = []
    summary_path = SCRIPTS_DIR / "keda_multirun_summary.csv"

    # Carrega runs anteriores se retomando (--start-run > 1)
    if args.start_run > 1 and summary_path.exists():
        with open(summary_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                for k in ("run", "exit_code", "scale_up_events",
                          "correct_scale_ups", "false_positives"):
                    if row.get(k):
                        row[k] = int(row[k])
                if row.get("duration_s"):
                    row["duration_s"] = float(row["duration_s"])
                all_results.append(dict(row))
        logger.info("Carregados %d run(s) anteriores do CSV.", len(all_results))

    for run_idx in runs_to_do:
        logger.info("\nPreparando Run %d...", run_idx)
        remove_fault_manifests()
        remove_scaled_objects(args.namespace)
        reset_replicas(args.namespace)
        time.sleep(15)

        result = run_single_keda(run_idx, args.cooldown, args.namespace)
        all_results.append(result)
        save_summary(all_results, summary_path)
        logger.info("  Summary atualizado: %s", summary_path)

    # Estatísticas
    ok = [r for r in all_results if r.get("status") == "OK"]
    tp_total = sum(r.get("correct_scale_ups", 0) for r in ok)
    fp_total = sum(r.get("false_positives", 0)  for r in ok)
    fn_total = sum(1 for r in ok if r.get("correct_scale_ups", 0) == 0)

    precision = tp_total / (tp_total + fp_total) if (tp_total + fp_total) > 0 else 0
    recall    = tp_total / (tp_total + fn_total) if (tp_total + fn_total) > 0 else 0
    f1        = (2 * precision * recall / (precision + recall)
                 if (precision + recall) > 0 else 0)

    logger.info("\n%s", "=" * 65)
    logger.info("  RESULTADOS FINAIS KEDA (n=%d runs OK)", len(ok))
    logger.info("  TP (scale_up correto)  : %d", tp_total)
    logger.info("  FP (falso positivo)    : %d", fp_total)
    logger.info("  FN (falha não detectada): %d", fn_total)
    logger.info("  Precision : %.1f%%", precision * 100)
    logger.info("  Recall    : %.1f%%", recall * 100)
    logger.info("  F1        : %.3f", f1)
    logger.info("  Summary CSV: %s", summary_path)
    logger.info("=" * 65)


if __name__ == "__main__":
    main()
