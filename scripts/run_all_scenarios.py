"""
run_all_scenarios.py — Executor de Múltiplos Runs para Validação Estatística

Roda N repetições independentes dos Cenários A, B e C do framework XAI-FinOps,
salvando resultados em diretórios separados e gerando CSV consolidado de métricas.
Compatível com retomada após interrupção (--start-run).

Uso:
    python run_all_scenarios.py [--runs N] [--scenarios A B C] [--cooldown S]

Exemplos:
    python run_all_scenarios.py --runs 5
    python run_all_scenarios.py --runs 5 --scenarios B C      # só B e C
    python run_all_scenarios.py --runs 5 --start-run 3        # retoma do run 3
    python run_all_scenarios.py --runs 5 --cooldown 60        # cooldown mais curto
"""

import argparse
import csv
import json
import logging
import os
import statistics
import subprocess
import sys
import time
import urllib.request
from datetime import datetime
from pathlib import Path

# ── Caminhos base ─────────────────────────────────────────────────────────────
SCRIPTS_DIR = Path(__file__).parent.resolve()
BASE_DIR    = SCRIPTS_DIR.parent.resolve()

# ── Serviços monitorados ───────────────────────────────────────────────────────
SERVICES = [
    "frontend",
    "productcatalogservice",
    "checkoutservice",
    "cartservice",
    "recommendationservice",
]

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("RunAll")


# ─────────────────────────────────────────────────────────────────────────────
# Utilitários de infraestrutura
# ─────────────────────────────────────────────────────────────────────────────

def check_prometheus(url: str = "http://localhost:9090") -> bool:
    """Retorna True se o Prometheus estiver acessível."""
    try:
        urllib.request.urlopen(f"{url}/-/healthy", timeout=5)
        return True
    except Exception:
        return False


def reset_replicas(namespace: str = "default") -> None:
    """Reseta todos os deployments monitorados para 1 réplica."""
    logger.info("  Resetando réplicas → 1 em todos os serviços...")
    for svc in SERVICES:
        result = subprocess.run(
            ["kubectl", "scale", "deploy", svc, "-n", namespace, "--replicas=1"],
            capture_output=True, text=True,
        )
        if result.returncode == 0:
            logger.info("    %s → 1", svc)
        else:
            logger.warning("    [WARN] %s: %s", svc, result.stderr.strip())


def remove_fault_manifests() -> None:
    """Remove qualquer VirtualService de injeção de falha que esteja ativo."""
    infra = BASE_DIR / "infrastructure"
    for manifest in [
        "fault-injection.yaml",
        "fault-cartservice.yaml",
        "fault-checkoutservice.yaml",
    ]:
        subprocess.run(
            ["kubectl", "delete", "-f", str(infra / manifest), "--ignore-not-found"],
            capture_output=True, text=True,
        )


def wait_for_prometheus(timeout_s: int = 120) -> bool:
    """Aguarda Prometheus ficar acessível, com timeout."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if check_prometheus():
            return True
        time.sleep(5)
    return False


# ─────────────────────────────────────────────────────────────────────────────
# Execução de um run individual
# ─────────────────────────────────────────────────────────────────────────────

def run_single_experiment(
    scenario: str,
    run_idx: int,
    cooldown_s: int,
    namespace: str,
) -> dict:
    """
    Executa uma repetição de um cenário e retorna dicionário com métricas.
    Salva stdout em logs_cenarioX_runY_stdout.txt.
    """
    results_dir = str(SCRIPTS_DIR / f"results_cenario{scenario}_run{run_idx}")
    log_dir     = str(SCRIPTS_DIR / f"logs_cenario{scenario}_run{run_idx}")
    stdout_log  = BASE_DIR / f"logs_cenario{scenario}_run{run_idx}_stdout.txt"

    env = os.environ.copy()
    env["XAI_RESULTS_DIR"] = results_dir
    env["XAI_LOG_DIR"]     = log_dir

    logger.info("┌─ Cenário %s | Run %d/%s ──────────────────────────────────",
                scenario, run_idx, "?")
    logger.info("│  Resultados → %s", results_dir)

    start = datetime.utcnow()

    with open(stdout_log, "w", encoding="utf-8") as f:
        proc = subprocess.run(
            [sys.executable, str(SCRIPTS_DIR / "run_experiment.py"),
             "--scenario", scenario],
            env=env,
            cwd=str(SCRIPTS_DIR),
            stdout=f,
            stderr=subprocess.STDOUT,
        )

    duration_s = (datetime.utcnow() - start).total_seconds()

    result = {
        "scenario":    scenario,
        "run":         run_idx,
        "timestamp":   start.isoformat(),
        "duration_s":  round(duration_s, 1),
        "exit_code":   proc.returncode,
        "results_dir": results_dir,
    }

    if proc.returncode != 0:
        logger.error("└─ FALHOU (exit %d). Log: %s", proc.returncode, stdout_log)
        result["status"] = "FAILED"
        return result

    # Extrai métricas do experiment_meta.json
    meta_path = Path(results_dir) / "experiment_meta.json"
    if meta_path.exists():
        with open(meta_path, encoding="utf-8") as f:
            meta = json.load(f)
        r = meta.get("results", {})
        result.update({
            "status":               "OK",
            "total_anomaly_events": r.get("total_anomaly_events", 0),
            "scale_up_events":      r.get("scale_up_events", 0),
            "suppressed_events":    r.get("suppressed_events", 0),
            "suppression_rate_pct": round(r.get("suppression_rate", 0) * 100, 1),
            "avg_pearson_score":    round(r.get("avg_pearson_score", 0), 4),
        })
    else:
        logger.warning("└─ experiment_meta.json não encontrado.")
        result["status"] = "NO_META"
        return result

    logger.info("└─ OK em %.0fs | anomalias=%d scale_up=%d supressao=%.1f%%",
                duration_s,
                result["total_anomaly_events"],
                result["scale_up_events"],
                result["suppression_rate_pct"])

    if cooldown_s > 0:
        logger.info("   Cooldown %ds antes do próximo run...", cooldown_s)
        time.sleep(cooldown_s)

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Resumo estatístico
# ─────────────────────────────────────────────────────────────────────────────

SUMMARY_FIELDS = [
    "scenario", "run", "timestamp", "duration_s", "exit_code", "status",
    "total_anomaly_events", "scale_up_events", "suppressed_events",
    "suppression_rate_pct", "avg_pearson_score", "results_dir",
]


def save_summary_csv(all_results: list[dict], output_path: Path) -> None:
    """Salva CSV incremental com todos os runs executados até agora."""
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SUMMARY_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_results)


def print_statistics(all_results: list[dict]) -> None:
    """Imprime média ± desvio padrão por cenário e métrica."""
    metrics = ["scale_up_events", "suppression_rate_pct", "avg_pearson_score"]
    ok_results = [r for r in all_results if r.get("status") == "OK"]

    logger.info("\n%s", "=" * 65)
    logger.info("  ESTATÍSTICAS POR CENÁRIO (runs com status OK)")
    logger.info("=" * 65)

    for scenario in ["A", "B", "C"]:
        sc = [r for r in ok_results if r["scenario"] == scenario]
        if not sc:
            continue
        logger.info("  Cenário %s (n=%d):", scenario, len(sc))
        for m in metrics:
            vals = [r[m] for r in sc if m in r]
            if not vals:
                continue
            mean = statistics.mean(vals)
            std  = statistics.stdev(vals) if len(vals) > 1 else 0.0
            logger.info("    %-25s %.4f ± %.4f", m, mean, std)

    logger.info("=" * 65)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Executor de múltiplos runs — validação estatística XAI-FinOps",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--runs",      type=int, default=5,
                        help="Número de repetições por cenário (default: 5)")
    parser.add_argument("--scenarios", nargs="+", choices=["A", "B", "C"],
                        default=["A", "B", "C"],
                        help="Cenários a executar (default: A B C)")
    parser.add_argument("--cooldown",  type=int, default=120,
                        help="Segundos de espera entre runs para estabilização (default: 120)")
    parser.add_argument("--start-run", type=int, default=1,
                        help="Índice inicial do run — para retomar sessão interrompida (default: 1)")
    parser.add_argument("--namespace", default="default",
                        help="Namespace Kubernetes (default: default)")
    args = parser.parse_args()

    runs_to_do = list(range(args.start_run, args.runs + 1))
    n_total    = len(args.scenarios) * len(runs_to_do)
    est_min    = n_total * 35 + (n_total - 1) * (args.cooldown // 60)

    logger.info("=" * 65)
    logger.info("  XAI-FinOps — Validação Estatística Multi-Run")
    logger.info("  Cenários  : %s", args.scenarios)
    logger.info("  Runs      : %s (início: run %d)", args.runs, args.start_run)
    logger.info("  Cooldown  : %ds entre runs", args.cooldown)
    logger.info("  Total runs: %d | Estimativa: ~%dh%02dmin",
                n_total, est_min // 60, est_min % 60)
    logger.info("=" * 65)

    # Verifica Prometheus
    logger.info("Verificando Prometheus...")
    if not wait_for_prometheus(timeout_s=60):
        logger.error("Prometheus não acessível em localhost:9090.")
        logger.error("Execute: kubectl port-forward svc/prometheus -n istio-system 9090:9090")
        sys.exit(1)
    logger.info("Prometheus OK.\n")

    all_results: list[dict] = []
    summary_path = SCRIPTS_DIR / "results_multi_run_summary.csv"

    for scenario in args.scenarios:
        for run_idx in runs_to_do:
            logger.info("")
            logger.info("Preparando Cenário %s Run %d...", scenario, run_idx)

            # Limpa estado herdado de runs anteriores
            remove_fault_manifests()
            reset_replicas(args.namespace)
            time.sleep(10)  # aguarda pods estabilizarem após reset

            result = run_single_experiment(scenario, run_idx, args.cooldown, args.namespace)
            all_results.append(result)

            # Salva CSV incremental após cada run (seguro contra interrupções)
            save_summary_csv(all_results, summary_path)
            logger.info("  Summary atualizado: %s", summary_path)

    # Estatísticas finais
    print_statistics(all_results)

    ok  = sum(1 for r in all_results if r.get("status") == "OK")
    err = len(all_results) - ok

    logger.info("\n  CONCLUÍDO | %d runs OK | %d falhas", ok, err)
    logger.info("  Summary CSV: %s", summary_path)

    if err:
        logger.warning("  %d runs falharam — verifique os logs em logs_cenarioX_runY_stdout.txt", err)
        sys.exit(1)


if __name__ == "__main__":
    main()
