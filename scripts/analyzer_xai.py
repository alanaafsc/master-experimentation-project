"""
analyzer_xai.py — Módulo de Análise, Planejamento e Execução (MAPE-K: Analyze + Plan + Execute)

Este módulo implementa o núcleo do framework XAI-FinOps. Orquestra as três
fases internas do ciclo MAPE-K que ocorrem após a coleta de métricas:

  Analyze  : Detecta anomalias via Regra 3σ e identifica a causa raiz via
             Correlação de Pearson sobre o grafo de dependências.

  Plan     : Consulta o ScalerModule (ARIMA) para confirmar se a anomalia
             representa uma tendência persistente ou uma oscilação passageira.

  Execute  : Aplica a ação de escalonamento no serviço CAUSA RAIZ (não no
             serviço sintomático), otimizando o custo FinOps da operação.

A cada evento de escalonamento, o XAIReporter produz evidências visuais e
relatório JSON que justificam a decisão perante o operador (SRE).
"""

import csv
import logging
import os
import time
from datetime import datetime

import numpy as np

from config import (
    BASELINE_WINDOW,
    ANOMALY_K,
    PEARSON_THRESHOLD,
    SCALE_UP_REPLICAS,
    SCRAPE_INTERVAL_S,
    SERVICES,
    RESULTS_DIR,
    LOG_DIR,
)
from executor import ExecutorModule
from monitor import MonitoringModule
from scaler import ScalerModule
from xai_reporter import XAIReporter

# ── Logging ───────────────────────────────────────────────────────────────────
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(RESULTS_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "mapek.log"), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("XAIAnalyzer")


class XAIAnalyzer:
    """
    Orquestrador do ciclo MAPE-K explicável.

    Attributes:
        monitor    : MonitoringModule — coleta métricas (fase Monitor)
        scaler     : ScalerModule    — predição ARIMA (fase Plan)
        executor   : ExecutorModule  — ação Kubernetes (fase Execute)
        reporter   : XAIReporter     — evidências XAI
        history    : dict            — séries temporais de P95 por serviço
        event_log  : list            — log de todos os eventos de anomalia
    """

    def __init__(self):
        self.monitor = MonitoringModule()
        self.scaler = ScalerModule()
        self.executor = ExecutorModule()
        self.reporter = XAIReporter()

        # Histórico: janela deslizante de até 2×BASELINE_WINDOW pontos
        self.history: dict[str, list[float]] = {svc: [] for svc in SERVICES}
        self.event_log: list[dict] = []
        self._cycle_count: int = 0

        logger.info("XAIAnalyzer iniciado. Serviços monitorados: %s", SERVICES)

    # ─────────────────────────────────────────────────────────────────────────
    # ANALYZE — Fase 1: Detecção de Anomalia pela Regra k-σ
    # ─────────────────────────────────────────────────────────────────────────

    def detect_anomaly(self, service: str, current_val: float) -> tuple[bool, dict]:
        """
        Aplica a Regra k-σ para detectar se current_val é anômalo.

        Limite superior de controle (UCL):
            UCL_k = μ_k + k · σ_k

        onde μ_k e σ_k são calculados sobre as últimas BASELINE_WINDOW
        observações do serviço k, e k = ANOMALY_K (padrão: 3).

        A escolha de k=3 resulta em P(falso positivo) ≈ 0,27% para
        distribuições aproximadamente normais (Regra Empírica 68-95-99,7).

        Args:
            service     : nome do serviço monitorado
            current_val : latência P95 observada em ms

        Returns:
            (is_anomalous, evidence) — booleano e dicionário de evidências XAI
        """
        data = self.history[service]

        if len(data) < BASELINE_WINDOW:
            logger.info("[%s] Baseline: %d/%d pontos.", service, len(data), BASELINE_WINDOW)
            return False, {}

        arr = np.array(data, dtype=float)
        mu = float(np.mean(arr))
        sigma = float(np.std(arr, ddof=1))   # desvio padrão amostral (ddof=1)
        ucl = mu + ANOMALY_K * sigma
        lcl = max(0.0, mu - ANOMALY_K * sigma)
        is_anomalous = current_val > ucl

        evidence = {
            "service": service,
            "timestamp": datetime.utcnow().isoformat(),
            "observed_p95_ms": round(current_val, 2),
            "baseline_mean_ms": round(mu, 2),
            "baseline_std_ms": round(sigma, 2),
            "k_factor": ANOMALY_K,
            "threshold_3sigma_ms": round(ucl, 2),
            "lower_control_limit_ms": round(lcl, 2),
            "anomaly_detected": is_anomalous,
        }

        if is_anomalous:
            logger.warning(
                "[ANOMALY][%s] P95=%.2fms > UCL=%.2fms (μ=%.2f, σ=%.2f, k=%g)",
                service, current_val, ucl, mu, sigma, ANOMALY_K,
            )
        else:
            logger.info(
                "[OK][%s] P95=%.2fms ≤ UCL=%.2fms", service, current_val, ucl
            )

        return is_anomalous, evidence

    # ─────────────────────────────────────────────────────────────────────────
    # ANALYZE — Fase 2: Identificação de Causa Raiz via Pearson
    # ─────────────────────────────────────────────────────────────────────────

    def identify_root_cause(self, trigger_service: str) -> tuple[str, float, dict]:
        """
        Identifica o serviço causa raiz avaliando a correlação de Pearson entre
        o serviço disparador e todos os candidatos monitorados.

        O grau de influência S_{k,m} entre os serviços k e m é:

            S_{k,m} = (Σ[(x_i - μ_x)(y_i - μ_y)]) / (n · σ_x · σ_y)

        O candidato com maior S_{k,m} > PEARSON_THRESHOLD é identificado
        como causa raiz. Se nenhum superar o limiar, assume-se que o serviço
        disparador é ele mesmo a causa raiz.

        Args:
            trigger_service : serviço onde a anomalia foi detectada

        Returns:
            (root_cause, best_score, evidence)
        """
        best_score = 0.0
        root_cause = trigger_service
        all_scores: dict[str, float] = {}

        for candidate in SERVICES:
            if candidate == trigger_service:
                continue

            hist_t = self.history.get(trigger_service, [])
            hist_c = self.history.get(candidate, [])
            min_len = min(len(hist_t), len(hist_c))

            if min_len < 10:
                continue

            arr_t = np.array(hist_t[-min_len:], dtype=float)
            arr_c = np.array(hist_c[-min_len:], dtype=float)
            corr = float(np.corrcoef(arr_t, arr_c)[0, 1])
            corr = 0.0 if np.isnan(corr) else corr
            all_scores[candidate] = round(corr, 4)

            if corr > best_score:
                best_score = corr
                if corr > PEARSON_THRESHOLD:
                    root_cause = candidate

        evidence = {
            "trigger_service": trigger_service,
            "root_cause_identified": root_cause,
            "pearson_scores": all_scores,
            "best_score": round(best_score, 4),
            "threshold": PEARSON_THRESHOLD,
            "interpretation": (
                f"Causa raiz: {root_cause} (S={best_score:.4f} > {PEARSON_THRESHOLD})."
                if best_score > PEARSON_THRESHOLD
                else f"Sem correlação dominante. Causa raiz assumida: {trigger_service}."
            ),
        }

        logger.info(
            "[XAI] Causa raiz: %s | Melhor score Pearson: %.4f (threshold: %.2f)",
            root_cause, best_score, PEARSON_THRESHOLD,
        )
        return root_cause, best_score, evidence

    # ─────────────────────────────────────────────────────────────────────────
    # Ciclo MAPE-K — uma iteração
    # ─────────────────────────────────────────────────────────────────────────

    def _collect_cycle(self) -> dict:
        """
        Executa uma iteração completa do ciclo MAPE-K para todos os serviços.

        Retorna o dicionário de dados do ciclo (timestamp + métricas coletadas).
        Eventos de anomalia (se houver) são adicionados a self.event_log e
        salvos via XAIReporter.
        """
        self._cycle_count += 1
        cycle_data = {
            "cycle": self._cycle_count,
            "timestamp": datetime.utcnow().isoformat(),
            "services": {},
        }

        for service in SERVICES:
            try:
                # ── MONITOR ──────────────────────────────────────────────────
                p95, throughput, cpu = self.monitor.get_metrics(service)
                cycle_data["services"][service] = {
                    "p95_ms": round(p95, 2),
                    "throughput_rps": round(throughput, 4),
                    "cpu_cores": round(cpu, 4),
                }

                # ── ANALYZE — Detecção ────────────────────────────────────────
                is_anomalous, anomaly_evidence = self.detect_anomaly(service, p95)

                if is_anomalous:
                    # ── ANALYZE — Causa Raiz ──────────────────────────────────
                    root_cause, best_score, correlation_evidence = (
                        self.identify_root_cause(service)
                    )

                    # ── PLAN ─────────────────────────────────────────────────
                    should_scale, arima_evidence = self.scaler.predict_trend(
                        self.history[root_cause]
                    )

                    # ── EXECUTE ───────────────────────────────────────────────
                    decision = "NO_ACTION"
                    execute_result = {}
                    if should_scale and best_score > PEARSON_THRESHOLD:
                        execute_result = self.executor.scale_service(
                            root_cause, replicas=SCALE_UP_REPLICAS
                        )
                        decision = f"SCALE_UP:{root_cause}:{SCALE_UP_REPLICAS}_replicas"
                    else:
                        reason = (
                            f"Pearson={best_score:.4f} < {PEARSON_THRESHOLD}"
                            if best_score <= PEARSON_THRESHOLD
                            else "ARIMA: tendência de queda ou estabilização"
                        )
                        logger.info("[EXECUTE] Escalonamento suprimido — %s", reason)

                    # ── XAI EVENT ─────────────────────────────────────────────
                    event = {
                        "cycle": self._cycle_count,
                        "timestamp": datetime.utcnow().isoformat(),
                        "trigger_service": service,
                        "root_cause_service": root_cause,
                        "decision": decision,
                        "anomaly_evidence": anomaly_evidence,
                        "correlation_evidence": correlation_evidence,
                        "arima_evidence": arima_evidence,
                        "execute_result": execute_result,
                    }
                    self.event_log.append(event)
                    self.reporter.generate_report(event, self.history)

                # ── Atualiza histórico (janela deslizante) ────────────────────
                self.history[service].append(p95)
                max_len = BASELINE_WINDOW * 2
                if len(self.history[service]) > max_len:
                    self.history[service].pop(0)

            except Exception as exc:
                logger.error(
                    "[ERROR] Ciclo %d, serviço %s: %s",
                    self._cycle_count, service, exc, exc_info=True,
                )

        self._flush_metrics(cycle_data)
        return cycle_data

    def _flush_metrics(self, cycle_data: dict):
        """Persiste as métricas brutas do ciclo em CSV para análise posterior."""
        path = os.path.join(RESULTS_DIR, "metrics_raw.csv")
        file_exists = os.path.isfile(path)
        with open(path, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow([
                    "cycle", "timestamp", "service",
                    "p95_ms", "throughput_rps", "cpu_cores",
                ])
            for svc, vals in cycle_data["services"].items():
                writer.writerow([
                    cycle_data["cycle"], cycle_data["timestamp"], svc,
                    vals["p95_ms"], vals["throughput_rps"], vals["cpu_cores"],
                ])

    # ─────────────────────────────────────────────────────────────────────────
    # Loop contínuo (uso standalone)
    # ─────────────────────────────────────────────────────────────────────────

    def run(self):
        """
        Executa o ciclo MAPE-K em loop contínuo com intervalo SCRAPE_INTERVAL_S.
        Use run_experiment.py para execução estruturada do experimento em duas fases.
        """
        logger.info(
            "Ciclo MAPE-K iniciado. Intervalo: %ds. Serviços: %s",
            SCRAPE_INTERVAL_S, SERVICES,
        )
        while True:
            self._collect_cycle()
            time.sleep(SCRAPE_INTERVAL_S)


if __name__ == "__main__":
    XAIAnalyzer().run()
