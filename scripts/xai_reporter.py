"""
xai_reporter.py — Módulo de Explicabilidade (XAI Reporter)

Converte as evidências numéricas produzidas pelos módulos de Análise e
Planejamento em representações visuais e relatórios estruturados,
interpretáveis por engenheiros de confiabilidade (SREs).

Saídas geradas por evento de escalonamento:
  1. Heatmap da Matriz de Correlação de Pearson entre serviços
  2. Gráfico de série temporal com anomalia destacada e previsão ARIMA
  3. Relatório JSON estruturado com evidências do ciclo MAPE-K completo

Saída consolidada ao fim do experimento:
  4. experiment_summary.json — métricas agregadas do experimento

A geração de evidências visuais é o mecanismo pelo qual o framework cumpre
o requisito de explicabilidade (XAI): cada ação de escalonamento é acompanhada
de justificativas interpretáveis que sustentam a decisão perante o operador.
"""

import json
import logging
import os
from datetime import datetime

import numpy as np


class _NumpyEncoder(json.JSONEncoder):
    """Converte tipos numpy para tipos Python nativos antes da serialização JSON."""
    def default(self, obj):
        if isinstance(obj, np.bool_):
            return bool(obj)
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)
import matplotlib
matplotlib.use("Agg")   # backend sem display para execução headless
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec

from config import SERVICES, RESULTS_DIR

logger = logging.getLogger("XAIReporter")


class XAIReporter:
    """Gerador de relatórios e visualizações XAI."""

    def __init__(self):
        self.plots_dir = os.path.join(RESULTS_DIR, "plots")
        self.reports_dir = os.path.join(RESULTS_DIR, "reports")
        os.makedirs(self.plots_dir, exist_ok=True)
        os.makedirs(self.reports_dir, exist_ok=True)

    # ─────────────────────────────────────────────────────────────────────────
    # Ponto de entrada principal
    # ─────────────────────────────────────────────────────────────────────────

    def generate_report(self, event: dict, history: dict[str, list[float]]):
        """
        Gera o pacote completo de evidências XAI para um evento MAPE-K.

        Args:
            event   : dicionário com evidências do ciclo (saída do XAIAnalyzer)
            history : histórico de latências por serviço (série completa)
        """
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        trigger = event.get("trigger_service", "unknown")
        root = event.get("root_cause_service", trigger)

        try:
            self._plot_correlation_heatmap(history, ts, trigger)
        except Exception as exc:
            logger.warning("[XAIReporter] Heatmap falhou: %s", exc)

        try:
            self._plot_timeseries(history, event, ts, root)
        except Exception as exc:
            logger.warning("[XAIReporter] Série temporal falhou: %s", exc)

        report_path = os.path.join(self.reports_dir, f"event_{ts}.json")
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(event, f, indent=2, ensure_ascii=False, cls=_NumpyEncoder)

        logger.info("[XAIReporter] Relatório salvo: %s", report_path)

    # ─────────────────────────────────────────────────────────────────────────
    # 1. Heatmap da Matriz de Correlação de Pearson
    # ─────────────────────────────────────────────────────────────────────────

    def _plot_correlation_heatmap(
        self, history: dict[str, list[float]], ts: str, trigger: str
    ):
        """
        Heatmap da matriz de correlação de Pearson entre todos os serviços
        monitorados. Permite ao SRE visualizar quais pares de serviços têm
        latência altamente correlacionada, evidenciando propagação de anomalia.

        Serviços com dados insuficientes (< 10 pontos) são omitidos.
        """
        valid = [s for s in SERVICES if len(history.get(s, [])) >= 10]
        if len(valid) < 2:
            logger.debug("[XAIReporter] Dados insuficientes para heatmap.")
            return

        n = len(valid)
        min_len = min(len(history[s]) for s in valid)
        matrix = np.array([history[s][-min_len:] for s in valid], dtype=float)
        corr_matrix = np.corrcoef(matrix)

        fig, ax = plt.subplots(figsize=(max(6, n + 1), max(5, n)))
        im = ax.imshow(corr_matrix, vmin=-1, vmax=1, cmap="RdYlGn", aspect="auto")
        cbar = plt.colorbar(im, ax=ax)
        cbar.set_label("Coeficiente de Pearson (S_{k,m})", fontsize=9)

        short_names = [s.replace("service", "svc").replace("catalog", "cat") for s in valid]
        ax.set_xticks(range(n))
        ax.set_yticks(range(n))
        ax.set_xticklabels(short_names, rotation=40, ha="right", fontsize=8)
        ax.set_yticklabels(short_names, fontsize=8)

        for i in range(n):
            for j in range(n):
                color = "white" if abs(corr_matrix[i, j]) > 0.7 else "black"
                ax.text(j, i, f"{corr_matrix[i, j]:.2f}", ha="center", va="center",
                        fontsize=8, color=color, fontweight="bold")

        # Destaca o serviço que disparou a anomalia
        if trigger in valid:
            idx = valid.index(trigger)
            for spine_type, spine in ax.spines.items():
                spine.set_visible(False)
            rect = plt.Rectangle(
                (idx - 0.5, -0.5), 1, n, linewidth=2, edgecolor="crimson",
                facecolor="none", zorder=5
            )
            ax.add_patch(rect)

        ax.set_title(
            f"Matriz de Correlação de Pearson — {ts}\n"
            f"Serviço disparador: {trigger}  |  N={min_len} observações",
            fontsize=10, pad=12,
        )
        fig.tight_layout()
        path = os.path.join(self.plots_dir, f"heatmap_{ts}.png")
        fig.savefig(path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        logger.info("[XAIReporter] Heatmap salvo: %s", path)

    # ─────────────────────────────────────────────────────────────────────────
    # 2. Série temporal com anomalia e previsão ARIMA
    # ─────────────────────────────────────────────────────────────────────────

    def _plot_timeseries(
        self,
        history: dict[str, list[float]],
        event: dict,
        ts: str,
        root_cause: str,
    ):
        """
        Gráfico de série temporal do serviço causa-raiz exibindo:
          - Série histórica de latência P95
          - Banda de controle 3σ (μ ± 3σ)
          - Ponto de anomalia detectado (marcador vermelho)
          - Previsão ARIMA com horizonte de h passos (linha tracejada)

        Este gráfico é a principal evidência visual apresentada ao SRE para
        justificar (ou rejeitar) a decisão de escalonamento.
        """
        data = history.get(root_cause, [])
        if len(data) < 5:
            return

        arima_ev = event.get("arima_evidence", {})
        forecast = arima_ev.get("forecast_ms", [])
        anomaly_ev = event.get("anomaly_evidence", {})
        threshold_3s = anomaly_ev.get("threshold_3sigma_ms")
        mu = anomaly_ev.get("baseline_mean_ms")
        sigma = anomaly_ev.get("baseline_std_ms")
        observed = anomaly_ev.get("observed_p95_ms")
        sla = arima_ev.get("sla_threshold_ms", 500)

        fig, ax = plt.subplots(figsize=(13, 5))

        x_hist = list(range(len(data)))
        ax.plot(x_hist, data, color="#2c7bb6", linewidth=1.5,
                label="P95 Latência (ms)", zorder=3)

        # Banda de controle 3σ
        if mu is not None and sigma is not None:
            mu_line = [mu] * len(data)
            upper = [mu + 3 * sigma] * len(data)
            lower = [max(0, mu - 3 * sigma)] * len(data)
            ax.fill_between(x_hist, lower, upper, alpha=0.12, color="steelblue",
                            label=f"Banda 3σ (μ={mu:.0f}ms, σ={sigma:.0f}ms)")
            ax.plot(x_hist, mu_line, color="steelblue", linestyle=":", linewidth=1,
                    alpha=0.6)

        # Limiar 3σ
        if threshold_3s is not None:
            ax.axhline(threshold_3s, color="crimson", linestyle="--", linewidth=1.3,
                       label=f"Limiar 3σ = {threshold_3s:.1f} ms", zorder=4)

        # SLA
        ax.axhline(sla, color="gray", linestyle=":", linewidth=1.0,
                   label=f"SLA = {sla:.0f} ms", alpha=0.7)

        # Ponto de anomalia
        if observed is not None:
            ax.scatter([len(data) - 1], [observed], color="crimson", zorder=6,
                       s=100, marker="X", label=f"Anomalia detectada ({observed:.1f} ms)")

        # Previsão ARIMA
        if forecast:
            x_fore = list(range(len(data), len(data) + len(forecast)))
            ax.plot(x_fore, forecast, color="#d7191c", linestyle="--",
                    marker="o", markersize=5, linewidth=1.5,
                    label=f"Previsão ARIMA(2,1,0) — {len(forecast)} passos")
            ax.axvspan(len(data) - 0.5, len(data) + len(forecast) - 0.5,
                       alpha=0.06, color="orange", label="Horizonte de previsão")

        ax.set_xlabel("Ciclo de monitoramento (t)", fontsize=10)
        ax.set_ylabel("Latência P95 (ms)", fontsize=10)
        ax.set_title(
            f"Evidência XAI — Série Temporal: {root_cause}\n"
            f"Evento: {ts}  |  Decisão: {event.get('decision', 'N/A')}",
            fontsize=10, pad=10,
        )
        ax.legend(fontsize=8, loc="upper left")
        ax.grid(True, alpha=0.25, linestyle="--")

        fig.tight_layout()
        path = os.path.join(self.plots_dir, f"timeseries_{root_cause}_{ts}.png")
        fig.savefig(path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        logger.info("[XAIReporter] Série temporal salva: %s", path)

    # ─────────────────────────────────────────────────────────────────────────
    # 3. Relatório consolidado do experimento
    # ─────────────────────────────────────────────────────────────────────────

    def generate_summary(self, event_log: list[dict]) -> dict:
        """
        Gera experiment_summary.json com métricas agregadas de todo o experimento.

        Métricas calculadas:
          - total_events       : total de anomalias detectadas
          - scale_up_events    : quantas resultaram em escalonamento efetivo
          - suppressed_events  : quantas foram suprimidas (Pearson ou ARIMA negativos)
          - suppression_rate   : taxa de supressão (proxy de falsos positivos evitados)
          - avg_pearson_score  : Pearson médio nos eventos detectados
        """
        scale_events = [e for e in event_log if "SCALE_UP" in e.get("decision", "")]
        suppressed = [e for e in event_log if e.get("decision") == "NO_ACTION"]

        pearson_scores = []
        for e in event_log:
            corr_ev = e.get("correlation_evidence", {})
            score = corr_ev.get("best_score")
            if score is not None:
                pearson_scores.append(score)

        total = len(event_log)
        summary = {
            "total_anomaly_events": total,
            "scale_up_events": len(scale_events),
            "suppressed_events": len(suppressed),
            "suppression_rate": round(len(suppressed) / total, 4) if total > 0 else 0,
            "avg_pearson_score": round(float(np.mean(pearson_scores)), 4) if pearson_scores else None,
            "events": event_log,
        }

        path = os.path.join(self.reports_dir, "experiment_summary.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)

        logger.info("[XAIReporter] Resumo do experimento: %s", path)
        return summary
