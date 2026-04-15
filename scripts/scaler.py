"""
scaler.py — Módulo de Planejamento via ARIMA (MAPE-K: Plan)

Implementa o modelo AutoRegressivo Integrado de Médias Móveis ARIMA(p,d,q)
para previsão de tendências nas séries temporais de latência.

Responsabilidade no ciclo MAPE-K:
  Dada uma anomalia confirmada pelo módulo de Análise, a fase Plan deve
  decidir se a anomalia representa uma TENDÊNCIA PERSISTENTE (justificando
  escalonamento) ou uma OSCILAÇÃO PASSAGEIRA (escalonamento desnecessário).

Critério de decisão (Equação do Plan):
  scale = True  se  E[L_{t+1:t+h}] > L_t   (previsão em alta)
               OU  E[L_{t+1:t+h}] > SLA     (previsão acima do SLA)
  scale = False caso contrário

Onde:
  E[L_{t+1:t+h}] = média das h previsões do ARIMA
  L_t             = última latência observada
  h               = FORECAST_STEPS (horizonte de previsão)
"""

import logging
import warnings

import numpy as np
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.stattools import adfuller

from config import ARIMA_MIN_POINTS, ARIMA_ORDER, FORECAST_STEPS, LATENCY_SLA_MS

logger = logging.getLogger("ScalerModule")


class ScalerModule:
    """
    Módulo de Planejamento: usa ARIMA(2,1,0) para confirmar tendências.

    O modelo ARIMA(2,1,0) foi selecionado com base em:
      - Análise exploratória das séries de latência do Hipster-shop
      - Ordem d=1 remove tendência linear (série diferenciada é estacionária)
      - Ordem p=2 captura autocorrelação nos dois ciclos anteriores (lag 1 e 2)
      - Ordem q=0 descartada via análise da PACF (sem componente MA necessário)
      - Validação AIC/BIC comparativo com ordens alternativas (1,1,0) e (2,1,1)
    """

    def __init__(self):
        self.order = ARIMA_ORDER
        self.min_points = ARIMA_MIN_POINTS
        self.forecast_steps = FORECAST_STEPS
        self.sla_ms = LATENCY_SLA_MS

    # ─────────────────────────────────────────────────────────────────────────
    # Diagnóstico de estacionariedade
    # ─────────────────────────────────────────────────────────────────────────

    def _adf_test(self, data: list[float]) -> dict:
        """
        Realiza o Teste de Dickey-Fuller Aumentado (ADF) para verificar
        estacionariedade da série temporal.

        H0: a série possui raiz unitária (não-estacionária)
        H1: a série é estacionária

        Returns:
            Dicionário com estatística ADF, p-valor e conclusão.
        """
        try:
            adf_stat, p_value, _, _, critical_values, _ = adfuller(data, autolag="AIC")
            is_stationary = p_value < 0.05
            return {
                "adf_statistic": round(float(adf_stat), 4),
                "p_value": round(float(p_value), 4),
                "critical_values": {k: round(v, 4) for k, v in critical_values.items()},
                "is_stationary": is_stationary,
                "conclusion": (
                    "Série estacionária (rejeita H0, p < 0.05)."
                    if is_stationary
                    else "Série não-estacionária (falha em rejeitar H0). Diferenciação d=1 aplicada pelo modelo."
                ),
            }
        except Exception as exc:
            return {"error": str(exc), "is_stationary": False}

    # ─────────────────────────────────────────────────────────────────────────
    # Previsão e decisão
    # ─────────────────────────────────────────────────────────────────────────

    def predict_trend(self, history_data: list[float]) -> tuple[bool, dict]:
        """
        Ajusta o modelo ARIMA(2,1,0) na série histórica e projeta h passos
        à frente para decidir se o escalonamento é necessário.

        Args:
            history_data : série temporal de latências P95 (ms)

        Returns:
            (should_scale, evidence):
              should_scale — True se o escalonamento é recomendado
              evidence     — dicionário com evidências XAI do Plan
        """
        if len(history_data) < self.min_points:
            msg = f"Dados insuficientes: {len(history_data)}/{self.min_points} pontos."
            logger.warning("[PLAN] %s", msg)
            return False, {"error": msg, "should_scale": False}

        adf_result = self._adf_test(history_data)

        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                model = ARIMA(history_data, order=self.order)
                fitted = model.fit()

            forecast_values = fitted.forecast(steps=self.forecast_steps)
            forecast_list = [round(float(v), 2) for v in forecast_values]
            future_mean = float(np.mean(forecast_values))
            last_val = float(history_data[-1])

            # ── Critério de decisão ──────────────────────────────────────────
            scale_reason = None
            if future_mean > last_val:
                scale_reason = (
                    f"Tendência de alta: E[L_{{t+1:t+{self.forecast_steps}}}]="
                    f"{future_mean:.2f}ms > L_t={last_val:.2f}ms"
                )
            if future_mean > self.sla_ms:
                scale_reason = (
                    f"Previsão acima do SLA: E[L]={future_mean:.2f}ms > SLA={self.sla_ms}ms"
                )

            should_scale = scale_reason is not None

            evidence = {
                "model": f"ARIMA{self.order}",
                "aic": round(float(fitted.aic), 2),
                "bic": round(float(fitted.bic), 2),
                "adf_test": adf_result,
                "forecast_ms": forecast_list,
                "forecast_mean_ms": round(future_mean, 2),
                "last_observed_ms": round(last_val, 2),
                "sla_threshold_ms": self.sla_ms,
                "should_scale": should_scale,
                "scale_reason": scale_reason or "Tendência de queda ou estabilização — escalonamento desnecessário.",
            }

            logger.info(
                "[PLAN] ARIMA%s | AIC=%.2f | Previsão=%s ms | Escalonar=%s",
                self.order, fitted.aic, forecast_list, should_scale,
            )
            return should_scale, evidence

        except Exception as exc:
            logger.error("[PLAN] Erro no ajuste ARIMA: %s", exc, exc_info=True)
            return False, {"error": str(exc), "should_scale": False}
