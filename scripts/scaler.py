import warnings
import numpy as np
from statsmodels.tsa.arima.model import ARIMA

# (Módulo de Previsão - Plan)
# Este módulo deve decidir se a anomalia é um surto passageiro ou uma tendência que exige escalonamento.

class ScalerModule:
    def __init__(self, threshold=500):
        self.threshold = threshold # Limite de latência aceitável (ex: 500ms)

    def predict_trend(self, history_data):
        """
        Retorna True se o ARIMA prever que a latência continuará subindo
        ou se manterá acima do threshold.
        """
        if len(history_data) < 15:
            return False # Aguarda ter dados suficientes para o modelo

        try:
            # Ignora avisos de convergência do ARIMA para não sujar o console
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                # Ordem (2,1,0) é mais estável para intervalos curtos de 5s
                model = ARIMA(history_data, order=(2, 1, 0))
                model_fit = model.fit()

            # Previsão para os próximos 3 ciclos (15 segundos)
            forecast = model_fit.forecast(steps=3)
            future_mean = np.mean(forecast)

            # Lógica de Decisão XAI: 
            # Previsão de subida OU previsão acima do limite
            if future_mean > history_data[-1] or future_mean > self.threshold:
                return True
            return False
        except Exception as e:
            print(f"⚠️ Erro no cálculo ARIMA: {e}")
            return False