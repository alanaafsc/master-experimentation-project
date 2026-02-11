import time
import numpy as np
from monitor import MonitoringModule

class XAIAnalyzer:
    def __init__(self):
        self.monitor = MonitoringModule()
        self.history = {}  # Dicionário: {serviço: [lista_latencias]}
        # Lista de serviços para monitorar simultaneamente conforme o Hipster-shop
        self.services_to_watch = ["frontend", "productcatalogservice"]

    def calculate_pearson(self, service_a, service_b):
        """Calcula o grau de influência S_k,m entre dois serviços"""
        hist_a = self.history.get(service_a, [])
        hist_b = self.history.get(service_b, [])

        if len(hist_a) < 10 or len(hist_b) < 10:
            return 0.0

        min_len = min(len(hist_a), len(hist_b))
        # Seleciona os últimos N pontos para correlação em tempo real
        correlation = np.corrcoef(hist_a[-min_len:], hist_b[-min_len:])[0, 1]
        return correlation if not np.isnan(correlation) else 0.0

    def detect_anomaly(self, service, current_val):
        """Implementa a Regra 3-sigma da Seção III.A"""
        data = self.history[service]
        if len(data) < 10:
            print(f"[{service}] Baseline: {len(data)}/10")
            return False

        mu = np.mean(data)
        sigma = np.std(data)
        limit = mu + 3 * sigma

        if current_val > limit:
            print(f"\n⚠️  ANOMALIA EM {service.upper()}!")
            print(f"Valor: {current_val:.2f}ms | Limite: {limit:.2f}ms")
            return True
        return False

    def run(self):
        print("Iniciando Ciclo MAPE-K (Monitor & Analyze)...")
        while True:
            for service in self.services_to_watch:
                try:
                    # 1. MONITOR: Coleta via Prometheus
                    p95, _ = self.monitor.get_metrics(service)
                    
                    if service not in self.history:
                        self.history[service] = []

                    # 2. ANALYZE: Detecção de Anomalia
                    is_anomalous = self.detect_anomaly(service, p95)
                    
                    if is_anomalous and service == "frontend":
                        # Se o frontend está lento, investigamos a causa raiz (Random Walk)
                        score = self.calculate_pearson("frontend", "productcatalogservice")
                        print(f"🔍 XAI DIAGNÓSTICO: Correlação com Catálogo: {score:.4f}")
                        if score > 0.8:
                            print("💡 EXPLICAÇÃO: O Catálogo está arrastando a performance do Frontend.")

                    # Atualiza histórico (Janela deslizante de 20 pontos)
                    self.history[service].append(p95)
                    if len(self.history[service]) > 20:
                        self.history[service].pop(0)

                except Exception as e:
                    print(f"Erro no monitoramento de {service}: {e}")

            time.sleep(30) # Intervalo de 30s conforme Seção III.A

if __name__ == "__main__":
    analyzer = XAIAnalyzer()
    analyzer.run()