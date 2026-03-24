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
        
        print(f"✅ {service.upper()} ANALISADO: {current_val:.2f}ms (Média: {np.mean(data):.2f})")

        mu = np.mean(data)
        sigma = np.std(data)
        limit = mu + 3 * sigma

        if current_val > limit:
            print(f"\n⚠️  ANOMALIA EM {service.upper()}!")
            print(f"Valor: {current_val:.2f}ms | Limite: {limit:.2f}ms")
            return True
        return False

def run(self):
        print("\n🚀 Iniciando Ciclo MAPE-K Completo (Monitor-Analyze-Plan-Execute)...")
        while True:
            for service in self.services_to_watch:
                try:
                    # 1. MONITOR
                    p95, _ = self.monitor.get_metrics(service)
                    
                    if service not in self.history:
                        self.history[service] = []

                    # 2. ANALYZE (Detecção)
                    is_anomalous = self.detect_anomaly(service, p95)
                    
                    if is_anomalous:
                        # 2.1 ANALYZE (XAI - Causa Raiz)
                        score = self.calculate_pearson("frontend", "productcatalogservice")
                        print(f"🔍 [XAI] Score de Correlação (Frontend <-> Catálogo): {score:.4f}")
                        
                        # 3. PLAN (ARIMA - Previsão)
                        print(f"📈 [PLAN] Rodando ARIMA para {service}...")
                        should_scale = self.scaler.predict_trend(self.history[service])
                        
                        trend_msg = "TENDÊNCIA DE ALTA" if should_scale else "OSCILAÇÃO PASSAGEIRA"
                        print(f"📊 [PLAN] Resultado ARIMA: {trend_msg}")

                        # 4. EXECUTE (Ação no Kubernetes)
                        if should_scale and score > 0.8:
                            print(f"⚖️ [EXECUTE] Gatilhos confirmados! Escalonando {service}...")
                            self.executor.scale_service("productcatalogservice", replicas=3)
                        else:
                            reason = "Correlação Baixa" if score <= 0.8 else "Tendência de Queda"
                            print(f"ℹ️ [EXECUTE] Escalonamento abortado. Motivo: {reason}")
                    else:
                        print(f"✅ {service.upper()}: Estável ({p95:.2f}ms)")

                    # Atualiza histórico
                    self.history[service].append(p95)
                    if len(self.history[service]) > 20:
                        self.history[service].pop(0)

                except Exception as e:
                    print(f"❌ Erro no ciclo de {service}: {e}")

            time.sleep(5) # voltar para Intervalo de 30s conforme Seção III.A

if __name__ == "__main__":
    analyzer = XAIAnalyzer()
    analyzer.run()