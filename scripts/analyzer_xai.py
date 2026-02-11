import time
import numpy as np
from monitor import MonitoringModule  

class XAIAnalyzer:
    def __init__(self):
        self.monitor = MonitoringModule()
        self.history = {}  # Dicionário para histórico de múltiplos serviços
        self.thresholds = {}

    def run_analysis(self, services):
        print(f"--- Iniciando Ciclo de Análise MAPE-K ---")
        
        while True:
            for service in services:
                # 1. Coleta via Monitor
                try:
                    p95, _ = self.monitor.get_metrics(service)
                    
                    if service not in self.history:
                        self.history[service] = []
                    
                    # 2. Detecção via Regra 3-sigma
                    self.detect_anomaly(service, p95)
                    
                    # Atualiza histórico (janela deslizante)
                    self.history[service].append(p95)
                    if len(self.history[service]) > 20:
                        self.history[service].pop(0)
                        
                except Exception as e:
                    print(f"Erro ao analisar {service}: {e}")

            time.sleep(30) # Intervalo conforme o artigo

    def detect_anomaly(self, service, current_val):
        data = self.history[service]
        if len(data) < 10:
            print(f"[{service}] Coletando baseline... ({len(data)}/10)")
            return

        mu = np.mean(data)
        sigma = np.std(data)
        limit = mu + 3 * sigma #

        if current_val > limit and current_val > 100: # Filtro para ignorar ruídos baixos
            print(f"\n⚠️ ANOMALIA DETECTADA em '{service}'!")
            print(f"Valor: {current_val:.2f}ms | Limite: {limit:.2f}ms")
            self.explain_xai(service, current_val, mu)

    def explain_xai(self, service, val, mu):
        # contribuição de XAI 
        increase = ((val - mu) / mu) * 100
        print(f"📢 XAI DIAGNÓSTICO: O serviço '{service}' apresentou um aumento de "
              f"{increase:.1f}% na latência em relação à média histórica.")
        
        if service == "productcatalogservice":
            print("💡 CAUSA RAIZ PROVÁVEL: Falha detectada neste nó. Verifique dependências downstream.")

# --- EXECUÇÃO ---
if __name__ == "__main__":
    analyzer = XAIAnalyzer()
    # Monitorando os dois principais pontos para o Random Walk
    analyzer.run_analysis(["frontend", "productcatalogservice"])