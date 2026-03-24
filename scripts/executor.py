from kubernetes import client, config

# (Módulo de Ação - Execute)
# recebe o sinal do scaler e executa comando para escalar 

class ExecutorModule:
    def __init__(self):
        config.load_kube_config() # Carrega o config do kubectl
        self.apps_v1 = client.AppsV1Api()

    def scale_service(self, service_name, replicas):
        namespace = "default" # o namespace
        # Nome do deployment costuma ser o mesmo do workload no Istio
        deployment_name = service_name 
        
        body = {"spec": {"replicas": replicas}}
        self.apps_v1.patch_namespaced_deployment_scale(
            name=deployment_name, namespace=namespace, body=body
        )
        print(f"🚀 EXECUTOR: {service_name} escalado para {replicas} réplicas.")