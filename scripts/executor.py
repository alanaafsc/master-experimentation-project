"""
executor.py — Módulo de Execução (MAPE-K: Execute)

Responsável por aplicar as ações de escalonamento no cluster Kubernetes
após a confirmação do ciclo Plan. Utiliza a API oficial do Kubernetes
(client-go para Python) para operar sobre objetos Deployment.

Requisito de RBAC:
  O ServiceAccount associado ao Pod/processo que executa este módulo
  precisa ter permissões de `get`, `patch` e `update` sobre `deployments`
  e `deployments/scale` no namespace alvo. Consulte infrastructure/rbac-config.yaml.

Lógica de Cooldown:
  Após cada ação de escalonamento (scale-up), o executor entra em período
  de cooldown de COOLDOWN_CYCLES ciclos para evitar oscilações (thrashing).
  O cooldown garante que o sistema observe o efeito do escalonamento antes
  de tomar nova decisão.
"""

import logging
from datetime import datetime

from kubernetes import client, config as k8s_config
from kubernetes.client.exceptions import ApiException

from config import KUBERNETES_NAMESPACE, SCALE_UP_REPLICAS, COOLDOWN_CYCLES

logger = logging.getLogger("ExecutorModule")


class ExecutorModule:
    """
    Interface com a API do Kubernetes para escalonamento de Deployments.

    Attributes:
        apps_v1         : AppsV1Api — cliente Kubernetes para recursos de apps
        _cooldown       : dict — contador de ciclos de cooldown por serviço
        scale_history   : list — log de todas as ações de escalonamento
    """

    def __init__(self):
        try:
            k8s_config.load_incluster_config()  # Quando rodando dentro do cluster
            logger.info("Kubernetes: configuração in-cluster carregada.")
        except k8s_config.ConfigException:
            k8s_config.load_kube_config()       # Quando rodando localmente (kubectl)
            logger.info("Kubernetes: configuração local (kubeconfig) carregada.")

        self.apps_v1 = client.AppsV1Api()
        self._cooldown: dict[str, int] = {}
        self.scale_history: list[dict] = []

    # ─────────────────────────────────────────────────────────────────────────
    # Controle de cooldown
    # ─────────────────────────────────────────────────────────────────────────

    def is_in_cooldown(self, service_name: str) -> bool:
        """Verifica se o serviço está em período de cooldown pós-escalonamento."""
        remaining = self._cooldown.get(service_name, 0)
        if remaining > 0:
            logger.info("[EXECUTE] %s em cooldown. Ciclos restantes: %d", service_name, remaining)
            self._cooldown[service_name] = remaining - 1
            return True
        return False

    def _start_cooldown(self, service_name: str):
        """Inicia o período de cooldown após um escalonamento."""
        self._cooldown[service_name] = COOLDOWN_CYCLES

    # ─────────────────────────────────────────────────────────────────────────
    # Ações de escalonamento
    # ─────────────────────────────────────────────────────────────────────────

    def get_current_replicas(self, service_name: str) -> int:
        """Consulta o número atual de réplicas de um Deployment."""
        try:
            dep = self.apps_v1.read_namespaced_deployment(
                name=service_name, namespace=KUBERNETES_NAMESPACE
            )
            return dep.spec.replicas or 1
        except ApiException as exc:
            logger.error("[EXECUTE] Erro ao consultar réplicas de %s: %s", service_name, exc)
            return -1

    def scale_service(self, service_name: str, replicas: int) -> dict:
        """
        Aplica patch no objeto Deployment para alterar o número de réplicas.

        O método registra a ação no scale_history com evidências suficientes
        para rastreabilidade no relatório XAI (quem, quando, de quantas para quantas).

        Args:
            service_name : nome do Deployment Kubernetes
            replicas     : número desejado de réplicas

        Returns:
            Dicionário com os detalhes da ação executada.
        """
        if self.is_in_cooldown(service_name):
            return {"status": "COOLDOWN", "service": service_name}

        current = self.get_current_replicas(service_name)
        if current == replicas:
            logger.info("[EXECUTE] %s já possui %d réplicas. Nenhuma ação.", service_name, replicas)
            return {"status": "NO_CHANGE", "service": service_name, "replicas": replicas}

        try:
            body = {"spec": {"replicas": replicas}}
            self.apps_v1.patch_namespaced_deployment_scale(
                name=service_name,
                namespace=KUBERNETES_NAMESPACE,
                body=body,
            )

            action = {
                "timestamp": datetime.utcnow().isoformat(),
                "service": service_name,
                "namespace": KUBERNETES_NAMESPACE,
                "from_replicas": current,
                "to_replicas": replicas,
                "direction": "SCALE_UP" if replicas > current else "SCALE_DOWN",
                "status": "SUCCESS",
            }
            self.scale_history.append(action)
            self._start_cooldown(service_name)

            logger.warning(
                "[EXECUTE] %s escalado: %d → %d réplicas.",
                service_name, current, replicas,
            )
            return action

        except ApiException as exc:
            logger.error(
                "[EXECUTE] Falha ao escalar %s: HTTP %d — %s",
                service_name, exc.status, exc.reason,
            )
            return {"status": "ERROR", "service": service_name, "error": str(exc)}
