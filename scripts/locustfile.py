"""
locustfile.py — Gerador de carga sintética para o experimento XAI-FinOps.

Simula perfil de tráfego realista sobre o Google Online Boutique (Hipster-shop),
cobrindo os endpoints que exercitam os serviços monitorados:
  - frontend            (todas as rotas)
  - productcatalogservice (browse + checkout)
  - cartservice          (add/view/empty cart)
  - checkoutservice      (checkout)
  - recommendationservice (página inicial)

Uso standalone (headless — recomendado para o experimento):
    locust -f scripts/locustfile.py \\
           --host=http://localhost:80 \\
           --users=50 --spawn-rate=5 \\
           --headless --run-time=40m

Uso com interface web (diagnóstico):
    locust -f scripts/locustfile.py --host=http://localhost:80

O host deve apontar para o frontend do Online Boutique.
Com kubectl port-forward: kubectl port-forward svc/frontend 80:80
"""

import datetime
import random

from faker import Faker
from locust import FastHttpUser, TaskSet, between, task

fake = Faker()

# IDs dos produtos disponíveis no Online Boutique
PRODUCTS = [
    "0PUK6V6EV0",
    "1YMWWN1N4O",
    "2ZYFJ3GM2N",
    "66VCHSJNUP",
    "6E92ZMYYFZ",
    "9SIQT8TOJO",
    "L9ECAV7KIM",
    "LS4PSXUNUM",
    "OLJCESPC7Z",
]

CURRENCIES = ["EUR", "USD", "JPY", "CAD", "GBP", "TRY"]


class ShoppingBehavior(TaskSet):
    """
    Perfil de comportamento de usuário navegando na loja.

    Pesos calibrados para gerar tráfego realista em todos os serviços
    monitorados. browseProduct tem peso alto porque exercita o
    productcatalogservice em todas as chamadas — serviço central do experimento.
    """

    def on_start(self):
        """Acessa a página inicial ao iniciar a sessão."""
        self.client.get("/", name="GET /")

    @task(2)
    def index(self):
        """Página inicial — aciona frontend + recommendationservice."""
        self.client.get("/", name="GET /")

    @task(2)
    def set_currency(self):
        """Troca de moeda — exercita frontend."""
        self.client.post(
            "/setCurrency",
            {"currency_code": random.choice(CURRENCIES)},
            name="POST /setCurrency",
        )

    @task(10)
    def browse_product(self):
        """Visualização de produto — exercita productcatalogservice diretamente."""
        product = random.choice(PRODUCTS)
        self.client.get(f"/product/{product}", name="GET /product/[id]")

    @task(3)
    def view_cart(self):
        """Visualização do carrinho — exercita cartservice."""
        self.client.get("/cart", name="GET /cart")

    @task(5)
    def add_to_cart(self):
        """Adiciona produto ao carrinho — exercita productcatalogservice + cartservice."""
        product = random.choice(PRODUCTS)
        self.client.get(f"/product/{product}", name="GET /product/[id]")
        self.client.post(
            "/cart",
            {"product_id": product, "quantity": random.randint(1, 5)},
            name="POST /cart",
        )

    @task(1)
    def checkout(self):
        """
        Fluxo completo de compra — exercita productcatalogservice + cartservice
        + checkoutservice em sequência. Maior impacto na latência end-to-end.
        """
        product = random.choice(PRODUCTS)
        self.client.post(
            "/cart",
            {"product_id": product, "quantity": random.randint(1, 3)},
            name="POST /cart",
        )
        current_year = datetime.datetime.now().year + 1
        self.client.post(
            "/cart/checkout",
            {
                "email": fake.email(),
                "street_address": fake.street_address(),
                "zip_code": fake.zipcode(),
                "city": fake.city(),
                "state": fake.state_abbr(),
                "country": "US",
                "credit_card_number": fake.credit_card_number(card_type="visa"),
                "credit_card_expiration_month": random.randint(1, 12),
                "credit_card_expiration_year": random.randint(current_year, current_year + 5),
                "credit_card_cvv": f"{random.randint(100, 999)}",
            },
            name="POST /cart/checkout",
        )

    @task(1)
    def empty_cart(self):
        """Esvazia o carrinho."""
        self.client.post("/cart/empty", name="POST /cart/empty")


class ExperimentUser(FastHttpUser):
    """
    Usuário sintético do experimento XAI-FinOps.

    Configurado para gerar carga suficiente para:
      (a) preencher o baseline do framework (BASELINE_WINDOW = 20 pontos)
      (b) tornar anomalias detectáveis pelo limiar 3σ durante injeção de falhas

    wait_time entre 1-5s → ~10-20 req/s por usuário com 50 usuários = ~500-1000 req/s total.
    """

    tasks = [ShoppingBehavior]
    wait_time = between(1, 5)
