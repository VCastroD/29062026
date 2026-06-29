"""Operador customizado para validar o schema dos produtos (requisito opcional)."""
from __future__ import annotations

import json
from typing import Any, Sequence

from airflow.exceptions import AirflowFailException
from airflow.models.baseoperator import BaseOperator


class ValidarProdutosOperator(BaseOperator):
    """Valida o schema dos produtos antes do processamento.

    Recebe a lista de produtos (via XCom/TaskFlow), garante que cada item
    possui os campos obrigatórios e que o ``price`` é numérico e não-negativo.
    Retorna a lista de produtos válidos (empurrada automaticamente como XCom).

    Como a validação é determinística, uma falha levanta ``AirflowFailException``
    para falhar **sem** acionar novas tentativas (não adianta repetir).
    """

    template_fields: Sequence[str] = ("produtos",)
    ui_color = "#f4d35e"

    def __init__(
        self,
        produtos: Any,
        campos_obrigatorios: list[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.produtos = produtos
        self.campos_obrigatorios = campos_obrigatorios or ["id", "title", "price", "category"]

    def execute(self, context) -> list[dict]:
        produtos = self.produtos
        if isinstance(produtos, str):  # segurança, caso venha templado como string
            produtos = json.loads(produtos)

        if not isinstance(produtos, list) or not produtos:
            raise AirflowFailException("Lista de produtos vazia ou inválida.")

        validos: list[dict] = []
        erros: list[str] = []

        for indice, produto in enumerate(produtos):
            if not isinstance(produto, dict):
                erros.append(f"#{indice}: não é um objeto.")
                continue

            faltando = [c for c in self.campos_obrigatorios if produto.get(c) in (None, "")]
            if faltando:
                erros.append(f"#{indice}: campos ausentes {faltando}.")
                continue

            preco = produto.get("price")
            if isinstance(preco, bool) or not isinstance(preco, (int, float)) or preco < 0:
                erros.append(f"#{indice}: preço inválido ({preco!r}).")
                continue

            validos.append(produto)

        self.log.info("Validação concluída: %d válidos, %d inválidos.", len(validos), len(erros))
        for e in erros:
            self.log.warning("Produto descartado -> %s", e)

        if not validos:
            raise AirflowFailException("Nenhum produto válido após a validação do schema.")

        return validos
