from typing import Literal

from pydantic import BaseModel, Field


class ValidationRequest(BaseModel):
    """
    Representa a requisicao enviada pelo Harness.

    Responsabilidade: Receber o YAML bruto gerado pela LLM e o tipo de pipeline para validacao.
    Uso no Harness: O GuardrailNode do Harness empacota o YAML gerado e envia nesta estrutura para a Plataforma.
    """

    pipeline_yaml: str = Field(description="Conteudo bruto do arquivo YAML gerado pelo Harness.")
    pipeline_type: Literal["relational", "file", "api"] = Field(
        description="Tipo de pipeline para aplicar regras de validacao especificas (ex: validacao de SQL aplica-se mais a 'relational')."
    )


class ValidationErrorDetail(BaseModel):
    """
    Detalha um erro especifico encontrado durante a validacao na plataforma.

    Responsabilidade: Fornecer feedback acionavel apontando exatamente onde o YAML esta errado e como corrigir.
    Uso no Harness: O EnricherNode do Harness pega esta estrutura e formata uma mensagem de 'feedforward' estruturada para a LLM entender e corrigir o erro de forma autonoma.

    Exemplo:
      json_pointer: "/source/objects/0/extraction_query"
      error_code: "INVALID_SQL"
      message: "Syntax error near 'FRO' on line 2."
      suggestion: "Corrija a palavra-chave para 'FROM'."
    """

    json_pointer: str = Field(
        description="Caminho JSON Pointer (RFC 6901) indicando o campo exato com erro."
    )
    error_code: str = Field(
        description="Codigo categorizado do erro (ex: MISSING_FIELD, INVALID_SQL)."
    )
    message: str = Field(description="Mensagem tecnica descrevendo o problema.")
    suggestion: str = Field(
        description="Sugestao corretiva direta e acionavel para a LLM aplicar no proximo prompt."
    )


class ValidationResponse(BaseModel):
    """
    Resposta agregada de validacao retornada ao Harness.

    Responsabilidade: Informar o status global de validacao e agrupar todos os erros encontrados.
    Uso no Harness: O GuardrailNode avalia `is_valid`. Se True, o pipeline avanca. Se False, envia `errors` para o EnricherNode gerar feedback para nova iteracao.
    """

    is_valid: bool = Field(description="True se o YAML atende a todos os requisitos da plataforma.")
    errors: list[ValidationErrorDetail] = Field(
        default_factory=list, description="Lista de erros de validacao; vazia se is_valid for True."
    )
