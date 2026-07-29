import yaml
from fastapi import APIRouter

from app.infrastructure.http.schemas.harness_schemas import (
    ValidationErrorDetail,
    ValidationRequest,
    ValidationResponse,
)

router = APIRouter(prefix="/v1/harness", tags=["harness"])


@router.post("/validate", response_model=ValidationResponse)
def validate_pipeline(request: ValidationRequest) -> ValidationResponse:
    """
    Valida um pipeline YAML submetido pelo Harness de acordo com as regras da plataforma.

    Responsabilidade:
    - Fazer parse do YAML gerado pela LLM.
    - Validar o schema (campos obrigatorios, tipos) via validacao Pydantic ou JSON Schema da plataforma.
    - Aplicar regras de negocio especificas da plataforma (ex: validacao de queries SQL usando AST parsers locais).
    - Mapear excecoes para objetos ValidationErrorDetail acionaveis.

    Uso pelo Harness:
    - O Harness invoca este endpoint como a "Single Source of Truth" para garantir que o que a LLM gerou funcionara no Airflow/Plataforma.
    - Se falhar, a LLM recebera o json_pointer e a sugestao para corrigir o erro iterativamente, sem a necessidade de intervençao humana imediata.
    """
    # Dummy implementation representing CI validation rules
    # In production, this would call the platform's core validation suite
    try:
        data = yaml.safe_load(request.pipeline_yaml)
        if not data or "pipeline_id" not in data:
            return ValidationResponse(
                is_valid=False,
                errors=[
                    ValidationErrorDetail(
                        json_pointer="/",
                        error_code="MISSING_ID",
                        message="pipeline_id is required",
                        suggestion="Adicione o campo 'pipeline_id' na raiz do documento YAML.",
                    )
                ],
            )
        return ValidationResponse(is_valid=True)
    except Exception as e:
        return ValidationResponse(
            is_valid=False,
            errors=[
                ValidationErrorDetail(
                    json_pointer="/",
                    error_code="YAML_PARSE_ERROR",
                    message=str(e),
                    suggestion="Verifique a sintaxe do arquivo YAML. Certifique-se de que a formatacao esta correta e que os campos estao indentados.",
                )
            ],
        )


@router.get("/schema")
def get_schema(type: str = "all") -> dict[str, object]:
    """
    Fornece a definicao de esquema atual (JSON Schema) usada pela plataforma.

    Responsabilidade: Retornar a estrutura de campos, tipos e restricoes esperadas para um YAML de pipeline de um determinado tipo.
    Uso pelo Harness: O ContextNode do Harness consome este schema e o inclui no prompt inicial do GeneratorNode para guiar a criacao do YAML (Zero-shot generation guide), diminuindo a chance de erro sintatico.
    """
    # Dummy schema response
    return {"type": "object", "properties": {"pipeline_id": {"type": "string"}}}


@router.get("/gold-examples")
def get_gold_examples(type: str = "all") -> dict[str, object]:
    """
    Fornece exemplos canonicos ("padrao ouro") de pipelines YAML perfeitos.

    Responsabilidade: Manter uma biblioteca de exemplos corretos de pipelines que ilustram as melhores praticas atuais da plataforma.
    Uso pelo Harness: O ContextNode do Harness busca estes exemplos para inclusao direta no prompt (Few-shot learning), servindo de template visual e estrutural para a LLM imitar o formato esperado.
    """
    # Dummy examples response
    return {
        "examples": [
            {
                "description": "Standard Ingestion for Relational DBs",
                "yaml_snippet": "pipeline_id: example",
            }
        ]
    }
