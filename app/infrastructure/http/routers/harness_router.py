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
