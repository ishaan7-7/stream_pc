from typing import Dict, Any, Tuple, Optional
from pydantic import BaseModel, ValidationError, create_model
from pydantic.config import ConfigDict

from replay.service.schema_loader import MasterSchema


class RowValidationError(Exception):

    def __init__(
        self,
        *,
        module: str,
        message: str,
        details: Dict[str, Any],
    ):
        self.module = module
        self.message = message
        self.details = details
        super().__init__(message)


class SchemaValidator:
    

    def __init__(self, schema: MasterSchema):
        self.schema = schema
        self._models: Dict[str, BaseModel] = {}

        self._build_models()

    
    def _build_models(self) -> None:
        for module_name, module_spec in self.schema.modules.items():
            fields = {}

            for col_name, col_spec in module_spec["columns"].items():
                dtype = col_spec["dtype"]
                nullable = col_spec["nullable"]

                py_type = self._map_dtype(dtype)
                default = None if nullable else ...

                fields[col_name] = (py_type, default)

            model = create_model(
                f"{module_name.capitalize()}Row",
                __config__=ConfigDict(extra="forbid"),
                **fields,
            )

            self._models[module_name] = model

    
    def validate_row(
        self,
        *,
        module: str,
        row: Dict[str, Any],
    ) -> Dict[str, Any]:
        

        if module not in self._models:
            raise RowValidationError(
                module=module,
                message="Unknown module",
                details={"module": module},
            )

        module_spec = self.schema.modules[module]
        expected_columns = list(module_spec["columns"].keys())

        
        if self.schema.validation_rules["enforce_column_order"]:
            actual_columns = list(row.keys())
            if actual_columns != expected_columns:
                raise RowValidationError(
                    module=module,
                    message="Column order mismatch",
                    details={
                        "expected": expected_columns,
                        "actual": actual_columns,
                    },
                )

        
        Model = self._models[module]

        try:
            validated = Model(**row)
        except ValidationError as e:
            raise RowValidationError(
                module=module,
                message="Schema validation failed",
                details=e.errors(),
            )

        return validated.model_dump()

    
    @staticmethod
    def _map_dtype(dtype: str):
        if dtype == "float64":
            return float
        if dtype == "int64":
            return int
        if dtype == "object":
            return str

        raise ValueError(f"Unsupported dtype: {dtype}")
