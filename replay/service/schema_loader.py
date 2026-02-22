from pathlib import Path
import json
from typing import Dict, Any


class MasterSchemaError(RuntimeError):
    """Raised when master.json is invalid or incompatible."""
    pass


class MasterSchema:
    """
    Read-only loader and accessor for contracts/master.json
    """

    REQUIRED_TOP_LEVEL_KEYS = {
        "contract_version",
        "created_at",
        "reference_sim",
        "sim_count_validated",
        "modules",
        "metadata_schema",
        "validation_rules",
    }

    SUPPORTED_CONTRACT_VERSIONS = {"v1"}

    def __init__(self, contract_path: Path):
        self.contract_path = contract_path
        self._raw: Dict[str, Any] = {}

        self._modules: Dict[str, Any] = {}
        self._metadata_schema: Dict[str, Any] = {}
        self._validation_rules: Dict[str, Any] = {}

        self._load_and_validate()

    # -----------------------------
    # Internal load & validation
    # -----------------------------
    def _load_and_validate(self) -> None:
        if not self.contract_path.exists():
            raise MasterSchemaError(
                f"master.json not found at: {self.contract_path}"
            )

        try:
            with self.contract_path.open("r", encoding="utf-8") as f:
                self._raw = json.load(f)
        except json.JSONDecodeError as e:
            raise MasterSchemaError("master.json is not valid JSON") from e

        if not isinstance(self._raw, dict):
            raise MasterSchemaError("master.json must be a JSON object")

        missing = self.REQUIRED_TOP_LEVEL_KEYS - self._raw.keys()
        if missing:
            raise MasterSchemaError(
                f"master.json missing required keys: {sorted(missing)}"
            )

        version = self._raw["contract_version"]
        if version not in self.SUPPORTED_CONTRACT_VERSIONS:
            raise MasterSchemaError(
                f"Unsupported contract_version '{version}'. "
                f"Supported: {self.SUPPORTED_CONTRACT_VERSIONS}"
            )

        # ---- Extract sections ----
        self._modules = self._validate_modules(self._raw["modules"])
        self._metadata_schema = self._validate_metadata_schema(
            self._raw["metadata_schema"]
        )
        self._validation_rules = self._validate_validation_rules(
            self._raw["validation_rules"]
        )

    # -----------------------------
    # Section validators
    # -----------------------------
    def _validate_modules(self, modules: Any) -> Dict[str, Any]:
        if not isinstance(modules, dict):
            raise MasterSchemaError("modules must be an object")

        if not modules:
            raise MasterSchemaError("modules cannot be empty")

        for module_name, spec in modules.items():
            if "columns" not in spec:
                raise MasterSchemaError(
                    f"Module '{module_name}' missing 'columns'"
                )
            if "column_count" not in spec:
                raise MasterSchemaError(
                    f"Module '{module_name}' missing 'column_count'"
                )
            if not isinstance(spec["columns"], dict):
                raise MasterSchemaError(
                    f"Module '{module_name}' columns must be an object"
                )

        return modules

    def _validate_metadata_schema(self, metadata: Any) -> Dict[str, Any]:
        if not isinstance(metadata, dict):
            raise MasterSchemaError("metadata_schema must be an object")

        required_fields = {"row_hash", "vehicle_id", "module", "source_file", "ingest_ts"}
        missing = required_fields - metadata.keys()
        if missing:
            raise MasterSchemaError(
                f"metadata_schema missing fields: {sorted(missing)}"
            )

        return metadata

    def _validate_validation_rules(self, rules: Any) -> Dict[str, Any]:
        if not isinstance(rules, dict):
            raise MasterSchemaError("validation_rules must be an object")

        required_flags = {
            "strict_columns",
            "allow_extra_fields",
            "enforce_column_order",
            "enforce_dtypes",
            "reject_null_violations",
        }

        missing = required_flags - rules.keys()
        if missing:
            raise MasterSchemaError(
                f"validation_rules missing flags: {sorted(missing)}"
            )

        return rules

    # -----------------------------
    # Public accessors (read-only)
    # -----------------------------
    @property
    def modules(self) -> Dict[str, Any]:
        return self._modules

    @property
    def metadata_schema(self) -> Dict[str, Any]:
        return self._metadata_schema

    @property
    def validation_rules(self) -> Dict[str, Any]:
        return self._validation_rules

    @property
    def contract_version(self) -> str:
        return self._raw["contract_version"]

    @property
    def reference_sim(self) -> str:
        return self._raw["reference_sim"]
