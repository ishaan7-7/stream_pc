from pathlib import Path
from typing import List, Dict, Any
import re

from replay.service.schema_loader import MasterSchema


SIM_FOLDER_PATTERN = re.compile(r"^sim\d+$")


class SourceDiscoveryError(RuntimeError):
    pass


def discover_sources(
    *,
    pipeline_root: Path,
    schema: MasterSchema,
    enabled_sims: List[str] | None,
) -> List[Dict[str, Any]]:

    vehicles_root = pipeline_root / "data" / "vehicles"

    if not vehicles_root.exists():
        raise SourceDiscoveryError(
            f"Vehicles directory not found: {vehicles_root}"
        )

    
    discovered_sims = sorted(
        p.name
        for p in vehicles_root.iterdir()
        if p.is_dir() and SIM_FOLDER_PATTERN.match(p.name)
    )

    if not discovered_sims:
        raise SourceDiscoveryError("No SIM folders discovered")

    
    if enabled_sims is not None:
        unknown = set(enabled_sims) - set(discovered_sims)
        if unknown:
            raise SourceDiscoveryError(
                f"Enabled SIMs not found on disk: {sorted(unknown)}"
            )
        active_sims = sorted(enabled_sims)
    else:
        active_sims = discovered_sims

    sources: List[Dict[str, Any]] = []

    for sim in active_sims:
        sim_dir = vehicles_root / sim

        for module_name, module_spec in schema.modules.items():
            pattern = module_spec.get("file_pattern")
            if not pattern:
                raise SourceDiscoveryError(
                    f"Module '{module_name}' missing file_pattern"
                )

            matches = list(sim_dir.glob(pattern))

            if len(matches) != 1:
                raise SourceDiscoveryError(
                    f"[{sim}] Expected exactly 1 CSV for module '{module_name}', "
                    f"found {len(matches)}"
                )

            csv_path = matches[0]

            source = {
                "vehicle_id": sim,
                "module": module_name,
                "csv_path": csv_path,
                "source_id": f"{sim}_{module_name}",
            }

            sources.append(source)

    return sources
