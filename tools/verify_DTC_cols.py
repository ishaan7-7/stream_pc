import os
import json
from pathlib import Path
from deltalake import DeltaTable

def verify_dtc_columns():
    print("==================================================")
    print("🔍 VERIFYING DTC FEATURES AGAINST SILVER TABLES")
    print("==================================================\n")

    tools_dir = Path(__file__).resolve().parent
    root_dir = tools_dir.parent
    dtc_master_path = root_dir / "contracts" / "DTC_master.json"
    silver_dir = root_dir / "data" / "delta" / "silver"

    if not dtc_master_path.exists():
        print(f"❌ Error: DTC Master contract not found at {dtc_master_path}")
        return

    with open(dtc_master_path, 'r') as f:
        dtc_master = json.load(f)

    modules = dtc_master.get("modules", {})
    if not modules:
        print("⚠️ No modules found in DTC_master.json.")
        return

    total_dtcs = 0
    failed_dtcs = 0

    for module_name, dtc_list in modules.items():
        print(f"📦 MODULE: {module_name.upper()}")
        print("-" * 50)
        
        table_path = silver_dir / module_name
        
        if not table_path.exists():
            print(f"   ⚠️ Silver table for '{module_name}' does not exist yet. Run stream emulator first.\n")
            continue
            
        try:
            dt = DeltaTable(str(table_path))
            silver_columns = set(dt.to_pyarrow_dataset().schema.names)
        except Exception as e:
            print(f"   ⚠️ Could not read Delta table for '{module_name}': {e}\n")
            continue
        
        print(f"   📋 Columns found in '{module_name}' Silver table:")
        for col in sorted(silver_columns):
            print(f"      - {col}")
        print()
        
        module_all_good = True
        
        for dtc in dtc_list:
            dtc_code = dtc.get("dtc_code", "UNKNOWN")
            features = dtc.get("features", [])
            total_dtcs += 1
            
            missing_cols = [f for f in features if f not in silver_columns]
            
            if missing_cols:
                print(f"   ❌ {dtc_code}: Missing {len(missing_cols)} required columns!")
                for col in missing_cols:
                    print(f"      -> Expected: '{col}' (Not found in Silver table)")
                failed_dtcs += 1
                module_all_good = False
                
        if module_all_good:
            print(f"   ✅ All {len(dtc_list)} DTC codes mapped perfectly to Silver table.")
            
        print("\n" + "="*50 + "\n")

    print(f"📊 SUMMARY: {total_dtcs - failed_dtcs}/{total_dtcs} DTCs perfectly mapped.")
    if failed_dtcs > 0:
        print(f"⚠️  WARNING: {failed_dtcs} DTCs will be completely skipped during deep dives!")
        print("    Action Required:")
        print("    1. Open contracts/DTC_master.json")
        print("    2. Fix the typos in the feature lists for the failed DTCs above.")
        print("    3. Run DTC/train_all_dtcs.bat to retrain models on correct names.")
    else:
        print(f"🚀 SUCCESS: All DTCs perfectly aligned. Ready for real-time inference!")
    print("==================================================")

if __name__ == "__main__":
    verify_dtc_columns()