import os
from deltalake import write_deltalake
from src import config

class SilverWriter:
    def write(self, df, module):
        """ Appends inferred DataFrame to Silver Delta Table """
        path = os.path.join(config.SILVER_DIR, module)
        os.makedirs(path, exist_ok=True)
        
        try:
            write_deltalake(
                path, 
                df, 
                mode="append", 
                schema_mode="merge" 
            )
        except Exception as e:
            print(f"❌ Failed to write {module} to Silver: {e}")
            raise e