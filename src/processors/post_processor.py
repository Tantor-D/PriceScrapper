import pandas as pd

class PostProcessor:
    def remove_duplicates(self, df, subset=None):
        """Remove duplicate entries from the DataFrame based on a subset of columns.
        Default subset is ['Link'] to ensure unique products.
        """
        if subset is None:
            subset = ["Link"]
        # Drop duplicates and keep the first occurrence of each unique subset value
        deduped_df = df.drop_duplicates(subset=subset, keep="first").reset_index(drop=True)
        return deduped_df
