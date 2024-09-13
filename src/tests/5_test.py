import pytest

from pandas1.MissingValues_Handling import MissingValues_Handling_df
from pyspark1.MissingValues_Handling import MissingValues_Handling


class Test5:
    def test_5(self):
        df = MissingValues_Handling_df()
        df_ps = MissingValues_Handling()
        df_pd = df_ps.select("*").toPandas()
        df_equals = df.equals(df_pd)

        print(df.dtypes)
        print(df)
        print(df_pd.dtypes)
        print(df_pd)

        assert df_equals == True
