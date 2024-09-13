import pytest

from pyspark1.Agg_Departments import Agg_Departments
from pandas1.Agg_Departments import Agg_Departments_df


class Test1:
    def test_1(self):
        df = Agg_Departments_df()
        df_ps = Agg_Departments()
        df_pd = df_ps.select("*").toPandas()
        df_equals = df.equals(df_pd)

        print(df.dtypes)
        print(df)
        print(df_pd.dtypes)
        print(df_pd)

        assert df_equals == True
