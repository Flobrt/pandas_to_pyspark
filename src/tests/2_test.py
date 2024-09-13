import pytest

from pyspark1.Concat_MedicalInfo import Concat_MedicalInfo
from pandas1.Concat_MedicalInfo import Concat_MedicalInfo_pd


class Test2:
    def test_2(self):
        # Dataframe Pandas
        df = Concat_MedicalInfo_pd()
        df_ps = Concat_MedicalInfo()
        df_pd = df_ps.select("*").toPandas()
        df_equals = df.equals(df_pd)

        assert df_equals == True
