import pytest

from pandas1.DataFiltering_PatientRecords import DataFiltering_PatientRecords_df
from pyspark1.DataFiltering_PatientRecords import DataFiltering_PatientRecords

class Test4():
    def test_4(self):
        df = DataFiltering_PatientRecords_df()
        df_ps = DataFiltering_PatientRecords()
        df_pd = df_ps.select("*").toPandas() 
        df_equals = df.equals(df_pd)

        print(df.dtypes)
        print(df)
        print(df_pd.dtypes)
        print(df_pd)
        
        assert df_equals == True