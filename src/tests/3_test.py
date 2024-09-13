import pytest

from pandas1.ConditionalCalculations_MedicalAge import (
    ConditionalCalculations_MedicalAge_df,
)
from pyspark1.ConditionalCalculations_MedicalAge import (
    ConditionalCalculations_MedicalAge,
)


class Test3:
    def test_3(self):
        df = ConditionalCalculations_MedicalAge_df()
        df_ps = ConditionalCalculations_MedicalAge()
        df_pd = df_ps.select("*").toPandas()
        df_equals = df.equals(df_pd)

        print(df.dtypes)
        print(df)
        print(df_pd.dtypes)
        print(df_pd)
        assert df_equals == True


# import pytest

# from pyspark1.Concat_MedicalInfo import Concat_MedicalInfo
# from pandas1.Concat_MedicalInfo import Concat_MedicalInfo_pd

# class Test1():
#     def test_1(self):
#         # Dataframe Pandas
#         df = Concat_MedicalInfo_pd()
#         # Dataframe Pyspark
#         result_df = Concat_MedicalInfo()
#         # Conversion en df Pandas
#         result_pdf = result_df.select("*").toPandas()
#         # Comparaison
#         dataframes_equal = df.equals(result_pdf)

#         assert dataframes_equal == True
