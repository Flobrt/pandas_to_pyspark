import pandas as pd


def DataFiltering_PatientRecords_df():
    df = pd.DataFrame(
        {
            "patient_id": [1, 2, 3, 4, 5],
            "age": [34, 45, 50, 20, 15],
            "department": [
                "Cardiology",
                "Neurology",
                "Orthopedics",
                "Cardiology",
                "Neurology",
            ],
        }
    )

    # Filtrer les patients Ã¢gÃ©s de plus de 30 ans
    filtered_df = df[df["age"] > 30]

    return filtered_df
