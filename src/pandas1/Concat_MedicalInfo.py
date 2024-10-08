import pandas as pd


def Concat_MedicalInfo_pd():
    # DonnÃ©es
    df = pd.DataFrame(
        {
            "patient_name": ["John Doe", "Jane Smith", "Alice Brown"],
            "diagnosis": ["Diabetes", "Heart Disease", "Hypertension"],
        }
    )

    # Conversion en minuscules et ajout d'un champ
    df["diagnosis_lower"] = df["diagnosis"].str.lower()
    df["full_info"] = df["patient_name"] + " - " + df["diagnosis_lower"]

    return df
