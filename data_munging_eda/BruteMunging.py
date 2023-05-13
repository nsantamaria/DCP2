import pandas as pd
import json


class Munging:

    def get_set(self, file):
        df = pd.read_csv(file)
        return df


if __name__ == '__main__':
    dframe = Munging().get_set("../Datasets/new_sample.csv")
    rows = []

    new_col_names = []
    new_row_vals = []
    for row_index, row in dframe.iterrows():
        if row[15] != "[]":
            data = json.loads(row[15])
            for dictionary in data:
                col_name = "target" + "_" + dictionary["target"]
                try:
                    row_value = dictionary["segment"]
                except:
                    continue
                if col_name in dframe.columns:
                    dframe.at[row_index, col_name] = row_value
                else:
                    dframe[col_name] = [''] * len(dframe)
                    dframe.at[row_index, col_name] = row_value

    print(dframe.iloc[:, -4:])
