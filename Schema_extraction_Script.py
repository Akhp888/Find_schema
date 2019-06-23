import os
import pandas as pd


class Find_schema:

    # finding schemas of all files path in list
    @staticmethod
    def detect_schema(files_list):
        output = pd.DataFrame([])
        var_all = []
        file_all = []
        for file in files_list:
            if file.endswith(".xls") or file.endswith(
                    ".xlsx") or file.endswith(".XLSX"):
                data = pd.read_excel(file)
            elif file.endswith(".DAT"):
                if os.stat(file).st_size != 0:
                    data = pd.read_csv(file, skiprows=1, delimiter=';')
                else:
                    pass
            else:
                print("unknown file format")
            column_name = data.columns.values.tolist()
            file_path = file
            var_all.append(column_name)
            file_all.append(file_path)
        output_schema_raw = pd.DataFrame(
            {'File_name': file_all, 'Schema': var_all})
        # for shuffled columns in schema
        sorted_schema = output_schema_raw.Schema.apply(sorted)
        output_schema_raw['sorted_schema'] = sorted_schema
        return output_schema_raw

    # getting all the files inside the path of extention xls,xlsx and dat
    @staticmethod
    def fetch_all_file_schema(path):
        filepath = path
        list_of_files = []
        for root, dirs, files in os.walk(path):
            for file in files:
                if file.endswith(".DAT") or file.endswith(
                        ".xls") or file.endswith(".xlsx") or file.endswith(".XLSX"):
                    list_of_files.append(os.path.join(root, file))
        schema_of_files = Find_schema.detect_schema(list_of_files)
        return schema_of_files

    # unknown schema name generator for mapping sheet
    @staticmethod
    def new_mapper_maker(dataset_nan):
        dataset = dataset_nan
        count = 0
        for index, row in dataset.iterrows():
            if pd.isnull(dataset.iloc[index].iloc[3]):
                count = count + 1
                dataset.schema_name[index] = "Schema_type_" + str(count)
        return dataset

    # mapping sheet updating with new found schema
    @staticmethod
    def mapping_schema(unmapped_dataset):
        dataset = unmapped_dataset.copy()
        dataset['sorted_schema'] = dataset['sorted_schema'].astype(str)
        # importing mapping file
        mapping_df = pd.read_csv('Mapping_sheet_schema.csv')
        mapping_dict = dict(
            mapping_df[['sorted_schema', 'schema_name']].values)
        dataset['schema_name'] = dataset.sorted_schema.map(mapping_dict)
        dataset = dataset.drop_duplicates(
            subset=['sorted_schema', 'schema_name']).reset_index()
        dataset = dataset.drop(['index'], axis=1)
        dataset_mapped = Find_schema.new_mapper_maker(dataset)
        dataset_mapped[['sorted_schema', 'schema_name']].to_csv(
            'Mapping_sheet_schema.csv', index=False)

    # mapping the updated sheet to schema found
    @staticmethod
    def generate_schema(dataset):
        dataset = dataset.drop(['Schema'], axis=1)
        dataset[['File_name', 'sorted_schema']] = dataset[[
            'File_name', 'sorted_schema']].astype(str)
        grouped_schema_list = dataset.groupby('sorted_schema')['File_name'].apply(
            lambda x: "{%s}" % ', '.join(x))
        grouped_schema_list_df = pd.DataFrame(grouped_schema_list)
        grouped_schema_list_df.reset_index(level=0, inplace=True)
        mapping_df = pd.read_csv('Mapping_sheet_schema.csv')
        mapping_dict = dict(
            mapping_df[['sorted_schema', 'schema_name']].values)
        #return (grouped_schema_list_df)
        grouped_schema_list_df['schema_name'] = grouped_schema_list_df.sorted_schema.map(
            mapping_dict)
        grouped_schema_list_df.to_csv('Schema_list.csv')
        grouped_schema_count = dataset.groupby('sorted_schema').count()
        grouped_schema_count_df = pd.DataFrame(grouped_schema_count)
        grouped_schema_count.reset_index(level=0, inplace=True)
        grouped_schema_count_df['schema_name'] = grouped_schema_count_df.sorted_schema.map(
            mapping_dict)
        grouped_schema_count_df.to_csv('Schema_count.csv')
        return grouped_schema_list_df, grouped_schema_count_df

    # main function for script
    @staticmethod
    def main(datapath):
        schemas_all_df = Find_schema.fetch_all_file_schema(datapath)
        # updating mapping sheet
        Find_schema.mapping_schema(schemas_all_df)
        # generate unique schema list and count
        return Find_schema.generate_schema(schemas_all_df)

