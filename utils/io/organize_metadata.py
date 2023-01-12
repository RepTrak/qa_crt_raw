import json


def prepare_metadata(json_metadata, spss_metadata, dataframe_cols):
    print("Working on json metadata.")
    # instantiate variable dictionaries for spss saving.
    titles = {}
    value_labels = {}

    json_variables = json_metadata['variables']
    for json_variable in json_variables:
        titles[json_variable['label']] = json_variable['title']
        value_label = {}
        if 'values' in json_variable.keys():
            for values in json_variable['values']:
                value_label[values['value']] = values['title']

                # respStatus is messed up in the raw data. This fixes the labels
            if json_variable['label'] == 'respStatus':
                value_label[2] = 'Complete'
                value_label[3] = 'Terminated'
                value_label[5] = 'Overquota'
                value_label[1] = 'In Process/Partial'

            value_labels[json_variable['label']] = value_label

    # likert-nominal sorting
    '''
    Requirements: 
    All values need to be numeric. 
    Ordinal: 7 or 8
    Nominal: Misc
    ''';
    variable_measure = {}
    for k, v in value_labels.items():
        if len(v) == 7 or len(v) == 8:
            variable_measure[k] = 'ordinal'
        else:
            variable_measure[k] = 'nominal'

    # Checking variables that exist in the json data
    exists_in_data = []
    not_exists_in_data = []
    for k, v in titles.items():
        if k in dataframe_cols:
            exists_in_data.append(k)
        else:
            not_exists_in_data.append(k)

    print(f"Number of columns that exist in the dataframe (cols: \
{len(dataframe_cols)}) ALSO in meta: {len(exists_in_data)}")
    print(f"Number of columns that DON'T exist in the dataframe ALSO in meta: {len(not_exists_in_data)}")

    # Assemble what we need.
    missing_ranges = spss_metadata.missing_ranges
    variable_display_width = spss_metadata.variable_display_width

    # Add what we don't have from SPSS_META to JSON_META
    json_variables_from_meta = [x.lower() for x in titles.keys()]
    new_variables = []
    existing_variables = []

    for k, v in spss_metadata.column_names_to_labels.items():
        if k.lower() not in json_variables_from_meta:
            titles[k] = v
            new_variables.append(k)
        else:
            # Only need to do this once.
            existing_variables.append(k)

    for k, v in spss_metadata.variable_value_labels.items():
        if k.lower() not in json_variables_from_meta:
            value_labels[k] = v

    for k, v in spss_metadata.variable_measure.items():
        if k.lower() not in json_variables_from_meta:
            variable_measure[k] = v

    lnv = len(new_variables)
    lev = len(existing_variables)
    print(f"Found {lnv} new variables.\nFound {lev} variables already in the data.")

    meta_dic = {'column_labels': titles,
                'variable_value_labels': value_labels,
                'missing_ranges': missing_ranges,
                'variable_display_width': variable_display_width,
                'variable_measure': variable_measure}
    return (meta_dic)