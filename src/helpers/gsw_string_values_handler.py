import pandas as pd

GSW_STR_COLUMNS_NAMES = ['Glascow_coma_scale_eye_opening', 'Glascow_coma_scale_motor_response', 'Glascow_coma_scale_verbal_response']

def unique_handle_for_non_numeric_gsw(df: pd.DataFrame):
    if 'Glascow_coma_scale_eye_opening' in df.columns:
        value_mapping = {
            'Spontaneously': '4 Spontaneously',
            'To Speech': '3 To Speech',
            'To Pain': '2 To Pain',
            'No response': '1 No response',
        }
        df['Glascow_coma_scale_eye_opening'] = df['Glascow_coma_scale_eye_opening'].replace(value_mapping)
        non_digit_count = df['Glascow_coma_scale_eye_opening'].str.match(r'^[^\d]').sum()
        if non_digit_count > 0:
            print("WARNING - Glascow_coma_scale_eye_opening --- non digit start value counts -- ", non_digit_count)
            print(df['Glascow_coma_scale_eye_opening'][df['Glascow_coma_scale_eye_opening'].str.match(r'^[^\d]')])
    if 'Glascow_coma_scale_motor_response' in df.columns:
        value_mapping = {
            'Obeys Commands': '6 Obeys Commands',
            'Localizes Pain': '5 Localizes Pain',
            'Flex-withdraws': '4 Flex-withdraws',
            'Abnormal Flexion': '3 Abnormal Flexion',
            'Abnormal extension': '2 Abnormal extension',
            'No response': '1 No response'
        }
        df['Glascow_coma_scale_motor_response'] = df['Glascow_coma_scale_motor_response'].replace(value_mapping)
        non_digit_count = df['Glascow_coma_scale_motor_response'].str.match(r'^[^\d]').sum()
        if non_digit_count > 0:
            print("WARNING - Glascow_coma_scale_motor_response --- non digit start value counts -- ", non_digit_count)
            print(df['Glascow_coma_scale_motor_response'][df['Glascow_coma_scale_motor_response'].str.match(r'^[^\d]')])

    if 'Glascow_coma_scale_verbal_response' in df.columns:
        value_mapping = {
            'Oriented': '5 Oriented',
            'Confused': '4 Confused',
            'Inappropriate Words': '3 Inappropriate Words',
            'Incomprehensible sounds': '2 Incomprehensible sounds',
            'No response': '1 No response',
            'No Response': '1 No Response',
            'No Response-ETT': '1 No Response-ETT'
        }
        df['Glascow_coma_scale_verbal_response'] = df['Glascow_coma_scale_verbal_response'].replace(value_mapping)
        non_digit_count = df['Glascow_coma_scale_verbal_response'].str.match(r'^[^\d]').sum()
        if non_digit_count > 0:
            print("WARNING - Glascow_coma_scale_verbal_response --- non digit start value counts -- ", non_digit_count)
            print(df['Glascow_coma_scale_verbal_response'][df['Glascow_coma_scale_verbal_response'].str.match(r'^[^\d]')])
    return df


def number_to_gsw_str(col_name, number, with_num_in_str = False):
    if pd.isna(number):
        return number

    number = int(number)
    map_n_to_gsw = {
        'Glascow_coma_scale_eye_opening': {
            4: 'Spontaneously',
            3: 'To Speech',
            2: 'To Pain',
            1: 'No response'
        },
        'Glascow_coma_scale_motor_response': {
            6: 'Obeys Commands',
            5: 'Localizes Pain',
            4: 'Flex-withdraws',
            3: 'Abnormal Flexion',
            2: 'Abnormal extension',
            1: 'No response'
        },
        'Glascow_coma_scale_verbal_response': {
            5: 'Oriented',
            4: 'Confused',
            3: 'Inappropriate Words',
            2: 'Incomprehensible sounds',
            1: 'No response',
        },
    }

    res = map_n_to_gsw[col_name][number]

    if with_num_in_str:
        return f'{number} {res}'

    return res
