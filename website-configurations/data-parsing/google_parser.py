import json 

table_cols = []
key_counts = {}

path_literal = 'resource/tags/'

# these were some hypothesis for static evaluation of the google file
# ultimately, we weren't able to definitively know
def status_check(status_checker):
    compound_status = None
    if status_checker['condition-4'] == True:
        '''if status_checker['condition-5'] == True:
            compound_status = True
        else:
            compound_status = False'''
        return False
    if status_checker['condition-6'] == True:
       return True
    if (
        #status_checker['condition-1'] == True and
        status_checker['condition-2'] == True and
        status_checker['condition-3'] == True and
        (compound_status == True or compound_status == None)
    ):
        return True 
    return False

def get_key(key):
    if key in key_counts:
        count = key_counts[key]
        new_count = count+1
        new_key = key + '-' + str(new_count)
        key_counts[key] = new_count
    else:
        key_counts[key] = 1
        new_key = key

    return new_key

def gtag_extract_vtp_properties(parsed_json, cursor, pk):

    css_values = [
       'vtp_emailValue',
       'vtp_phoneValue',
       'vtp_firstNameValue',
       'vtp_lastNameValue',
       'vtp_streetValue',
       'vtp_cityValue',
       'vtp_streetValue',
       'vtp_countryValue',
       'vtp_postalCodeValue'
    ]

    css_values_observed = []

    staticValue = False
    gtmValues = {'vtp_conversionId': []}
    remoteConfigOptions = [] # used for condition-5
    '''
    1) vtp_isEnabled must exist for data collection to be true
    2) __ccd_add_1p_data must exist, for data collection to be true
    3) __ccd_em_form must exist for data collection to be true
    4) if productSettings block exists, data collection is false
        (ex: var productSettings = {
            "AW-874615328": {
                "preAutoPii": true
            }
        };)
    5) 
    '''
    status_checker = {
        'condition-1': False,
        'condition-2': False,
        'condition-3': False,
        'condition-4': False,
        'condition-5': None,
        'condition-6': False # ccs values
    }
    baseObject = parsed_json['body'][0]['expression']['callee']
    data = baseObject['body']['body']
        
    firstDataItem = data[0]
    firstDataDeclarations = firstDataItem['declarations'][0] # there is only one item in 'declarations', but it is an array
    properties = firstDataDeclarations['init']['properties']
    resource = properties[0]['value']['properties']    
    tags = resource[2]['value']['elements']
    for tag in tags:
        tagProperties = tag['properties']
        # each item in the properties array corresponds to a key value pair in the function object
        # the first item is always the function name, ex: "function": "__ccd_em_form"
        function = tagProperties[0]['value']['value']
        function_path = path_literal + function
        new_key = get_key(function_path)

        #if function == '__ccd_add_1p_data':
        #    status_checker['condition-1'] = True
        if function == '__ccd_em_form':
            status_checker['condition-3'] = True

        # the second item should be priority, if it is present
        if 'priority' in tagProperties[1]['key']['value']:
            checked_key = get_key('priority')
            priority = tagProperties[1]['value']['value']
            table_cols.append({checked_key: priority})
        else:
            key = path_literal + function
            new_key = get_key(key)
        table_cols.append({new_key: True})
        # there are a variable amount of columns in the rest
        for element in tagProperties[2:]:
            try:
                key = new_key + '/' + element['key']['value']

                # check for any values set on CCS selectors
                if function == '__ogt_1p_data_v2' and element['key']['value'] in css_values:
                   if element['value']['value'] != '':
                      css_values_observed.append({'key': element['key']['value'], 'value': element['value']['value']})
                      status_checker['condition-6'] = True

                if function == '__ogt_1p_data_v2' and element['key']['value'] == 'vtp_isEnabled':
                    status_checker['condition-2'] = True
                if element['key']['value'] == 'vtp_enableConversionLinker':
                    gtmValues['vtp_enableConversionLinker'] = True

                if element['key']['value'] == 'vtp_conversionId' and element['value']['value'] not in gtmValues['vtp_conversionId']:
                    gtmValues['vtp_conversionId'].append(element['value']['value'])

                if function == '__rep' and element['key']['value'] == 'vtp_remoteConfig':
                    remote_config = element['value']['elements']
                    # I'm not sure if this is always structured the same, so I'm searching rather
                    # than directly accessing for now
                    for item in remote_config:
                        if 'elements' in item:
                            enhanced_conversions = item['elements']
                            for enhanced_conversion in enhanced_conversions:
                                if 'elements' in enhanced_conversion:
                                    enhanced_conversion_mode_items = enhanced_conversion['elements']
                                    config_value_index = -1
                                    for index, mode_item in enumerate(enhanced_conversion_mode_items):
                                        if 'value' in mode_item:
                                            if mode_item['value'] == 'enhanced_conversions_mode':
                                                config_value_index = index+1
                                            if index == config_value_index:
                                                remoteConfigOptions.append(mode_item['value'])
                    if 'off' in remoteConfigOptions and 'manual' not in remoteConfigOptions:
                        status_checker['condition-5'] = True
                checked_key = get_key(key)
                if element['value']['type'] == 'ArrayExpression':
                    value = json.dumps(element['value']['elements'])
                else:
                    value = element['value']['value']
                table_cols.append({checked_key: value})
            except Exception as e:
                table_cols.append({'error': str(e)})
                pass

    try:
        runtime = properties[1]
        table_cols.append({'runtime': json.dumps(runtime['value']['value'])})
    except Exception as e:
        table_cols.append({'error': str(e)})
        pass

    try:
        secondDataItem = data[1]
        secondDataDeclarations = secondDataItem['declarations'][0]
        # we don't know if this property will exist
        if 'id' in secondDataDeclarations and secondDataDeclarations['init'] != None:
            productSettings = secondDataDeclarations['init']['properties']
            value = productSettings[0]['value']['properties'][0]['key']['value']
            status_checker['condition-4'] = True
            table_cols.append({'productSettings': json.dumps(productSettings)})
    except Exception as e:
        table_cols.append({'error': str(e)})
        pass

    staticValue = status_check(status_checker)
    return staticValue, gtmValues, css_values_observed

