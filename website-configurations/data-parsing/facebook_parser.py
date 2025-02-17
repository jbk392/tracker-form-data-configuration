# Look for automatic matching in configuration
def fbook_extract_config(data, pk, cursor):
    # if the key for automatic matching is identified
    autoKeyFound = False
    # the PII values for automatic matching key
    matching_keys = None
    # the pixel Id
    pixelId = 'Not Extracted'

    # variables to help parse
    dataBodyLength = len(data['body'])
    fbEventsPlugin = data['body'][dataBodyLength-2]

    try:
        dataArguments = fbEventsPlugin['expression']['arguments']
        pluginValues = dataArguments[1]['properties'][1]['value']['body']['body']

        for val in pluginValues:
            # will be one of: 'config', 'fbq', 'instance'
            expressionType = val['expression']['callee']['object']['name']
            expressionProperty = val['expression']['callee']['property']['name']

            arguments = val['expression']['arguments']
            if expressionType == 'instance' and expressionProperty == 'configLoaded':
                pixelId = arguments[0]['value']
            
            if expressionType == 'config':
                key = arguments[1]['value']
                properties = arguments[2]['properties']
                if key == 'automaticMatching':   
                    selectedMatchKeys = properties[0]['value']['elements']
                    values_string = ','.join([d['value'] for d in selectedMatchKeys])
                    matching_keys = values_string
                    autoKeyFound = True

    except KeyError as e:
        return 'ERROR PARSING CONFIG FILE'

    return autoKeyFound, pixelId, matching_keys
