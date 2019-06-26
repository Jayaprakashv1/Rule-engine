## Author: Jayaprakash v
## File: Rule Engine
## Description: Based on the rules, data will get processed

import operator
import json
import datetime

input_file = 'raw_data.json'
rules_file = 'rules.json'
exp_dict = {"gt": "val1 > val2",
            "lt": "val1 < val2",
            "eq": "val1 == val2",
            "nq": "val1 != val2",
            }

def __init__(self):
    pass

# Start of functional code
def main():

    data = readjson()

    invalid_signal = validaterules(data)

    for sig in invalid_signal:
        print('Invalid signal:'+ sig)

# Read the raw json data
def readjson():
    with open(input_file) as json_file:  
        datastream = json.load(json_file)

        return datastream

# Get rule based on the value type
def validaterules(datastream):
    rejected = []

    for data in datastream:
        rule_exp,rule_value,rule_type = getconfig(data['signal'])

        if(data['value_type'] == 'Integer' and data['value_type'] == rule_type):
            invalid_signal = validateinteger(data,rule_exp,rule_value)

            if(invalid_signal != None):
                rejected.append(invalid_signal)

        elif(data['value_type'] == 'String' and data['value_type'] == rule_type):
            invalid_signal = validatestring(data,rule_exp,rule_value)

            if(invalid_signal != None):
                rejected.append(invalid_signal)

        elif(data['value_type'] == 'Datetime' and data['value_type'] == rule_type):
            invalid_signal = validatedate(data,rule_exp,rule_value)

            if(invalid_signal != None):
                rejected.append(invalid_signal)
        else:
            pass
            #print('no rule')
    return rejected

# Apply rules for Integer type
def validateinteger(data,rule_exp,rule_value):
    
    if(rule_exp!=""):
        val1 = int(float(data['value']))
        val2 = int(float(rule_value))
    
        con = eval(exp_dict[rule_exp])
    
        if(con):
            return data['signal']

# Apply rules for String type
def validatestring(data,rule_exp,rule_value):
    
    if(rule_exp!=""):
        val1 = data['value']
        val2 = rule_value
    
        con = eval(exp_dict[rule_exp])
    
        if(con):
            return data['signal']

# Apply rules for Datetime type
def validatedate(data,rule_exp,rule_value):
    
    if(rule_exp!=""):
        val1 = datetime.datetime.strptime(data['value'], '%Y-%m-%d %H:%M:%S')
    
        if(rule_value=='future'):
            val2 = datetime.datetime.now()
        else:
            val2 = datetime.datetime.strptime(rule_value, '%Y-%m-%d %H:%M:%S')

        con = eval(exp_dict[rule_exp])
    
        if(con):
            return data['signal']

# Read the rule based on the signal name specified
def getconfig(signalname):

    with open(rules_file) as config_file:
        rules = json.load(config_file)
        
        for rule in rules:
            if(signalname == rule['name']):
                return rule['condition'], rule['value'], rule['type']
        return "","",""

# Start program
main()
