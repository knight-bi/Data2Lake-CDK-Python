import json

table_list =['table1','table2']
rules =[]
for index, table in enumerate(table_list,1):
    rules.append(
        {
            "rule-type": "selection",
            "rule-id": str(index),
            "rule-name": str(index),
            "object-locator": {
                "schema-name": "pcr_gvg",
                "table-name": table
            },
            "rule-action": "include",
            "filters": []
        }
    )

mapping_rules ={
    "rules":rules
}

mr_json = json.dumps(mapping_rules,indent = 4)
with open ('mr.json','w') as outfile:
    outfile.write(mr_json)


