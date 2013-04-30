profiles = [
    {
        "builtin": true,
        "id": 0,
        "name": "Default profile",
        "styleRules": []
    },
    {
        "id": 1,
        "name": "kaiso",
        "styleRules": [
            {
                "filters": [
                    {
                        "compareValue": "TypeSystem",
                        "id": 1,
                        "method": "==",
                        "propertyName": "id",
                        "type": "propertyFilter"
                    }
                ],
                "id": 3,
                "order": null,
                "style": {
                    "labelColor": "#eeeeee",
                    "labelFont": "monospace",
                    "labelPattern": "[{id}] {prop.id}",
                    "labelSize": 10,
                    "shape": "box",
                    "shapeColor": "rgb(0, 0, 0)",
                    "type": "node"
                },
                "target": "node"
            },
            {
                "filters": [
                    {
                        "compareValue": "PersistableMeta",
                        "id": 1,
                        "method": "==",
                        "propertyName": "__type__",
                        "type": "propertyFilter"
                    }
                ],
                "id": 3,
                "order": null,
                "style": {
                    "labelColor": "#eeeeee",
                    "labelFont": "monospace",
                    "labelPattern": "[{id}] {prop.__type__}: {prop.id}",
                    "labelSize": 10,
                    "shape": "box",
                    "shapeColor": "rgb(2, 138, 179)",
                    "type": "node"
                },
                "target": "node"
            },
            {
                "filters": [
                    {
                        "id": 2,
                        "method": "exists",
                        "propertyName": "name",
                        "type": "propertyFilter"
                    }
                ],
                "id": 1,
                "order": 1,
                "style": {
                    "labelColor": "#eeeeee",
                    "labelFont": "monospace",
                    "labelPattern": "[{id}] {prop.__type__}: {prop.name}",
                    "labelSize": 10,
                    "shape": "box",
                    "shapeColor": "rgb(130, 4, 130)",
                    "type": "node"
                },
                "target": "node"
            },
            {
                "filters": [],
                "id": 2,
                "order": 3,
                "style": {
                    "labelColor": "#eeeeee",
                    "labelFont": "monospace",
                    "labelPattern": "[{id}] {prop.__type__}",
                    "labelSize": 10,
                    "shape": "box",
                    "shapeColor": "rgb(174, 196, 6)",
                    "type": "node"
                },
                "target": "node"
            }
        ]
    }
]

profiles_str = JSON.stringify(profiles)
localStorage.setItem("databrowser.visualization.profiles", profiles_str)
localStorage.setItem("databrowser.visualization.currentProfile", 1)