export const seed={
  "runId": "run1",
  "job": {
    "namespace": "local",
    "name": "prepare"
  },
  "outputs": [
    {
      "namespace": "local",
      "name": "clean"
    }
  ]
};
export const seedPath="/run-summary";
export const routes=[
  {
    "path": "/events",
    "body": {
      "next": "/events?offset=1",
      "events": [
        {
          "eventId": "e2",
          "eventTime": 20,
          "eventType": "COMPLETE",
          "run": {
            "runId": "run1"
          },
          "job": {
            "namespace": "local",
            "name": "prepare"
          },
          "outputs": [
            {
              "namespace": "local",
              "name": "clean",
              "outputFacets": {
                "rows": {
                  "producer": "p1",
                  "schemaURL": "urn:rows:v1",
                  "count": 12
                }
              }
            }
          ]
        }
      ]
    },
    "method": "GET",
    "status": 200,
    "query": {
      "offset": "0"
    },
    "headers": {}
  },
  {
    "path": "/events",
    "body": {
      "next": null,
      "events": [
        {
          "eventId": "e1",
          "eventTime": 10,
          "eventType": "START",
          "run": {
            "runId": "run1"
          },
          "job": {
            "namespace": "local",
            "name": "prepare"
          },
          "inputs": [
            {
              "namespace": "local",
              "name": "raw",
              "facets": {
                "schema": {
                  "producer": "p1",
                  "schemaURL": "urn:schema:v1",
                  "fields": [
                    "id"
                  ]
                }
              }
            }
          ]
        }
      ]
    },
    "method": "GET",
    "status": 200,
    "query": {
      "offset": "1"
    },
    "headers": {}
  }
];
export const phases={
  "open-run": [
    {
      "path": "/events",
      "body": {
        "next": "/events?offset=7",
        "events": [
          {
            "eventId": "e4",
            "eventTime": 40,
            "eventType": "START",
            "run": {
              "runId": "run3"
            },
            "job": {
              "namespace": "local",
              "name": "prepare"
            },
            "inputs": [
              {
                "namespace": "local",
                "name": "raw"
              }
            ]
          }
        ]
      },
      "method": "GET",
      "status": 200,
      "query": {
        "offset": "0"
      },
      "headers": {}
    },
    {
      "path": "/events",
      "body": {
        "next": null,
        "events": [
          {
            "eventId": "e5",
            "eventTime": 45,
            "eventType": "RUNNING",
            "run": {
              "runId": "run3"
            },
            "job": {
              "namespace": "local",
              "name": "prepare"
            },
            "outputs": [
              {
                "namespace": "local",
                "name": "clean",
                "outputFacets": {
                  "rows": {
                    "producer": "p1",
                    "schemaURL": "urn:rows:v1",
                    "count": 5
                  }
                }
              }
            ]
          }
        ]
      },
      "method": "GET",
      "status": 200,
      "query": {
        "offset": "7"
      },
      "headers": {}
    }
  ],
  "open-run-closed": [
    {
      "path": "/events",
      "body": {
        "next": null,
        "events": [
          {
            "eventId": "e6",
            "eventTime": 50,
            "eventType": "COMPLETE",
            "run": {
              "runId": "run3"
            },
            "job": {
              "namespace": "local",
              "name": "prepare"
            }
          }
        ]
      },
      "method": "GET",
      "status": 200,
      "query": {
        "offset": "0"
      },
      "headers": {}
    }
  ],
  "rerun": [
    {
      "path": "/events",
      "body": {
        "next": null,
        "events": [
          {
            "eventId": "e3",
            "eventTime": 30,
            "eventType": "FAIL",
            "run": {
              "runId": "run2"
            },
            "job": {
              "namespace": "local",
              "name": "prepare"
            },
            "outputs": [
              {
                "namespace": "local",
                "name": "clean",
                "outputFacets": {
                  "rows": {
                    "producer": "p1",
                    "schemaURL": "urn:rows:v1",
                    "count": 3
                  }
                }
              }
            ]
          }
        ]
      },
      "method": "GET",
      "status": 200,
      "query": {
        "offset": "0"
      },
      "headers": {}
    }
  ],
  "schema-conflict": [
    {
      "path": "/events",
      "body": {
        "next": "/events?offset=1",
        "events": [
          {
            "eventId": "conflict1",
            "eventTime": 20,
            "eventType": "COMPLETE",
            "run": {
              "runId": "run1"
            },
            "job": {
              "namespace": "local",
              "name": "prepare"
            },
            "outputs": [
              {
                "namespace": "local",
                "name": "clean",
                "outputFacets": {
                  "rows": {
                    "producer": "p1",
                    "schemaURL": "urn:rows:v2",
                    "count": 12
                  }
                }
              }
            ]
          }
        ]
      },
      "method": "GET",
      "status": 200,
      "query": {
        "offset": "0"
      },
      "headers": {}
    }
  ]
};
export const portalKind="";
